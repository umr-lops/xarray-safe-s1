import logging
import os
import re
from xml.sax.saxutils import escape

import dask
import fsspec
import numpy as np
import pandas as pd
import rasterio
import xarray as xr
import yaml
from affine import Affine
from rioxarray import rioxarray

from safe_s1 import remote, sentinel1_xml_mappings
from safe_s1.xml_parser import XmlParser


def _float_vrt(path, nodata):
    """
    VRT (XML string) of the first band of `path`, as float32, with a nodata value.

    Declaring a nodata value makes GDAL average the pixels itself, in float32.
    Without it, GDAL delegates the resampling to the file, in its own dtype
    (uint16), and the cell means are rounded to the nearest integer.
    """
    with rasterio.open(path) as src:
        width = src.width
        height = src.height
    return f"""<VRTDataset rasterXSize="{width}" rasterYSize="{height}">
  <VRTRasterBand dataType="Float32" band="1">
    <NoDataValue>{nodata}</NoDataValue>
    <SimpleSource>
      <SourceFilename relativeToVRT="0">{escape(str(path))}</SourceFilename>
      <SourceBand>1</SourceBand>
    </SimpleSource>
  </VRTRasterBand>
</VRTDataset>"""


def _read_on_cells(path, resolution, out_shape, resampling, nodata):
    """
    Resample `path` on `out_shape` cells of exactly
    `resolution["line"]` x `resolution["sample"]` pixels, starting at pixel (0, 0).

    The window is cropped to the whole cells, so that cell k covers pixels
    [k r, (k + 1) r). Non-integer ratios are supported: GDAL weights the pixels
    cut by a cell edge.
    """
    window = rasterio.windows.Window(
        0,
        0,
        out_shape[1] * resolution["sample"],
        out_shape[0] * resolution["line"],
    )
    with rasterio.open(_float_vrt(path, nodata)) as vrt:
        return vrt.read(1, out_shape=out_shape, window=window, resampling=resampling)


def read_cells(path, resolution, out_shape, resampling):
    """
    Resample a measurement file on a grid of whole cells.

    Each cell is the `resampling` mean of the valid pixels it covers, in float32.
    Pixels at 0 (outside the swath) are not counted; a cell without any valid
    pixel is 0.

    Parameters
    ----------
    path: str
        measurement file
    resolution: dict
        cell size in pixels, like `{"line": 100.0, "sample": 100.0}`
    out_shape: tuple
        number of cells `(line, sample)`
    resampling: rasterio.enums.Resampling

    Returns
    -------
    numpy.ndarray
        shape `(1, line, sample)`, float32
    """
    cells = _read_on_cells(path, resolution, out_shape, resampling, nodata=0)
    return cells[np.newaxis]


def read_valid_fraction(path, resolution, out_shape):
    """
    Fraction of non-zero pixels in each cell of the grid used by `read_cells`.

    It is the mean with the zeros counted divided by the mean without them
    (= valid pixels / all pixels). For the mean with the zeros counted, the nodata
    value is -1, which never occurs in the file, so that GDAL still averages in
    float32.

    Returns
    -------
    numpy.ndarray
        shape `(line, sample)`, float32, 0 outside the swath, 1 for a full cell
    """
    average = rasterio.enums.Resampling.average
    mean_all = _read_on_cells(path, resolution, out_shape, average, nodata=-1)
    mean_valid = _read_on_cells(path, resolution, out_shape, average, nodata=0)
    fraction = mean_all / np.where(mean_valid > 0, mean_valid, 1)
    return np.where(mean_valid > 0, fraction, 0).astype("float32")


# dataset id of a SAFE subdataset, see sentinel1_xml_mappings.df_files:
# 'IW', 'IW1', 'EW5', 'S3', 'WV_012'...
_DSID_RE = re.compile(r"^[A-Z]{1,2}[0-9]?(_[0-9]{3})?$")


def split_dataset_name(name):
    """
    Split a dataset name into its SAFE path and its dataset id.

    Parameters
    ----------
    name: str or os.PathLike
        gdal-like name ``SENTINEL1_DS:<path>:<dsid>`` (``<dsid>`` may be empty), or a bare SAFE path.
        ``<path>`` may contain colons: fsspec URL (``s3://bucket/x.SAFE``), windows drive (``C:\\x.SAFE``).

    Returns
    -------
    tuple(str, str)
        (path without trailing slash, dataset id like 'WV_012', 'IW1', or '' for the whole SAFE)
    """
    name = os.fspath(name)
    rest = name[len("SENTINEL1_DS:") :] if name.startswith("SENTINEL1_DS:") else name
    path, sep, dsid = rest.rpartition(":")
    if not sep or not (dsid == "" or _DSID_RE.match(dsid)):
        # no dataset id: the last colon belongs to the path (URL scheme, windows drive)
        path, dsid = rest, ""
    path = path.rstrip("/")
    if not path:
        raise ValueError(f"no SAFE path in dataset name {name!r}")
    return path, dsid


class Sentinel1Reader:
    """
    Reader of a Sentinel-1 SAFE product, local or remote.

    Parameters
    ----------
    name: str or os.PathLike
        SAFE path, or dataset name ``SENTINEL1_DS:<path>:<dsid>`` like
        ``SENTINEL1_DS:/data/x.SAFE:WV_001``. The path can be a local path or a fsspec
        URL of a remote SAFE (``s3://bucket/x.SAFE``, see the *Reading a SAFE from S3* page).
    backend_kwargs: dict or None
        ``{"storage_options": {...}}``: fsspec storage options of a remote SAFE (like
        ``key``, ``secret``, ``client_kwargs={"endpoint_url": ...}`` for ``s3fs``). They
        are used for the XML files, and translated to GDAL config options for the
        measurement files. Without them, the settings come from the environment.

    Examples
    --------
    >>> reader = Sentinel1Reader("SENTINEL1_DS:/data/x.SAFE:WV_001")  # doctest: +SKIP
    >>> resolution, ds = reader.load_digital_number(resolution="1000m")  # doctest: +SKIP
    """

    def __init__(self, name, backend_kwargs=None):
        logging.debug("input name: %s", name)
        if not isinstance(name, (str, os.PathLike)):
            raise ValueError(f"cannot deal with object of type {type(name)}: {name}")
        self.path, dsid = split_dataset_name(name)
        """Dataset path (local path or fsspec URL), without trailing slash.

        For example ``/data/x.SAFE`` or ``s3://bucket/x.SAFE``.
        """
        self.name = f"SENTINEL1_DS:{self.path}:{dsid}"
        """Gdal dataset name"""
        self.short_name = f"SENTINEL1_DS:{os.path.basename(self.path)}:{dsid}"
        """Like name, but without path"""
        logging.debug("name: %s, path: %s", self.name, self.path)
        self.safe = os.path.basename(self.path)

        self.path = os.fspath(self.path)

        if backend_kwargs is None:
            backend_kwargs = {}

        storage_options = backend_kwargs.get("storage_options", {})
        self._gdal_env = remote.gdal_env_options(self.path, storage_options)
        """keyword arguments of `rasterio.Env` to read the measurement files with GDAL"""

        mapper = fsspec.get_mapper(self.path, **storage_options)
        self.xml_parser = XmlParser(
            xpath_mappings=sentinel1_xml_mappings.xpath_mappings,
            compounds_vars=sentinel1_xml_mappings.compounds_vars,
            namespaces=sentinel1_xml_mappings.namespaces,
            mapper=mapper,
        )

        self.manifest = "manifest.safe"
        if "SLC" in self.path or "GRD" in self.path:
            self.manifest_attrs = self.xml_parser.get_compound_var(
                self.manifest, "safe_attributes_slcgrd"
            )
        elif "SL2" in self.path:
            self.manifest_attrs = self.xml_parser.get_compound_var(
                self.manifest, "safe_attributes_sl2"
            )
        else:
            raise Exception("case not handled")

        self._safe_files = None
        self._multidataset = False
        """True if multi dataset"""
        self._datasets_names = list(self.safe_files["dsid"].sort_index().unique())
        self.xsd_definitions = self.get_annotation_definitions()
        self.xsd_units = self.get_annotation_units()
        if self.name.endswith(":") and len(self._datasets_names) == 1:
            self.name = self._datasets_names[0]
        self.dsid = split_dataset_name(self.name)[1]
        """Dataset identifier (like 'WV_001', 'IW1', 'IW'), or empty string for multidataset"""

        try:
            self.product = os.path.basename(self.path).split("_")[2]
        except ValueError:
            print("path: %s" % self.path)
            self.product = "XXX"
        """Product type, like 'GRDH', 'SLC', etc .."""

        # submeta is a list of submeta objects if multidataset and TOPS
        # this list will remain empty for _WV__SLC because it will be time-consuming to process them
        # self._submeta = []
        if self.short_name.endswith(":"):
            self.short_name = self.short_name + self.dsid
        if self.files.empty:
            self._multidataset = True

        self.dt = None
        self._dict = {
            "geolocationGrid": None,
        }
        if not self.multidataset:
            self._dict = {
                "geolocationGrid": self.geoloc,
                "orbit": self.orbit,
                "image": self.image,
                "azimuth_fmrate": self.azimuth_fmrate,
                "doppler_estimate": self.doppler_estimate,
                "bursts": self.bursts,
                "calibration_luts": self.get_calibration_luts,
                "noise_azimuth_raw": self.get_noise_azi_raw,
                "noise_range_raw": self.get_noise_range_raw,
                "antenna_pattern": self.antenna_pattern,
                "swath_merging": self.swath_merging,
            }
            self.dt = xr.DataTree.from_dict(self._dict)
            assert self.dt == self.datatree
        else:
            print("multidataset")
            # there is no error raised here, because we want to let the user access the metadata for multidatasets

    def load_digital_number(
        self, resolution=None, chunks=None, resampling=rasterio.enums.Resampling.rms
    ):
        """
        Load the digital numbers of the measurement files (all polarisations) as an `xarray.Dataset`.

        With `resolution=None`, the files are read lazily at full resolution, in uint16.

        Otherwise, the files are resampled while reading, on a grid of whole cells of
        `r_line` x `r_sample` pixels (`r` is the cell size in pixels):

        * cell `k` covers the pixels `[k * r, (k + 1) * r)`, and its coordinate is the
          cell center `k * r + (r - 1) / 2`, in pixels of the full resolution image;
        * there are `int(size / r)` cells: the pixels left over at the bottom and right
          edges of the image (less than one cell) are not used;
        * the pixels at 0 (outside the swath) are not counted: a cell straddling the
          swath border is the `resampling` mean of its valid pixels only, and a cell
          without any valid pixel is 0;
        * the cell values are kept in float32, they are not rounded to integers;
        * `r` may be non-integer (e.g. 40 m pixels at "100m" give 2.5 pixels): a pixel
          cut by a cell edge is weighted by the part of it inside the cell.

        The dataset also holds `valid_pixel_fraction`: the fraction of non-zero pixels in
        each cell (0: outside the swath, 1: full cell). Since IPF 2.90, the pixels outside
        the swath are set to 0 by the IPF, the same in all polarisations: the fraction is
        computed on the co-polarisation (VV or HH) and given for each polarisation.
        Before IPF 2.90, the near range border holds low non-zero values, counted as valid.

        Parameters
        ----------
        resolution: None, numbers.Number, str or dict
            cell size, None by default:

            * None: full resolution, no resampling;
            * str ending with "m", like "1000m": size in meters, converted to pixels with
              `pixel_line_m` and `pixel_sample_m`;
            * number: size in meters, as for a str;
            * dict like `{"line": 100, "sample": 100}`: size in pixels.
        chunks: dict
            dask chunks, with keys "line" and "sample", like `{"line": 5000, "sample": 5000}`.
            A "pol" key is set to 1.
        resampling: rasterio.enums.Resampling
            mean used for the cells, only with a `resolution`. `rasterio.enums.Resampling.rms` by default.

        Returns
        -------
        (float or None, xarray.Dataset)

            * the resolution in meters if `resolution` is a str, None otherwise;
            * dataset with `digital_number` and, if resampled, `valid_pixel_fraction`
              (dims "pol", "line", "sample").
        """

        def get_glob(strlist):
            # from list of str, replace diff by '?'
            def _get_glob(st):
                stglob = "".join(
                    [
                        "?" if len(charlist) > 1 else charlist[0]
                        for charlist in [list(set(charset)) for charset in zip(*st)]
                    ]
                )
                return re.sub(r"\?+", "*", stglob)

            strglob = _get_glob(strlist)
            if strglob.endswith("*"):
                strglob += _get_glob(s[::-1] for s in strlist)[::-1]
                strglob = strglob.replace("**", "*")

            return strglob

        map_dims = {"pol": "band", "line": "y", "sample": "x"}

        _dtypes = {
            "latitude": "f4",
            "longitude": "f4",
            "incidence": "f4",
            "elevation": "f4",
            "altitude": "f4",
            "ground_heading": "f4",
            "nesz": None,
            "negz": None,
            "sigma0_raw": None,
            "gamma0_raw": None,
            "noise_lut": "f4",
            "noise_lut_range": "f4",
            "noise_lut_azi": "f4",
            "sigma0_lut": "f8",
            "gamma0_lut": "f8",
            "azimuth_time": np.datetime64,
            "slant_range_time": None,
        }

        if resolution is not None:
            comment = 'resampled at "%s" with %s.%s.%s' % (
                resolution,
                resampling.__module__,
                resampling.__class__.__name__,
                resampling.name,
            )
        else:
            comment = "read at full resolution"

        # Add root to path
        files_measurement = self.files["measurement"].copy()
        if remote.is_remote(self.path):
            files_measurement = [f"{self.path}/{f}" for f in files_measurement]
        else:
            files_measurement = [os.path.join(self.path, f) for f in files_measurement]
        # paths for GDAL: /vsis3/... for a remote SAFE
        gdal_files = [remote.gdal_path(f) for f in files_measurement]

        # image shape (the data is read below)
        with rasterio.Env(**self._gdal_env), rasterio.open(gdal_files[0]) as rio:
            height, width = rio.height, rio.width

        chunks["pol"] = 1
        # sort chunks keys like map_dims
        chunks = dict(
            sorted(
                chunks.items(), key=lambda pair: list(map_dims.keys()).index(pair[0])
            )
        )
        chunks_rio = {map_dims[d]: chunks[d] for d in map_dims.keys()}
        res = None
        if resolution is None:
            # using tiff driver: need to read individual tiff and concat them
            # riofiles['rio'] is ordered like self.sar_meta.manifest_attrs['polarizations']

            if remote.is_remote(self.path):
                # each dask block is read in its own rasterio.Env (credentials, endpoint)
                bands = [
                    xr.DataArray(
                        remote.open_band(
                            f, self._gdal_env, (chunks_rio["y"], chunks_rio["x"])
                        )[np.newaxis],
                        dims=("band", "y", "x"),
                    )
                    for f in gdal_files
                ]
            else:
                bands = [
                    rioxarray.open_rasterio(
                        f, chunks=chunks_rio, parse_coordinates=False
                    )
                    for f in gdal_files
                ]
            dn = xr.concat(bands, "band").assign_coords(
                band=np.arange(len(self.manifest_attrs["polarizations"])) + 1
            )

            # set dimensions names
            dn = dn.rename(dict(zip(map_dims.values(), map_dims.keys())))

            # create coordinates from dimension index (because of parse_coordinates=False)
            dn = dn.assign_coords({"line": dn.line, "sample": dn.sample})
            dn = dn.drop_vars("spatial_ref", errors="ignore")
        else:
            if not isinstance(resolution, dict):
                if isinstance(resolution, str) and resolution.endswith("m"):
                    resolution = float(resolution[:-1])
                    res = resolution
                resolution = dict(
                    line=resolution / self.pixel_line_m,
                    sample=resolution / self.pixel_sample_m,
                )
                # resolution = dict(line=resolution / self.dataset['sampleSpacing'].values,
                #                   sample=resolution / self.dataset['lineSpacing'].values)

            # resample the DN at gdal level, before feeding it to the dataset.
            # Whole cells only: the pixels left over at the bottom and right edges
            # (less than one cell) are not used.
            out_shape = (
                int(height / resolution["line"]),
                int(width / resolution["sample"]),
            )
            # the swath border is the same in all polarisations (zeros set by the
            # IPF): one read, on the co-polarisation, which has the strongest signal
            pols = [str(p) for p in self.manifest_attrs["polarizations"]]
            co_pol_file = next(
                (f for f, p in zip(gdal_files, pols) if p[0] == p[1]),
                gdal_files[0],
            )
            # read resampled array in one chunk, and rechunk
            # this doesn't optimize memory, but total size remain quite small
            with rasterio.Env(**self._gdal_env):
                cells = [
                    read_cells(f, resolution, out_shape, resampling) for f in gdal_files
                ]
                valid_fraction = read_valid_fraction(co_pol_file, resolution, out_shape)
            dn = xr.concat(
                [
                    xr.DataArray(
                        dask.array.from_array(cell, chunks=chunks_rio),
                        dims=tuple(map_dims.keys()),
                        coords={"pol": [pol]},
                    )
                    for cell, pol in zip(cells, self.manifest_attrs["polarizations"])
                ],
                "pol",
            ).chunk(chunks)

            # coordinates at cell center: cell k covers pixels [k r, (k + 1) r)
            translate = Affine.translation(
                (resolution["sample"] - 1) / 2, (resolution["line"] - 1) / 2
            )
            scale = Affine.scale(resolution["sample"], resolution["line"])
            sample, _ = translate * scale * (dn.sample, 0)
            _, line = translate * scale * (0, dn.line)
            dn = dn.assign_coords({"line": line, "sample": sample})

        # for GTiff driver, pols are already ordered. just rename them
        # fix 2 June 2025: https://github.com/umr-lops/xsar/issues/254
        polas = [str(uu) for uu in self.manifest_attrs["polarizations"]]
        dn = dn.assign_coords(pol=polas)

        if not all(self.denoised.values()):
            descr = "denoised"
        else:
            descr = "not denoised"
        var_name = "digital_number"

        dn.attrs = {
            "comment": "%s digital number, %s" % (descr, comment),
            "history": yaml.safe_dump(
                {
                    var_name: get_glob(
                        [p.replace(self.path + "/", "") for p in files_measurement]
                    )
                }
            ),
        }
        ds = dn.to_dataset(name=var_name)
        astype = _dtypes.get(var_name)
        if astype is not None:
            ds = ds.astype(_dtypes[var_name])
        if resolution is not None:
            ds["valid_pixel_fraction"] = (
                xr.DataArray(
                    valid_fraction,
                    dims=("line", "sample"),
                    coords={"line": ds.line, "sample": ds.sample},
                    attrs={
                        "long_name": "fraction of valid pixels in the cell",
                        "units": "1",
                        "comment": "pixels at 0 (outside the swath) are not valid; "
                        "0: outside the swath, 1: full cell; "
                        "computed on the co-polarisation, the same for all polarisations",
                    },
                )
                .expand_dims(pol=ds.pol.values)
                .copy()
            )

        return res, ds

    @property
    def pixel_line_m(self):
        """
        pixel line spacing, in meters (at sensor level)

        Returns
        -------
        xarray.Dataset
            Sample spacing
        """
        if self.multidataset:
            res = None  # not defined for multidataset
        else:
            res = self.image["azimuthPixelSpacing"]
        return res

    @property
    def pixel_sample_m(self):
        """
        pixel sample spacing, in meters (at sensor level)

        Returns
        -------
        xarray.Dataset
            Sample spacing
        """
        if self.multidataset:
            res = None  # not defined for multidataset
        else:
            res = self.image["groundRangePixelSpacing"]
        return res

    @property
    def datasets_names(self):
        """
        Alias to `Sentinel1Reader._datasets_names`

        Returns
        -------
        list
            datasets names
        """
        return self._datasets_names

    @property
    def datatree(self):
        """
        Return data of the reader as datatree. Can't open data from a multiple dataset (must select a single one with
        displayed in `Sentinel1Reader.datasets_names`). So if multiple dataset, returns None.
        Alias to `Sentinel1Reader.dt`.

        Returns
        -------
        xarray.DataTree
            Contains data from the reader
        """
        return self.dt

    @property
    def geoloc(self):
        """
        xarray.Dataset with `['longitude', 'latitude', 'altitude', 'azimuth_time', 'slant_range_time','incidence','elevation' ]` variables
        and `['line', 'sample']` coordinates, at the geolocation grid

        Returns
        -------
        xarray.Dataset
            Geolocation Grid
        """
        if self.multidataset:
            raise TypeError("geolocation_grid not available for multidataset")
        if self._dict["geolocationGrid"] is None:
            xml_annotation = self.files["annotation"].iloc[0]
            da_var_list = []
            for var_name in [
                "longitude",
                "latitude",
                "height",
                "azimuthTime",
                "slantRangeTime",
                "incidenceAngle",
                "elevationAngle",
            ]:
                # TODO: we should use dask.array.from_delayed so xml files are read on demand
                da_var = self.xml_parser.get_compound_var(xml_annotation, var_name)
                da_var.name = var_name
                da_var.attrs["history"] = self.xml_parser.get_compound_var(
                    self.files["annotation"].iloc[0], var_name, describe=True
                )
                da_var.attrs["units"] = self.xsd_units.get(var_name, None)
                da_var_list.append(da_var)

            return xr.merge(da_var_list)

    @property
    def orbit(self):
        """
        orbit, as a geopandas.GeoDataFrame, with columns:
          - 'velocity' : shapely.geometry.Point with velocity in x, y, z direction
          - 'geometry' : shapely.geometry.Point with position in x, y, z direction

        crs is set to 'geocentric'

        attrs keys:
          - 'orbit_pass': 'Ascending' or 'Descending'
          - 'platform_heading': in degrees, relative to north

        Notes
        -----
        orbit is longer than the SAFE, because it belongs to all datatakes, not only this slice
        """
        if self.multidataset:
            return None  # not defined for multidataset
        gdf_orbit = self.xml_parser.get_compound_var(
            self.files["annotation"].iloc[0], "orbit"
        )
        for vv in gdf_orbit:
            if vv in self.xsd_definitions:
                gdf_orbit[vv].attrs["definition"] = self.xsd_definitions[vv]
        gdf_orbit.attrs["history"] = self.xml_parser.get_compound_var(
            self.files["annotation"].iloc[0], "orbit", describe=True
        )
        return gdf_orbit

    @property
    def denoised(self):
        """
        dict with pol as key, and bool as values (True is DN is predenoised at L1 level)

        Returns
        -------
        None | dict

        """
        if self.multidataset:
            return None  # not defined for multidataset
        else:
            return dict(
                [
                    self.xml_parser.get_compound_var(f, "denoised")
                    for f in self.files["annotation"]
                ]
            )

    @property
    def time_range(self):
        """
        Get time range

        Returns
        -------

        """
        if not self.multidataset:
            return self.xml_parser.get_var(
                self.files["annotation"].iloc[0], "annotation.line_time_range"
            )

    @property
    def image(self):
        """
        Get image information

        Returns
        -------
        xarray.Dataset
            Image information dataArrays
        """
        if self.multidataset:
            return None
        img_dict = self.xml_parser.get_compound_var(
            self.files["annotation"].iloc[0], "image"
        )
        img_dict["history"] = self.xml_parser.get_compound_var(
            self.files["annotation"].iloc[0], "image", describe=True
        )
        for vv in img_dict:
            if vv in self.xsd_definitions:
                img_dict[vv].attrs["definition"] = self.xsd_definitions[vv]
        return img_dict

    @property
    def azimuth_fmrate(self):
        """

        Returns
        -------
        xarray.Dataset
            Frequency Modulation rate annotations such as t0 (azimuth time reference) and polynomial coefficients: Azimuth FM rate = c0 + c1(tSR - t0) + c2(tSR - t0)^2
        """
        fmrates = self.xml_parser.get_compound_var(
            self.files["annotation"].iloc[0], "azimuth_fmrate"
        )
        fmrates.attrs["history"] = self.xml_parser.get_compound_var(
            self.files["annotation"].iloc[0], "azimuth_fmrate", describe=True
        )
        for vv in fmrates:
            if vv in self.xsd_definitions:
                fmrates[vv].attrs["definition"] = self.xsd_definitions[vv]
        return fmrates

    @property
    def doppler_estimate(self):
        """

        Returns
        -------
        xarray.Dataset
            with Doppler Centroid Estimates from annotations such as geo_polynom,data_polynom or frequency
        """
        dce = self.xml_parser.get_compound_var(
            self.files["annotation"].iloc[0], "doppler_estimate"
        )
        for vv in dce:
            if vv in self.xsd_definitions:
                dce[vv].attrs["definition"] = self.xsd_definitions[vv]
        dce.attrs["history"] = self.xml_parser.get_compound_var(
            self.files["annotation"].iloc[0], "doppler_estimate", describe=True
        )
        return dce

    @property
    def bursts(self):
        """
        Get bursts information

        Returns
        -------
        xarray.Dataset
            Bursts information dataArrays
        """
        if (
            self.xml_parser.get_var(
                self.files["annotation"].iloc[0], "annotation.number_of_bursts"
            )
            > 0
        ):
            bursts = self.xml_parser.get_compound_var(
                self.files["annotation"].iloc[0], "bursts"
            )
            for vv in bursts:
                if vv in self.xsd_definitions:
                    bursts[vv].attrs["definition"] = self.xsd_definitions[vv]
            bursts.attrs["history"] = self.xml_parser.get_compound_var(
                self.files["annotation"].iloc[0], "bursts", describe=True
            )
            return bursts
        else:
            bursts = self.xml_parser.get_compound_var(
                self.files["annotation"].iloc[0], "bursts_grd"
            )
            bursts.attrs["history"] = self.xml_parser.get_compound_var(
                self.files["annotation"].iloc[0], "bursts_grd", describe=True
            )
            return bursts

    @property
    def antenna_pattern(self):
        ds = self.xml_parser.get_compound_var(
            self.files["annotation"].iloc[0], "antenna_pattern"
        )
        ds.attrs["history"] = self.xml_parser.get_compound_var(
            self.files["annotation"].iloc[0], "antenna_pattern", describe=True
        )
        return ds

    @property
    def swath_merging(self):
        if "GRD" in self.product:
            ds = self.xml_parser.get_compound_var(
                self.files["annotation"].iloc[0], "swath_merging"
            )
            ds.attrs["history"] = self.xml_parser.get_compound_var(
                self.files["annotation"].iloc[0], "swath_merging", describe=True
            )
        else:
            ds = xr.Dataset()
        return ds

    @property
    def multidataset(self):
        """
        Alias to `Sentinel1Reader._multidataset`

        Returns
        -------
        bool
        """
        return self._multidataset

    def get_annotation_definitions(self):
        """
        Get annotation definitions (paths used to retrieve concerned data in the files)

        Returns
        -------
        dict
            annotations definitions
        """
        final_dict = {}
        ds_path_xsd = self.xml_parser.get_compound_var(self.manifest, "xsd_files")
        path_xsd = ds_path_xsd["xsd_product"].values[0]
        try:
            # read through the fsspec mapper: works for local paths and remote URLs
            rootxsd = self.xml_parser.getroot(path_xsd)
        except KeyError:  # the mapper raises KeyError for a missing file
            logging.debug("no annotation definitions: %s not found", path_xsd)
        else:
            mypath = "/xsd:schema/xsd:complexType/xsd:sequence/xsd:element"

            for lulu, uu in enumerate(
                rootxsd.xpath(mypath, namespaces=sentinel1_xml_mappings.namespaces)
            ):
                mykey = uu.values()[0]
                if uu.getchildren() != []:
                    myvalue = uu.getchildren()[0].getchildren()[0]
                else:
                    myvalue = None
                final_dict[mykey] = myvalue

        return final_dict

    def get_annotation_units(self):
        """
        Get variable units from annotation definitions (inside square brackets : "[UNIT]")

        Returns
        -------
        dict
            annotation units
        """
        definitions = self.xsd_definitions

        units = {}
        for key, definition in definitions.items():
            match = re.search(r"\[([^\]]*)\]", str(definition))
            if match and match.group(1):
                units[key] = match.group(1)

        return units

    @property
    def get_calibration_luts(self):
        """
        get original (ie not interpolation) xr.Dataset sigma0 and gamma0 Look Up Tables to apply calibration

        Returns
        -------
        xarray.Dataset
            Original sigma0 and gamma0 calibration Look Up Tables
        """
        # sigma0_lut = self.xml_parser.get_var(self.files['calibration'].iloc[0], 'calibration.sigma0_lut',describe=True)
        pols = []
        tmp = []
        for pol_code, xml_file in self.files["calibration"].items():
            luts_ds = self.xml_parser.get_compound_var(xml_file, "luts_raw")
            # add history to attributes
            minifile = re.sub(".*SAFE/", "", xml_file)
            minifile = re.sub(r"-.*\.xml", ".xml", minifile)
            for da in luts_ds:
                histo = self.xml_parser.get_var(
                    xml_file, f"calibration.{da}", describe=True
                )
                luts_ds[da].attrs["history"] = yaml.safe_dump({da: {minifile: histo}})

            pol = os.path.basename(xml_file).split("-")[4].upper()
            pols.append(pol)
            tmp.append(luts_ds)
        ds = xr.concat(tmp, pd.Index(pols, name="pol").astype(np.str_))
        # ds.attrs = {'description':
        #                                 'original (ie not interpolation) xr.Dataset sigma0 and gamma0 Look Up Tables'}
        return ds

    @property
    def get_noise_azi_raw(self):
        """
        Get raw noise azimuth lut

        Returns
        -------
        xarray.Dataset
            raw noise azimuth lut
        """
        tmp = []
        pols = []
        history = []
        for pol_code, xml_file in self.files["noise"].items():
            pol = os.path.basename(xml_file).split("-")[4].upper()
            pols.append(pol)
            if self.product == "SLC" or self.product == "SL2":
                noise_lut_azi_raw_ds = self.xml_parser.get_compound_var(
                    xml_file, "noise_lut_azi_raw_slc"
                )
                history.append(
                    self.xml_parser.get_compound_var(
                        xml_file, "noise_lut_azi_raw_slc", describe=True
                    )
                )
            else:
                noise_lut_azi_raw_ds = self.xml_parser.get_compound_var(
                    xml_file, "noise_lut_azi_raw_grd"
                )
                # noise_lut_azi_raw_ds.attrs[f'raw_azi_lut_{pol}'] = \
                #    self.xml_parser.get_var(xml_file, 'noise.azi.noiseLut')
                history.append(
                    self.xml_parser.get_compound_var(
                        xml_file, "noise_lut_azi_raw_grd", describe=True
                    )
                )
            for vari in noise_lut_azi_raw_ds:
                if "noise_lut" in vari:
                    varitmp = "noiseLut"
                    hihi = self.xml_parser.get_var(
                        self.files["noise"].iloc[0],
                        "noise.azi.%s" % varitmp,
                        describe=True,
                    )
                elif vari == "noise_lut" and self.product == "WV":  # WV case
                    hihi = "dummy variable, noise is not defined in azimuth for WV acquisitions"
                else:
                    varitmp = vari
                    hihi = self.xml_parser.get_var(
                        self.files["noise"].iloc[0],
                        "noise.azi.%s" % varitmp,
                        describe=True,
                    )

                noise_lut_azi_raw_ds[vari].attrs["description"] = hihi
            tmp.append(noise_lut_azi_raw_ds)
        ds = xr.concat(tmp, pd.Index(pols, name="pol", dtype=object))
        ds.attrs["history"] = "\n".join(history)
        return ds

    @property
    def get_noise_range_raw(self):
        """
        Get raw noise range lut

        Returns
        -------
        xarray.Dataset
            raw noise range lut
        """
        tmp = []
        pols = []
        history = []
        for pol_code, xml_file in self.files["noise"].items():
            # pol = self.files['polarization'].cat.categories[pol_code - 1]
            pol = os.path.basename(xml_file).split("-")[4].upper()
            pols.append(pol)
            noise_lut_range_raw_ds = self.xml_parser.get_compound_var(
                xml_file, "noise_lut_range_raw"
            )
            for vari in noise_lut_range_raw_ds:
                if "noise_lut" in vari:
                    varitmp = "noiseLut"
                hihi = self.xml_parser.get_var(
                    self.files["noise"].iloc[0],
                    "noise.range.%s" % varitmp,
                    describe=True,
                )
                noise_lut_range_raw_ds[vari].attrs["description"] = hihi
                history.append(
                    self.xml_parser.get_compound_var(
                        xml_file, "noise_lut_range_raw", describe=True
                    )
                )
            tmp.append(noise_lut_range_raw_ds)
        ds = xr.concat(tmp, pd.Index(pols, name="pol", dtype=object))
        ds.attrs["history"] = "\n".join(history)
        return ds

    def get_noise_azi_initial_parameters(self, pol):
        """
        Retrieve initial noise lut and lines

        Parameters
        ----------
        pol: str
            polarization selected

        Returns
        -------
        (List, List, List, List, List, List, List)
            Tuple that contains the swaths, noise azimuth lines, line_start, line_stop, sample_start, sample_stop and
            noise azimuth lut for the pol selected.
        """
        for pol_code, xml_file in self.files["noise"].items():
            if pol in os.path.basename(xml_file).upper():
                return (
                    self.xml_parser.get_var(xml_file, "noise.azi.swath"),
                    self.xml_parser.get_var(xml_file, "noise.azi.line"),
                    self.xml_parser.get_var(xml_file, "noise.azi.line_start"),
                    self.xml_parser.get_var(xml_file, "noise.azi.line_stop"),
                    self.xml_parser.get_var(xml_file, "noise.azi.sample_start"),
                    self.xml_parser.get_var(xml_file, "noise.azi.sample_stop"),
                    self.xml_parser.get_var(xml_file, "noise.azi.noiseLut"),
                )

    @property
    def safe_files(self):
        """
        Files and polarizations for whole SAFE.
        The index is the file number, extracted from the filename.
        To get files in official SAFE order, the resulting dataframe should be sorted by polarization or index.

        Returns
        -------
        pandas.DataFrame
            Columns:
                * index         : file number, extracted from the filename.
                * dsid          : dataset id, compatible with gdal sentinel1 driver ('SENTINEL1_DS:/path/file.SAFE:WV_012')
                * polarization  : polarization name.
                * annotation    : xml annotation file.
                * calibration   : xml calibration file.
                * noise         : xml noise file.
                * measurement   : tiff measurement file.

        See Also
        --------
        Sentinel1Reader.files

        """
        if self._safe_files is None:
            files = self.xml_parser.get_compound_var(self.manifest, "files")

            """
            # add path
            for f in ['annotation', 'measurement', 'noise', 'calibration']:
                files[f] = files[f].map(lambda f: os.path.join(# self.path,
                     f))"""

            # set "polarization" as a category, so sorting dataframe on polarization
            # will return the dataframe in same order as self._safe_attributes['polarizations']
            files["polarization"] = files.polarization.astype(
                "category"
            ).cat.reorder_categories(self.manifest_attrs["polarizations"], ordered=True)
            # replace 'dsid' with full path, compatible with gdal sentinel1 driver
            files["dsid"] = files["dsid"].map(
                lambda dsid: "SENTINEL1_DS:%s:%s" % (self.path, dsid)
            )
            files.sort_values("polarization", inplace=True)
            self._safe_files = files
        return self._safe_files

    @property
    def files(self):
        """
        Files for current dataset. (Empty for multi datasets)

        See Also
        --------
        Sentinel1Reader.safe_files
        """
        return self.safe_files[self.safe_files["dsid"] == self.name]

    def __repr__(self):
        if self.multidataset:
            typee = "multi (%d)" % len(self.subdatasets)
        else:
            typee = "single"
        return "<Sentinel1Reader %s object>" % typee
