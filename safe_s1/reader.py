import logging
import os
import re

import dask
import fsspec
import numpy as np
import pandas as pd
import rasterio
import xarray as xr
import yaml
from affine import Affine
from rioxarray import rioxarray

from safe_s1 import sentinel1_xml_mappings
from safe_s1.xml_parser import XmlParser


class Sentinel1Reader:
    def __init__(self, name, backend_kwargs=None):
        logging.debug("input name: %s", name)
        if not isinstance(name, (str, os.PathLike)):
            raise ValueError(f"cannot deal with object of type {type(name)}: {name}")
        # gdal dataset name
        if not name.startswith("SENTINEL1_DS:"):
            name = "SENTINEL1_DS:%s:" % name
        self.name = name
        """Gdal dataset name"""
        name_parts = self.name.split(":")
        if len(name_parts) > 3:
            logging.debug("windows case")
            # windows might have semicolon in path ('c:\...')
            name_parts[1] = ":".join(name_parts[1:-1])
            del name_parts[2:-1]
        name_parts[1] = os.path.basename(name_parts[1])
        self.short_name = ":".join(name_parts)
        logging.debug("short_name : %s", self.short_name)
        """Like name, but without path"""
        if len(name_parts) == 2:
            self.path = self.name.split(":")[1]
        else:
            self.path = ":".join(self.name.split(":")[1:-1])
        logging.debug("path: %s", self.path)
        # remove trailing slash in the safe path
        if self.path[-1] == "/":
            self.path = self.path.rstrip("/")
        """Dataset path"""
        self.safe = os.path.basename(self.path)

        self.path = os.fspath(self.path)

        if backend_kwargs is None:
            backend_kwargs = {}

        storage_options = backend_kwargs.get("storage_options", {})

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
        if self.name.endswith(":") and len(self._datasets_names) == 1:
            self.name = self._datasets_names[0]
        self.dsid = self.name.split(":")[-1]
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
        load digital_number from self.sar_meta.files['measurement'], as an `xarray.Dataset`.

        Parameters
        ----------
        resolution: None, numbers.Number, str or dict
        resampling: rasterio.enums.Resampling

        Returns
        -------
        (float, xarray.Dataset)
            tuple that contains resolution and dataset (possibly dual-pol), with basic coords/dims naming convention
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
        files_measurement = [os.path.join(self.path, f) for f in files_measurement]

        # arbitrary rio object, to get shape, etc ... (will not be used to read data)
        rio = rasterio.open(files_measurement[0])

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

            dn = xr.concat(
                [
                    rioxarray.open_rasterio(
                        f, chunks=chunks_rio, parse_coordinates=False
                    )
                    for f in files_measurement
                ],
                "band",
            ).assign_coords(
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

            # resample the DN at gdal level, before feeding it to the dataset
            out_shape = (
                int(rio.height / resolution["line"]),
                int(rio.width / resolution["sample"]),
            )
            out_shape_pol = (1,) + out_shape
            # read resampled array in one chunk, and rechunk
            # this doesn't optimize memory, but total size remain quite small

            if isinstance(resolution["line"], int):
                # legacy behaviour: winsize is the maximum full image size that can be divided  by resolution (int)
                winsize = (
                    0,
                    0,
                    rio.width // resolution["sample"] * resolution["sample"],
                    rio.height // resolution["line"] * resolution["line"],
                )
                window = rasterio.windows.Window(*winsize)
            else:
                window = None

            dn = xr.concat(
                [
                    xr.DataArray(
                        dask.array.from_array(
                            rasterio.open(f).read(
                                out_shape=out_shape_pol,
                                resampling=resampling,
                                window=window,
                            ),
                            chunks=chunks_rio,
                        ),
                        dims=tuple(map_dims.keys()),
                        coords={"pol": [pol]},
                    )
                    for f, pol in zip(
                        files_measurement, self.manifest_attrs["polarizations"]
                    )
                ],
                "pol",
            ).chunk(chunks)

            # create coordinates at box center
            translate = Affine.translation(
                (resolution["sample"] - 1) / 2, (resolution["line"] - 1) / 2
            )
            scale = Affine.scale(
                rio.width // resolution["sample"] * resolution["sample"] / out_shape[1],
                rio.height // resolution["line"] * resolution["line"] / out_shape[0],
            )
            sample, _ = translate * scale * (dn.sample, 0)
            _, line = translate * scale * (0, dn.line)
            dn = dn.assign_coords({"line": line, "sample": sample})

        # for GTiff driver, pols are already ordered. just rename them
        dn = dn.assign_coords(pol=self.manifest_attrs["polarizations"])

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
        xr.DataTree
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
        full_path_xsd = os.path.join(self.path, path_xsd)
        if os.path.exists(full_path_xsd):
            rootxsd = self.xml_parser.getroot(path_xsd)
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
        ds = xr.concat(tmp, pd.Index(pols, name="pol"))
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
        ds = xr.concat(tmp, pd.Index(pols, name="pol"))
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
        ds = xr.concat(tmp, pd.Index(pols, name="pol"))
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
        pandas.core.frame.DataFrame
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
