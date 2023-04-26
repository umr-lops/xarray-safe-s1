import os
import fsspec
from . import sentinel1_xml_mappings
from .xml_parser import XmlParser
import xarray as xr
import geopandas as gpd
import datatree
import pandas as pd
import warnings


class Sentinel1Reader:

    def __init__(self, name, backend_kwargs=None):
        if not isinstance(name, (str, os.PathLike)):
            raise ValueError(f"cannot deal with object of type {type(name)}: {name}")
        # gdal dataset name
        if not name.startswith('SENTINEL1_DS:'):
            name = 'SENTINEL1_DS:%s:' % name
        self.name = name
        """Gdal dataset name"""
        name_parts = self.name.split(':')
        if len(name_parts) > 3:
            # windows might have semicolon in path ('c:\...')
            name_parts[1] = ':'.join(name_parts[1:-1])
            del name_parts[2:-1]
        name_parts[1] = os.path.basename(name_parts[1])
        self.short_name = ':'.join(name_parts)
        """Like name, but without path"""
        self.path = ':'.join(self.name.split(':')[1:-1])
        """Dataset path"""
        self.safe = os.path.basename(self.path)

        if backend_kwargs is None:
            backend_kwargs = {}
        self.path = os.fspath(self.path)
        storage_options = backend_kwargs.get("storage_options", {})
        mapper = fsspec.get_mapper(self.path, **storage_options)
        self.xml_parser = XmlParser(
            xpath_mappings=sentinel1_xml_mappings.xpath_mappings,
            compounds_vars=sentinel1_xml_mappings.compounds_vars,
            namespaces=sentinel1_xml_mappings.namespaces,
            mapper=mapper
        )

        self.manifest = 'manifest.safe'
        self.manifest_attrs = self.xml_parser.get_compound_var(self.manifest, 'safe_attributes')

        self._safe_files = None
        self._multidataset = False
        """True if multi dataset"""
        self.subdatasets = gpd.GeoDataFrame(geometry=[], index=[])
        """Subdatasets as GeodataFrame (empty if single dataset)"""
        self._datasets_names = list(self.safe_files['dsid'].sort_index().unique())
        self.xsd_definitions = self.get_annotation_definitions()
        if self.name.endswith(':') and len(self._datasets_names) == 1:
            self.name = self._datasets_names[0]
        self.dsid = self.name.split(':')[-1]
        """Dataset identifier (like 'WV_001', 'IW1', 'IW'), or empty string for multidataset"""

        try:
            self.product = os.path.basename(self.path).split('_')[2]
        except:
            print("path: %s" % self.path)
            self.product = "XXX"
        """Product type, like 'GRDH', 'SLC', etc .."""

        # submeta is a list of submeta objects if multidataset and TOPS
        # this list will remain empty for _WV__SLC because it will be time-consuming to process them
        # self._submeta = []
        if self.short_name.endswith(':'):
            self.short_name = self.short_name + self.dsid
        if self.files.empty:
            try:
                self.subdatasets = gpd.GeoDataFrame(geometry=self.manifest_attrs['footprints'], index=self._datasets_names)
            except ValueError:
                # not as many footprints than subdatasets count. (probably TOPS product)
                #self._submeta = [ReadMetadata(subds) for subds in self._datasets_names]
                # sub_footprints = [submeta.footprint for submeta in self._submeta]
                self.subdatasets = gpd.GeoDataFrame(  # geometry=sub_footprints,
                    index=self._datasets_names)
            self._multidataset = True

        self.dt = None
        self._dict = {
            'geolocationGrid': None,
        }
        if not self.multidataset:

            self._dict = {
                'geolocationGrid': self.geoloc,
                'orbit': self.orbit,
                'image': self.image,
                'azimuth_fmrate': self.azimuth_fmrate,
                'doppler_estimate': self.doppler_estimate,
                'bursts': self.bursts,
                'calibration_luts': self.get_calibration_luts,
                'noise_azimuth_raw': self.get_noise_azi_raw,
                'noise_range_raw': self.get_noise_range_raw,
            }
            self.dt = datatree.DataTree.from_dict(self._dict)

    @property
    def datasets_names(self):
        """
        Alias to `Sentinel1Reader._datasets_names`

        Returns
        -------

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
        datatree.Datatree
            Contains data from the reader
        """
        """if self.multidataset:
            raise warnings.warn("Please select an only dataset from `Sentinel1Reader.datasets_names`")
            return self.dt
        else:"""
        return self.dt

    @property
    def geoloc(self):
        """
        xarray.Dataset with `['longitude', 'latitude', 'altitude', 'azimuth_time', 'slant_range_time','incidence','elevation' ]` variables
        and `['line', 'sample']` coordinates, at the geolocation grid
        """
        if self.multidataset:
            raise TypeError('geolocation_grid not available for multidataset')
        if self._dict['geolocationGrid'] is None:
            xml_annotation = self.files['annotation'].iloc[0]
            da_var_list = []
            for var_name in ['longitude', 'latitude', 'height', 'azimuthTime', 'slantRangeTime', 'incidenceAngle',
                             'elevationAngle']:
                # TODO: we should use dask.array.from_delayed so xml files are read on demand
                da_var = self.xml_parser.get_compound_var(xml_annotation, var_name)
                da_var.name = var_name
                da_var.attrs['history'] = self.xml_parser.get_compound_var(self.files['annotation'].iloc[0],
                                                                           var_name,
                                                                           describe=True)
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
        gdf_orbit = self.xml_parser.get_compound_var(self.files['annotation'].iloc[0], 'orbit')
        for vv in gdf_orbit:
            if vv in self.xsd_definitions:
                gdf_orbit[vv].attrs['definition'] = self.xsd_definitions[vv]
        gdf_orbit.attrs['history'] = self.xml_parser.get_compound_var(self.files['annotation'].iloc[0], 'orbit',
                                                                      describe=True)
        return gdf_orbit

    @property
    def denoised(self):
        """dict with pol as key, and bool as values (True is DN is predenoised at L1 level)"""
        if self.multidataset:
            return None  # not defined for multidataset
        else:
            return dict(
                [self.xml_parser.get_compound_var(f, 'denoised') for f in self.files['annotation']])

    @property
    def time_range(self):
        """
        Get time range

        Returns
        -------

        """
        if not self.multidataset:
            return self.xml_parser.get_var(self.files['annotation'].iloc[0], 'annotation.line_time_range')

    @property
    def image(self) -> xr.Dataset:
        if self.multidataset:
            return None
        img_dict = self.xml_parser.get_compound_var(self.files['annotation'].iloc[0], 'image')
        img_dict['history'] = self.xml_parser.get_compound_var(self.files['annotation'].iloc[0], 'image', describe=True)
        for vv in img_dict:
            if vv in self.xsd_definitions:
                img_dict[vv].attrs['definition'] = self.xsd_definitions[vv]
        return img_dict

    @property
    def azimuth_fmrate(self):
        """
        xarray.Dataset
            Frequency Modulation rate annotations such as t0 (azimuth time reference) and polynomial coefficients: Azimuth FM rate = c0 + c1(tSR - t0) + c2(tSR - t0)^2
        """
        fmrates = self.xml_parser.get_compound_var(self.files['annotation'].iloc[0], 'azimuth_fmrate')
        fmrates.attrs['history'] = self.xml_parser.get_compound_var(self.files['annotation'].iloc[0], 'azimuth_fmrate',
                                                                    describe=True)
        for vv in fmrates:
            if vv in self.xsd_definitions:
                fmrates[vv].attrs['definition'] = self.xsd_definitions[vv]
        return fmrates

    @property
    def doppler_estimate(self):
        """
        xarray.Dataset
            with Doppler Centroid Estimates from annotations such as geo_polynom,data_polynom or frequency
        """
        dce = self.xml_parser.get_compound_var(self.files['annotation'].iloc[0], 'doppler_estimate')
        for vv in dce:
            if vv in self.xsd_definitions:
                dce[vv].attrs['definition'] = self.xsd_definitions[vv]
        dce.attrs['history'] = self.xml_parser.get_compound_var(self.files['annotation'].iloc[0], 'doppler_estimate',
                                                                describe=True)
        return dce

    @property
    def bursts(self):
        if self.xml_parser.get_var(self.files['annotation'].iloc[0], 'annotation.number_of_bursts') > 0:
            bursts = self.xml_parser.get_compound_var(self.files['annotation'].iloc[0], 'bursts')
            for vv in bursts:
                if vv in self.xsd_definitions:
                    bursts[vv].attrs['definition'] = self.xsd_definitions[vv]
            bursts.attrs['history'] = self.xml_parser.get_compound_var(self.files['annotation'].iloc[0], 'bursts',
                                                                       describe=True)
            return bursts
        else:
            bursts = self.xml_parser.get_compound_var(self.files['annotation'].iloc[0], 'bursts_grd')
            bursts.attrs['history'] = self.xml_parser.get_compound_var(self.files['annotation'].iloc[0], 'bursts_grd',
                                                                       describe=True)
            return bursts

    @property
    def multidataset(self):
        """
        Alias to `Sentinel1Reader._multidataset`
        """
        return self._multidataset

    def get_annotation_definitions(self):
        """

        :return:
        """
        final_dict = {}
        ds_path_xsd = self.xml_parser.get_compound_var(self.manifest, 'xsd_files')
        path_xsd = ds_path_xsd['xsd_product'].values[0]
        full_path_xsd = os.path.join(self.path, path_xsd)
        if os.path.exists(full_path_xsd):
            rootxsd = self.xml_parser.getroot(path_xsd)
            mypath = '/xsd:schema/xsd:complexType/xsd:sequence/xsd:element'

            for lulu, uu in enumerate(rootxsd.xpath(mypath, namespaces=sentinel1_xml_mappings.namespaces)):
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
        """
        #sigma0_lut = self.xml_parser.get_var(self.files['calibration'].iloc[0], 'calibration.sigma0_lut',describe=True)
        pols = []
        tmp = []
        for pol_code, xml_file in self.files['calibration'].items():
            luts_ds = self.xml_parser.get_compound_var(xml_file, 'luts_raw')
            # add history to attributes
            for da in luts_ds:
                luts_ds[da].attrs['history'] = self.xml_parser.get_compound_var(xml_file, da, describe=True)

            pol = os.path.basename(xml_file).split('-')[4].upper()
            pols.append(pol)
            tmp.append(luts_ds)
        ds = xr.concat(tmp, pd.Index(pols, name="pol"))
        # ds.attrs = {'description':
        #                                 'original (ie not interpolation) xr.Dataset sigma0 and gamma0 Look Up Tables'}
        return ds

    @property
    def get_noise_azi_raw(self):
        tmp = []
        pols = []
        history = []
        for pol_code, xml_file in self.files['noise'].items():
            #pol = self.files['polarization'].cat.categories[pol_code-1]
            pol = os.path.basename(xml_file).split('-')[4].upper()
            pols.append(pol)
            if self.product == 'SLC':
                noise_lut_azi_raw_ds = self.xml_parser.get_compound_var(xml_file, 'noise_lut_azi_raw_slc')
                history.append(self.xml_parser.get_compound_var(xml_file, 'noise_lut_azi_raw_slc', describe=True))
            else:
                noise_lut_azi_raw_ds = self.xml_parser.get_compound_var(xml_file, 'noise_lut_azi_raw_grd')
                history.append(self.xml_parser.get_compound_var(xml_file, 'noise_lut_azi_raw_grd', describe=True))
            for vari in noise_lut_azi_raw_ds:
                if 'noise_lut' in vari:
                    varitmp = 'noiseLut'
                    hihi = self.xml_parser.get_var(self.files['noise'].iloc[0], 'noise.azi.%s' % varitmp,
                                                   describe=True)
                elif vari == 'noise_lut' and self.product=='WV': #WV case
                    hihi = 'dummy variable, noise is not defined in azimuth for WV acquisitions'
                else:
                    varitmp = vari
                    hihi = self.xml_parser.get_var(self.files['noise'].iloc[0], 'noise.azi.%s' % varitmp,
                                                   describe=True)

                noise_lut_azi_raw_ds[vari].attrs['description'] = hihi
            tmp.append(noise_lut_azi_raw_ds)
        ds = xr.concat(tmp, pd.Index(pols, name="pol"))
        ds.attrs['history'] = '\n'.join(history)
        return ds

    @property
    def get_noise_range_raw(self):
        tmp = []
        pols = []
        for pol_code, xml_file in self.files['noise'].items():
            #pol = self.files['polarization'].cat.categories[pol_code - 1]
            pol = os.path.basename(xml_file).split('-')[4].upper()
            pols.append(pol)
            noise_lut_range_raw_ds = self.xml_parser.get_compound_var(xml_file, 'noise_lut_range_raw')
            for vari in noise_lut_range_raw_ds:
                if 'noise_lut' in vari:
                    varitmp = 'noiseLut'
                hihi = self.xml_parser.get_var(self.files['noise'].iloc[0], 'noise.range.%s' % varitmp,
                                               describe=True)
                noise_lut_range_raw_ds[vari].attrs['description'] = hihi
            tmp.append(noise_lut_range_raw_ds)
        ds = xr.concat(tmp, pd.Index(pols, name="pol"))
        return ds

    @property
    def safe_files(self):
        """
        Files and polarizations for whole SAFE.
        The index is the file number, extracted from the filename.
        To get files in official SAFE order, the resulting dataframe should be sorted by polarization or index.

        Returns
        -------
        pandas.Dataframe
            with columns:
                * index         : file number, extracted from the filename.
                * dsid          : dataset id, compatible with gdal sentinel1 driver ('SENTINEL1_DS:/path/file.SAFE:WV_012')
                * polarization  : polarization name.
                * annotation    : xml annotation file.
                * calibration   : xml calibration file.
                * noise         : xml noise file.
                * measurement   : tiff measurement file.

        See Also
        --------
        xsar.Sentinel1Meta.files

        """
        if self._safe_files is None:
            files = self.xml_parser.get_compound_var(self.manifest, 'files')

            """
            # add path
            for f in ['annotation', 'measurement', 'noise', 'calibration']:
                files[f] = files[f].map(lambda f: os.path.join(# self.path,
                     f))"""

            # set "polarization" as a category, so sorting dataframe on polarization
            # will return the dataframe in same order as self._safe_attributes['polarizations']
            files["polarization"] = files.polarization.astype('category').cat.reorder_categories(
                self.manifest_attrs['polarizations'], ordered=True)
            # replace 'dsid' with full path, compatible with gdal sentinel1 driver
            files['dsid'] = files['dsid'].map(lambda dsid: "SENTINEL1_DS:%s:%s" % (self.path, dsid))
            files.sort_values('polarization', inplace=True)
            self._safe_files = files
        return self._safe_files

    @property
    def files(self):
        """
        Files for current dataset. (Empty for multi datasets)

        See Also
        --------
        xsar.Sentinel1Meta.safe_files
        """
        return self.safe_files[self.safe_files['dsid'] == self.name]

    def __repr__(self):
        if self.multidataset:
            typee = "multi (%d)" % len(self.subdatasets)
        else:
            typee = "single"
        return "<Sentinel1Reader %s object>" % typee
