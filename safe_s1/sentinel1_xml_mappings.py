"""
xpath mapping from xml file, with convertion functions
"""
import zipfile

import aiohttp
import fsspec
import xarray
from datetime import datetime
import numpy as np
import pandas as pd
import xarray as xr
from numpy.polynomial import Polynomial
import warnings
import geopandas as gpd
from shapely.geometry import Polygon, Point
import os.path
import pyproj

namespaces = {
    "xfdu": "urn:ccsds:schema:xfdu:1",
    "s1sarl1": "http://www.esa.int/safe/sentinel-1.0/sentinel-1/sar/level-1",
    "s1sar": "http://www.esa.int/safe/sentinel-1.0/sentinel-1/sar",
    "s1": "http://www.esa.int/safe/sentinel-1.0/sentinel-1",
    "safe": "http://www.esa.int/safe/sentinel-1.0",
    "gml": "http://www.opengis.net/gml"
}
# xpath convertion function: they take only one args (list returned by xpath)
scalar = lambda x: x[0]
scalar_int = lambda x: int(x[0])
scalar_float = lambda x: float(x[0])
date_converter = lambda x: datetime.strptime(x[0], '%Y-%m-%dT%H:%M:%S.%f')
datetime64_array = lambda x: np.array([np.datetime64(date_converter([sx])) for sx in x])
int_1Darray_from_string = lambda x: np.fromstring(x[0], dtype=int, sep=' ')
float_2Darray_from_string_list = lambda x: np.vstack([np.fromstring(e, dtype=float, sep=' ') for e in x])
list_of_float_1D_array_from_string = lambda x: [np.fromstring(e, dtype=float, sep=' ') for e in x]
int_1Darray_from_join_strings = lambda x: np.fromstring(" ".join(x), dtype=int, sep=' ')
float_1Darray_from_join_strings = lambda x: np.fromstring(" ".join(x), dtype=float, sep=' ')
int_array = lambda x: np.array(x, dtype=int)
bool_array = lambda x: np.array(x, dtype=bool)
float_array = lambda x: np.array(x, dtype=float)
uniq_sorted = lambda x: np.array(sorted(set(x)))
ordered_category = lambda x: pd.Categorical(x).reorder_categories(x, ordered=True)
normpath = lambda paths: [os.path.normpath(p) for p in paths]


def get_test_file(fname):
    """
    get test file from  https://cyclobs.ifremer.fr/static/sarwing_datarmor/xsardata/
    file is unzipped and extracted to `config['data_dir']`

    Parameters
    ----------
    fname: str
        file name to get (without '.zip' extension)

    Returns
    -------
    str
        path to file, relative to `config['data_dir']`

    """
    config = {'data_dir': '/tmp'}

    def url_get(url, cache_dir=os.path.join(config['data_dir'], 'fsspec_cache')):
        """
        Get fil from url, using caching.

        Parameters
        ----------
        url: str
        cache_dir: str
            Cache dir to use. default to `os.path.join(config['data_dir'], 'fsspec_cache')`

        Raises
        ------
        FileNotFoundError

        Returns
        -------
        filename: str
            The local file name

        Notes
        -----
        Due to fsspec, the returned filename won't match the remote one.
        """

        if '://' in url:
            with fsspec.open(
                    'filecache::%s' % url,
                    https={'client_kwargs': {'timeout': aiohttp.ClientTimeout(total=3600)}},
                    filecache={'cache_storage': os.path.join(os.path.join(config['data_dir'], 'fsspec_cache'))}
            ) as f:
                fname = f.name
        else:
            fname = url

        return fname

    res_path = config['data_dir']
    base_url = 'https://cyclobs.ifremer.fr/static/sarwing_datarmor/xsardata'
    file_url = '%s/%s.zip' % (base_url, fname)
    if not os.path.exists(os.path.join(res_path, fname)):
        warnings.warn("Downloading %s" % file_url)
        local_file = url_get(file_url)
        warnings.warn("Unzipping %s" % os.path.join(res_path, fname))
        with zipfile.ZipFile(local_file, 'r') as zip_ref:
            zip_ref.extractall(res_path)
    return os.path.join(res_path, fname)


def or_ipf28(xpath):
    """change xpath to match ipf <2.8 or >2.9 (for noise range)"""
    xpath28 = xpath.replace('noiseRange', 'noise').replace('noiseAzimuth', 'noise')
    if xpath28 != xpath:
        xpath += " | %s" % xpath28
    return xpath


def list_poly_from_list_string_coords(str_coords_list):
    footprints = []
    for gmlpoly in str_coords_list:
        footprints.append(Polygon(
            [(float(lon), float(lat)) for lat, lon in [latlon.split(",")
                                                       for latlon in gmlpoly.split(" ")]]))
    return footprints


# xpath_mappings:
# first level key is xml file type
# second level key is variable name
# mappings may be 'xpath', or 'tuple(func,xpath)', or 'dict'
#  - xpath is an lxml xpath
#  - func is a decoder function fed by xpath
#  - dict is a nested dict, to create more hierarchy levels.
xpath_mappings = {
    "manifest": {
        'ipf_version': (scalar_float, '//xmlData/safe:processing/safe:facility/safe:software/@version'),
        'swath_type': (scalar, '//s1sarl1:instrumentMode/s1sarl1:mode'),
        # 'product': (scalar, '/xfdu:XFDU/informationPackageMap/xfdu:contentUnit/@textInfo'),
        'polarizations': (
            ordered_category, '//s1sarl1:standAloneProductInformation/s1sarl1:transmitterReceiverPolarisation'),
        'footprints': (list_poly_from_list_string_coords, '//safe:frame/safe:footPrint/gml:coordinates'),
        'product_type': (scalar, '//s1sarl1:standAloneProductInformation/s1sarl1:productType'),
        'mission': (scalar, '//safe:platform/safe:familyName'),
        'satellite': (scalar, '//safe:platform/safe:number'),
        'start_date': (date_converter, '//safe:acquisitionPeriod/safe:startTime'),
        'stop_date': (date_converter, '//safe:acquisitionPeriod/safe:stopTime'),
        'aux_cal': (scalar,
                    '//metadataSection/metadataObject/metadataWrap/xmlData/safe:processing/safe:resource/safe:processing/safe:resource[@role="AUX_CAL"]/@name'),
        'aux_cal_sl2': (scalar,'//metadataSection/metadataObject/metadataWrap/xmlData/safe:processing/safe:resource[@role="AUX_CAL"]/@name'),
        'annotation_files': (
            normpath, '/xfdu:XFDU/dataObjectSection/*[@repID="s1Level1ProductSchema"]/byteStream/fileLocation/@href'),
        'measurement_files': (
            normpath,
            '/xfdu:XFDU/dataObjectSection/*[@repID="s1Level1MeasurementSchema"]/byteStream/fileLocation/@href'),
        'noise_files': (
            normpath, '/xfdu:XFDU/dataObjectSection/*[@repID="s1Level1NoiseSchema"]/byteStream/fileLocation/@href'),
        'calibration_files': (
            normpath,
            '/xfdu:XFDU/dataObjectSection/*[@repID="s1Level1CalibrationSchema"]/byteStream/fileLocation/@href'),
        'xsd_product_file': (
            normpath, '/xfdu:XFDU/metadataSection/metadataObject[@ID="s1Level1ProductSchema"]/metadataReference/@href'),
        'xsd_Noise_file': (
            normpath, '/xfdu:XFDU/metadataSection/metadataObject[@ID="s1Level1NoiseSchema"]/metadataReference/@href'),
        'xsd_RFI_file': (
            normpath, '/xfdu:XFDU/metadataSection/metadataObject[@ID="s1Level1RfiSchema"]/metadataReference/@href'),
        'xsd_calibration_file': (
            normpath,
            '/xfdu:XFDU/metadataSection/metadataObject[@ID="s1Level1CalibrationSchema"]/metadataReference/@href'),
        'xsd_objecttype_file': (
            normpath, '/xfdu:XFDU/metadataSection/metadataObject[@ID="s1ObjectTypesSchema"]/metadataReference/@href'),
        'xsd_measurement_file': (
            normpath,
            '/xfdu:XFDU/metadataSection/metadataObject[@ID="s1Level1MeasurementSchema"]/metadataReference/@href'),
        'xsd_level1product_file': (normpath,
                                   '/xfdu:XFDU/metadataSection/metadataObject[@ID="s1Level1ProductPreviewSchema"]/metadataReference/@href'),
        'xsd_overlay_file': (
            normpath,
            '/xfdu:XFDU/metadataSection/metadataObject[@ID="s1Level1MapOverlaySchema"]/metadataReference/@href'),
    },
    'calibration': {
        'polarization': (scalar, '/calibration/adsHeader/polarisation'),
        # 'number_of_vector': '//calibration/calibrationVectorList/@count',
        'line': (np.array, '//calibration/calibrationVectorList/calibrationVector/line'),
        'sample': (int_1Darray_from_string, '//calibration/calibrationVectorList/calibrationVector[1]/pixel'),
        'sigma0_lut': (
            float_2Darray_from_string_list, '//calibration/calibrationVectorList/calibrationVector/sigmaNought'),
        'gamma0_lut': (float_2Darray_from_string_list, '//calibration/calibrationVectorList/calibrationVector/gamma')
    },
    'noise': {
        'mode': (scalar, '/noise/adsHeader/mode'),
        'polarization': (scalar, '/noise/adsHeader/polarisation'),
        'range': {
            'line': (int_array, or_ipf28('/noise/noiseRangeVectorList/noiseRangeVector/line')),
            'sample': (lambda x: [np.fromstring(s, dtype=int, sep=' ') for s in x],
                       or_ipf28('/noise/noiseRangeVectorList/noiseRangeVector/pixel')),
            'noiseLut': (
                lambda x: [np.fromstring(s, dtype=float, sep=' ') for s in x],
                or_ipf28('/noise/noiseRangeVectorList/noiseRangeVector/noiseRangeLut'))
        },
        'azi': {
            'swath': '/noise/noiseAzimuthVectorList/noiseAzimuthVector/swath',
            'line': (lambda x: [np.fromstring(str(s), dtype=int, sep=' ') for s in x],
                     '/noise/noiseAzimuthVectorList/noiseAzimuthVector/line'),
            'line_start': (int_array, '/noise/noiseAzimuthVectorList/noiseAzimuthVector/firstAzimuthLine'),
            'line_stop': (int_array, '/noise/noiseAzimuthVectorList/noiseAzimuthVector/lastAzimuthLine'),
            'sample_start': (int_array, '/noise/noiseAzimuthVectorList/noiseAzimuthVector/firstRangeSample'),
            'sample_stop': (int_array, '/noise/noiseAzimuthVectorList/noiseAzimuthVector/lastRangeSample'),
            'noiseLut': (
                lambda x: [np.fromstring(str(s), dtype=float, sep=' ') for s in x],
                '/noise/noiseAzimuthVectorList/noiseAzimuthVector/noiseAzimuthLut'),
        }
    },
    'annotation': {
        'product_type': (scalar, '/product/adsHeader/productType'),
        'swath_subswath': (scalar, '/product/adsHeader/swath'),
        'line': (uniq_sorted, '/product/geolocationGrid/geolocationGridPointList/geolocationGridPoint/line'),
        'sample': (uniq_sorted, '/product/geolocationGrid/geolocationGridPointList/geolocationGridPoint/pixel'),
        'incidenceAngle': (
            float_array, '/product/geolocationGrid/geolocationGridPointList/geolocationGridPoint/incidenceAngle'),
        'elevationAngle': (
            float_array, '/product/geolocationGrid/geolocationGridPointList/geolocationGridPoint/elevationAngle'),
        'height': (float_array, '/product/geolocationGrid/geolocationGridPointList/geolocationGridPoint/height'),
        'azimuthTime': (
            datetime64_array, '/product/geolocationGrid/geolocationGridPointList/geolocationGridPoint/azimuthTime'),
        'slantRangeTime': (
            float_array, '/product/geolocationGrid/geolocationGridPointList/geolocationGridPoint/slantRangeTime'),
        'longitude': (float_array, '/product/geolocationGrid/geolocationGridPointList/geolocationGridPoint/longitude'),
        'latitude': (float_array, '/product/geolocationGrid/geolocationGridPointList/geolocationGridPoint/latitude'),
        'polarization': (scalar, '/product/adsHeader/polarisation'),
        'line_time_range': (
            datetime64_array, '/product/imageAnnotation/imageInformation/*[contains(name(),"LineUtcTime")]'),
        'line_size': (scalar, '/product/imageAnnotation/imageInformation/numberOfLines'),
        'sample_size': (scalar, '/product/imageAnnotation/imageInformation/numberOfSamples'),
        'incidence_angle_mid_swath': (scalar_float, '/product/imageAnnotation/imageInformation/incidenceAngleMidSwath'),
        'azimuth_time_interval': (scalar_float, '/product/imageAnnotation/imageInformation/azimuthTimeInterval'),
        'slant_range_time_image': (scalar_float, '/product/imageAnnotation/imageInformation/slantRangeTime'),
        'rangePixelSpacing': (scalar_float, '/product/imageAnnotation/imageInformation/rangePixelSpacing'),
        'azimuthPixelSpacing': (scalar_float, '/product/imageAnnotation/imageInformation/azimuthPixelSpacing'),
        'denoised': (scalar, '/product/imageAnnotation/processingInformation/thermalNoiseCorrectionPerformed'),
        'pol': (scalar, '/product/adsHeader/polarisation'),
        'pass': (scalar, '/product/generalAnnotation/productInformation/pass'),
        'platform_heading': (scalar_float, '/product/generalAnnotation/productInformation/platformHeading'),
        'radar_frequency': (scalar_float, '/product/generalAnnotation/productInformation/radarFrequency'),
        'range_sampling_rate': (scalar_float, '/product/generalAnnotation/productInformation/rangeSamplingRate'),
        'azimuth_steering_rate': (scalar_float, '/product/generalAnnotation/productInformation/azimuthSteeringRate'),
        'orbit_time': (datetime64_array, '//product/generalAnnotation/orbitList/orbit/time'),
        'orbit_frame': (np.array, '//product/generalAnnotation/orbitList/orbit/frame'),
        'orbit_pos_x': (float_array, '//product/generalAnnotation/orbitList/orbit/position/x'),
        'orbit_pos_y': (float_array, '//product/generalAnnotation/orbitList/orbit/position/y'),
        'orbit_pos_z': (float_array, '//product/generalAnnotation/orbitList/orbit/position/z'),
        'orbit_vel_x': (float_array, '//product/generalAnnotation/orbitList/orbit/velocity/x'),
        'orbit_vel_y': (float_array, '//product/generalAnnotation/orbitList/orbit/velocity/y'),
        'orbit_vel_z': (float_array, '//product/generalAnnotation/orbitList/orbit/velocity/z'),
        'number_of_bursts': (scalar_int, '/product/swathTiming/burstList/@count'),
        'linesPerBurst': (scalar, '/product/swathTiming/linesPerBurst'),
        'samplesPerBurst': (scalar, '/product/swathTiming/samplesPerBurst'),
        'all_bursts': (np.array, '//product/swathTiming/burstList/burst'),
        'burst_azimuthTime': (datetime64_array, '//product/swathTiming/burstList/burst/azimuthTime'),
        'burst_azimuthAnxTime': (float_array, '//product/swathTiming/burstList/burst/azimuthAnxTime'),
        'burst_sensingTime': (datetime64_array, '//product/swathTiming/burstList/burst/sensingTime'),
        'burst_byteOffset': (np.array, '//product/swathTiming/burstList/burst/byteOffset'),
        'burst_firstValidSample': (
            float_2Darray_from_string_list, '//product/swathTiming/burstList/burst/firstValidSample'),
        'burst_lastValidSample': (
            float_2Darray_from_string_list, '//product/swathTiming/burstList/burst/lastValidSample'),
        'nb_dcestimate': (scalar_int, '/product/dopplerCentroid/dcEstimateList/@count'),
        'nb_geoDcPoly': (
            scalar_int, '/product/dopplerCentroid/dcEstimateList/dcEstimate[1]/geometryDcPolynomial/@count'),
        'nb_dataDcPoly': (scalar_int, '/product/dopplerCentroid/dcEstimateList/dcEstimate[1]/dataDcPolynomial/@count'),
        'nb_fineDce': (scalar_int, '/product/dopplerCentroid/dcEstimateList/dcEstimate[1]/fineDceList/@count'),
        'dc_azimuth_time': (datetime64_array, '//product/dopplerCentroid/dcEstimateList/dcEstimate/azimuthTime'),
        'dc_t0': (np.array, '//product/dopplerCentroid/dcEstimateList/dcEstimate/t0'),
        'dc_geoDcPoly': (
            list_of_float_1D_array_from_string,
            '//product/dopplerCentroid/dcEstimateList/dcEstimate/geometryDcPolynomial'),
        'dc_dataDcPoly': (
            list_of_float_1D_array_from_string, '//product/dopplerCentroid/dcEstimateList/dcEstimate/dataDcPolynomial'),
        'dc_rmserr': (np.array, '//product/dopplerCentroid/dcEstimateList/dcEstimate/dataDcRmsError'),
        'dc_rmserrAboveThres': (
            bool_array, '//product/dopplerCentroid/dcEstimateList/dcEstimate/dataDcRmsErrorAboveThreshold'),
        'dc_azstarttime': (
            datetime64_array, '//product/dopplerCentroid/dcEstimateList/dcEstimate/fineDceAzimuthStartTime'),
        'dc_azstoptime': (
            datetime64_array, '//product/dopplerCentroid/dcEstimateList/dcEstimate/fineDceAzimuthStopTime'),
        'dc_slantRangeTime': (
            float_array, '///product/dopplerCentroid/dcEstimateList/dcEstimate/fineDceList/fineDce/slantRangeTime'),
        'dc_frequency': (
            float_array, '///product/dopplerCentroid/dcEstimateList/dcEstimate/fineDceList/fineDce/frequency'),
        'nb_fmrate': (scalar_int, '/product/generalAnnotation/azimuthFmRateList/@count'),
        'fmrate_azimuthtime': (
            datetime64_array, '//product/generalAnnotation/azimuthFmRateList/azimuthFmRate/azimuthTime'),
        'fmrate_t0': (float_array, '//product/generalAnnotation/azimuthFmRateList/azimuthFmRate/t0'),
        'fmrate_c0': (float_array, '//product/generalAnnotation/azimuthFmRateList/azimuthFmRate/c0'),
        'fmrate_c1': (float_array, '//product/generalAnnotation/azimuthFmRateList/azimuthFmRate/c1'),
        'fmrate_c2': (float_array, '//product/generalAnnotation/azimuthFmRateList/azimuthFmRate/c2'),
        'fmrate_azimuthFmRatePolynomial': (
            list_of_float_1D_array_from_string,
            '//product/generalAnnotation/azimuthFmRateList/azimuthFmRate/azimuthFmRatePolynomial'),

    },
    'xsd': {'all': (str, '/xsd:schema/xsd:complexType/xsd:sequence/xsd:element/xsd:annotation/xsd:documentation'),
            'names': (str, '/xsd:schema/xsd:complexType/xsd:sequence/xsd:element/@name'),
            'sensingtime': (str, '/xsd:schema/xsd:complexType/xsd:sequence/xsd:element/sensingTime')
            }

}


def signal_lut_raw(line, sample, lut_sigma0, lut_gamma0):
    ds = xr.Dataset()
    ds['sigma0_lut'] = xr.DataArray(lut_sigma0, dims=['line', 'sample'], coords={'line': line, 'sample': sample},
                                    name='sigma0', attrs={'description': 'look up table sigma0'})
    ds['gamma0_lut'] = xr.DataArray(lut_gamma0, dims=['line', 'sample'], coords={'line': line, 'sample': sample},
                                    name='gamma0', attrs={'description': 'look up table gamma0'})

    return ds


def noise_lut_range_raw(lines, samples, noiseLuts):
    """

        Parameters
        ----------
        lines: np.ndarray
            1D array of lines. lut is defined at each line
        samples: list of np.ndarray
            arrays of samples. list length is same as samples. each array define samples where lut is defined
        noiseLuts: list of np.ndarray
            arrays of luts. Same structure as samples.

        Returns
        -------
    """

    ds = xr.Dataset()
    # check that all the noiseLuts vector are the same size in range, in old IPF eg <=2017, there was one +/- 1 point over 634
    minimum_pts = 100000
    normalized_noise_luts = []
    normalized_samples = []
    for uu in range(len(noiseLuts)):
        if len(noiseLuts[uu]) < minimum_pts:
            minimum_pts = len(noiseLuts[uu])
    # reduce to the smaller number of points (knowing that it is quite often that last noise value is zero )
    for uu in range(len(noiseLuts)):
        normalized_noise_luts.append(noiseLuts[uu][0:minimum_pts])
        normalized_samples.append(samples[uu][0:minimum_pts])
    tmp_noise = np.stack(normalized_noise_luts)
    ds['noise_lut'] = xr.DataArray(tmp_noise,
                                   coords={'line': lines, 'sample': samples[0][0:minimum_pts]},
                                   dims=['line', 'sample'])
    # ds['sample'] = xr.DataArray(np.stack(normalized_samples), coords={'lines': lines, 'sample_index': np.arange(minimum_pts)},
    #                             dims=['lines', 'sample_index'])

    return ds


def noise_lut_azi_raw_grd(line_azi, line_azi_start, line_azi_stop,
                          sample_azi_start, sample_azi_stop, noise_azi_lut, swath):
    ds = xr.Dataset()
    for ii, swathi in enumerate(swath):  # with 2018 data the noise vector are not the same size -> stacking impossible
        ds['noise_lut_%s' % swathi] = xr.DataArray(noise_azi_lut[ii], coords={'line': line_azi[ii]}, dims=['line'])
    ds['line_start'] = xr.DataArray(line_azi_start, coords={'swath': swath}, dims=['swath'])
    ds['line_stop'] = xr.DataArray(line_azi_stop, coords={'swath': swath}, dims=['swath'])
    ds['sample_start'] = xr.DataArray(sample_azi_start, coords={'swath': swath}, dims=['swath'])
    ds['sample_stop'] = xr.DataArray(sample_azi_stop, coords={'swath': swath}, dims=['swath'])

    return ds


def noise_lut_azi_raw_slc(line_azi, line_azi_start, line_azi_stop,
                          sample_azi_start, sample_azi_stop, noise_azi_lut, swath):
    ds = xr.Dataset()
    # if 'WV' in mode: # there is no noise in azimuth for WV acquisitions
    if swath == []:  # WV SLC case
        ds['noise_lut'] = xr.DataArray(
            1.)  # set noise_azimuth to one to make post steps like noise_azi*noise_range always possible
        ds['line_start'] = xr.DataArray(line_azi_start, attrs={'swath': swath})
        ds['line_stop'] = xr.DataArray(line_azi_stop, attrs={'swath': swath})
        ds['sample_start'] = xr.DataArray(sample_azi_start, attrs={'swath': swath})
        ds['sample_stop'] = xr.DataArray(sample_azi_stop, attrs={'swath': swath})
    else:
        ds['noise_lut'] = xr.DataArray(noise_azi_lut[0], coords={'line': line_azi[0]},
                                       dims=['line'])  # only on subswath opened
        ds['line_start'] = xr.DataArray(line_azi_start[0], attrs={'swath': swath})
        ds['line_stop'] = xr.DataArray(line_azi_stop[0], attrs={'swath': swath})
        ds['sample_start'] = xr.DataArray(sample_azi_start[0], attrs={'swath': swath})
        ds['sample_stop'] = xr.DataArray(sample_azi_stop[0], attrs={'swath': swath})
    # ds['noise_lut'] = xr.DataArray(np.stack(noise_azi_lut).T, coords={'line_index': np.arange(len(line_azi[0])), 'swath': swath},
    #                               dims=['line_index', 'swath'])
    # ds['line'] = xr.DataArray(np.stack(line_azi).T, coords={'line_index': np.arange(len(line_azi[0])), 'swath': swath},
    #                           dims=['line_index', 'swath'])

    return ds


def datetime64_array(dates):
    """list of datetime to np.datetime64 array"""
    return np.array([np.datetime64(d) for d in dates])


def df_files(annotation_files, measurement_files, noise_files, calibration_files):
    # get polarizations and file number from filename
    pols = [os.path.basename(f).split('-')[3].upper() for f in annotation_files]
    num = [int(os.path.splitext(os.path.basename(f))[0].split('-')[8]) for f in annotation_files]
    dsid = [os.path.basename(f).split('-')[1].upper() for f in annotation_files]

    # check that dsid are spatialy uniques (i.e. there is only one dsid per geographic position)
    # some SAFES like WV, dsid are not uniques ('WV1' and 'WV2')
    # we want them uniques, and compatibles with gdal sentinel driver (ie 'WV_012')
    pols_count = len(set(pols))
    subds_count = len(annotation_files) // pols_count
    dsid_count = len(set(dsid))
    if dsid_count != subds_count:
        dsid_rad = dsid[0][:-1]  # WV
        dsid = ["%s_%03d" % (dsid_rad, n) for n in num]
        assert len(set(dsid)) == subds_count  # probably an unknown mode we need to handle

    df = pd.DataFrame(
        {
            'polarization': pols,
            'dsid': dsid,
            'annotation': annotation_files,
            'measurement': measurement_files,
            'noise': noise_files,
            'calibration': calibration_files,
        },
        index=num
    )
    return df


def xsd_files_func(xsd_product_file):
    """
    return a xarray Dataset with path of the different xsd files
    :param xsd_product:
    :return:
    """
    ds = xr.Dataset()

    ds['xsd_product'] = xarray.DataArray(xsd_product_file)
    return ds


def orbit(time, frame, pos_x, pos_y, pos_z, vel_x, vel_y, vel_z, orbit_pass, platform_heading, return_xarray=True):
    """
    Parameters
    ----------
    return_xarray: bool, True-> return a xarray.Dataset, False-> returns a GeoDataFrame
    Returns
    -------
    geopandas.GeoDataFrame
        with 'geometry' as position, 'time' as index, 'velocity' as velocity, and 'geocent' as crs.
    """

    if (frame[0] != 'Earth Fixed') or (np.unique(frame).size != 1):
        raise NotImplementedError('All orbit frames must be of type "Earth Fixed"')
    if return_xarray is False:
        crs = pyproj.crs.CRS(proj='geocent', ellps='WGS84', datum='WGS84')

        res = gpd.GeoDataFrame(
            {
                'velocity': list(map(Point, zip(vel_x, vel_y, vel_z)))
            },
            geometry=list(map(Point, zip(pos_x, pos_y, pos_z))),
            crs=crs,
            index=time
        )
    else:
        res = xr.Dataset()
        res['velocity_x'] = xr.DataArray(vel_x, dims=['time'], coords={'time': time})
        res['velocity_y'] = xr.DataArray(vel_y, dims=['time'], coords={'time': time})
        res['velocity_z'] = xr.DataArray(vel_z, dims=['time'], coords={'time': time})
        res['position_x'] = xr.DataArray(pos_x, dims=['time'], coords={'time': time})
        res['position_y'] = xr.DataArray(pos_y, dims=['time'], coords={'time': time})
        res['position_z'] = xr.DataArray(pos_z, dims=['time'], coords={'time': time})
    res.attrs = {
        'orbit_pass': orbit_pass,
        'platform_heading': platform_heading,
        'frame': frame[0]
    }
    return res


def azimuth_fmrate(azimuthtime, t0, c0, c1, c2, polynomial):
    """
    decode FM rate information from xml annotations
    Parameters
    ----------
    azimuthtime
    t0
    c0
    c1
    c2
    polynomial

    Returns
    -------
    xarray.Dataset
        containing the polynomial coefficient for each of the FM rate along azimuth time coordinates
    """
    if (np.sum([c.size for c in [c0, c1, c2]]) != 0) and (len(polynomial) == 0):
        # old IPF annotation
        polynomial = np.stack([c0, c1, c2], axis=1)
    res = xr.Dataset()
    res['t0'] = xr.DataArray(t0, dims=['azimuthTime'], coords={'azimuthTime': azimuthtime},
                             attrs={'source': xpath_mappings['annotation']['fmrate_t0'][1]})
    res['azimuthFmRatePolynomial'] = xr.DataArray([Polynomial(p) for p in polynomial],
                                                  dims=['azimuthTime'],
                                                  coords={'azimuthTime': azimuthtime},
                                                  attrs={'source': xpath_mappings['annotation'][
                                                      'fmrate_azimuthFmRatePolynomial'][1]})
    return res


def image(product_type, line_time_range, line_size, sample_size, incidence_angle_mid_swath, azimuth_time_interval,
          slant_range_time_image, azimuthPixelSpacing, rangePixelSpacing, swath_subswath, radar_frequency,
          range_sampling_rate, azimuth_steering_rate):
    """
    Decode attribute describing the SAR image
    Parameters
    ----------
    product_type: str
    line_time_range: int
    line_size: int
    sample_size: int
    incidence_angle_mid_swath: float
    azimuth_time_interval: float [ in seconds]
    slant_range_time_image: float [ in seconds]
    azimuthPixelSpacing: int [m]
    rangePixelSpacing: int [m]
    swath_subswath: str
    radar_frequency: float [second-1]
    range_sampling_rate: float
    azimuth_steering_rate: float
    Returns
    -------
    xarray.Dataset
    """
    if product_type == 'SLC':
        pixel_sample_m = rangePixelSpacing / np.sin(np.radians(incidence_angle_mid_swath))
    else:
        pixel_sample_m = rangePixelSpacing
    tmp = {
        'LineUtcTime': (line_time_range, 'line_time_range'),
        'numberOfLines': (line_size, 'line_size'),
        'numberOfSamples': (sample_size, 'sample_size'),
        'azimuthPixelSpacing': (azimuthPixelSpacing, 'azimuthPixelSpacing'),
        'slantRangePixelSpacing': (rangePixelSpacing, 'rangePixelSpacing'),
        'groundRangePixelSpacing': (pixel_sample_m, 'rangePixelSpacing'),
        'incidenceAngleMidSwath': (incidence_angle_mid_swath, 'incidence_angle_mid_swath'),
        'azimuthTimeInterval': (azimuth_time_interval, 'azimuth_time_interval'),
        'slantRangeTime': (slant_range_time_image, 'slant_range_time_image'),
        'swath_subswath': (swath_subswath, 'swath_subswath'),
        'radarFrequency': (radar_frequency, 'radar_frequency'),
        'rangeSamplingRate': (range_sampling_rate, 'range_sampling_rate'),
        'azimuthSteeringRate': (azimuth_steering_rate, 'azimuth_steering_rate'),
    }
    ds = xr.Dataset()
    for ke in tmp:
        ds[ke] = xr.DataArray(tmp[ke][0], attrs={'source': xpath_mappings['annotation'][tmp[ke][1]][1]})
    return ds


def bursts(line_per_burst, sample_per_burst, burst_azimuthTime, burst_azimuthAnxTime, burst_sensingTime,
           burst_byteOffset, burst_firstValidSample, burst_lastValidSample):
    """return burst as an xarray dataset"""
    da = xr.Dataset()
    if (line_per_burst == 0) and (sample_per_burst == 0):
        pass
    else:

        # convert to float, so we can use NaN as missing value, instead of -1
        burst_firstValidSample = burst_firstValidSample.astype(float)
        burst_lastValidSample = burst_lastValidSample.astype(float)
        burst_firstValidSample[burst_firstValidSample == -1] = np.nan
        burst_lastValidSample[burst_lastValidSample == -1] = np.nan
        nbursts = len(burst_azimuthTime)
        # valid_locations = np.empty((nbursts, 4), dtype='int32')
        # for ibur in range(nbursts):
        #     fvs = burst_firstValidSample[ibur, :]
        #     lvs = burst_lastValidSample[ibur, :]
        #     # valind = np.where((fvs != -1) | (lvs != -1))[0]
        #     valind = np.where(np.isfinite(fvs) | np.isfinite(lvs))[0]
        #     valloc = [ibur * line_per_burst + valind.min(), fvs[valind].min(),
        #               ibur * line_per_burst + valind.max(), lvs[valind].max()]
        #     valid_locations[ibur, :] = valloc
        da = xr.Dataset(
            {
                'azimuthTime': ('burst', burst_azimuthTime),
                'azimuthAnxTime': ('burst', burst_azimuthAnxTime),
                'sensingTime': ('burst', burst_sensingTime),
                'byteOffset': ('burst', burst_byteOffset),
                'firstValidSample': (['burst', 'line'], burst_firstValidSample),
                'lastValidSample': (['burst', 'line'], burst_lastValidSample),
                # 'valid_location': xr.DataArray(dims=['burst', 'limits'], data=valid_locations,
                #                                attrs={
                #                                    'description': 'start line index, start sample index, stop line index, stop sample index'}),
            }
        )
        da['azimuthTime'].attrs = {'source': xpath_mappings['annotation']['burst_azimuthTime'][1]}
        da['azimuthAnxTime'].attrs = {'source': xpath_mappings['annotation']['burst_azimuthAnxTime'][1]}
        da['sensingTime'].attrs = {'source': xpath_mappings['annotation']['burst_sensingTime'][1]}
        da['byteOffset'].attrs = {'source': xpath_mappings['annotation']['burst_byteOffset'][1]}
        da['firstValidSample'].attrs = {'source': xpath_mappings['annotation']['burst_firstValidSample'][1]}
        da['lastValidSample'].attrs = {'source': xpath_mappings['annotation']['burst_lastValidSample'][1]}
        # da['valid_location'].attrs = {'source': xpath_mappings['annotation']['burst_firstValidSample'][1]+'\n'+xpath_mappings['annotation']['burst_lastValidSample'][1]}
    da['linesPerBurst'] = xr.DataArray(line_per_burst,
                                       attrs={'source': xpath_mappings['annotation']['linesPerBurst'][1]})
    da['samplesPerBurst'] = xr.DataArray(sample_per_burst,
                                         attrs={'source': xpath_mappings['annotation']['samplesPerBurst'][1]})
    return da


def bursts_grd(line_per_burst, sample_per_burst):
    """return burst as an xarray dataset"""
    da = xr.Dataset({'azimuthTime': ('burst', [])})

    da['linesPerBurst'] = xr.DataArray(line_per_burst)
    da['samplesPerBurst'] = xr.DataArray(sample_per_burst)
    return da


def doppler_centroid_estimates(nb_dcestimate,
                               nb_fineDce, dc_azimuth_time, dc_t0, dc_geoDcPoly,
                               dc_dataDcPoly, dc_rmserr, dc_rmserrAboveThres, dc_azstarttime,
                               dc_azstoptime, dc_slantRangeTime, dc_frequency):
    """
    decoding Doppler Centroid estimates information from xml annotation files
    Parameters
    ----------
    nb_dcestimate
    nb_geoDcPoly
    nb_dataDcPoly
    nb_fineDce
    dc_azimuth_time
    dc_t0
    dc_geoDcPoly
    dc_dataDcPoly
    dc_rmserr
    dc_rmserrAboveThres
    dc_azstarttime
    dc_azstoptime
    dc_slantRangeTime
    dc_frequency

    Returns
    -------

    """
    ds = xr.Dataset()
    ds['t0'] = xr.DataArray(dc_t0.astype(float), dims=['azimuthTime'],
                            attrs={'source': xpath_mappings['annotation']['dc_t0'][1]},
                            coords={'azimuthTime': dc_azimuth_time})
    ds['geometryDcPolynomial'] = xr.DataArray([Polynomial(p) for p in dc_geoDcPoly], dims=['azimuthTime'],
                                              attrs={'source': xpath_mappings['annotation']['dc_geoDcPoly'][1]},
                                              coords={'azimuthTime': dc_azimuth_time})
    ds['dataDcPolynomial'] = xr.DataArray([Polynomial(p) for p in dc_dataDcPoly], dims=['azimuthTime'],
                                          attrs={'source': xpath_mappings['annotation']['dc_dataDcPoly'][1]},
                                          coords={'azimuthTime': dc_azimuth_time})
    dims = (nb_dcestimate, nb_fineDce)

    ds['azimuthTime'].attrs = {'source': xpath_mappings['annotation']['dc_azimuth_time'][1]}
    ds['fineDceAzimuthStartTime'] = xr.DataArray(dc_azstarttime, dims=['azimuthTime'],
                                                 attrs={'source': xpath_mappings['annotation']['dc_azstarttime'][1]},
                                                 coords={'azimuthTime': dc_azimuth_time})
    ds['fineDceAzimuthStopTime'] = xr.DataArray(dc_azstoptime, dims=['azimuthTime'],
                                                attrs={'source': xpath_mappings['annotation']['dc_azstoptime'][1]},
                                                coords={'azimuthTime': dc_azimuth_time})
    ds['dataDcRmsError'] = xr.DataArray(dc_rmserr.astype(float), dims=['azimuthTime'],
                                        attrs={'source': xpath_mappings['annotation']['dc_rmserr'][1]},
                                        coords={'azimuthTime': dc_azimuth_time})
    ds['slantRangeTime'] = xr.DataArray(dc_slantRangeTime.reshape(dims), dims=['azimuthTime', 'nb_fine_dce'],
                                        attrs={'source': xpath_mappings['annotation']['dc_slantRangeTime'][1]},
                                        coords={'azimuthTime': dc_azimuth_time, 'nb_fine_dce': np.arange(nb_fineDce)})
    ds['frequency'] = xr.DataArray(dc_frequency.reshape(dims), dims=['azimuthTime', 'nb_fine_dce'],
                                   attrs={'source': xpath_mappings['annotation']['dc_frequency'][1]},
                                   coords={'azimuthTime': dc_azimuth_time, 'nb_fine_dce': np.arange(nb_fineDce)})
    ds['dataDcRmsErrorAboveThreshold'] = xr.DataArray(dc_rmserrAboveThres, dims=['azimuthTime'],
                                                      attrs={
                                                          'source': xpath_mappings['annotation']['dc_rmserrAboveThres'][
                                                              1]},
                                                      coords={'azimuthTime': dc_azimuth_time})
    return ds


def geolocation_grid(line, sample, values):
    """

    Parameters
    ----------
    line: np.ndarray
        1D array of line dimension
    sample: np.ndarray

    Returns
    -------
    xarray.DataArray
        with line and sample coordinates, and values as 2D

    """
    shape = (line.size, sample.size)
    values = np.reshape(values, shape)
    return xr.DataArray(values, dims=['line', 'sample'], coords={'line': line, 'sample': sample})


# dict of compounds variables.
# compounds variables are variables composed of several variables.
# the key is the variable name, and the value is a python structure,
# where leaves are jmespath in xpath_mappings
compounds_vars = {
    'safe_attributes_slcgrd': {
        'ipf_version': 'manifest.ipf_version',
        'swath_type': 'manifest.swath_type',
        'polarizations': 'manifest.polarizations',
        'product_type': 'manifest.product_type',
        'mission': 'manifest.mission',
        'satellite': 'manifest.satellite',
        'start_date': 'manifest.start_date',
        'stop_date': 'manifest.stop_date',
        'footprints': 'manifest.footprints',
        'aux_cal': 'manifest.aux_cal'
    },
    'safe_attributes_sl2': {
        'ipf_version': 'manifest.ipf_version',
        'swath_type': 'manifest.swath_type',
        'polarizations': 'manifest.polarizations',
        'product_type': 'manifest.product_type',
        'mission': 'manifest.mission',
        'satellite': 'manifest.satellite',
        'start_date': 'manifest.start_date',
        'stop_date': 'manifest.stop_date',
        'footprints': 'manifest.footprints',
        'aux_cal_sl2': 'manifest.aux_cal_sl2'
    },
    'files': {
        'func': df_files,
        'args': (
            'manifest.annotation_files', 'manifest.measurement_files', 'manifest.noise_files',
            'manifest.calibration_files')
    },
    'xsd_files': {
        'func': xsd_files_func,
        'args': (
            'manifest.xsd_product_file',
        )
    },
    'luts_raw': {
        'func': signal_lut_raw,
        'args': ('calibration.line', 'calibration.sample', 'calibration.sigma0_lut', 'calibration.gamma0_lut')
    },
    'noise_lut_range_raw': {
        'func': noise_lut_range_raw,
        'args': ('noise.range.line', 'noise.range.sample', 'noise.range.noiseLut')
    },
    'noise_lut_azi_raw_grd': {
        'func': noise_lut_azi_raw_grd,
        'args': (
            'noise.azi.line', 'noise.azi.line_start', 'noise.azi.line_stop',
            'noise.azi.sample_start',
            'noise.azi.sample_stop', 'noise.azi.noiseLut',
            'noise.azi.swath')
    },
    'noise_lut_azi_raw_slc': {
        'func': noise_lut_azi_raw_slc,
        'args': (
            'noise.azi.line', 'noise.azi.line_start', 'noise.azi.line_stop',
            'noise.azi.sample_start',
            'noise.azi.sample_stop', 'noise.azi.noiseLut',
            'noise.azi.swath')
    },
    'denoised': ('annotation.pol', 'annotation.denoised'),
    'incidenceAngle': {
        'func': geolocation_grid,
        'args': ('annotation.line', 'annotation.sample', 'annotation.incidenceAngle')
    },
    'elevationAngle': {
        'func': geolocation_grid,
        'args': ('annotation.line', 'annotation.sample', 'annotation.elevationAngle')
    },
    'longitude': {
        'func': geolocation_grid,
        'args': ('annotation.line', 'annotation.sample', 'annotation.longitude')
    },
    'latitude': {
        'func': geolocation_grid,
        'args': ('annotation.line', 'annotation.sample', 'annotation.latitude')
    },
    'height': {
        'func': geolocation_grid,
        'args': ('annotation.line', 'annotation.sample', 'annotation.height')
    },
    'azimuthTime': {
        'func': geolocation_grid,
        'args': ('annotation.line', 'annotation.sample', 'annotation.azimuthTime')
    },
    'slantRangeTime': {
        'func': geolocation_grid,
        'args': ('annotation.line', 'annotation.sample', 'annotation.slantRangeTime')
    },
    'bursts': {
        'func': bursts,
        'args': ('annotation.linesPerBurst', 'annotation.samplesPerBurst', 'annotation.burst_azimuthTime',
                 'annotation.burst_azimuthAnxTime', 'annotation.burst_sensingTime', 'annotation.burst_byteOffset',
                 'annotation.burst_firstValidSample', 'annotation.burst_lastValidSample')
    },
    'bursts_grd': {
        'func': bursts_grd,
        'args': ('annotation.linesPerBurst', 'annotation.samplesPerBurst',)
    },

    'orbit': {
        'func': orbit,
        'args': ('annotation.orbit_time', 'annotation.orbit_frame',
                 'annotation.orbit_pos_x', 'annotation.orbit_pos_y', 'annotation.orbit_pos_z',
                 'annotation.orbit_vel_x', 'annotation.orbit_vel_y', 'annotation.orbit_vel_z',
                 'annotation.pass', 'annotation.platform_heading')
    },
    'image': {
        'func': image,
        'args': (
            'annotation.product_type', 'annotation.line_time_range', 'annotation.line_size', 'annotation.sample_size',
            'annotation.incidence_angle_mid_swath', 'annotation.azimuth_time_interval',
            'annotation.slant_range_time_image', 'annotation.azimuthPixelSpacing', 'annotation.rangePixelSpacing',
            'annotation.swath_subswath', 'annotation.radar_frequency', 'annotation.range_sampling_rate',
            'annotation.azimuth_steering_rate')
    },
    'azimuth_fmrate': {
        'func': azimuth_fmrate,
        'args': (
            'annotation.fmrate_azimuthtime', 'annotation.fmrate_t0',
            'annotation.fmrate_c0', 'annotation.fmrate_c1', 'annotation.fmrate_c2',
            'annotation.fmrate_azimuthFmRatePolynomial')
    },
    'doppler_estimate': {
        'func': doppler_centroid_estimates,
        'args': ('annotation.nb_dcestimate',
                 'annotation.nb_fineDce', 'annotation.dc_azimuth_time', 'annotation.dc_t0', 'annotation.dc_geoDcPoly',
                 'annotation.dc_dataDcPoly', 'annotation.dc_rmserr', 'annotation.dc_rmserrAboveThres',
                 'annotation.dc_azstarttime',
                 'annotation.dc_azstoptime', 'annotation.dc_slantRangeTime', 'annotation.dc_frequency'

                 ),
    },
}
