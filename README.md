
[![Documentation Status](https://readthedocs.org/projects/xarray-safe-s1/badge/?version=latest)](https://xarray-safe-s1.readthedocs.io/en/latest/?badge=latest)

# xarray-safe-s1

Xarray Sentinel1 python SAFE files reader

 

# Install

## Conda

1) Install xarray-safe-s1

```
conda create -n safe_s1
conda activate safe_s1
conda install -c conda-forge xarray-safe-s1
```


## Pypi

1) Install xarray-safe-s1

```
conda create -n safe_s1
conda activate safe_s1
pip install xarray-safe-s1
```


```pycon
>>> from safe_s1 import Sentinel1Reader, sentinel1_xml_mappings
>>> filename = sentinel1_xml_mappings.get_test_file('S1A_IW_GRDH_1SDV_20170907T103020_20170907T103045_018268_01EB76_Z010.SAFE')
>>> Sentinel1Reader(filename).datatree

DataTree('None', parent=None)
├── DataTree('geolocationGrid')
│       Dimensions:         (line: 10, sample: 21)
│       Coordinates:
│         * line            (line) int64 0 2014 4028 6042 ... 12084 14098 16112 16777
│         * sample          (sample) int64 0 1260 2520 3780 ... 21420 22680 23940 25186
│       Data variables:
│           longitude       (line, sample) float64 -67.84 -67.96 -68.08 ... -70.4 -70.51
│           latitude        (line, sample) float64 20.73 20.75 20.77 ... 19.62 19.64
│           height          (line, sample) float64 8.405e-05 8.058e-05 ... 3.478e-05
│           azimuthTime     (line, sample) datetime64[ns] 2017-09-07T10:30:20.936147 ...
│           slantRangeTime  (line, sample) float64 0.005331 0.005375 ... 0.006382
│           incidenceAngle  (line, sample) float64 30.82 31.7 32.57 ... 44.71 45.36 46.0
│           elevationAngle  (line, sample) float64 27.5 28.27 29.02 ... 39.89 40.41
│       Attributes:
│           history:  longitude:\n  annotation/s1a.xml:\n  - /product/geolocationGrid...
├── DataTree('orbit')
│       Dimensions:     (time: 17)
│       Coordinates:
│         * time        (time) datetime64[ns] 2017-09-07T10:29:14.474905 ... 2017-09-...
│       Data variables:
│           velocity_x  (time) float64 -116.7 -154.1 -191.4 ... -628.1 -663.4 -698.6
│           velocity_y  (time) float64 -3.433e+03 -3.368e+03 ... -2.413e+03 -2.342e+03
│           velocity_z  (time) float64 -6.776e+03 -6.808e+03 ... -7.174e+03 -7.194e+03
│           position_x  (time) float64 2.892e+06 2.89e+06 ... 2.833e+06 2.826e+06
│           position_y  (time) float64 -5.782e+06 -5.816e+06 ... -6.222e+06 -6.246e+06
│           position_z  (time) float64 2.869e+06 2.801e+06 ... 1.82e+06 1.748e+06
│       Attributes:
│           orbit_pass:        Descending
│           platform_heading:  -167.7668824808032
│           frame:             Earth Fixed
│           history:           orbit:\n  annotation/s1a.xml:\n  - //product/generalAn...
├── DataTree('image')
│       Dimensions:                  (dim_0: 2)
│       Dimensions without coordinates: dim_0
│       Data variables: (12/14)
│           LineUtcTime              (dim_0) datetime64[ns] 2017-09-07T10:30:20.93640...
│           numberOfLines            int64 16778
│           numberOfSamples          int64 25187
│           azimuthPixelSpacing      float64 10.0
│           slantRangePixelSpacing   float64 10.0
│           groundRangePixelSpacing  float64 10.0
│           ...                       ...
│           slantRangeTime           float64 0.005331
│           swath_subswath           <U2 'IW'
│           radarFrequency           float64 5.405e+09
│           rangeSamplingRate        float64 6.435e+07
│           azimuthSteeringRate      float64 1.59
│           history                  <U824 'image:\n  annotation/s1a.xml:\n  - /produ...
├── DataTree('azimuth_fmrate')
│       Dimensions:                  (azimuthTime: 11)
│       Coordinates:
│         * azimuthTime              (azimuthTime) datetime64[ns] 2017-09-07T10:30:18...
│       Data variables:
│           t0                       (azimuthTime) float64 0.005331 ... 0.005331
│           azimuthFmRatePolynomial  (azimuthTime) object -2337.303489601216 + 448953...
│       Attributes:
│           history:  azimuth_fmrate:\n  annotation/s1a.xml:\n  - //product/generalAn...
├── DataTree('doppler_estimate')
│       Dimensions:                       (azimuthTime: 27, nb_fine_dce: 20)
│       Coordinates:
│         * azimuthTime                   (azimuthTime) datetime64[ns] 2017-09-07T10:...
│         * nb_fine_dce                   (nb_fine_dce) int64 0 1 2 3 4 ... 16 17 18 19
│       Data variables:
│           t0                            (azimuthTime) float64 0.005332 ... 0.005331
│           geometryDcPolynomial          (azimuthTime) object -1.070619 - 183.4602·x...
│           dataDcPolynomial              (azimuthTime) object -74.90705 + 85274.16·x...
│           fineDceAzimuthStartTime       (azimuthTime) datetime64[ns] 2017-09-07T10:...
│           fineDceAzimuthStopTime        (azimuthTime) datetime64[ns] 2017-09-07T10:...
│           dataDcRmsError                (azimuthTime) float64 6.181 6.181 ... 7.745
│           slantRangeTime                (azimuthTime, nb_fine_dce) float64 0.006021...
│           frequency                     (azimuthTime, nb_fine_dce) float64 -28.36 ....
│           dataDcRmsErrorAboveThreshold  (azimuthTime) bool False False ... False False
│       Attributes:
│           history:  doppler_estimate:\n  annotation/s1a.xml:\n  - /product/dopplerC...
├── DataTree('bursts')
│       Dimensions:          (burst: 0)
│       Dimensions without coordinates: burst
│       Data variables:
│           azimuthTime      (burst) float64 
│           linesPerBurst    int64 0
│           samplesPerBurst  int64 0
│       Attributes:
│           history:  bursts_grd:\n  annotation/s1a.xml:\n  - /product/swathTiming/li...
├── DataTree('calibration_luts')
│       Dimensions:     (line: 27, sample: 631, pol: 2)
│       Coordinates:
│         * line        (line) int64 0 671 1342 2013 2684 ... 15436 16107 16778 17449
│         * sample      (sample) int64 0 40 80 120 160 ... 25040 25080 25120 25160 25186
│         * pol         (pol) object 'VV' 'VH'
│       Data variables:
│           sigma0_lut  (pol, line, sample) float64 662.0 661.7 661.5 ... 558.9 558.8
│           gamma0_lut  (pol, line, sample) float64 613.4 613.1 612.7 ... 465.8 465.7
├── DataTree('noise_azimuth_raw')
│       Dimensions:       (swath: 0, pol: 2)
│       Coordinates:
│         * swath         (swath) float64 
│         * pol           (pol) object 'VV' 'VH'
│       Data variables:
│           line_start    (pol, swath) int64 
│           line_stop     (pol, swath) int64 
│           sample_start  (pol, swath) int64 
│           sample_stop   (pol, swath) int64 
│       Attributes:
│           history:  noise_lut_azi_raw_grd:\n  annotation/calibration/noise.xml:\n  ...
└── DataTree('noise_range_raw')
        Dimensions:    (line: 27, sample: 634, pol: 2)
        Coordinates:
          * line       (line) int64 0 671 1342 2013 2684 ... 15433 16104 16775 16777
          * sample     (sample) int64 0 40 80 120 160 ... 25028 25068 25108 25148 25186
          * pol        (pol) object 'VV' 'VH'
        Data variables:
            noise_lut  (pol, line, sample) float64 1.641e+03 1.62e+03 ... 0.0 0.0

```

Example of usage for S3: access

```pycon
url = 's3:///eodata/..../.SAFE'
storage_options = {"anon": False, "client_kwargs": {"endpoint_url": 'https://'+entrypoint_url, 'aws_access_key_id':access_key,
    'aws_secret_access_key':secret_key}}
reader = Sentinel1Reader(url,backend_kwargs={"storage_options": storage_options})
```
