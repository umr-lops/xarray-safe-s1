from safe_s1 import Sentinel1Reader
import pdb
import os

import boto3

session = boto3.session.Session()
s3 = boto3.resource(
    's3',
    endpoint_url='https://eodata.dataspace.copernicus.eu',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name='default'
)  # generated secrets

tiff='/home/antoine/Documents/data/sentinel1/S1A_IW_SLC__1SDV_20220507T162437_20220507T162504_043107_0525DE_B14E.SAFE/measurement/s1a-iw1-slc-vv-20220507t162439-20220507t162504-043107-0525de-004.tiff'
safe = 's3://eodata/Sentinel-1/SAR/SLC/2019/10/13/S1B_IW_SLC__1SDV_20191013T155948_20191013T160015_018459_022C6B_13A2.SAFE/:IW1'
reader = Sentinel1Reader(os.path.dirname(os.path.dirname(tiff)))
#sub_reader = Sentinel1Reader(tiff)
sub_reader = Sentinel1Reader(safe)
dt = sub_reader.datatree
print('out of the reader')
print(dt)
pdb.set_trace()
