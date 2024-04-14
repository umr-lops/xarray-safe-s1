# see https://stackoverflow.com/questions/69624867/no-such-file-error-when-trying-to-create-local-cache-of-s3-object
from safe_s1 import Sentinel1Reader,getconfig
import pdb
import os
import logging
import boto3
import fsspec
logging.basicConfig(level=logging.INFO)
logging.info('test start')
conf = getconfig.get_config()
access_key = conf['access_key']
secret_key = conf['secret_key']
entrypoint_url = conf['entrypoint_url']
session = boto3.session.Session()
#s3 = boto3.resource(
#    's3',
#    endpoint_url='https://eodata.dataspace.copernicus.eu',
#    aws_access_key_id=access_key,
#    aws_secret_access_key=secret_key,
#    region_name='default'
#)  # generated secrets
s3 = fsspec.filesystem("s3", anon=False,
      key=access_key,
      secret=secret_key,
      endpoint_url='https://'+entrypoint_url)
#bu = s3.Bucket("eodata")
safe = 's3://eodata/Sentinel-1/SAR/SLC/2019/10/13/S1B_IW_SLC__1SDV_20191013T155948_20191013T160015_018459_022C6B_13A2.SAFE/:IW1'
logging.info('safe = %s',safe)
tiff='/home/antoine/Documents/data/sentinel1/S1A_IW_SLC__1SDV_20220507T162437_20220507T162504_043107_0525DE_B14E.SAFE/measurement/s1a-iw1-slc-vv-20220507t162439-20220507t162504-043107-0525de-004.tiff'
#reader = Sentinel1Reader(os.path.dirname(os.path.dirname(tiff)))
#sub_reader = Sentinel1Reader(tiff)
pattern="Sentinel-1/SAR/SLC/2019/10/13/S1B_IW_SLC__1SDV_20191013T155948_20191013T160015_018459_022C6B_13A2.SAFE"
pattern="Sentinel-1/SAR/SLC/2019/10/13/S1B_IW_SLC__1SDV_20191013T155*.SAFE"
#files = bu.objects.filter(Prefix=pattern) # to test like in the example of creodias documentation
#lol = [filee.key for filee in files if os.path.splitext(filee.key)[1]=='.SAFE']
safe2='SENTINEL1_DS:/eodata/Sentinel-1/SAR/SLC/2019/10/13/S1B_IW_SLC__1SDV_20191013T155948_20191013T160015_018459_022C6B_13A2.SAFE:IW1'
sub_reader = Sentinel1Reader(s3.get_mapper(safe2))
#sub_reader = Sentinel1Reader(safe)
dt = sub_reader.datatree
print('out of the reader')
print(dt)
