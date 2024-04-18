# see https://stackoverflow.com/questions/69624867/no-such-file-error-when-trying-to-create-local-cache-of-s3-object
from safe_s1 import Sentinel1Reader,getconfig
import pdb
import os
import time
import logging
import fsspec
logging.basicConfig(level=logging.INFO)
logging.info('test start')
conf = getconfig.get_config()
access_key = conf['access_key']
secret_key = conf['secret_key']
entrypoint_url = conf['entrypoint_url']
s3 = fsspec.filesystem("s3", anon=False,
      key=access_key,
      secret=secret_key,
      endpoint_url='https://'+entrypoint_url)

# this syntaxe works we can get content xml files but I would have to precise which subswath I want to decode in case of SLC
# safe2 = 's3:///eodata/Sentinel-1/SAR/SLC/2019/10/13/S1B_IW_SLC__1SDV_20191013T155948_20191013T160015_018459_022C6B_13A2.SAFE'
safe2 = 's3:///eodata/Sentinel-1/SAR/IW_GRDH_1S/2024/04/18/S1A_IW_GRDH_1SSH_20240418T080141_20240418T080210_053485_067D74_C073.SAFE'
# safe2 = conf['s3_iw_grd_path']
option = 'kwargs'
if option == 'kwargs':
    storage_options = {"anon": False, "client_kwargs": {"endpoint_url": 'https://'+entrypoint_url, 'aws_access_key_id':access_key,
    'aws_secret_access_key':secret_key}}
    t0 = time.time()
    sub_reader = Sentinel1Reader(safe2,backend_kwargs={"storage_options": storage_options})
    elapse_t = time.time()-t0
    print('time to read the SAFE through S3: %1.2f sec'%elapse_t)
else:
    # this solution is not supported.
    sub_reader = Sentinel1Reader(s3.get_mapper(safe2)) # botocore.errorfactory.NoSuchKey: An error occurred (NoSuchKey) when calling the GetObject operation: Unknown
dt = sub_reader.datatree
print('out of the reader')
print(dt)
