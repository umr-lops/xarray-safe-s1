from safe_s1 import Sentinel1Reader
import pdb
import time
import os
tiff='/home/antoine/Documents/data/sentinel1/S1A_IW_SLC__1SDV_20220507T162437_20220507T162504_043107_0525DE_B14E.SAFE/measurement/s1a-iw1-slc-vv-20220507t162439-20220507t162504-043107-0525de-004.tiff'
tiff='/home/datawork-cersat-public/cache/project/mpc-sentinel1/data/esa/sentinel-1a/L1/IW/S1A_IW_SLC__1S/2022/128/S1A_IW_SLC__1SDV_20220508T181331_20220508T181358_043123_052660_B2F8.SAFE/measurement/s1a-iw1-slc-vv-20220508t181332-20220508t181357-043123-052660-004.tiff'
subswath = 'SENTINEL1_DS:/home/datawork-cersat-public/cache/project/mpc-sentinel1/data/esa/sentinel-1a/L1/IW/S1A_IW_SLC__1S/2022/128/S1A_IW_SLC__1SDV_20220508T181331_20220508T181358_043123_052660_B2F8.SAFE:IW1'
#reader = Sentinel1Reader(os.path.dirname(os.path.dirname(tiff)))
#sub_reader = Sentinel1Reader(tiff)

print(subswath)
#sub_reader = Sentinel1Reader(reader.datasets_names[0])
t0 = time.time()
sub_reader = Sentinel1Reader(subswath)
elapse_t = time.time()-t0

dt = sub_reader.datatree
print('out of the reader')
print(dt)
print('time to read the SAFE through S3: %1.2f sec'%elapse_t)
