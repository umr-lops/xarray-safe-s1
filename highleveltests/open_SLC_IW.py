from safe_s1 import Sentinel1Reader
import pdb
import os
tiff='/home/antoine/Documents/data/sentinel1/S1A_IW_SLC__1SDV_20220507T162437_20220507T162504_043107_0525DE_B14E.SAFE/measurement/s1a-iw1-slc-vv-20220507t162439-20220507t162504-043107-0525de-004.tiff'
reader = Sentinel1Reader(os.path.dirname(os.path.dirname(tiff)))
#sub_reader = Sentinel1Reader(tiff)
sub_reader = Sentinel1Reader(reader.datasets_names[0])
dt = sub_reader.datatree
print('out of the reader')
print(dt)
pdb.set_trace()
