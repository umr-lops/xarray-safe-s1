import pdb
from safe_s1 import Sentinel1Reader, getconfig
import time
conf = getconfig.get_config()
subswath = conf['nfs_iw_grd_path']
print(subswath)
t0 = time.time()
sub_reader = Sentinel1Reader(subswath)
elapse_t = time.time()-t0

dt = sub_reader.datatree
print('out of the reader')
print(dt)
print('time to read the SAFE through nfs: %1.2f sec'%elapse_t)
pdb.set_trace()
