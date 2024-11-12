import pdb
from safe_s1 import Sentinel1Reader, getconfig
import time
import logging
logging.basicConfig(level=logging.DEBUG)
logging.debug('start GRD test')
conf = getconfig.get_config()
subswath = conf['product_paths'][0]
print(subswath)
t0 = time.time()
if 'GRD' in subswath:
    sub_reader = Sentinel1Reader(subswath)
else:
    sub_reader = Sentinel1Reader('SENTINEL1_DS:'+subswath+':IW3')
elapse_t = time.time()-t0

dt = sub_reader.datatree
print('out of the reader')
print(dt)
print('time to read the SAFE through nfs: %1.2f sec'%elapse_t)
DN = sub_reader.load_digital_number(chunks={'pol':'VV','line':6000,'sample':8000})
print('DN',DN)
# pdb.set_trace()
