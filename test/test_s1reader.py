import safe_s1
from safe_s1 import sentinel1_xml_mappings, Sentinel1Reader
import os
import logging
from pathlib import Path
import yaml

logging.basicConfig()
logging.captureWarnings(True)

logger = logging.getLogger('s1_reader_test')
logger.setLevel(logging.DEBUG)


# determine the config file we will use (config.yml by default, and a local config if one is present) and retrieve
# the products names
local_config_pontential_path = Path(os.path.join('~', 'xarray-safe-s1', 'localconfig.yml')).expanduser()
if local_config_pontential_path.exists():
    config_path = local_config_pontential_path
    with open(config_path) as config_content:
        products = yaml.load(config_content, Loader=yaml.SafeLoader)['product_paths']
else:
    config_path = Path(os.path.join(os.path.dirname(safe_s1.__file__), 'config.yml'))
    with open(config_path) as config_content:
        raw_products = yaml.load(config_content, Loader=yaml.SafeLoader)['product_paths']
    products = [sentinel1_xml_mappings.get_test_file(filename) for filename in raw_products]


# Try to apply the reader on different products
def test_reader():
    try:
        for product in products:
            reader = Sentinel1Reader(product)
            # When a product is a multidataset, datatree is none, so we want to be sure that the datatree isn't empty
            # (selecting a dataset)
            sub_reader = Sentinel1Reader(reader.datasets_names[0])
            dt = sub_reader.datatree
            for ds in dt:
                dt[ds].to_dataset().compute()
        assert True
    except:
        assert False






