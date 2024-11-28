import logging
import os
from pathlib import Path

import yaml

import safe_s1


# determine the config file we will use (config.yml by default, and a local config if one is present) and retrieve
# the products names
def get_config():
    local_config_pontential_path = os.path.join(
        os.path.dirname(safe_s1.__file__), "localconfig.yml"
    )
    logging.info("potential local config: %s", local_config_pontential_path)
    # local_config_pontential_path = Path(os.path.join('~', 'xarray-safe-s1', 'localconfig.yml')).expanduser()
    if os.path.exists(local_config_pontential_path):
        logging.info("localconfig used")
        config_path = local_config_pontential_path
        with open(config_path) as config_content:
            conf = yaml.load(config_content, Loader=yaml.SafeLoader)
    else:
        logging.info("default config")
        config_path = Path(
            os.path.join(os.path.dirname(safe_s1.__file__), "config.yml")
        )
        with open(config_path) as config_content:
            conf = yaml.load(config_content, Loader=yaml.SafeLoader)
    return conf
