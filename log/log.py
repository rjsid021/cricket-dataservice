import logging
import logging.handlers
import os
import sys
import yaml
import logging.config

sys.path.append("./../../")
sys.path.append("./")
from common.utils.helper import get_project_root

# root_dir = sys.path[1]
root_dir = get_project_root()
logs_data_dir = os.path.join(root_dir, "log/log_data")
logs_data_api_dir = os.path.join(logs_data_dir, "api")
config_dir = os.path.join(root_dir, "log/")
config_path = os.path.abspath(config_dir) + "/" + 'log_config.yaml'


# setting up the logger configs from log_config.yaml file
def set_logger(api_name, default_level=logging.INFO):
    api_dir = os.path.join(logs_data_api_dir, api_name)
    if not os.path.exists(api_dir):
        os.makedirs(api_dir, exist_ok=True)
    os.chmod(api_dir, 0o777)

    fname = os.path.join(api_dir, "service.log")

    WEBAPP_CONSTANTS = {
        'LOGFILE': fname
    }

    LOGFILE = WEBAPP_CONSTANTS.get('LOGFILE', False)

    if os.path.exists(config_path):
        with open(config_path, 'rt') as f:
            try:
                config = yaml.safe_load(f.read())
                config['handlers']['file_handler']['filename'] = LOGFILE
                logging.config.dictConfig(config)
            except Exception as e:
                print(e)
                print('Error in Logging Configuration. Using default configs')
                logging.basicConfig(level=default_level)
    else:
        logging.basicConfig(level=default_level)
        print('Failed to load configuration file. Using default configs')


# function to get the logger
def get_logger(name, api_name):
    set_logger(api_name, default_level=logging.INFO)
    logger = logging.getLogger(name)
    return logger
