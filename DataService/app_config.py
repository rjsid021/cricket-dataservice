import os

from DataIngestion.config import FILE_SHARE_PATH
from common.utils.helper import get_project_root, getEnvVariables

# App Configuration
HOST = getEnvVariables("APP_HOST")
PORT = getEnvVariables("APP_PORT")
ALLOWED_HOSTS = getEnvVariables("ALLOWED_HOSTS").split(",")
HOME_DIR = get_project_root()
TOP_TEAMS_FILE_PATH = os.path.join(FILE_SHARE_PATH, "data/pre_selected_players.csv")
MAPPING_FILE_PATH = os.path.join(FILE_SHARE_PATH, "data/mapping_new.csv")
MLC_SQUAD_PATH = os.path.join(FILE_SHARE_PATH, "data/MLC-2023.csv")
IPL_RETAINED_LIST_PATH = os.path.join(FILE_SHARE_PATH, "data/retained-player-list-2024.csv")
IPL_AUCTION_LIST_PATH = os.path.join(FILE_SHARE_PATH, "data/ipl-auction-list-2024.csv")
OPEN_API_SPEC_FILE = os.path.join(HOME_DIR, "openapi_spec/app_open_api_spec.yaml")
SMARTABASE_PLAYER_MAPPING = os.path.join(FILE_SHARE_PATH, "data/competition_group_mapping.json")
