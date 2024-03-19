from ast import literal_eval

import pandas as pd

from DataIngestion.utils.helper import readCSV
from boot_script.utils import PLAYER_MAPPING_PATH
from common.dao.insert_data import insertToDB
from common.dao_client import session
from common.db_config import DB_NAME


if __name__ == "__main__":
    players_df = readCSV("/Users/achintya.chaudhary/Documents/projects/CricketDataService/boot_script/player_mapping_download.csv")
    players_df["cricinfo_id"] = players_df["cricinfo_id"].astype(int)
    players_df["smartabase_id"] = players_df["smartabase_id"].fillna(-1).astype(int)
    players_df["catapult_id"] = players_df['catapult_id'].apply(lambda x: list(literal_eval(x)) if pd.notna(x) else [])
    players_data = players_df.fillna("").to_dict(orient="records")
    insertToDB(session, players_data, DB_NAME, "playermapping", allow_logging=True)