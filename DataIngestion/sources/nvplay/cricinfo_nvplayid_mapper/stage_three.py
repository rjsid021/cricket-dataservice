# This file just add name to stage two.csv file
# stored in player mapping table

import pandas as pd

from DataIngestion.utils.helper import readCSV
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session
from common.db_config import DB_NAME

if __name__ == "__main__":
    source_df = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/DataIngestion/sources/nvplay/cricinfo_nvplayid_mapper/stage_two.csv"
    )

    player_mapping_df = getPandasFactoryDF(
        session,
        f"select * from {DB_NAME}.playermapping;"
    )
    all_3_common = pd.merge(
        source_df,
        player_mapping_df[['cricinfo_id', 'name']],
        on='cricinfo_id',
        how='left'
    )
    all_3_common[['nvplay_name', 'name', 'cricinfo_id']].to_csv("stage_three.csv")
