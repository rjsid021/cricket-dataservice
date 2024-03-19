import sys

import numpy as np
import pandas as pd

from DataIngestion.config import PLAYER_MAPPER_TABLE_NAME
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao.insert_data import insertToDB
from common.db_config import DB_NAME

sys.path.append("./../../")
sys.path.append("./")

from DataIngestion.utils.helper import readCSV
from common.dao_client import session

if __name__ == "__main__":
    sm_df = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/DataIngestion/player_mapper_parser/trio_ingestion/3__non_null_values.csv"
    )

    xm_df = sm_df[['identifier', 'cricinfo_id']]

    finalPlayerMapping = getPandasFactoryDF(
        session,
        f"select * from {PLAYER_MAPPER_TABLE_NAME}"
    )

    merged__df = pd.merge(
        xm_df[['cricinfo_id', 'identifier']],
        finalPlayerMapping.drop(['cricsheet_id'], axis=1),
        how='outer',
        left_on='cricinfo_id',
        right_on='cricinfo_id',
        indicator=True
    ).rename(columns={
            'identifier': 'cricsheet_id'
        })
    merged__df = merged__df[merged__df['_merge'] == "both"].drop('_merge', axis=1)
    merged__df['cricinfo_id'] = merged__df['cricinfo_id'].astype(int)

    from datetime import datetime
    merged__df['id'] = merged__df['id'].astype(int)
    merged__df['is_wicket_keeper'] = merged__df['is_wicket_keeper'].astype(int)
    merged__df['is_bowler'] = merged__df['is_bowler'].astype(int)
    merged__df['is_batsman'] = merged__df['is_batsman'].astype(int)

    merged__df['load_timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    merged__df['full_name'] = merged__df['full_name'].apply(lambda x: str(x).replace("'", ""))
    merged__df['short_name'] = merged__df['short_name'].apply(lambda x: str(x).replace("'", ""))
    merged__df['name'] = merged__df['name'].apply(lambda x: str(x).replace("'", ""))
    xxx = merged__df.to_dict(orient="records")
    insertToDB(session, xxx, DB_NAME, PLAYER_MAPPER_TABLE_NAME)
