import sys

from DataIngestion.config import PLAYER_MAPPER_TABLE_NAME

sys.path.append("./../../")
sys.path.append("./")

import pandas as pd

from common.dao.fetch_db_data import getPandasFactoryDF

sys.path.append("./../../")
sys.path.append("./")
from common.dao.insert_data import insertToDB
from common.db_config import DB_NAME

sys.path.append("./../../")
sys.path.append("./")

from DataIngestion.utils.helper import readCSV
from common.dao_client import session

if __name__ == "__main__":
    sm_df = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/DataIngestion/player_mapper_parser/leftover_sm/unique_failed_player.csv"
    )
    finalPlayerMapping = getPandasFactoryDF(
        session,
        f"select * from {PLAYER_MAPPER_TABLE_NAME}"
    )

    merged__df = pd.merge(
        sm_df[['sports_mechanics_id', 'cricinfo_id']],
        finalPlayerMapping,
        how='inner',
        left_on='cricinfo_id',
        right_on='cricinfo_id',

    )

    merged__df['cricinfo_id'] = merged__df['cricinfo_id'].astype(int)
    merged__df = merged__df.drop(['sports_mechanics_id_y'], axis=1).rename(columns={
        'sports_mechanics_id_x': 'sports_mechanics_id'
    })
    from datetime import datetime, time
    merged__df['load_timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    merged__df['full_name'] = merged__df['full_name'].apply(lambda x: str(x).replace("'", ""))
    merged__df['short_name'] = merged__df['short_name'].apply(lambda x: str(x).replace("'", ""))
    merged__df['name'] = merged__df['name'].apply(lambda x: str(x).replace("'", ""))

    insertToDB(session, merged__df.to_dict(orient="record"), DB_NAME, PLAYER_MAPPER_TABLE_NAME)
