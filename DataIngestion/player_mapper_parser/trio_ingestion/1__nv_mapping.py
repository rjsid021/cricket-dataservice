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
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/DataIngestion/player_mapper_parser/trio_ingestion/1__merged_and_manual_nv.csv"
    )
    finalPlayerMapping = getPandasFactoryDF(
        session,
        f"select * from {PLAYER_MAPPER_TABLE_NAME}"
    )

    merged__df = pd.merge(
        sm_df[['src_player_id', 'cricinfo_id']],
        finalPlayerMapping,
        how='inner',
        left_on='cricinfo_id',
        right_on='cricinfo_id',

    )

    merged__df['cricinfo_id'] = merged__df['cricinfo_id'].astype(int)
    merged__df = merged__df.drop(['nvplay_id'], axis=1).rename(columns={
        'src_player_id': 'nvplay_id'
    })
    from datetime import datetime, time
    merged__df['load_timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    merged__df['full_name'] = merged__df['full_name'].apply(lambda x: str(x).replace("'", ""))
    merged__df['short_name'] = merged__df['short_name'].apply(lambda x: str(x).replace("'", ""))
    merged__df['name'] = merged__df['name'].apply(lambda x: str(x).replace("'", ""))
    xxx = merged__df.to_dict(orient="records")
    insertToDB(session, xxx, DB_NAME, PLAYER_MAPPER_TABLE_NAME)
