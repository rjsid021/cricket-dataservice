import datetime
import hashlib
import sys

from DataIngestion.config import PLAYER_MAPPER_TABLE_NAME
from common.dao.insert_data import insertToDB

sys.path.append("./../../")
sys.path.append("./")
from common.db_config import DB_NAME
import pandas as pd

from DataIngestion.utils.helper import readCSV
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session


def create_short_hash(player_name):
    if type(player_name) != str:
        return ""
    # Create a hash object using SHA-256
    sha256_hash = hashlib.sha256(player_name.encode()).hexdigest()
    return f"nv_{sha256_hash}"


def update_new_players():
    sm_df = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/DataIngestion/player_mapper_parser/ukt/ukt.csv"
    )

    sm_df.to_csv("ukt_nvplay_id.csv")
    player_mapping = getPandasFactoryDF(session, "select * from playerMapping")
    merged_df = pd.merge(
        sm_df['cricinfo_id'],
        player_mapping,
        on='cricinfo_id',
    )
    # Print top 100 record from above dataframe
    print(merged_df.head(100))
    merged_df = merged_df.drop(['nvplay_id'], axis=1)
    merged_df['nvplay_id'] = merged_df['name'].apply(lambda x: create_short_hash(x))
    merged_df['load_timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    merged_df = merged_df.to_dict(orient='records')
    insertToDB(session, merged_df, DB_NAME, PLAYER_MAPPER_TABLE_NAME)


if __name__ == "__main__":
    update_new_players()
