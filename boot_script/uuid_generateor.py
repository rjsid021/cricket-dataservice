import datetime

from DataIngestion.config import PLAYER_MAPPER_TABLE_NAME
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao.insert_data import insertToDB
from common.dao_client import session
from common.db_config import DB_NAME

if __name__ == "__main__":
    import uuid
    player_mapping_df = getPandasFactoryDF(session, "select * from playermapping")
    # generate uuid for each row
    for row, index in player_mapping_df.iterrows():
        # assign default vale to uuid column
        player_mapping_df.at[row, 'nv_play_id'] = str(uuid.uuid4())

    player_mapping_df['load_timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    player_mapping_df = player_mapping_df.to_dict(orient='records')
    insertToDB(session, player_mapping_df, DB_NAME, PLAYER_MAPPER_TABLE_NAME)
