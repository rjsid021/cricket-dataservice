import sys

sys.path.append("../../../")
sys.path.append("../")
from common.dao.fetch_db_data import getMaxId
from DataIngestion.utils.helper import generateSeq

from DataIngestion.utils.helper import readCSV
from common.dao.insert_data import insertToDB
from common.dao_client import session
from common.db_config import DB_NAME


def mapped_id():
    sm_df = readCSV(
        "/DataIngestion/player_mapper_parser/leftover_sm/sm-cricinfo-id.csv"
    ).rename(columns={
        'src_player_id': 'sports_mechanics_id'
    })
    sm_df = sm_df[['sports_mechanics_id', 'cricinfo_id']]
    PLAYER_MAPPER_TABLE_NAME = 'pppMapping'
    PLAYER_MAPPER_KEY_COL = "id"
    max_key_val = getMaxId(session, PLAYER_MAPPER_TABLE_NAME, PLAYER_MAPPER_KEY_COL, DB_NAME)

    bat_card_final_data = generateSeq(
        sm_df, PLAYER_MAPPER_KEY_COL, max_key_val
    ).to_dict(orient='records')

    insertToDB(session, bat_card_final_data, DB_NAME, PLAYER_MAPPER_TABLE_NAME)


def unmapped__id():
    sm_df = readCSV(
        "/DataIngestion/player_mapper_parser/leftover_sm/sm-cricinfo-id-manual.csv"
    ).rename(columns={
        'src_player_id': 'sports_mechanics_id'
    })
    sm_df = sm_df[['sports_mechanics_id']]
    PLAYER_MAPPER_TABLE_NAME = 'pppMapping'
    PLAYER_MAPPER_KEY_COL = "id"
    max_key_val = getMaxId(session, PLAYER_MAPPER_TABLE_NAME, PLAYER_MAPPER_KEY_COL, DB_NAME)

    bat_card_final_data = generateSeq(
        sm_df, PLAYER_MAPPER_KEY_COL, max_key_val
    ).to_dict(orient='records')

    insertToDB(session, bat_card_final_data, DB_NAME, PLAYER_MAPPER_TABLE_NAME)


if __name__ == '__main__':
    # mapped_id()
    # unmapped__id()
    pass
