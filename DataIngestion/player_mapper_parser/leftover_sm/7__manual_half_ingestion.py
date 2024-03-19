import sys

import pandas as pd

sys.path.append("../../../")
sys.path.append("../")
from common.dao.fetch_db_data import getMaxId, getPandasFactoryDF

from DataIngestion.utils.helper import readCSV
from common.dao.insert_data import insertToDB
from common.dao_client import session
from common.db_config import DB_NAME


def mapped_id():
    sm_df = readCSV(
        "/DataIngestion/player_mapper_parser/leftover_sm/7__manual_half_ingestion.csv"
    ).rename(columns={
        'src_player_id': 'sports_mechanics_id'
    })
    pppMapping = getPandasFactoryDF(session, f"select * from {DB_NAME}.pppMapping")
    sm_df = sm_df[['sports_mechanics_id', 'cricinfo_id']]
    PLAYER_MAPPER_TABLE_NAME = 'pppMapping'
    PLAYER_MAPPER_KEY_COL = "id"
    max_key_val = getMaxId(session, PLAYER_MAPPER_TABLE_NAME, PLAYER_MAPPER_KEY_COL, DB_NAME)
    mergeddd = pd.merge(
        pppMapping[['id', 'sports_mechanics_id', 'cricinfo_id']],
        sm_df[['sports_mechanics_id', 'cricinfo_id']],
        on='sports_mechanics_id',
        how='inner',
        indicator=True
    ).drop(['cricinfo_id_x', '_merge'], axis=1)
    mergeddd = mergeddd.rename(
        columns={
            'cricinfo_id_y': 'cricinfo_id'
        }
    )
    bat_card_final_data = mergeddd.to_dict(orient='record')
    insertToDB(session, bat_card_final_data, DB_NAME, PLAYER_MAPPER_TABLE_NAME)


if __name__ == '__main__':
    mapped_id()
    # unmapped__id()
    pass
