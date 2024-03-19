import pandas as pd

from DataIngestion.utils.helper import readCSV
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session
from common.db_config import DB_NAME


def update_new_players():
    sM_df_unique = readCSV(
        f"/DataIngestion/player_mapper_parser/leftover_sm/db_players_unique_sm_id.csv"
    )

    players_data = getPandasFactoryDF(
        session, f'''select * from  {DB_NAME}.pppMapping;'''
    )

    mismatch_join = pd.merge(
        sM_df_unique,
        players_data,
        how='outer',
        left_on='src_player_id',
        right_on='sports_mechanics_id',
        indicator=True
    )

    print(mismatch_join[mismatch_join['_merge']] == 'right_only')


if __name__ == "__main__":
    update_new_players()
