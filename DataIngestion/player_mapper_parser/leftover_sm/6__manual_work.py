import pandas as pd

from DataIngestion.utils.helper import readCSV
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session
from common.db_config import DB_NAME


def manual_work_006():
    sM_df_unique = readCSV(
        f"/DataIngestion/player_mapper_parser/leftover_sm/sm-cricinfo-id-manual.csv"
    )[['src_player_id']]
    players_data = getPandasFactoryDF(
        session, f'''select * from  {DB_NAME}.players;'''
    )
    mismatch_join = pd.merge(
        sM_df_unique,
        players_data,
        how='inner',
        left_on='src_player_id',
        right_on='src_player_id',
        indicator=True
    )[['src_player_id', 'player_name', 'competition_name', 'season', 'team_id']]
    mismatch_join.to_csv("6__manual_works.csv")


if __name__ == "__main__":
    manual_work_006()
