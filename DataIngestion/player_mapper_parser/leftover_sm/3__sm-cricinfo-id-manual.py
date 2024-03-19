import pandas as pd

from DataIngestion.utils.helper import readCSV
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session
from common.db_config import DB_NAME


def cricinfo_id_manual():
    players_data = getPandasFactoryDF(
        session, f'''select * from  {DB_NAME}.Players; '''
    )
    sm_df = readCSV(
        "/DataIngestion/player_mapper_parser/leftover_sm/sm-cricinfo-id.csv"
    )
    merged_df = pd.merge(
        players_data[['src_player_id', 'player_name']],
        sm_df,
        how="outer",
        on='src_player_id',
        indicator=True
    )
    unmapped = merged_df[merged_df['_merge'] == 'left_only'].drop_duplicates()
    unmapped.to_csv("sm-cricinfo-id-manual.csv")


if __name__ == '__main__':
    cricinfo_id_manual()
