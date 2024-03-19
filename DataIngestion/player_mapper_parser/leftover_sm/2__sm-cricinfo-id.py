import pandas as pd

from DataIngestion.utils.helper import readCSV
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session
from common.db_config import DB_NAME


def sm_parser():
    players_data = getPandasFactoryDF(
        session, f'''select * from  {DB_NAME}.Players; '''
    )
    sm_df = readCSV("/DataIngestion/player_mapper_parser/sm_with_names.csv")
    merged_df = pd.merge(
        players_data[['src_player_id', 'player_name']],
        sm_df[['name', 'cricinfo_id']].drop_duplicates(),
        left_on='player_name',
        right_on='name',
        how='inner'
    )
    merged_df = merged_df[['cricinfo_id', 'src_player_id']]
    merged_df_name = merged_df.drop_duplicates()

    merged_df = pd.merge(
        players_data[['src_player_id', 'player_name']],
        sm_df[['full_name', 'cricinfo_id']].drop_duplicates(),
        left_on='player_name',
        right_on='full_name',
        how='inner'
    )
    merged_df = merged_df[['cricinfo_id', 'src_player_id']]
    merged_df_full_name = merged_df.drop_duplicates()

    merged_df = pd.merge(
        players_data[['src_player_id', 'player_name']],
        sm_df[['short_name', 'cricinfo_id']].drop_duplicates(),
        left_on='player_name',
        right_on='short_name',
        how='inner'
    )
    merged_df = merged_df[['cricinfo_id', 'src_player_id']]
    merged_df_short_name = merged_df.drop_duplicates()
    merged_df_cricinfo = pd.concat([merged_df_name, merged_df_full_name, merged_df_short_name], ignore_index=True)
    merged_df_cricinfo = merged_df_cricinfo[['src_player_id', 'cricinfo_id']].drop_duplicates()
    merged_df_cricinfo.to_csv("sm-cricinfo-id.csv")
    print(merged_df)


if __name__ == '__main__':
    sm_parser()
