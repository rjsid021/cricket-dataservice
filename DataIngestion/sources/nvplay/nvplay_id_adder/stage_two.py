from DataIngestion.utils.helper import readCSV
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session

if __name__ == "__main__":
    source_file = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/DataIngestion/sources/nvplay/nvplay_id_adder/stage_one.csv"
    )

    player_mapping_df = getPandasFactoryDF(
        session, f"select name, nv_play_id from "
                 f"playermapping where cricinfo_id in "
                 f"(429981, 1159495, 1159371, 793463, 697279, 532424, 1283740, 42639, 1185538, 1158099, 316363, 1234111, 1137262, 480603, 1168651) "
                 f"allow filtering")
    column_to_check = 'name'
    all_player_mapping = getPandasFactoryDF(session, "select name, nv_play_id from playermapping")
    duplicates_in_column = all_player_mapping[all_player_mapping.duplicated(subset=[column_to_check], keep=False)]
    df_no_duplicates = all_player_mapping.drop(duplicates_in_column.index)
    players_mapping = dict(zip(player_mapping_df['name'], player_mapping_df['nv_play_id']))
    cc = dict(zip(df_no_duplicates['name'], df_no_duplicates['nv_play_id']))
    abc = {
        **cc,
        **players_mapping
    }
    source_file['batsman_id'] = source_file['Batter'].map(abc)
    source_file['bowler_id'] = source_file['Bowler'].map(abc)
    source_file['non_striker_id'] = source_file['Non-Striker'].map(abc)
    source_file['out_batsman_id'] = source_file['Dismissed Batter'].map(abc)
    source_file.to_csv("stage_two.csv")
