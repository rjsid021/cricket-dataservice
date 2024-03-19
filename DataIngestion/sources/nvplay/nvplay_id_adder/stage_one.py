from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session

if __name__ == "__main__":
    player_mapping_df = getPandasFactoryDF(session, "select name, nv_play_id from playermapping")
    import pandas as pd

    column_to_check = 'name'

    duplicates_in_column = player_mapping_df[player_mapping_df.duplicated(subset=[column_to_check], keep=False)]
    df_no_duplicates = player_mapping_df.drop(duplicates_in_column.index)

    players_mapping = dict(zip(df_no_duplicates['name'], df_no_duplicates['nv_play_id']))
    source_file = pd.read_csv(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/data/nv_play/PSL/PSL 2023/PSL 2023.csv"
    )
    source_file.to_csv("stage_one.csv")
    source_file['batsman_id'] = source_file['Batter'].map(players_mapping)
    source_file['batsman_id'] = source_file['batsman_id'].fillna("")

    source_file['bowler_id'] = source_file['Bowler'].map(players_mapping)
    source_file['bowler_id'] = source_file['bowler_id'].fillna("")

    source_file['non_striker_id'] = source_file['Non-Striker'].map(players_mapping)
    source_file['non_striker_id'] = source_file['non_striker_id'].fillna("")

    source_file['out_batsman_id'] = source_file['Dismissed Batter'].map(players_mapping)
    source_file['out_batsman_id'] = source_file['out_batsman_id'].fillna("-1")

    left_over_batter = source_file[['batsman_id', 'Batter']].drop_duplicates()
    left_over_bowler = source_file[['bowler_id', 'Bowler']].drop_duplicates()
    left_over_non_striker = source_file[['non_striker_id', 'Non-Striker']].drop_duplicates()
    left_over_out_batter = source_file[['out_batsman_id', 'Dismissed Batter']].drop_duplicates()
    unique_set = set()
    batter = left_over_batter[left_over_batter['batsman_id'] == ''][['Batter']].rename(columns={'Batter': 'player'})[
        'player'].tolist()
    bowler =left_over_bowler[left_over_bowler['bowler_id'] == ''][['Bowler']].rename(columns={'Bowler': 'player'})[
        'player'].tolist()
    non_striker =left_over_non_striker[left_over_non_striker['non_striker_id'] == ''][['Non-Striker']].rename(
        columns={'Non-Striker': 'player'})['player']
    outter =left_over_out_batter[left_over_out_batter['out_batsman_id'] == ''][['Dismissed Batter']].rename(
        columns={'Dismissed Batter': 'player'})['player']
    for b in batter:
        unique_set.add(b)
    for b in bowler:
        unique_set.add(b)
    for b in non_striker:
        unique_set.add(b)
    for b in outter:
        unique_set.add(b)
    print(unique_set)
