# This file is used to change the name of player who have played in nvplay to name which we have
# stored in player mapping table

import pandas as pd

from DataIngestion.utils.helper import readCSV
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session
from common.db_config import DB_NAME


def parse_multiple_seasons(file_name, file_path):
    striker = pd.DataFrame(columns=['Batter'])
    bowler = pd.DataFrame(columns=['Bowler'])
    non_striker = pd.DataFrame(columns=['Non-Striker'])
    dismissed_batter = pd.DataFrame(columns=['Dismissed Batter'])
    season_matches_df = readCSV(file_path)
    striker = striker.append(season_matches_df, ignore_index=True)
    bowler = bowler.append(season_matches_df, ignore_index=True)
    non_striker = non_striker.append(season_matches_df, ignore_index=True)
    dismissed_batter = dismissed_batter.append(season_matches_df, ignore_index=True)

    # rename df to player
    striker.rename(columns={'Batter': 'player'}, inplace=True)
    bowler.rename(columns={'Bowler': 'player'}, inplace=True)
    non_striker.rename(columns={'Non-Striker': 'player'}, inplace=True)
    dismissed_batter.rename(columns={'Dismissed Batter': 'player'}, inplace=True)

    # take only player column
    striker = striker[['player', 'Match', 'Date']]
    bowler = bowler[['player', 'Match', 'Date']]
    non_striker = non_striker[['player', 'Match', 'Date']]
    dismissed_batter = dismissed_batter[['player', 'Match', 'Date']]

    # append all df
    final_df = pd.concat([striker, bowler, non_striker, dismissed_batter], axis=0, ignore_index=True)
    unique_nv_players = final_df.drop_duplicates(subset=['player'])
    unique_nv_players.rename(columns={'player': 'nvplay_name'}, inplace=True)
    # read player mapper from cassandra db using getPandasFactoryDF method
    # and store it in pandas dataframe
    player_mapping_df = getPandasFactoryDF(
        session,
        f"select * from {DB_NAME}.playermapping;"
    )

    # merge both dataframes on player column
    all_3_common = pd.merge(
        unique_nv_players,
        player_mapping_df[['cricinfo_id', 'name']],
        left_on='nvplay_name',
        right_on='name',
        how='left'
    )
    all_3_common.sort_values('Date').to_csv(f"{file_name}")


if __name__ == "__main__":
    file_name = "stage_one.csv"
    file_path = "/Users/achintya.chaudhary/Documents/projects/CricketDataService/data/nv_play/ILT20/ILT20 2023/ILT20.csv"
    parse_multiple_seasons(file_name, file_path)
