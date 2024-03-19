# This file is used to change the name of player who have played in nvplay to name which we have
# stored in player mapping table
import os

import pandas as pd

from DataIngestion.config import NVPLAY_PATH
from DataIngestion.utils.helper import readCSV
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session
from common.db_config import DB_NAME


def fun_cll():
    folder_paths = []
    nv_play_files = NVPLAY_PATH
    cricsheet_files = os.listdir(nv_play_files)
    striker = pd.DataFrame(columns=['Batter'])
    bowler = pd.DataFrame(columns=['Bowler'])
    non_striker = pd.DataFrame(columns=['Non-Striker'])
    dismissed_batter = pd.DataFrame(columns=['Dismissed Batter'])
    for file in cricsheet_files:
        if file == ".DS_Store":
            continue
        folder_path = os.path.join(nv_play_files, file)
        folder_path_sub_dir = os.listdir(folder_path)
        for sub_dir in folder_path_sub_dir:
            folder_paths.append(os.path.join(folder_path, sub_dir))
    for folder_path in folder_paths:
        if folder_path.split("/")[-1] == ".DS_Store":
            continue
        file_names = os.listdir(folder_path)
        for idx, file_name in enumerate(file_names):
            if file_name[-3:] != "csv":
                continue
            print("started for match ------> ", folder_path.split('/')[-1], file_name)
            season_matches_df = readCSV(os.path.join(folder_path, file_name))
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
    striker = striker[['player']]
    bowler = bowler[['player']]
    non_striker = non_striker[['player']]
    dismissed_batter = dismissed_batter[['player']]

    # append all df
    final_df = pd.concat([striker, bowler, non_striker, dismissed_batter], axis=0, ignore_index=True)
    unique_nv_players = final_df.drop_duplicates(subset=['player'])

    # read player mapper from cassandra db using getPandasFactoryDF method
    # and store it in pandas dataframe
    player_mapping_df = getPandasFactoryDF(
        session,
        f"select * from {DB_NAME}.playermapping;"
    )

    # merge both dataframes on player column
    merged_name_df = pd.merge(
        unique_nv_players,
        player_mapping_df[['cricinfo_id', 'name']],
        left_on='player',
        right_on='name',
        how='inner'
    ).drop(columns=['name'], axis=1)
    merged_short_name_df = pd.merge(
        unique_nv_players,
        player_mapping_df[['cricinfo_id', 'short_name']],
        left_on='player',
        right_on='short_name',
        how='inner'
    ).drop(columns=['short_name'], axis=1)
    merged_full_name_df = pd.merge(
        unique_nv_players,
        player_mapping_df[['cricinfo_id', 'full_name']],
        left_on='player',
        right_on='full_name',
        how='inner'
    ).drop(columns=['full_name'], axis=1)
    all_3_common = pd.concat([merged_name_df, merged_short_name_df, merged_full_name_df], axis=0, ignore_index=True)
    all_3_common = all_3_common.drop_duplicates()
    all_mix = pd.merge(
        unique_nv_players,
        all_3_common,
        left_on='player',
        right_on='player',
        how='left'
    )
    return all_mix


if __name__ == "__main__":
    folder_paths = []
    nv_play_files = NVPLAY_PATH
    cricsheet_files = os.listdir(nv_play_files)
    striker = pd.DataFrame(columns=['Batter'])
    bowler = pd.DataFrame(columns=['Bowler'])
    non_striker = pd.DataFrame(columns=['Non-Striker'])
    dismissed_batter = pd.DataFrame(columns=['Dismissed Batter'])
    for file in cricsheet_files:
        if file == ".DS_Store":
            continue
        folder_path = os.path.join(nv_play_files, file)
        folder_path_sub_dir = os.listdir(folder_path)
        for sub_dir in folder_path_sub_dir:
            folder_paths.append(os.path.join(folder_path, sub_dir))
    for folder_path in folder_paths:
        if folder_path.split("/")[-1] == ".DS_Store":
            continue
        file_names = os.listdir(folder_path)
        for idx, file_name in enumerate(file_names):
            if file_name[-3:] != "csv":
                continue
            print("started for match ------> ", folder_path.split('/')[-1], file_name)
            season_matches_df = readCSV(os.path.join(folder_path, file_name))
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
    ppp = fun_cll()
    merged_name_df = pd.merge(
        unique_nv_players,
        ppp,
        on='player',
        how='inner'
    )
    merged_name_df.to_csv("unfilled_mapping.csv")
