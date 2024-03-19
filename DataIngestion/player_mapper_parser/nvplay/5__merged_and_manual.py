import pandas as pd

from DataIngestion.utils.helper import readCSV


def update_new_players():
    nv_players = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/DataIngestion/player_mapper_parser/nvplay/3__map_playername.csv"
    )
    data_dir_unique_players = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/DataIngestion/player_mapper_parser/nvplay/4__merged_nv_play_players.csv"
    )


    full_name = pd.merge(
        nv_players,
        data_dir_unique_players,
        left_on='src_player_id',
        right_on='src_player_id',
        how="outer",
        indicator=True
    )
    full_name = full_name[['player', 'src_player_id', 'cricinfo_id']]
    full_name.to_csv("5__merged_and_manual.csv")


if __name__ == "__main__":
    update_new_players()
