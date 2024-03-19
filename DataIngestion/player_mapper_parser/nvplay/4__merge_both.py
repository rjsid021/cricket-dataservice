import pandas as pd

from DataIngestion.utils.helper import readCSV


def update_new_players():
    nv_players = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/DataIngestion/player_mapper_parser/nvplay/3__map_playername.csv"
    )
    data_dir_unique_players = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/DataIngestion/player_mapper_parser/nvplay/2___csv_and_cricinfo_parser.csv"
    )

    name = pd.merge(
        nv_players,
        data_dir_unique_players,
        left_on='player',
        right_on='name',
        indicator=True
    )
    name = name[name['_merge'] == 'both']

    short_name = pd.merge(
        nv_players,
        data_dir_unique_players,
        left_on='player',
        right_on='short_name',
        indicator=True
    )
    short_name = short_name[short_name['_merge'] == 'both']

    full_name = pd.merge(
        nv_players,
        data_dir_unique_players,
        left_on='player',
        right_on='full_name',
        indicator=True
    )
    full_name = full_name[full_name['_merge'] == 'both']

    names_cricinfo = pd.concat([name, short_name, full_name])
    names_cricinfo = names_cricinfo.drop_duplicates()[['src_player_id', 'cricinfo_id']]
    names_cricinfo.to_csv("4__merged_nv_play_players.csv")


if __name__ == "__main__":
    update_new_players()
