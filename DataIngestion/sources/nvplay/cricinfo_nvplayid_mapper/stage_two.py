# This file is used to change the name of player who have played in nvplay to name which we have
# stored in player mapping table

import pandas as pd

from DataIngestion.utils.helper import readCSV


def get_unique_players_from_stage_one():
    stage_one_as_input = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/DataIngestion/sources/nvplay/cricinfo_nvplayid_mapper/stage_one.csv"
    )
    return stage_one_as_input.sort_values("Date")


if __name__ == "__main__":
    stage_one_as_input = get_unique_players_from_stage_one()

    # check if any entry exists in master_mapper
    master_mapper = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/DataIngestion/sources/nvplay/mapper_dir/master_mapper.csv"
    )
    all_3_common = pd.merge(
        stage_one_as_input,
        master_mapper[['cricinfo_id', 'nvplay_name']],
        on='nvplay_name',
        how='left'
    )
    all_3_common.to_csv(f"stage_two.csv")
