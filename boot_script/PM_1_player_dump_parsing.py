import pandas as pd

from DataIngestion.utils.helper import readExcel, readCSV

if __name__ == "__main__":
    master_player_dump = "/Users/achintya.chaudhary/Documents/projects/CricketDataService/boot_script/PlayerList_WPL_2024.xlsx"
    player_dump_sheet = "Sheet1"
    player_dump_excel = readExcel(master_player_dump, player_dump_sheet)
    player_dump_excel = player_dump_excel[player_dump_excel['RefID'].notnull()]
    player_mapping_downloaded = "/Users/achintya.chaudhary/Documents/projects/CricketDataService/boot_script/player_mapping_download.csv"
    # read data from player mapping table and merge with above df
    player_mapping_downloaded = readCSV(player_mapping_downloaded)

    # merge data
    player_dump_merged = pd.merge(
        player_mapping_downloaded,
        player_dump_excel,
        how="right",
        left_on="sports_mechanics_id",
        right_on="RefID"
    )

    # get all the rows from x df where sports_mechanics_id is not null
    player_dump_merged = player_dump_merged[[
        'sports_mechanics_id', 'RefID', 'id', 'name', 'full_name', 'PLAYER_DISPLAY_NAME', 'PLAYER_FULL_NAME'
         ]]
    player_dump_merged.to_csv("player_dump_merged.csv")
