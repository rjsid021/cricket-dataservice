import pandas as pd

from DataIngestion.utils.helper import readExcel, readCSV


def make_uk():
    possible_cricinfo = readExcel(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/data/uk_tour_player_name_ (1).xlsx",
        "Sheet1"
    )
    csv_sheet = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/data/nv_play/UKT/UKT 2023/UKT 2023.csv")
    batter = csv_sheet[['Batter']].rename(columns={
        'Batter': 'Player'
    })
    bowler = csv_sheet[['Bowler']].rename(columns={"Bowler": "Player"})
    non_striker = csv_sheet[['Non-Striker']].rename(columns={"Non-Striker": "Player"})
    players = pd.concat([batter, bowler, non_striker], axis=0)
    players = players.drop_duplicates()
    players = players.reset_index()
    players = players.drop(columns=['index'])
    players = players.dropna()
    uk_directory = pd.merge(
        possible_cricinfo,
        players,
        left_on='Player',
        right_on='Player',
        how='inner'
    )
    print(uk_directory.to_csv("uk_tour_cricinfo_id.csv"))


def print_db_name_with_cricinfo_id():
    possible_cricinfo = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/data/uk_tour_cricinfo_id.csv"
    )
    csv_sheet = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/data/nv_play/UKT/UKT 2023/UKT 2023.csv")
    batter = csv_sheet[['Batter']].rename(columns={'Batter': 'Player'})
    bowler = csv_sheet[['Bowler']].rename(columns={"Bowler": "Player"})
    non_striker = csv_sheet[['Non-Striker']].rename(columns={"Non-Striker": "Player"})
    players = pd.concat([batter, bowler, non_striker], axis=0)
    players = players.drop_duplicates()
    players = players.reset_index()
    players = players.drop(columns=['index'])
    players = players.dropna()
    uk_directory = pd.merge(
        possible_cricinfo,
        players,
        left_on='Player_Excel',
        right_on='Player',
        how='inner'
    )
    print(uk_directory.to_csv("uk_tour_cricinfo_id.csv"))


def create_squad_for_ukt():
    final_response = []
    csv_sheet = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/data/nv_play/UKT/UKT 2023/UKT 2023.csv"
    )
    # Group csv_sheet df by team name
    grouped_csv_sheet = csv_sheet.groupby(["Match", "Date"])
    for group_name, group_data in grouped_csv_sheet:
        x = group_data[['Batter', 'Batting Team']].rename(columns={'Batter': 'Player', 'Batting Team': 'Team'})
        y = group_data[['Dismissed Batter', 'Batting Team']].rename(
            columns={'Dismissed Batter': 'Player', 'Batting Team': 'Team'})
        z = group_data[['Non-Striker', 'Batting Team']].rename(
            columns={'Non-Striker': 'Player', 'Batting Team': 'Team'})
        a = group_data[['Bowler', 'Bowling Team']].rename(columns={'Bowler': 'Player', 'Bowling Team': 'Team'})

        player_team = pd.concat([x, y, z, a], axis=0).dropna()
        possible_cricinfo = readCSV(
            "/Users/achintya.chaudhary/Documents/projects/CricketDataService/data/uk_tour_cricinfo_id.csv"
        )
        player_team = pd.merge(
            player_team,
            possible_cricinfo[['Player_Excel', 'key_cricinfo']],
            left_on='Player',
            right_on='Player_Excel',
            how='inner'
        ).drop(columns=['Player_Excel'], axis=1)
        player_team = player_team.groupby(['Team'])
        current_match_response = {
            "venue": f"{group_name[1]} : {group_name[0]}",
            "match_name": group_name[0],
            "match_date": group_name[1],
        }
        for group_name, group_data in player_team:
            group_data = group_data.drop_duplicates()
            group_data = group_data[['key_cricinfo', 'Player']].rename(
                columns={'Player': 'player_name', 'key_cricinfo': 'cricinfo_id'})
            current_match_response[group_name] = group_data.to_dict(orient="records")

        final_response.append(current_match_response)
    return final_response


from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session


def get_new_name():
    player_mapping = getPandasFactoryDF(session, "select * from playerMapping")
    possible_cricinfo = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/data/uk_tour_cricinfo_id.csv"
    )
    merged = pd.merge(
        player_mapping,
        possible_cricinfo,
        left_on='cricinfo_id',
        right_on='key_cricinfo',
        how='inner'
    )
    merged[['name', 'cricinfo_id', 'Player_Excel']].to_csv("name_to_name_mapper.csv")


def update_csv_uk_tour():
    main_csv = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/data/nv_play/UKT/UKT 2023/UKT 2023.csv")
    name_to_name = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/data/name_to_name_mapper.csv")
    mapper_dict = {}
    for index, row in name_to_name.iterrows():
        mapper_dict[row['Player_Excel']] = row['name']
    print(mapper_dict)
    main_csv['Batter'] = main_csv['Batter'].replace(mapper_dict)
    main_csv['Non-Striker'] = main_csv['Non-Striker'].replace(mapper_dict)
    main_csv['Dismissed Batter'] = main_csv['Dismissed Batter'].replace(mapper_dict)
    main_csv['Bowler'] = main_csv['Bowler'].replace(mapper_dict)
    print(main_csv.to_csv("UKT 2023.csv"))


if __name__ == "__main__":
    # make_uk()
    # print_db_name_with_cricinfo_id()
    # print(create_squad_for_ukt())
    # get_new_name()
    update_csv_uk_tour()
