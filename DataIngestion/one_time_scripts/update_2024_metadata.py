from DataIngestion.config import PLAYERS_TABLE_NAME, PLAYERS_KEY_COL, FILE_SHARE_PATH, IMAGE_STORE_URL
from DataIngestion.utils.helper import readCSV
from common.dao.fetch_db_data import getPandasFactoryDF, getMaxId
from common.dao.insert_data import insertToDB
from common.dao_client import session
from common.db_config import DB_NAME
from common.utils.helper import getPrettyDF
import numpy as np
import datetime
import os

# league_name = "ILT20"
# updating teams table for SA20
squad_data_path = os.path.join(FILE_SHARE_PATH, "data/squad_2024.csv")
#squad_data_path = "/Users/siddharth.nautiyal/Desktop/cricket/GIT/git-master/CricketDataService/data/squad_2024.csv"
latest_players_df = readCSV(squad_data_path)
#leagues = list(latest_players_df['competition_name'].unique())
leagues = ["IPL"]
print(f"leagues --> {leagues}")
for league_name in leagues:
    teams_df = getPandasFactoryDF(session,
                                  f'''select * from {DB_NAME}.teams where competition_name='{league_name}' ALLOW FILTERING;''')
    teams_df['seasons_played'] = np.where(teams_df["team_short_name"].isin(["PSG", "GL", "PWI", "KTK", "DCH"]), teams_df['seasons_played'], teams_df['seasons_played'].apply(lambda x: x + [2024]))
    # teams_df['seasons_played'] =teams_df['seasons_played'].apply(lambda x: [2023, 2024])
    teams_df['load_timestamp'] = teams_df['load_timestamp'].astype(str)
    teams_df['color_code_gradient'] = "#000000"
    # print(getPrettyDF(teams_df))
    # insertToDB(session, teams_df.to_dict(orient="records"), DB_NAME, "teams", allow_logging=True)

    # updating players table for SA20

    teams_df = getPandasFactoryDF(session,
                                  f'''select team_id, team_name from {DB_NAME}.teams where competition_name='{league_name}' ALLOW FILTERING;''')
    players_df = getPandasFactoryDF(session, f'''select * from {DB_NAME}.players''').drop(columns=['competition_name'],
                                                                                          axis=1)
    players_df["player_rank"] = players_df.groupby(["player_id"])["season"].rank(method="first", ascending=False)
    players_df = players_df[players_df["player_rank"] == 1]

    player_mapping_df = getPandasFactoryDF(session, f'''select name, id as src_player_id, bowler_sub_type as bowling_type, is_batsman, is_bowler,is_wicket_keeper,
     striker_batting_type as batting_type from {DB_NAME}.playerMapping''')
    # player_mapping_df = player_mapping_df[player_mapping_df["sports_mechanics_id"]!=""]#.drop(["sports_mechanics_id"], axis=1)
    # print(getPrettyDF(player_mapping_df))
    new_players_df = latest_players_df[latest_players_df['competition_name'] == league_name]
    load_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    merged_players_df = new_players_df.merge(players_df, left_on='player_name', right_on='player_name', how='left')
    available_players_df = merged_players_df[~merged_players_df['player_id'].isnull()].drop(
        ['team_id', 'player_rank', 'load_timestamp'], axis=1)
    available_players_df['competition_name'] = league_name
    available_players_df['season'] = "2024"
    available_players_df['player_image_url'] = available_players_df['player_image_url'] = (IMAGE_STORE_URL + 'players/' + available_players_df['player_name'].apply(
            lambda x: x.replace(' ', '-').lower()).astype(str) + ".png")

    available_players_df = available_players_df.merge(teams_df, on=["team_name"], how="left").drop(["team_name"],
                                                                                                   axis=1)

    not_available_players_df = merged_players_df[merged_players_df['player_id'].isnull()][['player_name', 'team_name']]
    not_available_players_df = not_available_players_df.merge(player_mapping_df,
                                                              left_on=not_available_players_df['player_name'].apply(lambda x: x.strip().replace(" ", "").lower()),
                                                              right_on=player_mapping_df['name'].apply(lambda x: x.strip().replace(" ", "").lower()),
                                                              how='left').drop(columns=["key_0"], axis=1)
    not_available_players_df['competition_name'] = league_name
    not_available_players_df['season'] = "2024"
    not_available_players_df['player_image_url'] = not_available_players_df['player_image_url'] = (IMAGE_STORE_URL + 'players/' + not_available_players_df['player_name'].apply(
            lambda x: x.replace(' ', '-').lower()).astype(str) + ".png")
    not_available_players_df['is_captain'] = 0
    not_available_players_df['player_type'] = 'Overseas'
    not_available_players_df['bowl_major_type'] = np.where(
        (not_available_players_df['bowling_type'] == 'LEFT ARM FAST') |
        (not_available_players_df['bowling_type'] == 'RIGHT ARM FAST'),
        'SEAM',
        'SPIN'
    )

    skill_conditions = [
        (not_available_players_df['is_batsman'] == 1) & (not_available_players_df['is_bowler'] == 0) | (
                    not_available_players_df['is_wicket_keeper'] == 1),
        (not_available_players_df['is_batsman'] == 1) & (not_available_players_df['is_bowler'] == 1),
        (not_available_players_df['is_wicket_keeper'] == 1),
        (not_available_players_df['is_batsman'] == 0) & (not_available_players_df['is_bowler'] == 1)
    ]

    # different batting phases
    skill_values = ["BATSMAN", "ALLROUNDER", "WICKETKEEPER", "BOWLER"]

    # create batting_phase column
    not_available_players_df['player_skill'] = np.select(skill_conditions, skill_values)
    not_available_players_df = not_available_players_df.merge(teams_df, on="team_name", how="left").drop(
        ["team_name", "name"], axis=1)
    max_key_val = getMaxId(session, PLAYERS_TABLE_NAME, PLAYERS_KEY_COL, DB_NAME, False)
    # Add player id to new players

    print(getPrettyDF(not_available_players_df))
    not_available_players_df['player_id'] = not_available_players_df['src_player_id'].rank(
        method='dense', ascending=False).apply(lambda x: x + max_key_val).astype(int)

    final_players_df = not_available_players_df.append(available_players_df, ignore_index=True)
    final_players_df["load_timestamp"] = load_timestamp

    int_cols = ["player_id", "is_batsman", "is_wicket_keeper", "is_bowler", "is_captain"]
    for col in int_cols:
        final_players_df[col] = final_players_df[col].astype(int)
    final_players_df["src_player_id"] = final_players_df["src_player_id"].astype(str)
    # import uuid
    # final_players_df = final_players_df[final_players_df['src_player_id'].isnull()]
    # final_players_df['hash'] = final_players_df['player_name'].apply(lambda x: str(uuid.uuid4()))
    # print(getPrettyDF(final_players_df))
    insertToDB(session, final_players_df.fillna("").to_dict(orient="records"), DB_NAME, "players", allow_logging=True)
