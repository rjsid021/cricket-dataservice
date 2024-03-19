import json
import warnings
from datetime import datetime

import numpy as np
import pandas as pd

from DataIngestion.config import PLAYER_MAPPER_TABLE_NAME
from DataIngestion.config import TEAM_MAPPING_PATH
from DataIngestion.sources.cricsheet.handler import NoResultHandler
from DataIngestion.utils.helper import readCSV
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session

warnings.filterwarnings("ignore")

bowl_major_type_dict = {
    'LEGBREAK': 'LEGBREAK',
    'LEFT ARM CHINAMAN': 'SPIN',
    'LEFT ARM FAST': 'SEAM',
    'LEFT ARM MEDIUM': 'LEFT ARM MEDIUM',
    'LEFT ARM ORTHODOX': 'SPIN',
    'nan': 'nan',
    'RIGHT ARM FAST': 'SEAM',
    'RIGHT ARM FAST MEDIUM': 'RIGHT ARM FAST MEDIUM',
    'RIGHT ARM LEG SPIN': 'SEAM',
    'RIGHT ARM MEDIUM': 'RIGHT ARM MEDIUM',
    'RIGHT ARM MEDIUM FAST': 'RIGHT ARM MEDIUM FAST',
    'RIGHT ARM OFF SPIN': 'SPIN',
    'RIGHT ARM OFFBREAK': 'SPIN',
    'SLOW LEFT ARM ORTHODOX': 'SLOW LEFT ARM ORTHODOX'
}


class MatchParser:

    def __init__(self):
        self.team_mapping = readCSV(TEAM_MAPPING_PATH)

    def get_match_name(self, team_one, team_two):
        return (
                self.team_mapping[self.team_mapping['team_name'] == team_one.upper()].iloc[0][1]
                + "VS"
                + self.team_mapping[self.team_mapping['team_name'] == team_two.upper()].iloc[0][1]
        ).upper()

    # def odi_parser(self, data, player_info, match_id):
    #     if not ("info" in data and "innings" in data):
    #         raise Exception("There is an issue with JSON data input")
    #
    #     date = data["info"]["dates"][0]
    #     date_object = datetime.strptime(date, '%Y-%m-%d')
    #     match_number = date_object.strftime('%d%m%Y')
    #     match_name = (("VS".join(data["info"]["teams"])).replace(" ", "_") + str(date)).replace("-", "")
    #     match_name = match_name + match_number
    #     winning_team = (data["info"]["outcome"].get("winner", "None")).upper()
    #     toss_team = (data["info"]["toss"].get("winner")).upper()
    #     toss_decision = f'{data["info"]["toss"].get("winner")} Won The Toss elected to {data["info"]["toss"].get("decision")}'
    #
    #     # Parse match Result
    #     if data["info"]["outcome"].get("result") and data["info"]["outcome"].get("result") == "no result":
    #         match_result = "No Match Result"
    #         NoResultHandler(match_result, match_id, match_name).log_ingestion_table()
    #     elif not data["info"]["outcome"].get("by"):
    #         if data["info"]["outcome"].get("method"):
    #             match_result = f'{data["info"]["outcome"].get("winner")} Won By {data["info"]["outcome"].get("winner")}'
    #         else:
    #             match_result = data["info"]["outcome"]["result"]
    #     else:
    #         if data["info"]["outcome"]["by"].get("runs"):
    #             match_result = f'{data["info"]["outcome"].get("winner")} Won By {data["info"]["outcome"]["by"].get("runs")} Runs'
    #         else:
    #             match_result = f'{data["info"]["outcome"].get("winner")} Won By {data["info"]["outcome"]["by"].get("wickets")} Wickets'
    #
    #     # Remove everything after stadium name. i.e - Country or Location of stadium
    #     stadium_name = data["info"]["venue"].split(",", 1)[0]
    #     if len(data["innings"]) >= 2:
    #         team_short_name_x = (data["innings"][0]["team"]).upper()
    #         team_short_name_y = (data["innings"][1]["team"]).upper()
    #     elif len(data["innings"]) == 1:
    #         team_short_name_x = (data["innings"][0]["team"]).upper()
    #         team_short_name_y = None
    #         teams = data["info"]["teams"]
    #         for t in teams:
    #             if t.upper() == team_short_name_x:
    #                 continue
    #             else:
    #                 team_short_name_y = t.upper()
    #
    #     # Check if match is of playoffs
    #     try:
    #         stage = data["info"]["event"]["stage"]
    #     except:
    #         stage = "None"
    #     stage = stage.lower()
    #     if (
    #             ("final" in stage)
    #             or ("play" in stage and "off" in stage)
    #             or ("qualifier" in stage)
    #             or ("eliminator" in stage)
    #     ):
    #         is_playoff = 1
    #     else:
    #         is_playoff = 0
    #
    #     season = data["info"]["season"]
    #     player_id_map = data["info"]["registry"]["people"]
    #     player_name_to_mapping_name = {}
    #     for player_name, player_cricsheet_id in player_id_map.items():
    #         player_name_to_mapping_name[player_name] = player_info.get(player_cricsheet_id)['name']
    #
    #     # Make it title case for player names
    #     player_id_map = {k.title(): v for k, v in player_id_map.items()}
    #
    #     ball_by_ball = []
    #
    #     for idx, innings in enumerate(data["innings"]):
    #         if idx >= 2:
    #             continue
    #         bat_pos_counter = 0
    #         bowl_pos_counter = 0
    #         bat_pos_dict = dict()
    #         bowl_pos_dict = dict()
    #         # To calculate the target of match
    #         if idx == 1:
    #             try:
    #                 target = innings["target"]["runs"]
    #             except:
    #                 target = np.nan
    #
    #             team_short_name_x, team_short_name_y = team_short_name_y, team_short_name_x
    #         else:
    #             target = np.nan
    #
    #         overs = innings["overs"]
    #
    #         for over in overs:
    #             over_number = over["over"]
    #             ball_no = 1
    #             # Create overwise ball by ball frame for each ball
    #             for ball_data in over["deliveries"]:
    #                 ball_dict = {}
    #                 ball_dict["over_number"] = over_number + 1
    #                 ball_dict["ball_number"] = ball_no
    #                 ball_no = ball_no + 1
    #                 ball_dict["is_four"] = 0
    #                 ball_dict["is_six"] = 0
    #                 ball_dict["is_extra"] = 0
    #                 ball_dict["is_wide"] = 0
    #                 ball_dict["is_no_ball"] = 0
    #                 ball_dict["is_dot_ball"] = 0
    #                 ball_dict["innings"] = idx + 1
    #                 ball_dict["is_wicket"] = 0
    #                 ball_dict["is_leg_bye"] = 0
    #                 ball_dict["is_bye"] = 0
    #                 ball_dict["is_bowler_wicket"] = 0
    #                 ball_dict["is_one"] = 0
    #                 ball_dict["is_two"] = 0
    #                 ball_dict["is_three"] = 0
    #                 ball_dict["match_date"] = date
    #                 ball_dict["season"] = season
    #                 ball_dict["stadium_name"] = stadium_name
    #                 ball_dict["winning_team"] = winning_team
    #                 ball_dict["toss_team"] = toss_team
    #                 ball_dict["toss_decision"] = toss_decision
    #                 ball_dict["match_result"] = match_result
    #                 ball_dict["target_runs"] = target
    #                 ball_dict["batsman"] = ball_data["batter"].title()
    #                 # batsman_id will have player_id
    #                 ball_dict["batsman_id"] = player_id_map.get(ball_dict["batsman"], -1)
    #                 # ball_dict["stage"] = stage
    #                 ball_dict["is_playoff"] = is_playoff
    #                 ball_dict["non_striker"] = ball_data["non_striker"].title()
    #                 # non_striker_id will have player_id
    #                 ball_dict["non_striker_id"] = player_id_map.get(ball_dict["non_striker"], -1)
    #                 ball_dict["bowler"] = ball_data["bowler"].title()
    #                 # bowler_id will have player_id
    #                 ball_dict["bowler_id"] = player_id_map.get(ball_dict["bowler"], -1)
    #                 if ball_dict["bowler_id"] in bowl_pos_dict:
    #                     ball_dict["bowling_order"] = bowl_pos_dict[ball_dict["bowler_id"]]
    #                 else:
    #                     ball_dict["bowling_order"] = bowl_pos_counter + 1
    #                     bowl_pos_dict[ball_dict["bowler_id"]] = bowl_pos_counter + 1
    #                     bowl_pos_counter += 1
    #                 if ball_dict["batsman_id"] in bat_pos_dict:
    #                     ball_dict["batting_position"] = bat_pos_dict[ball_dict["batsman_id"]]
    #                 else:
    #                     ball_dict["batting_position"] = bat_pos_counter + 1
    #                     bat_pos_dict[ball_dict["batsman_id"]] = bat_pos_counter + 1
    #                     bat_pos_counter += 1
    #
    #                 if "runs" in ball_data:
    #                     runs = ball_data["runs"]
    #                     ball_dict["runs"] = runs["total"]
    #                     ball_dict["ball_runs"] = runs["batter"]
    #                     ball_dict["extras"] = runs["extras"]
    #                     if ball_dict["extras"] > 0:
    #                         ball_dict["is_extra"] = 1
    #                     if ball_dict["ball_runs"] == 0 and ball_dict["extras"] == 0:
    #                         ball_dict["is_dot_ball"] = 1
    #                     else:
    #                         if "non_boundary" not in runs:
    #                             if ball_dict["ball_runs"] == 4:
    #                                 ball_dict["is_four"] = 1
    #                             elif ball_dict["ball_runs"] == 6:
    #                                 ball_dict["is_six"] = 1
    #                         if ball_dict["ball_runs"] == 1:
    #                             ball_dict["is_one"] = 1
    #                         elif ball_dict["ball_runs"] == 2:
    #                             ball_dict["is_two"] = 1
    #                         elif ball_dict["ball_runs"] == 3:
    #                             ball_dict["is_three"] = 1
    #
    #                 if "extras" in ball_data:
    #                     extras = ball_data["extras"]
    #                     if "noballs" in extras:
    #                         ball_no -= 1
    #                         ball_dict["is_no_ball"] = 1
    #                     elif "legbyes" in extras:
    #                         ball_dict["is_leg_bye"] = 1
    #                     elif "byes" in extras:
    #                         ball_dict["is_bye"] = 1
    #                     elif "wides" in extras:
    #                         ball_dict["is_wide"] = 1
    #                         ball_no -= 1
    #
    #                 ball_dict["out_batsman_id"] = -1
    #                 ball_dict["out_batsman"] = np.nan
    #                 ball_dict["out_desc"] = ''
    #                 if "wickets" in ball_data:
    #                     wickets = ball_data["wickets"][0]
    #                     ball_dict["is_wicket"] = 1
    #                     ball_dict["is_bowler_wicket"] = 1
    #                     ball_dict["wicket_type"] = wickets["kind"]
    #                     ball_dict["out_batsman"] = wickets["player_out"].title()
    #                     ball_dict["out_batsman_id"] = player_id_map.get(ball_dict["out_batsman"], -1)
    #
    #                     if wickets["kind"] in [
    #                         "run out",
    #                         "retired hurt",
    #                         "obstructing the field",
    #                         "handled the ball",
    #                     ]:
    #                         ball_dict["is_bowler_wicket"] = 0
    #                     if ball_dict["non_striker_id"] not in bat_pos_dict:
    #                         bat_pos_dict[ball_dict["non_striker_id"]] = bat_pos_counter + 1
    #                         bat_pos_counter += 1
    #                     if wickets["kind"] == "caught":
    #                         ball_dict["out_desc"] = f"c {wickets['fielders'][0].get('name')} b {ball_data['bowler']}"
    #                     elif wickets["kind"] == "stumped":
    #                         ball_dict["out_desc"] = f"st {wickets['fielders'][0].get('name')} b {ball_data['bowler']}"
    #                     elif wickets["kind"] == "caught and bowled":
    #                         ball_dict["out_desc"] = f"c & b {ball_data['bowler']}"
    #                     elif wickets["kind"] == "bowled":
    #                         ball_dict["out_desc"] = f"b {ball_data['bowler']}"
    #                     elif wickets["kind"] == "run out":
    #                         ball_dict["out_desc"] = f"run out"
    #                     elif wickets["kind"] == "lbw":
    #                         ball_dict["out_desc"] = f"lbw b {ball_data['bowler']}"
    #                 ball_dict["batting_team"] = team_short_name_x.upper()
    #                 ball_dict["team_short_name_x"] = team_short_name_x.upper()
    #                 ball_dict["bowling_team"] = team_short_name_y.upper()
    #                 ball_dict["team_short_name_y"] = team_short_name_y.upper()
    #                 ball_dict["over_text"] = float(
    #                     str(ball_dict["over_number"] - 1) + "." + str(ball_dict["ball_number"])
    #                 )
    #                 ball_by_ball.append(ball_dict)
    #
    #     ball_by_ball = pd.DataFrame((ball_by_ball))
    #
    #     ball_by_ball["batsman_map"] = ball_by_ball["batsman_id"].apply(
    #         lambda x: player_info.get(x, {}).get("name", "nan")
    #     )
    #     ball_by_ball["batsman_id_map"] = ball_by_ball["batsman_id"]
    #     ball_by_ball["batsman_source_id_map"] = ball_by_ball["batsman_id"]
    #
    #     ball_by_ball["striker_batting_type"] = ball_by_ball["batsman_id"].apply(
    #         lambda x: player_info.get(x, {}).get("striker_batting_type", "nan")
    #     )
    #
    #     ball_by_ball["non_striker_map"] = ball_by_ball["non_striker_id"].apply(
    #         lambda x: player_info.get(x, {}).get("name", "nan")
    #     )
    #     ball_by_ball["non_striker_id_map"] = ball_by_ball["non_striker_id"]
    #     ball_by_ball["non_striker_source_id_map"] = ball_by_ball["non_striker_id"]
    #
    #     ball_by_ball["bowler_map"] = ball_by_ball["bowler_id"].apply(
    #         lambda x: player_info.get(x, {}).get("name", "nan")
    #     )
    #     ball_by_ball["bowler_id_map"] = ball_by_ball["bowler_id"]
    #
    #     ball_by_ball["bowler_source_id_map"] = ball_by_ball["bowler_id"]
    #     ball_by_ball["bowler_sub_type"] = ball_by_ball["bowler_id"].apply(
    #         lambda x: player_info.get(x, {}).get("bowler_sub_type", "nan")
    #     )
    #
    #     ball_by_ball["out_batsman_map"] = ball_by_ball["out_batsman_id"].apply(
    #         lambda x: player_info.get(x, {}).get("name", "nan")
    #     )
    #     ball_by_ball["out_batsman_id_map"] = ball_by_ball["out_batsman_id"]
    #     ball_by_ball["out_batsman_source_id_map"] = ball_by_ball["out_batsman_id"]
    #
    #     ball_by_ball["bowl_major_type"] = ball_by_ball["bowler_sub_type"].map(
    #         bowl_major_type_dict
    #     )
    #
    #     ball_by_ball["batsman"] = ball_by_ball["batsman_map"]
    #     ball_by_ball["batsman_id"] = ball_by_ball["batsman_id_map"]
    #     ball_by_ball["batsman_source_id"] = ball_by_ball["batsman_source_id_map"]
    #
    #     ball_by_ball["bowler"] = ball_by_ball["bowler_map"]
    #     ball_by_ball["bowler_id"] = ball_by_ball["bowler_id_map"]
    #     ball_by_ball["bowler_source_id"] = ball_by_ball["bowler_source_id_map"]
    #
    #     ball_by_ball["non_striker"] = ball_by_ball["non_striker_map"]
    #     ball_by_ball["non_striker_id"] = ball_by_ball["non_striker_id_map"]
    #     ball_by_ball["non_striker_source_id"] = ball_by_ball["non_striker_source_id_map"]
    #
    #     ball_by_ball["out_batsman"] = ball_by_ball["out_batsman_map"]
    #     ball_by_ball["out_batsman_id"] = ball_by_ball["batsman_id_map"]
    #     ball_by_ball["out_batsman_source_id"] = ball_by_ball["out_batsman_source_id_map"]
    #     remove_cols = [
    #         "batsman_map",
    #         "batsman_id_map",
    #         "batsman_source_id_map",
    #         "non_striker_map",
    #         "non_striker_id_map",
    #         "non_striker_source_id_map",
    #         "bowler_map",
    #         "bowler_id_map",
    #         "bowler_source_id_map",
    #         "out_batsman_map",
    #         "out_batsman_id_map",
    #         "out_batsman_source_id_map",
    #         "batsman_source_id",
    #         "bowler_source_id",
    #         "non_striker_source_id",
    #         "out_batsman_source_id",
    #     ]
    #     ball_by_ball = ball_by_ball.drop(remove_cols, axis=1)
    #     ball_by_ball = ball_by_ball.applymap(lambda x: np.nan if x == "nan" else x)
    #     ball_by_ball['out_batsman_id'] = np.where(ball_by_ball['out_batsman_id'] == -1, np.nan,
    #                                               ball_by_ball['out_batsman_id'])
    #     # Key to add if it is not already there
    #     key_columns = [
    #         'over_number',
    #         'ball_number',
    #         'is_four',
    #         'is_six',
    #         'is_extra',
    #         'is_batsman',
    #         'is_bowler',
    #         'is_wicket_keeper',
    #         'is_wide',
    #         'is_no_ball',
    #         'is_dot_ball',
    #         'innings',
    #         'is_wicket',
    #         'is_leg_bye',
    #         'is_bye',
    #         'is_bowler_wicket',
    #         'is_one',
    #         'is_two',
    #         'is_three',
    #         'match_name',
    #         'match_date',
    #         'season',
    #         'stadium_name',
    #         'winning_team',
    #         'toss_team',
    #         'toss_decision',
    #         'match_result',
    #         'target_runs',
    #         'batsman',
    #         'batsman_id',
    #         'is_playoff',
    #         'non_striker',
    #         'non_striker_id',
    #         'bowler',
    #         'bowler_id',
    #         'bowling_order',
    #         'batting_position',
    #         'runs',
    #         'ball_runs',
    #         'extras',
    #         'out_batsman_id',
    #         'out_batsman',
    #         'out_desc',
    #         'batting_team',
    #         'team_short_name_x',
    #         'bowling_team',
    #         'team_short_name_y',
    #         'over_text',
    #         'wicket_type',
    #         'striker_batting_type',
    #         'bowler_sub_type',
    #         'bowl_major_type'
    #     ]
    #     for column in key_columns:
    #         if column not in ball_by_ball.columns:
    #             if column == "is_batsman":
    #                 ball_by_ball[column] = -1
    #             elif column == "is_bowler":
    #                 ball_by_ball[column] = -1
    #             elif column == "is_wicket_keeper":
    #                 ball_by_ball[column] = -1
    #             elif column == "bowler_sub_type":
    #                 ball_by_ball[column] = ""
    #             elif column == "bowl_major_type":
    #                 ball_by_ball[column] = ""
    #             elif column == "striker_batting_type":
    #                 ball_by_ball[column] = ""
    #             else:
    #                 ball_by_ball[column] = pd.NA
    #
    #     # Replace player name with the name from player mapping table
    #     ball_by_ball['batsman'] = ball_by_ball['batsman'].replace(player_name_to_mapping_name)
    #     ball_by_ball['non_striker'] = ball_by_ball['non_striker'].replace(player_name_to_mapping_name)
    #     ball_by_ball['bowler'] = ball_by_ball['bowler'].replace(player_name_to_mapping_name)
    #     ball_by_ball['out_batsman'] = ball_by_ball['out_batsman'].replace(player_name_to_mapping_name)
    #     ball_by_ball['match_name'] = getInitials(team_short_name_x).upper() + "VS" + getInitials(
    #         team_short_name_y).upper() + match_number
    #     registry = data["info"]["registry"]["people"]
    #     squad = {}
    #     for key, value in data["info"]["players"].items():
    #         squad[key.upper()] = [registry[player] for player in value]
    #     return ball_by_ball, squad
    #
    # def test_parser(self, data, player_info, match_id):
    #     if not ("info" in data and "innings" in data):
    #         raise Exception("There is an issue with JSON data input")
    #
    #     date = data["info"]["dates"][0]
    #     date_object = datetime.strptime(date, '%Y-%m-%d')
    #     match_number = date_object.strftime('%d%m%Y')
    #     match_name = (("VS".join(data["info"]["teams"])).replace(" ", "_") + str(date)).replace("-", "")
    #     match_name = match_name + match_number
    #     winning_team = (data["info"]["outcome"].get("winner", "None")).upper()
    #     toss_team = (data["info"]["toss"].get("winner")).upper()
    #     toss_decision = f'{data["info"]["toss"].get("winner")} Won The Toss elected to {data["info"]["toss"].get("decision")}'
    #
    #     # Parse match Result
    #     if data["info"]["outcome"].get("result") and data["info"]["outcome"].get("result") == "no result":
    #         match_result = "No Match Result"
    #         NoResultHandler(match_result, match_id, match_name).log_ingestion_table()
    #     elif not data["info"]["outcome"].get("by"):
    #         if data["info"]["outcome"].get("method"):
    #             match_result = f'{data["info"]["outcome"].get("winner")} Won By {data["info"]["outcome"].get("winner")}'
    #         else:
    #             match_result = data["info"]["outcome"]["result"]
    #     else:
    #         if data["info"]["outcome"]["by"].get("runs"):
    #             match_result = f'{data["info"]["outcome"].get("winner")} Won By {data["info"]["outcome"]["by"].get("runs")} Runs'
    #         else:
    #             match_result = f'{data["info"]["outcome"].get("winner")} Won By {data["info"]["outcome"]["by"].get("wickets")} Wickets'
    #
    #     # Remove everything after stadium name. i.e - Country or Location of stadium
    #     stadium_name = data["info"]["venue"].split(",", 1)[0]
    #     if len(data["innings"]) >= 2:
    #         team_short_name_x = (data["innings"][0]["team"]).upper()
    #         team_short_name_y = (data["innings"][1]["team"]).upper()
    #     elif len(data["innings"]) == 1:
    #         team_short_name_x = (data["innings"][0]["team"]).upper()
    #         team_short_name_y = None
    #         teams = data["info"]["teams"]
    #         for t in teams:
    #             if t.upper() == team_short_name_x:
    #                 continue
    #             else:
    #                 team_short_name_y = t.upper()
    #
    #     # Check if match is of playoffs
    #     try:
    #         stage = data["info"]["event"]["stage"]
    #     except:
    #         stage = "None"
    #     stage = stage.lower()
    #     if (
    #             ("final" in stage)
    #             or ("play" in stage and "off" in stage)
    #             or ("qualifier" in stage)
    #             or ("eliminator" in stage)
    #     ):
    #         is_playoff = 1
    #     else:
    #         is_playoff = 0
    #
    #     season = data["info"]["season"]
    #     player_id_map = data["info"]["registry"]["people"]
    #     player_name_to_mapping_name = {}
    #     for player_name, player_cricsheet_id in player_id_map.items():
    #         player_name_to_mapping_name[player_name] = player_info.get(player_cricsheet_id)['name']
    #
    #     # Make it title case for player names
    #     player_id_map = {k.title(): v for k, v in player_id_map.items()}
    #
    #     ball_by_ball = []
    #
    #     for idx, innings in enumerate(data["innings"]):
    #         bat_pos_counter = 0
    #         bowl_pos_counter = 0
    #         bat_pos_dict = dict()
    #         bowl_pos_dict = dict()
    #         # To calculate the target of match
    #         if idx == 1:
    #             try:
    #                 #TODO: what is target run in test??
    #                 target = innings["target"]["runs"]
    #             except:
    #                 target = np.nan
    #
    #             team_short_name_x, team_short_name_y = team_short_name_y, team_short_name_x
    #         else:
    #             target = np.nan
    #
    #         overs = innings["overs"]
    #
    #         for over in overs:
    #             over_number = over["over"]
    #             ball_no = 1
    #             # Create overwise ball by ball frame for each ball
    #             for ball_data in over["deliveries"]:
    #                 ball_dict = {}
    #                 ball_dict["over_number"] = over_number + 1
    #                 ball_dict["ball_number"] = ball_no
    #                 ball_no = ball_no + 1
    #                 ball_dict["is_four"] = 0
    #                 ball_dict["is_six"] = 0
    #                 ball_dict["is_extra"] = 0
    #                 ball_dict["is_wide"] = 0
    #                 ball_dict["is_no_ball"] = 0
    #                 ball_dict["is_dot_ball"] = 0
    #                 ball_dict["innings"] = idx + 1
    #                 ball_dict["is_wicket"] = 0
    #                 ball_dict["is_leg_bye"] = 0
    #                 ball_dict["is_bye"] = 0
    #                 ball_dict["is_bowler_wicket"] = 0
    #                 ball_dict["is_one"] = 0
    #                 ball_dict["is_two"] = 0
    #                 ball_dict["is_three"] = 0
    #                 ball_dict["match_date"] = date
    #                 ball_dict["season"] = season
    #                 ball_dict["stadium_name"] = stadium_name
    #                 ball_dict["winning_team"] = winning_team
    #                 ball_dict["toss_team"] = toss_team
    #                 ball_dict["toss_decision"] = toss_decision
    #                 ball_dict["match_result"] = match_result
    #                 ball_dict["target_runs"] = target
    #                 ball_dict["batsman"] = ball_data["batter"].title()
    #                 # batsman_id will have player_id
    #                 ball_dict["batsman_id"] = player_id_map.get(ball_dict["batsman"], -1)
    #                 # ball_dict["stage"] = stage
    #                 ball_dict["is_playoff"] = is_playoff
    #                 ball_dict["non_striker"] = ball_data["non_striker"].title()
    #                 # non_striker_id will have player_id
    #                 ball_dict["non_striker_id"] = player_id_map.get(ball_dict["non_striker"], -1)
    #                 ball_dict["bowler"] = ball_data["bowler"].title()
    #                 # bowler_id will have player_id
    #                 ball_dict["bowler_id"] = player_id_map.get(ball_dict["bowler"], -1)
    #                 if ball_dict["bowler_id"] in bowl_pos_dict:
    #                     ball_dict["bowling_order"] = bowl_pos_dict[ball_dict["bowler_id"]]
    #                 else:
    #                     ball_dict["bowling_order"] = bowl_pos_counter + 1
    #                     bowl_pos_dict[ball_dict["bowler_id"]] = bowl_pos_counter + 1
    #                     bowl_pos_counter += 1
    #                 if ball_dict["batsman_id"] in bat_pos_dict:
    #                     ball_dict["batting_position"] = bat_pos_dict[ball_dict["batsman_id"]]
    #                 else:
    #                     ball_dict["batting_position"] = bat_pos_counter + 1
    #                     bat_pos_dict[ball_dict["batsman_id"]] = bat_pos_counter + 1
    #                     bat_pos_counter += 1
    #
    #                 if "runs" in ball_data:
    #                     runs = ball_data["runs"]
    #                     ball_dict["runs"] = runs["total"]
    #                     ball_dict["ball_runs"] = runs["batter"]
    #                     ball_dict["extras"] = runs["extras"]
    #                     if ball_dict["extras"] > 0:
    #                         ball_dict["is_extra"] = 1
    #                     if ball_dict["ball_runs"] == 0 and ball_dict["extras"] == 0:
    #                         ball_dict["is_dot_ball"] = 1
    #                     else:
    #                         if "non_boundary" not in runs:
    #                             if ball_dict["ball_runs"] == 4:
    #                                 ball_dict["is_four"] = 1
    #                             elif ball_dict["ball_runs"] == 6:
    #                                 ball_dict["is_six"] = 1
    #                         if ball_dict["ball_runs"] == 1:
    #                             ball_dict["is_one"] = 1
    #                         elif ball_dict["ball_runs"] == 2:
    #                             ball_dict["is_two"] = 1
    #                         elif ball_dict["ball_runs"] == 3:
    #                             ball_dict["is_three"] = 1
    #
    #                 if "extras" in ball_data:
    #                     extras = ball_data["extras"]
    #                     if "noballs" in extras:
    #                         ball_no -= 1
    #                         ball_dict["is_no_ball"] = 1
    #                     elif "legbyes" in extras:
    #                         ball_dict["is_leg_bye"] = 1
    #                     elif "byes" in extras:
    #                         ball_dict["is_bye"] = 1
    #                     elif "wides" in extras:
    #                         ball_dict["is_wide"] = 1
    #                         ball_no -= 1
    #
    #                 ball_dict["out_batsman_id"] = -1
    #                 ball_dict["out_batsman"] = np.nan
    #                 ball_dict["out_desc"] = ''
    #                 if "wickets" in ball_data:
    #                     wickets = ball_data["wickets"][0]
    #                     ball_dict["is_wicket"] = 1
    #                     ball_dict["is_bowler_wicket"] = 1
    #                     ball_dict["wicket_type"] = wickets["kind"]
    #                     ball_dict["out_batsman"] = wickets["player_out"].title()
    #                     ball_dict["out_batsman_id"] = player_id_map.get(ball_dict["out_batsman"], -1)
    #
    #                     if wickets["kind"] in [
    #                         "run out",
    #                         "retired hurt",
    #                         "obstructing the field",
    #                         "handled the ball",
    #                     ]:
    #                         ball_dict["is_bowler_wicket"] = 0
    #                     if ball_dict["non_striker_id"] not in bat_pos_dict:
    #                         bat_pos_dict[ball_dict["non_striker_id"]] = bat_pos_counter + 1
    #                         bat_pos_counter += 1
    #                     if wickets["kind"] == "caught":
    #                         ball_dict["out_desc"] = f"c {wickets['fielders'][0].get('name')} b {ball_data['bowler']}"
    #                     elif wickets["kind"] == "stumped":
    #                         ball_dict["out_desc"] = f"st {wickets['fielders'][0].get('name')} b {ball_data['bowler']}"
    #                     elif wickets["kind"] == "caught and bowled":
    #                         ball_dict["out_desc"] = f"c & b {ball_data['bowler']}"
    #                     elif wickets["kind"] == "bowled":
    #                         ball_dict["out_desc"] = f"b {ball_data['bowler']}"
    #                     elif wickets["kind"] == "run out":
    #                         ball_dict["out_desc"] = f"run out"
    #                     elif wickets["kind"] == "lbw":
    #                         ball_dict["out_desc"] = f"lbw b {ball_data['bowler']}"
    #                 ball_dict["batting_team"] = team_short_name_x.upper()
    #                 ball_dict["team_short_name_x"] = team_short_name_x.upper()
    #                 ball_dict["bowling_team"] = team_short_name_y.upper()
    #                 ball_dict["team_short_name_y"] = team_short_name_y.upper()
    #                 ball_dict["over_text"] = float(
    #                     str(ball_dict["over_number"] - 1) + "." + str(ball_dict["ball_number"])
    #                 )
    #                 ball_by_ball.append(ball_dict)
    #
    #     ball_by_ball = pd.DataFrame((ball_by_ball))
    #
    #     ball_by_ball["batsman_map"] = ball_by_ball["batsman_id"].apply(
    #         lambda x: player_info.get(x, {}).get("name", "nan")
    #     )
    #     ball_by_ball["batsman_id_map"] = ball_by_ball["batsman_id"]
    #     ball_by_ball["batsman_source_id_map"] = ball_by_ball["batsman_id"]
    #
    #     ball_by_ball["striker_batting_type"] = ball_by_ball["batsman_id"].apply(
    #         lambda x: player_info.get(x, {}).get("striker_batting_type", "nan")
    #     )
    #
    #     ball_by_ball["non_striker_map"] = ball_by_ball["non_striker_id"].apply(
    #         lambda x: player_info.get(x, {}).get("name", "nan")
    #     )
    #     ball_by_ball["non_striker_id_map"] = ball_by_ball["non_striker_id"]
    #     ball_by_ball["non_striker_source_id_map"] = ball_by_ball["non_striker_id"]
    #
    #     ball_by_ball["bowler_map"] = ball_by_ball["bowler_id"].apply(
    #         lambda x: player_info.get(x, {}).get("name", "nan")
    #     )
    #     ball_by_ball["bowler_id_map"] = ball_by_ball["bowler_id"]
    #
    #     ball_by_ball["bowler_source_id_map"] = ball_by_ball["bowler_id"]
    #     ball_by_ball["bowler_sub_type"] = ball_by_ball["bowler_id"].apply(
    #         lambda x: player_info.get(x, {}).get("bowler_sub_type", "nan")
    #     )
    #
    #     ball_by_ball["out_batsman_map"] = ball_by_ball["out_batsman_id"].apply(
    #         lambda x: player_info.get(x, {}).get("name", "nan")
    #     )
    #     ball_by_ball["out_batsman_id_map"] = ball_by_ball["out_batsman_id"]
    #     ball_by_ball["out_batsman_source_id_map"] = ball_by_ball["out_batsman_id"]
    #
    #     ball_by_ball["bowl_major_type"] = ball_by_ball["bowler_sub_type"].map(
    #         bowl_major_type_dict
    #     )
    #
    #     ball_by_ball["batsman"] = ball_by_ball["batsman_map"]
    #     ball_by_ball["batsman_id"] = ball_by_ball["batsman_id_map"]
    #     ball_by_ball["batsman_source_id"] = ball_by_ball["batsman_source_id_map"]
    #
    #     ball_by_ball["bowler"] = ball_by_ball["bowler_map"]
    #     ball_by_ball["bowler_id"] = ball_by_ball["bowler_id_map"]
    #     ball_by_ball["bowler_source_id"] = ball_by_ball["bowler_source_id_map"]
    #
    #     ball_by_ball["non_striker"] = ball_by_ball["non_striker_map"]
    #     ball_by_ball["non_striker_id"] = ball_by_ball["non_striker_id_map"]
    #     ball_by_ball["non_striker_source_id"] = ball_by_ball["non_striker_source_id_map"]
    #
    #     ball_by_ball["out_batsman"] = ball_by_ball["out_batsman_map"]
    #     ball_by_ball["out_batsman_id"] = ball_by_ball["batsman_id_map"]
    #     ball_by_ball["out_batsman_source_id"] = ball_by_ball["out_batsman_source_id_map"]
    #     remove_cols = [
    #         "batsman_map",
    #         "batsman_id_map",
    #         "batsman_source_id_map",
    #         "non_striker_map",
    #         "non_striker_id_map",
    #         "non_striker_source_id_map",
    #         "bowler_map",
    #         "bowler_id_map",
    #         "bowler_source_id_map",
    #         "out_batsman_map",
    #         "out_batsman_id_map",
    #         "out_batsman_source_id_map",
    #         "batsman_source_id",
    #         "bowler_source_id",
    #         "non_striker_source_id",
    #         "out_batsman_source_id",
    #     ]
    #     ball_by_ball = ball_by_ball.drop(remove_cols, axis=1)
    #     ball_by_ball = ball_by_ball.applymap(lambda x: np.nan if x == "nan" else x)
    #     ball_by_ball['out_batsman_id'] = np.where(ball_by_ball['out_batsman_id'] == -1, np.nan,
    #                                               ball_by_ball['out_batsman_id'])
    #     # Key to add if it is not already there
    #     key_columns = [
    #         'over_number',
    #         'ball_number',
    #         'is_four',
    #         'is_six',
    #         'is_extra',
    #         'is_batsman',
    #         'is_bowler',
    #         'is_wicket_keeper',
    #         'is_wide',
    #         'is_no_ball',
    #         'is_dot_ball',
    #         'innings',
    #         'is_wicket',
    #         'is_leg_bye',
    #         'is_bye',
    #         'is_bowler_wicket',
    #         'is_one',
    #         'is_two',
    #         'is_three',
    #         'match_name',
    #         'match_date',
    #         'season',
    #         'stadium_name',
    #         'winning_team',
    #         'toss_team',
    #         'toss_decision',
    #         'match_result',
    #         'target_runs',
    #         'batsman',
    #         'batsman_id',
    #         'is_playoff',
    #         'non_striker',
    #         'non_striker_id',
    #         'bowler',
    #         'bowler_id',
    #         'bowling_order',
    #         'batting_position',
    #         'runs',
    #         'ball_runs',
    #         'extras',
    #         'out_batsman_id',
    #         'out_batsman',
    #         'out_desc',
    #         'batting_team',
    #         'team_short_name_x',
    #         'bowling_team',
    #         'team_short_name_y',
    #         'over_text',
    #         'wicket_type',
    #         'striker_batting_type',
    #         'bowler_sub_type',
    #         'bowl_major_type'
    #     ]
    #     for column in key_columns:
    #         if column not in ball_by_ball.columns:
    #             if column == "is_batsman":
    #                 ball_by_ball[column] = -1
    #             elif column == "is_bowler":
    #                 ball_by_ball[column] = -1
    #             elif column == "is_wicket_keeper":
    #                 ball_by_ball[column] = -1
    #             elif column == "bowler_sub_type":
    #                 ball_by_ball[column] = ""
    #             elif column == "bowl_major_type":
    #                 ball_by_ball[column] = ""
    #             elif column == "striker_batting_type":
    #                 ball_by_ball[column] = ""
    #             else:
    #                 ball_by_ball[column] = pd.NA
    #
    #     # Replace player name with the name from player mapping table
    #     ball_by_ball['batsman'] = ball_by_ball['batsman'].replace(player_name_to_mapping_name)
    #     ball_by_ball['non_striker'] = ball_by_ball['non_striker'].replace(player_name_to_mapping_name)
    #     ball_by_ball['bowler'] = ball_by_ball['bowler'].replace(player_name_to_mapping_name)
    #     ball_by_ball['out_batsman'] = ball_by_ball['out_batsman'].replace(player_name_to_mapping_name)
    #     ball_by_ball['match_name'] = getInitials(team_short_name_x).upper() + "VS" + getInitials(
    #         team_short_name_y).upper() + match_number
    #     registry = data["info"]["registry"]["people"]
    #     squad = {}
    #     for key, value in data["info"]["players"].items():
    #         squad[key.upper()] = [registry[player] for player in value]
    #     return ball_by_ball, squad

    def t20_parser(self, data, player_info, match_id):
        if not ("info" in data and "innings" in data):
            raise Exception("There is an issue with JSON data input")

        date = data["info"]["dates"][0]
        date_object = datetime.strptime(date, '%Y-%m-%d')
        match_number = date_object.strftime('%d%m%Y')
        match_name = (("VS".join(data["info"]["teams"])).replace(" ", "_") + str(date)).replace("-", "")
        match_name = match_name + match_number
        winning_team = (data["info"]["outcome"].get("winner", "None")).upper()
        toss_team = (data["info"]["toss"].get("winner")).upper()
        toss_decision = f'{data["info"]["toss"].get("winner")} Won The Toss elected to {data["info"]["toss"].get("decision")}'

        # Parse match Result
        if data["info"]["outcome"].get("result") and data["info"]["outcome"].get("result") == "no result":
            match_result = "No Match Result"
            NoResultHandler(match_result, match_id, match_name).log_ingestion_table()
        elif not data["info"]["outcome"].get("by"):
            if data["info"]["outcome"].get("method"):
                match_result = f'{data["info"]["outcome"].get("winner")} Won By {data["info"]["outcome"].get("winner")}'
            else:
                match_result = data["info"]["outcome"]["result"]
        else:
            if data["info"]["outcome"]["by"].get("runs"):
                match_result = f'{data["info"]["outcome"].get("winner")} Won By {data["info"]["outcome"]["by"].get("runs")} Runs'
            else:
                match_result = f'{data["info"]["outcome"].get("winner")} Won By {data["info"]["outcome"]["by"].get("wickets")} Wickets'

        # Remove everything after stadium name. i.e - Country or Location of stadium
        stadium_name = data["info"]["venue"].split(",", 1)[0]
        if len(data["innings"]) >= 2:
            team_short_name_x = (data["innings"][0]["team"]).upper()
            team_short_name_y = (data["innings"][1]["team"]).upper()
        elif len(data["innings"]) == 1:
            team_short_name_x = (data["innings"][0]["team"]).upper()

            short_name_y = None
            teams = data["info"]["teams"]
            for t in teams:
                if t.upper() == team_short_name_x:
                    continue
                else:
                    team_short_name_y = t.upper()

        # Check if match is of playoffs
        try:
            stage = data["info"]["event"]["stage"]
        except:
            stage = "None"
        stage = stage.lower()
        if (
                ("final" in stage)
                or ("play" in stage and "off" in stage)
                or ("qualifier" in stage)
                or ("eliminator" in stage)
        ):
            is_playoff = 1
        else:
            is_playoff = 0

        season = data["info"]["season"]
        player_id_map = data["info"]["registry"]["people"]
        teams_players = data["info"]["players"]
        playing_22 = []
        players_id = {}
        for team_name, players in teams_players.items():
            playing_22.extend(players)
        for player in playing_22:
            players_id[player] = player_id_map.get(player)
        player_name_to_mapping_name = {}
        for player_name, player_cricsheet_id in players_id.items():
            player_name_to_mapping_name[player_name] = player_info.get(player_cricsheet_id)['name']
        # Make it title case for player names
        player_id_map = {k.title(): v for k, v in player_id_map.items()}

        ball_by_ball = []

        for idx, innings in enumerate(data["innings"]):
            if idx >= 2:
                continue
            bat_pos_counter = 0
            bowl_pos_counter = 0
            bat_pos_dict = dict()
            bowl_pos_dict = dict()
            # To calculate the target of match
            if idx == 1:
                try:
                    target = innings["target"]["runs"]
                except:
                    target = np.nan

                team_short_name_x, team_short_name_y = team_short_name_y, team_short_name_x
            else:
                target = np.nan

            overs = innings["overs"]

            for over in overs:
                over_number = over["over"]
                ball_no = 1
                # Create overwise ball by ball frame for each ball
                for ball_data in over["deliveries"]:
                    ball_dict = {}
                    ball_dict["over_number"] = over_number + 1
                    ball_dict["ball_number"] = ball_no
                    ball_no = ball_no + 1
                    ball_dict["is_four"] = 0
                    ball_dict["is_six"] = 0
                    ball_dict["is_extra"] = 0
                    ball_dict["is_wide"] = 0
                    ball_dict["is_no_ball"] = 0
                    ball_dict["is_dot_ball"] = 0
                    ball_dict["innings"] = idx + 1
                    ball_dict["is_wicket"] = 0
                    ball_dict["is_leg_bye"] = 0
                    ball_dict["is_bye"] = 0
                    ball_dict["is_bowler_wicket"] = 0
                    ball_dict["is_one"] = 0
                    ball_dict["is_two"] = 0
                    ball_dict["is_three"] = 0
                    ball_dict["match_date"] = date
                    ball_dict["season"] = season
                    ball_dict["stadium_name"] = stadium_name
                    ball_dict["winning_team"] = winning_team
                    ball_dict["toss_team"] = toss_team
                    ball_dict["toss_decision"] = toss_decision
                    ball_dict["match_result"] = match_result
                    ball_dict["target_runs"] = target
                    ball_dict["batsman"] = ball_data["batter"].title()
                    # batsman_id will have player_id
                    ball_dict["batsman_id"] = player_id_map.get(ball_dict["batsman"], -1)
                    # ball_dict["stage"] = stage
                    ball_dict["is_playoff"] = is_playoff
                    ball_dict["non_striker"] = ball_data["non_striker"].title()
                    # non_striker_id will have player_id
                    ball_dict["non_striker_id"] = player_id_map.get(ball_dict["non_striker"], -1)
                    ball_dict["bowler"] = ball_data["bowler"].title()
                    # bowler_id will have player_id
                    ball_dict["bowler_id"] = player_id_map.get(ball_dict["bowler"], -1)
                    if ball_dict["bowler_id"] in bowl_pos_dict:
                        ball_dict["bowling_order"] = bowl_pos_dict[ball_dict["bowler_id"]]
                    else:
                        ball_dict["bowling_order"] = bowl_pos_counter + 1
                        bowl_pos_dict[ball_dict["bowler_id"]] = bowl_pos_counter + 1
                        bowl_pos_counter += 1
                    if ball_dict["batsman_id"] in bat_pos_dict:
                        ball_dict["batting_position"] = bat_pos_dict[ball_dict["batsman_id"]]
                    else:
                        ball_dict["batting_position"] = bat_pos_counter + 1
                        bat_pos_dict[ball_dict["batsman_id"]] = bat_pos_counter + 1
                        bat_pos_counter += 1

                    if "runs" in ball_data:
                        runs = ball_data["runs"]
                        ball_dict["runs"] = runs["total"]
                        ball_dict["ball_runs"] = runs["batter"]
                        ball_dict["extras"] = runs["extras"]
                        if ball_dict["extras"] > 0:
                            ball_dict["is_extra"] = 1
                        if ball_dict["ball_runs"] == 0 and ball_dict["extras"] == 0:
                            ball_dict["is_dot_ball"] = 1
                        else:
                            if "non_boundary" not in runs:
                                if ball_dict["ball_runs"] == 4:
                                    ball_dict["is_four"] = 1
                                elif ball_dict["ball_runs"] == 6:
                                    ball_dict["is_six"] = 1
                            if ball_dict["ball_runs"] == 1:
                                ball_dict["is_one"] = 1
                            elif ball_dict["ball_runs"] == 2:
                                ball_dict["is_two"] = 1
                            elif ball_dict["ball_runs"] == 3:
                                ball_dict["is_three"] = 1

                    if "extras" in ball_data:
                        extras = ball_data["extras"]
                        if "noballs" in extras:
                            ball_no -= 1
                            ball_dict["is_no_ball"] = 1
                        elif "legbyes" in extras:
                            ball_dict["is_leg_bye"] = 1
                        elif "byes" in extras:
                            ball_dict["is_bye"] = 1
                        elif "wides" in extras:
                            ball_dict["is_wide"] = 1
                            ball_no -= 1

                    ball_dict["out_batsman_id"] = -1
                    ball_dict["out_batsman"] = np.nan
                    ball_dict["out_desc"] = ''
                    if "wickets" in ball_data:
                        wickets = ball_data["wickets"][0]
                        ball_dict["is_wicket"] = 1
                        ball_dict["is_bowler_wicket"] = 1
                        ball_dict["wicket_type"] = wickets["kind"]
                        ball_dict["out_batsman"] = wickets["player_out"].title()
                        ball_dict["out_batsman_id"] = player_id_map.get(ball_dict["out_batsman"], -1)

                        if wickets["kind"] in [
                            "run out",
                            "retired hurt",
                            "obstructing the field",
                            "handled the ball",
                        ]:
                            ball_dict["is_bowler_wicket"] = 0
                        if ball_dict["non_striker_id"] not in bat_pos_dict:
                            bat_pos_dict[ball_dict["non_striker_id"]] = bat_pos_counter + 1
                            bat_pos_counter += 1
                        if wickets["kind"] == "caught":
                            ball_dict["out_desc"] = f"c {wickets['fielders'][0].get('name')} b {ball_data['bowler']}"
                        elif wickets["kind"] == "stumped":
                            ball_dict["out_desc"] = f"st {wickets['fielders'][0].get('name')} b {ball_data['bowler']}"
                        elif wickets["kind"] == "caught and bowled":
                            ball_dict["out_desc"] = f"c & b {ball_data['bowler']}"
                        elif wickets["kind"] == "bowled":
                            ball_dict["out_desc"] = f"b {ball_data['bowler']}"
                        elif wickets["kind"] == "run out":
                            ball_dict["out_desc"] = f"run out"
                        elif wickets["kind"] == "lbw":
                            ball_dict["out_desc"] = f"lbw b {ball_data['bowler']}"
                    ball_dict["batting_team"] = team_short_name_x.upper()
                    ball_dict["team_short_name_x"] = team_short_name_x.upper()
                    ball_dict["bowling_team"] = team_short_name_y.upper()
                    ball_dict["team_short_name_y"] = team_short_name_y.upper()
                    ball_dict["over_text"] = float(
                        str(ball_dict["over_number"] - 1) + "." + str(ball_dict["ball_number"])
                    )
                    ball_by_ball.append(ball_dict)

        ball_by_ball = pd.DataFrame((ball_by_ball))

        ball_by_ball["batsman_map"] = ball_by_ball["batsman_id"].apply(
            lambda x: player_info.get(x, {}).get("name", "nan")
        )
        ball_by_ball["batsman_id_map"] = ball_by_ball["batsman_id"]
        ball_by_ball["batsman_source_id_map"] = ball_by_ball["batsman_id"]

        ball_by_ball["striker_batting_type"] = ball_by_ball["batsman_id"].apply(
            lambda x: player_info.get(x, {}).get("striker_batting_type", "nan")
        )

        ball_by_ball["non_striker_map"] = ball_by_ball["non_striker_id"].apply(
            lambda x: player_info.get(x, {}).get("name", "nan")
        )
        ball_by_ball["non_striker_id_map"] = ball_by_ball["non_striker_id"]
        ball_by_ball["non_striker_source_id_map"] = ball_by_ball["non_striker_id"]

        ball_by_ball["bowler_map"] = ball_by_ball["bowler_id"].apply(
            lambda x: player_info.get(x, {}).get("name", "nan")
        )
        ball_by_ball["bowler_id_map"] = ball_by_ball["bowler_id"]

        ball_by_ball["bowler_source_id_map"] = ball_by_ball["bowler_id"]
        ball_by_ball["bowler_sub_type"] = ball_by_ball["bowler_id"].apply(
            lambda x: player_info.get(x, {}).get("bowler_sub_type", "nan")
        )

        ball_by_ball["out_batsman_map"] = ball_by_ball["out_batsman_id"].apply(
            lambda x: player_info.get(x, {}).get("name", "nan")
        )
        ball_by_ball["out_batsman_id_map"] = ball_by_ball["out_batsman_id"]
        # ball_by_ball["out_batsman_source_id_map"] = ball_by_ball["out_batsman_id"]

        ball_by_ball["bowl_major_type"] = ball_by_ball["bowler_sub_type"].map(
            bowl_major_type_dict
        )

        ball_by_ball["batsman"] = ball_by_ball["batsman_map"]
        ball_by_ball["batsman_id"] = ball_by_ball["batsman_id_map"]
        ball_by_ball["batsman_source_id"] = ball_by_ball["batsman_source_id_map"]

        ball_by_ball["bowler"] = ball_by_ball["bowler_map"]
        ball_by_ball["bowler_id"] = ball_by_ball["bowler_id_map"]
        ball_by_ball["bowler_source_id"] = ball_by_ball["bowler_source_id_map"]

        ball_by_ball["non_striker"] = ball_by_ball["non_striker_map"]
        ball_by_ball["non_striker_id"] = ball_by_ball["non_striker_id_map"]
        ball_by_ball["non_striker_source_id"] = ball_by_ball["non_striker_source_id_map"]

        ball_by_ball["out_batsman"] = ball_by_ball["out_batsman_map"]
        # ball_by_ball["out_batsman_id"] = ball_by_ball["batsman_id_map"]
        # ball_by_ball["out_batsman_source_id"] = ball_by_ball["out_batsman_source_id_map"]
        remove_cols = [
            "batsman_map",
            "batsman_id_map",
            "batsman_source_id_map",
            "non_striker_map",
            "non_striker_id_map",
            "non_striker_source_id_map",
            "bowler_map",
            "bowler_id_map",
            "bowler_source_id_map",
            "out_batsman_map",
            # "out_batsman_id_map",
            # "out_batsman_source_id_map",
            "batsman_source_id",
            "bowler_source_id",
            "non_striker_source_id",
            # "out_batsman_source_id",
        ]
        ball_by_ball = ball_by_ball.drop(remove_cols, axis=1)
        ball_by_ball = ball_by_ball.applymap(lambda x: np.nan if x == "nan" else x)
        # ball_by_ball['out_batsman_id'] = np.where(ball_by_ball['out_batsman_id'] == -1, np.nan,
        #                                           ball_by_ball['out_batsman_id'])
        # Key to add if it is not already there
        key_columns = [
            'over_number',
            'ball_number',
            'is_four',
            'is_six',
            'is_extra',
            'is_batsman',
            'is_bowler',
            'is_wicket_keeper',
            'is_wide',
            'is_no_ball',
            'is_dot_ball',
            'innings',
            'is_wicket',
            'is_leg_bye',
            'is_bye',
            'is_bowler_wicket',
            'is_one',
            'is_two',
            'is_three',
            'match_name',
            'match_date',
            'season',
            'stadium_name',
            'winning_team',
            'toss_team',
            'toss_decision',
            'match_result',
            'target_runs',
            'batsman',
            'batsman_id',
            'is_playoff',
            'non_striker',
            'non_striker_id',
            'bowler',
            'bowler_id',
            'bowling_order',
            'batting_position',
            'runs',
            'ball_runs',
            'extras',
            'out_batsman_id',
            'out_batsman',
            'out_desc',
            'batting_team',
            'team_short_name_x',
            'bowling_team',
            'team_short_name_y',
            'over_text',
            'wicket_type',
            'striker_batting_type',
            'bowler_sub_type',
            'bowl_major_type'
        ]
        for column in key_columns:
            if column not in ball_by_ball.columns:
                if column == "is_batsman":
                    ball_by_ball[column] = -1
                elif column == "is_bowler":
                    ball_by_ball[column] = -1
                elif column == "is_wicket_keeper":
                    ball_by_ball[column] = -1
                elif column == "bowler_sub_type":
                    ball_by_ball[column] = ""
                elif column == "bowl_major_type":
                    ball_by_ball[column] = ""
                elif column == "striker_batting_type":
                    ball_by_ball[column] = ""
                else:
                    ball_by_ball[column] = pd.NA

        # Replace player name with the name from player mapping table
        ball_by_ball['batsman'] = ball_by_ball['batsman'].replace(player_name_to_mapping_name)
        ball_by_ball['non_striker'] = ball_by_ball['non_striker'].replace(player_name_to_mapping_name)
        ball_by_ball['bowler'] = ball_by_ball['bowler'].replace(player_name_to_mapping_name)
        if not ball_by_ball['out_batsman'].isna().all():
            ball_by_ball['out_batsman'] = ball_by_ball['out_batsman'].replace(player_name_to_mapping_name)
        ball_by_ball['match_name'] = self.get_match_name(team_short_name_x, team_short_name_y)
        registry = data["info"]["registry"]["people"]
        squad = {}
        for key, value in data["info"]["players"].items():
            squad[key.upper()] = [registry[player] for player in value]
        return ball_by_ball, squad

    def parser(self, data, player_info, match_id):
        # if data["info"]["match_type"] == "ODI" or data["info"]["match_type"] == "ODM":
        #     return self.odi_parser(data, player_info, match_id)
        if data["info"]["match_type"] == "T20":
            return self.t20_parser(data, player_info, match_id)
        # if data["info"]["match_type"] == "Test" or data["info"]["match_type"] == "MDM":
        #     return self.test_parser(data, player_info, match_id)


def get_byb_cricsheet(data, player_info, match_id):
    return MatchParser().parser(data, player_info, match_id)


class MatchData:
    def __init__(self, ):
        self.player_info = None

    def convert_cricsheet_json(self, data, match_id):
        if self.player_info is None:
            self.player_info = {}
            player_mapper = getPandasFactoryDF(
                session,
                f"select * from {PLAYER_MAPPER_TABLE_NAME}"
            )
            loaded_json = json.loads(player_mapper.to_json(orient="records"))
            for iterator in loaded_json:
                self.player_info[iterator['cricsheet_id']] = iterator
        return get_byb_cricsheet(data, self.player_info, match_id)

    def get_match_dataframe(self, data, match_id, competition_name):
        df, squad = self.convert_cricsheet_json(data, match_id)
        df["src_match_id"] = match_id
        df["competition_name"] = competition_name
        return df, squad
