import hashlib
import json
import uuid

import pandas as pd

from DataIngestion import config
from DataIngestion.config import TEAM_MAPPING_PATH
from DataIngestion.query import GET_PLAYER_MAPPER_SQL
from DataIngestion.utils.helper import readCSV
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session


class NVPlayFileParser:
    BATTING_TEAM = 'batting_team'
    BOWLING_TEAM = 'bowling_team'
    WINNING_TEAM = 'winning_team'
    TOSS_TEAM = 'toss_team'
    BALL_RUNS = 'ball_runs'
    IS_DOT_BALL = 'is_dot_ball'
    OVER_NUMBER = 'over_number'
    TEAM_NAME = 'team_name'
    BALL_NUMBER = 'ball_number'
    MATCH_NAME = 'match_name'
    SEASON = 'season'
    MATCH_DATE = 'match_date'
    OVER_TEXT = 'over_text'
    WICKET_TYPE = 'wicket_type'
    EXTRAS = 'extras'
    RUNS = 'runs'
    TARGET_RUNS = 'target_runs'
    IS_WICKET = 'is_wicket'
    SRC_MATCH_ID = 'src_match_id'
    OUT_DESC = 'out_desc'
    IS_ONE = 'is_one'
    IS_TWO = 'is_two'
    IS_THREE = 'is_three'
    IS_FOUR = 'is_four'
    IS_SIX = 'is_six'
    IS_WIDE = 'is_wide'
    INNINGS = 'innings'
    IS_LEG_BYE = 'is_leg_bye'
    IS_BYE = 'is_bye'
    IS_NO_BALL = 'is_no_ball'
    IS_BATSMAN = 'is_batsman'
    IS_BOWLER = 'is_bowler'
    IS_BOWLER_WICKET = 'is_bowler_wicket'
    COMPETITION_NAME = 'competition_name'
    IS_EXTRA = 'is_extra'
    BOWLER_ID = 'bowler_id'
    BOWLING_ORDER = 'bowling_order'
    BATTING_POSITION = 'batting_position'
    BATSMAN_ID = 'batsman_id'
    BATSMAN = 'batsman'
    NVPLAY_ID = 'nvplay_id'
    NON_STRIKER = 'non_striker'
    BOWLER = 'bowler'
    FIRST_FIELDER = 'Fielder1'
    OUT_BATSMAN = 'out_batsman'
    OUT_BATSMAN_ID = 'out_batsman_id'
    IS_WICKET_KEEPER = 'is_wicket_keeper'
    STADIUM_NAME = 'stadium_name'
    TOSS_DECISION = 'toss_decision'
    MATCH_RESULT = 'match_result'
    IS_PLAYOFF = 'is_playoff'
    NON_STRIKER_ID = 'non_striker_id'
    STRIKER_BATTING_TYPE = 'striker_batting_type'
    BOWLER_SUB_TYPE = 'bowler_sub_type'
    BOWL_MAJOR_TYPE = 'bowl_major_type'
    PITCH_X = 'pitch_x'
    PITCH_Y = 'pitch_y'
    BOWL_LINE = 'bowl_line'
    BOWL_LENGTH = 'bowl_length'
    BOWL_TYPE = 'bowl_type'
    SHOT_TYPE = 'shot_type'

    BOWL_LENGTH_MAPPING = {
        'Length': 'Good Length',
        'Full': 'Full Length',
        'Back Of Length': 'Good Length',
        'Short': 'Short Length',
        'Yorker': 'Yorker',
        'Full Toss': 'Full Toss',
        'Bouncer': 'Short Length',
        '': ''
    }

    BOWL_LINE_MAPPING = {
        "Line": "Middle Stump",
        "Leg": "Outside Leg Stump",
        "Off": "Outside Off Stump",
        "Leg Wide": "Wide Outside Leg Stump",
        "Off Wide": "Wide Outside Off Stump",
        '': ''
    }

    BOWL_TYPE_MAPPING = {
        "Angle Across": "ANGLED ACROSS",
        "Angle In": "ANGLED IN",
        "Arm Ball": "ARM BALL",
        "Back of Hand": "BACKHAND SLOWER",
        "Carrom Ball": "CARROM BALL",
        "Flipper": "FLIPPER",
        "Googly": "GOOGLY",
        "In Swinger": "INSWINGER",
        "Knuckle Ball": "SLOWER BALL",
        "Left-Arm Leg Spinner": "CHINAMAN",
        "Leg Cutter": "LEGCUTTER",
        "Leg Spinner": "LEG SPIN",
        "No Movement": "STRAIGHT BALL",
        "Off Break": "OFF SPIN",
        "Off Cutter": "OFF CUTTER",
        "Out Swinger": "OUTSWINGER",
        "Quicker Ball": "NA",
        "Reverse Swing In": "REVERSE SWING",
        "Reverse Swing": "REVERSE SWING",
        "Reverse Swing Out": "REVERSE SWING",
        "Seam Away": "NIPPED AWAY",
        "Seam In": "NIPBACKER",
        "Slider": "SLIDER",
        "Slower Ball": "SLOWER BALL",
        "Stock Ball": "ORTHODOX",
        "Top Spinner": "TOP SPINNER",
        "Bouncer": "BOUNCER",
        '': ''
    }

    SHOT_TYPE_MAPPING = {
        "Drive": "ON DRIVE",
        "Back Defence": "BACKFOOT DEFENCE",
        "Push Shot": "BACKFOOT PUSH",
        "Cut": "CUT SHOT",
        "Flick": "FLICK",
        "Forward Defence": "FORWARD DEFENCE",
        "Pull": "PULL SHOT",
        "Steer": "GLIDE",
        "Hook": "HOOK SHOT",
        "No Shot": "NO SHOT",
        "Leg Glance": "LEG GLANCE",
        "Late Cut": "LATE CUT",
        "Slog": "SLOG SHOT",
        "Sweep": "SWEEP SHOT",
        "Reverse Sweep": "REVERSE SWEEP",
        "Scoop": "SCOOP",
        "Upper Cut": "UPPER CUT",
        "Slog Sweep": "SLOG SWEEP",
        'Worked': 'WORKED',
        'Drop and Run': 'DROP AND RUN',
        'Switch Hit': 'SWITCH HIT',
        'Padded Away': 'PADDED AWAY',
        'Lap': 'LAP',
        'Fended': 'FENDED',
        'Reverse Swing': 'REVERSE SWING',
        '': ''
    }

    def __init__(self, season_matches_df, competition_name):
        self.season_matches_df = season_matches_df
        self.competition_name = competition_name
        self.team_mapping = readCSV(TEAM_MAPPING_PATH)
        # Open and read the JSON file for NV Play config
        with open(config.INGESTION_CONFIG, 'r') as json_file:
            ingestion_config = json.load(json_file)
        mapping = ingestion_config['columns_mapping']
        self.SRC_BALL_NUMBER = mapping['SRC_BALL_NUMBER']
        self.SRC_OVER_NUMBER = mapping['SRC_OVER_NUMBER']
        self.SRC_INNINGS = mapping['SRC_INNINGS']
        self.SRC_BATSMAN = mapping['SRC_BATSMAN']
        self.SRC_BOWLER = mapping['SRC_BOWLER']
        self.SRC_NON_STRIKER = mapping['SRC_NON_STRIKER']
        self.SRC_BALL_RUNS = mapping['SRC_BALL_RUNS']
        self.SRC_EXTRAS = mapping['SRC_EXTRAS']
        self.SRC_OUT_BATSMAN = mapping['SRC_OUT_BATSMAN']
        self.SRC_MATCH_DATE = mapping['SRC_MATCH_DATE']
        self.SRC_MATCH_RESULT = mapping['SRC_MATCH_RESULT']
        self.SRC_WINNING_TEAM = mapping['SRC_WINNING_TEAM']
        self.SRC_TOSS_TEAM = mapping['SRC_TOSS_TEAM']
        self.SRC_TOSS_DECISION = mapping['SRC_TOSS_DECISION']
        self.SRC_STADIUM_NAME = mapping['SRC_STADIUM_NAME']
        self.SRC_BATTING_TEAM = mapping['SRC_BATTING_TEAM']
        self.SRC_BOWLING_TEAM = mapping['SRC_BOWLING_TEAM']
        self.SRC_WICKET_TYPE = mapping['SRC_WICKET_TYPE']
        self.SRC_PITCH_X = mapping['SRC_PITCH_X']
        self.SRC_PITCH_Y = mapping['SRC_PITCH_Y']
        self.SRC_MATCH = mapping['SRC_MATCH']
        self.SRC_DATE = mapping['SRC_DATE']
        self.SRC_TEAM_RUNS = mapping['SRC_TEAM_RUNS']
        self.SRC_EXTRA = mapping['SRC_EXTRA']
        self.SRC_BOWL_LENGTH = mapping['SRC_BOWL_LENGTH']
        self.SRC_BOWL_LINE = mapping['SRC_BOWL_LINE']
        self.SRC_BOWL_TYPE = mapping['SRC_BOWL_TYPE']
        self.SRC_SHOT_TYPE = mapping['SRC_SHOT_TYPE']

    def create_short_hash(self, player_name):
        if type(player_name) != str:
            return ''
        # Create a hash object using SHA-256
        sha256_hash = hashlib.sha256(player_name.encode()).hexdigest()
        return f'nv_{sha256_hash}'

    def is_bowler_wicket(self, wicket_type):
        if wicket_type == '' or wicket_type == 'Run Out' or wicket_type == 'Retired - Not Out':
            return 0
        else:
            return 1

    def get_match_name(self, match_name):
        team_one, team_two = match_name.split(' v ')
        return (
                self.team_mapping[self.team_mapping[NVPlayFileParser.TEAM_NAME] == team_one.upper()].iloc[0][1]
                + 'VS'
                + self.team_mapping[self.team_mapping[NVPlayFileParser.TEAM_NAME] == team_two.upper()].iloc[0][1]
        ).upper()

    def get_ball_by_ball(self, squads):
        # This will return all the ball_by_ball df for all matches of particular season
        season_matches_df = self.season_matches_df
        unique_matches = season_matches_df[[self.SRC_MATCH, self.SRC_DATE]].drop_duplicates()
        ball_by_ball_dataframes = []
        matches_squads = []
        # Parse the data for each match
        for index, row in unique_matches.iterrows():
            match_df = season_matches_df[
                (season_matches_df[self.SRC_MATCH] == row[self.SRC_MATCH]) &
                (season_matches_df[self.SRC_DATE] == row[self.SRC_DATE])
                ]
            match_df = match_df.rename(
                columns={
                    self.SRC_BALL_NUMBER: NVPlayFileParser.BALL_NUMBER,
                    self.SRC_OVER_NUMBER: NVPlayFileParser.OVER_NUMBER,
                    self.SRC_INNINGS: NVPlayFileParser.INNINGS,
                    self.SRC_BATSMAN: NVPlayFileParser.BATSMAN,
                    self.SRC_BOWLER: NVPlayFileParser.BOWLER,
                    self.SRC_NON_STRIKER: NVPlayFileParser.NON_STRIKER,
                    self.SRC_BALL_RUNS: NVPlayFileParser.BALL_RUNS,
                    self.SRC_EXTRAS: NVPlayFileParser.EXTRAS,
                    self.SRC_OUT_BATSMAN: NVPlayFileParser.OUT_BATSMAN,
                    self.SRC_MATCH_DATE: NVPlayFileParser.MATCH_DATE,
                    self.SRC_MATCH_RESULT: NVPlayFileParser.MATCH_RESULT,
                    self.SRC_WINNING_TEAM: NVPlayFileParser.WINNING_TEAM,
                    self.SRC_TOSS_TEAM: NVPlayFileParser.TOSS_TEAM,
                    self.SRC_TOSS_DECISION: NVPlayFileParser.TOSS_DECISION,
                    self.SRC_STADIUM_NAME: NVPlayFileParser.STADIUM_NAME,
                    self.SRC_BATTING_TEAM: NVPlayFileParser.BATTING_TEAM,
                    self.SRC_BOWLING_TEAM: NVPlayFileParser.BOWLING_TEAM,
                    self.SRC_WICKET_TYPE: NVPlayFileParser.WICKET_TYPE,
                    self.SRC_PITCH_X: NVPlayFileParser.PITCH_X,
                    self.SRC_PITCH_Y: NVPlayFileParser.PITCH_Y,
                    self.SRC_BOWL_LINE: NVPlayFileParser.BOWL_LINE,
                    self.SRC_BOWL_LENGTH: NVPlayFileParser.BOWL_LENGTH,
                    self.SRC_BOWL_TYPE: NVPlayFileParser.BOWL_TYPE,
                    self.SRC_SHOT_TYPE: NVPlayFileParser.SHOT_TYPE
                }
            )
            # use BOWL_LENGTH, BOWL_LINE, BOWL_TYPE to map the values of bowl_length, bowl_line, bowl_type
            # Replace nan to empty cell
            match_df[NVPlayFileParser.BOWL_LENGTH] = match_df[NVPlayFileParser.BOWL_LENGTH].fillna('')
            match_df[NVPlayFileParser.BOWL_LENGTH] = match_df[NVPlayFileParser.BOWL_LENGTH].apply(
                lambda x: self.BOWL_LENGTH_MAPPING[x])
            match_df[NVPlayFileParser.BOWL_LINE] = match_df[NVPlayFileParser.BOWL_LINE].fillna('')
            match_df[NVPlayFileParser.BOWL_LINE] = match_df[NVPlayFileParser.BOWL_LINE].apply(
                lambda x: self.BOWL_LINE_MAPPING[x])
            match_df[NVPlayFileParser.BOWL_TYPE] = match_df[NVPlayFileParser.BOWL_TYPE].fillna('')
            match_df[NVPlayFileParser.BOWL_TYPE] = match_df[NVPlayFileParser.BOWL_TYPE].apply(
                lambda x: self.BOWL_TYPE_MAPPING[x])
            match_df[NVPlayFileParser.SHOT_TYPE] = match_df[NVPlayFileParser.SHOT_TYPE].fillna('')
            match_df[NVPlayFileParser.SHOT_TYPE] = match_df[NVPlayFileParser.SHOT_TYPE].apply(
                lambda x: self.SHOT_TYPE_MAPPING[x])
            match_df[NVPlayFileParser.OVER_TEXT] = match_df[NVPlayFileParser.OVER_NUMBER].apply(lambda x: x - 1).astype(
                str) + '.' + match_df[NVPlayFileParser.BALL_NUMBER].astype(str)
            match_name = match_df[self.SRC_MATCH].iloc[0]
            match_date = match_df[NVPlayFileParser.MATCH_DATE].iloc[0]
            match_df = match_df.fillna({NVPlayFileParser.WICKET_TYPE: ''})
            match_df[NVPlayFileParser.RUNS] = match_df[NVPlayFileParser.BALL_RUNS] + match_df[NVPlayFileParser.EXTRAS]
            match_df[NVPlayFileParser.MATCH_NAME] = match_df[self.SRC_MATCH].apply(self.get_match_name) + match_df[
                NVPlayFileParser.MATCH_DATE].str.replace(
                '/', '')
            match_df[NVPlayFileParser.SEASON] = match_df[NVPlayFileParser.MATCH_DATE].str[6:]
            match_df[NVPlayFileParser.IS_WICKET] = match_df[NVPlayFileParser.WICKET_TYPE].apply(
                lambda x: 1 if x != '' else 0)
            match_df[NVPlayFileParser.MATCH_DATE] = pd.to_datetime(
                match_df[NVPlayFileParser.MATCH_DATE].str.replace('/', '-'), format='%d-%m-%Y'
            )
            match_df[NVPlayFileParser.TARGET_RUNS] = match_df[match_df[NVPlayFileParser.INNINGS] == 1][
                self.SRC_TEAM_RUNS].max()
            match_df[NVPlayFileParser.SRC_MATCH_ID] = str(uuid.uuid4())
            match_df[NVPlayFileParser.BATTING_TEAM] = match_df[NVPlayFileParser.BATTING_TEAM].str.upper()
            match_df[NVPlayFileParser.BOWLING_TEAM] = match_df[NVPlayFileParser.BOWLING_TEAM].str.upper()
            match_df[NVPlayFileParser.WINNING_TEAM] = match_df[NVPlayFileParser.WINNING_TEAM].str.upper()
            match_df[NVPlayFileParser.TOSS_TEAM] = match_df[NVPlayFileParser.TOSS_TEAM].str.upper()
            match_df[NVPlayFileParser.IS_ONE] = match_df[NVPlayFileParser.BALL_RUNS].apply(lambda x: 1 if x == 1 else 0)
            match_df[NVPlayFileParser.IS_TWO] = match_df[NVPlayFileParser.BALL_RUNS].apply(lambda x: 1 if x == 2 else 0)
            match_df[NVPlayFileParser.IS_THREE] = match_df[NVPlayFileParser.BALL_RUNS].apply(
                lambda x: 1 if x == 3 else 0)
            match_df[NVPlayFileParser.IS_FOUR] = match_df[NVPlayFileParser.BALL_RUNS].apply(
                lambda x: 1 if x == 4 else 0)
            match_df[NVPlayFileParser.IS_SIX] = match_df[NVPlayFileParser.BALL_RUNS].apply(lambda x: 1 if x == 6 else 0)
            match_df[NVPlayFileParser.IS_WIDE] = match_df[self.SRC_EXTRA].apply(
                lambda x: 1 if x in [
                    'Wide',
                    'Wide,No Ball',
                    'No Ball,Wide',
                ] else 0
            )
            match_df[NVPlayFileParser.IS_LEG_BYE] = match_df[self.SRC_EXTRA].apply(
                lambda x: 1 if x in [
                    'Leg Bye',
                    'No Ball,Leg Bye',
                    'Leg Bye,No Ball'
                ] else 0
            )
            match_df[NVPlayFileParser.IS_BYE] = match_df[self.SRC_EXTRA].apply(
                lambda x: 1 if x in [
                    'Bye',
                    'Bye,No Ball',
                    'No Ball,Bye'
                ] else 0
            )
            match_df[NVPlayFileParser.IS_NO_BALL] = match_df[self.SRC_EXTRA].apply(
                lambda x: 1 if x in [
                    'No Ball',
                    'Bye,No Ball',
                    'No Ball,Bye',
                    'No Ball,Leg Bye',
                    'Leg Bye,No Ball',
                    'Wide,No Ball',
                    'No Ball,Wide'
                ] else 0
            )
            match_df[NVPlayFileParser.IS_DOT_BALL] = match_df[NVPlayFileParser.RUNS].apply(lambda x: 1 if x == 0 else 0)
            match_df[NVPlayFileParser.IS_BOWLER_WICKET] = match_df[NVPlayFileParser.WICKET_TYPE].apply(
                self.is_bowler_wicket)
            match_df[NVPlayFileParser.COMPETITION_NAME] = self.competition_name
            match_df[NVPlayFileParser.IS_EXTRA] = match_df.apply(
                lambda row: 1 if (
                        row[self.SRC_EXTRA] in [
                    'Wide',
                    'Wide,No Ball',
                    'No Ball,Wide',
                ] or
                        row[self.SRC_EXTRA] in [
                            'Leg Bye',
                            'No Ball,Leg Bye',
                            'Leg Bye,No Ball'
                        ] or
                        row[self.SRC_EXTRA] in [
                            'No Ball',
                            'Bye,No Ball',
                            'No Ball,Bye',
                            'No Ball,Leg Bye',
                            'Leg Bye,No Ball',
                            'Wide,No Ball',
                            'No Ball,Wide'
                        ] or
                        row[self.SRC_EXTRA] in [
                            'Bye',
                            'Bye,No Ball',
                            'No Ball,Bye'
                        ]
                )
                else 0, axis=1
            )
            bat_pos_dict = dict()
            bowl_pos_dict = dict()
            bat_pos_counter = 0
            bowl_pos_counter = 0
            match_df_innings_first = match_df[match_df[NVPlayFileParser.INNINGS] == 1]
            match_df_innings_second = match_df[match_df[NVPlayFileParser.INNINGS] == 2]
            for index, row in match_df_innings_first.iterrows():
                if row[NVPlayFileParser.BOWLER_ID] in bowl_pos_dict:
                    match_df_innings_first.at[index, NVPlayFileParser.BOWLING_ORDER] = bowl_pos_dict[
                        row[NVPlayFileParser.BOWLER_ID]]
                else:
                    match_df_innings_first.at[index, NVPlayFileParser.BOWLING_ORDER] = bowl_pos_counter + 1
                    bowl_pos_dict[row[NVPlayFileParser.BOWLER_ID]] = bowl_pos_counter + 1
                    bowl_pos_counter += 1
                if row[NVPlayFileParser.BATSMAN_ID] in bat_pos_dict:
                    match_df_innings_first.at[index, NVPlayFileParser.BATTING_POSITION] = bat_pos_dict[
                        row[NVPlayFileParser.BATSMAN_ID]]
                else:
                    match_df_innings_first.at[index, NVPlayFileParser.BATTING_POSITION] = bat_pos_counter + 1
                    bat_pos_dict[row[NVPlayFileParser.BATSMAN_ID]] = bat_pos_counter + 1
                    bat_pos_counter += 1

                if row[NVPlayFileParser.WICKET_TYPE] == 'Caught':
                    match_df_innings_first.at[
                        index, NVPlayFileParser.OUT_DESC] = f'c {row[NVPlayFileParser.FIRST_FIELDER]} b {row[NVPlayFileParser.BOWLER]}'
                elif row[NVPlayFileParser.WICKET_TYPE] == 'Stumped':
                    match_df_innings_first.at[
                        index, NVPlayFileParser.OUT_DESC] = f'st {row[NVPlayFileParser.FIRST_FIELDER]} b {row[NVPlayFileParser.BOWLER]}'
                elif row[NVPlayFileParser.WICKET_TYPE] == 'caught and bowled':
                    match_df_innings_first.at[
                        index, NVPlayFileParser.OUT_DESC] = f'c & b {row[NVPlayFileParser.BOWLER]}'
                elif row[NVPlayFileParser.WICKET_TYPE] == 'Bowled':
                    match_df_innings_first.at[index, NVPlayFileParser.OUT_DESC] = f'b {row[NVPlayFileParser.BOWLER]}'
                elif row[NVPlayFileParser.WICKET_TYPE] == 'Run Out':
                    match_df_innings_first.at[index, NVPlayFileParser.OUT_DESC] = f'run out'
                elif row[NVPlayFileParser.WICKET_TYPE] == 'LBW':
                    match_df_innings_first.at[
                        index, NVPlayFileParser.OUT_DESC] = f'lbw b {row[NVPlayFileParser.BOWLER]}'
                elif row[NVPlayFileParser.WICKET_TYPE] != '':
                    match_df_innings_first.at[index, NVPlayFileParser.OUT_DESC] = row[NVPlayFileParser.WICKET_TYPE]
                elif row[NVPlayFileParser.WICKET_TYPE] == '':
                    continue

            bat_pos_counter = 0
            bowl_pos_counter = 0
            for index, row in match_df_innings_second.iterrows():
                if row[NVPlayFileParser.BOWLER_ID] in bowl_pos_dict:
                    match_df_innings_second.at[index, NVPlayFileParser.BOWLING_ORDER] = bowl_pos_dict[
                        row[NVPlayFileParser.BOWLER_ID]]
                else:
                    match_df_innings_second.at[index, NVPlayFileParser.BOWLING_ORDER] = bowl_pos_counter + 1
                    bowl_pos_dict[row[NVPlayFileParser.BOWLER_ID]] = bowl_pos_counter + 1
                    bowl_pos_counter += 1
                if row[NVPlayFileParser.BATSMAN_ID] in bat_pos_dict:
                    match_df_innings_second.at[index, NVPlayFileParser.BATTING_POSITION] = bat_pos_dict[
                        row[NVPlayFileParser.BATSMAN_ID]]
                else:
                    match_df_innings_second.at[index, NVPlayFileParser.BATTING_POSITION] = bat_pos_counter + 1
                    bat_pos_dict[row[NVPlayFileParser.BATSMAN_ID]] = bat_pos_counter + 1
                    bat_pos_counter += 1

                if row[NVPlayFileParser.WICKET_TYPE] == 'Caught':
                    match_df_innings_second.at[
                        index, NVPlayFileParser.OUT_DESC] = f'c {row[NVPlayFileParser.FIRST_FIELDER]} b {row[NVPlayFileParser.BOWLER]}'
                elif row[NVPlayFileParser.WICKET_TYPE] == 'Stumped':
                    match_df_innings_second.at[
                        index, NVPlayFileParser.OUT_DESC] = f'st {row[NVPlayFileParser.FIRST_FIELDER]} b {row[NVPlayFileParser.BOWLER]}'
                elif row[NVPlayFileParser.WICKET_TYPE] == 'caught and bowled':
                    match_df_innings_second.at[
                        index, NVPlayFileParser.OUT_DESC] = f'c & b {row[NVPlayFileParser.BOWLER]}'
                elif row[NVPlayFileParser.WICKET_TYPE] == 'Bowled':
                    match_df_innings_second.at[index, NVPlayFileParser.OUT_DESC] = f'b {row[NVPlayFileParser.BOWLER]}'
                elif row[NVPlayFileParser.WICKET_TYPE] == 'Run Out':
                    match_df_innings_second.at[index, NVPlayFileParser.OUT_DESC] = f'run out'
                elif row[NVPlayFileParser.WICKET_TYPE] == 'LBW':
                    match_df_innings_second.at[
                        index, NVPlayFileParser.OUT_DESC] = f'lbw b {row[NVPlayFileParser.BOWLER]}'
                elif row[NVPlayFileParser.WICKET_TYPE] != '':
                    match_df_innings_second.at[index, NVPlayFileParser.OUT_DESC] = row[NVPlayFileParser.WICKET_TYPE]
                elif row[NVPlayFileParser.WICKET_TYPE] == '':
                    continue

            match_df = match_df_innings_first.append(match_df_innings_second)
            match_df[NVPlayFileParser.BOWLING_ORDER] = match_df[NVPlayFileParser.BOWLING_ORDER].astype(int)
            match_df[NVPlayFileParser.BATTING_POSITION] = match_df[NVPlayFileParser.BATTING_POSITION].astype(int)
            # Key to add if it is not already there
            key_columns = [
                NVPlayFileParser.OVER_NUMBER,
                NVPlayFileParser.BALL_NUMBER,
                NVPlayFileParser.IS_FOUR,
                NVPlayFileParser.IS_SIX,
                NVPlayFileParser.IS_EXTRA,
                NVPlayFileParser.IS_BATSMAN,
                NVPlayFileParser.IS_BOWLER,
                NVPlayFileParser.IS_WICKET_KEEPER,
                NVPlayFileParser.IS_WIDE,
                NVPlayFileParser.IS_NO_BALL,
                NVPlayFileParser.IS_DOT_BALL,
                NVPlayFileParser.INNINGS,
                NVPlayFileParser.IS_WICKET,
                NVPlayFileParser.IS_LEG_BYE,
                NVPlayFileParser.IS_BYE,
                NVPlayFileParser.IS_BOWLER_WICKET,
                NVPlayFileParser.IS_ONE,
                NVPlayFileParser.IS_TWO,
                NVPlayFileParser.IS_THREE,
                NVPlayFileParser.MATCH_NAME,
                NVPlayFileParser.MATCH_DATE,
                NVPlayFileParser.SEASON,
                NVPlayFileParser.STADIUM_NAME,
                NVPlayFileParser.WINNING_TEAM,
                NVPlayFileParser.TOSS_TEAM,
                NVPlayFileParser.TOSS_DECISION,
                NVPlayFileParser.MATCH_RESULT,
                NVPlayFileParser.TARGET_RUNS,
                NVPlayFileParser.BATSMAN,
                NVPlayFileParser.BATSMAN_ID,
                NVPlayFileParser.IS_PLAYOFF,
                NVPlayFileParser.NON_STRIKER,
                NVPlayFileParser.NON_STRIKER_ID,
                NVPlayFileParser.BOWLER,
                NVPlayFileParser.BOWLER_ID,
                NVPlayFileParser.BOWLING_ORDER,
                NVPlayFileParser.BATTING_POSITION,
                NVPlayFileParser.RUNS,
                NVPlayFileParser.BALL_RUNS,
                NVPlayFileParser.EXTRAS,
                NVPlayFileParser.OUT_BATSMAN_ID,
                NVPlayFileParser.OUT_BATSMAN,
                NVPlayFileParser.OUT_DESC,
                NVPlayFileParser.BATTING_TEAM,
                'team_short_name_x',
                NVPlayFileParser.BOWLING_TEAM,
                'team_short_name_y',
                NVPlayFileParser.OVER_TEXT,
                NVPlayFileParser.WICKET_TYPE,
                NVPlayFileParser.STRIKER_BATTING_TYPE,
                NVPlayFileParser.BOWLER_SUB_TYPE,
                NVPlayFileParser.BOWL_MAJOR_TYPE,
                NVPlayFileParser.BOWL_LINE,
                NVPlayFileParser.BOWL_LENGTH,
                NVPlayFileParser.BOWL_TYPE,
                NVPlayFileParser.SHOT_TYPE
            ]

            for column in key_columns:
                if column not in match_df.columns:
                    if column == NVPlayFileParser.IS_BATSMAN:
                        match_df[column] = -1
                    elif column == NVPlayFileParser.IS_BOWLER:
                        match_df[column] = -1
                    elif column == NVPlayFileParser.IS_WICKET_KEEPER:
                        match_df[column] = -1
                    elif column == NVPlayFileParser.BOWLER_SUB_TYPE:
                        match_df[column] = ''
                    elif column == NVPlayFileParser.BOWL_MAJOR_TYPE:
                        match_df[column] = ''
                    elif column == NVPlayFileParser.STRIKER_BATTING_TYPE:
                        match_df[column] = ''
                    else:
                        match_df[column] = pd.NA

            match_df = match_df[[
                NVPlayFileParser.OVER_NUMBER,
                NVPlayFileParser.BALL_NUMBER,
                NVPlayFileParser.IS_FOUR,
                NVPlayFileParser.IS_SIX,
                NVPlayFileParser.IS_EXTRA,
                NVPlayFileParser.IS_BATSMAN,
                NVPlayFileParser.IS_BOWLER,
                NVPlayFileParser.IS_WICKET_KEEPER,
                NVPlayFileParser.IS_WIDE,
                NVPlayFileParser.IS_NO_BALL,
                NVPlayFileParser.IS_DOT_BALL,
                NVPlayFileParser.INNINGS,
                NVPlayFileParser.IS_WICKET,
                NVPlayFileParser.IS_LEG_BYE,
                NVPlayFileParser.IS_BYE,
                NVPlayFileParser.IS_BOWLER_WICKET,
                NVPlayFileParser.IS_ONE,
                NVPlayFileParser.IS_TWO,
                NVPlayFileParser.IS_THREE,
                NVPlayFileParser.MATCH_NAME,
                NVPlayFileParser.MATCH_DATE,
                NVPlayFileParser.SEASON,
                NVPlayFileParser.STADIUM_NAME,
                NVPlayFileParser.WINNING_TEAM,
                NVPlayFileParser.TOSS_TEAM,
                NVPlayFileParser.TOSS_DECISION,
                NVPlayFileParser.MATCH_RESULT,
                NVPlayFileParser.TARGET_RUNS,
                NVPlayFileParser.BATSMAN,
                NVPlayFileParser.BATSMAN_ID,
                NVPlayFileParser.IS_PLAYOFF,
                NVPlayFileParser.NON_STRIKER,
                NVPlayFileParser.NON_STRIKER_ID,
                NVPlayFileParser.BOWLER,
                NVPlayFileParser.BOWLER_ID,
                NVPlayFileParser.BOWLING_ORDER,
                NVPlayFileParser.BATTING_POSITION,
                NVPlayFileParser.RUNS,
                NVPlayFileParser.BALL_RUNS,
                NVPlayFileParser.EXTRAS,
                NVPlayFileParser.OUT_BATSMAN_ID,
                NVPlayFileParser.OUT_BATSMAN,
                NVPlayFileParser.OUT_DESC,
                NVPlayFileParser.BATTING_TEAM,
                'team_short_name_x',
                NVPlayFileParser.BOWLING_TEAM,
                'team_short_name_y',
                NVPlayFileParser.OVER_TEXT,
                NVPlayFileParser.WICKET_TYPE,
                NVPlayFileParser.STRIKER_BATTING_TYPE,
                NVPlayFileParser.BOWLER_SUB_TYPE,
                NVPlayFileParser.BOWL_MAJOR_TYPE,
                NVPlayFileParser.PITCH_X,
                NVPlayFileParser.PITCH_Y,
                NVPlayFileParser.COMPETITION_NAME,
                NVPlayFileParser.SRC_MATCH_ID,
                NVPlayFileParser.BOWL_LINE,
                NVPlayFileParser.BOWL_LENGTH,
                NVPlayFileParser.BOWL_TYPE,
                NVPlayFileParser.SHOT_TYPE
            ]]

            # Use pd.melt to melt the specified columns into a single column in a new DataFrame
            melted_df = pd.melt(
                match_df, value_vars=[NVPlayFileParser.BOWLER, NVPlayFileParser.BATSMAN, NVPlayFileParser.NON_STRIKER,
                                      NVPlayFileParser.OUT_BATSMAN], value_name='players'
            ).drop('variable', axis=1).drop_duplicates()
            melted_df[NVPlayFileParser.NVPLAY_ID] = melted_df['players'].apply(self.create_short_hash)
            players_mapping_df = getPandasFactoryDF(session, GET_PLAYER_MAPPER_SQL)

            merged_df = pd.merge(
                melted_df,
                players_mapping_df[[NVPlayFileParser.NVPLAY_ID, 'name']],
                on=NVPlayFileParser.NVPLAY_ID,
                how='inner'
            ).drop([NVPlayFileParser.NVPLAY_ID], axis=1)
            merged_df = merged_df.dropna(axis=0, subset=['players'])
            player_name_to_mapping_name = {}
            for index, row in merged_df.iterrows():
                player_name_to_mapping_name[row['players']] = row['name']
            match_df[NVPlayFileParser.BATSMAN] = match_df[NVPlayFileParser.BATSMAN].replace(player_name_to_mapping_name)
            match_df[NVPlayFileParser.NON_STRIKER] = match_df[NVPlayFileParser.NON_STRIKER].replace(
                player_name_to_mapping_name)
            match_df[NVPlayFileParser.OUT_DESC] = match_df[NVPlayFileParser.OUT_DESC].fillna('')
            match_df[NVPlayFileParser.BOWLER] = match_df[NVPlayFileParser.BOWLER].replace(player_name_to_mapping_name)
            match_df[NVPlayFileParser.OUT_BATSMAN] = match_df[NVPlayFileParser.OUT_BATSMAN].replace(
                player_name_to_mapping_name)
            match_df[NVPlayFileParser.OUT_BATSMAN_ID] = match_df[NVPlayFileParser.OUT_BATSMAN_ID].fillna(-1)
            ball_by_ball_dataframes.append(match_df)
            for squad in squads:
                if (
                        squad[NVPlayFileParser.MATCH_NAME] == match_name or squad.get('match_name_') == match_name
                ) and squad[NVPlayFileParser.MATCH_DATE] == match_date:
                    team_one, team_two = match_name.split(' v ')
                    squad_one = squad.get(team_one)
                    squad_one_players = []
                    for player in squad_one:
                        squad_one_players.append(int(player['cricinfo_id']))
                    squad_two = squad.get(team_two)
                    squad_two_players = []
                    for player in squad_two:
                        squad_two_players.append(int(player['cricinfo_id']))
                    matches_squads.append({
                        team_one.upper(): squad_one_players,
                        team_two.upper(): squad_two_players
                    })
        return ball_by_ball_dataframes, matches_squads
