import sys
sys.path.append("./../../")
sys.path.append("./")
import json
import requests
import pandas as pd
from DataIngestion.utils.helper import dataToDF, readExcel, generateSeq
from DataIngestion.config import DAILY_ACTIVITY_EXCEL_DATA_PATH, DAILY_ACTIVITY_COLS_MAPPING, BOWLING_PLANNING_TEMPLATE, \
    BOWL_PLANNING_TABLE_NAME, BOWL_PLANNING_KEY_COL
from common.dao.fetch_db_data import getPandasFactoryDF, getMaxId
from DataIngestion.query import GET_ALREADY_EXISTING_PLANNED_SQL
from common.dao_client import session
from datetime import datetime, timedelta
from log.log import get_logger
import numpy as np
from common.db_config import DB_NAME

logger = get_logger("GPSDataIngestion", "GPSDataIngestion")


def fetchGPSData(base_url, api_name, max_date_query, token, gps_data_src_col_mapping, grouping_list, teams_list=["Mumbai Indians", "MI Capetown", "MI New York"]):
    src_cols = [key for key, value in gps_data_src_col_mapping.items()]
    data_list = []
    for team in teams_list:
        max_date = getPandasFactoryDF(session, max_date_query + f" where team_name='{team}' ALLOW FILTERING;").iloc[0, 0]
        if max_date:
            max_date = datetime.strptime(str(max_date), '%Y-%m-%d') - timedelta(days=3)
            max_date = max_date.strftime('%d/%m/%Y')
        else:
            max_date = '01/01/2000'

        logger.info(f"Max Date in DB --> {max_date}")

        auth = {'Authorization': 'Bearer ' + token,
                'content-type': 'application/json'}

        payload = '{"filters": [{"name": "date", "comparison": ">", "values": [' + \
                  json.dumps(max_date) + ']}, {"name": "team_name", "comparison": "=", "values": [' + \
                  json.dumps(team) + ']}], "parameters": ' + json.dumps(src_cols) \
                  + ', "group_by":' + json.dumps(grouping_list) + '} '

        try:
            req = requests.post(base_url + api_name, headers=auth, data=payload)
            data_list.extend(req.json())
        except requests.ConnectionError as e:
            logger.info("OOPS!! Connection Error. Make sure you are connected to Internet. Technical Details given below.\n")
            logger.info(str(e))
        except requests.Timeout as e:
            logger.info("Timeout Error!")
            logger.info(str(e))
        except requests.RequestException as e:
            logger.info(str(e))
        except Exception as e:
            logger.info(str(e))

    if len(data_list)>0:
        renamed_cols = [value for key, value in gps_data_src_col_mapping.items()]
        logger.info("Fetched latest GPS Data from Catapult API!")
        df = dataToDF(data_list, col_list=None).rename(columns=gps_data_src_col_mapping)[renamed_cols]
        load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['load_timestamp'] = load_timestamp
        return df
    else:
        logger.info("No new data available in Catapult API!")
        return pd.DataFrame()


def generateGPSData(gps_data_df, GET_ALREADY_EXISTING_GPS_SQL, join_list, gps_decimal_columns=None, gps_int_columns=None):
    if not gps_data_df.empty:
        logger.info("GPS Data Generation Started!")
        gps_data_df['date_name'] = gps_data_df['date_name'].apply(
            lambda x: datetime.strptime(x, '%d/%m/%Y').strftime('%Y-%m-%d'))
        gps_data_df['record_date'] = gps_data_df['date_name'].apply(
            lambda x: datetime.strptime(x, '%Y-%m-%d').strftime('%A, %b %d, %Y'))

        gps_existing_data = getPandasFactoryDF(session, GET_ALREADY_EXISTING_GPS_SQL)
        gps_existing_data['date_name'] = gps_existing_data['date_name'].astype(str)
        gps_data_df = pd.merge(gps_data_df, gps_existing_data,
                                 on=join_list, how='left',
                                 indicator=True)

        gps_data_df = gps_data_df[gps_data_df['_merge'] == "left_only"].drop('_merge', axis=1)
        if not gps_data_df.empty:
            if gps_decimal_columns:
                gps_data_df[gps_decimal_columns] = gps_data_df[gps_decimal_columns].fillna(0.0)

            if gps_int_columns:
                gps_data_df[gps_int_columns] = gps_data_df[gps_int_columns].fillna(0)

            players_mapper_data = getPandasFactoryDF(session, f'''select id, name, catapult_id from {DB_NAME}.playermapping;''').explode('catapult_id')
            gps_data_df = (gps_data_df.merge(players_mapper_data, left_on='athlete_id', right_on='catapult_id', how='left')
                           .rename(columns={"id": "player_id", "name": "player_name"}))

            gps_data_df["player_name"] = gps_data_df["player_name"].fillna(gps_data_df["athlete_name"])

            gps_data_df['player_id'] = gps_data_df['player_id'].fillna(-1).astype(int)

            logger.info("GPS Data Generation Completed!")
            return gps_data_df.drop(["catapult_id", "athlete_name"], axis=1)
        else:
            logger.info("No New GPS Data Available!")
            return pd.DataFrame()

    else:
        logger.info("No New GPS Data Available!")
        return pd.DataFrame()



def getGPSBallData(df):
    if not df.empty:
        df['is_match_fielding'] = np.where(
            ((df['activity_name'].str.contains('Match', case=False) | df['activity_name'].str.contains('Game', case=False))
             & (~df['activity_name'].str.contains('Practice', case=False)) & (
                 df['period_name'].str.contains('Fielding', case=False))), 1, 0)
        df['season'] = df['record_date'].apply(lambda x: int(x.split(',')[2].strip()))
        df['ball_no'] = df['delivery_name'].apply(lambda x: int(x.split(" ")[1].strip()))
        return df.to_dict(orient='records')
    else:
        logger.info("No new data available for GPS Ball data!")
        return []


def dailyActivityData(GET_EXISTING_FORM_ENTRIES_SQL):
    existing_entries_data = getPandasFactoryDF(session, GET_EXISTING_FORM_ENTRIES_SQL)
    existing_entries_list = existing_entries_data['id'].tolist()

    daily_activity = readExcel(DAILY_ACTIVITY_EXCEL_DATA_PATH, "Form1")[
        [key for key, value in DAILY_ACTIVITY_COLS_MAPPING.items()]].rename(columns=DAILY_ACTIVITY_COLS_MAPPING)

    daily_activity = daily_activity[~daily_activity['id'].isin(existing_entries_list)]
    if ~daily_activity.empty:
        daily_activity['record_date'] = daily_activity['record_date'].apply(
            lambda x: datetime.strptime(str(x).split(" ")[0], '%Y-%m-%d').strftime('%A, %b %d, %Y'))
        daily_activity['team_name'] = "Mumbai Indians"
        daily_activity[['played_today', 'reason_noplay_or_train', 'form_filler']] = daily_activity[
            ['played_today', 'reason_noplay_or_train', 'form_filler']].fillna('NA')
        daily_activity['batting_train_mins'] = daily_activity['batting_train_mins'].str.replace('\D+',
                                                                                                '').str.strip().replace(
            '',None)
        daily_activity['player_name'] = daily_activity['player_name'].apply(
            lambda x: x.strip().replace('Ashwin Murugan', 'Murugan Ashwin') \
                .replace('M Arshad Khan', 'Arshad Khan').replace('Tilak Varma', 'Thakur Tilak Varma').replace('Karthikeya Singh', 'Kumar Kartikeya Singh')
            .replace('Duan Jansan', 'Duan Jansen'))
        daily_activity['start_time'] = daily_activity['start_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        daily_activity['completion_time'] = daily_activity['completion_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        daily_activity[['start_time', 'completion_time']] = daily_activity[['start_time', 'completion_time']].fillna(
            '00-00-00 00:00:00')
        daily_activity = daily_activity.fillna(-1)
        daily_activity[['batting_train_mins', 'batting_train_rpe', 'bowling_train_mins', 'fielding_train_mins',
                        'fielding_train_rpe', 'strength_mins', 'strength_rpe', 'running_mins', 'running_rpe',
                        'cross_training_mins', 'cross_training_rpe', 'rehab_mins', 'rehab_rpe', 'batting_match_mins',
                        'bowling_match_balls', 'bowling_match_rpe', 'batting_match_rpe', 'bowling_match_mins',
                        'fielding_match_mins', 'fielding_match_rpe', 'fatigue_level_rating', 'sleep_rating',
                        'muscle_soreness_rating', 'stress_levels_rating', 'wellness_rating', 'bowling_train_rpe', 'bowling_train_balls']] = \
        daily_activity[
            ['batting_train_mins', 'batting_train_rpe', 'bowling_train_mins', 'fielding_train_mins',
             'fielding_train_rpe', 'strength_mins', 'strength_rpe', 'running_mins', 'running_rpe',
             'cross_training_mins', 'cross_training_rpe', 'rehab_mins', 'rehab_rpe', 'batting_match_mins',
             'bowling_match_balls', 'bowling_match_rpe', 'batting_match_rpe', 'bowling_match_mins',
             'fielding_match_mins', 'fielding_match_rpe', 'fatigue_level_rating', 'sleep_rating',
             'muscle_soreness_rating', 'stress_levels_rating', 'wellness_rating', 'bowling_train_rpe', 'bowling_train_balls']].astype(int)

        return daily_activity.to_dict(orient="records")
    else:
        logger.info("No New Data in Daily Activity File!")
        return None


def getFitnessFormDF(GET_EXISTING_PLAYER_LOAD_ID):
    fitness_form_df = getPandasFactoryDF(session, f'''select id, team_name, record_date, player_name, 
    batting_train_mins, batting_train_rpe, bowling_train_mins, bowling_train_rpe, fielding_train_mins, fielding_train_rpe, running_mins, running_rpe,
                cross_training_mins, cross_training_rpe, strength_mins, strength_rpe, rehab_mins, rehab_rpe, batting_match_mins,
                batting_match_rpe, bowling_match_mins, bowling_match_rpe, fielding_match_mins, fielding_match_rpe,
                 bowling_match_balls from {DB_NAME}.fitnessForm''').drop_duplicates(
        subset=["record_date", "player_name"], keep="last")
    existing_entries_data = getPandasFactoryDF(session, GET_EXISTING_PLAYER_LOAD_ID)
    existing_entries_list = existing_entries_data['id'].tolist()
    fitness_form_df = fitness_form_df[~fitness_form_df['id'].isin(existing_entries_list)]
    return fitness_form_df

def playerLoadData(fitness_form_df):

    if ~fitness_form_df.empty:
        fitness_form_df = fitness_form_df.mask(fitness_form_df == -1, 0)
        fitness_form_df['bat_load'] = fitness_form_df['batting_train_mins'] * fitness_form_df['batting_train_rpe']
        fitness_form_df['bowl_load'] = fitness_form_df['bowling_train_mins'] * fitness_form_df['bowling_train_rpe']
        fitness_form_df['field_load'] = fitness_form_df['fielding_train_mins'] * fitness_form_df['fielding_train_rpe']
        fitness_form_df['run_load'] = fitness_form_df['running_mins'] * fitness_form_df['running_rpe']
        fitness_form_df['bat_match_load'] = fitness_form_df['batting_match_mins'] * fitness_form_df['batting_match_rpe']
        fitness_form_df['bowl_match_load'] = fitness_form_df['bowling_match_mins'] * fitness_form_df[
            'bowling_match_rpe']
        fitness_form_df['field_match_load'] = fitness_form_df['fielding_match_mins'] * fitness_form_df[
            'fielding_match_rpe']

        fitness_form_df['match_load'] = fitness_form_df['bat_match_load'] + fitness_form_df['bowl_match_load'] + \
                                        fitness_form_df['field_match_load']
        fitness_form_df['x_train_load'] = fitness_form_df['cross_training_mins'] * fitness_form_df['cross_training_rpe']
        fitness_form_df['strength_load'] = fitness_form_df['strength_mins'] * fitness_form_df['strength_rpe']
        fitness_form_df['rehab_load'] = fitness_form_df['rehab_mins'] * fitness_form_df['rehab_rpe']
        fitness_form_df['total_snc_load'] = fitness_form_df['x_train_load'] + fitness_form_df['rehab_load'] + \
                                            fitness_form_df['strength_load'] + fitness_form_df['run_load']
        fitness_form_df['total_trn_load'] = fitness_form_df['bat_load'] + fitness_form_df['bowl_load'] + \
                                            fitness_form_df['total_snc_load'] + fitness_form_df['field_load']
        fitness_form_df['total_load'] = fitness_form_df['total_trn_load'] + fitness_form_df['match_load']

        fitness_form_df[['bat_load', 'bowl_load', 'field_load', 'run_load',
                         'match_load', 'x_train_load', 'strength_load', 'rehab_load', 'total_snc_load',
                         'total_trn_load', 'total_load', 'bowling_match_balls', 'bat_match_load', 'bowl_match_load',
                         'field_match_load']] = fitness_form_df[['bat_load', 'bowl_load', 'field_load', 'run_load',
                                                                 'match_load', 'x_train_load', 'strength_load',
                                                                 'rehab_load', 'total_snc_load', 'total_trn_load',
                                                                 'total_load', 'bowling_match_balls', 'bat_match_load',
                                                                 'bowl_match_load', 'field_match_load']].fillna(0).astype(int)

        fitness_form_data = fitness_form_df[
            ['id', 'player_id', 'team_name', 'record_date', 'player_name', 'bat_load', 'bowl_load', 'field_load', 'run_load',
             'match_load', 'x_train_load', 'strength_load', 'rehab_load', 'total_snc_load', 'total_trn_load',
             'total_load', 'bowling_match_balls', 'bat_match_load', 'bowl_match_load', 'field_match_load']].to_dict(
            orient="records")

        return fitness_form_data

    else:
        logger.info("No New Data in Daily Activity File!")
        return None


def plannedBowlingData():
    planned_df = readExcel(BOWLING_PLANNING_TEMPLATE, "Sheet1").fillna(-1)
    planned_df['record_date'] = planned_df['record_date'].apply(
        lambda x: datetime.strptime(str(x).split(" ")[0], '%Y-%m-%d').strftime('%A, %b %d, %Y'))

    planned_existing_data = getPandasFactoryDF(session, GET_ALREADY_EXISTING_PLANNED_SQL)
    planned_df = pd.merge(planned_df, planned_existing_data,
                           on=["record_date", "player_name", "team_name"], how='left',
                           indicator=True)

    planned_df = planned_df[planned_df['_merge'] == "left_only"].drop('_merge', axis=1)
    if len(planned_df)>0:
        max_key_val = getMaxId(session, BOWL_PLANNING_TABLE_NAME, BOWL_PLANNING_KEY_COL, DB_NAME)
        planned_df[['match_balls', 'train_balls']] = planned_df[['match_balls', 'train_balls']].astype(int)
        planned_data = generateSeq(planned_df, BOWL_PLANNING_KEY_COL, max_key_val).to_dict(orient='records')
        return planned_data
