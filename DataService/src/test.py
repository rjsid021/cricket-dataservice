import sys
from ast import literal_eval

import pandas as pd
import pandasql as psql
from marshmallow import ValidationError, fields

from DataIngestion.config import IMAGE_STORE_URL, EMIRATES_TOKEN, GPS_SRC_KEY_MAPPING, GPS_AGG_DATA_GROUP_LIST, TOKEN, \
    PLAYERS_TABLE_NAME, PLAYERS_KEY_COL, BASE_URL, EMIRATES_BASE_URL, BOWL_PLANNING_TABLE_NAME, \
    DAILY_ACTIVITY_TABLE_NAME, PLAYER_LOAD_TABLE_NAME, WPL_TOKEN, GPS_TABLE_NAME, GPS_DELIVERY_GROUP_LIST, \
    PEAK_LOAD_TABLE_NAME
from DataIngestion.preprocessor.fitness_gps_data import playerLoadData
from DataIngestion.query import GET_PLAYER_MAPPER_DETAILS_SQL
# from DataIngestion.query import GET_PLAYER_MAPPER_DETAILS_SQL
# from DataIngestion.sources.smartabase.config import SMARTABASE_USER, SMARTABASE_PASSWORD, SMARTABASE_APP, LOGIN_API, \
#     SMARTABASE_USERS_URL
# from DataIngestion.sources.smartabase.smartabase import Smartabase
from DataIngestion.utils.helper import readCSV, random_string_generator
from DataService.fetch_sql_queries import GET_FITNESS_DATA, GET_GPS_DELIVERY_DATA
# from DataService.src import con
# from DataService.utils.helper import executeQuery
# from DataService.src import con
# from DataService.utils.helper import executeQuery
from common.dao.fetch_db_data import getPandasFactoryDF
# from common.dao.fetch_db_data import getPandasFactoryDF, getMaxId
# import warnings
#
# from flask import Flask, request, jsonify, Response
# from flask_cors import CORS
# from werkzeug.exceptions import HTTPException
#
# from DataIngestion import load_timestamp
# from DataService.app_config import IPL_RETAINED_LIST_PATH, IPL_AUCTION_LIST_PATH
# from common.dao.fetch_db_data import getMaxId
from common.dao.insert_data import insertToDB
from common.dao_client import session
from common.db_config import DB_NAME
from common.utils.helper import getPrettyDF, getTeamsMapping

sys.path.append("./../../")
sys.path.append("./")
# from DataIngestion.utils.helper import generateSeq, readExcel, readCSV
# from DataService.src import *
import numpy as np
# from DataIngestion.config import QUERY_FEEDBACK_TABLE_NAME, PLAYER_MAPPER_TABLE_NAME, PLAYER_MAPPER_KEY_COL
# from DataService.utils.helper import globalFilters, generateWhereClause, dropFilter, executeQuery, validateRequest, \
#     getListIndexDF, getJoinCondition, transform_matchName

# FITNESS_FORM_SQL = f'''select * from {DB_NAME}.{DAILY_ACTIVITY_TABLE_NAME} where id>=9165 ALLOW FILTERING;'''
# # #
# fitness_form_df = getPandasFactoryDF(session, FITNESS_FORM_SQL)
# print(getPrettyDF(fitness_form_df.sort_values(by=["id"],ascending=False).head(150)))
# fitness_form_df = readCSV("/Users/siddharth.nautiyal/Desktop/cricket/GIT/git-master/CricketDataService/data/new_updated_forms/fitness_form_miw.csv")
# fitness_form_df['start_time'] = pd.to_datetime(fitness_form_df['start_time'])
# fitness_form_df['completion_time'] = pd.to_datetime(fitness_form_df['completion_time'])
#
#
# # Convert datetime back to desired format
# fitness_form_df['start_time'] = fitness_form_df['start_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
# fitness_form_df['completion_time'] = fitness_form_df['completion_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
# print(fitness_form_df.count())
# fitness_form_df.sort_values(by=["id"]).to_csv("/Users/siddharth.nautiyal/Desktop/cricket/GIT/git-master/CricketDataService/data/new_updated_forms/fitness_form_miw.csv", index=False)

# player_mapping_df = getPandasFactoryDF(session, f'''select id as player_id, name from {DB_NAME}.playermapping''')
#
# fitness_form_df = fitness_form_df.merge(player_mapping_df, left_on="player_name", right_on="name", how="left")
# print(getPrettyDF(fitness_form_df[fitness_form_df["player_id"].isnull()].sort_values(by=["id"])))


# fitness_updated = readCSV("/Users/siddharth.nautiyal/Desktop/cricket/GIT/git-master/CricketDataService/data/new_updated_forms/fitness_form_emirates.csv")
# fitness_updated = fitness_updated[fitness_updated["team_name"].isin(["Mumbai Indians", "MI New York", "MI Capetown"])]
# fitness_updated = fitness_updated.append(fitness_form_df, ignore_index=True)
# fitness_updated.sort_values(by=["id"]).to_csv("/Users/siddharth.nautiyal/Desktop/cricket/GIT/git-master/CricketDataService/data/new_updated_forms/fitness_form.csv", index=False)
# print(getPrettyDF(fitness_updated))

# players_df = getPandasFactoryDF(session, f'''select * from {DB_NAME}.players where season=2024 and competition_name='WPL' and team_id=32 ALLOW FILTERING; ''')[["player_name"]]
# fitness_form_df['player_name_lower'] = fitness_form_df['player_name'].str.lower()
# players_df['player_name_lower'] = players_df['player_name'].str.lower()
# # print(getPrettyDF(fitness_form_df))
# #
# print(getPrettyDF(players_df))
# final_df = pd.DataFrame()
# record_date_list = list(fitness_form_df["record_date"].unique())
# for dt in record_date_list:
#     fitness_form_data = fitness_form_df[fitness_form_df["record_date"] == dt]
#     # print(getPrettyDF(fitness_form_data))
#
#     players_temp_df = players_df.merge(fitness_form_data, how="left", on="player_name_lower").drop(["player_name_lower", "player_name_y"], axis=1).rename(columns={"player_name_x": "player_name"})
#     players_temp_df["record_date"] = players_temp_df["record_date"].fillna(dt)
#     players_temp_df["form_filler"] = players_temp_df["form_filler"].fillna("NA")
#     final_df = final_df.append(players_temp_df)
#
#
#
# final_df["form_filled_for_the_day"] = np.where(final_df["form_filler"]=="NA", "No", "Yes")
#
# form_filler_conditions = [
#     (final_df['form_filler'] == "NA"),
#     (final_df['form_filler'].str.lower() == "me"),
#     (final_df['form_filler'].str.lower() == "staff")
# ]
#
# # Different batting phases
# filler_values = ["NA", "Player", "Staff"]
#
#
# final_df["form_filled_by"] =  np.select(form_filler_conditions, filler_values)
# final_df[["record_date", "player_name", "form_filled_for_the_day", "form_filled_by"]].to_csv("fitness_form_details.csv", index=False)
# print(getPrettyDF(final_df[["record_date", "player_name", "form_filled_for_the_day", "form_filled_by"]]))
from datetime import datetime
match_id_list = [848169,890608,849538,839955,878287,927571,913881,842693,883763,900191,865966,934416,901560,935785,879656,863228,22668,907036,908405,22669,896084,898822,22663,846800,894715,928940,852276,902929,920726,22666,867335,874180,864597,909774,857752,837217,844062,885132,930309,915250,905667,912512,911143,853645,841324,887870,889239,916619,937154,870073,22670,855014,891977,861859,838586,868704,22664,922095,897453,931678,933047,871442,856383,923464,850907,845431,926202,917988,872811,924833,859121,886501,919357,893346,876918,881025,904298,860490,882394,875549]

# extras_data = getPandasFactoryDF(session, f"select * from {DB_NAME}.fitnessGPSBalldata where team_name='Mumbai Indian Womens' and record_date='Saturday, Mar 02, 2024' ALLOW FILTERING;")
# extras_data["record_date"] = extras_data['record_date'].apply(lambda x: datetime.strptime(
#                         datetime.strptime(str(x), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date())
# extras_data = extras_data[(extras_data["player_name"].isin(["Pooja Vastrakar", "Amanjot Kaur"])) & (extras_data["record_date"]>=pd.to_datetime('2024-02-19'))]
# # id_list = list(extras_data[extras_data["match_id"].isin(match_id_list)]["id"].unique())
# # print(id_list)
# extras_data.sort_values(by=["player_name", "record_date"]).to_csv("raw-peak-data-amanjot-pooja.csv", index=False)
# # extras_data = extras_data[extras_data["activity_name"]=="Activity 20240223161541"]
# print(getPrettyDF(extras_data.sort_values(by=["player_name", "record_date"])))
# print(getPrettyDF(extras_data))
#(season, record_date), period_name, activity_name, player_name, ball_no, delivery_time))
"delete from fitnessgpsballdata where season=2024 and record_date='Saturday, Mar 02, 2024' and period_name='AutoCreatedPeriod' and activity_name='Activity 20240223161541';"
'''delete from fitnessgpsdata where record_date='Friday, Feb 23, 2024' and period_name='AutoCreatedPeriod' and activity_name='Activity 20240223161541' and player_name in ('Amanjot Kaur', 'Amelia Kerr', 'Nat Sciver-Brunt', 
 'Pooja Vastrakar', 'Saika Ishaque', 'Sajeevan Sajana', 'Sathyamoorthy Keerthana', 'Shabnim Ismail', 'Yastika Bhatia');'''
########################## code to insert player mapping #########################################
# player_mapping_df = getPandasFactoryDF(session, f'''select * from {DB_NAME}.playermapping''')
#
# player_mapping_df.sort_values(by="full_name").to_csv("/Users/siddharth.nautiyal/Desktop/cricket/GIT/git-master/CricketDataService/data/player_mapping-prod13mar.csv", index=False)


# players_df = readCSV("/Users/siddharth.nautiyal/Desktop/cricket/GIT/git-master/CricketDataService/data/player_mapping.csv")
# players_df = players_df[~players_df["catapult_id"].isnull()][["id", "catapult_id"]]
#
# players_df = readCSV("/Users/siddharth.nautiyal/Desktop/cricket/GIT/git-master/CricketDataService/data/player_mapping-prod13mar.csv")
# player_mapping_df= player_mapping_df.drop(["catapult_id"], axis=1).merge(players_df, how="left", on="id")
# player_mapping_df.to_csv("/Users/siddharth.nautiyal/Desktop/cricket/GIT/git-master/CricketDataService/data/player_mapping_final.csv", index=False)
# print(getPrettyDF(player_mapping_df[~player_mapping_df["catapult_id"].isnull()].head(100)))
# players_df["cricinfo_id"] = players_df["cricinfo_id"].astype(int)
# players_df["smartabase_id"] = players_df["smartabase_id"].fillna(-1).astype(int)
# players_df["catapult_id"] = players_df['catapult_id'].apply(lambda x: literal_eval(x) if (pd.notna(x)) else [])
# print(getPrettyDF(players_df[~players_df["catapult_id"].isnull()]))
# insertToDB(session, players_df.fillna("").to_dict(orient="records"), DB_NAME, "playermapping", allow_logging=True)
########################## code to insert player mapping #########################################
#
# GET_GPS_AGG_MAX_DATE = f'''select max(date_name) as max_ts from {DB_NAME}.{GPS_TABLE_NAME} '''
# max_date = getPandasFactoryDF(session,GET_GPS_AGG_MAX_DATE).iloc[0, 0]
# print(f"max_date --> {max_date}")
# GET_FITNESS_DATA_SQL = GET_FITNESS_DATA + f''' where date_name='{max_date}' ALLOW FILTERING;'''
# print(GET_FITNESS_DATA_SQL)
# fitness_data = getPandasFactoryDF(session, GET_FITNESS_DATA_SQL)[
#             ['record_date', 'team_name', 'total_distance_m']]
# fitness_data['total_distance_m'] = fitness_data['total_distance_m'].apply(pd.to_numeric, errors='coerce').round(decimals=0)
#
# gps_fitness_agg_df = fitness_data.groupby(["team_name", "record_date"]).mean().round(decimals=0).astype(int).reset_index()
#
# print(gps_fitness_agg_df["total_distance_m"].iloc[0])

# print(getPrettyDF(gps_fitness_agg_df[gps_fitness_agg_df["total_distance_m"]>10000]))
# print(getPrettyDF(gps_fitness_agg_df))
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from datetime import datetime
# fitness_data = getPandasFactoryDF(session, GET_FITNESS_DATA)[['record_date', 'player_name', 'team_name', 'player_id']]
# fitness_data['season'] = fitness_data['record_date'].apply(
#         lambda x: datetime.strptime(str(x), '%A, %b %d, %Y').strftime('%Y'))
# print(getPrettyDF(fitness_data))
#
# players_sql = '''
# select
#   pd.player_name,
#   td.team_short_name,
#   pd.season,
#   cast(pd.src_player_id as int) as player_id
# from
#   players_data pd
#   inner join teams_data td on (pd.team_id = td.team_id)
# where
#   td.team_name in ('MUMBAI INDIANS', 'MI NEW YORK', 'MI EMIRATES', 'MI CAPE TOWN', 'MUMBAI INDIANS WOMEN')
#   and pd.season>=2022
# '''
# season_data = executeQuery(con, players_sql)
# season_data["team_name"] = season_data["team_short_name"].apply(lambda x: getTeamsMapping()[x.replace(" ", "")])
# season_data['player_image_url'] = (IMAGE_STORE_URL + season_data['team_name']
#                                    .apply(lambda x: x.replace(' ', '-').lower()).astype(str) + '/' + season_data['player_name'].apply(
#     lambda x: x.replace(' ', '-').lower()).astype(str) + ".png")
# season_data['player_details'] = season_data[['player_id', 'player_name', 'player_image_url']].to_dict(orient='records')
#
# # season_data = season_data.groupby(['team_name', 'season'])['player_details'].agg(list).reset_index().to_json(orient='index')
#
# season_data = season_data.groupby(['team_name', 'season']).agg({"player_details": lambda x: list(x)}).reset_index()
#
# response = season_data.groupby('team_name').apply(
#     lambda x: x.set_index('season')[['player_details']].to_dict(orient='index')).to_dict()
#
# date_df = fitness_data.groupby('team_name')['record_date'].apply(
#             lambda x: list(np.unique(x))
#         ).reset_index()
#
# fitness_data['player_details'] = fitness_data[['player_name', 'player_id']].to_dict(orient='records')
# fitness_data['player_details'] = fitness_data['player_details'].apply(lambda x: {x['player_id']: x['player_name']})
# fitness_data = fitness_data.groupby(["team_name", "record_date"])["player_details"].agg(list).reset_index()
# fitness_data['player_details'] = fitness_data['player_details'].apply(lambda x: {k: v for d in x for k, v in d.items()})
#
# response["team_name"] = list(fitness_data["team_name"].unique())
# for team in date_df['team_name'].unique():
#     record_date = date_df.query(f"team_name=='{team}'")['record_date']
#     c_record_date = [date for _, *dates_list in record_date for date in dates_list]
#     s_record_date = sorted(c_record_date, key=lambda x: pd.to_datetime(x, format='%A, %b %d, %Y'), reverse=True)
#     response[team]['record_date'] = s_record_date
#
# for team in fitness_data['team_name'].unique():
#     response[team]["record_date_details"]= fitness_data.query(f"team_name=='{team}'").set_index('record_date')[
#         'player_details'].to_dict()
#
# print(response)
# # print(final_season_data)
# print(getPrettyDF(fitness_data))
# fitness_df = readCSV("/Users/siddharth.nautiyal/Desktop/cricket/GIT/git-master/CricketDataService/DataService/src/fitness_gps_data.csv")
# fitness_df = fitness_df.drop_duplicates()
# print(fitness_df["date_name"].count())
# players_df = getPandasFactoryDF(session, f'''select * from {DB_NAME}.bowlPlanning;''')
# players_df.to_csv("/Users/siddharth.nautiyal/Desktop/cricket/GIT/git-master/CricketDataService/data/prod_dump/bowlPlanning.csv", index=False)
# from ast import literal_eval
# player_mapping_df = getPandasFactoryDF(session, f'''select id as player_id, name, catapult_id from cricketsimulatordb.playermapping''')
# player_mapping_df = player_mapping_df[~player_mapping_df["catapult_id"].isnull()]
# players_df = readCSV("/Users/siddharth.nautiyal/Desktop/cricket/GIT/git-master/CricketDataService/data/prod_dump/matchPeakLoad.csv")
# insertToDB(session, players_df.to_dict(orient="records"), DB_NAME, PEAK_LOAD_TABLE_NAME)
#
# players_df = players_df.merge(player_mapping_df, how="left", left_on="player_name", right_on="name")
# players_df = players_df[players_df["player_id"].isnull()]
# print(getPrettyDF(players_df))
# players_df["catapult_id"] = players_df['catapult_id'].apply(lambda x: literal_eval(x) if (pd.notna(x)) else [])
# insertToDB(session, players_df.fillna("").to_dict(orient="records"), DB_NAME, "playermapping", allow_logging=True)


# player_mapping_df = player_mapping_df.merge(players_df, on="id", how="left").rename(columns={"catapult_id_y": "catapult_id"}).drop(["catapult_id_x"], axis=1)
# load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# player_mapping_df['load_timestamp'] = load_timestamp

# print(getPrettyDF(player_mapping_df[~player_mapping_df['catapult_id'].isnull()]))



# fitness_df = readCSV("/Users/siddharth.nautiyal/Desktop/cricket/GIT/git-master/CricketDataService/data/prod_dump/fitnessform.csv")
# print(fitness_df["record_date"].count())
# player_mapping_df = getPandasFactoryDF(session, f'''select id as player_id, name from cricketsimulatordb.playermapping''')
#
# fitness_df = fitness_df.merge(player_mapping_df, left_on="player_name", right_on="name", how="left")
# fitness_df = (fitness_df[~fitness_df["player_id"].isin([1298, 8432, 14210, 8430, 10868, 10808, 10161, 10165])]
#               .drop_duplicates(subset=['player_name', 'record_date', 'team_name', 'player_id'], keep='last'))
# player_load_data = playerLoadData(fitness_df)
# fitness_df.to_csv("fitness_form_updated.csv", index=False)
# fitness_df.to_csv("player_load_updated.csv", index=False)
# print(fitness_df["record_date"].count())
# print(player_load_data["record_date"].count())
# print(getPrettyDF(player_load_data))
# print(getPrettyDF(fitness_df[fitness_df["player_id"].isnull()]))
# print(fitness_df[fitness_df["player_id"].isnull()]["player_name"].unique())
# print(fitness_df.duplicated(subset=['player_name', 'record_date', 'team_name']).sum())
# dupes_df = psql.sqldf('''select player_name,record_date, team_name from  fitness_df where player_id not in (1298, 8432, 14210, 8430, 10868, 10808, 10161, 10165) group by  player_name,record_date, team_name
# having count(player_name)>1;''')
# print(dupes_df['player_name'].unique())
# print(getPrettyDF(dupes_df))

# print(getPrettyDF(fitness_df[(fitness_df['player_name']=='Rahul Singh') & (fitness_df['record_date']=='Friday, Apr 09, 2021') & (fitness_df['team_name']=='Mumbai Indians')]))

'''['Arshad Khan' 'Mohsin Khan' 'Rahul Singh' 'Rohit Sharma']
remove id - 1298, 8432, 14210, 8430, 10868, 10808, 10161, 10165

Arshad Khan   | Friday, Apr 01, 2022    | Mumbai Indians
Mohsin Khan   | Friday, Mar 26, 2021    | Mumbai Indians
Rohit Sharma  | Friday, Apr 16, 2021    | Mumbai Indians
Rahul Singh   | Friday, Apr 09, 2021    | Mumbai Indians
'''

# player_mapping_df.sort_values(by="full_name").to_csv("players_mapping_updated.csv", index=False)
# players_df = readCSV("/Users/siddharth.nautiyal/Desktop/cricket/GIT/git-master/CricketDataService/data/players_mapping_updated.csv")
# players_df["cricinfo_id"] = players_df["cricinfo_id"].astype(int)
# players_df["smartabase_id"] = players_df["smartabase_id"].fillna(-1).astype(int)
# from ast import literal_eval
# players_df["catapult_id"] = players_df['catapult_id'].apply(lambda x: literal_eval(x) if (pd.notna(x)) else None)
# print(getPrettyDF(players_df.head(100)))
# # # print(players_df.dtypes)
# # print(getPrettyDF(player_mapping_df[player_mapping_df['catapult_id']!="[]"].head(100)))
# print(getPrettyDF(player_mapping_df.head(10)))
# player_mapping_df.sort_values(by="full_name").to_csv("players_mapping_updated.csv", index=False)
# players_data = player_mapping_df.fillna("").to_dict(orient="records")
# # print(players_data)
# insertToDB(session, players_data, DB_NAME, "playermapping", allow_logging=True)



# fitness_data = getPandasFactoryDF(session, f'''select * from {DB_NAME}.fitnessForm where team_name='MI Women' ALLOW FILTERING;''')
# fitness_data = fitness_data.mask(fitness_data == -1, 0)
# player_load = playerLoadData(fitness_data)
# fitness_data.to_csv("wpl_form_data.csv", index=False)
# player_load.to_csv("wpl_player_load.csv", index=False)

# fitness_form = readCSV("/Users/siddharth.nautiyal/Desktop/cricket/GIT/git-master/CricketDataService/DataService/src/wpl_form_data.csv")
# insertToDB(session, fitness_form.fillna("").to_dict(orient="records"), DB_NAME, DAILY_ACTIVITY_TABLE_NAME, allow_logging=True)
# player_load = readCSV("/Users/siddharth.nautiyal/Desktop/cricket/GIT/git-master/CricketDataService/DataService/src/wpl_player_load.csv")
# insertToDB(session, player_load.to_dict(orient="records"), DB_NAME, PLAYER_LOAD_TABLE_NAME, allow_logging=True)
# gps_data['date_name'] = gps_data['date_name'].astype(str)
# gps_data = gps_data[gps_data['date_name'].isin(['2024-01-01','2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-06', '2024-01-07'])]
# print(getPrettyDF(gps_data.head(100)))



# mi_avai_df = readCSV("/Users/siddharth.nautiyal/Desktop/cricket/GIT/git-master/CricketDataService/smartabase_data/MI Availability.csv")
# print(getPrettyDF(mi_avai_df.head(100)))
# ipl_df = executeQuery(con, '''select * from players_data_df; ''')

# print(getPrettyDF(ipl_df.sort_values(by="player_name")))

# retained_df = readCSV("/Users/siddharth.nautiyal/Desktop/cricket/GIT/git-master/CricketDataService/data/retained-list-new.csv")
# retained_df = list(readCSV(IPL_RETAINED_LIST_PATH)["player_name"])
# auction_df = list(readCSV(IPL_AUCTION_LIST_PATH)["player_name"])
# print(getPrettyDF(retained_df.head(100)))

# print(list(set(retained_df).intersection(auction_df)))
# retained_df['player_name'] = retained_df['player_name'].apply(lambda x: x.strip().replace("'", "").replace(".", ""))
# retained_df = retained_df.merge(ipl_df, how="left", on ="player_name")
# retained_df = retained_df[retained_df['player_id'].isnull()].reset_index()

# auction_df = auction_df[~auction_df["player_name"].isin(retained_df)]
# print(getPrettyDF(auction_df[~auction_df['player_id'].isnull()]))
# print(getPrettyDF(auction_df))
# print(getPrettyDF(retained_df[retained_df['player_id'].isnull()].reset_index()))
##left_on=retained_df["player_name"].apply(lambda x: x.strip().replace(" ", "").lower()),
# right_on=ipl_df["player_name"].apply(lambda x: x.strip().replace(" ", "").lower())


# from datetime import datetime, timedelta
#
# def generate_dates(from_date, to_date):
#     # Convert string dates to datetime objects
#     from_date = datetime.strptime(from_date, '%d/%m/%Y')
#     to_date = datetime.strptime(to_date, '%d/%m/%Y')
#
#     # Generate dates between from_date and to_date
#     current_date = from_date
#     date_tuple = ()
#
#     while current_date <= to_date:
#         date_tuple += (current_date.strftime('%d/%m/%Y'),)
#         current_date += timedelta(days=1)
#
#     return date_tuple
#
# # Example usage
# from_date_str = '14/09/2023'
# to_date_str = '18/09/2023'
#
# date_tuple = generate_dates(from_date_str, to_date_str)
# print(str(date_tuple))
#
# import requests
# # Function to check if the image URL exists
# def url_exists(url):
#     try:
#         response = requests.head(url)
#         return response.status_code == 200
#     except requests.ConnectionError:
#         return False
#
# # ['04/12/2023' '19/10/2023' '15/09/2023' '23/10/2023' '14/09/2023'
# #  '16/11/2023' '14/12/2023' '11/12/2023']
#
# def smartabase_tournament_mapping():
#     tournament_group_mapping = {"ILT20": "ILT20",
#                                 "WPL": "WPL Squad",
#                                 "SA20": "SA20",
#                                 "IPL": "IPL Squad",
#                                 "MLC": "MLC Squad",
#                                 "UK Tour (Mens) 2023": "UK Tour (Mens) 2023",
#                                 "ALL TOURNAMENTS": "All Teams"}
#
#     return tournament_group_mapping
#
#
# smartabase = Smartabase(SMARTABASE_USER, SMARTABASE_PASSWORD, SMARTABASE_APP)
# session_id = smartabase.login(LOGIN_API)
#
#
# tournament_list = [key for key,value in smartabase_tournament_mapping().items()]
# filter_dict = {}

#
# for group in tournament_list:
#     group_name = smartabase_tournament_mapping()[group]
#     player_id_list = smartabase.get_player_ids(session_id, SMARTABASE_USERS_URL, group_name)
#     filtered_df = player_mapping_df[player_mapping_df['smartabase_id'].isin(player_id_list)]
#     # data_df = data_df.merge(player_mapping_df, on="smartabase_id", how="left")
#     # data_df['player_name'] = data_df['player_name'].fillna(data_df['first_name'] + " " + data_df['last_name'])
#     filtered_df = filtered_df[filtered_df["smartabase_id"].isin(player_id_list)]
#     filtered_df["competition_name"] = group
#
#     filter_data = filtered_df.groupby('competition_name')['player_name'].agg(list).to_dict()
#     filter_dict.update(filter_data)
#     # filtered_df = filtered_df[['competition_name', 'record_date', 'player_name']].groupby(['competition_name', 'record_date'])[
#     #     'player_name'].agg(list).reset_index()
#     # response = filtered_df.groupby(['competition_name']).apply(lambda x: x.set_index('record_date').to_dict(orient='index')).to_dict()
#     # print(filter_data)
#     # print(getPrettyDF(filtered_df.drop(["smartabase_id", "src_player_id"], axis=1).head(100)))
#     # filter_dict.update(response)
#
# print(filter_dict)


# if len(date_tuple) == 1:
#     start_date_clause = " where start_date='" + date_tuple[0] + "'"
# elif len(date_tuple)>1:
#     start_date_clause = " where start_date in " + str(date_tuple)
# else:
#     start_date_clause = ""

# print(f"{start_date_clause} --> {start_date_clause}")

# readiness_df = getPandasFactoryDF(session, f'''select smartabase_id, player_name, start_date as record_date, playing_status,
# physical_status, injury_status, action_status_comments from {DB_NAME}.readiness_to_perform {start_date_clause};''')
# # print(getPrettyDF(readiness_df))
# # print(readiness_df['record_date'].unique())
# availability_df = getPandasFactoryDF(session, f'''select smartabase_id, start_date as record_date,
#  overall_status as availability_status from {DB_NAME}.mi_availability {start_date_clause} ;''')
# print(getPrettyDF(availability_df))


# competition_name = ["IPL", "WPL"]
# player_id = []
# for group in competition_name:
#     group_name = tournament_group_mapping[group]
#     player_id_list = smartabase.get_player_ids(session_id, SMARTABASE_USERS_URL, group_name)
#     player_id.extend(player_id_list)
#
# print(player_id)
# readiness_df = readiness_df.merge(availability_df, on=["smartabase_id", "record_date"], how="left")
# readiness_df = readiness_df[readiness_df["smartabase_id"].isin(player_id)]
# readiness_df['player_image_url'] = IMAGE_STORE_URL + "players_images/mi-all-teams/" + readiness_df['player_name'].apply(
#             lambda x: x.replace(' ', '-').lower()
#         ).astype(str) + ".png"
#
# readiness_df['player_image_url'] = readiness_df['player_image_url'].apply(lambda x: x if url_exists(x) else "")
# print(getPrettyDF(readiness_df.head(100)))
import pytz
from datetime import datetime

def convert_epoch_to_uae(epoch_timestamp):
    gmt_timezone = pytz.timezone('GMT')
    uae_timezone = pytz.timezone('Asia/Dubai')

    # Convert epoch timestamp to datetime object
    dt_object = datetime.utcfromtimestamp(epoch_timestamp)
    dt_object = gmt_timezone.localize(dt_object)

    # Convert datetime to UAE timezone
    uae_datetime = dt_object.astimezone(uae_timezone)

    return uae_datetime


import json, requests, numpy as np

max_date = '01/01/2000'

src_cols = [key for key, value in GPS_SRC_KEY_MAPPING.items()]
# em_auth = {'Authorization': 'Bearer ' + EMIRATES_TOKEN,
#         'content-type': 'application/json'}

auth = {'Authorization': 'Bearer ' + TOKEN,
        'content-type': 'application/json'}

payload = '{"filters": [{"name": "date", "comparison": ">", "values": [' + \
          json.dumps(max_date) + ']}], "parameters": ' + json.dumps(src_cols) \
          + ', "group_by":' + json.dumps(GPS_DELIVERY_GROUP_LIST) + '} '
#GPS_DELIVERY_GROUP_LIST GPS_AGG_DATA_GROUP_LIST
# print(payload)
# # from datetime import datetime
# EMIRATES_BASE_URL = "https://connect-au.catapultsports.com/api/v6/"
BASE_URL="https://connect-eu.catapultsports.com/api/v6/"
req = requests.get(BASE_URL + "athletes", headers=auth)
# # req = requests.post(BASE_URL + "stats", headers=auth, data=payload)
# em_req = requests.get(EMIRATES_BASE_URL + "athletes", headers=em_auth)
# em_req = requests.post(EMIRATES_BASE_URL + "stats", headers=em_auth, data=payload)
data = req.json()
# em_data = em_req.json()
#
# # print(em_data)75054b55-9900-11e3-b9b6-22000af8166b
data_df = pd.DataFrame(data) #.rename(columns={"id": "catapult_id"})
data_df = data_df[data_df["current_team_id"]=="75054b55-9900-11e3-b9b6-22000af8166b"]
# em_data_df = pd.DataFrame(em_data)
# em_data_df = em_data_df[em_data_df['date_name']=='02/03/2024'].sort_values(by="period_name")
# em_data_df.to_csv("catapult_agg_28022024-raw.csv", index=False)
# print(em_data_df['activity_name'].unique())
print(getPrettyDF(data_df))

# print(data_df['activity_name'].unique())
# print(getPrettyDF(data_df.head(100)))

# player_mapping_df = getPandasFactoryDF(session, f'''select id , name from cricketsimulatordb.playermapping;''')
# player_data_mapping_df = getPandasFactoryDF(session, f'''select * from cricketsimulatordb.playermapping;''')
# data_df = pd.DataFrame(data, columns=["id", "first_name", "last_name"]).rename(columns={"id": "catapult_id"})
# data_df['name'] = data_df['first_name'] + " " + data_df["last_name"]
# print(getPrettyDF(data_df.head(100)))
# em_data_df = pd.DataFrame(em_data, columns=["id", "first_name", "last_name"]).rename(columns={"id": "catapult_id"})
# em_data_df['name'] = em_data_df['first_name'] + " " + em_data_df["last_name"]
# print(getPrettyDF(em_data_df.head(100)))
# data_df = data_df.append(em_data_df, ignore_index=True).dropna()



# data_df["start_time_converted"] = data_df["start_time"].apply(lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
# data_df["start_time_uae"] = data_df["start_time"].apply(convert_epoch_to_uae)
# data_df = data_df[(data_df["date_name"].isin(["14/01/2024","15/01/2024"]))].sort_values(by="start_time")#[["date_name", "period_name", "activity_name", "team_name",
#                                                        "athlete_name", "date", "start_time_h", "end_time_h", "start_time", "end_time" , "start_time_converted", "start_time_uae"]]
#
# data_df.to_csv("catapult_emirates.csv")
# print(getPrettyDF(data_df[data_df['name'].apply(lambda x: x.replace(" ", "").lower()).isin(["duanjansen", "jasonbehrendorff"])]))
# print(getPrettyDF(player_mapping_df.head(10)))
# data_df = player_mapping_df.merge(data_df[['catapult_id', 'name']], on='name', how='right')
# player_data_df = data_df[['catapult_id', 'name']].merge(player_mapping_df, right_on=player_mapping_df['name'].apply(
#     lambda x: x.strip().replace(" ", "").lower()),
#                                             left_on=data_df['name'].apply(
#                                                 lambda x: x.strip().replace(" ", "").lower()), how='left').rename(columns={"name_y": "name"}) #.drop(["name_x", "key_0"], axis=1)
#
# final_data_df = player_data_df[player_data_df['id'].isnull()]
# player_data_df = player_data_df.groupby(["id", "name"])[["catapult_id"]].agg(set).reset_index()
# player_data_df["catapult_id"] = player_data_df["catapult_id"].apply(list)
# player_data_df["id"] = player_data_df["id"].astype(int)
# # player_data_df.to_csv("catapult_mapping.csv", index=False)

# player_data_mapping_df = player_data_mapping_df.merge(player_data_df[["id", "catapult_id"]], on="id", how="left").rename(columns={"catapult_id_y": "catapult_id"}).drop(["catapult_id_x"],axis=1)
# player_data_mapping_df["catapult_id"] = player_data_mapping_df["catapult_id"].apply(lambda x: [] if x is np.nan else x)
# player_data_mapping_df['load_timestamp'] = load_timestamp
# player_data_mapping_df['smartabase_id'] = player_data_mapping_df['smartabase_id'].fillna(-1).astype(int)
# print(getPrettyDF(player_data_df.head(100)))
# print(getPrettyDF(final_data_df))
# player_data_mapping_df.to_csv("catapult_player_mapping.csv", index=False)
# insertToDB(session, player_data_mapping_df.fillna("").to_dict(orient="records"), "cricketsimulatordb", "playermapping", allow_logging=True)

# player_mapping_df = player_mapping_df.merge(data_df[['catapult_id', 'name']], left_on=player_mapping_df['name'].apply(
#     lambda x: x.strip().replace(" ", "").lower()),
#                                             right_on=data_df['name'].apply(
#                                                 lambda x: x.strip().replace(" ", "").lower()), how='left').rename(columns={"name_x": "name"}).drop(["name_y", "key_0"], axis=1)
# data_df['catapult_id'] = data_df['catapult_id'].fillna(-1)
# print(list(data_df['team_name'].unique()))


# print(final_data_df['catapult_id'].count())
# player_mapping_df.to_csv("player_mapping.csv", index=False)
# print(getPrettyDF(data_df[data_df['id'].isnull()].head(100)))


#
# sa20_teams_df = getPandasFactoryDF(session, '''select team_id, team_name from cricketsimulatordb.teams where competition_name='ILT20' ALLOW FILTERING;''')
# # # sa20_teams_df['seasons_played'] = sa20_teams_df['seasons_played'].apply(lambda x:[2023, 2024])
# # sa20_teams_df['load_timestamp'] = sa20_teams_df['load_timestamp'].astype(str)
# # # sa20_teams_df.to_csv('sa20_teams_df.csv', index=False)
# #
# # # insertToDB(session, sa20_teams_df.to_dict(orient="records"), "cricketsimulatordb", "teams", allow_logging=True)
# print(getPrettyDF(sa20_teams_df.head(100)))
# import numpy as np
# import datetime
# import os
# players_df = getPandasFactoryDF(session, f'''select * from {DB_NAME}.players where competition_name='SA20' and season=2024 ALLOW FILTERING;''')
# # players_df["player_rank"] = players_df.groupby(["player_id"])["season"].rank(method="first", ascending=False)
# # players_df = players_df[players_df["player_rank"] == 1]
# print(getPrettyDF(players_df.head(100)))
# import uuid
# player_mapping_df = getPandasFactoryDF(session, f'''select name, id as src_player_id, bowler_sub_type as bowling_type, is_batsman, is_bowler,is_wicket_keeper,
#  striker_batting_type as batting_type from {DB_NAME}.playerMapping''')
#
# new_players_df = readCSV("/Users/siddharth.nautiyal/Desktop/cricket/GIT/git-master/CricketDataService/data/squad_2024.csv")
# load_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# new_players_df = new_players_df.merge(players_df, left_on='player_name', right_on='player_name', how='left')
# # new_players_df = new_players_df.merge(player_mapping_df, left_on='player_name', right_on='name', how='left')
# available_players_df = new_players_df[~new_players_df['player_id'].isnull()].drop(['team_id', 'player_rank', 'load_timestamp'], axis=1)
# available_players_df['competition_name'] = "SA20"
# available_players_df['season'] = "2024"
# available_players_df['player_image_url'] = None
# available_players_df = available_players_df.merge(sa20_teams_df, on="team_name", how="left").drop(["team_name"], axis=1)
#
# not_available_players_df = new_players_df[new_players_df['player_id'].isnull()][['player_name', 'team_name']]
# not_available_players_df = not_available_players_df.merge(player_mapping_df, left_on='player_name', right_on='name', how='left')
# not_available_players_df['competition_name'] = "SA20"
# not_available_players_df['season'] = "2024"
# not_available_players_df['player_image_url'] = None
# not_available_players_df['is_captain'] = 0
# not_available_players_df['player_type'] = 'Overseas'
# not_available_players_df['bowl_major_type'] = np.where(
#     (not_available_players_df['bowling_type'] == 'LEFT ARM FAST') |
#     (not_available_players_df['bowling_type'] == 'RIGHT ARM FAST'),
#     'SEAM',
#     'SPIN'
# )
#
# skill_conditions = [
#     (not_available_players_df['is_batsman'] == 1) & (not_available_players_df['is_bowler'] == 0) | (not_available_players_df['is_wicket_keeper'] == 1),
#     (not_available_players_df['is_batsman'] == 1) & (not_available_players_df['is_bowler'] == 1),
#     (not_available_players_df['is_wicket_keeper'] == 1),
#     (not_available_players_df['is_batsman'] == 0) & (not_available_players_df['is_bowler'] == 1)
# ]
#
# # different batting phases
# skill_values = ["BATSMAN", "ALLROUNDER", "WICKETKEEPER", "BOWLER"]
#
# # create batting_phase column
# not_available_players_df['player_skill'] = np.select(skill_conditions, skill_values)
# not_available_players_df = not_available_players_df.merge(sa20_teams_df, on="team_name", how="left").drop(["team_name", "name"], axis=1)
# max_key_val = getMaxId(session, PLAYERS_TABLE_NAME, PLAYERS_KEY_COL, DB_NAME, False)
# # Add player id to new players
# not_available_players_df['player_id'] = not_available_players_df['src_player_id'].rank(
#     method='dense', ascending=False).apply(lambda x: x + max_key_val).astype(int)
# # new_players_df['hash'] = new_players_df['player_name'].apply(lambda x: str(uuid.uuid4()))
# final_players_df = not_available_players_df.append(available_players_df, ignore_index=True)
# final_players_df["load_timestamp"] = load_timestamp
# print(getPrettyDF(available_players_df.head(10)))
# print(getPrettyDF(final_players_df.head(100)))
