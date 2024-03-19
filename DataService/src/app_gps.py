import datetime as DT
import re
import sys

from DataIngestion import load_timestamp
from DataService.bg_tasks.calendar_create_update_task import CalendarCreateUpdateBGTask
from DataService.bg_tasks.calendar_delete_task import CalendarDeleteBGTask
from DataService.utils.catapult_duplicity import check_duplicates_fitness_agg
from common.authentication.role_config import SUPER_ADMIN
from common.utils.helper import getTeamsMapping
from third_party_service.notification.utils import get_roles_token, get_ums_sa_leagues
from third_party_service.ums import UMS

sys.path.append("./../../")
sys.path.append("./")
from third_party_service.constants import EMAIL_REGEX
from third_party_service.knightwatch.knightwatch import KnightWatch
from DataService.utils.constants import NOTIFICATION_TEMPLATE_ID
from scheduler.notification_scheduler import schedule_players_notification_cron
from scheduler.constants import WELLNESS_NOTIFICATION_MODULE
from scheduler.gps_report_scheduler import gps_periodic_scheduler
from third_party_service.notification.notification import Notification
from third_party_service.notification.payload import Payload
from third_party_service.notification.whatsapp.config import GPS_CAMPAIGN_TEMPLATE, WELLNESS_CAMPAIGN_TEMPLATE

from flask import Blueprint, request, Response, jsonify
from common.upload_images_to_blob.upload_image import dir_name, uploadImageToBlob
from DataIngestion.config import GPS_COLUMN_MAPPING, BASE_URL, TOKEN, GPS_SRC_KEY_MAPPING, GPS_TABLE_NAME, \
    STATS_API_NAME, GPS_AGG_DATA_GROUP_LIST, GPS_DELIVERY_SRC_KEY_MAPPING, GPS_DELIVERY_GROUP_LIST, \
    GPS_BALL_DECIMAL_COL_LIST, GPS_BALL_INT_COL_LIST, GPS_DELIVERY_TABLE_NAME, GPS_AGG_DECIMAL_COL_LIST, \
    GPS_AGG_INT_COL_LIST, DAILY_ACTIVITY_TABLE_NAME, PLAYER_LOAD_TABLE_NAME, PEAK_LOAD_TABLE_NAME, \
    USER_QUERY_TABLE_NAME, GPS_DELIVERY_JOIN_LIST, \
    RECIPIENT_GROUP_TABLE_NAME, \
    RECIPIENT_GROUP_COL, FIELD_ANALYSIS_FILE, FIELDING_ANALYSIS_TEMP, \
    GPS_AGG_DATA_JOIN_LIST, BOWL_PLANNING_TABLE_NAME, BOWL_PLANNING_KEY_COL, FIELDING_ANALYSIS_TABLE_NAME, \
    FIELDING_ANALYSIS_KEY_COL, EMIRATES_BASE_URL, EMIRATES_TOKEN, FILE_SHARE_PATH, CALENDAR_EVENT_TABLE_NAME, \
    CALENDAR_EVENT_KEY_COL, WPL_TOKEN
from DataIngestion.utils.helper import generateSeq
import math
from dateutil import tz
from DataService.utils.fielding_analysis import writeXlData, parseXl, decodeAndUploadXL, sum_ignore_negative, \
    matchNameConv, dupCheckBeforeInsert, encodeAndDeleteXl

from common.authentication.auth import token_required
from DataService.utils.gps_delivery_calculation import getDeliveryData, getGroupedGPSData, generateResponse, \
    getFormBowlingData, getGPSBowlingData
from DataIngestion.preprocessor.fitness_gps_data import generateGPSData, fetchGPSData, getGPSBallData, playerLoadData
from DataIngestion.query import GET_GPS_AGG_MAX_DATE, GET_GPS_BALL_MAX_DATE, GET_ALREADY_EXISTING_GPS_BALL_DATA, \
    GET_ALREADY_EXISTING_GPS_DATA
from DataService.fetch_sql_queries import GET_FITNESS_DATA, GET_GPS_DELIVERY_DATA, GET_FIELD_ANALYSIS
from DataService.utils.helper import filterDF, getNormalizedDf, validateRequest, dropFilter, getFormNotFilledPlayers, \
    getUpdateSetValues, updatePlannedBallData, updateFieldAnalysisData, open_api_spec, generate_api_function, \
    updateCalendarEventData
from common.dao.fetch_db_data import getMaxId
from common.dao.insert_data import insertToDB
from common.db_config import DB_NAME
from werkzeug.exceptions import HTTPException, BadRequest
from marshmallow import ValidationError
import numpy as np
from datetime import date, timedelta, timezone
from DataService.src import *

app_gps = Blueprint("app_gps", __name__)
open_api_spec = open_api_spec()

pd.options.mode.chained_assignment = None


@token_required
def getGPSAggData():
    logger = get_logger("getGPSAggData", "getGPSAggData")
    try:
        request_json = request.json
        list_keys = ["user_name", "user_id"]
        filter_dict = dropFilter(list_keys, request_json) if 'app' in request_json else request_json
        validateRequest(filter_dict)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        parameter_list = []
        if "team_name" in request_json:
            team_name = request_json.get('team_name')
            parameter_list.append(team_name)
            GET_FITNESS_DATA_SQL = GET_FITNESS_DATA + f" where team_name=? ALLOW FILTERING;"
        else:
            GET_FITNESS_DATA_SQL = GET_FITNESS_DATA

        fitness_data = \
            getPandasFactoryDF(session, GET_FITNESS_DATA_SQL, is_prepared=True, parameter_list=parameter_list)[
                ['record_date', 'team_name', 'total_distance_m', 'max_velocity_kmh', 'total_player_load', 'player_name',
                 'player_id']]

        fitness_data[['total_distance_m', 'max_velocity_kmh', 'total_player_load']] = fitness_data[
            ['total_distance_m', 'max_velocity_kmh', 'total_player_load']].apply(pd.to_numeric, errors='coerce').round(
            decimals=0)

        group_col = "team_name"

        if "record_date" in request.json:
            record_date = request.json['record_date']
            fitness_data = fitness_data[fitness_data['record_date'].isin(record_date)]
            if len(record_date) == 1:
                group_col = 'record_date'

        elif all(key in request.json for key in ('from_date', 'to_date')):
            from_date = request.json.get('from_date')
            from_date = datetime.strptime(
                datetime.strptime(str(from_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()
            to_date = request.json.get('to_date')
            to_date = datetime.strptime(
                datetime.strptime(str(to_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()
            fitness_data['record_date_form'] = fitness_data['record_date'].apply(lambda x: datetime.strptime(
                datetime.strptime(str(x), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date())
            fitness_data = fitness_data[
                (fitness_data['record_date_form'] >= from_date) & (fitness_data['record_date_form'] <= to_date)].drop(
                'record_date_form', axis=1)

            if "user_name" in request_json:
                player_name = request_json["user_name"]
                fitness_data = fitness_data[fitness_data['player_name'].isin([player_name])]

            if "user_id" in request_json:
                player_id = request_json["user_id"]
                fitness_data = fitness_data[fitness_data['player_id'].isin([player_id])]

            group_col = 'player_id'
        else:
            fitness_data = fitness_data
            group_col = 'record_date'

        if "player_name" in request.json:
            player_name = request.json['player_name']
            fitness_data = fitness_data[fitness_data['player_name'].isin(player_name)]

        if "player_id" in request.json:
            player_id = request.json['player_id']
            fitness_data = fitness_data[fitness_data['player_id'].isin(player_id)]

        if len(fitness_data) > 0:
            gps_fitness_agg_df = fitness_data.groupby([group_col]).mean().round(decimals=0).astype(int) \
                .rename(
                columns={'total_distance_m': 'Average Distance (m)', 'max_velocity_kmh': 'Average HSR Distance(m)',
                         'total_player_load': 'Average Player Load'})
            if "player_id" in gps_fitness_agg_df.columns:
                gps_fitness_agg_df = gps_fitness_agg_df.drop(["player_id"], axis=1)

            response = jsonify(gps_fitness_agg_df.to_dict('records'))
        else:
            response = jsonify({})

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def getDistanceData():
    logger = get_logger("getDistanceData", "getDistanceData")
    try:
        filters = request.json.copy()
        req_filters = request.json
        list_keys = ["Velocity Band 1 Total Distance (m)", "Velocity Band 2 Total Distance (m)",
                     "Velocity Band 3 Total Distance (m)",
                     "Velocity Band 4 Total Distance (m)", "Velocity Band 5 Total Distance (m)", "Total Distance (m)"]
        list_keys.extend(["user_name", "user_id"]) if 'app' in req_filters else list_keys
        validateRequest(dropFilter(list_keys, req_filters))
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        group_list = ["player_name"]
        parameter_list = []
        if "team_name" in req_filters:
            team_name = req_filters.get('team_name')
            parameter_list.append(team_name)
            GET_FITNESS_DATA_SQL = GET_FITNESS_DATA + f" where team_name=? ALLOW FILTERING;"
        else:
            GET_FITNESS_DATA_SQL = GET_FITNESS_DATA

        fitness_data = \
            getPandasFactoryDF(session, GET_FITNESS_DATA_SQL, is_prepared=True, parameter_list=parameter_list)[
                ['record_date', 'player_name', 'player_id', 'vel_b1_tot_dist_m', 'vel_b2_tot_dist_m',
                 'vel_b3_tot_dist_m',
                 'vel_b4_tot_dist_m', 'vel_b5_tot_dist_m', 'total_distance_m']]
        fitness_data[
            ['vel_b1_tot_dist_m', 'vel_b2_tot_dist_m', 'vel_b3_tot_dist_m', 'vel_b4_tot_dist_m', 'vel_b5_tot_dist_m',
             'total_distance_m']] = fitness_data[
            ['vel_b1_tot_dist_m', 'vel_b2_tot_dist_m', 'vel_b3_tot_dist_m', 'vel_b4_tot_dist_m', 'vel_b5_tot_dist_m',
             'total_distance_m']].apply(pd.to_numeric, errors='coerce').round(decimals=0)

        column_mapping_new = dict([(value, key) for key, value in GPS_COLUMN_MAPPING.items()])

        if filters:
            for key, value in filters.items():
                if key == "record_date":
                    fitness_data = fitness_data[fitness_data['record_date'].isin(value)]
                elif key == "player_name":
                    group_list = ["player_name"]
                    fitness_data = fitness_data[fitness_data['player_name'].isin(value)]
                elif key == "player_id":
                    group_list = ["player_id"]
                    fitness_data = fitness_data[fitness_data['player_id'].isin(value)]
                elif all(key in req_filters for key in ('from_date', 'to_date')):
                    group_list = ["record_date", "player_id", "player_name"]
                    from_date = req_filters.get('from_date')
                    from_date = datetime.strptime(
                        datetime.strptime(str(from_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()
                    to_date = req_filters.get('to_date')
                    to_date = datetime.strptime(
                        datetime.strptime(str(to_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()
                    fitness_data['record_date_form'] = fitness_data['record_date'].apply(lambda x: datetime.strptime(
                        datetime.strptime(str(x), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date())
                    fitness_data = fitness_data[
                        (fitness_data['record_date_form'] >= from_date) & (
                                fitness_data['record_date_form'] <= to_date)].drop('record_date_form', axis=1)
                    if "user_name" in req_filters:
                        fitness_data = fitness_data[fitness_data['player_name'].isin([req_filters.get('user_name')])]
                    if "user_id" in req_filters:
                        fitness_data = fitness_data[fitness_data['player_id'].isin([req_filters.get('user_id')])]
                else:
                    fitness_data = fitness_data

        if 'agg_type' in request.json:
            if request.json['agg_type'] == 'mean':
                fitness_data = fitness_data.groupby(group_list).agg(
                    {'vel_b1_tot_dist_m': 'mean', 'vel_b2_tot_dist_m': 'mean',
                     'vel_b3_tot_dist_m': 'mean', 'vel_b4_tot_dist_m': 'mean',
                     'vel_b5_tot_dist_m': 'mean',
                     'total_distance_m': 'mean'}).reset_index()
            else:
                fitness_data = fitness_data.groupby(group_list).agg(
                    {'vel_b1_tot_dist_m': 'sum', 'vel_b2_tot_dist_m': 'sum',
                     'vel_b3_tot_dist_m': 'sum', 'vel_b4_tot_dist_m': 'sum',
                     'vel_b5_tot_dist_m': 'sum',
                     'total_distance_m': 'sum'}).reset_index()
        else:
            fitness_data = fitness_data.groupby(group_list).agg(
                {'vel_b1_tot_dist_m': 'sum', 'vel_b2_tot_dist_m': 'sum',
                 'vel_b3_tot_dist_m': 'sum', 'vel_b4_tot_dist_m': 'sum',
                 'vel_b5_tot_dist_m': 'sum',
                 'total_distance_m': 'sum'}).reset_index()

        stats_list_keys = ['record_date', 'player_name', 'from_date', 'to_date', 'user_name', 'player_id']
        stats_filters = dropFilter(stats_list_keys, filters)
        if stats_filters:
            for key, value in stats_filters.items():
                if key == "Velocity Band 1 Total Distance (m)":
                    fitness_data = filterDF(fitness_data, 'vel_b1_tot_dist_m', value[0], value[1])
                elif key == "Velocity Band 2 Total Distance (m)":
                    fitness_data = filterDF(fitness_data, 'vel_b2_tot_dist_m', value[0], value[1])
                elif key == "Velocity Band 3 Total Distance (m)":
                    fitness_data = filterDF(fitness_data, 'vel_b3_tot_dist_m', value[0], value[1])
                elif key == "Velocity Band 4 Total Distance (m)":
                    fitness_data = filterDF(fitness_data, 'vel_b4_tot_dist_m', value[0], value[1])
                elif key == "Velocity Band 5 Total Distance (m)":
                    fitness_data = filterDF(fitness_data, 'vel_b5_tot_dist_m', value[0], value[1])
                elif key == "Total Distance (m)":
                    fitness_data = filterDF(fitness_data, 'total_distance_m', value[0], value[1])

        if 'record_date' in fitness_data.columns:
            fitness_data['record_date'] = pd.to_datetime(fitness_data['record_date'], format='%A, %b %d, %Y')
            fitness_data = fitness_data.sort_values(by='record_date', ascending=False)
            fitness_data['record_date'] = fitness_data['record_date'].dt.strftime('%A, %b %d, %Y')

        return jsonify(fitness_data.rename(columns=column_mapping_new).to_dict(
            orient='records')), 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def getMaxVelocity():
    logger = get_logger("getMaxVelocity", "getMaxVelocity")
    try:
        filters = request.json.copy()
        req_filters = request.json
        list_keys = ["Velocity Band 5 Total Distance (m)", "Maximum Velocity (km/h)"]
        list_keys.extend(["user_name", "user_id"]) if 'app' in req_filters else list_keys
        validateRequest(dropFilter(list_keys, req_filters))
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        parameter_list = []
        if "team_name" in req_filters:
            team_name = req_filters.get('team_name')
            parameter_list.append(team_name)
            GET_FITNESS_DATA_SQL = GET_FITNESS_DATA + f" where team_name=? ALLOW FILTERING;"
        else:
            GET_FITNESS_DATA_SQL = GET_FITNESS_DATA

        fitness_data = \
            getPandasFactoryDF(session, GET_FITNESS_DATA_SQL, is_prepared=True, parameter_list=parameter_list)[
                ['record_date', 'player_id', 'player_name', 'max_velocity_kmh', 'vel_b5_tot_dist_m']]

        fitness_data[
            ['vel_b5_tot_dist_m', 'max_velocity_kmh']] = fitness_data[['vel_b5_tot_dist_m', 'max_velocity_kmh']] \
            .apply(pd.to_numeric, errors='coerce').round(decimals=0)

        column_mapping_new = dict([(value, key) for key, value in GPS_COLUMN_MAPPING.items()])
        group_list = ["player_name"]
        if filters:
            for key, value in filters.items():
                if key == "record_date":
                    fitness_data = fitness_data[fitness_data['record_date'].isin(value)]
                elif key == "player_name":
                    fitness_data = fitness_data[fitness_data['player_name'].isin(value)]
                    group_list = ["player_name"]
                elif key == "player_id":
                    fitness_data = fitness_data[fitness_data['player_id'].isin(value)]
                    group_list = ["player_id"]
                elif all(key in req_filters for key in ('from_date', 'to_date')):
                    group_list = ["player_name", "player_id", "record_date"]
                    from_date = req_filters.get('from_date')
                    from_date = datetime.strptime(
                        datetime.strptime(str(from_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()
                    to_date = req_filters.get('to_date')
                    to_date = datetime.strptime(
                        datetime.strptime(str(to_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()
                    fitness_data['record_date_form'] = fitness_data['record_date'].apply(lambda x: datetime.strptime(
                        datetime.strptime(str(x), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date())
                    fitness_data = fitness_data[
                        (fitness_data['record_date_form'] >= from_date) & (
                                fitness_data['record_date_form'] <= to_date)].drop('record_date_form', axis=1)
                    if "user_name" in req_filters:
                        fitness_data = fitness_data[fitness_data['player_name'].isin([req_filters.get('user_name')])]
                    if "user_id" in req_filters:
                        fitness_data = fitness_data[fitness_data['player_id'].isin([req_filters.get('user_id')])]
                else:
                    fitness_data = fitness_data

        if 'agg_type' in request.json:
            if request.json['agg_type'] == 'mean':
                fitness_data = fitness_data.groupby(group_list).agg(
                    {'vel_b5_tot_dist_m': 'mean', 'max_velocity_kmh': 'mean'}).reset_index()
            else:
                fitness_data = fitness_data.groupby(group_list).agg(
                    {'vel_b5_tot_dist_m': 'sum', 'max_velocity_kmh': 'sum'}).reset_index()
        else:
            fitness_data = fitness_data.groupby(group_list).agg(
                {'vel_b5_tot_dist_m': 'sum', 'max_velocity_kmh': 'sum'}).reset_index()

        stats_list_keys = ['record_date', 'player_id', 'player_name', 'from_date', 'to_date', 'user_name']
        stats_filters = dropFilter(stats_list_keys, filters)
        if stats_filters:
            for key, value in stats_filters.items():
                if key == "Velocity Band 5 Total Distance (m)":
                    fitness_data = filterDF(fitness_data, 'vel_b5_tot_dist_m', value[0], value[1])
                elif key == "Maximum Velocity (km/h)":
                    fitness_data = filterDF(fitness_data, 'max_velocity_kmh', value[0], value[1])

        if 'record_date' in fitness_data.columns:
            fitness_data['record_date'] = pd.to_datetime(fitness_data['record_date'], format='%A, %b %d, %Y')
            fitness_data = fitness_data.sort_values(by='record_date', ascending=False)
            fitness_data['record_date'] = fitness_data['record_date'].dt.strftime('%A, %b %d, %Y')

        return jsonify(fitness_data.rename(columns=column_mapping_new).to_dict(
            orient='records')), 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def getAccelDecel():
    logger = get_logger("getAccelDecel", "getAccelDecel")
    try:
        filters = request.json.copy()
        req_filters = request.json
        list_keys = ["Acceleration B2 Efforts (Gen 2)", "Deceleration B2 Efforts (Gen 2)"]
        list_keys.extend(["user_name", "user_id"]) if 'app' in req_filters else list_keys
        validateRequest(dropFilter(list_keys, req_filters))
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        parameter_list = []
        if "team_name" in req_filters:
            team_name = req_filters.get('team_name')
            parameter_list.append(team_name)
            GET_FITNESS_DATA_SQL = GET_FITNESS_DATA + f" where team_name=? ALLOW FILTERING;"
        else:
            GET_FITNESS_DATA_SQL = GET_FITNESS_DATA

        fitness_data = \
            getPandasFactoryDF(session, GET_FITNESS_DATA_SQL, is_prepared=True, parameter_list=parameter_list)[
                ['record_date', 'player_id', 'player_name', 'accel_b2_eff_gen2', 'decel_b2_eff_gen2']]

        group_list = ['player_name']
        column_mapping_new = dict([(value, key) for key, value in GPS_COLUMN_MAPPING.items()])
        if filters:
            for key, value in filters.items():
                if key == "record_date":
                    fitness_data = fitness_data[fitness_data['record_date'].isin(value)]
                elif key == "player_name":
                    group_list = ['player_name']
                    fitness_data = fitness_data[fitness_data['player_name'].isin(value)]
                elif key == "player_id":
                    group_list = ['player_id']
                    fitness_data = fitness_data[fitness_data['player_id'].isin(value)]
                elif all(key in req_filters for key in ('from_date', 'to_date')):
                    group_list = ['record_date', 'player_id', 'player_name']
                    from_date = req_filters.get('from_date')
                    from_date = datetime.strptime(
                        datetime.strptime(str(from_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()
                    to_date = req_filters.get('to_date')
                    to_date = datetime.strptime(
                        datetime.strptime(str(to_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()
                    fitness_data['record_date_form'] = fitness_data['record_date'].apply(lambda x: datetime.strptime(
                        datetime.strptime(str(x), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date())
                    fitness_data = fitness_data[
                        (fitness_data['record_date_form'] >= from_date) & (
                                fitness_data['record_date_form'] <= to_date)].drop('record_date_form', axis=1)
                    if "user_name" in req_filters:
                        fitness_data = fitness_data[fitness_data['player_name'].isin([req_filters.get('user_name')])]
                    if "user_id" in req_filters:
                        fitness_data = fitness_data[fitness_data['user_id'].isin([req_filters.get('user_id')])]
                else:
                    fitness_data = fitness_data

        if 'agg_type' in request.json:
            if request.json['agg_type'] == 'mean':
                fitness_data = fitness_data.groupby(group_list).agg(
                    {'accel_b2_eff_gen2': 'mean', 'decel_b2_eff_gen2': 'mean'}).reset_index()
            else:
                fitness_data = fitness_data.groupby(group_list).agg(
                    {'accel_b2_eff_gen2': 'sum', 'decel_b2_eff_gen2': 'sum'}).reset_index()
        else:
            fitness_data = fitness_data.groupby(group_list).agg(
                {'accel_b2_eff_gen2': 'sum', 'decel_b2_eff_gen2': 'sum'}).reset_index()

        stats_list_keys = ['record_date', 'player_id', 'player_name', 'from_date', 'to_date', 'user_name']
        stats_filters = dropFilter(stats_list_keys, filters)
        if stats_filters:
            for key, value in stats_filters.items():
                if key == "Acceleration B2 Efforts (Gen 2)":
                    fitness_data = filterDF(fitness_data, 'accel_b2_eff_gen2', value[0], value[1])
                elif key == "Deceleration B2 Efforts (Gen 2)":
                    fitness_data = filterDF(fitness_data, 'decel_b2_eff_gen2', value[0], value[1])

        if 'record_date' in fitness_data.columns:
            fitness_data['record_date'] = pd.to_datetime(fitness_data['record_date'], format='%A, %b %d, %Y')
            fitness_data = fitness_data.sort_values(by='record_date', ascending=False)
            fitness_data['record_date'] = fitness_data['record_date'].dt.strftime('%A, %b %d, %Y')

        return jsonify(fitness_data.rename(columns=column_mapping_new).to_dict(
            orient='records')), 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def getPlayerLoad():
    logger = get_logger("getPlayerLoad", "getPlayerLoad")
    try:
        filters = request.json.copy()
        req_filters = request.json
        list_keys = ["Total Player Load"]
        list_keys.extend(["user_name", "user_id"]) if 'app' in req_filters else list_keys
        validateRequest(dropFilter(list_keys, req_filters))
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        parameter_list = []
        if "team_name" in req_filters:
            team_name = req_filters.get('team_name')
            parameter_list.append(team_name)
            GET_FITNESS_DATA_SQL = GET_FITNESS_DATA + f" where team_name=? ALLOW FILTERING;"
        else:
            GET_FITNESS_DATA_SQL = GET_FITNESS_DATA

        fitness_data = \
            getPandasFactoryDF(session, GET_FITNESS_DATA_SQL, is_prepared=True, parameter_list=parameter_list)[
                ['record_date', 'player_name', 'player_id', 'total_player_load']]
        fitness_data['total_player_load'] = fitness_data['total_player_load'].apply(pd.to_numeric,
                                                                                    errors='coerce').round(decimals=0)
        group_list = ['player_name']
        column_mapping_new = dict([(value, key) for key, value in GPS_COLUMN_MAPPING.items()])
        if filters:
            for key, value in filters.items():
                if key == "record_date":
                    fitness_data = fitness_data[fitness_data['record_date'].isin(value)]
                elif key == "player_name":
                    group_list = ['player_name']
                    fitness_data = fitness_data[fitness_data['player_name'].isin(value)]
                elif key == "player_id":
                    group_list = ['player_id']
                    fitness_data = fitness_data[fitness_data['player_id'].isin(value)]
                elif all(key in req_filters for key in ('from_date', 'to_date')):
                    group_list = ['player_name', 'player_id', 'record_date']
                    from_date = req_filters.get('from_date')
                    from_date = datetime.strptime(
                        datetime.strptime(str(from_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()
                    to_date = req_filters.get('to_date')
                    to_date = datetime.strptime(
                        datetime.strptime(str(to_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()
                    fitness_data['record_date_form'] = fitness_data['record_date'].apply(lambda x: datetime.strptime(
                        datetime.strptime(str(x), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date())
                    fitness_data = fitness_data[
                        (fitness_data['record_date_form'] >= from_date) & (
                                fitness_data['record_date_form'] <= to_date)].drop('record_date_form', axis=1)
                    if "user_name" in req_filters:
                        fitness_data = fitness_data[fitness_data['player_name'].isin([req_filters.get('user_name')])]
                    if "user_id" in req_filters:
                        fitness_data = fitness_data[fitness_data['player_id'].isin([req_filters.get('user_id')])]

                else:
                    fitness_data = fitness_data

        if 'agg_type' in request.json:
            if request.json['agg_type'] == 'mean':
                fitness_data = fitness_data.groupby(group_list).agg({'total_player_load': 'mean'}).reset_index()
            else:
                fitness_data = fitness_data.groupby(group_list).agg({'total_player_load': 'sum'}).reset_index()
        else:
            fitness_data = fitness_data.groupby(group_list).agg({'total_player_load': 'sum'}).reset_index()

        stats_list_keys = ['record_date', 'player_id', 'player_name', 'from_date', 'to_date', 'user_name']
        stats_filters = dropFilter(stats_list_keys, filters)
        if stats_filters:
            for key, value in stats_filters.items():
                if key == "Total Player Load":
                    fitness_data = filterDF(fitness_data, 'total_player_load', value[0], value[1])

        if 'record_date' in fitness_data.columns:
            fitness_data['record_date'] = pd.to_datetime(fitness_data['record_date'], format='%A, %b %d, %Y')
            fitness_data = fitness_data.sort_values(by='record_date', ascending=False)
            fitness_data['record_date'] = fitness_data['record_date'].dt.strftime('%A, %b %d, %Y')

        return jsonify(fitness_data.rename(columns=column_mapping_new).to_dict(
            orient='records')), 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def getGPSFilters():
    logger = get_logger("getGPSFilters", "getGPSFilters")
    try:

        fitness_data = getPandasFactoryDF(session, GET_FITNESS_DATA)[['record_date', 'player_name', 'team_name']]
        fitness_data['season'] = fitness_data['record_date'].apply(
            lambda x: datetime.strptime(str(x), '%A, %b %d, %Y').strftime('%Y')
        ).astype(int)

        players_sql = '''
        select 
          pd.player_name, 
          td.team_short_name,
          pd.season
        from 
          players_data pd 
          inner join teams_data td on (pd.team_id = td.team_id) 
        where 
          td.team_name in ('MUMBAI INDIANS', 'MI NEW YORK', 'MI EMIRATES', 'MI CAPE TOWN', 'MUMBAI INDIANS WOMEN')
          and pd.season>=2019
        '''
        season_data = executeQuery(con, players_sql)
        season_data["team_name"] = season_data["team_short_name"].apply(lambda x: getTeamsMapping()[x.replace(" ", "")])

        if request.args:
            player_name = [request.args.get("player_name")]
            fitness_data = fitness_data.loc[fitness_data['player_name'].isin(player_name)]

        team_name = list(fitness_data['team_name'].unique())
        season_data = season_data[season_data['team_name'].isin(team_name)]

        folder_name = 'mi-teams-images/'
        season_data['player_image_url'] = IMAGE_STORE_URL + folder_name + season_data['team_name'].apply(lambda x: x.replace(' ', '-').lower()).astype(str) + '/'+\
                                          season_data['player_name'].apply(
            lambda x: x.replace(' ', '-').lower()
        ).astype(str) + ".png"
        defaulting_image_url(season_data, 'player_image_url', 'team_name', 'Mumbai Indian Womens', folder_name)

        season_data['player_details'] = season_data[['player_name', 'player_image_url']].to_dict(orient='records')
        season_data['player_details'] = season_data['player_details'].apply(
            lambda x: {x['player_name']: x['player_image_url']}
        )
        season_wise_player = season_data.groupby(['season', 'team_name']).agg({"player_details": list}).reset_index()

        date_df = fitness_data.groupby('team_name')['record_date'].apply(
            lambda x: list(np.unique(x))
        ).reset_index()

        fitness_data = fitness_data.groupby(["record_date", "team_name"]).agg(
            {"player_name": lambda x: list(x)}
        ).reset_index()

        response = fitness_data.groupby('team_name').apply(
            lambda x: x.set_index('record_date')[['player_name']].to_dict(orient='index')
        ).to_dict()

        for team in date_df['team_name'].unique():
            record_date = date_df.query(f"team_name=='{team}'")['record_date']
            record_date_list = record_date.tolist()
            first_element = record_date_list[0] if record_date_list else None
            if first_element:
                response[team]['record_date'] = sorted(first_element,
                                                       key=lambda x: pd.to_datetime(x, format='%A, %b %d, %Y'),
                                                       reverse=True)
            else:
                response[team]['record_date'] = []

        for team in season_wise_player['team_name'].unique():
            response[team]['player_name'] = season_wise_player.query(f"team_name=='{team}'").set_index('season')[
                'player_details'].to_dict()
            response[team]['player_name'] = season_wise_player.query(f"team_name=='{team}'").set_index('season')[
                'player_details'].to_dict()
        return response, 200, logger.info("Status - 200")
    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def getWellnessFilters():
    logger = get_logger("getWellnessFilters", "getWellnessFilters")
    try:

        fitness_data = getPandasFactoryDF(session, GET_FITNESS_DATA)[
            ['record_date', 'player_name', 'team_name', 'player_id']]
        fitness_data['season'] = fitness_data['record_date'].apply(
            lambda x: datetime.strptime(str(x), '%A, %b %d, %Y').strftime('%Y'))

        players_sql = '''
        select 
          pd.player_name, 
          td.team_short_name,
          pd.season,
          cast(pd.src_player_id as int) as player_id
        from 
          players_data pd 
          inner join teams_data td on (pd.team_id = td.team_id) 
        where 
          td.team_name in ('MUMBAI INDIANS', 'MI NEW YORK', 'MI EMIRATES', 'MI CAPE TOWN', 'MUMBAI INDIANS WOMEN')
          and pd.season>=2022
        '''
        season_data = executeQuery(con, players_sql)
        season_data["team_name"] = season_data["team_short_name"].apply(lambda x: getTeamsMapping()[x.replace(" ", "")])
        folder_name = 'mi-teams-images/'
        season_data['player_image_url'] = (IMAGE_STORE_URL + folder_name + season_data['team_name'].apply(lambda x: x.replace(' ', '-').lower()).astype(str) + '/' +
                                           season_data[
                                               'player_name'].apply(
                                               lambda x: x.replace(' ', '-').lower()).astype(str) + ".png")
        defaulting_image_url(season_data, 'player_image_url', 'team_name', 'Mumbai Indian Womens', folder_name)
        team_names = list(season_data["team_name"].unique())
        if request.args:
            if "team_name" in request.args:
                team_name = request.args.get("team_name", "Mumbai Indians")
                season_data = season_data[season_data["team_name"] == team_name]
                fitness_data = fitness_data[fitness_data["team_name"] == team_name]

        season_data['player_details'] = season_data[['player_id', 'player_name', 'player_image_url']].to_dict(
            orient='records')

        season_data = season_data.groupby(['team_name', 'season']).agg(
            {"player_details": lambda x: list(x)}).reset_index()

        response = season_data.groupby('team_name').apply(
            lambda x: x.set_index('season')[['player_details']].to_dict(orient='index')).to_dict()

        date_df = fitness_data.groupby('team_name')['record_date'].apply(
            lambda x: list(np.unique(x))
        ).reset_index()

        fitness_data['player_details'] = fitness_data[['player_name', 'player_id']].to_dict(orient='records')
        fitness_data['player_details'] = fitness_data['player_details'].apply(
            lambda x: {x['player_id']: x['player_name']})
        fitness_data = fitness_data.groupby(["team_name", "record_date"])["player_details"].agg(list).reset_index()
        fitness_data['player_details'] = fitness_data['player_details'].apply(
            lambda x: {k: v for d in x for k, v in d.items()})

        response["team_name"] = team_names
        for team in date_df['team_name'].unique():
            record_date = date_df.query(f"team_name=='{team}'")['record_date']
            record_date_list = record_date.tolist()
            first_element = record_date_list[0] if record_date_list else None
            if first_element:
                response[team]['record_date'] = sorted(first_element,
                                                       key=lambda x: pd.to_datetime(x, format='%A, %b %d, %Y'),
                                                       reverse=True)
            else:
                response[team]['record_date'] = []

        for team in fitness_data['team_name'].unique():
            response[team]["record_date_details"] = fitness_data.query(f"team_name=='{team}'").set_index('record_date')[
                'player_details'].to_dict()

        return json.dumps(response), 200, logger.info("Status - 200")
    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


def fetchLatestGPSData():
    logger = get_logger("fetchLatestGPSData", "fetchLatestGPSData")
    try:
        if request.args:
            team_list = [request.args.get('team_list')]
        else:
            team_list = ["Mumbai Indians", "MI Capetown", "MI New York", "MI Emirates", "Mumbai Indian Womens"]

        for team in team_list:
            if team == "MI Emirates":
                token = EMIRATES_TOKEN
                base_url = EMIRATES_BASE_URL
            elif team == "Mumbai Indian Womens":
                token = WPL_TOKEN
                base_url = EMIRATES_BASE_URL
            else:
                token = TOKEN
                base_url = BASE_URL

            gps_agg_data = generateGPSData(
                fetchGPSData(base_url, STATS_API_NAME, GET_GPS_AGG_MAX_DATE, token, GPS_SRC_KEY_MAPPING,
                             GPS_AGG_DATA_GROUP_LIST, teams_list=team_list), GET_ALREADY_EXISTING_GPS_DATA,
                GPS_AGG_DATA_JOIN_LIST,
                gps_decimal_columns=GPS_AGG_DECIMAL_COL_LIST,
                gps_int_columns=GPS_AGG_INT_COL_LIST)

            if not gps_agg_data.empty:
                insertToDB(session, gps_agg_data.to_dict(orient='records'), DB_NAME, GPS_TABLE_NAME)
                check_duplicates_fitness_agg()
            else:
                logger.info("No New Data Available")

            gps_ball_data = getGPSBallData(generateGPSData(
                fetchGPSData(base_url, STATS_API_NAME, GET_GPS_BALL_MAX_DATE, token, GPS_DELIVERY_SRC_KEY_MAPPING,
                             GPS_DELIVERY_GROUP_LIST, teams_list=team_list), GET_ALREADY_EXISTING_GPS_BALL_DATA,
                GPS_DELIVERY_JOIN_LIST,
                gps_decimal_columns=GPS_BALL_DECIMAL_COL_LIST,
                gps_int_columns=GPS_BALL_INT_COL_LIST))

            if len(gps_ball_data) > 0:
                insertToDB(session, gps_ball_data, DB_NAME, GPS_DELIVERY_TABLE_NAME)
            else:
                logger.info("No New Data Available")

        return jsonify({}), 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def getGPSDeliveryStats():
    logger = get_logger("getGPSDeliveryStats", "getGPSDeliveryStats")
    try:
        filters = request.json
        validateRequest(filters)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        parameter_list = []
        if "team_name" in filters:
            team_name = filters.get('team_name')
            parameter_list.append(team_name)
            GET_GPS_DELIVERY_DATA_SQL = GET_GPS_DELIVERY_DATA + f" and team_name=? ALLOW FILTERING;"
        else:
            GET_GPS_DELIVERY_DATA_SQL = GET_GPS_DELIVERY_DATA

        gps_delivery_data = getPandasFactoryDF(session, GET_GPS_DELIVERY_DATA_SQL, is_prepared=True,
                                               parameter_list=parameter_list)
        gps_delivery_data[['delivery_runup_distance', 'peak_player_load', 'raw_peak_roll', 'raw_peak_yaw']] = \
            gps_delivery_data[
                ['delivery_runup_distance', 'peak_player_load', 'raw_peak_roll', 'raw_peak_yaw']].apply(pd.to_numeric,
                                                                                                        errors='coerce').round(
                decimals=0)

        match_ball_data = mi_ball_data
        gps_ball_data = gps_delivery_data
        if "player_name" in request.json:
            player_name = request.json["player_name"]
            gps_ball_data = gps_delivery_data.loc[gps_delivery_data['player_name'].isin(player_name)]
            match_ball_data = match_ball_data.loc[match_ball_data['bowler_name'].isin(player_name)]

        if "player_id" in request.json:
            player_id = request.json["player_id"]
            gps_ball_data = gps_delivery_data.loc[gps_delivery_data['player_id'].isin(player_id)]
            match_ball_data = match_ball_data.loc[match_ball_data['src_bowler_id'].isin(player_id)]

        if "user_name" in request.json:
            player_name = [request.json["user_name"]]
            gps_ball_data = gps_delivery_data.loc[gps_delivery_data['player_name'].isin(player_name)]
            match_ball_data = match_ball_data.loc[match_ball_data['bowler_name'].isin(player_name)]

        if "user_id" in request.json:
            player_id = [request.json["user_id"]]
            gps_ball_data = gps_delivery_data.loc[gps_delivery_data['player_id'].isin(player_id)]
            match_ball_data = match_ball_data.loc[match_ball_data['src_bowler_id'].isin(player_id)]

        if "season" in request.json:
            season = request.json.get('season')
            gps_ball_data = gps_ball_data.loc[gps_ball_data['season'].isin(season)]
            match_ball_data = match_ball_data.loc[match_ball_data['season'].isin(season)]

        data_df = getDeliveryData(match_ball_data, gps_ball_data)

        if len(data_df) > 0:
            normalization_columns = ['delivery_runup_distance', 'peak_player_load', 'raw_peak_roll',
                                     'raw_peak_yaw']
            normalization_group_list = ['match_id', 'match_name', 'player_id', 'season', 'player_name', 'bowl_length',
                                        'over_no', 'player_image_url']
            normalized_df = getNormalizedDf(data_df, normalization_columns, normalization_group_list)

            agg_key = request.json.get('agg_key')
            agg_type = request.json.get('agg_type')

            if agg_key == 'match':
                group_list = ['match_id', 'match_name', 'player_id', 'player_name', 'bowl_length', 'player_image_url']
                req_col_list = ['match_id', 'match_name', 'player_id', 'player_name', 'bowl_length',
                                'delivery_runup_distance', 'peak_player_load', 'raw_peak_roll',
                                'raw_peak_yaw', 'player_image_url']
                final_df = getGroupedGPSData(normalized_df, agg_type, req_col_list, group_list)

                if final_df.empty:
                    response = jsonify({})
                else:
                    response = generateResponse(final_df,
                                                ['match_name', 'player_id', 'player_name', 'player_image_url'],
                                                ['bowl_length', 'delivery_runup_distance',
                                                 'peak_player_load', 'raw_peak_roll',
                                                 'raw_peak_yaw'])

            elif agg_key == 'over':
                req_col_list = ['over_no', 'player_id', 'player_name', 'bowl_length', 'delivery_runup_distance',
                                'peak_player_load', 'raw_peak_roll', 'raw_peak_yaw', 'player_image_url']
                group_list = ['over_no', 'player_id', 'player_name', 'bowl_length', 'player_image_url']
                final_df = getGroupedGPSData(normalized_df, agg_type, req_col_list, group_list)

                if 'over_no' in request.json:
                    over_no = request.json.get('over_no')
                    final_df = final_df.loc[final_df['over_no'].isin(over_no)]

                if final_df.empty:
                    response = jsonify({})
                else:
                    response = generateResponse(final_df, ['over_no', 'player_id', 'player_name', 'player_image_url'],
                                                ['bowl_length', 'delivery_runup_distance',
                                                 'peak_player_load', 'raw_peak_roll',
                                                 'raw_peak_yaw'])

            elif agg_key == 'season':
                group_list = ['season', 'player_id', 'player_name', 'bowl_length', 'player_image_url']
                req_col_list = ['season', 'player_id', 'player_name', 'bowl_length', 'delivery_runup_distance',
                                'peak_player_load', 'raw_peak_roll', 'raw_peak_yaw', 'player_image_url']
                final_df = getGroupedGPSData(normalized_df, agg_type, req_col_list, group_list)
                if final_df.empty:
                    response = jsonify({})
                else:
                    response = generateResponse(final_df, ['season', 'player_id', 'player_name', 'player_image_url'],
                                                ['bowl_length', 'delivery_runup_distance',
                                                 'peak_player_load', 'raw_peak_roll',
                                                 'raw_peak_yaw'])

            else:
                group_list = ['season', 'player_id', 'player_name', 'bowl_length', 'player_image_url']
                req_col_list = ['season', 'player_id', 'player_name', 'bowl_length', 'delivery_runup_distance',
                                'peak_player_load', 'raw_peak_roll', 'raw_peak_yaw', 'player_image_url']
                final_df = getGroupedGPSData(normalized_df, agg_type, req_col_list, group_list)
                if final_df.empty:
                    response = jsonify({})
                else:
                    response = generateResponse(final_df, ['season', 'player_id', 'player_name', 'player_image_url'],
                                                ['bowl_length', 'delivery_runup_distance',
                                                 'peak_player_load', 'raw_peak_roll',
                                                 'raw_peak_yaw'])
        else:
            response = jsonify({})

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def getPlayerReadiness():
    logger = get_logger("getPlayerReadiness", "getPlayerReadiness")
    try:
        req_filters = request.json
        validateRequest(req_filters)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        FITNESS_FORM_SQL = f'''select record_date, player_id, player_name, reason_noplay_or_train 
         from {DB_NAME}.fitnessForm '''

        PLAYER_LOAD_SQL = f'''select record_date, player_id, player_name, total_load,bowling_match_balls 
             from {DB_NAME}.playerLoad '''
        parameter_list = []
        if "team_name" in request.json:
            team_name = request.json.get('team_name')
            parameter_list.append(team_name)
            GET_FITNESS_DATA_SQL = GET_FITNESS_DATA + f" where team_name=? ALLOW FILTERING;"
            FITNESS_FORM_SQL = FITNESS_FORM_SQL + f" where team_name=? ALLOW FILTERING;"
            PLAYER_LOAD_SQL = PLAYER_LOAD_SQL + f" where team_name=? ALLOW FILTERING;"
        else:
            GET_FITNESS_DATA_SQL = GET_FITNESS_DATA
            FITNESS_FORM_SQL = FITNESS_FORM_SQL
            PLAYER_LOAD_SQL = PLAYER_LOAD_SQL

        data = getPandasFactoryDF(session, GET_FITNESS_DATA_SQL, is_prepared=True, parameter_list=parameter_list)[
            ['record_date', 'player_name', 'player_id', 'max_velocity_kmh',
             'vel_b5_tot_dist_m']]  # .rename(columns={'date_name': 'record_date'})

        fitness_form_df = getPandasFactoryDF(session, FITNESS_FORM_SQL, is_prepared=True,
                                             parameter_list=parameter_list).drop_duplicates(
            subset=["record_date", "player_name", "player_id"], keep="last")

        player_load_df = getPandasFactoryDF(session, PLAYER_LOAD_SQL, is_prepared=True,
                                            parameter_list=parameter_list).drop_duplicates(
            subset=["record_date", "player_name", "player_id"], keep="last")

        if request.json:
            if "from_date" in request.json:
                from_date = request.json.get('from_date')
                from_date = datetime.strptime(
                    datetime.strptime(str(from_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()

            if "to_date" in request.json:
                to_date = request.json.get('to_date')
                to_date = datetime.strptime(
                    datetime.strptime(str(to_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()

            if "player_name" in request.json:
                player_name = request.json.get('player_name')
                data = data[data['player_name'].isin(player_name)]
                fitness_form_df = fitness_form_df[fitness_form_df['player_name'].isin(player_name)]
                player_load_df = player_load_df[player_load_df['player_name'].isin(player_name)]

            if "player_id" in request.json:
                player_id = request.json.get('player_id')
                data = data[data['player_id'].isin(player_id)]
                fitness_form_df = fitness_form_df[fitness_form_df['player_id'].isin(player_id)]
                player_load_df = player_load_df[player_load_df['player_id'].isin(player_id)]

            if "user_name" in request.json:
                player_name = request.json["user_name"]
                data = data[data['player_name'].isin([player_name])]
                fitness_form_df = fitness_form_df[fitness_form_df['player_name'].isin([player_name])]
                player_load_df = player_load_df[player_load_df['player_name'].isin([player_name])]

            if "user_id" in request.json:
                player_id = request.json["user_id"]
                data = data[data['player_id'].isin([player_id])]
                fitness_form_df = fitness_form_df[fitness_form_df['player_id'].isin([player_id])]
                player_load_df = player_load_df[player_load_df['player_id'].isin([player_id])]
        else:
            to_date = date.today()
            from_date = date.today() - timedelta(days=7)

        player_load_df['record_date_form'] = player_load_df['record_date'].apply(
            lambda x: datetime.strptime(datetime.strptime(str(x), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                                        '%Y-%m-%d').date())

        ######### calculating loading_exposure and ball_load

        player_load_data = player_load_df[
            (player_load_df['record_date_form'] >= from_date) & (player_load_df['record_date_form'] <= to_date)]

        if len(player_load_data) > 0:
            player_load_data['record_date_form'] = player_load_data['record_date_form'].apply(
                lambda x: pd.to_datetime(x))

            player_load_data['week_number'] = (((player_load_data['record_date_form'].view(np.int64) - pd.to_datetime(
                [from_date]).view(np.int64)) / (1e9 * 60 * 60 * 24) - player_load_data[
                                                    'record_date_form'].dt.dayofweek + 6) // 7 + 1).astype(np.int64)

            player_load_data = player_load_data[
                ['player_id', 'player_name', 'record_date_form', 'total_load', 'week_number',
                 'bowling_match_balls']].groupby(
                ['player_id', 'player_name', 'week_number']).agg({'record_date_form': 'min',
                                                                  'total_load': 'sum',
                                                                  'bowling_match_balls': 'sum'}).reset_index() \
                .rename(columns={'record_date_form': 'record_date'})

            player_load_data['record_date-28days'] = player_load_data['record_date'] - timedelta(days=28)

            player_load_df['record_date_form'] = player_load_df['record_date_form'].apply(lambda x: pd.to_datetime(x))

            player_load_data = player_load_data.merge(
                player_load_df[['player_id', 'player_name', 'record_date_form', 'total_load', 'bowling_match_balls']],
                left_on=['player_id', 'record_date-28days'],
                right_on=['player_id', 'record_date_form'], how='left').rename(
                columns={"player_name_x": "player_name"}).drop(["player_name_y"], axis=1)

            player_load_data['load_per'] = (player_load_data['total_load_y'] * 100.00) / player_load_data[
                'total_load_x']
            player_load_data['ball_per'] = (
                    (player_load_data['bowling_match_balls_y'].fillna(0) * 100.00) / player_load_data[
                'bowling_match_balls_x'].fillna(0)).fillna(0)

            player_load_data['loading_exposure'] = np.where(player_load_data['load_per'] >= 50, '>=50% Change',
                                                            'No Change')
            player_load_data['bowling_load'] = np.where(player_load_data['ball_per'] >= 50, '>=50% Change', 'No Change')
        else:
            player_load_data = pd.DataFrame({'player_name': pd.Series(dtype='str'),
                                             'week_number': pd.Series(dtype='int'),
                                             'loading_exposure': pd.Series(dtype='int'),
                                             'bowling_load': pd.Series(dtype='int')})
        ###################################################################################################
        fitness_form_df['record_date_form'] = fitness_form_df['record_date'].apply(
            lambda x: datetime.strptime(datetime.strptime(str(x), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                                        '%Y-%m-%d').date())

        fitness_form_df = fitness_form_df[
            (fitness_form_df['record_date_form'] >= from_date) & (fitness_form_df['record_date_form'] <= to_date)]

        if len(fitness_form_df) > 0:
            fitness_form_df['record_date_form'] = fitness_form_df['record_date_form'].apply(lambda x: pd.to_datetime(x))
            fitness_form_df['week_number'] = (((fitness_form_df['record_date_form'].view(np.int64) - pd.to_datetime(
                [from_date]).view(np.int64)) / (1e9 * 60 * 60 * 24) - fitness_form_df[
                                                   'record_date_form'].dt.dayofweek + 6) // 7 + 1).astype(np.int64)

            fitness_form_df['reason_noplay_or_train'] = fitness_form_df['reason_noplay_or_train'].apply(
                lambda x: x.split(';'))
            fitness_form_df = fitness_form_df.groupby(['player_id', 'player_name', 'week_number'])[
                'reason_noplay_or_train'].apply(
                list).reset_index()
            fitness_form_df['reason_noplay_or_train'] = fitness_form_df['reason_noplay_or_train'].apply(np.concatenate)
            unhealthy_list = ['Injury', 'Illness']

            fitness_form_df['player_condition'] = fitness_form_df['reason_noplay_or_train'].map(
                lambda x: 'Unhealthy' if any(item in x for item in unhealthy_list) else 'Healthy')
        else:
            fitness_form_df = pd.DataFrame({'player_id': pd.Series(dtype='int'),
                                            'player_name': pd.Series(dtype='str'),
                                            'week_number': pd.Series(dtype='int'),
                                            'player_condition': pd.Series(dtype='str')})

        data['record_date_form'] = data['record_date'].apply(
            lambda x: datetime.strptime(datetime.strptime(str(x), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                                        '%Y-%m-%d').date())

        data[['max_velocity_kmh', 'vel_b5_tot_dist_m']] = data[['max_velocity_kmh', 'vel_b5_tot_dist_m']] \
            .apply(pd.to_numeric, errors='coerce')

        data = data[(data['record_date_form'] >= from_date) & (data['record_date_form'] <= to_date)]
        if len(data) > 0:
            data['player_name'] = data['player_name'].str.strip()
            try:
                ball_bat = mi_bat_data[
                    ['src_batsman_id', 'batsman_name', 'match_id', 'match_date_form']].drop_duplicates().rename(
                    columns={"src_batsman_id": "batsman_id"})
                ball_bat = ball_bat[
                    (ball_bat['match_date_form'] >= from_date) & (ball_bat['match_date_form'] <= to_date)]
                ball_bat = ball_bat[['batsman_id', 'batsman_name', 'match_id', 'match_date_form']].rename(
                    columns={'batsman_name': 'player_name', 'batsman_id': 'player_id'})
                ball_bowl = mi_ball_data[
                    ['src_bowler_id', 'bowler_name', 'match_id', 'match_date_form']].drop_duplicates().rename(
                    columns={"src_bowler_id": "bowler_id"})
                ball_bowl = ball_bowl[
                    (ball_bowl['match_date_form'] >= from_date) & (ball_bowl['match_date_form'] <= to_date)]
                ball_bowl = ball_bowl[['bowler_id', 'bowler_name', 'match_id', 'match_date_form']].rename(
                    columns={'bowler_name': 'player_name', 'bowler_id': 'player_id'})
                ball_final = pd.concat([ball_bat, ball_bowl], ignore_index=True)
                ball_final = ball_final.drop_duplicates()
            except:
                ball_final = pd.DataFrame({'player_id': pd.Series(dtype='int'),
                                           'player_name': pd.Series(dtype='str'),
                                           'match_id': pd.Series(dtype='int'),
                                           'record_date_form': pd.Series(dtype='date')})

            data_player = pd.merge(data, ball_final, left_on=["player_id", "record_date_form"],
                                   right_on=["player_id", "match_date_form"], how="left").rename(
                columns={"player_name_x": "player_name"}).drop(["player_name_y"], axis=1)
            data_player['record_date_form'] = data_player['record_date_form'].apply(lambda x: pd.to_datetime(x))
            data_player['week_number'] = (((data_player['record_date_form'].view(np.int64) - pd.to_datetime(
                [from_date]).view(np.int64)) / (1e9 * 60 * 60 * 24) - data_player[
                                               'record_date_form'].dt.dayofweek + 6) // 7 + 1).astype(np.int64)

            data_player['record_date'] = pd.to_datetime(data_player['record_date'])
            data_max_vel = data_player[['player_id', 'player_name', 'max_velocity_kmh']].groupby(
                ['player_name', 'player_id']).agg(
                player_benchmark=('max_velocity_kmh', 'max')).reset_index()
            data_grp = data_player.groupby(['player_id', 'player_name', 'week_number']).agg(
                max_velocity=('max_velocity_kmh', 'max'),
                number_of_opportunities=('max_velocity_kmh', 'count'),
                number_of_matches=('match_id', 'count'),
                velocity_band5_distance=("vel_b5_tot_dist_m", "sum")
            ).reset_index()
            data_grp = data_grp.merge(data_max_vel, on='player_id', how='left').rename(
                columns={"player_name_x": "player_name"}).drop(["player_name_y"], axis=1)

            data_grp['deviation'] = data_grp['player_benchmark'] - data_grp['max_velocity']
            data_grp['player_readiness_flag'] = np.where(data_grp['deviation'] >= 10, "NOT READY", "READY")
            data_grp['<100'] = np.where(data_grp['velocity_band5_distance'] <= 100, 1, 0)
            data_grp['100-200'] = np.where(
                (data_grp['velocity_band5_distance'] > 100) & (data_grp['velocity_band5_distance'] <= 200), 1, 0)
            data_grp['200-400'] = np.where(
                (data_grp['velocity_band5_distance'] > 200) & (data_grp['velocity_band5_distance'] <= 400), 1, 0)
            data_grp['>400'] = np.where((data_grp['velocity_band5_distance'] > 400), 1, 0)
            data_grp["injury_flag"] = np.where(data_grp['<100'] == 1, "Injury Risk", "No Risk")
            data_grp['deviation_from_benchmark'] = np.where(data_grp['velocity_band5_distance'] >= 100, 0,
                                                            100 - data_grp['velocity_band5_distance'])

            data_grp = data_grp.merge(fitness_form_df[['player_id', 'player_name', 'week_number', 'player_condition']],
                                      on=['week_number', 'player_id'], how='left').rename(
                columns={"player_name_x": "player_name"}).drop(["player_name_y"], axis=1)
            data_grp = data_grp.merge(
                player_load_data[['player_id', 'player_name', 'week_number', 'loading_exposure', 'bowling_load']],
                on=['week_number', 'player_id'], how='left').rename(columns={"player_name_x": "player_name"}).drop(
                ["player_name_y"], axis=1)

            data_grp['player_condition'] = data_grp['player_condition'].fillna('Healthy')
            phase_conditions = [
                (data_grp['player_readiness_flag'] == 'NOT READY') & (data_grp['injury_flag'] == 'Injury Risk')
                & (data_grp['player_condition'] == 'Unhealthy'),
                (data_grp['player_readiness_flag'] == 'NOT READY') | (data_grp['injury_flag'] == 'Injury Risk')
                | (data_grp['player_condition'] == 'Unhealthy'),
                (data_grp['player_readiness_flag'] == 'READY') & (data_grp['injury_flag'] == 'No Risk')
                & (data_grp['player_condition'] == 'Healthy')
            ]

            # different alerts
            phase_values = ['High Alert', 'Medium Alert', 'Low Alert']

            data_grp['overall_health_status'] = np.select(phase_conditions, phase_values)

            data_grp = data_grp.round(2)

            return data_grp.to_json(orient="records"), 200, logger.info("Status - 200")

        else:
            return jsonify({}), 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def groupWellness():
    logger = get_logger("groupWellness", "groupWellness")
    try:
        req_filters = request.json
        validateRequest(req_filters)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        FITNESS_FORM_SQL = f'''select record_date, player_id, player_name, fatigue_level_rating, sleep_rating, muscle_soreness_rating, 
                stress_levels_rating, wellness_rating from {DB_NAME}.{DAILY_ACTIVITY_TABLE_NAME} '''
        parameter_list = []
        if "team_name" in request.json:
            team_name = request.json.get('team_name')
            parameter_list.append(team_name)
            FITNESS_FORM_SQL = FITNESS_FORM_SQL + f" where team_name=? ALLOW FILTERING;"
        else:
            FITNESS_FORM_SQL = FITNESS_FORM_SQL

        fitness_form_df = getPandasFactoryDF(session, FITNESS_FORM_SQL, is_prepared=True,
                                             parameter_list=parameter_list).drop_duplicates(
            subset=["record_date", "player_name"], keep="last")

        if request.json:
            if "from_date" in request.json:
                from_date = request.json.get('from_date')
                from_date = datetime.strptime(
                    datetime.strptime(str(from_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()

            if "to_date" in request.json:
                to_date = request.json.get('to_date')
                to_date = datetime.strptime(
                    datetime.strptime(str(to_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()

            if "player_name" in request.json:
                player_name = request.json.get('player_name')
                fitness_form_df = fitness_form_df[fitness_form_df['player_name'].isin(player_name)]

            if "player_id" in request.json:
                player_id = request.json.get('player_id')
                fitness_form_df = fitness_form_df[fitness_form_df['player_id'].isin(player_id)]

            if "user_name" in request.json:
                player_name = request.json["user_name"]
                fitness_form_df = fitness_form_df[fitness_form_df['player_name'].isin([player_name])]

            if "user_id" in request.json:
                player_id = request.json.get('user_id')
                fitness_form_df = fitness_form_df[fitness_form_df['player_id'].isin(player_id)]
        else:
            to_date = date.today()
            from_date = date.today() - timedelta(days=7)

        fitness_form_df['record_date_form'] = fitness_form_df['record_date'].apply(
            lambda x: datetime.strptime(datetime.strptime(str(x), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                                        '%Y-%m-%d').date())

        fitness_form_df = fitness_form_df[
            (fitness_form_df['record_date_form'] >= from_date) & (
                    fitness_form_df['record_date_form'] <= to_date)]

        fitness_form_df = fitness_form_df.mask(fitness_form_df == -1, np.NaN)

        fitness_form_df['player_ratings'] = fitness_form_df[
            ['fatigue_level_rating', 'sleep_rating', 'muscle_soreness_rating', 'stress_levels_rating',
             'wellness_rating']].to_dict(orient='records')

        response = fitness_form_df.sort_values(by='record_date_form').groupby(['player_name']) \
            .apply(lambda x: x.set_index('record_date')[['player_ratings']].to_dict(orient='index')).to_json()

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def groupActivitySessions():
    logger = get_logger("groupActivitySessions", "groupActivitySessions")
    try:
        req_filters = request.json
        validateRequest(req_filters)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        FITNESS_FORM_SQL = f'''select record_date, player_id, player_name, played_today , batting_train_mins, 
                bowling_train_mins, fielding_train_mins, running_mins, cross_training_mins,strength_mins, rehab_mins from 
                {DB_NAME}.{DAILY_ACTIVITY_TABLE_NAME} '''
        parameter_list = []
        if "team_name" in request.json:
            team_name = request.json.get('team_name')
            parameter_list.append(team_name)
            FITNESS_FORM_SQL = FITNESS_FORM_SQL + f" where team_name=? ALLOW FILTERING;"
        else:
            FITNESS_FORM_SQL = FITNESS_FORM_SQL

        fitness_form_df = getPandasFactoryDF(session, FITNESS_FORM_SQL, is_prepared=True,
                                             parameter_list=parameter_list).drop_duplicates(
            subset=["record_date", "player_name", "player_id"], keep="last")
        if request.json:
            if "from_date" in request.json:
                from_date = request.json.get('from_date')
                from_date = datetime.strptime(
                    datetime.strptime(str(from_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()

            if "to_date" in request.json:
                to_date = request.json.get('to_date')
                to_date = datetime.strptime(
                    datetime.strptime(str(to_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()

            if "player_name" in request.json:
                player_name = request.json.get('player_name')
                fitness_form_df = fitness_form_df[fitness_form_df['player_name'].isin(player_name)]

            if "player_id" in request.json:
                player_id = request.json.get('player_id')
                fitness_form_df = fitness_form_df[fitness_form_df['player_id'].isin(player_id)]

            if "user_name" in request.json:
                player_name = request.json["user_name"]
                fitness_form_df = fitness_form_df[fitness_form_df['player_name'].isin([player_name])]

            if "user_id" in request.json:
                player_id = request.json.get('user_id')
                fitness_form_df = fitness_form_df[fitness_form_df['player_id'].isin(player_id)]
        else:
            to_date = date.today()
            from_date = date.today() - timedelta(days=7)

        if len(fitness_form_df) > 0:
            fitness_form_df['match_days'] = np.where(
                fitness_form_df['played_today'].apply(lambda x: x.lower()) == "yes", 1,
                0)
            fitness_form_df['batting'] = np.where(fitness_form_df['batting_train_mins'] != -1, 1, 0)
            fitness_form_df['bowling'] = np.where(fitness_form_df['bowling_train_mins'] != -1, 1, 0)
            fitness_form_df['fielding'] = np.where(fitness_form_df['fielding_train_mins'] != -1, 1, 0)
            fitness_form_df['running'] = np.where(fitness_form_df['running_mins'] != -1, 1, 0)
            fitness_form_df['strength'] = np.where(fitness_form_df['strength_mins'] != -1, 1, 0)
            fitness_form_df['x_train'] = np.where(fitness_form_df['cross_training_mins'] != -1, 1, 0)
            fitness_form_df['rehab'] = np.where(fitness_form_df['rehab_mins'] != -1, 1, 0)

            fitness_form_df['record_date_form'] = fitness_form_df['record_date'].apply(
                lambda x: datetime.strptime(datetime.strptime(str(x), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                                            '%Y-%m-%d').date())

            fitness_form_df = fitness_form_df[
                (fitness_form_df['record_date_form'] >= from_date) & (
                        fitness_form_df['record_date_form'] <= to_date)].drop(
                ['record_date_form', 'played_today', 'batting_train_mins', 'bowling_train_mins', 'fielding_train_mins',
                 'running_mins', 'strength_mins', 'cross_training_mins', 'record_date', 'rehab_mins'], axis=1)

            fitness_form_df = fitness_form_df.groupby(['player_name', 'player_id']).agg({'match_days': 'sum',
                                                                                         'batting': 'sum',
                                                                                         'bowling': 'sum',
                                                                                         'fielding': 'sum',
                                                                                         'running': 'sum',
                                                                                         'strength': 'sum',
                                                                                         'x_train': 'sum',
                                                                                         'rehab': 'sum'}).reset_index()
            response = fitness_form_df.to_json(orient='records')
            return response, 200, logger.info("Status - 200")
        else:
            return jsonify({}), 200, logger.info("Status - 200")
    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def bowlingGPSReport():
    logger = get_logger("bowlingGPSReport", "bowlingGPSReport")
    try:
        filters = request.json.copy()
        req_filters = request.json
        list_keys = ["max_runup_velocity", "bowling_intensity", "match_peak_load"]
        validateRequest(dropFilter(list_keys, req_filters))
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        GET_FITNESS_DATA = f'''select activity_name, player_id, delivery_time, period_name, record_date, player_name,
                 ball_no, max_runup_velocity, peak_player_load from  {DB_NAME}.fitnessGPSBallData 
                 where season >= 2019 '''

        GET_MATCH_PEAK_LOAD = f'''select record_date, player_id, player_name, match_peak_load from 
                    {DB_NAME}.matchPeakLoad '''
        parameter_list = []
        if "team_name" in req_filters:
            team_name = req_filters.get('team_name')
            parameter_list.append(team_name)
            GET_FITNESS_DATA_SQL = GET_FITNESS_DATA + f" and team_name=? ALLOW FILTERING;"
        else:
            GET_FITNESS_DATA_SQL = GET_FITNESS_DATA

        response = {}

        fitness_data = getPandasFactoryDF(session, GET_FITNESS_DATA_SQL, is_prepared=True,
                                          parameter_list=parameter_list)
        fitness_data['period_name'] = fitness_data['period_name'].apply(lambda x: x.lower())
        fitness_data['activity_name'] = fitness_data['activity_name'].apply(lambda x: x.lower())
        training_conditions = [(fitness_data["activity_name"].str.contains(
            "train|practice|warm|session|net|prep|pre|throwing|skills|running|quarter")) | (
                                   fitness_data["period_name"].str.contains(
                                       "train|practice|warm|session|net|prep|pre|throwing|skills|running|quarter")),
                               (fitness_data["period_name"].str.contains("innings|scenario|duties|match")),
                               (~fitness_data["period_name"].str.contains(
                                   "train|practice|warm|session|net|prep|pre|throwing|skills|running|quarter")) &
                               (~fitness_data["period_name"].str.contains("innings|scenario|duties|match"))
                               ]

        training_values = ['Practice', 'Match', 'Practice']
        fitness_data['gen_period_name'] = np.select(training_conditions, training_values)

        match_peak_load_df = getPandasFactoryDF(session, GET_MATCH_PEAK_LOAD)
        if filters:
            for key, value in filters.items():
                if key == "record_date":
                    fitness_data = fitness_data[fitness_data['record_date'].isin(value)]
                elif key == "player_name":
                    fitness_data = fitness_data[fitness_data['player_name'].isin(value)]
                    match_peak_load_df = match_peak_load_df[match_peak_load_df['player_name'].isin(value)]
                elif key == "player_id":
                    fitness_data = fitness_data[fitness_data['player_id'].isin(value)]
                    match_peak_load_df = match_peak_load_df[match_peak_load_df['player_name'].isin(value)]
                elif key == "user_name":
                    fitness_data = fitness_data[fitness_data['player_name'].isin([value])]
                    match_peak_load_df = match_peak_load_df[match_peak_load_df['player_name'].isin([value])]
                elif key == "user_id":
                    fitness_data = fitness_data[fitness_data['player_id'].isin([value])]
                    match_peak_load_df = match_peak_load_df[match_peak_load_df['player_id'].isin([value])]
                elif key == "from_date":
                    from_date = request.json.get('from_date')
                    from_date = datetime.strptime(
                        datetime.strptime(str(from_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()
                    to_date = request.json.get('to_date')
                    to_date = datetime.strptime(
                        datetime.strptime(str(to_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()
                    fitness_data['record_date_form'] = fitness_data['record_date'].apply(lambda x: datetime.strptime(
                        datetime.strptime(str(x), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date())
                    fitness_data = fitness_data[
                        (fitness_data['record_date_form'] >= from_date) & (
                                fitness_data['record_date_form'] <= to_date)].drop('record_date_form', axis=1)

        if 'match_peak_load' in filters:
            fitness_data['match_peak_load'] = filters['match_peak_load']
        else:
            fitness_data['record_date_form'] = fitness_data['record_date'].apply(lambda x: datetime.strptime(
                datetime.strptime(str(x), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date())

            match_peak_load_df['record_date_form'] = match_peak_load_df['record_date'].apply(
                lambda x: datetime.strptime(datetime.strptime(str(x), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                                            '%Y-%m-%d').date())
            load_list = []
            # for value in fitness_data['record_date_form']:
            for player, r_date in zip(fitness_data['player_id'], fitness_data['record_date_form']):
                player_load_avg = \
                    match_peak_load_df[(match_peak_load_df['player_id'] == player) &
                                       (match_peak_load_df['record_date_form'] <= r_date)].sort_values(
                        by='record_date_form', ascending=False)[['match_peak_load']].head(1)
                if len(player_load_avg) > 0:
                    load_list.append(player_load_avg.iloc[0, 0])
                else:
                    load_list.append(8)

            fitness_data['match_peak_load'] = load_list

        fitness_data['bowling_intensity'] = round(
            fitness_data['peak_player_load'].astype('float') * 100.00 / fitness_data['match_peak_load'].astype('float'),
            2)

        fitness_data['max_runup_velocity'] = round(pd.to_numeric(fitness_data['max_runup_velocity']), 2)
        fitness_data['delivery_time'] = fitness_data['delivery_time'].apply(
            lambda x: datetime.fromtimestamp(x / 100) - timedelta(minutes=30))
        fitness_data['delivery_time'] = fitness_data['delivery_time'].apply(
            lambda x: datetime.strptime(str(x).split(".")[0], '%Y-%m-%d %H:%M:%S').strftime('%Y-%b-%d %I:%M:%S %p'))

        if filters:
            for key, value in filters.items():
                if key == "max_runup_velocity":
                    fitness_data = filterDF(fitness_data, 'max_runup_velocity', value[0], value[1])
                elif key == "bowling_intensity":
                    fitness_data = filterDF(fitness_data, 'bowling_intensity', value[0], value[1])
                else:
                    fitness_data = fitness_data
        if len(fitness_data) > 0:
            player_list = list(fitness_data['player_name'].unique())
            fitness_agg_data = fitness_data.groupby(['player_id', 'player_name', 'gen_period_name']).agg(
                total_deliveries=('ball_no', 'count'),
                max_runup_velocity=('max_runup_velocity', 'max'),
                avg_runup_velocity=('max_runup_velocity', 'mean'),
                avg_bowling_intensity=("bowling_intensity", "mean")).reset_index()
            fitness_agg_data = fitness_agg_data.round(2)
            fitness_data['player_details'] = fitness_data[
                ['ball_no', 'delivery_time', 'max_runup_velocity', 'bowling_intensity']].to_dict(orient='records')

            fitness_data = fitness_data.groupby(['player_id', 'player_name', 'gen_period_name', 'record_date']) \
                .agg({'player_details': list, 'match_peak_load': lambda x: int(list(x.unique())[0])}).reset_index()

            for player in player_list:
                response[player] = {'data': '', 'agg_data': ''}
                response[player]['agg_data'] = \
                    fitness_agg_data[fitness_agg_data['player_name'] == player].set_index("gen_period_name")[
                        ["total_deliveries", "max_runup_velocity", "avg_runup_velocity",
                         "avg_bowling_intensity"]].to_dict(orient="index")

                response[player]['data'] = fitness_data[fitness_data['player_name'] == player].groupby(
                    'gen_period_name') \
                    .apply(lambda x: x.set_index('record_date')[['player_details', 'match_peak_load']]
                           .to_dict(orient='index')).to_dict()

            return response, 200, logger.info("Status - 200")
        else:
            return jsonify({}), 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def groupWeeklyLoadReport():
    logger = get_logger("groupWeeklyLoadReport", "groupWeeklyLoadReport")
    try:
        req_filters = request.json
        validateRequest(req_filters)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        response = {}
        PLAYER_LOAD_SQL = f'''select record_date, cast(player_id as int) as player_id, player_name, bat_load, bowl_load, field_load,
         run_load,match_load, x_train_load, strength_load, rehab_load, total_snc_load, total_trn_load,total_load 
         from {DB_NAME}.{PLAYER_LOAD_TABLE_NAME} '''
        parameter_list = []
        if "team_name" in request.json:
            team_name = request.json.get('team_name')
            parameter_list.append(team_name)
            PLAYER_LOAD_SQL = PLAYER_LOAD_SQL + f" where team_name=? ALLOW FILTERING;"
        else:
            PLAYER_LOAD_SQL = PLAYER_LOAD_SQL

        fitness_form_df = getPandasFactoryDF(session, PLAYER_LOAD_SQL, is_prepared=True,
                                             parameter_list=parameter_list).drop_duplicates(
            subset=["record_date", "player_name", "player_id"], keep="last") \
            .rename(columns={'total_snc_load': 'total_s&c_load', 'x_train_load': 'x-train_load'})

        if request.json:
            if "from_date" in request.json:
                from_date = request.json.get('from_date')
                from_date = datetime.strptime(
                    datetime.strptime(str(from_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()

            if "to_date" in request.json:
                to_date = request.json.get('to_date')
                to_date = datetime.strptime(
                    datetime.strptime(str(to_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()

            if "player_name" in request.json:
                player_name = request.json.get('player_name')
                fitness_form_df = fitness_form_df[fitness_form_df['player_name'].isin(player_name)]

            if "user_name" in request.json:
                player_name = request.json["user_name"]
                fitness_form_df = fitness_form_df[fitness_form_df['player_name'].isin([player_name])]

            if "player_id" in request.json:
                player_id = request.json.get('player_id')
                fitness_form_df = fitness_form_df[fitness_form_df['player_id'].isin(player_id)]

            if "user_id" in request.json:
                player_id = request.json["user_id"]
                fitness_form_df = fitness_form_df[fitness_form_df['player_id'].isin([player_id])]
        else:
            to_date = date.today()
            from_date = date.today() - timedelta(days=7)

        fitness_form_df['record_date_form'] = fitness_form_df['record_date'].apply(
            lambda x: datetime.strptime(datetime.strptime(str(x), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                                        '%Y-%m-%d').date())

        fitness_form_df = fitness_form_df[
            (fitness_form_df['record_date_form'] >= from_date) & (fitness_form_df['record_date_form'] <= to_date)]

        if ("player_name" in request.json) | ("player_id" in request.json):
            player_load_data = fitness_form_df[['record_date_form', 'total_load']].rename(
                columns={'record_date_form': 'record_date'})

            player_load_data['record_date'] = player_load_data['record_date'].astype(str)
            response['player_load_data'] = player_load_data.set_index('record_date')['total_load'].to_dict()
        else:
            response['player_load_data'] = {}

        fitness_form_df = fitness_form_df[
            ['player_id', 'player_name', 'bat_load', 'bowl_load', 'field_load', 'run_load', 'match_load',
             'x-train_load',
             'strength_load', 'rehab_load', 'total_s&c_load', 'total_trn_load', 'total_load']] \
            .groupby(['player_name', 'player_id']).agg({'bat_load': 'sum',
                                                        'bowl_load': 'sum',
                                                        'field_load': 'sum',
                                                        'run_load': 'sum',
                                                        'match_load': 'sum',
                                                        'x-train_load': 'sum',
                                                        'strength_load': 'sum',
                                                        'rehab_load': 'sum',
                                                        'total_s&c_load': 'sum',
                                                        'total_trn_load': 'sum',
                                                        'total_load': 'sum'}).reset_index()

        fitness_form_df["player_id"] = fitness_form_df["player_id"].fillna(-1).astype(int)

        response['group_load'] = fitness_form_df.to_dict(orient='records')

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def getIndividualTrend():
    logger = get_logger("getIndividualTrend", "getIndividualTrend")
    try:
        req_filters = request.json
        validateRequest(req_filters)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        PLAYER_LOAD_SQL = f'''select record_date, player_id, player_name, bat_load, bowl_load, field_load,
                 run_load,match_load, x_train_load, strength_load, rehab_load, total_snc_load, total_trn_load,total_load, 
                 bowling_match_balls,bat_match_load, bowl_match_load,field_match_load 
                   from {DB_NAME}.{PLAYER_LOAD_TABLE_NAME}'''

        FITNESS_FORM_SQL = f'''select record_date, player_id, player_name, 
        fatigue_level_rating, sleep_rating, muscle_soreness_rating, 
                stress_levels_rating, wellness_rating from {DB_NAME}.{DAILY_ACTIVITY_TABLE_NAME}'''
        parameter_list = []
        if "team_name" in request.json:
            team_name = request.json.get('team_name')
            parameter_list.append(team_name)
            PLAYER_LOAD_SQL = PLAYER_LOAD_SQL + f" where team_name=? ALLOW FILTERING;"
            FITNESS_FORM_SQL = FITNESS_FORM_SQL + f" where team_name=? ALLOW FILTERING;"
        else:
            PLAYER_LOAD_SQL = PLAYER_LOAD_SQL
            FITNESS_FORM_SQL = FITNESS_FORM_SQL

        player_load_df = getPandasFactoryDF(session, PLAYER_LOAD_SQL, is_prepared=True,
                                            parameter_list=parameter_list).drop_duplicates(
            subset=["record_date", "player_name", "player_id"],
            keep="last") \
            .rename(columns={'total_snc_load': 'total_s&c_load', 'x_train_load': 'x-train_load',
                             'bowling_match_balls': 'total_deliveries',
                             'match_load': 'total_match_load', 'bat_load': 'bat_trn_load', 'bowl_load': 'bowl_trn_load',
                             'field_load': 'field_trn_load'})

        fitness_form_df = getPandasFactoryDF(session, FITNESS_FORM_SQL, is_prepared=True, parameter_list=parameter_list) \
            .rename(columns={"bowling_match_balls": "total_deliveries"}).drop_duplicates(
            subset=["record_date", "player_name", "player_id"],
            keep="last")

        fitness_form_df['record_date_form'] = fitness_form_df['record_date'].apply(
            lambda x: datetime.strptime(datetime.strptime(str(x), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                                        '%Y-%m-%d').date())
        player_load_df['record_date_form'] = player_load_df['record_date'].apply(
            lambda x: datetime.strptime(datetime.strptime(str(x), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                                        '%Y-%m-%d').date())

        if request.json:
            if "from_date" in request.json:
                from_date = request.json.get('from_date')
                from_date = datetime.strptime(
                    datetime.strptime(str(from_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()

            if "to_date" in request.json:
                to_date = request.json.get('to_date')
                to_date = datetime.strptime(
                    datetime.strptime(str(to_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()

            if "player_name" in request.json:
                player_name = request.json.get('player_name')
                fitness_form_df = fitness_form_df[fitness_form_df['player_name'].isin(player_name)]
                player_load_df = player_load_df[player_load_df['player_name'].isin(player_name)]

            if "user_name" in request.json:
                player_name = request.json["user_name"]
                fitness_form_df = fitness_form_df[fitness_form_df['player_name'].isin([player_name])]
                player_load_df = player_load_df[player_load_df['player_name'].isin([player_name])]

            if "player_id" in request.json:
                player_id = request.json.get('player_id')
                fitness_form_df = fitness_form_df[fitness_form_df['player_id'].isin(player_id)]

            if "user_id" in request.json:
                player_id = request.json["user_id"]
                fitness_form_df = fitness_form_df[fitness_form_df['player_id'].isin([player_id])]
        else:
            to_date = date.today()
            from_date = date.today() - timedelta(days=7)

        fitness_form_df = fitness_form_df[
            (fitness_form_df['record_date_form'] >= from_date) & (fitness_form_df['record_date_form'] <= to_date)]
        if len(fitness_form_df) > 0:
            fitness_form_df['record_date_form'] = fitness_form_df['record_date_form'].apply(lambda x: pd.to_datetime(x))

            fitness_form_df['week_number'] = (((fitness_form_df['record_date_form'].view(np.int64) - pd.to_datetime(
                [from_date]).view(np.int64)) / (1e9 * 60 * 60 * 24) - fitness_form_df[
                                                   'record_date_form'].dt.dayofweek + 6) // 7 + 1).astype(np.int64)

            player_load_df = player_load_df[
                (player_load_df['record_date_form'] >= from_date) & (player_load_df['record_date_form'] <= to_date)]

            player_load_df['record_date_form'] = player_load_df['record_date_form'].apply(lambda x: pd.to_datetime(x))

            player_load_df['week_number'] = (((player_load_df['record_date_form'].view(np.int64) - pd.to_datetime(
                [from_date]).view(np.int64)) / (1e9 * 60 * 60 * 24) - player_load_df[
                                                  'record_date_form'].dt.dayofweek + 6) // 7 + 1).astype(np.int64)

            fitness_form_df = fitness_form_df.mask(fitness_form_df == -1, np.NaN)

            player_load_df = player_load_df[
                ['player_id', 'player_name', 'week_number', 'bat_trn_load', 'bowl_trn_load', 'field_trn_load',
                 'run_load',
                 'total_match_load', 'strength_load', 'rehab_load', 'total_s&c_load', 'total_load', 'total_deliveries',
                 'bat_match_load', 'bowl_match_load', 'field_match_load']] \
                .groupby(['player_id', 'player_name', 'week_number']).agg({'bat_trn_load': 'sum',
                                                                           'bowl_trn_load': 'sum',
                                                                           'field_trn_load': 'sum',
                                                                           'run_load': 'sum',
                                                                           'total_match_load': 'sum',
                                                                           'strength_load': 'sum',
                                                                           'rehab_load': 'sum',
                                                                           'total_s&c_load': 'sum',
                                                                           'total_deliveries': 'sum',
                                                                           'total_load': 'sum',
                                                                           'bat_match_load': 'sum',
                                                                           'bowl_match_load': 'sum',
                                                                           'field_match_load': 'sum'
                                                                           }).reset_index()

            fitness_form_df = fitness_form_df[
                ['player_id', 'player_name', 'week_number', 'fatigue_level_rating', 'sleep_rating',
                 'muscle_soreness_rating',
                 'stress_levels_rating', 'wellness_rating']] \
                .groupby(['player_id', 'player_name', 'week_number']).agg({'fatigue_level_rating': 'mean',
                                                                           'sleep_rating': 'mean',
                                                                           'muscle_soreness_rating': 'mean',
                                                                           'stress_levels_rating': 'mean',
                                                                           'wellness_rating': 'mean'
                                                                           }).reset_index()

            fitness_form_df = fitness_form_df.merge(player_load_df, on=['player_id', 'player_name', 'week_number'],
                                                    how='left')
            fitness_form_df["player_id"] = fitness_form_df["player_id"].fillna(-1).astype(int)
            fitness_form_df = fitness_form_df.round(2)

            return fitness_form_df.to_json(orient='records'), 200, logger.info("Status - 200")

        else:
            return jsonify({}), 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def getPlayersDailyFitness():
    logger = get_logger("getPlayersDailyFitness", "getPlayersDailyFitness")
    try:
        req_filters = request.json
        validateRequest(req_filters)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        FITNESS_FORM_SQL = f'''select record_date, player_id, player_name, reason_noplay_or_train
             from {DB_NAME}.{DAILY_ACTIVITY_TABLE_NAME}'''
        parameter_list = []
        if "team_name" in request.json:
            team_name = request.json.get('team_name')
            FITNESS_FORM_SQL = FITNESS_FORM_SQL + f" where team_name=? ALLOW FILTERING;"
            parameter_list.append(team_name)
        else:
            FITNESS_FORM_SQL = FITNESS_FORM_SQL

        fitness_form_df = getPandasFactoryDF(session, FITNESS_FORM_SQL, is_prepared=True,
                                             parameter_list=parameter_list).drop_duplicates(
            subset=["record_date", "player_id", "player_name"], keep="last")

        fitness_form_df['record_date_form'] = fitness_form_df['record_date'].apply(
            lambda x: datetime.strptime(datetime.strptime(str(x), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                                        '%Y-%m-%d').date())

        if request.json:
            if "from_date" in request.json:
                from_date = request.json.get('from_date')
                from_date = datetime.strptime(
                    datetime.strptime(str(from_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()

            if "to_date" in request.json:
                to_date = request.json.get('to_date')
                to_date = datetime.strptime(
                    datetime.strptime(str(to_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()

            if "player_name" in request.json:
                player_name = request.json.get('player_name')
                fitness_form_df = fitness_form_df[fitness_form_df['player_name'].isin(player_name)]

            if "user_name" in request.json:
                player_name = request.json["user_name"]
                fitness_form_df = fitness_form_df[fitness_form_df['player_name'].isin([player_name])]

            if "player_id" in request.json:
                player_id = request.json.get('player_id')
                fitness_form_df = fitness_form_df[fitness_form_df['player_id'].isin(player_id)]

            if "user_id" in request.json:
                player_id = request.json["user_id"]
                fitness_form_df = fitness_form_df[fitness_form_df['player_id'].isin([player_id])]
        else:
            to_date = date.today()
            from_date = date.today() - timedelta(days=7)

        fitness_form_df = fitness_form_df[
            (fitness_form_df['record_date_form'] >= from_date) & (fitness_form_df['record_date_form'] <= to_date)]
        fitness_form_df['physical_condition'] = fitness_form_df['physical_condition'] = fitness_form_df[
            'reason_noplay_or_train'].apply(
            lambda x: 'Healthy' if x in ['NA', ""] else x if x[-1] != ";" else x[:-1])

        fitness_form_df["player_id"] = fitness_form_df["player_id"].fillna(-1).astype(int)

        return fitness_form_df[['player_id', 'player_name', 'record_date', 'physical_condition']].to_json(
            orient='records'), 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def bowlingAggSummary():
    logger = get_logger("bowlingAggSummary", "bowlingAggSummary")
    try:
        req_filters = request.json
        validateRequest(req_filters)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        response = {}
        GET_FITNESS_DATA = f'''select record_date, period_name, activity_name, season, player_id, player_name, team_name, ball_no
         from {DB_NAME}.{GPS_DELIVERY_TABLE_NAME};'''
        fitness_data = getPandasFactoryDF(session, GET_FITNESS_DATA)

        FITNESS_FORM_SQL = f'''select record_date, team_name, player_id, player_name, bowling_train_balls, 
        bowling_match_balls from {DB_NAME}.{DAILY_ACTIVITY_TABLE_NAME}'''

        fitness_form_df = getPandasFactoryDF(session, FITNESS_FORM_SQL).drop_duplicates(
            subset=["record_date", "player_name", "player_id"], keep="last")

        if "player_name" in request.json:
            player_name = request.json.get('player_name')
            fitness_form_df = fitness_form_df[fitness_form_df['player_name'].isin(player_name)]
            fitness_data = fitness_data[fitness_data['player_name'].isin(player_name)]

        if "player_id" in request.json:
            player_id = request.json.get('player_id')
            fitness_form_df = fitness_form_df[fitness_form_df['player_id'].isin(player_id)]
            fitness_data = fitness_data[fitness_data['player_id'].isin(player_id)]

        if "team_name" in request.json:
            team_name = request.json.get('team_name')
            fitness_form_df = fitness_form_df[fitness_form_df['team_name'] == team_name]
            fitness_data = fitness_data[fitness_data['team_name'] == team_name]

        if 'to_date' in request.json:
            to_date = request.json.get('to_date')
            to_date = datetime.strptime(
                datetime.strptime(str(to_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()
        else:
            to_date = date.today()

        from_date = to_date - timedelta(days=6)
        month_date = to_date - timedelta(days=27)

        form_df, form_data = getFormBowlingData(fitness_form_df, from_date, to_date, month_date)
        gps_df, gps_data = getGPSBowlingData(fitness_data, from_date, to_date, month_date)

        combined_df = form_df.merge(gps_df, how='outer', on='player_name')
        combined_df["player_id"] = combined_df["player_id_x"].fillna(combined_df["player_id_y"]).fillna(-1).astype(int)
        col_list = ['training', 'match', 'total_deliveries', 'chronic', 'loading_status', 'bowling_sessions']

        for col in col_list:
            combined_df[col] = round((combined_df[col + "_x"].fillna(0) + combined_df[col + "_y"].fillna(0)) / 2, 2)

        combined_data = combined_df[
            ['player_id', 'player_name', 'training', 'match', 'total_deliveries', 'chronic', 'loading_status',
             'bowling_sessions']].fillna(0)

        response['form_data'] = form_data
        response['gps_data'] = gps_data
        response['combined_data'] = json.loads(re.sub(r'\binfinity\b', '\"\"', combined_data.to_json(orient='records')))

        return jsonify(response), 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def bowlingActualReport():
    logger = get_logger("bowlingActualReport", "bowlingActualReport")
    try:
        req_filters = request.json
        validateRequest(req_filters)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        response = {}
        GET_FITNESS_DATA = f'''select record_date, period_name, activity_name, season, player_id, player_name, team_name, ball_no
                     from {DB_NAME}.{GPS_DELIVERY_TABLE_NAME}'''
        fitness_data = getPandasFactoryDF(session, GET_FITNESS_DATA)

        GET_PLANNED_DATA = f'''select record_date, team_name, player_id, player_name, match_balls, train_balls  from {DB_NAME}.{BOWL_PLANNING_TABLE_NAME}'''
        planned_df = getPandasFactoryDF(session, GET_PLANNED_DATA)
        planned_df['record_date'] = pd.to_datetime(planned_df['record_date'])

        FITNESS_FORM_SQL = f'''select record_date, team_name, player_id, player_name, bowling_train_balls, bowling_match_balls  from {DB_NAME}.{DAILY_ACTIVITY_TABLE_NAME} '''

        fitness_form_df = getPandasFactoryDF(session, FITNESS_FORM_SQL).drop_duplicates(
            subset=["record_date", "player_name"], keep="last")

        fitness_form_df = fitness_form_df.mask(fitness_form_df == -1, 0)
        planned_df = planned_df.mask(planned_df == -1, 0)
        fitness_data = fitness_data.mask(fitness_data == -1, 0)

        if "player_name" in request.json:
            player_name = request.json.get('player_name')
            planned_df = planned_df[planned_df['player_name'].isin(player_name)]
            fitness_data = fitness_data[fitness_data['player_name'].isin(player_name)]
            fitness_form_df = fitness_form_df[fitness_form_df['player_name'].isin(player_name)]

        if "player_id" in request.json:
            player_id = request.json.get('player_id')
            planned_df = planned_df[planned_df['player_id'].isin(player_id)]
            fitness_data = fitness_data[fitness_data['player_id'].isin(player_id)]
            fitness_form_df = fitness_form_df[fitness_form_df['player_id'].isin(player_id)]

        if "team_name" in request.json:
            team_name = request.json.get('team_name')
            planned_df = planned_df[planned_df['team_name'] == team_name]
            fitness_data = fitness_data[fitness_data['team_name'] == team_name]
            fitness_form_df = fitness_form_df[fitness_form_df['team_name'] == team_name]

        if all(key in request.json for key in ('from_date', 'to_date')):
            from_date = request.json.get('from_date')
            from_date = datetime.strptime(
                datetime.strptime(str(from_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()
            to_date = request.json.get('to_date')
            to_date = datetime.strptime(
                datetime.strptime(str(to_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()
        else:
            to_date = date.today()
            from_date = date.today() - timedelta(days=7)

        planned_df = planned_df[(planned_df['record_date'] >= pd.Timestamp(from_date).floor('D')) & (
                planned_df['record_date'] <= pd.Timestamp(to_date).floor('D'))]
        planned_df = planned_df.sort_values(by='record_date')
        planned_df_grp = planned_df.groupby(['player_name', 'record_date']).agg(
            {'train_balls': 'sum', 'match_balls': 'sum'}).reset_index()
        planned_df_grp.set_index('record_date', inplace=True)
        planned_df_grp['match_train_balls'] = planned_df_grp['train_balls'].fillna(0) + planned_df_grp[
            'match_balls'].fillna(0)
        planned_df_grp['planned_week_balls'] = planned_df_grp.groupby('player_name')['match_train_balls'].rolling(
            window="7D", min_periods=1).sum().reset_index(0, drop=True)
        planned_df_grp['planned_month_balls'] = planned_df_grp.groupby('player_name')['match_train_balls'].rolling(
            window="28D", min_periods=1).sum().reset_index(0, drop=True)
        planned_df_grp['planned_month_balls'] = (planned_df_grp['planned_month_balls'] / 4).round()
        planned_df_grp['planned_spike'] = (planned_df_grp['planned_week_balls'].fillna(0) / planned_df_grp[
            'planned_month_balls'].fillna(0)) * 100
        planned_df_grp = planned_df_grp.reset_index()
        planned_df_grp.drop('match_train_balls', axis=1, inplace=True)
        fut_date = planned_df_grp[planned_df_grp['record_date'] > pd.Timestamp(date.today()).floor('D')]

        fitness_data['record_date'] = pd.to_datetime(fitness_data['record_date'])
        fitness_data = fitness_data[(fitness_data['record_date'] >= pd.Timestamp(from_date).floor('D')) & (
                fitness_data['record_date'] <= pd.Timestamp(to_date).floor('D'))]
        gps_new = fitness_data.sort_values(by='record_date')
        gps_grp = gps_new.groupby(['player_name', 'record_date']).agg({'ball_no': 'count'}).reset_index()
        gps_grp.set_index('record_date', inplace=True)
        gps_grp['week_balls'] = gps_grp.groupby('player_name')['ball_no'].rolling(window="7D",
                                                                                  min_periods=1).sum().reset_index(0,
                                                                                                                   drop=True)
        gps_grp['month_balls'] = gps_grp.groupby('player_name')['ball_no'].rolling(window="28D",
                                                                                   min_periods=1).sum().reset_index(0,
                                                                                                                    drop=True)
        gps_grp['month_balls'] = (gps_grp['month_balls'] / 4).round()
        gps_grp['gps_spike'] = (gps_grp['week_balls'].fillna(0) / gps_grp['month_balls'].fillna(0)) * 100
        gps_grp = gps_grp.reset_index()
        gps_final_new = pd.merge(gps_grp, planned_df_grp, on=['player_name', 'record_date'], how='left').rename(
            columns={'ball_no': 'total_balls'})
        gps_final_new = pd.merge(gps_final_new, fut_date, on=['player_name', 'record_date'], how='outer')
        gps_final_new['planned_week_balls_x'] = gps_final_new['planned_week_balls_x'].fillna(
            gps_final_new['planned_week_balls_y'])
        gps_final_new['planned_spike_x'] = gps_final_new['planned_spike_x'].fillna(gps_final_new['planned_spike_y'])
        gps_final_new['planned_month_balls_x'] = gps_final_new['planned_month_balls_x'].fillna(
            gps_final_new['planned_month_balls_y'])
        gps_final_new.drop(['planned_month_balls_y', 'planned_week_balls_y'], axis=1, inplace=True)
        gps_final_new.rename(
            columns={'planned_week_balls_x': 'planned_week_balls', 'planned_month_balls_x': 'planned_month_balls',
                     'planned_spike_x': 'planned_spike'},
            inplace=True)

        fitness_form_df['record_date'] = pd.to_datetime(fitness_form_df['record_date'])
        fitness_form_df = fitness_form_df[(fitness_form_df['record_date'] >= pd.Timestamp(from_date).floor('D')) & (
                fitness_form_df['record_date'] <= pd.Timestamp(to_date).floor('D'))]
        play_new = fitness_form_df.groupby(['player_name', 'record_date']).agg(
            {'bowling_match_balls': 'sum', 'bowling_train_balls': 'sum'}).reset_index()
        play_new['player_entered_total_balls'] = play_new['bowling_match_balls'] + play_new['bowling_train_balls']
        play_new.set_index('record_date', inplace=True)
        play_new['player_entered_week_balls'] = play_new.groupby('player_name')['player_entered_total_balls'].rolling(
            window="7D", min_periods=1).sum().reset_index(0, drop=True)
        play_new['player_entered_month_balls'] = play_new.groupby('player_name')['player_entered_total_balls'].rolling(
            window="28D", min_periods=1).sum().reset_index(0, drop=True)
        play_new['player_entered_month_balls'] = (play_new['player_entered_month_balls'] / 4).round()
        play_new['player_entered_spike'] = (play_new['player_entered_week_balls'].fillna(0) / play_new[
            'player_entered_month_balls'].fillna(0)) * 100
        play_new = play_new.reset_index()
        play_new_final = pd.merge(play_new, planned_df_grp, on=['player_name', 'record_date'], how='left')
        play_new_final = pd.merge(play_new_final, fut_date, on=['player_name', 'record_date'], how='outer')
        play_new_final['planned_week_balls_x'] = play_new_final['planned_week_balls_x'].fillna(
            play_new_final['planned_week_balls_y'])
        play_new_final['planned_month_balls_x'] = play_new_final['planned_month_balls_x'].fillna(
            play_new_final['planned_month_balls_y'])
        play_new_final['planned_spike_x'] = play_new_final['planned_spike_x'].fillna(play_new_final['planned_spike_y'])
        play_new_final.drop(['planned_month_balls_y', 'planned_week_balls_y'], axis=1, inplace=True)
        play_new_final.rename(
            columns={'planned_week_balls_x': 'planned_week_balls', 'planned_month_balls_x': 'planned_month_balls',
                     'planned_spike_x': 'planned_spike'}, inplace=True)

        play_new_final['record_date'] = play_new_final['record_date'].astype(str)
        gps_final_new['record_date'] = gps_final_new['record_date'].astype(str)
        gps_play = gps_final_new[
            ['week_balls', 'month_balls', 'planned_week_balls', 'planned_month_balls', 'total_balls', 'player_name',
             'record_date', 'gps_spike', 'planned_spike']].copy()
        gps_play = gps_play[
            (gps_play['planned_spike'].notna()) | (gps_play['gps_spike'].notna()) | (gps_play['week_balls'].notna()) | (
                gps_play['month_balls'].notna()) | (gps_play['planned_week_balls'].notna()) | (
                gps_play['planned_month_balls'].notna()) | (gps_play['total_balls'].notna())]
        # gps_play = gps_play.dropna(axis=0, subset=['week_balls', 'month_balls', 'planned_week_balls', 'planned_month_balls','total_balls'], thresh=5)
        gps_play = gps_play.replace(np.inf, -1)
        gps_play[['week_balls', 'month_balls', 'planned_week_balls', 'planned_month_balls', 'total_balls', 'gps_spike',
                  'planned_spike']] = gps_play[
            ['week_balls', 'month_balls', 'planned_week_balls', 'planned_month_balls', 'total_balls', 'gps_spike',
             'planned_spike']].fillna(-1).astype(int)
        gps_play['data'] = gps_play[
            ['week_balls', 'month_balls', 'planned_week_balls', 'planned_month_balls', 'total_balls', 'gps_spike',
             'planned_spike']].to_dict(orient='records')
        response['gps_data'] = gps_play.drop(
            ['week_balls', 'month_balls', 'planned_week_balls', 'planned_month_balls', 'total_balls', 'gps_spike',
             'planned_spike'], axis=1).groupby('player_name') \
            .apply(lambda x: x.set_index('record_date')[['data']].to_dict(orient='index')).to_dict()

        form_play = play_new_final[
            ['player_entered_week_balls', 'player_entered_month_balls', 'planned_week_balls', 'planned_month_balls',
             'player_entered_total_balls', 'player_name', 'record_date', 'planned_spike',
             'player_entered_spike']].copy()
        form_play = form_play[(form_play['planned_spike'].notna()) | (form_play['player_entered_spike'].notna()) | (
            form_play['player_entered_week_balls'].notna()) | (form_play['player_entered_month_balls'].notna()) | (
                                  form_play['planned_week_balls'].notna()) | (
                                  form_play['planned_month_balls'].notna()) | (
                                  form_play['player_entered_total_balls'].notna())]
        form_play = form_play.replace(np.inf, -1)
        form_play[
            ['planned_week_balls', 'planned_month_balls', 'player_entered_week_balls', 'player_entered_month_balls',
             'player_entered_total_balls', 'planned_spike', 'player_entered_spike']] = \
            form_play[
                ['planned_week_balls', 'planned_month_balls', 'player_entered_week_balls', 'player_entered_month_balls',
                 'player_entered_total_balls', 'planned_spike', 'player_entered_spike']].fillna(-1).astype(int)

        form_play['data'] = form_play[
            ['planned_week_balls', 'planned_month_balls', 'player_entered_total_balls', 'player_entered_month_balls',
             'player_entered_week_balls', 'planned_spike', 'player_entered_spike']].to_dict(orient='records')
        response['form_data'] = form_play.drop(
            ['planned_week_balls', 'planned_month_balls', 'player_entered_total_balls', 'player_entered_month_balls',
             'player_entered_week_balls', 'planned_spike', 'player_entered_spike'], axis=1).groupby('player_name') \
            .apply(lambda x: x.set_index('record_date')[['data']].to_dict(orient='index')).to_dict()
        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def getDailyWellnessAverage():
    logger = get_logger("getDailyWellnessAverage", "getDailyWellnessAverage")
    try:
        req_filters = request.json
        validateRequest(req_filters)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        record_date = request.json.get('record_date')
        record_date = datetime.strptime(datetime.strptime(str(record_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                                        '%Y-%m-%d').date()
        record_date_7days = record_date - timedelta(days=6)
        record_date_28days = record_date - timedelta(days=27)

        FITNESS_FORM_SQL = f'''select record_date, player_id, player_name, team_name, fatigue_level_rating, sleep_rating, muscle_soreness_rating, 
                        stress_levels_rating, wellness_rating from {DB_NAME}.{DAILY_ACTIVITY_TABLE_NAME}; '''

        fitness_form_df = getPandasFactoryDF(session, FITNESS_FORM_SQL).drop_duplicates(
            subset=["record_date", "player_name"], keep="last")

        if "player_name" in request.json:
            player_name = request.json.get('player_name')
            fitness_form_df = fitness_form_df[fitness_form_df['player_name'].isin(player_name)]

        if "team_name" in request.json:
            team_name = request.json.get('team_name')
            fitness_form_df = fitness_form_df[fitness_form_df['team_name'] == team_name]

        if "player_id" in request.json:
            player_id = request.json.get('player_id')
            fitness_form_df = fitness_form_df[fitness_form_df['player_id'].isin(player_id)]

        fitness_form_df = fitness_form_df.mask(fitness_form_df == -1, 0)
        fitness_form_df['record_date_form'] = fitness_form_df['record_date'].apply(
            lambda x: datetime.strptime(datetime.strptime(str(x), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                                        '%Y-%m-%d').date())

        fitness_form_df_7d = fitness_form_df[(fitness_form_df['record_date_form'] >= record_date_7days) &
                                             (fitness_form_df['record_date_form'] <= record_date)]

        fitness_form_df_7d = fitness_form_df_7d.groupby(['player_name', 'player_id']).agg(
            fatigue_level_rating_7d=('fatigue_level_rating', 'mean'),
            sleep_rating_7d=('sleep_rating', 'mean'),
            muscle_soreness_rating_7d=('muscle_soreness_rating', 'mean'),
            stress_levels_rating_7d=("stress_levels_rating", "mean"),
            wellness_rating_7d=("wellness_rating", "mean")).reset_index()

        fitness_form_df_7d = fitness_form_df_7d.round(decimals=1)

        fitness_form_df_28d = fitness_form_df[(fitness_form_df['record_date_form'] >= record_date_28days) &
                                              (fitness_form_df['record_date_form'] <= record_date)]

        fitness_form_df_28d = fitness_form_df_28d.groupby(['player_name', 'player_id']).agg(
            fatigue_level_rating_28d=('fatigue_level_rating', 'mean'),
            sleep_rating_28d=('sleep_rating', 'mean'),
            muscle_soreness_rating_28d=('muscle_soreness_rating', 'mean'),
            stress_levels_rating_28d=("stress_levels_rating", "mean"),
            wellness_rating_28d=("wellness_rating", "mean")).reset_index()

        fitness_form_df_28d = fitness_form_df_28d.round(decimals=1)
        final_fitness_df = (fitness_form_df_7d.merge(fitness_form_df_28d, on='player_name', how='left')
                            .rename(columns={"player_id_x": "player_id"}).drop(["player_id_y"], axis=1))

        final_fitness_df['fatigue_level_rating_7dv28d'] = round(
            final_fitness_df['fatigue_level_rating_7d'] - final_fitness_df['fatigue_level_rating_28d'], 1)

        final_fitness_df['sleep_rating_7dv28d'] = round(
            final_fitness_df['sleep_rating_7d'] - final_fitness_df['sleep_rating_28d'], 1)

        final_fitness_df['muscle_soreness_rating_7dv28d'] = round(
            final_fitness_df['muscle_soreness_rating_7d'] - final_fitness_df['muscle_soreness_rating_28d'], 1)

        final_fitness_df['stress_levels_rating_7dv28d'] = round(
            final_fitness_df['stress_levels_rating_7d'] - final_fitness_df['stress_levels_rating_28d'], 1)

        final_fitness_df['wellness_rating_7dv28d'] = round(
            final_fitness_df['wellness_rating_7d'] - final_fitness_df['wellness_rating_28d'], 1)

        phase_conditions = [
            (final_fitness_df['fatigue_level_rating_7d'] + final_fitness_df['sleep_rating_7d'] +
             + final_fitness_df['muscle_soreness_rating_7d'] + final_fitness_df['stress_levels_rating_7d'] +
             final_fitness_df['wellness_rating_7d']) > (
                    final_fitness_df['fatigue_level_rating_28d'] + final_fitness_df['sleep_rating_28d'] +
                    + final_fitness_df['muscle_soreness_rating_28d'] + final_fitness_df[
                        'stress_levels_rating_28d'] +
                    final_fitness_df['wellness_rating_28d']),
            (final_fitness_df['fatigue_level_rating_7d'] + final_fitness_df['sleep_rating_7d'] +
             + final_fitness_df['muscle_soreness_rating_7d'] + final_fitness_df['stress_levels_rating_7d'] +
             final_fitness_df['wellness_rating_7d']) == (
                    final_fitness_df['fatigue_level_rating_28d'] + final_fitness_df['sleep_rating_28d'] +
                    + final_fitness_df['muscle_soreness_rating_28d'] + final_fitness_df[
                        'stress_levels_rating_28d'] +
                    final_fitness_df['wellness_rating_28d']),
            (final_fitness_df['fatigue_level_rating_7d'] + final_fitness_df['sleep_rating_7d'] +
             + final_fitness_df['muscle_soreness_rating_7d'] + final_fitness_df['stress_levels_rating_7d'] +
             final_fitness_df['wellness_rating_7d']) < (
                    final_fitness_df['fatigue_level_rating_28d'] + final_fitness_df['sleep_rating_28d'] +
                    + final_fitness_df['muscle_soreness_rating_28d'] + final_fitness_df[
                        'stress_levels_rating_28d'] +
                    final_fitness_df['wellness_rating_28d'])
        ]
        # different alerts
        phase_values = [1, 0, -1]

        final_fitness_df['7dv28d_trend'] = np.select(phase_conditions, phase_values)
        final_fitness_df["player_id"] = final_fitness_df["player_id"].fillna(-1).astype(int)

        return final_fitness_df.to_json(orient='records'), 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def putFitnessForm():
    logger = get_logger("putFitnessForm", "putFitnessForm")
    try:
        req_filters = request.json
        for req in req_filters:
            validateRequest(req)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        response = {}
        json_data = request.json
        record_date = request.json[0].get('record_date')
        record_date_form = datetime.strptime(str(record_date).split(" ")[0], '%Y-%m-%d').strftime('%A, %b %d, %Y')
        player_name = request.json[0].get('player_name')
        team_name = request.json[0].get('team_name')

        input_date = datetime.strptime(record_date, "%Y-%m-%d")

        current_date = datetime.now().date()

        # Check if the given date is less than or equal to the current date
        if input_date.date() <= current_date:

            FITNESS_FORM_SQL = f'''select record_date, player_name, team_name from 
            {DB_NAME}.{DAILY_ACTIVITY_TABLE_NAME} where record_date=? and player_name=? and team_name=? ALLOW FILTERING;'''
            past_form_df = getPandasFactoryDF(session, FITNESS_FORM_SQL, is_prepared=True,
                                              parameter_list=[record_date_form, player_name, team_name])
            if len(past_form_df) == 0:

                form_df = pd.DataFrame(json_data)

                if 'player_id' not in form_df.columns:
                    player_mapping_df = getPandasFactoryDF(session,
                                                           f'''select id as player_id, name, catapult_id from {DB_NAME}.playermapping''')
                    player_mapping_df = player_mapping_df[~player_mapping_df["catapult_id"].isnull()][
                        ["name", "player_id"]]

                    form_df = form_df.merge(player_mapping_df, left_on="player_name", right_on="name", how="left").drop(
                        ["name"], axis=1)
                    try:
                        form_df["player_id"] = int(form_df["player_id"])
                    except Exception as e:
                        raise HTTPException(response=Response(f"Player does not exist in the system", 500))

                form_df['record_date'] = form_df['record_date'].apply(
                    lambda x: datetime.strptime(str(x).split(" ")[0], '%Y-%m-%d').strftime('%A, %b %d, %Y'))
                max_key_val = getMaxId(session, DAILY_ACTIVITY_TABLE_NAME, "id", DB_NAME)
                form_data = generateSeq(form_df, "id", max_key_val)
                if len(form_data) > 0:
                    insertToDB(session, form_data.to_dict(orient='records'), DB_NAME, DAILY_ACTIVITY_TABLE_NAME)
                    player_load_data = playerLoadData(form_data)
                    if player_load_data:
                        insertToDB(session, player_load_data, DB_NAME, PLAYER_LOAD_TABLE_NAME)
                    response = jsonify("Data Inserted Successfully!")

                return response, 200, logger.info("Status - 200")
            else:
                response = jsonify(f"Data for DATE-{record_date} already exists for {player_name}")
                return response, 200, logger.info("Status - 200")
        else:
            response = jsonify(f"The input date is greater then the current date")
            return response, 200, logger.info("Status - 200")
    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def updateFitnessForm():
    try:
        req_filters = request.json
        for req in req_filters:
            validateRequest(req)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        json_data = request.json
        form_df = pd.DataFrame(json_data)
        if 'player_id' not in form_df.columns:
            player_mapping_df = getPandasFactoryDF(session,
                                                   f'''select id as player_id, name, catapult_id from {DB_NAME}.playermapping''')
            player_mapping_df = player_mapping_df[~player_mapping_df["catapult_id"].isnull()][["name", "player_id"]]

            form_df = form_df.merge(player_mapping_df, left_on="player_name", right_on="name", how="left").drop(["name"],
                                                                                                                axis=1)

        form_df['record_date'] = form_df['record_date'].apply(
            lambda x: datetime.strptime(str(x).split(" ")[0], '%Y-%m-%d').strftime('%A, %b %d, %Y'))

        id = json_data[0].get('id')
        parameter_list = [id]
        fitness_form_update_dict = dropFilter(['record_date', 'player_id', 'player_name', 'team_name', 'id'], json_data[0])
        fitness_form_set_values = getUpdateSetValues(fitness_form_update_dict)
        fitness_form_update_sql = f"update {DB_NAME}.{DAILY_ACTIVITY_TABLE_NAME} set {', '.join(fitness_form_set_values)} where id=?"
        prepared_query = session.prepare(fitness_form_update_sql)
        session.execute(prepared_query, parameter_list)

        player_load_data = playerLoadData(form_df)
        if player_load_data:
            player_load_update_dict = dropFilter(['record_date', 'player_id', 'player_name', 'team_name', 'id'],
                                                 player_load_data[0])
            player_load_set_values = getUpdateSetValues(player_load_update_dict)

            player_load_update_sql = f"update {DB_NAME}.{PLAYER_LOAD_TABLE_NAME} set {', '.join(player_load_set_values)} where id=?"

            pl_prepared_query = session.prepare(player_load_update_sql)
            session.execute(pl_prepared_query, parameter_list)

        response = jsonify(["Data Updated Successfully!"])

        return response
    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))

# @token_required
def getFormStats():
    logger = get_logger("getFormStats", "getFormStats")
    try:
        req_filters = request.json
        validateRequest(req_filters)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))

    def rpe_checker(value):
        if 0 < value <= 6:
            return -1
        elif 7 <= value <= 8:
            return 0
        elif 9 <= value <= 10:
            return 1

    def nps_checker(value):
        if value == 0:
            return "passive"
        elif value == -1:
            return "detractor"
        elif value == 1:
            return "promoter"

    try:
        record_date = request.json.get('record_date')
        season = datetime.strptime(record_date, '%A, %b %d, %Y').year

        FITNESS_FORM_SQL = f'''select batting_match_mins,batting_match_rpe,batting_train_mins, 
        batting_train_rpe,bowling_match_balls,bowling_match_mins,bowling_match_rpe,bowling_train_mins, 
        bowling_train_rpe,cross_training_mins,cross_training_rpe,fatigue_level_rating,fielding_match_mins, 
        fielding_match_rpe,fielding_train_mins,fielding_train_rpe,form_filler,muscle_soreness_rating,played_today,
        player_name,player_id, reason_noplay_or_train,record_date,rehab_mins,rehab_rpe,running_mins,running_rpe,
        sleep_rating,strength_mins,strength_rpe,stress_levels_rating,trained_today,wellness_rating,bowling_train_balls from 
         {DB_NAME}.{DAILY_ACTIVITY_TABLE_NAME}'''

        parameter_list = []
        if "team_name" in request.json:
            team_name = request.json.get('team_name')
            parameter_list.append(team_name)
            FITNESS_FORM_SQL = FITNESS_FORM_SQL + f" where team_name=? ALLOW FILTERING;"
        else:
            team_name = 'Mumbai Indians'
            FITNESS_FORM_SQL = FITNESS_FORM_SQL

        response = {}
        fitness_form_df = getPandasFactoryDF(session, FITNESS_FORM_SQL, is_prepared=True,
                                             parameter_list=parameter_list).drop_duplicates(
            subset=["record_date", "player_name", "player_id"],
            keep="last")
        if len(fitness_form_df) > 0:

            fitness_form_df = fitness_form_df.mask(fitness_form_df == -1, np.NaN)
            fitness_form_df = fitness_form_df.mask(fitness_form_df == 'NA', np.NaN)
            folder_name = 'mi-teams-images/'
            fitness_form_df['player_image_url'] = IMAGE_STORE_URL + folder_name + + team_name.replace(" ", "-").lower() + "/" +\
                                                  fitness_form_df['player_name'].apply(lambda x: x.replace(' ', '-')
                                                                                       .lower()).astype(str) + ".png"
            defaulting_image_url(fitness_form_df, 'player_image_url', 'team_name', 'Mumbai Indian Womens', folder_name)

            value_count_list = [
                'form_filler',
                'muscle_soreness_rating',
                'played_today',
                'reason_noplay_or_train',
                'stress_levels_rating',
                'trained_today',
                'wellness_rating',
                'fatigue_level_rating',
                'sleep_rating'
            ]
            stats_mins_column = [
                'batting_match_mins',
                'batting_train_mins',
                'bowling_match_mins',
                'bowling_train_mins',
                'cross_training_mins',
                'fielding_train_mins',
                'fielding_match_mins',
                'rehab_mins',
                'strength_mins',
                'running_mins',

            ]
            stats_rpe_column = [
                'batting_match_rpe',
                'batting_train_rpe',
                'bowling_match_rpe',
                'bowling_train_rpe',
                'cross_training_rpe',
                'fielding_match_rpe',
                'fielding_train_rpe',
                'rehab_rpe',
                'running_rpe',
                'strength_rpe'
            ]

            fitness_form_df = fitness_form_df[fitness_form_df['record_date'] == record_date]
            df_row_len = len(fitness_form_df.index)
            for col in stats_mins_column:
                value = round(fitness_form_df[col].fillna(0).sum() / df_row_len, 2)
                response[col] = value if not math.isnan(value) else -1
            for col in stats_rpe_column:
                fitness_form_df[col] = fitness_form_df[col].apply(lambda x: rpe_checker(x))
                if fitness_form_df.groupby(col).count().empty:
                    response[col] = {
                        "nps": "nan",
                        "detractor": -1,
                        "passive": -1,
                        "promoter": -1
                    }
                else:
                    response[col] = {
                        "nps": round((fitness_form_df[col].sum() / df_row_len) * 100, 2)
                    }
                    fitness_form_df[col] = fitness_form_df[col].apply(lambda x: nps_checker(x))
                    response[col].update({
                        "detractor": str(fitness_form_df[col].str.count("detractor").sum()),
                        "passive": str(fitness_form_df[col].str.count("passive").sum()),
                        "promoter": str(fitness_form_df[col].str.count("promoter").sum())
                    })

            response['filled'] = fitness_form_df.set_index('player_name')['player_image_url'].to_dict()
            for col in value_count_list:
                response[col] = fitness_form_df[col].value_counts().to_dict()
            response["bowling_match_balls"] = int(fitness_form_df["bowling_match_balls"].count())
            response["bowling_train_balls"] = int(fitness_form_df["bowling_train_balls"].count())

        else:
            response = {"batting_match_mins": -1,
                        "batting_match_rpe": {"detractor": -1, "nps": "nan", "passive": -1, "promoter": -1},
                        "batting_train_mins": -1,
                        "batting_train_rpe": {"detractor": -1, "nps": "nan", "passive": -1, "promoter": -1},
                        "bowling_match_balls": 0, "bowling_match_mins": -1,
                        "bowling_match_rpe": {"detractor": -1, "nps": "nan", "passive": -1, "promoter": -1},
                        "bowling_train_balls": 0, "bowling_train_mins": -1,
                        "bowling_train_rpe": {"detractor": -1, "nps": "nan", "passive": -1, "promoter": -1},
                        "cross_training_mins": -1,
                        "cross_training_rpe": {"detractor": -1, "nps": "nan", "passive": -1, "promoter": -1},
                        "fatigue_level_rating": {}, "fielding_match_mins": -1,
                        "fielding_match_rpe": {"detractor": -1, "nps": "nan", "passive": -1, "promoter": -1},
                        "fielding_train_mins": -1,
                        "fielding_train_rpe": {"detractor": -1, "nps": "nan", "passive": -1, "promoter": -1},
                        "filled": {}, "form_filler": {}, "muscle_soreness_rating": {}, "played_today": {},
                        "reason_noplay_or_train": {}, "rehab_mins": -1,
                        "rehab_rpe": {"detractor": -1, "nps": "nan", "passive": -1, "promoter": -1}, "running_mins": -1,
                        "running_rpe": {"detractor": -1, "nps": "nan", "passive": -1, "promoter": -1},
                        "sleep_rating": {}, "strength_mins": -1,
                        "strength_rpe": {"detractor": -1, "nps": "nan", "passive": -1, "promoter": -1},
                        "stress_levels_rating": {}, "trained_today": {}, "wellness_rating": {}}

        fitness_form_not_filled = getFormNotFilledPlayers(fitness_form_df, season, team_name)
        if len(fitness_form_not_filled) > 0:
            folder_name = 'mi-teams-images/'
            fitness_form_not_filled['player_image_url'] = IMAGE_STORE_URL + folder_name  + team_name.replace(" ", "-").lower() + "/"+\
                                                          fitness_form_not_filled['player_name'].apply(
                                                              lambda x: x.replace(' ', '-')
                                                              .lower()).astype(str) + ".png"
            defaulting_image_url(fitness_form_not_filled, 'player_image_url', 'team_name', 'Mumbai Indian Womens', folder_name)
        else:
            fitness_form_not_filled = pd.DataFrame({'player_name': pd.Series(dtype='str'),
                                                    'player_image_url': pd.Series(dtype='str')})

        response['not_filled'] = fitness_form_not_filled.set_index('player_name')['player_image_url'].to_dict()
        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def checkInputForm():
    logger = get_logger("checkInputForm", "checkInputForm")
    try:
        req_filters = request.json
        validateRequest(req_filters)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        fitness_form_df = getPandasFactoryDF(session, f'''select record_date, player_id, player_name, team_name  from 
                        {DB_NAME}.{DAILY_ACTIVITY_TABLE_NAME}''').drop_duplicates(
            subset=["record_date", "player_name", "team_name"],
            keep="last")

        record_date = request.json.get('record_date')
        player_name = request.json.get('player_name')
        team_name = request.json.get('team_name')

        if "player_name" in request.json:
            fitness_form_df = fitness_form_df[
                (fitness_form_df['record_date'] == record_date) & (fitness_form_df['player_name'] == player_name)
                & (fitness_form_df['team_name'] == team_name)]
        elif "player_id" in request.json:
            player_id = request.json.get('player_id')
            fitness_form_df = fitness_form_df[
                (fitness_form_df['record_date'] == record_date) & (fitness_form_df['player_id'] == player_id)
                & (fitness_form_df['team_name'] == team_name)]

        if len(fitness_form_df) > 0:
            return jsonify({"data_available": 1}), 200
        else:
            return jsonify({"data_available": 0}), 200

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def getPlayerFormInput():
    logger = get_logger("getPlayerFormInput", "getPlayerFormInput")
    try:
        req_filters = request.json
        validateRequest(req_filters)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:

        FITNESS_FORM_SQL = f'''select id, record_date, player_id, player_name, team_name, batting_match_mins,batting_match_rpe,batting_train_mins, 
        batting_train_rpe,bowling_match_balls,bowling_match_mins,bowling_match_rpe,bowling_train_mins, 
        bowling_train_rpe,cross_training_mins,cross_training_rpe,fatigue_level_rating,fielding_match_mins, 
        fielding_match_rpe,fielding_train_mins,fielding_train_rpe,form_filler,muscle_soreness_rating,played_today,
        reason_noplay_or_train,rehab_mins,rehab_rpe,running_mins,running_rpe,
        sleep_rating,strength_mins,strength_rpe,stress_levels_rating,trained_today,wellness_rating,bowling_train_balls from 
         {DB_NAME}.{DAILY_ACTIVITY_TABLE_NAME} '''
        parameter_list = []
        if "team_name" in request.json:
            team_name = request.json.get('team_name')
            parameter_list.append(team_name)
            FITNESS_FORM_SQL = FITNESS_FORM_SQL + f" where team_name=? ALLOW FILTERING;"
        else:
            FITNESS_FORM_SQL = FITNESS_FORM_SQL

        fitness_form_df = getPandasFactoryDF(session, FITNESS_FORM_SQL, is_prepared=True,
                                             parameter_list=parameter_list).drop_duplicates(
            subset=["record_date", "player_id", "player_name", "team_name"],
            keep="last")

        if "player_name" in request.json:
            player_name = request.json.get('player_name')
            fitness_form_df = fitness_form_df[fitness_form_df['player_name'].isin(player_name)]

        if "player_id" in request.json:
            player_id = request.json.get('player_id')
            fitness_form_df = fitness_form_df[fitness_form_df['player_id'].isin(player_id)]

        if "user_id" in request.json:
            player_id = request.json["user_id"]
            fitness_form_df = fitness_form_df[fitness_form_df['player_id'].isin([player_id])]

        if "user_name" in request.json:
            player_name = request.json["user_name"]
            fitness_form_df = fitness_form_df[fitness_form_df['player_name'].isin([player_name])]

        if "record_date" in request.json:
            record_date = request.json.get('record_date')
            fitness_form_df = fitness_form_df[fitness_form_df['record_date'].isin([record_date])]

        if all(key in request.json for key in ('from_date', 'to_date')):
            from_date = request.json.get('from_date')
            from_date = datetime.strptime(
                datetime.strptime(str(from_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()
            to_date = request.json.get('to_date')
            to_date = datetime.strptime(
                datetime.strptime(str(to_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()

            fitness_form_df['record_date_form'] = fitness_form_df['record_date'].apply(
                lambda x: datetime.strptime(datetime.strptime(str(x), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                                            '%Y-%m-%d').date())

            fitness_form_df = fitness_form_df[
                (fitness_form_df['record_date_form'] >= from_date) & (
                        fitness_form_df['record_date_form'] <= to_date)].drop('record_date_form', axis=1)

        folder_name = 'mi-teams-images/'
        fitness_form_df['player_image_url'] = IMAGE_STORE_URL + folder_name + 'mumbai-indians/' + \
                                              fitness_form_df['player_name'].apply(lambda x: x.replace(' ', '-')
                                                                                   .lower()).astype(str) + ".png"
        defaulting_image_url(fitness_form_df, 'player_image_url', 'team_name', 'Mumbai Indian Womens', folder_name)

        return fitness_form_df.to_json(orient="records"), logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def putMatchPeakLoad():
    logger = get_logger("putMatchPeakLoad", "putMatchPeakLoad")
    try:
        req_filters = request.json
        for req in req_filters:
            validateRequest(req)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        json_data = request.json
        peak_load_Df = pd.DataFrame(json_data)
        if 'player_id' not in peak_load_Df.columns:
            player_mapping_df = getPandasFactoryDF(session,
                                                   f'''select id as player_id, name, catapult_id from {DB_NAME}.playermapping''')
            player_mapping_df = player_mapping_df[~player_mapping_df["catapult_id"].isnull()][["name", "player_id"]]

            peak_load_Df = peak_load_Df.merge(player_mapping_df, left_on="player_name", right_on="name",
                                              how="left").drop(
                ["name"], axis=1)
        peak_load_Df['record_date'] = peak_load_Df['record_date'].apply(
            lambda x: datetime.strptime(str(x).split(" ")[0], '%Y-%m-%d').strftime('%A, %b %d, %Y'))
        max_key_val = getMaxId(session, PEAK_LOAD_TABLE_NAME, "id", DB_NAME)
        peak_load_data = generateSeq(peak_load_Df, "id", max_key_val)

        if len(peak_load_data) > 0:
            insertToDB(session, peak_load_data.to_dict(orient='records'), DB_NAME, PEAK_LOAD_TABLE_NAME)

            response = jsonify("Data Inserted Successfully!")
        else:
            response = jsonify("No New Data!")

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def getMatchPeakLoad():
    logger = get_logger("getMatchPeakLoad", "getMatchPeakLoad")
    try:
        req = dict()
        req['team_name'] = request.args.get('team_name')
        validateRequest(req)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        team_name = request.args.get('team_name')
        parameter_list = [team_name]
        GET_MATCH_PEAK_LOAD = f'''select record_date, team_name, player_id, player_name,  
                match_peak_load from {DB_NAME}.matchPeakLoad where team_name=? ALLOW FILTERING;'''
        match_peak_load_df = getPandasFactoryDF(session, GET_MATCH_PEAK_LOAD, is_prepared=True,
                                                parameter_list=parameter_list)
        match_peak_load_df['match_peak_load'] = match_peak_load_df['match_peak_load'].apply(pd.to_numeric,
                                                                                            errors='coerce')

        folder_name = 'mi-teams-images/'
        match_peak_load_df['player_image_url'] = IMAGE_STORE_URL + folder_name + match_peak_load_df['team_name'].apply(lambda x: x.replace(' ', '-').lower()).astype(str) + '/' +  \
                                                 match_peak_load_df['player_name'].apply(lambda x: x.replace(' ', '-')
                                                                                         .lower()).astype(str) + ".png"
        defaulting_image_url(match_peak_load_df, 'player_image_url', 'team_name', 'Mumbai Indian Womens', folder_name)

        match_peak_load_df['player_details'] = match_peak_load_df[
            ['record_date', 'match_peak_load', 'player_image_url']].to_dict(
            'records')
        match_peak_load_df = match_peak_load_df.groupby('player_name')['player_details'].agg(list).reset_index()
        response = match_peak_load_df.set_index('player_name')['player_details'].to_dict()

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def putPlannedBalls():
    logger = get_logger("putPlannedBalls", "putPlannedBalls")
    try:
        req_filters = request.json
        for req in req_filters:
            validateRequest(req)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        json_data = request.json
        planned_df = pd.DataFrame(json_data)
        if 'player_id' not in planned_df.columns:
            player_mapping_df = getPandasFactoryDF(session,
                                                   f'''select id as player_id, name, catapult_id from {DB_NAME}.playermapping''')
            player_mapping_df = player_mapping_df[~player_mapping_df["catapult_id"].isnull()][["name", "player_id"]]

            planned_df = planned_df.merge(player_mapping_df, left_on="player_name", right_on="name", how="left").drop(
                ["name"], axis=1)
        if any("id" in d for d in json_data):
            update_planned_df = planned_df[~planned_df['id'].isnull()].to_dict(orient='records')
            new_planned_df = planned_df[planned_df['id'].isnull()].drop('id', axis=1, errors='ignore')
        else:
            update_planned_df = pd.DataFrame({'record_date': pd.Series(dtype='str'),
                                              'player_id': pd.Series(dtype='int'),
                                              'player_name': pd.Series(dtype='str'),
                                              'team_name': pd.Series(dtype='str'),
                                              'train_balls': pd.Series(dtype='int'),
                                              'match_balls': pd.Series(dtype='int')})
            new_planned_df = planned_df

        new_planned_df['record_date'] = new_planned_df['record_date'].apply(
            lambda x: datetime.strptime(str(x).split(" ")[0], '%Y-%m-%d').strftime('%A, %b %d, %Y'))
        max_key_val = getMaxId(session, BOWL_PLANNING_TABLE_NAME, BOWL_PLANNING_KEY_COL, DB_NAME)
        new_planned_data = generateSeq(new_planned_df, BOWL_PLANNING_KEY_COL, max_key_val)

        if len(new_planned_data) > 0:
            insertToDB(session, new_planned_data.to_dict(orient='records'), DB_NAME, BOWL_PLANNING_TABLE_NAME)

            logger.info("Data Inserted Successfully!")
        else:
            logger.info("No New Data!")

        # CHECK FOR UPDATES

        if len(update_planned_df) > 0:
            for obj in update_planned_df:
                updatePlannedBallData(obj, BOWL_PLANNING_TABLE_NAME)
                logger.info("Data Updated Successfully!")
        else:
            logger.info("No New Updates!")
        response = jsonify("Data Added Successfully!")
        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def getPlannedBalls():
    logger = get_logger("getPlannedBalls", "getPlannedBalls")
    try:
        req_filters = request.json
        validateRequest(req_filters)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        GET_PLANNED_DATA = f'''select id,record_date, team_name, player_id, player_name, match_balls, train_balls  from {DB_NAME}.bowlPlanning'''
        planned_df = getPandasFactoryDF(session, GET_PLANNED_DATA)
        planned_df['record_date_form'] = pd.to_datetime(planned_df['record_date'])
        if "player_name" in request.json:
            player_name = request.json.get('player_name')
            planned_df = planned_df[planned_df['player_name'].isin(player_name)]

        if "player_id" in request.json:
            player_id = request.json.get('player_id')
            planned_df = planned_df[planned_df['player_id'].isin(player_id)]

        if "team_name" in request.json:
            team_name = request.json.get('team_name')
            planned_df = planned_df[planned_df['team_name'] == team_name]

        if all(key in request.json for key in ('from_date', 'to_date')):
            from_date = request.json.get('from_date')
            from_date = datetime.strptime(
                datetime.strptime(str(from_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()
            to_date = request.json.get('to_date')
            to_date = datetime.strptime(
                datetime.strptime(str(to_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'), '%Y-%m-%d').date()
        else:
            to_date = date.today()
            from_date = date.today() - timedelta(days=7)

        planned_df = planned_df[(planned_df['record_date_form'] >= pd.Timestamp(from_date).floor('D')) & (
                planned_df['record_date_form'] <= pd.Timestamp(to_date).floor('D'))]

        if len(planned_df) > 0:
            folder_name = 'mi-teams-images/'
            planned_df['player_image_url'] = IMAGE_STORE_URL + folder_name + planned_df['team_name'].apply(lambda x: x.replace(' ', '-').lower()).astype(str) + '/' + \
                                             planned_df['player_name'].apply(lambda x: x.replace(' ', '-')
                                                                             .lower()).astype(str) + ".png"
            defaulting_image_url(planned_df, 'player_image_url', 'team_name', 'Mumbai Indian Womens', folder_name)

            planned_df = planned_df.mask(planned_df == -1, 0)
            planned_df['data'] = planned_df[['record_date', 'train_balls', 'match_balls', 'id']].to_dict(
                orient='records')
            planned_df['data'] = planned_df['data'].apply(
                lambda x: {x['record_date']: {'train_balls': x['train_balls'], 'match_balls': x['match_balls'],
                                              'id': x['id']}})

            response = planned_df.groupby(['player_name', 'player_image_url', 'player_id'])['data'] \
                .agg(list).reset_index().to_json(orient='records')

            return response, 200, logger.info("Status - 200")

        else:
            return jsonify({}), logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def insertUserQueries():
    logger = get_logger("insertUserQueries", "insertUserQueries")
    try:
        req_filters = request.json
        validateRequest(req_filters)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))

    try:
        json_data = request.json
        final_data_dict = json_data.copy()
        uuid = json_data.get("uuid")
        desc_image_code = json_data.get("desc_image")
        local_image_path = os.path.join(FILE_SHARE_PATH, "data/" + dir_name + "/")
        max_key_val = getMaxId(session, USER_QUERY_TABLE_NAME, "s_no", DB_NAME)

        if json_data.get("desc_image") != "":
            image_name = json_data.get("desc_file_name")
            final_data_dict.update(
                {"desc_image": f"{IMAGE_STORE_URL}{dir_name}/{str(max_key_val)}/{image_name}"})
            uploadImageToBlob(desc_image_code, local_image_path, str(max_key_val), f"{image_name}")

        final_data_dict = {k: [v] for k, v in final_data_dict.items()}
        final_data_dict = dropFilter(["desc_file_name"], final_data_dict)
        user_query_df = pd.DataFrame(final_data_dict)
        user_query_data = generateSeq(user_query_df, "s_no", max_key_val)
        if len(user_query_data) > 0:
            insertToDB(session, user_query_data.to_dict(orient='records'), DB_NAME, USER_QUERY_TABLE_NAME)
            response = jsonify("Data Inserted Successfully!")
        else:
            response = jsonify("No Data To Insert!")

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def listQueries():
    logger = get_logger("listQueries", "listQueries")
    try:
        req_filters = request.json
        del_list = ["user_type"]
        validateRequest(dropFilter(del_list, req_filters))
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        GET_USER_QUERIES_LIST = f'''select s_no, uuid, title, module_name, state from {DB_NAME}.userQueries'''
        user_query_df = getPandasFactoryDF(session, GET_USER_QUERIES_LIST)

        if request.json:
            if "user_type" in request.json:
                uuid = request.json.get('uuid')
                user_query_df = user_query_df[user_query_df['uuid'].isin(uuid)]

            if "module_name" in request.json:
                module_name = request.json.get('module_name')
                user_query_df = user_query_df[user_query_df['module_name'].isin(module_name)]

            if "state" in request.json:
                state = request.json.get('state')
                user_query_df = user_query_df[user_query_df['state'].isin(state)]

        response = user_query_df.to_json(orient='records')

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def getQueryDetails():
    logger = get_logger("getQueryDetails", "getQueryDetails")
    try:
        req_filters = request.json
        validateRequest(req_filters)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        s_no = request.args.get('s_no')
        GET_USER_QUERIES_DATA = f'''select * from {DB_NAME}.userQueries where s_no={s_no} ALLOW FILTERING;'''
        user_query_df = getPandasFactoryDF(session, GET_USER_QUERIES_DATA)
        user_query_df['update_ts'] = user_query_df['update_ts'].apply(
            lambda x: datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S'))
        user_query_df['desc_file_name'] = user_query_df['desc_image'].apply(
            lambda x: x.split('/')[-1] if x not in ["", np.NaN, None] else x)
        user_query_df['res_file_name'] = user_query_df['resolution_image'].apply(
            lambda x: x.split('/')[-1] if x not in ["", np.NaN, None] else x)
        response = user_query_df.to_json(orient='records')

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def updateQueryDetails():
    logger = get_logger("updateQueryDetails", "updateQueryDetails")
    try:
        req_filters = request.json
        validateRequest(req_filters)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        json_data = request.json
        uuid = json_data.get("uuid")
        res_image_code = json_data.get("resolution_image")
        s_no = json_data.get("s_no")
        local_image_path = os.path.join(FILE_SHARE_PATH, "data/" + dir_name + "/")
        update_ts = json_data.get('update_ts')
        state = json_data.get('state')
        update_query = f"update {DB_NAME}.userQueries set state='{state}', update_ts='{update_ts}' "

        if "resolution" in json_data:
            if json_data.get("resolution") != "":
                resolution = json_data.get("resolution")
                update_query = f"{update_query}, resolution='{resolution}' "

        if json_data.get("resolution_image") != "":
            image_name = json_data.get("res_file_name")
            resolution_image = f"{IMAGE_STORE_URL}{dir_name}/{str(s_no)}/{image_name}"
            update_query = f"{update_query},resolution_image='{resolution_image}' "
            uploadImageToBlob(res_image_code, local_image_path, str(s_no), f"{image_name}")

        final_update_query = f"{update_query} where s_no={s_no};"
        session.execute(final_update_query)

        response = jsonify(["Data Updated Successfully!"])

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def getQueriesCount():
    logger = get_logger("getQueriesCount", "getQueriesCount")
    try:
        req_filters = request.json
        del_list = ["user_type"]
        validateRequest(dropFilter(del_list, req_filters))
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        GET_USER_QUERIES_LIST = f'''select s_no, uuid, title, module_name, state from {DB_NAME}.userQueries'''
        user_query_df = getPandasFactoryDF(session, GET_USER_QUERIES_LIST)

        if request.json:
            if "user_type" in request.json:
                uuid = request.json.get('uuid')
                user_query_df = user_query_df[user_query_df['uuid'].isin(uuid)]

            if "module_name" in request.json:
                module_name = request.json.get('module_name')
                user_query_df = user_query_df[user_query_df['module_name'].isin(module_name)]

            if "state" in request.json:
                state = request.json.get('state')
                user_query_df = user_query_df[user_query_df['state'].isin(state)]

        response = {'query_count': int(user_query_df['s_no'].count())}

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def player_notifier():
    logger = get_logger("player_notifier", "player_notifier")
    try:
        players = request.json.get('players')
        is_text = request.json.get('is_text')
        is_mail = request.json.get('is_mail')
        is_whatsapp = request.json.get('is_whatsapp')
        date_filter = request.json.get('date_filter')
        recipient = [player.lower() for player in players]
        token_roles = get_roles_token(request.headers['token'], request.headers['token_roles'])
        # Create payload for mail, whatsapp and text message to recipient.
        notification = Notification(
            is_text=is_text,
            is_mail=is_mail,
            is_whatsapp=is_whatsapp,
            text_template_id=NOTIFICATION_TEMPLATE_ID,
            mail_template_id=NOTIFICATION_TEMPLATE_ID
        )
        payloads = {}
        if is_whatsapp:
            payloads["whatsapp_payload"] = Payload(token_roles).whatsapp_bulk_payload(
                {
                    'recipient': recipient,
                    'report_name': WELLNESS_NOTIFICATION_MODULE,
                    'campaign_template': WELLNESS_CAMPAIGN_TEMPLATE,
                    'date_filter': date_filter
                }
            )
        if is_text:
            payloads["text_payload"] = Payload(token_roles).generate_fitness_payload_text_message(
                recipient,
                NOTIFICATION_TEMPLATE_ID
            )
        if is_mail:
            payloads["mail_payload"] = Payload(token_roles).generate_fitness_payload_mail(
                recipient,
                NOTIFICATION_TEMPLATE_ID
            )

        # Send notification to players
        notification.send_bulk_notification(payloads)
        return {
            "message": "Successfully sent notification to players",
            "players": players
        }
    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def player_notifier_scheduler():
    logger = get_logger("player_notifier_scheduler", "player_notifier_scheduler")
    try:
        token_roles = get_roles_token(request.headers['token'], request.headers['token_roles'])
        team_name = request.json.get('team_name')
        players = request.json.get('players')
        is_text = request.json.get('is_text')
        is_mail = request.json.get('is_mail')
        is_whatsapp = request.json.get('is_whatsapp')
        recipient = [player.lower() for player in players]
        schedule_hour = request.json.get('schedule_hour')
        schedule_minute = request.json.get('schedule_minute')
        start_date = datetime.strptime(request.json.get('start_date'), "%Y-%m-%d")
        end_date = datetime.strptime(request.json.get('end_date'), "%Y-%m-%d")
        start_date_time = datetime(
            start_date.year, start_date.month, start_date.day, schedule_hour, schedule_minute, 0,
            0, tz.gettz('Asia/Kolkata')
        )
        end_date_time = datetime(
            end_date.year, end_date.month, end_date.day, schedule_hour, schedule_minute, 0,
            0, tz.gettz('Asia/Kolkata')
        )
        start_date_time_utc = start_date_time.astimezone(timezone.utc)
        start_date = datetime.strptime(start_date_time_utc.strftime("%Y-%m-%d"), "%Y-%m-%d")
        # End date parse to UTC
        end_date_time_utc = end_date_time.astimezone(timezone.utc)
        end_date = datetime.strptime(end_date_time_utc.strftime("%Y-%m-%d"), "%Y-%m-%d")
        schedule_hour = start_date_time_utc.hour
        schedule_minute = start_date_time_utc.minute
        # Call notification Service to send mail
        logger.warning(f"{schedule_hour}, {schedule_minute}, {start_date}, {end_date}")
        schedule_players_notification_cron(
            start_date,
            end_date,
            schedule_hour,
            schedule_minute,
            team_name,
            recipient,
            is_whatsapp,
            is_text,
            is_mail,
            token_roles
        )
        logger.info("Cron notification scheduler Setup is done via API invocation.")
        return {
            "message": "Successfully Scheduled Notification for players"
        }
    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def get_schedule():
    logger = get_logger("get_schedule", "get_schedule")
    try:
        req = dict()
        req['team_name'] = request.args.get('team_name')
        req['module'] = request.args.get('module')
        validateRequest(req)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))

    team_name = request.args.get('team_name')
    module = request.args.get('module')
    parameter_list = [team_name, module]

    try:

        result = getPandasFactoryDF(
            session,
            f"select * from {DB_NAME}.notificationscheduler where team_name=? and module=? and is_active=True ALLOW FILTERING"
            , is_prepared=True, parameter_list=parameter_list)
        if result.empty:
            return f"No Schedule Active for {team_name} of {module}"
        result['start_date'] = result['start_date'].apply(
            lambda x: datetime.strptime(str(x), '%Y-%m-%d').strftime('%Y-%m-%d')
        )
        result['end_date'] = result['end_date'].apply(
            lambda x: datetime.strptime(str(x), '%Y-%m-%d').strftime('%Y-%m-%d')
        )
        result["schedule_hour"] = result['schedule_time'][0].hour
        result["schedule_minute"] = result['schedule_time'][0].minute
        result = result.drop(['schedule_time'], axis=1)
        result = result.to_json(orient='records')
        response = json.loads(result)[0]
        start_date = datetime.strptime(response.get("start_date"), "%Y-%m-%d")
        end_date = datetime.strptime(response.get("end_date"), "%Y-%m-%d")
        start_date_time = datetime(
            start_date.year, start_date.month, start_date.day, response.get("schedule_hour"),
            response.get("schedule_minute"), 0, 0, timezone.utc
        )
        end_date_time = datetime(
            end_date.year, end_date.month, end_date.day, response.get("schedule_hour"),
            response.get("schedule_minute"), 0, 0, timezone.utc
        )
        start_date_time_ist = start_date_time.astimezone(tz.gettz('Asia/Kolkata'))
        start_date = datetime.strptime(start_date_time_ist.strftime("%Y-%m-%d"), "%Y-%m-%d")
        # End date parse to UTC
        end_date_time_utc = end_date_time.astimezone(tz.gettz('Asia/Kolkata'))
        end_date = datetime.strptime(end_date_time_utc.strftime("%Y-%m-%d"), "%Y-%m-%d")
        schedule_hour = start_date_time_ist.hour
        schedule_minute = start_date_time_ist.minute

        # Create Response
        response["start_date"] = start_date.strftime('%Y-%m-%d')
        response["end_date"] = end_date.strftime('%Y-%m-%d')
        response["schedule_hour"] = schedule_hour
        response["schedule_minute"] = schedule_minute
        return response
    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def player_report_weekly():
    logger = get_logger("player_report_weekly", "player_report_weekly")
    try:
        req_filters = request.json
        validateRequest(req_filters)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        import time
        seven_days_ago = datetime.strptime(request.json.get('end_date'), '%Y-%m-%d')
        season = seven_days_ago.year
        today = datetime.strptime(request.json.get('start_date'), '%Y-%m-%d')
        player_list = request.json.get('player_list')
        player_list = [player.lower() for player in player_list] if player_list else player_list
        eight_days_ago = seven_days_ago - DT.timedelta(days=1)
        thirteen_days_ago = seven_days_ago - DT.timedelta(days=7)
        today = pd.to_datetime(today)
        seven_days_ago = pd.to_datetime(seven_days_ago)
        eight_days_ago = pd.to_datetime(eight_days_ago)
        thirteen_days_ago = pd.to_datetime(thirteen_days_ago)
        team_name = request.json.get('team_name')
        parameter_list = [team_name]
        player_load_sql = "SELECT record_date, player_name, total_load FROM {}.{} where team_name=? ALLOW FILTERING".format(
            DB_NAME,
            PLAYER_LOAD_TABLE_NAME,
            team_name
        )
        player_load_rows = getPandasFactoryDF(session, player_load_sql, is_prepared=True, parameter_list=parameter_list)
        player_load_rows['record_date'] = player_load_rows['record_date'].apply(
            lambda x: pd.to_datetime(x, format="%A, %b %d, %Y")
        )
        player_load_rows = player_load_rows[
            (player_load_rows['record_date'] >= thirteen_days_ago) & (player_load_rows['record_date'] <= today)
            ]
        player_load_rows_current_week = player_load_rows[
            (player_load_rows['record_date'] >= seven_days_ago) & (player_load_rows['record_date'] <= today)
            ]
        player_load_rows_last_week = player_load_rows[
            (player_load_rows['record_date'] >= thirteen_days_ago) &
            (player_load_rows['record_date'] <= eight_days_ago)
            ]
        player_load_rows_last_week = player_load_rows_last_week.groupby('player_name')['total_load'].agg(
            sum).reset_index()
        player_load_rows_current_week = player_load_rows_current_week.groupby('player_name')['total_load'].agg(
            sum).reset_index()

        player_load_rows_last_week = player_load_rows_last_week[['player_name', 'total_load']]
        player_load_rows_current_week = player_load_rows_current_week[['player_name', 'total_load']]
        player_load_rows_last_week["total_load"] = player_load_rows_last_week["total_load"].astype(str)
        player_load_rows_current_week["total_xload"] = player_load_rows_current_week["total_load"].astype(str)
        player_load_combined = pd.merge(
            player_load_rows_last_week,
            player_load_rows_current_week,
            on='player_name',
            how='outer'
        )

        fitness_gps_sql = "SELECT record_date, player_name, total_player_load FROM {}.{} where team_name='{}' ALLOW FILTERING".format(
            DB_NAME,
            GPS_TABLE_NAME,
            team_name
        )
        fitness_gps_rows = getPandasFactoryDF(session, fitness_gps_sql)
        fitness_gps_rows['record_date'] = fitness_gps_rows['record_date'].apply(
            lambda x: pd.to_datetime(x, format="%A, %b %d, %Y"))
        fitness_gps_rows = fitness_gps_rows[
            (fitness_gps_rows['record_date'] >= thirteen_days_ago) & (fitness_gps_rows['record_date'] <= today)]
        fitness_gps_rows_current_week = fitness_gps_rows[
            (fitness_gps_rows['record_date'] >= seven_days_ago) & (fitness_gps_rows['record_date'] <= today)].drop(
            'record_date', axis=1)
        fitness_gps_rows_last_week = fitness_gps_rows[
            (fitness_gps_rows['record_date'] >= thirteen_days_ago) & (fitness_gps_rows['record_date'] <= eight_days_ago)
            ].drop('record_date', axis=1)
        fitness_gps_rows_current_week = fitness_gps_rows_current_week.groupby('player_name')['total_player_load'].agg(
            sum).reset_index()
        fitness_gps_rows_last_week = fitness_gps_rows_last_week.groupby('player_name')['total_player_load'].agg(
            sum).reset_index()

        fitness_gps_rows_current_week = fitness_gps_rows_current_week[['player_name', 'total_player_load']]
        fitness_gps_rows_last_week = fitness_gps_rows_last_week[['player_name', 'total_player_load']]
        fitness_gps_rows_current_week["total_player_load"] = fitness_gps_rows_current_week["total_player_load"].astype(
            str)
        fitness_gps_rows_last_week["total_player_load"] = fitness_gps_rows_last_week["total_player_load"].astype(str)

        fitness_gps_combined = pd.merge(
            fitness_gps_rows_last_week,
            fitness_gps_rows_current_week,
            on='player_name',
            how='outer'
        )

        player_load_combined = player_load_combined.rename(
            columns={
                "total_load_x": "last_week",
                "total_load_y": "current_week"
            }
        )
        fitness_gps_combined = fitness_gps_combined.rename(
            columns={
                "total_player_load_x": "last_week",
                "total_player_load_y": "current_week"
            }
        )
        player_load_combined['current_week'] = player_load_combined['current_week'].fillna(np.Infinity)
        player_load_combined['last_week'] = player_load_combined['last_week'].fillna(np.Infinity)

        fitness_gps_combined['current_week'] = fitness_gps_combined['current_week'].fillna(np.Infinity)
        fitness_gps_combined['last_week'] = fitness_gps_combined['last_week'].fillna(np.Infinity)
        fitness_gps_ball_data_sql = "SELECT record_date, player_name FROM {}.{} where team_name='{}' and season={} ALLOW FILTERING".format(
            DB_NAME,
            GPS_DELIVERY_TABLE_NAME,
            team_name,
            season
        )
        fitness_gps_ball_data = getPandasFactoryDF(session, fitness_gps_ball_data_sql)
        fitness_gps_ball_data['record_date'] = fitness_gps_ball_data['record_date'].apply(
            lambda x: pd.to_datetime(x, format="%A, %b %d, %Y"))
        fitness_gps_ball_data = fitness_gps_ball_data[
            (fitness_gps_ball_data['record_date'] >= seven_days_ago) & (
                    fitness_gps_ball_data['record_date'] <= today)
            ]
        players = fitness_gps_ball_data.drop_duplicates('player_name')['player_name'].tolist()
        players += player_load_combined.drop_duplicates('player_name')['player_name'].tolist()
        players += fitness_gps_combined.drop_duplicates('player_name')['player_name'].tolist()
        unique_players = set(players)
        players_delivery_mapping = {}
        for players in unique_players:
            players_delivery_mapping[players] = len(
                (fitness_gps_ball_data.loc[fitness_gps_ball_data['player_name'] == players]).index
            )
        player_load_combined_change = player_load_combined.to_dict(orient='record')
        fitness_gps_combined_change = fitness_gps_combined.to_dict(orient='record')
        for items in player_load_combined_change:
            change_indicator = 0
            if items['last_week'] == np.Infinity:
                items['message'] = "Previous Week data is not there"
                items['percent_change'] = np.Infinity
                items['change_indicator'] = np.NAN
                continue
            if items['current_week'] == np.Infinity:
                items['message'] = "Current Week data is not there"
                items['percent_change'] = np.Infinity
                items['change_indicator'] = np.NAN
                continue
            if items['last_week'] == '0':
                items['message'] = "Previous Week data is 0"
                items['percent_change'] = np.Infinity
                items['change_indicator'] = np.NAN
                continue
            change = (((float(items['current_week']) - float(items['last_week'])) / float(items['last_week'])) * 100)
            if change < 20:
                flag = "Decrease by more than or equals to 20%"
                change_indicator = -1
            elif change > 15:
                flag = "Increase in more than or equals to 15%"
            else:
                flag = "Value in between 19% Decrease to 14% increase"
                change_indicator = 1
            items['message'] = flag
            items['change_indicator'] = change_indicator
            items['percent_change'] = change
        for items in fitness_gps_combined_change:
            change_indicator = 0
            items['percent_change'] = np.Infinity
            items['change_indicator'] = np.NAN
            if items['last_week'] == np.Infinity:
                items['message'] = "Previous Week data is not there"
                items['percent_change'] = np.Infinity
                items['change_indicator'] = np.NAN
                continue
            if items['current_week'] == np.Infinity:
                items['message'] = "Current Week data is not there"
                items['percent_change'] = np.Infinity
                items['change_indicator'] = np.NAN
                continue
            if items['last_week'] == '0':
                items['message'] = "Previous Week data is 0"
                items['change_indicator'] = np.NAN
                continue
            change = (((float(items['current_week']) - float(items['last_week'])) / float(
                items['last_week'])
                       ) * 100)

            if change < 20:
                flag = "Decrease by more than or equals to 20%"
                change_indicator = -1
            elif change > 15:
                flag = "Increase in more than or equals to 15%"
            else:
                flag = "Value in between 19% Decrease to 14% increase"
                change_indicator = 1
            items['message'] = flag
            items['percent_change'] = change
            items['change_indicator'] = change_indicator
        for gps_data in fitness_gps_combined_change:
            gps_data['total_deliveries'] = players_delivery_mapping[gps_data['player_name']]
            gps_data['player_name'] = gps_data['player_name'].lower()

        for data in player_load_combined_change:
            data['player_name'] = data['player_name'].lower()
        player_load_dict = {}
        total_player_load_dict = {}
        for iter in player_load_combined_change:
            player_load_dict[iter['player_name']] = {
                "total_load_percentage_change": iter['percent_change'],
                "total_load_weekly": iter['current_week']
            }
        for iter in fitness_gps_combined_change:
            total_player_load_dict[iter["player_name"]] = {
                "total_player_load_percentage_change": iter['percent_change'],
                "total_player_load": iter['current_week'],
                "total_deliveries": iter['total_deliveries']
            }
        response = []
        player_list = player_list or sorted(set(list(player_load_dict.keys()) + list(total_player_load_dict.keys())))
        for player in player_list:
            player_load = player_load_dict.get(player)
            total_player_load = total_player_load_dict.get(player)
            total_load_weekly = (
                round(float(player_load["total_load_weekly"]))
                if player_load["total_load_weekly"] != np.Infinity
                else "NA"
            ) if player_load else "NA"
            total_load_percentage_change = (
                round(float(player_load["total_load_percentage_change"]))
                if player_load["total_load_percentage_change"] != np.Infinity
                else "NA"
            ) if player_load else "NA"
            total_player_load_value = (
                round(float(total_player_load["total_player_load"]))
                if total_player_load["total_player_load"] != np.Infinity
                else "NA"
            ) if total_player_load else "NA"
            total_player_load_percentage_change = (
                round(float(total_player_load["total_player_load_percentage_change"]))
                if total_player_load["total_player_load_percentage_change"] != np.Infinity
                else "NA"
            ) if total_player_load else "NA"
            response.append({
                "name": player.title(),
                "total_load_weekly": total_load_weekly,
                "total_load_percentage_change": total_load_percentage_change,
                "total_player_load": total_player_load_value,
                "total_player_load_percentage_change": total_player_load_percentage_change,
                "total_deliveries": total_player_load["total_deliveries"] if total_player_load else "NA"
            })
        return jsonify(response), 200, logger.info("Status - 200")
    except Exception as e:
        raise HTTPException(response=Response(f"Internal Server error --> {str(e)}", 500))


@token_required
def gps_report_notifier():
    logger = get_logger("gps_report_notifier", "gps_report_notifier")
    try:
        recipients = request.json.get('recipients')
        recipients = [recipient.lower() for recipient in recipients]
        token_roles = get_roles_token(request.headers['token'], request.headers['token_roles'])
        report_name = request.json.get('report_name')
        date_filter = request.json.get('date_filter')
        user_name = request.json.get('user_name')
        is_text = request.json.get('is_text')
        is_mail = request.json.get('is_mail')
        is_whatsapp = request.json.get('is_whatsapp')
        active_tab = request.json.get('active_tab', 0)

        # Create payload for mail, whatsapp and text message to recipient.
        notification = Notification(
            is_text=is_text,
            is_mail=is_mail,
            is_whatsapp=is_whatsapp,
            text_template_id=None,
            mail_template_id=None
        )
        payloads = {}
        if is_whatsapp:
            payloads["whatsapp_payload"] = Payload(token_roles).whatsapp_bulk_payload({
                'recipient': recipients,
                'report_name': report_name,
                'campaign_template': GPS_CAMPAIGN_TEMPLATE,
                'date_filter': date_filter,
                'active_tab': active_tab,
                'user_name': user_name
            })
        if is_text:
            payloads["text_payload"] = None
        if is_mail:
            payloads["mail_payload"] = None

        # Send notification to players
        notification.send_bulk_notification(payloads)

        return {
            "message": "Successfully sent notification",
            "recipients": recipients
        }
    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def gps_report_scheduler():
    logger = get_logger("gps_report_scheduler", "gps_report_scheduler")
    try:
        token_roles = get_roles_token(request.headers['token'], request.headers['token_roles'])
        team_name = request.json.get('team_name')
        recipients = request.json.get('recipients')
        report_name = request.json.get('report_name')
        is_text = request.json.get('is_text')
        is_mail = request.json.get('is_mail')
        is_whatsapp = request.json.get('is_whatsapp')
        user_name = request.json.get('user_name')
        recipients = [recipient.lower() for recipient in recipients]
        schedule_hour = request.json.get('schedule_hour')
        schedule_minute = request.json.get('schedule_minute')
        start_date = datetime.strptime(request.json.get('start_date'), "%Y-%m-%d")
        end_date = datetime.strptime(request.json.get('end_date'), "%Y-%m-%d")
        start_date_time = datetime(
            start_date.year, start_date.month, start_date.day, schedule_hour, schedule_minute, 0,
            0, tz.gettz('Asia/Kolkata')
        )
        end_date_time = datetime(
            end_date.year, end_date.month, end_date.day, schedule_hour, schedule_minute, 0,
            0, tz.gettz('Asia/Kolkata')
        )
        start_date_time_utc = start_date_time.astimezone(timezone.utc)
        start_date = datetime.strptime(start_date_time_utc.strftime("%Y-%m-%d"), "%Y-%m-%d")
        # End date parse to UTC
        end_date_time_utc = end_date_time.astimezone(timezone.utc)
        end_date = datetime.strptime(end_date_time_utc.strftime("%Y-%m-%d"), "%Y-%m-%d")
        schedule_hour = start_date_time_utc.hour
        schedule_minute = start_date_time_utc.minute
        # Call notification Service to send mail
        logger.warning(f"{schedule_hour}, {schedule_minute}, {start_date}, {end_date}")
        gps_periodic_scheduler(
            start_date,
            end_date,
            schedule_hour,
            schedule_minute,
            team_name,
            recipients,
            report_name,
            is_whatsapp,
            is_text,
            is_mail,
            user_name,
            token_roles
        )
        logger.info("Cron notification scheduler Setup is done via API invocation.")
        return {
            "message": f"Successfully Scheduled {report_name}."
        }
    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def recipients():
    logger = get_logger("recipients", "recipients")
    try:
        token = request.headers['token']
        token_roles = request.headers['token_roles']
        knight_watch = KnightWatch()
        kw_recipients = knight_watch.players_coaches_info_knightwatch(get_roles_token(token, token_roles))
        response = {
            "admins": [],
            "coaches": [],
            "analysts": []
        }
        for recipient in kw_recipients['results']:
            contact_info = []
            for contacts in recipient["contacts"]:
                contact = contacts.get('contact')
                is_email = re.fullmatch(EMAIL_REGEX, contact)
                if is_email:
                    contact_info.append({
                        "email": contact
                    })
                if not is_email:
                    contact_info.append({
                        "phone": contact
                    })
            document = recipient['documents']
            profile_image = ""
            if document.get('image'):
                profile_image = document.get('image')[-1].get('docAccessUrl')
            recipient_meta_data = {
                "name": recipient['metadata'].get('db_name', recipient['metadata'].get('name')),
                "contacts": contact_info,
                "profile_image": profile_image
            }
            if 'admin' in recipient['roles'][0].split(":")[1]:
                response["admins"].append(recipient_meta_data)
            if 'coach' in recipient['roles'][0].split(":")[1]:
                response["coaches"].append(recipient_meta_data)
            if 'analyst' in recipient['roles'][0].split(":")[1]:
                response["analysts"].append(recipient_meta_data)

        return response, 200, logger.info("Status - 200")
    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def putFieldAnalysis():
    logger = get_logger("putFieldAnalysis", "putFieldAnalysis")
    try:
        req_filters = request.json
        for req in req_filters:
            validateRequest(req)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        response = ''
        json_data = request.json
        field_df = pd.DataFrame(json_data)
        # Create a dictionary mapping player_name to id from df2
        player_id_mapping = dict(zip(match_playing_xi_df['player_name'], match_playing_xi_df['src_player_id']))

        if any("encode_xl" in d for d in json_data):
            try:
                encode_xl_df = field_df['encode_xl']
                match_name = field_df['match_name'].values[0]
                encode_xl = encode_xl_df.values[0]
                fielder_df, wk_df = decodeAndUploadXL(encode_xl)
                new_field_df = parseXl(fielder_df, wk_df, match_name, player_id_mapping)
                new_field_df['load_timestamp'] = load_timestamp
                update_field_df = pd.DataFrame(
                    {'match_name': pd.Series(dtype='str'), 'player_name': pd.Series(dtype='str'),
                     'team_name': pd.Series(dtype='str'), 'season': pd.Series(dtype='str'),
                     'player_type': pd.Series(dtype='str'), 'player_id': pd.Series(dtype='int'),
                     'match_id': pd.Series(dtype='int'), 'clean_takes': pd.Series(dtype='int'),
                     'miss_fields': pd.Series(dtype='int'),
                     'miss_fields_cost': pd.Series(dtype='int'),
                     'dives_made': pd.Series(dtype='int'), 'runs_saved': pd.Series(dtype='int'),
                     'dives_missed': pd.Series(dtype='int'),
                     'missed_runs': pd.Series(dtype='int'),
                     'good_attempt': pd.Series(dtype='int'), 'taken': pd.Series(dtype='int'),
                     'stumping': pd.Series(dtype='int'),
                     'dropped_percent_difficulty': pd.Series(dtype='int'),
                     'caught_and_bowled': pd.Series(dtype='int'),
                     'good_return': pd.Series(dtype='int'),
                     'poor_return': pd.Series(dtype='int'), 'direct_hit': pd.Series(dtype='int'),
                     'missed_shy': pd.Series(dtype='int'),
                     'run_out_obtained': pd.Series(dtype='int'),
                     'pop_ups': pd.Series(dtype='int'), 'support_run': pd.Series(dtype='int'),
                     'back_up': pd.Series(dtype='int'),
                     'standing_back_plus': pd.Series(dtype='int'),
                     'standing_back_minus': pd.Series(dtype='int'),
                     'standing_up_plus': pd.Series(dtype='int'),
                     'standing_up_minus': pd.Series(dtype='int'),
                     'returns_taken_plus': pd.Series(dtype='int'),
                     'returns_untidy': pd.Series(dtype='int')})

                new_field_df[
                    ['clean_takes', 'miss_fields', 'miss_fields_cost', 'dives_made', 'runs_saved', 'dives_missed',
                     'missed_runs', 'good_attempt', 'taken', 'stumping', 'dropped_percent_difficulty',
                     'caught_and_bowled', 'good_return', 'poor_return', 'direct_hit', 'missed_shy',
                     'run_out_obtained', 'pop_ups', 'support_run', 'back_up', 'standing_back_plus',
                     'standing_back_minus', 'standing_up_plus', 'standing_up_minus', 'returns_taken_plus',
                     'returns_untidy', 'season', 'match_id', 'player_id']] = \
                    new_field_df[
                        ['clean_takes', 'miss_fields', 'miss_fields_cost', 'dives_made', 'runs_saved', 'dives_missed',
                         'missed_runs', 'good_attempt', 'taken', 'stumping', 'dropped_percent_difficulty',
                         'caught_and_bowled', 'good_return', 'poor_return', 'direct_hit', 'missed_shy',
                         'run_out_obtained', 'pop_ups', 'support_run', 'back_up', 'standing_back_plus',
                         'standing_back_minus',
                         'standing_up_plus', 'standing_up_minus', 'returns_taken_plus', 'returns_untidy', 'season',
                         'match_id', 'player_id']] \
                        .astype(int)
            except Exception as e:
                logger.info(e)
                sys.exit(f"{e}")
        elif any("id" in d for d in json_data):
            update_field_df = field_df[~field_df['id'].isnull()]
            # Assign ids to players in df1 based on the mapping
            update_field_df['player_id'] = update_field_df['player_name'].map(player_id_mapping).astype(int)
            update_field_df['load_timestamp'] = load_timestamp
            update_field_df = update_field_df.to_dict(orient='records')
            new_field_df = field_df[field_df['id'].isnull()].drop('id', axis=1, errors='ignore')
            match_name = [d.get('match_name', None) for d in update_field_df][0]
            f_match_name = matchNameConv(match_name)
            match_id = matches_join_data[matches_join_data['match_name'] == f_match_name]['match_id'].values[0]
            update_field_df = [dict(item, match_id=match_id) for item in update_field_df]
        else:
            update_field_df = pd.DataFrame({'match_name': pd.Series(dtype='str'), 'player_name': pd.Series(dtype='str'),
                                            'team_name': pd.Series(dtype='str'), 'season': pd.Series(dtype='str'),
                                            'player_type': pd.Series(dtype='str'), 'player_id': pd.Series(dtype='int'),
                                            'match_id': pd.Series(dtype='int'), 'clean_takes': pd.Series(dtype='int'),
                                            'miss_fields': pd.Series(dtype='int'),
                                            'miss_fields_cost': pd.Series(dtype='int'),
                                            'dives_made': pd.Series(dtype='int'),
                                            'runs_saved': pd.Series(dtype='int'),
                                            'dives_missed': pd.Series(dtype='int'),
                                            'missed_runs': pd.Series(dtype='int'),
                                            'good_attempt': pd.Series(dtype='int'),
                                            'taken': pd.Series(dtype='int'), 'stumping': pd.Series(dtype='int'),
                                            'dropped_percent_difficulty': pd.Series(dtype='int'),
                                            'caught_and_bowled': pd.Series(dtype='int'),
                                            'good_return': pd.Series(dtype='int'),
                                            'poor_return': pd.Series(dtype='int'), 'direct_hit': pd.Series(dtype='int'),
                                            'missed_shy': pd.Series(dtype='int'),
                                            'run_out_obtained': pd.Series(dtype='int'),
                                            'pop_ups': pd.Series(dtype='int'), 'support_run': pd.Series(dtype='int'),
                                            'back_up': pd.Series(dtype='int'),
                                            'standing_back_plus': pd.Series(dtype='int'),
                                            'standing_back_minus': pd.Series(dtype='int'),
                                            'standing_up_plus': pd.Series(dtype='int'),
                                            'standing_up_minus': pd.Series(dtype='int'),
                                            'returns_taken_plus': pd.Series(dtype='int'),
                                            'returns_untidy': pd.Series(dtype='int')})
            new_field_df = field_df
            match_name = new_field_df['match_name'].values[0]
            f_match_name = matchNameConv(match_name)
            match_id = matches_join_data[
                matches_join_data['match_name'] == f_match_name]['match_id'].values[0]
            new_field_df['match_id'] = match_id
            # Assign ids to players in df1 based on the mapping
            new_field_df['player_id'] = new_field_df['player_name'].map(player_id_mapping)
            new_field_df['load_timestamp'] = load_timestamp

        max_key_val = getMaxId(session, FIELDING_ANALYSIS_TABLE_NAME, FIELDING_ANALYSIS_KEY_COL, DB_NAME)
        new_field_data = generateSeq(new_field_df, FIELDING_ANALYSIS_KEY_COL, max_key_val)

        if len(new_field_data) > 0:
            field_analysis_df = getPandasFactoryDF(session, GET_FIELD_ANALYSIS)
            filtered_fa_df = field_analysis_df[field_analysis_df['match_name'].isin(new_field_data['match_name'])].drop(
                'id', axis=1)
            new_field_data = dupCheckBeforeInsert(filtered_fa_df, new_field_data)

            insertToDB(session, new_field_data.to_dict(orient='records'), DB_NAME, FIELDING_ANALYSIS_TABLE_NAME)
            response = "Data Added Successfully!"
            logger.info("Field Analysis Data Inserted Successfully!")
        else:
            logger.info("No New Data!")

        # CHECK FOR UPDATES
        if len(update_field_df) > 0:
            for obj in update_field_df:
                updateFieldAnalysisData(obj, FIELDING_ANALYSIS_TABLE_NAME)
                logger.info("Field Analysis Data Updated Successfully!")
            response = "Data Updated Successfully!"
        else:
            logger.info("No New Updates!")

        return jsonify(response), 200, logger.info("Status - 200")
    except SystemExit as s:
        logger.info(s)
        raise HTTPException(response=Response(f"File Error: "
                                              f"Please upload a proper Excel File after downloading the template from website for a specific match.",
                                              500))
    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def getFieldAnalysis():
    logger = get_logger("getFieldAnalysis", "getFieldAnalysis")
    try:
        req_filters = request.json
        validateRequest(req_filters)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        response = {}
        match_name = 'All Matches'
        field_analysis_df = getPandasFactoryDF(session, GET_FIELD_ANALYSIS)
        fielders_list = match_playing_xi_df[
            match_playing_xi_df['player_skill'] != 'WICKETKEEPER'][
            ['player_name', 'match_id', 'team1', 'player_image_url']]
        wk_list = match_playing_xi_df[
            match_playing_xi_df['player_skill'] == 'WICKETKEEPER'][
            ['player_name', 'match_id', 'team1', 'player_image_url']]

        if "player_name" in request.json:
            player_name = request.json.get('player_name')
            field_analysis_df = field_analysis_df[field_analysis_df['player_name'].isin(player_name)]

        if "player_id" in request.json:
            player_id = request.json.get('player_id')
            field_analysis_df = field_analysis_df[field_analysis_df['player_id'].isin(player_id)]

        if "team_name" in request.json:
            team_name = request.json.get('team_name')
            team_name_f = team_name.strip().replace(" ", "").lower()
            field_analysis_df = field_analysis_df[
                (field_analysis_df['team_name'].apply(lambda x: x.strip().replace(" ", "").lower()) == team_name_f)]
            fielders_list = fielders_list[
                (fielders_list['team1'].apply(lambda x: x.strip().replace(" ", "").lower()) == team_name_f)]
            wk_list = wk_list[(wk_list['team1'].apply(lambda x: x.strip().replace(" ", "").lower()) == team_name_f)]

        if "season" in request.json:
            season = request.json.get('season')
            field_analysis_df = field_analysis_df[field_analysis_df['season'] == season]

        if "match_name" in request.json:
            match_name = request.json.get('match_name')
            if match_name != 'All Matches':
                field_analysis_df = field_analysis_df[field_analysis_df['match_name'] == match_name]
                # Get match_id for specific match
                f_match_name = matchNameConv(match_name)
                match_id = matches_join_data[
                    matches_join_data['match_name'] == f_match_name]['match_id'].values[0]
                fielders_list = fielders_list[fielders_list['match_id'] == match_id]
                wk_list = wk_list[wk_list['match_id'] == match_id]

        folder_name = 'mi-teams-images/'
        field_analysis_df['player_image_url'] = IMAGE_STORE_URL + folder_name + field_analysis_df['team_name'].str.replace(
            "MI CAPE TOWN", "MI Capetown", case=False).apply(
            lambda x: x.replace(' ', '-')
            .lower()).astype(str) + '/' + field_analysis_df['player_name'].apply(lambda x: x.replace(' ', '-')
                                                                                 .lower()).astype(str) + ".png"
        defaulting_image_url(field_analysis_df, 'player_image_url', 'team_name', 'Mumbai Indian Womens', folder_name)

        fielder_df = field_analysis_df[field_analysis_df['player_type'] == "Fielder"].drop(
            columns=['match_name', 'player_type', 'team_name', 'season'], axis=1)
        wicketkeeper_df = field_analysis_df[field_analysis_df['player_type'] == "Wicketkeeper"].drop(
            columns=['match_name', 'player_type', 'team_name', 'season'], axis=1)

        if match_name == 'All Matches':
            if not fielder_df.empty:
                fielder_df = fielder_df.drop('id', axis=1).groupby(['player_name', 'player_image_url']) \
                    .apply(lambda x: x.iloc[:, 0:].apply(sum_ignore_negative)).reset_index()
            fielder_df['data'] = fielder_df.drop('player_name', axis=1).to_dict(orient='records')
            response['fielder_data'] = fielder_df[['player_name', 'data']].set_index('player_name')[
                ['data']].to_dict(orient='index')

            if not wicketkeeper_df.empty:
                wicketkeeper_df = wicketkeeper_df.drop('id', axis=1).groupby(['player_name', 'player_image_url']) \
                    .apply(lambda x: x.iloc[:, 1:].apply(sum_ignore_negative)).reset_index()
            wicketkeeper_df['data'] = wicketkeeper_df.drop('player_name', axis=1).to_dict(orient='records')
            response['wicketkeeper_data'] = wicketkeeper_df[['player_name', 'data']].set_index('player_name')[
                ['data']].to_dict(orient='index')
        else:
            if 'player_name' or 'player_id' in locals():
                fielder_df['data'] = fielder_df.drop('player_name', axis=1).to_dict(orient='records')
                response['fielder_data'] = fielder_df.set_index('player_name')[['data']].to_dict(orient='index')

                wicketkeeper_df['data'] = wicketkeeper_df.drop('player_name', axis=1).to_dict(orient='records')
                response['wicketkeeper_data'] = wicketkeeper_df.set_index('player_name')[['data']].to_dict(
                    orient='index')
            else:
                new_values_f = fielders_list[~fielders_list['player_name'].isin(fielder_df['player_name'])][[
                    'player_name', 'player_image_url']]
                fielder_df = pd.concat([fielder_df, new_values_f], ignore_index=True).fillna(
                    -1)  # Append the missing values to 'player_name'
                fielder_df['data'] = fielder_df.drop('player_name', axis=1).to_dict(orient='records')
                response['fielder_data'] = fielder_df.set_index('player_name')[['data']].to_dict(orient='index')

                new_values_wk = wk_list[~wk_list['player_name'].isin(wicketkeeper_df['player_name'])][
                    ['player_name', 'player_image_url']]
                wicketkeeper_df = pd.concat([wicketkeeper_df, new_values_wk], ignore_index=True).fillna(
                    -1)  # Append the missing values to 'player_name'
                wicketkeeper_df['data'] = wicketkeeper_df.drop('player_name', axis=1).to_dict(orient='records')
                response['wicketkeeper_data'] = wicketkeeper_df.set_index('player_name')[['data']].to_dict(
                    orient='index')

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def sendSampleUploadFile():
    logger = get_logger("sendSampleUploadFile", "sendSampleUploadFile")
    try:
        req_filters = request.json
        validateRequest(req_filters)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        fielder_list_df = match_playing_xi_df[
            match_playing_xi_df['player_skill'] != 'WICKETKEEPER']
        wk_list_df = match_playing_xi_df[
            match_playing_xi_df['player_skill'] == 'WICKETKEEPER']

        if "team_name" in request.json:
            team_name = request.json['team_name']
            team_name_f = team_name.strip().replace(" ", "").lower()
            fielder_list_df = fielder_list_df[
                fielder_list_df['team1'].apply(lambda x: x.strip().replace(" ", "").lower()) == team_name_f]
            wk_list_df = wk_list_df[
                wk_list_df['team1'].apply(lambda x: x.strip().replace(" ", "").lower()) == team_name_f]

        if "season" in request.json:
            season = request.json['season']
            fielder_list_df = fielder_list_df[fielder_list_df['season'] == season]
            wk_list_df = wk_list_df[wk_list_df['season'] == season]

        if "match_name" in request.json:
            match_name = request.json['match_name']
            f_match_name = matchNameConv(match_name)
            match_id = matches_join_data[
                matches_join_data['match_name'] == f_match_name]['match_id'].values[0]
            fielder_list_df = fielder_list_df[fielder_list_df['match_id'] == match_id]
            wk_list_df = wk_list_df[wk_list_df['match_id'] == match_id]
        else:
            match_name = 'NA'

        fielder_template_df = pd.read_excel(FIELDING_ANALYSIS_TEMP, sheet_name='Fielder Data',
                                            header=[0, 1], nrows=0,
                                            index_col=None)

        wk_template_df = pd.read_excel(FIELDING_ANALYSIS_TEMP, sheet_name='Wicketkeeper Data',
                                       header=[0, 1], nrows=0,
                                       index_col=None)

        fielder_template_df[('GENERAL DETAILS', 'PLAYER NAME')] = fielder_list_df['player_name']
        fielder_template_df[('GENERAL DETAILS', 'TEAM NAME')] = fielder_list_df['team1'].str.replace("MI CAPE TOWN",
                                                                                                     "MI Capetown",
                                                                                                     case=False)
        fielder_template_df[('GENERAL DETAILS', 'SEASON')] = fielder_list_df['season']
        fielder_template_df[('GENERAL DETAILS', 'MATCH NAME')] = match_name
        wk_template_df[('GENERAL DETAILS', 'PLAYER NAME')] = wk_list_df['player_name']
        wk_template_df[('GENERAL DETAILS', 'TEAM NAME')] = wk_list_df['team1'].str.replace("MI CAPE TOWN",
                                                                                           "MI Capetown", case=False)
        wk_template_df[('GENERAL DETAILS', 'SEASON')] = wk_list_df['season']
        wk_template_df[('GENERAL DETAILS', 'MATCH NAME')] = match_name

        fileWritten = writeXlData(fielder_template_df, wk_template_df, match_name)

        if fileWritten:
            response = encodeAndDeleteXl(FIELD_ANALYSIS_FILE)
        else:
            response = jsonify("File General Details not Written Successfully")

        return response, 200, logger.info("Status - 200")
    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def showMatchList():
    logger = get_logger("showMatchList", "showMatchList")
    try:
        req_filters = request.json
        validateRequest(req_filters)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise BadRequest(response=Response(f"Bad Request", 400))
    try:
        match_list_df = matches_join_data

        if "season" in request.json:
            season = request.json.get('season')
            match_list_df = match_list_df[match_list_df['season'] == season]

        if "team_name" in request.json:
            team_name = request.json.get('team_name')
            team_name = team_name.strip().replace(" ", "").lower()
            team_name = 'mumbaiindianswomen' if team_name == 'mumbaiindianwomens' else team_name
            match_list_df = match_list_df[
                (match_list_df['team1_name'].apply(lambda x: x.strip().replace(" ", "").lower()) == team_name) |
                (match_list_df['team2_name'].apply(lambda x: x.strip().replace(" ", "").lower()) == team_name)]

        match_list_df['match_date_formatted'] = pd.to_datetime(match_list_df.match_date).dt.strftime('%d-%B-%Y')
        match_list_df['match_date'] = pd.to_datetime(match_list_df.match_date)
        match_list_df[['team1', 'team2']] = match_list_df['match_name'].str.extract(r'(.*?)VS(.*?)(?=\d)')
        match_list_df['match_name_formatted'] = match_list_df['team1'] + ' VS ' + match_list_df[
            'team2'] + ' ' + match_list_df['match_date_formatted']

        if len(match_list_df[['match_name_formatted']]) > 0:
            match_list_df = match_list_df.append({'match_name_formatted': 'All Matches'}, ignore_index=True)
        match_list_df = match_list_df.drop(columns=['match_name'], axis=1).rename(
            columns={'match_name_formatted': 'match_name'})

        response = match_list_df.sort_values(by='match_date')[['match_name']].to_json(orient='records')

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def create_calendar_event():
    logger = get_logger("create_calendar_event", "create_calendar_event")
    try:
        logger.info("create Match Schedule API invoked")
        json_data = request.json
        calendar_data_df = pd.DataFrame(json_data)
        response = []
        token = request.headers['token']
        token_roles = request.headers['token_roles']
        roles = get_roles_token(token, token_roles)
        calendar_data_df['load_timestamp'] = load_timestamp
        calendar_data_df['reminder_processed'] = False
        calendar_data_df['token_roles'] = [roles] * len(calendar_data_df)

        if any("id" in d for d in json_data):
            update_calendar_data_df = calendar_data_df[~calendar_data_df['id'].isnull()].to_dict(orient='records')
            new_calendar_data_df = calendar_data_df[calendar_data_df['id'].isnull()].drop('id', axis=1, errors='ignore')
        else:
            update_calendar_data_df = pd.DataFrame(
                {
                    'recipient_name': pd.Series([], dtype='str'),
                    'recipient_uuid': pd.Series([], dtype='str'),
                    'event_type': pd.Series(dtype='str'),
                    'event_title': pd.Series(dtype='str'),
                    'description': pd.Series(dtype='str'),
                    'event_dates': pd.to_datetime([], format='%d/%m/%Y'),
                    'start_time': pd.to_datetime([], format='%H:%M:%S'),
                    'end_time': pd.to_datetime([], format='%H:%M:%S'),
                    'all_day': pd.Series(dtype='bool'),
                    'reminder_time': pd.Series(dtype='str'),
                    'reminder_processed': pd.Series(dtype='bool'),
                    'src_player_id': pd.Series([], dtype='str'),
                    'token_roles': pd.Series([], dtype='str'),
                    'venue': pd.Series(dtype='str')
                }
            )
            new_calendar_data_df = calendar_data_df

        max_key_val = getMaxId(session, CALENDAR_EVENT_TABLE_NAME, CALENDAR_EVENT_KEY_COL, DB_NAME)
        new_calendar_data = generateSeq(new_calendar_data_df, CALENDAR_EVENT_KEY_COL, max_key_val)

        if len(new_calendar_data) > 0:
            insertToDB(session, new_calendar_data.to_dict(orient='records'), DB_NAME, CALENDAR_EVENT_TABLE_NAME)
            logger.info("Data Inserted Successfully!")
            response = jsonify("Data Added Successfully!")
            email_sender = CalendarCreateUpdateBGTask(pks=new_calendar_data['id'].to_list(), is_new_event=True)
            email_sender.start()
        else:
            logger.info("No New Data!")

        # CHECK FOR UPDATES
        if len(update_calendar_data_df) > 0:
            for obj in update_calendar_data_df:
                change_id = obj['id']
                # Taking event_creator_uuid of the specific id
                event_creator_uuid = getPandasFactoryDF(
                    session,
                    f"select event_creator_uuid from {CALENDAR_EVENT_TABLE_NAME} where id = {change_id};"
                ).iloc[0, 0]

                for d in json_data:
                    if event_creator_uuid == d['event_creator_uuid']:
                        updateCalendarEventData(obj, CALENDAR_EVENT_TABLE_NAME)
                        email_sender = CalendarCreateUpdateBGTask(pks=[change_id], is_new_event=False)
                        email_sender.start()
                        logger.info("Data Updated Successfully!")
                        response = jsonify("Data Updated Successfully!")
                    else:
                        logger.info("user can't edit this event")
                        response = jsonify("user can't edit this event")
        else:
            logger.info("No New Updates!")
        return response, 200, logger.info("Status - 200")
    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def fetch_calendar_events():
    logger = get_logger("fetch_calendar_events", "fetch_calendar_events")
    try:
        if "year" in request.json:
            year = request.json['year']

        if "month" in request.json:
            month = request.json['month']

        if "day" in request.json:
            day = request.json['day']

        if "uuid" in request.json:
            uuid = request.json['uuid']
            calendar_events = getPandasFactoryDF(
                session,
                f"select * from {CALENDAR_EVENT_TABLE_NAME};"
            )
            calendar_events = calendar_events[calendar_events['recipient_uuid'].apply(lambda x: uuid in x) | (
                    calendar_events['event_creator_uuid'] == uuid)]

            # Assign 'edit_flag' as False for all records
            calendar_events['editable_flag'] = False
            # Assign 'edit_flag' as True for records where 'event_creator_uuid' matches the given UUID
            calendar_events.loc[calendar_events['event_creator_uuid'] == uuid, 'editable_flag'] = True
        else:
            calendar_events = getPandasFactoryDF(
                session,
                f"select * from {CALENDAR_EVENT_TABLE_NAME};"
            )
            calendar_events['editable_flag'] = False

        output_dict = {}

        # Remove trailing zeros after decimal point
        calendar_events['start_time'] = calendar_events['start_time'].astype(str).apply(
            lambda x: x[:-10] if x.endswith('.000000000') else x)
        calendar_events['end_time'] = calendar_events['end_time'].astype(str).apply(
            lambda x: x[:-10] if x.endswith('.000000000') else x)
        calendar_events['reminder_time'] = calendar_events['reminder_time'].astype(str).apply(
            lambda x: x[:-10] if x.endswith('.000000000') else x)

        month = locals().get('month', [])
        day = locals().get('day', [])

        # Function to check if a date matches the specified filters
        def date_matches_filters(date):
            # date_datetime = datetime.fromtimestamp(date.days_since_epoch / 1000.0)
            year_match = date.date().year in year
            month_match = not month or date.date().month in month
            day_match = not day or date.date().day in day
            return year_match and month_match and day_match

        # Apply the filter condition
        filter_conditions = calendar_events['event_dates'].apply(
            lambda dates: any(date_matches_filters(date) for date in
                              (dates if isinstance(dates, list) else [dates])) if dates else False
        )
        # Filter the DataFrame
        calendar_events = calendar_events[filter_conditions]

        for index, row in calendar_events.iterrows():
            formatted_date = row['event_dates'].date().strftime("%d-%m-%Y")

            # Create dictionary for each event date
            event_dict = {
                "id": row['id'],
                "event_type": row['event_type'],
                "event_title": row['event_title'],
                "venue": row['venue'],
                "description": row['description'],
                "start_time": str(row['start_time']),
                "end_time": str(row['end_time']),
                "reminder": row['reminder_time'],
                "players": row['src_player_id'] if row['src_player_id'] is not None else [],
                "user_uuid": row['recipient_uuid'] if row['recipient_uuid'] is not None else [],
                "user_name": row['recipient_name'] if row['recipient_name'] is not None else [],
                "event_creator_uuid": row['event_creator_uuid'],
                "editable_flag": row['editable_flag']
            }

            # Add the dictionary to the main output dictionary for each date
            if formatted_date not in output_dict:
                output_dict[formatted_date] = []
            output_dict[formatted_date].append(event_dict)

        return output_dict, 200, logger.info("Status - 200")
    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def delete_calendar_events():
    logger = get_logger("delete_calendar_events", "delete_calendar_events")
    try:
        id = request.json.get('id')
        # Taking event_creator_uuid of the specific id
        event_creator_uuid = getPandasFactoryDF(
            session,
            f"select event_creator_uuid from {CALENDAR_EVENT_TABLE_NAME} where id = {id};"
        ).iloc[0, 0]

        if event_creator_uuid == request.json.get('uuid'):
            calendar_events_delete_sql = f"delete from {DB_NAME}.{CALENDAR_EVENT_TABLE_NAME} where id={id}"
            calendar_events = getPandasFactoryDF(
                session,
                f"select recipient_uuid, event_dates, start_time, end_time, event_title, event_type, venue, token_roles from {CALENDAR_EVENT_TABLE_NAME} where id = {id};"
            )
            key_value_pairs = zip(calendar_events.columns, calendar_events.values[0])
            session.execute(calendar_events_delete_sql)
            response = jsonify(f"Event Deleted Successfully!")
            calendar_del_bg_task = CalendarDeleteBGTask(**dict(key_value_pairs))
            calendar_del_bg_task.start()
        else:
            logger.info("user can't delete this event")
            response = jsonify(f"user can't delete this event")
        return response, 200, logger.info("Status - 200")
    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def create_recipient_group():
    logger = get_logger("create_recipient_group", "create_recipient_group")
    try:
        name = request.json.get('name')
        recipients = request.json.get('recipients')
        # Prepare data to be inserted into the Scheduler table
        id = getMaxId(session, RECIPIENT_GROUP_TABLE_NAME, RECIPIENT_GROUP_COL, DB_NAME)
        scheduler_data = [{
            'id': int(id),
            'name': name,
            'recipients': recipients
        }]
        insertToDB(session, scheduler_data, DB_NAME, RECIPIENT_GROUP_TABLE_NAME)
        logger.info("Cron notification scheduler Setup is done via API invocation.")
        return {
            "message": f"Successfully Added {name} group."
        }
    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def get_recipient_group():
    logger = get_logger("get_recipient_group", "get_recipient_group")
    try:
        recipient_group = getPandasFactoryDF(
            session,
            f"select * from {RECIPIENT_GROUP_TABLE_NAME}"
        )
        return {
            "recipient-group": recipient_group.to_dict(orient='records')
        }
    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


@token_required
def eligible_recipients():
    logger = get_logger("eligible_recipients", "eligible_recipients")
    try:
        token = request.headers['token']
        token_roles = request.headers['token_roles']

        if request.args:
            team = request.args['team_name']
        else:
            if SUPER_ADMIN in token_roles:
                team = get_ums_sa_leagues(token)
            else:
                team = [item.split(':')[0] for item in token_roles]

        team_responses = {}
        ums = UMS()
        # Check if team_names is a list
        if isinstance(team, list):
            for team_name in team:
                # Get team info for each team name in the list
                team_output = ums.get_team_info(token, team_name)
                team_responses[team_name] = team_output
        else:
            # Get team info for single team name
            team_output = ums.get_team_info(token, team)
            team_responses[team] = team_output

        response = json.dumps(team_responses, indent=2)
        return response, 200, logger.info("Status - 200")
    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error", 500))


generate_api_function(open_api_spec, app_gps, '/getGPSAggData', 'post', getGPSAggData, 'getGPSAggData')
generate_api_function(open_api_spec, app_gps, '/getDistanceData', 'post', getDistanceData, 'getDistanceData')
generate_api_function(open_api_spec, app_gps, '/getMaxVelocity', 'post', getMaxVelocity, 'getMaxVelocity')
generate_api_function(open_api_spec, app_gps, '/getAccelDecel', 'post', getAccelDecel, 'getAccelDecel')
generate_api_function(open_api_spec, app_gps, '/getPlayerLoad', 'post', getPlayerLoad, 'getPlayerLoad')
generate_api_function(open_api_spec, app_gps, '/getGPSFilters', 'get', getGPSFilters, 'getGPSFilters')
generate_api_function(open_api_spec, app_gps, '/getWellnessFilters', 'get', getWellnessFilters, 'getWellnessFilters')
generate_api_function(open_api_spec, app_gps, '/fetchLatestGPSData', 'get', fetchLatestGPSData, 'fetchLatestGPSData')
generate_api_function(open_api_spec, app_gps, '/getGPSDeliveryStats', 'post', getGPSDeliveryStats,
                      'getGPSDeliveryStats')
generate_api_function(open_api_spec, app_gps, '/getPlayerReadiness', 'post', getPlayerReadiness, 'getPlayerReadiness')
generate_api_function(open_api_spec, app_gps, '/groupWellness', 'post', groupWellness, 'groupWellness')
generate_api_function(open_api_spec, app_gps, '/groupActivitySessions', 'post', groupActivitySessions,
                      'groupActivitySessions')
generate_api_function(open_api_spec, app_gps, '/bowlingGPSReport', 'post', bowlingGPSReport, 'bowlingGPSReport')
generate_api_function(open_api_spec, app_gps, '/groupWeeklyLoadReport', 'post', groupWeeklyLoadReport,
                      'groupWeeklyLoadReport')
generate_api_function(open_api_spec, app_gps, '/getIndividualTrend', 'post', getIndividualTrend, 'getIndividualTrend')
generate_api_function(open_api_spec, app_gps, '/getPlayersDailyFitness', 'post', getPlayersDailyFitness,
                      'getPlayersDailyFitness')
generate_api_function(open_api_spec, app_gps, '/getDailyWellnessAverage', 'post', getDailyWellnessAverage,
                      'getDailyWellnessAverage')
generate_api_function(open_api_spec, app_gps, '/bowlingAggSummary', 'post', bowlingAggSummary, 'bowlingAggSummary')
generate_api_function(open_api_spec, app_gps, '/bowlingActualReport', 'post', bowlingActualReport,
                      'bowlingActualReport')
generate_api_function(open_api_spec, app_gps, '/putFitnessForm', 'post', putFitnessForm, 'putFitnessForm')
generate_api_function(open_api_spec, app_gps, '/updateFitnessForm', 'post', updateFitnessForm, 'updateFitnessForm')
generate_api_function(open_api_spec, app_gps, '/getFormStats', 'post', getFormStats, 'getFormStats')
generate_api_function(open_api_spec, app_gps, '/checkInputForm', 'post', checkInputForm, 'checkInputForm')
generate_api_function(open_api_spec, app_gps, '/getPlayerFormInput', 'post', getPlayerFormInput, 'getPlayerFormInput')
generate_api_function(open_api_spec, app_gps, '/putMatchPeakLoad', 'post', putMatchPeakLoad, 'putMatchPeakLoad')
generate_api_function(open_api_spec, app_gps, '/getMatchPeakLoad', 'get', getMatchPeakLoad, 'getMatchPeakLoad')
generate_api_function(open_api_spec, app_gps, '/putPlannedBalls', 'post', putPlannedBalls, 'putPlannedBalls')
generate_api_function(open_api_spec, app_gps, '/getPlannedBalls', 'post', getPlannedBalls, 'getPlannedBalls')
generate_api_function(open_api_spec, app_gps, '/insertUserQueries', 'post', insertUserQueries, 'insertUserQueries')
generate_api_function(open_api_spec, app_gps, '/listQueries', 'post', listQueries, 'listQueries')
generate_api_function(open_api_spec, app_gps, '/getQueryDetails', 'get', getQueryDetails, 'getQueryDetails')
generate_api_function(open_api_spec, app_gps, '/updateQueryDetails', 'post', updateQueryDetails, 'updateQueryDetails')
generate_api_function(open_api_spec, app_gps, '/getQueriesCount', 'post', getQueriesCount, 'getQueriesCount')
generate_api_function(open_api_spec, app_gps, '/showMatchList', 'post', showMatchList, 'showMatchList')
generate_api_function(open_api_spec, app_gps, '/sendSampleUploadFile', 'post', sendSampleUploadFile,
                      'sendSampleUploadFile')
generate_api_function(open_api_spec, app_gps, '/getFieldAnalysis', 'post', getFieldAnalysis, 'getFieldAnalysis')
generate_api_function(open_api_spec, app_gps, '/putFieldAnalysis', 'post', putFieldAnalysis, 'putFieldAnalysis')

generate_api_function(open_api_spec, app_gps, '/recipient-group', 'get', get_recipient_group, 'recipient-group')
generate_api_function(open_api_spec, app_gps, '/fetch-calendar-event', 'post', fetch_calendar_events,
                      'fetch-calendar-event')
generate_api_function(open_api_spec, app_gps, '/create-calendar-event', 'post', create_calendar_event,
                      'create-calendar-event')
generate_api_function(open_api_spec, app_gps, '/delete-calendar-event', 'post', delete_calendar_events,
                      'delete-calendar-event')
generate_api_function(open_api_spec, app_gps, '/eligible_recipients', 'get', eligible_recipients,
                      'eligible_recipients')
generate_api_function(open_api_spec, app_gps, '/create-recipient-group', 'post', create_recipient_group,
                      'create-recipient-group')
generate_api_function(open_api_spec, app_gps, '/recipients', 'get', recipients, 'recipients')
generate_api_function(open_api_spec, app_gps, '/get-schedule', 'get', get_schedule, 'get-schedule')
generate_api_function(open_api_spec, app_gps, '/gps-report-scheduler', 'post', gps_report_scheduler,
                      'gps-report-scheduler')
generate_api_function(open_api_spec, app_gps, '/player_notifier', 'post', player_notifier, 'player_notifier')
generate_api_function(open_api_spec, app_gps, '/player-report-weekly', 'post', player_report_weekly,
                      'player-report-weekly')
generate_api_function(open_api_spec, app_gps, '/gps-notifier', 'post', gps_report_notifier, 'gps-notifier')
generate_api_function(open_api_spec, app_gps, '/player_notifier_scheduler', 'post', player_notifier_scheduler,
                      'player_notifier_scheduler')
