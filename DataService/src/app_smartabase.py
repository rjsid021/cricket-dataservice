from flask import Blueprint, request, Response, jsonify
from marshmallow import ValidationError
from DataIngestion.config import IMAGE_STORE_URL
from DataIngestion.preprocessor.smartabase import generate_smartabase_form_data
from DataIngestion.query import GET_PLAYER_MAPPER_DETAILS_SQL, GET_READINESS_MAX_SYNC_TIME, \
    GET_AVAILABILITY_MAX_SYNC_TIME
from DataIngestion.sources.smartabase.config import READINESS_TO_PERFORM_FORM_NAME, READINESS_TO_PERFORM_TEXT_COLS, \
    READINESS_TO_PERFORM_COLUMN_MAPPING, READINESS_TO_PERFORM_TABLE, MI_AVAILABILITY_FORM_NAME, \
    MI_AVAILABILITY_TEXT_COLS, MI_AVAILABILITY_COLUMN_MAPPING, MI_AVAILABILITY_TABLE
from DataIngestion.utils.helper import readJson
from DataService.app_config import SMARTABASE_PLAYER_MAPPING
from DataService.utils.helper import validateRequest, generate_dates, generate_api_function, open_api_spec
from common.authentication.auth import token_required
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao.insert_data import insertToDB
from common.dao_client import session
from common.db_config import DB_NAME
from werkzeug.exceptions import HTTPException
import pandas as pd
from log.log import get_logger
import aiohttp
import asyncio
import datetime

app_smartabase = Blueprint("app_smartabase", __name__)
open_api_spec = open_api_spec()


# async def async_url_exists(session, url):
#     try:
#         async with session.head(url) as response:
#             return response.status == 200
#     except aiohttp.ClientError:
#         return False
#
#
# async def check_url_and_replace(session, url):
#     if await async_url_exists(session, url):
#         return url
#     else:
#         return IMAGE_STORE_URL + "players_images/mi-all-teams/men-default-placeholder.png"
#
#
# async def check_urls_async(df):
#     connector = aiohttp.TCPConnector(limit=50)  # Adjust the limit based on your needs
#     async with aiohttp.ClientSession(connector=connector) as session:
#         tasks = [check_url_and_replace(session, url) for url in df['player_image_url']]
#         results = await asyncio.gather(*tasks)
#     return results


def smartabase_tournament_mapping():
    tournament_group_mapping = {"ILT20": "ILT20",
                                "WPL": "WPL Squad",
                                "SA20": "SA20",
                                "IPL": "IPL Squad",
                                "MLC": "MLC Squad",
                                "UK Tour (Mens) 2023": "UK Tour (Mens) 2023",
                                "ALL TOURNAMENTS": "All Teams"}

    return tournament_group_mapping


def get_competition_group_mapping(key=None):
    data = readJson(SMARTABASE_PLAYER_MAPPING)
    if key:
        return data[key]
    else:
        return data


def generate_start_date_clause(date_tuple):
    if len(date_tuple) == 1:
        start_date_clause = " where start_date='" + date_tuple[0] + "'"
    elif len(date_tuple) > 1:
        start_date_clause = " where start_date in " + str(date_tuple)
    else:
        start_date_clause = ""

    return start_date_clause


@token_required
def fetchLatestSmartabaseData():
    logger = get_logger("fetchLatestSmartabaseData", "fetchLatestSmartabaseData")
    try:
        load_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        group_name = "All Teams"

        readiness_data = generate_smartabase_form_data(group_name, READINESS_TO_PERFORM_FORM_NAME,
                                                       READINESS_TO_PERFORM_TEXT_COLS, load_timestamp,
                                                       READINESS_TO_PERFORM_COLUMN_MAPPING, GET_READINESS_MAX_SYNC_TIME,
                                                       READINESS_TO_PERFORM_TABLE)

        if readiness_data:
            insertToDB(session, readiness_data, DB_NAME, READINESS_TO_PERFORM_TABLE)
        else:
            logger.info("No New Data Available for Readiness To Perform!!!")

        availability_data = generate_smartabase_form_data(group_name, MI_AVAILABILITY_FORM_NAME,
                                                          MI_AVAILABILITY_TEXT_COLS, load_timestamp,
                                                          MI_AVAILABILITY_COLUMN_MAPPING,
                                                          GET_AVAILABILITY_MAX_SYNC_TIME, MI_AVAILABILITY_TABLE)

        if availability_data:
            insertToDB(session, availability_data, DB_NAME, MI_AVAILABILITY_TABLE)
        else:
            logger.info("No New Data Available for MI Availability!!!")

        return jsonify({}), 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


@token_required
def getSmartabaseFilters():
    logger = get_logger("getSmartabaseFilters", "getSmartabaseFilters")
    try:
        request_json = request.json
        logger.info(f"Request --> {request_json}")
        validateRequest(request_json)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))

    try:
        competition_name = request_json.get("competition_name", ["ALL TOURNAMENTS"])

        filter_dict = {}
        player_mapping_df = getPandasFactoryDF(session, GET_PLAYER_MAPPER_DETAILS_SQL)

        for group in competition_name:
            player_id_list = get_competition_group_mapping(key=group)
            filtered_df = player_mapping_df[player_mapping_df['smartabase_id'].isin(player_id_list)]
            filtered_df = filtered_df[filtered_df["smartabase_id"].isin(player_id_list)]
            filtered_df["competition_name"] = group

            filter_data = filtered_df.groupby('competition_name')['player_name'].agg(list).to_dict()
            filter_dict.update(filter_data)

        filter_dict["competition_name"] = [key for key, value in smartabase_tournament_mapping().items()]

        return jsonify(filter_dict), 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


@token_required
def squadFitnessOverview():
    logger = get_logger("squadFitnessOverview", "squadFitnessOverview")
    try:
        request_json = request.json
        logger.info(f"Request --> {request_json}")
        validateRequest(request_json)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        from_date = request_json.get('from_date')
        to_date = request_json.get('to_date')
        competition_name = request_json.get('competition_name', ["ALL TOURNAMENTS"])

        date_tuple = generate_dates(from_date, to_date)

        start_date_clause = generate_start_date_clause(date_tuple)

        readiness_df = getPandasFactoryDF(session, f'''select smartabase_id, player_name, start_date as record_date, playing_status, 
        physical_status, injury_status, action_status_comments, last_sync_time from {DB_NAME}.readiness_to_perform {start_date_clause};''', 500)

        availability_df = getPandasFactoryDF(session, f'''select smartabase_id, player_name, start_date as record_date, 
         overall_status as availability_status, last_sync_time from {DB_NAME}.mi_availability {start_date_clause} ;''', 500)

        if "player_name" in request_json:
            player_name = request_json["player_name"]
            readiness_df = readiness_df[readiness_df["player_name"].isin(player_name)]
            availability_df = availability_df[availability_df["player_name"].isin(player_name)]

        player_id = []
        for group in competition_name:
            if group != "ALL TOURNAMENTS":
                player_id_list = get_competition_group_mapping(key=group)
                player_id.extend(player_id_list)

        readiness_df = readiness_df.merge(availability_df[["smartabase_id", "record_date", "availability_status"]],
                                          on=["smartabase_id", "record_date"], how="left")

        if competition_name != ["ALL TOURNAMENTS"]:
            readiness_df = readiness_df[readiness_df["smartabase_id"].isin(player_id)]

        if "availability_status" in request.json:
            availability_status = request.json.get('availability_status')
            readiness_df = readiness_df[readiness_df["availability_status"].isin(availability_status)]

        readiness_df['player_image_url'] = (IMAGE_STORE_URL + "mi-all-teams-images/"
                                            + readiness_df['player_name'].apply(lambda x: x.replace(' ', '-').lower()
                                                                                ).astype(str) + ".png")

        # readiness_df['player_image_url'] = asyncio.run(check_urls_async(readiness_df))

        if "sort_key" in request_json:
            asc = request_json.get("asc", True)
            sort_key = request_json["sort_key"]
            if request_json["sort_key"] == "record_date":
                readiness_df['record_date_form'] = pd.to_datetime(readiness_df['record_date'], format='%d/%m/%Y')
                readiness_df = readiness_df.sort_values(by='record_date_form', ascending=asc).drop(["record_date_form"], axis=1)
            else:
                readiness_df = readiness_df.sort_values(by=sort_key, ascending=asc)

        if len(readiness_df)>0:
            readiness_df["player_rank"] = readiness_df.groupby(["player_name", "record_date"])["last_sync_time"].rank(method="first", ascending=False)
            readiness_df = readiness_df[readiness_df["player_rank"] == 1].drop(["player_rank", "last_sync_time"], axis=1)

        return readiness_df.to_json(orient="records"), 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


@token_required
def fitnessStatusReport():
    logger = get_logger("fitnessStatusReport", "fitnessStatusReport")
    try:
        request_json = request.json
        logger.info(f"Request --> {request_json}")
        validateRequest(request_json)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        from_date = request_json.get('from_date')
        to_date = request_json.get('to_date')
        competition_name = request_json.get('competition_name', ["ALL TOURNAMENTS"])

        date_tuple = generate_dates(from_date, to_date)

        start_date_clause = generate_start_date_clause(date_tuple)

        availability_df = getPandasFactoryDF(session, f'''select smartabase_id, player_name, start_date as record_date, 
         overall_status as availability_status, comments, last_sync_time from {DB_NAME}.mi_availability {start_date_clause} ;''')

        if "player_name" in request_json:
            player_name = request_json["player_name"]
            availability_df = availability_df[availability_df["player_name"].isin(player_name)]

        player_id = []
        for group in competition_name:
            if group != "ALL TOURNAMENTS":
                player_id_list = get_competition_group_mapping(key=group)
                player_id.extend(player_id_list)

        if competition_name != ["ALL TOURNAMENTS"]:
            availability_df = availability_df[availability_df["smartabase_id"].isin(player_id)]

        if "availability_status" in request_json:
            availability_status = request_json.get('availability_status')
            availability_df = availability_df[availability_df["availability_status"].isin(availability_status)]

        availability_df['player_image_url'] = (IMAGE_STORE_URL + "mi-all-teams-images/"
                                               + availability_df['player_name'].apply(
                    lambda x: x.replace(' ', '-').lower()
                    ).astype(str) + ".png")

        # availability_df['player_image_url'] = asyncio.run(check_urls_async(availability_df))

        if "sort_key" in request_json:
            asc = request_json.get("asc", True)
            sort_key = request_json["sort_key"]
            if request_json["sort_key"] == "record_date":
                availability_df['record_date_form'] = pd.to_datetime(availability_df['record_date'], format='%d/%m/%Y')
                availability_df = availability_df.sort_values(by='record_date_form', ascending=asc).drop(["record_date_form"], axis=1)
            else:
                availability_df = availability_df.sort_values(by=sort_key, ascending=asc)

        if len(availability_df) > 0:
            availability_df["player_rank"] = availability_df.groupby(["player_name", "record_date"])["last_sync_time"].rank(method="first", ascending=False)
            availability_df = availability_df[availability_df["player_rank"] == 1].drop(["player_rank", "last_sync_time"], axis=1)

        return availability_df.drop(["smartabase_id"], axis=1).to_json(orient="records"), 200, logger.info(
            "Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


generate_api_function(open_api_spec, app_smartabase, '/fetchLatestSmartabaseData', 'get', fetchLatestSmartabaseData, 'fetchLatestSmartabaseData')
generate_api_function(open_api_spec, app_smartabase, '/getSmartabaseFilters', 'post', getSmartabaseFilters, 'getSmartabaseFilters')
generate_api_function(open_api_spec, app_smartabase, '/squadFitnessOverview', 'post', squadFitnessOverview, 'squadFitnessOverview')
generate_api_function(open_api_spec, app_smartabase, '/fitnessStatusReport', 'post', fitnessStatusReport, 'fitnessStatusReport')