import json

from DataIngestion import load_timestamp, config
from DataIngestion.config import (
    BASE_URL,
    EMIRATES_BASE_URL,
    EMIRATES_TOKEN,
    GPS_AGG_DATA_GROUP_LIST,
    GPS_AGG_DATA_JOIN_LIST,
    GPS_AGG_DECIMAL_COL_LIST,
    GPS_AGG_INT_COL_LIST,
    GPS_BALL_DECIMAL_COL_LIST,
    GPS_BALL_INT_COL_LIST,
    GPS_DELIVERY_GROUP_LIST,
    GPS_DELIVERY_JOIN_LIST,
    GPS_DELIVERY_SRC_KEY_MAPPING,
    GPS_DELIVERY_TABLE_NAME,
    GPS_SRC_KEY_MAPPING,
    GPS_TABLE_NAME,
    STATS_API_NAME,
    TOKEN, PRESSURE_INDEX_TABLE_NAME, SFTP_INGESTION_ENABLED, INGESTION_ENABLED,
    WPL_TOKEN, MATCHES_TABLE_NAME,
)
from DataIngestion.ingestion_validation import IngestionValidation
from DataIngestion.match_ingestion import Ingestion
from DataIngestion.preprocessor.fitness_gps_data import (
    fetchGPSData,
    generateGPSData,
    getGPSBallData
)
from DataIngestion.query import (
    GET_GPS_AGG_MAX_DATE,
    GET_GPS_BALL_MAX_DATE,
    GET_ALREADY_EXISTING_GPS_BALL_DATA,
    GET_ALREADY_EXISTING_GPS_DATA
)
from DataIngestion.service.sftp.sftp_incremental_players import SportsMechanicsSFTPPlayerIncrementService
from DataIngestion.service.sftp.sftp_sports_mechanics import SportsMechanicsSftpService
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao.insert_data import insertToDB
from common.dao_client import session
from common.db_config import DB_NAME
from log.log import get_logger

logger = get_logger("Ingestion", "Ingestion")

# Fetching current timestamp
logger.info("Load Timestamp --> {}".format(load_timestamp))

if __name__ == "__main__":
    if INGESTION_ENABLED:
        sql = f"SELECT COUNT(*) AS row_count FROM {MATCHES_TABLE_NAME};"
        initial_ingestion_count = getPandasFactoryDF(session, sql).loc[0][0]
        logger.info("Data Ingestion Module Invoked")

        # Sftp service to download files, when matches are played during tournament daily.
        if SFTP_INGESTION_ENABLED:
            # Download match files
            SportsMechanicsSftpService().add_match_file()

            # Download latest file from sports mechanics
            SportsMechanicsSFTPPlayerIncrementService().download_files()

        # Do ingestion from various source like:
        # 1: Sports Mechanics
        # 2: Nvplay
        # 3: Cricsheet
        Ingestion().ingestion()

        post_ingestion_count = getPandasFactoryDF(session, sql).loc[0][0]
        gps_ball_data = getGPSBallData(generateGPSData(
            fetchGPSData(BASE_URL, STATS_API_NAME, GET_GPS_BALL_MAX_DATE, TOKEN, GPS_DELIVERY_SRC_KEY_MAPPING,
                         GPS_DELIVERY_GROUP_LIST, teams_list=["Mumbai Indians", "MI Capetown", "MI New York"]),
            GET_ALREADY_EXISTING_GPS_BALL_DATA, GPS_DELIVERY_JOIN_LIST,
            gps_decimal_columns=GPS_BALL_DECIMAL_COL_LIST,
            gps_int_columns=GPS_BALL_INT_COL_LIST))

        if len(gps_ball_data) > 0:
            insertToDB(session, gps_ball_data, DB_NAME, GPS_DELIVERY_TABLE_NAME)

        gps_agg_data = generateGPSData(
            fetchGPSData(BASE_URL, STATS_API_NAME, GET_GPS_AGG_MAX_DATE, TOKEN, GPS_SRC_KEY_MAPPING,
                         GPS_AGG_DATA_GROUP_LIST, teams_list=["Mumbai Indians", "MI Capetown", "MI New York"]),
            GET_ALREADY_EXISTING_GPS_DATA, GPS_AGG_DATA_JOIN_LIST,
            gps_decimal_columns=GPS_AGG_DECIMAL_COL_LIST,
            gps_int_columns=GPS_AGG_INT_COL_LIST)

        if not gps_agg_data.empty:
            insertToDB(session, gps_agg_data.to_dict(orient='records'), DB_NAME, GPS_TABLE_NAME)

        em_gps_ball_data = getGPSBallData(generateGPSData(
            fetchGPSData(EMIRATES_BASE_URL, STATS_API_NAME, GET_GPS_BALL_MAX_DATE, EMIRATES_TOKEN,
                         GPS_DELIVERY_SRC_KEY_MAPPING,
                         GPS_DELIVERY_GROUP_LIST, teams_list=["MI Emirates"]), GET_ALREADY_EXISTING_GPS_BALL_DATA,
            GPS_DELIVERY_JOIN_LIST,
            gps_decimal_columns=GPS_BALL_DECIMAL_COL_LIST,
            gps_int_columns=GPS_BALL_INT_COL_LIST))

        if len(em_gps_ball_data) > 0:
            insertToDB(session, em_gps_ball_data, DB_NAME, GPS_DELIVERY_TABLE_NAME)

        em_gps_agg_data = generateGPSData(
            fetchGPSData(EMIRATES_BASE_URL, STATS_API_NAME, GET_GPS_AGG_MAX_DATE, EMIRATES_TOKEN, GPS_SRC_KEY_MAPPING,
                         GPS_AGG_DATA_GROUP_LIST, teams_list=["MI Emirates"]), GET_ALREADY_EXISTING_GPS_DATA,
            GPS_AGG_DATA_JOIN_LIST,
            gps_decimal_columns=GPS_AGG_DECIMAL_COL_LIST,
            gps_int_columns=GPS_AGG_INT_COL_LIST)

        if not em_gps_agg_data.empty:
            insertToDB(session, em_gps_agg_data.to_dict(orient='records'), DB_NAME, GPS_TABLE_NAME)

        wpl_gps_ball_data = getGPSBallData(generateGPSData(
            fetchGPSData(EMIRATES_BASE_URL, STATS_API_NAME, GET_GPS_BALL_MAX_DATE, WPL_TOKEN,
                         GPS_DELIVERY_SRC_KEY_MAPPING,
                         GPS_DELIVERY_GROUP_LIST, teams_list=["Mumbai Indian Womens"]),
            GET_ALREADY_EXISTING_GPS_BALL_DATA,
            GPS_DELIVERY_JOIN_LIST,
            gps_decimal_columns=GPS_BALL_DECIMAL_COL_LIST,
            gps_int_columns=GPS_BALL_INT_COL_LIST))

        if len(wpl_gps_ball_data) > 0:
            insertToDB(session, wpl_gps_ball_data, DB_NAME, GPS_DELIVERY_TABLE_NAME)

        wpl_gps_agg_data = generateGPSData(
            fetchGPSData(EMIRATES_BASE_URL, STATS_API_NAME, GET_GPS_AGG_MAX_DATE, WPL_TOKEN, GPS_SRC_KEY_MAPPING,
                         GPS_AGG_DATA_GROUP_LIST, teams_list=["Mumbai Indian Womens"]), GET_ALREADY_EXISTING_GPS_DATA,
            GPS_AGG_DATA_JOIN_LIST,
            gps_decimal_columns=GPS_AGG_DECIMAL_COL_LIST,
            gps_int_columns=GPS_AGG_INT_COL_LIST)

        if not wpl_gps_agg_data.empty:
            insertToDB(session, wpl_gps_agg_data.to_dict(orient='records'), DB_NAME, GPS_TABLE_NAME)

        # daily_activity_data = dailyActivityData(GET_EXISTING_FORM_ENTRIES_SQL)
        # if daily_activity_data:
        #     insertToDB(session, daily_activity_data, DB_NAME, DAILY_ACTIVITY_TABLE_NAME)
        #
        # fitness_form_df = getFitnessFormDF(GET_EXISTING_PLAYER_LOAD_ID)
        # player_load_data = playerLoadData(fitness_form_df)
        # if player_load_data:
        #     insertToDB(session, player_load_data, DB_NAME, PLAYER_LOAD_TABLE_NAME)
        #
        # planned_data = plannedBowlingData()
        # if planned_data:
        #     insertToDB(session, planned_data, DB_NAME, BOWL_PLANNING_TABLE_NAME)

        logger.info("Processing Started for Pressure Index")
        from DataIngestion.pressure_index import BybData
        from DataIngestion.pressure_index.pi import get_byb_pi

        # Read from config.json file
        with open(config.DUCK_DB_CONFIG_PATH, "r") as f:
            data = json.load(f)
            pressure_index = data["pressure_index"]
        load_all_data = pressure_index['first_load']
        # Read file share to get this var.
        if load_all_data or post_ingestion_count != initial_ingestion_count:
            final_pi_data = get_byb_pi(BybData.combine_data(load_all_data))
            if final_pi_data:
                # Pressure Index BYB Data insert
                logger.info("Insertion Started for Pressure Index")
                insertToDB(session, final_pi_data, DB_NAME, PRESSURE_INDEX_TABLE_NAME)
                logger.info("Insertion Completed for Pressure Index")

        # Validate if data ingestion is done and send mail if ingestion fails.
        IngestionValidation(load_timestamp).validate_ingestion()
        logger.info("Data Ingestion is successfully completed")
