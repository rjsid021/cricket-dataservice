from DataIngestion import config
from DataIngestion.config import GPS_TABLE_NAME, BUILD_ENV
from DataService.fetch_sql_queries import GET_FITNESS_DATA
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session
from common.db_config import DB_NAME
import pandas as pd
import json
from log.log import get_logger
from third_party_service.smtp import SMTPMailer

logger = get_logger("check_src_duplicates", "check_src_duplicates")


def check_duplicates_fitness_agg():
    GET_GPS_AGG_MAX_DATE = f'''select max(date_name) as max_ts from {DB_NAME}.{GPS_TABLE_NAME} '''
    max_date = getPandasFactoryDF(session,GET_GPS_AGG_MAX_DATE).iloc[0, 0]
    logger.info(f"max_date --> {max_date}")

    GET_FITNESS_DATA_SQL = GET_FITNESS_DATA + f''' where date_name='{max_date}' ALLOW FILTERING;'''

    fitness_data = getPandasFactoryDF(session, GET_FITNESS_DATA_SQL)[
                ['record_date', 'team_name', 'total_distance_m']]
    fitness_data['total_distance_m'] = fitness_data['total_distance_m'].apply(pd.to_numeric, errors='coerce').round(decimals=0)

    gps_fitness_agg_df = fitness_data.groupby(["team_name", "record_date"]).mean().round(decimals=0).astype(int).reset_index()

    if gps_fitness_agg_df["total_distance_m"].iloc[0]>10000:
        with open(config.NVPLAY_INGESTION_CONFIG, 'r') as json_file:
            ingestion_config = json.load(json_file)
        recipient_emails = ingestion_config['recipients']
        subject = f"Catapult Data Duplicacy Alert: Env: {BUILD_ENV.upper()}"
        message = \
        f'''Hi, 
        The data appears to have duplicates from the source for date {str(gps_fitness_agg_df["record_date"].iloc[0])} 
        for team {str(gps_fitness_agg_df["team_name"].iloc[0])}, leading to inflation. 
        Kindly review at your earliest convenience. 
        Thank you!'''
        SMTPMailer().send_bulk_email(recipient_emails, subject, message)

    return