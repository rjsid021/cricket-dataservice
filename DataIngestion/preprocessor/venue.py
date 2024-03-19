import json
import os
import sys

from DataIngestion import load_timestamp
from common.dao_client import session

sys.path.append("./../../")
sys.path.append("./")
from log.log import get_logger
from common.dao.fetch_db_data import getMaxId, getAlreadyExistingValue
from DataIngestion.query import GET_VENUE_DETAILS_SQL
from common.db_config import DB_NAME
from DataIngestion.utils.helper import readJsFile, excludeAlreadyExistingRecords, generateSeq, random_string_generator
from DataIngestion.config import (VENUE_TABLE_NAME, VENUE_KEY_COL, FILE_SHARE_PATH)

import pandas as pd

logger = get_logger("Ingestion", "Ingestion")


def getVenueData(session, root_data_files, load_timestamp):
    logger.info("Venue Data Generation Started!")
    if root_data_files:
        path_set = set(value for key, value in root_data_files.items()
                       if 'matchschedule' in key.split("-")[1].split(".")[0].strip().lower())

        data_li = []
        for path in path_set:
            for data in readJsFile(path)['Result']:
                data_li.append(data)

        venues_df = pd.DataFrame(data_li)[["GroundID", "GroundName"]].drop_duplicates().reset_index() \
            .rename(columns={"GroundID": "src_venue_id", "GroundName": "stadium_name"}).drop("index", axis=1)

        venues_df['stadium_name'] = venues_df['stadium_name'].apply(lambda x: x.upper())

        venues_df['load_timestamp'] = load_timestamp

        venues_df = venues_df.drop_duplicates(subset='stadium_name', keep='first')
        # Fetching name of the players already exists in the db table
        venues_list = getAlreadyExistingValue(session, GET_VENUE_DETAILS_SQL)

        # Excluding the records already present in the DB table
        venues_df = excludeAlreadyExistingRecords(venues_df, 'stadium_name', venues_list)
        venue_mapping = os.path.join(FILE_SHARE_PATH, "data/venue_mapping.json")
        with open(venue_mapping, 'r', encoding='utf-8') as file:
            venue_mapping_content = json.loads(file.read())
        venues_df['stadium_name'] = venues_df['stadium_name'].str.upper()
        venues_df['stadium_name'] = venues_df['stadium_name'].replace(venue_mapping_content)
        venues_list = getAlreadyExistingValue(session, GET_VENUE_DETAILS_SQL)
        venues_df = excludeAlreadyExistingRecords(venues_df, 'stadium_name', venues_list)
        # Fetching max primary key value
        max_key_val = getMaxId(session, VENUE_TABLE_NAME, VENUE_KEY_COL, DB_NAME)

        # Generating and adding the sequence to the primary key and converting it to dictionary
        venue_final_data = generateSeq(venues_df, VENUE_KEY_COL, max_key_val).to_dict(orient='records')
        logger.info("Venue Data Generation Completed!")
        return venue_final_data

    else:
        logger.info("No New Venue Data Available!")


def get_cricsheet_venue(ball_by_ball_df):
    venue_df = ball_by_ball_df[["stadium_name"]].drop_duplicates()
    venue_mapping = os.path.join(FILE_SHARE_PATH, "data/venue_mapping.json")
    with open(venue_mapping, 'r', encoding='utf-8') as file:
        venue_mapping_content = json.loads(file.read())
    venue_df['stadium_name'] = venue_df['stadium_name'].str.upper()
    venue_df['stadium_name'] = venue_df['stadium_name'].replace(venue_mapping_content)
    venue_df['src_venue_id'] = venue_df['stadium_name'].apply(lambda x: random_string_generator(x))
    venues_list = getAlreadyExistingValue(session, GET_VENUE_DETAILS_SQL)
    # Excluding the records already present in the DB table
    venues_df = excludeAlreadyExistingRecords(venue_df, 'stadium_name', venues_list)
    venues_df['load_timestamp'] = load_timestamp
    # Generating and adding the sequence to the primary key and converting it to dictionary
    venue_final_data = generateSeq(
        venues_df,
        VENUE_KEY_COL,
        getMaxId(session, VENUE_TABLE_NAME, VENUE_KEY_COL, DB_NAME, False),
    ).to_dict(orient='records')
    return venue_final_data
