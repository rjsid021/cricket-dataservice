# This file is used to change the name of player who have played in nvplay to name which we have
# stored in player mapping table
import os

import pandas as pd

from DataIngestion.config import NVPLAY_PATH
from DataIngestion.utils.helper import readCSV
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session
from common.db_config import DB_NAME

if __name__ == "__main__":
    stage_three_mapper = readCSV("/Users/achintya.chaudhary/Documents/projects/CricketDataService/DataIngestion/sources/nvplay/cricinfo_nvplayid_mapper/stage_three.csv")
    nvplay_ingestion_csv_file_to_be_updated = readCSV("/Users/achintya.chaudhary/Documents/projects/CricketDataService/data/nv_play/ILT20/ILT20 2023/ILT20.csv")
    mapper = dict(zip(stage_three_mapper['nvplay_name'], stage_three_mapper['name']))
    nvplay_ingestion_csv_file_to_be_updated['Batter'] = nvplay_ingestion_csv_file_to_be_updated['Batter'].replace(mapper)
    nvplay_ingestion_csv_file_to_be_updated['Bowler'] = nvplay_ingestion_csv_file_to_be_updated['Bowler'].replace(mapper)
    nvplay_ingestion_csv_file_to_be_updated['Dismissed Batter'] = nvplay_ingestion_csv_file_to_be_updated['Dismissed Batter'].replace(mapper)
    nvplay_ingestion_csv_file_to_be_updated['Non-Striker'] = nvplay_ingestion_csv_file_to_be_updated['Non-Striker'].replace(mapper)
    nvplay_ingestion_csv_file_to_be_updated.to_csv("MLC.csv")
