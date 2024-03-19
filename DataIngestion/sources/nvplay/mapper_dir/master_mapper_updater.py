# This file just add name to stage two.csv file
# stored in player mapping table

import pandas as pd

from DataIngestion.utils.helper import readCSV
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session
from common.db_config import DB_NAME

if __name__ == "__main__":
    new_batch = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/DataIngestion/sources/nvplay/mapper_dir/T20_2022.csv"
    )
    master_mapper = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/DataIngestion/sources/nvplay/mapper_dir/master_mapper.csv"
    )

    new_batch = new_batch[['cricinfo_id', 'nvplay_name', 'name']]

    # append both dataframe
    master_mapper = master_mapper.append(new_batch)
    # drop duplicates
    master_mapper = master_mapper.drop_duplicates(subset=['cricinfo_id', 'nvplay_name', 'name'])
    master_mapper.to_csv("master_mapper.csv")
