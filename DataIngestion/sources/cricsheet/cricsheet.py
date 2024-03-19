import json
import os

from DataIngestion.config import CRICSHEET_DATA_PATH
from DataIngestion.match_ingestion import MatchIngestion
from DataIngestion.query import GET_EXISTING_FILES
from DataIngestion.sources.cricsheet.parser import MatchData
from common.dao.fetch_db_data import getAlreadyExistingValue
from common.dao_client import session


class Cricsheet(MatchIngestion):
    def __init__(self):
        loaded_files = getAlreadyExistingValue(session, GET_EXISTING_FILES)
        self.cricsheet_loaded_file = [file for file in loaded_files]
        self.cricsheet_files = CRICSHEET_DATA_PATH

    def cricsheet_competition_name(self, folder_path):
        # This method can be extended when cricsheet will be used to upload more data of different game format
        return " ".join(folder_path.split('/')[-1].split(' ')[0: -1])

    def read_json(self, path):
        f = open(path)
        return json.load(f)

    def match_processing(self, players_existing_mapping):
        folder_paths = []
        cricsheet_files = os.listdir(self.cricsheet_files)
        for file in cricsheet_files:
            if file == ".DS_Store":
                continue
            folder_path = os.path.join(self.cricsheet_files, file)
            folder_path_sub_dir = os.listdir(folder_path)
            for sub_dir in folder_path_sub_dir:
                folder_paths.append(os.path.join(folder_path, sub_dir))
        for folder_path in folder_paths:
            if folder_path.split("/")[-1] == ".DS_Store":
                continue
            file_names = os.listdir(folder_path)
            for idx, file_name in enumerate(file_names):
                if file_name[-4:] != "json" or file_name in self.cricsheet_loaded_file:
                    continue
                print("started for match ------> ", folder_path.split('/')[-1], file_name)
                data = self.read_json(os.path.join(folder_path, file_name))
                match_data = MatchData()
                ball_by_ball_df, squad = match_data.get_match_dataframe(
                    data,
                    file_name.split(".json")[0],
                    self.cricsheet_competition_name(folder_path)
                )
                self.match_ingestion(
                    ball_by_ball_df,
                    {file_name: os.path.join(folder_path, file_name)},
                    players_existing_mapping,
                    squad
                )
