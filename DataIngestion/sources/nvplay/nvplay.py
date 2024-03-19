import json
import os

from DataIngestion.config import NVPLAY_PATH, NVPLAY_PATH_SQUAD
from DataIngestion.match_ingestion import MatchIngestion
from DataIngestion.query import GET_EXISTING_FILES
from DataIngestion.sources.nvplay.parser import NVPlayFileParser
from DataIngestion.utils.helper import readCSV
from common.dao.fetch_db_data import getAlreadyExistingValue
from common.dao_client import session


class NVPlay(MatchIngestion):
    def __init__(self):
        from log.log import get_logger
        self.logger = get_logger("nvplay", "ingestion")
        loaded_files = getAlreadyExistingValue(session, GET_EXISTING_FILES)
        self.nv_play_loaded_file = [file for file in loaded_files]
        self.nv_play_files = NVPLAY_PATH

    def nv_play_competition_name(self, folder_path):
        # This method can be extended when cricsheet will be used to upload more data of different game format
        return " ".join(folder_path.split('/')[-1].split(' ')[0: -1])

    def match_processing(self, players_existing_mapping):
        folder_paths = []
        cricsheet_files = os.listdir(self.nv_play_files)
        for file in cricsheet_files:
            if file == ".DS_Store":
                continue
            folder_path = os.path.join(self.nv_play_files, file)
            folder_path_sub_dir = os.listdir(folder_path)
            for sub_dir in folder_path_sub_dir:
                folder_paths.append(os.path.join(folder_path, sub_dir))
        # sort folder_paths
        folder_paths.sort()
        for folder_path in folder_paths:
            if folder_path.split("/")[-1] == ".DS_Store":
                continue
            file_names = os.listdir(folder_path)
            # sort file_names
            file_names.sort()
            for idx, file_name in enumerate(file_names):
                if file_name[-3:] != "csv" or file_name in self.nv_play_loaded_file:
                    continue
                self.logger.info(f"started for match ------> {folder_path.split('/')[-1]} {file_name}")
                season_matches_df = readCSV(os.path.join(folder_path, file_name))
                nv_play_parser = NVPlayFileParser(
                    season_matches_df,
                    self.nv_play_competition_name(folder_path)
                )

                squad_file = os.path.join(NVPLAY_PATH_SQUAD, f"{folder_path.split('/')[-1]}/squad.json")
                with open(squad_file) as json_file:
                    squads = json.load(json_file)
                ball_by_ball_dataframes, matches_squad = nv_play_parser.get_ball_by_ball(squads)
                iterator = 0
                for ball_by_ball_df in ball_by_ball_dataframes:
                    self.logger.info(f"--------> {ball_by_ball_df.iloc[0]['match_name']} {str(ball_by_ball_df.iloc[0]['match_date'])} <-------")
                    self.match_ingestion(
                        ball_by_ball_df,
                        {file_name: os.path.join(folder_path, file_name)},
                        players_existing_mapping,
                        matches_squad[iterator]
                    )
                    iterator += 1
