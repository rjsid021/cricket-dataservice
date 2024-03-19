import datetime
import json
import uuid

import numpy as np
import pandas as pd
import yaml

from DataIngestion import config
from DataIngestion.config import PLAYER_INCREMENTAL_FILE_PATH, PLAYER_MAPPER_TABLE_NAME, PLAYER_MAPPER_KEY_COL
from DataIngestion.utils.helper import readExcel
from common.dao.fetch_db_data import getMaxId, getPandasFactoryDF
from common.dao.insert_data import insertToDB
from common.dao_client import session
from common.db_config import DB_NAME
from log.log import get_logger
from third_party_service.smtp import SMTPMailer

logger = get_logger("DataIngestion utils", "update_player_mapping")


class PlayerMapping:
    def __init__(self):
        with open(config.SFTP_CONFIG_PATH, 'r') as yaml_file:
            self.sftp_config = yaml.safe_load(yaml_file)

        self.sheet_name = self.sftp_config['player_incremental_dump_sheet']

    # def get_blob_player_mapping(self):
    #     # Download azure blob to local storage
    #     MicipBlobStorage().get_blob(
    #         remote_dir_name=PLAYER_MAPPING_BLOB_PATH,
    #         local_dir_path=PLAYER_MAPPING_LOCAL_PATH
    #     )
    #
    #     # Read the blob and return
    #     return readCSV(PLAYER_MAPPING_LOCAL_PATH)
    #
    # def update_blob_player_mapping(self, player_mapping):
    #     # convert player mapping df to csv file
    #     player_mapping.to_csv(PLAYER_MAPPING_LOCAL_PATH)
    #
    #     # read the blob from local storage and upload to blob
    #     MicipBlobStorage().update_blob(
    #         remote_dir_name=PLAYER_MAPPING_BLOB_PATH,
    #         local_dir_name=PLAYER_MAPPING_LOCAL_PATH
    #     )

    def create_new_player_mapping(self, player_mapping_incremental, ref_id, max_id):
        player_mapping_incremental = player_mapping_incremental[player_mapping_incremental['RefID'] == ref_id]
        player = player_mapping_incremental.iloc[0]
        player_info = {
            'id': int(max_id),
            'age': None,
            'born': None,
            'bowler_sub_type': player['BOWLING_TYPE'].upper(),
            'catapult_id': None,
            'country': player['COUNTRY'].title(),
            'cricinfo_id': -1,
            'cricsheet_id': None,
            'full_name': player['PLAYER_DISPLAY_NAME'],
            'is_batsman': None,
            'is_bowler': None,
            'is_wicket_keeper': None,
            'load_timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'name': player['PLAYER_DISPLAY_NAME'],
            'nv_play_id': str(uuid.uuid4()),
            'nvplay_id': None,
            'short_name': player['PLAYER_DISPLAY_NAME'],
            'smartabase_id': -1,
            'source_name': None,
            'sports_mechanics_id': ref_id,
            'striker_batting_type': player['BATTING_TYPE'].upper()
        }
        # Create DataFrame with one row
        players_df = pd.DataFrame([player_info])

        players_df['bowler_sub_type'] = players_df['bowler_sub_type'].apply(
            lambda
                x: "LEFT ARM FAST" if x == "LEFT ARM KNUCKLEBALL" else "RIGHT ARM FAST" if x == "RIGHT ARM KNUCKLEBALL" else x
        )
        # Update values in the Series
        player = player.copy()
        player[['PlayerSkill']] = player[['PlayerSkill']].map(
            lambda x: x.strip().upper().replace('ALLRONDER', 'ALLROUNDER'))

        players_df['is_batsman'] = np.where(
            (player['PlayerSkill'] == 'BATSMAN') |
            (player['PlayerSkill'] == 'ALLROUNDER') |
            (player['PlayerSkill'] == 'WICKETKEEPER'),
            1,
            0
        )

        players_df['is_bowler'] = np.where(
            (player['PlayerSkill'] == 'BOWLER') |
            (player['PlayerSkill'] == 'ALLROUNDER'),
            1,
            0
        )

        players_df['is_wicket_keeper'] = np.where(
            player['PlayerSkill'] == 'WICKETKEEPER',
            1,
            0
        )

        players_df = players_df.iloc[0]
        player_info['is_batsman'] = int(players_df['is_batsman'])
        player_info['is_bowler'] = int(players_df['is_bowler'])
        player_info['is_wicket_keeper'] = int(players_df['is_wicket_keeper'])
        return player_info

    def update_mapping(self):
        player_mapping_incremental = readExcel(PLAYER_INCREMENTAL_FILE_PATH, sheet=self.sheet_name)
        player_mapping_df = getPandasFactoryDF(session, f"select * from {DB_NAME}.{PLAYER_MAPPER_TABLE_NAME}")
        merged_df = pd.merge(
            player_mapping_incremental[['RefID', 'PLAYER_DISPLAY_NAME']],
            player_mapping_df,
            left_on='RefID',
            right_on='sports_mechanics_id',
            how='left'
        )
        max_id = getMaxId(session, PLAYER_MAPPER_TABLE_NAME, PLAYER_MAPPER_KEY_COL, DB_NAME)
        # Filter rows where ID is not present in right DataFrame
        missing_entries_df = merged_df[merged_df['sports_mechanics_id'].isnull()]
        unmapped_players = player_mapping_df[player_mapping_df['sports_mechanics_id'] == ""]
        messages, updated_players = [], []
        for index, series in missing_entries_df.iterrows():
            player_display_name = series['PLAYER_DISPLAY_NAME']
            name_entry = unmapped_players[unmapped_players['name'] == player_display_name]

            if len(name_entry) > 1:
                name_entry = name_entry.iloc[0]
                # update the df with RefID
                player_mapping_df.loc[player_mapping_df['id'] == name_entry['id'], 'sports_mechanics_id'] = series[
                    'RefID']
                player_mapping_df.loc[
                    player_mapping_df['id'] == name_entry['id'], 'load_timestamp'] = datetime.datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S")
                # Create message for the update that happened to player mapping df
                messages.append(
                    f"Multiple Player entry with name: {player_display_name} found! We choose first name and updated with refID: {series['RefID']}"
                )
                series = player_mapping_df[player_mapping_df['sports_mechanics_id'] == series['RefID']].iloc[0]
                series['id'] = int(series['id'])
                series['smartabase_id'] = int(series['smartabase_id'])
                series['is_batsman'] = int(series['is_batsman'])
                series['is_bowler'] = int(series['is_bowler'])
                series['is_wicket_keeper'] = int(series['is_wicket_keeper'])
                if series['cricinfo_id']:
                    series['cricinfo_id'] = int(series['cricinfo_id'])

                updated_players.append(series.to_dict())
            elif len(name_entry) == 0:
                # No entry found create new entry and update to blob
                newly_added_player_df = self.create_new_player_mapping(
                    player_mapping_incremental, series['RefID'],
                    max_id
                )

                max_id += 1
                updated_players.append(newly_added_player_df)
                # Create message for the update that happened to player mapping df
                messages.append(
                    f"Player entry with name: {player_display_name} was not there in micip, so new player with id: {newly_added_player_df['id']} has been added"
                )
            else:
                name_entry = name_entry.iloc[0]
                # update the df with RefID
                # Locate the row with matching id and update the 'sports_mechanics_id' column
                player_mapping_df.loc[player_mapping_df['id'] == name_entry['id'], 'sports_mechanics_id'] = series[
                    'RefID']
                player_mapping_df.loc[
                    player_mapping_df['id'] == name_entry['id'], 'load_timestamp'] = datetime.datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S")
                # Create message for the update that happened to player mapping df
                messages.append(
                    f"Multiple Player entry with name: {player_display_name} found and got updated with refID: {series['RefID']}"
                )
                series = player_mapping_df[player_mapping_df['sports_mechanics_id'] == series['RefID']].iloc[0]
                series['id'] = int(series['id'])

                series['smartabase_id'] = int(series['smartabase_id'])
                series['is_batsman'] = int(series['is_batsman'])
                series['is_bowler'] = int(series['is_bowler'])
                series['is_wicket_keeper'] = int(series['is_wicket_keeper'])
                if series['cricinfo_id']:
                    series['cricinfo_id'] = int(series['cricinfo_id'])

                updated_players.append(series.to_dict())
        # update player mapping into database
        insertToDB(session, updated_players, DB_NAME, PLAYER_MAPPER_TABLE_NAME)
        # load config of recipients email whom to send mail
        with open(config.NVPLAY_INGESTION_CONFIG, 'r') as json_file:
            ingestion_config = json.load(json_file)

        # only send mail if any player info is updated
        if len(missing_entries_df) >= 1:
            # Send mail to recipients
            SMTPMailer().send_bulk_email(
                ingestion_config['recipients'],
                "MICIP Player Mapping Added/Updated, Please Review ... ⚠️",
                str(messages)
            )
        else:
            logger.info("No new player mapping updated.")
