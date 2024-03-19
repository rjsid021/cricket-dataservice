import datetime
import json
import os
import ssl

import pandas as pd
import requests

from DataIngestion.config import PLAYER_MAPPER_TABLE_NAME, PLAYER_MAPPER_KEY_COL
from DataIngestion.utils.helper import readCSV
from common.dao.fetch_db_data import getMaxId, getPandasFactoryDF
from common.dao.insert_data import insertToDB
from common.dao_client import session
from common.db_config import DB_NAME

BATTING_STYLE_MAP = {
    'rhb': 'RIGHT HAND BATSMAN',
    'lhb': 'LEFT HAND BATSMAN'
}

BOWLING_STYLE_MAP = {
    'sla': 'SLOW LEFT ARM ORTHODOX',
    'ob': 'RIGHT ARM OFFBREAK',
    'lmf': 'LEFT ARM MEDIUM FAST',
    'rm': 'RIGHT ARM MEDIUM',
    'rmf': 'RIGHT ARM MEDIUM FAST',
    'lm': 'LEFT ARM MEDIUM',
    'rfm': 'RIGHT ARM FAST MEDIUM',
    'lbg': 'LEGBREAK GOOGLY',
    'lb': 'LEGBREAK',
    'rsm': 'RIGHT ARM SLOW MEDIUM',
    'rf': 'RIGHT ARM FAST',
    'lfm': 'LEFT ARM FAST MEDIUM',
    'rab': 'RIGHT ARM BOWLER',
    'ls': 'LEFT ARM SLOW',
    'lsm': 'LEFT ARM SLOW MEDIUM',
    'lws': 'LEFT ARM WRIST SPIN',
    'rs': 'RIGHT ARM SLOW'
}


def update_new_players():
    people_file_common = readCSV(f"{os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../../data/people.csv')}")
    people_file = people_file_common[['key_cricinfo', 'identifier', 'key_cricinfo_2', 'unique_name']]
    people_file_one = people_file[['key_cricinfo', 'identifier', 'unique_name']]
    people_file_one['key_cricinfo'] = people_file_one['key_cricinfo'].dropna().astype(int)
    people_file_two = people_file[['key_cricinfo_2', 'identifier', 'unique_name']]
    people_file_two['key_cricinfo'] = people_file_two['key_cricinfo_2'].dropna().astype(int)
    people_file = pd.concat([people_file_one, people_file_two[['key_cricinfo', 'identifier']]]).drop_duplicates()
    max_id = getMaxId(session, PLAYER_MAPPER_TABLE_NAME, PLAYER_MAPPER_KEY_COL, DB_NAME)
    existing_players_mapping = getPandasFactoryDF(session, f"select cricsheet_id from {DB_NAME}.{PLAYER_MAPPER_TABLE_NAME}")
    player_mapper_list = []
    iterator = 1
    people_file.dropna(subset=['key_cricinfo'], inplace=True)
    # people_file['identifier'] = people_file['identifier']
    existing_players_mapping.dropna(subset=['cricsheet_id'], inplace=True)
    existing_players_mapping = existing_players_mapping[existing_players_mapping['cricsheet_id'] != '']
    # existing_players_mapping['cricsheet_id'] = existing_players_mapping['cricsheet_id'].astype(int)
    # people_file['key_cricinfo'] = people_file['key_cricinfo'].astype(int)
    people_file = people_file[~(people_file['identifier'].isin(existing_players_mapping['cricsheet_id']))]
    for index, row in people_file.iterrows():
        print("Iteration: ", iterator)
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        url = f"https://hs-consumer-api.espncricinfo.com/v1/pages/player/home?playerId={row[0]}"
        payload = {}
        headers = {}
        try:
            response = requests.request("GET", url, headers=headers, data=payload)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:

            print(f"not found {row[0]}, {row[1]}")
            if str(row['unique_name']) == 'nan':
                continue
            player_mapper_list.append(
                {
                    'id': int(max_id),
                    'cricsheet_id': row['identifier'],
                    'name': row['unique_name'].strip(),
                    'short_name': row['unique_name'].strip(),
                    'full_name': row['unique_name'].strip(),
                    'cricinfo_id': str(row['key_cricinfo']),
                    'sports_mechanics_id': "",
                    'country': "",
                    'born': "",
                    'age': "",
                    'source_name': row['unique_name'].strip(),
                    'bowler_sub_type': "",
                    'striker_batting_type': "",
                    'load_timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'is_batsman': -1,
                    'is_bowler': -1,
                    'is_wicket_keeper': -1
                }
            )
            max_id += 1
            iterator += 1
            continue
        response = json.loads(response.text)
        player = response['player']
        playing_role = player['playingRoles']
        roles = []

        for role in playing_role:
            roles.extend(role.split(' '))
        is_wicket_keeper = 0
        is_bowler = 0
        is_batsman = 0

        for role in roles:
            if role in ['wicketkeeper', 'wicketkeeper batter']:
                is_wicket_keeper = 1
            elif role in ['opening batter', 'batter', 'wicketkeeper batter', 'batting allrounder',
                          'top-order batter', 'middle-order batter']:
                is_batsman = 1
            elif role in ['bowler', 'bowling allrounder']:
                is_bowler = 1
            elif role == 'allrounder':
                is_batsman = 1
                is_bowler = 1

        bowling_style = ""
        if player.get('longBowlingStyles'):
            bowling_style = BOWLING_STYLE_MAP.get(player['bowlingStyles'][0])
        batting_style = ""
        if player.get('longBattingStyles'):
            batting_style = BATTING_STYLE_MAP.get(player['battingStyles'][0])
        born = ""
        if player.get('dateOfBirth'):
            born = datetime.datetime(player['dateOfBirth']['year'], player['dateOfBirth']['month'],
                                     player['dateOfBirth']['date']).strftime("%Y-%m-%d")
        country = ""
        if player.get('country'):
            country = player['country']['name']
        print(f"Processing for player: {player['longName']}")
        player_mapper_list.append(
            {
                'id': int(max_id),
                'cricsheet_id': row['identifier'],
                'name': player['longName'].replace("'", "''").strip(),
                'short_name': player['name'].replace("'", "''").strip(),
                'full_name': player['fullName'].replace("'", "''").strip(),
                'cricinfo_id': str(row['key_cricinfo']),
                'sports_mechanics_id': "",
                'country': country,
                'born': born,
                'age': "",
                'source_name': player['longName'].replace("'", "''").strip(),
                'bowler_sub_type': bowling_style,
                'striker_batting_type': batting_style,
                'load_timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'is_batsman': is_batsman,
                'is_bowler': is_bowler,
                'is_wicket_keeper': is_wicket_keeper
            }
        )
        max_id += 1
        iterator += 1
    insertToDB(session, player_mapper_list, DB_NAME, PLAYER_MAPPER_TABLE_NAME)


if __name__ == "__main__":
    # This will insert all the players, running it multiple times will crate multiple entities
    update_new_players()
