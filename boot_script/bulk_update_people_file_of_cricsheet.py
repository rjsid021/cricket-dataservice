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


def cricinfo_missing_players(cricsheet_id):
    peoples_csv = readCSV(f"/Users/achintya.chaudhary/Documents/projects/CricketDataService/data/people.csv")
    people_csv = peoples_csv[peoples_csv['identifier'] == cricsheet_id].iloc[0]
    max_id = getMaxId(session, PLAYER_MAPPER_TABLE_NAME, PLAYER_MAPPER_KEY_COL, DB_NAME)
    player_mapper_list = []
    BOWLING_STYLE_MAP = {
        'sla': 'LEFT ARM ORTHODOX',
        'ob': 'RIGHT ARM OFF SPINNER',
        'lmf': 'LEFT ARM FAST',
        'rm': 'RIGHT ARM FAST',
        'rmf': 'RIGHT ARM FAST',
        'lm': 'LEFT ARM FAST',
        'rfm': 'RIGHT ARM FAST',
        'lbg': 'RIGHT ARM LEGSPIN',
        'lb': 'RIGHT ARM LEGSPIN',
        'rsm': 'RIGHT ARM FAST',
        'rf': 'RIGHT ARM FAST',
        'lfm': 'LEFT ARM FAST',
        'rab': 'RIGHT ARM FAST',
        'ls': 'LEFT ARM FAST',
        'lsm': 'LEFT ARM FAST',
        'lws': 'LEFT ARM CHINAMAN',
        'rs': 'RIGHT ARM FAST'
    }
    BATTING_STYLE_MAP = {
        'rhb': 'RIGHT HAND BATSMAN',
        'lhb': 'LEFT HAND BATSMAN'
    }
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    url = f"https://hs-consumer-api.espncricinfo.com/v1/pages/player/home?playerId={people_csv['key_cricinfo']}"
    payload = {}
    headers = {}
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        response.raise_for_status()
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
                'cricsheet_id': cricsheet_id,
                'name': player['fullName'].replace("'", "''").strip(),
                'short_name': player['fullName'].replace("'", "''").strip(),
                'full_name': player['fullName'].replace("'", "''").strip(),
                'cricinfo_id': people_csv['key_cricinfo'],
                'sports_mechanics_id': "",
                'nvplay_id': "",
                'country': country,
                'born': born,
                'age': "",
                'bowler_sub_type': bowling_style,
                'striker_batting_type': batting_style,
                'load_timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'is_batsman': is_batsman,
                'is_bowler': is_bowler,
                'is_wicket_keeper': is_wicket_keeper
            }
        )
        max_id += 1
    except requests.exceptions.HTTPError as e:
        try:
            url = f"https://hs-consumer-api.espncricinfo.com/v1/pages/player/home?playerId={int(people_csv['key_cricinfo_2'])}"
            response = requests.request("GET", url, headers=headers, data=payload)
            response.raise_for_status()
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
                    'cricsheet_id': cricsheet_id,
                    'name': player['fullName'].replace("'", "''").strip(),
                    'short_name': player['fullName'].replace("'", "''").strip(),
                    'full_name': player['fullName'].replace("'", "''").strip(),
                    'cricinfo_id': people_csv['key_cricinfo_2'],
                    'sports_mechanics_id': "",
                    'nvplay_id': "",
                    'country': country,
                    'born': born,
                    'age': "",
                    'bowler_sub_type': bowling_style,
                    'striker_batting_type': batting_style,
                    'load_timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'is_batsman': is_batsman,
                    'is_bowler': is_bowler,
                    'is_wicket_keeper': is_wicket_keeper
                }
            )
            max_id += 1
        except Exception as e:
            print(1/0)
    insertToDB(session, player_mapper_list, DB_NAME, PLAYER_MAPPER_TABLE_NAME)


if __name__ == "__main__":
    existing_mapping = getPandasFactoryDF(session, f"select cricsheet_id from {DB_NAME}.{PLAYER_MAPPER_TABLE_NAME}")
    peoples_csv = readCSV(f"/Users/achintya.chaudhary/Documents/projects/CricketDataService/data/people.csv")
    peoples_csv = peoples_csv[['identifier']]
    # Merge existing_mapping and peoples_csv on 'identifier' column
    # merged = existing_mapping.merge(peoples_csv, on='identifier', how='left', indicator=True)
    merged = pd.merge(
        existing_mapping,
        peoples_csv,
        left_on='cricsheet_id',
        right_on='identifier',
        how='right',
        indicator=True
    )
    # Filter rows where '_merge' is 'left_only' to exclude rows present in existing_mapping
    result = merged[merged['_merge'] == 'right_only'].drop('_merge', axis=1)
    for iter in result.iterrows():
        cricinfo_missing_players(iter[1]['identifier'])

