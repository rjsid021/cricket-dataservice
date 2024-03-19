import sys

from DataIngestion.config import PLAYER_MAPPER_TABLE_NAME
from common.dao.fetch_db_data import getPandasFactoryDF, getMaxId

sys.path.append("../../../")
sys.path.append("../")

sys.path.append("../../../")
sys.path.append("../")

import datetime
import json
import ssl

import pandas as pd
import requests

from DataIngestion.utils.helper import readCSV
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
    load_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sm_df = readCSV(
        "/DataIngestion/player_mapper_parser/leftover_sm/unique_failed_player.csv"
    )




    PLAYER_MAPPER_KEY_COL = "id"
    max_key_val = getMaxId(session, PLAYER_MAPPER_TABLE_NAME, PLAYER_MAPPER_KEY_COL, DB_NAME)
    for index, row in sm_df.iterrows():
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        try:
            url = f"https://hs-consumer-api.espncricinfo.com/v1/pages/player/home?playerId={row['cricinfo_id']}"
            payload = {}
            headers = {}
            response = requests.request("GET", url, headers=headers, data=payload)
            response.raise_for_status()
            response = json.loads(response.text)
        except Exception as e:
            print(f"{row['cricinfo_id']}")
            continue

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
        # if player.get('dateOfBirth'):
        #     born = datetime.datetime(player['dateOfBirth']['year'], player['dateOfBirth']['month'],
        #                              player['dateOfBirth']['date']).strftime("%Y-%m-%d")
        country = ""
        if player.get('country'):
            country = player['country']['name']

        xx = {
            'id': int(max_key_val),
            'sports_mechanics_id': row['sports_mechanics_id'],
            'name': player['longName'].replace("'", "''").strip(),
            'short_name': player['name'].replace("'", "''").strip(),
            'full_name': player['fullName'].replace("'", "''").strip(),
            'country': country,
            'born': born,
            'age': "",
            'bowler_sub_type': bowling_style,
            'striker_batting_type': batting_style,
            'load_timestamp': load_timestamp,
            'is_batsman': is_batsman,
            'is_bowler': is_bowler,
            'is_wicket_keeper': is_wicket_keeper
        }
        max_key_val += 1

        insertToDB(session, [xx], DB_NAME, PLAYER_MAPPER_TABLE_NAME)


if __name__ == "__main__":
    # This will insert all the players, running it multiple times will crate multiple entities
    update_new_players()
