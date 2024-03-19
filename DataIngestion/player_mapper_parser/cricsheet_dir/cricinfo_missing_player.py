import datetime
import os
import ssl

import requests

from DataIngestion.config import PLAYER_MAPPER_TABLE_NAME, PLAYER_MAPPER_KEY_COL
from DataIngestion.utils.helper import readCSV
from common.dao.fetch_db_data import getMaxId
from common.dao.insert_data import insertToDB
from common.dao_client import session
from common.db_config import DB_NAME


def cricinfo_missing_players(cricsheet_id):
    peoples_csv = readCSV(f"{os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../../data/people.csv')}")
    people_csv = peoples_csv[peoples_csv['identifier'] == cricsheet_id].iloc[0]
    max_id = getMaxId(session, PLAYER_MAPPER_TABLE_NAME, PLAYER_MAPPER_KEY_COL, DB_NAME)
    player_mapper_list = []
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    url = f"https://hs-consumer-api.espncricinfo.com/v1/pages/player/home?playerId={people_csv['key_cricinfo']}"
    payload = {}
    headers = {}
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        try:
            url = f"https://hs-consumer-api.espncricinfo.com/v1/pages/player/home?playerId={int(people_csv['key_cricinfo_2'])}"
            response = requests.request("GET", url, headers=headers, data=payload)
            response.raise_for_status()
        except Exception as e:
            player_mapper_list.append(
                {
                    'id': int(max_id),
                    'cricsheet_id': people_csv['identifier'],
                    'name': people_csv['name'].replace("'", "''").strip(),
                    'short_name': people_csv['name'].replace("'", "''").strip(),
                    'full_name': people_csv['name'].replace("'", "''").strip(),
                    'cricinfo_id': "",
                    'sports_mechanics_id': "",
                    'load_timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
    insertToDB(session, player_mapper_list, DB_NAME, PLAYER_MAPPER_TABLE_NAME)


