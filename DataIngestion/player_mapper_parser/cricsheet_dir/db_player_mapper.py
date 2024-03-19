import datetime
import json
import os

from DataIngestion.config import PLAYER_MAPPER_TABLE_NAME
from common.dao.insert_data import insertToDB
from common.dao_client import session
from common.db_config import DB_NAME


def player_mapper():
    data = None
    file_name = f"{os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../sources/cricsheet/files/player_info_cricsheet.json')}"
    with open(file_name) as json_file:
        data = json.load(json_file)
    player_mapper_list = []
    max_id = 1

    for row in data:
        is_wicket_keeper = 0
        is_bowler = 0
        is_batsman = 0
        try:
            import ssl
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            import requests
            url = f"https://hs-consumer-api.espncricinfo.com/v1/pages/player/home?playerId={str(data[row]['key_cricinfo'])}"
            payload = {}
            headers = {}
            response = requests.request("GET", url, headers=headers, data=payload)
            response.raise_for_status()
            response = json.loads(response.text)
            playing_role = response['player']['playingRoles']
            roles = []
            for role in playing_role:
                roles.extend(role.split(' '))

            for role in roles:
                if role == 'wicketkeeper':
                    is_wicket_keeper = 1
                elif role == 'batter':
                    is_batsman = 1
                elif role == 'bowler':
                    is_bowler = 1
        except requests.exceptions.HTTPError as err:
            pass
        player_mapper_list.append(
            {
                'id': int(max_id),
                'cricsheet_id': str(row),
                'name': (str(data[row]['Name'])).replace("'", "''"),
                'short_name': (str(data[row]['short_name'])).replace("'", "''"),
                'full_name': (str(data[row]['Full Name'])).replace("'", "''"),
                'cricinfo_id': str(data[row]['key_cricinfo']),
                'sports_mechanics_id': str(data[row]['SourceIdToUse']) if len(
                    str(data[row]['SourceIdToUse'])) > 8 else "",
                'country': str(data[row]['Country']),
                'born': str(data[row]['Born']).replace("'", "''"),
                'age': str(data[row]['Age']),
                'bowler_sub_type': str(data[row]['bowler_sub_type']),
                'striker_batting_type': str(data[row]['striker_batting_type']),
                'load_timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'is_batsman': is_batsman,
                'is_bowler': is_bowler,
                'is_wicket_keeper': is_wicket_keeper
            }
        )
        max_id += 1
        print((str(data[row]['Name'])).replace("'", "''")),
    insertToDB(session, player_mapper_list, DB_NAME, PLAYER_MAPPER_TABLE_NAME)


if __name__ == "__main__":
    # This will insert all the players, running it multiple times will crate multiple entities
    player_mapper()
