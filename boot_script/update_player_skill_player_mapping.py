import datetime
import json
import ssl

import requests

from DataIngestion.config import PLAYER_MAPPER_TABLE_NAME
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao.insert_data import insertToDB
from common.dao_client import session
from common.db_config import DB_NAME

if __name__ == "__main__":
    player_mapping = getPandasFactoryDF(session, f"SELECT * FROM {DB_NAME}.playermapping")
    counter = 1
    for row, index in player_mapping.iterrows():
        print(counter, index['name'])
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        url = f"https://hs-consumer-api.espncricinfo.com/v1/pages/player/home?playerId={index['cricinfo_id']}"
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
                if role in [
                    'opening batter',
                    'batter',
                    'wicketkeeper batter',
                    'batting allrounder',
                    'top-order batter',
                    'middle-order batter'
                ]:
                    is_batsman = 1
                if role in ['bowler', 'bowling allrounder']:
                    is_bowler = 1
                if role == 'allrounder':
                    is_batsman = 1
                    is_bowler = 1
            player_mapping.at[row, 'is_batsman'] = is_batsman
            player_mapping.at[row, 'is_bowler'] = is_bowler
            player_mapping.at[row, 'is_wicket_keeper'] = is_wicket_keeper
            counter += 1
        except requests.exceptions.HTTPError as e:
            print("exception ------------------->", e)
    player_mapping['load_timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    player_mapping_df = player_mapping.to_dict(orient='records')
    insertToDB(session, player_mapping_df, DB_NAME, PLAYER_MAPPER_TABLE_NAME)
