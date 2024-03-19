import json
import ssl

import pandas as pd
import requests

from DataIngestion.config import PLAYER_MAPPER_TABLE_NAME
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao.insert_data import insertToDB
from common.dao_client import session
from common.db_config import DB_NAME


def update_new_players():
    existing_players_mapping = getPandasFactoryDF(session, f"select * from {DB_NAME}.{PLAYER_MAPPER_TABLE_NAME}")
    updated_df = pd.DataFrame(columns=existing_players_mapping.columns)
    for index, row in existing_players_mapping.iterrows():
        print(row['name'])
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        url = f"https://hs-consumer-api.espncricinfo.com/v1/pages/player/home?playerId={row['cricinfo_id']}"
        payload = {}
        headers = {}
        try:
            response = requests.request("GET", url, headers=headers, data=payload)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            continue

        response = json.loads(response.text)
        player = response['player']
        playing_role = player['playingRoles']
        roles = []

        for role in playing_role:
            roles.extend(role.split(' '))

        for role in roles:
            if role in ['wicketkeeper', 'wicketkeeper batter']:
                row['is_wicket_keeper'] = 1
            elif role in ['opening batter', 'batter', 'wicketkeeper batter', 'batting allrounder',
                          'top-order batter', 'middle-order batter']:
                row['is_batsman'] = 1
            elif role in ['bowler', 'bowling allrounder']:
                row['is_bowler'] = 1
            elif role == 'allrounder':
                row['is_batsman'] = 1
                row['is_bowler'] = 1
        row['load_timestamp'] = str(row['load_timestamp'])
        row['born'] = row['born'].replace("'", "")
        row['full_name'] = row['full_name'].replace("'", "")
        row['name'] = row['name'].replace("'", "")
        row['short_name'] = row['short_name'].replace("'", "")

        updated_df = updated_df.append(row, ignore_index=True)
        insertToDB(
            session,
            updated_df.to_dict(orient='records'),
            DB_NAME,
            PLAYER_MAPPER_TABLE_NAME
        )
        updated_df = pd.DataFrame(columns=existing_players_mapping.columns)


if __name__ == "__main__":
    # This will insert all the players, running it multiple times will crate multiple entities
    update_new_players()
