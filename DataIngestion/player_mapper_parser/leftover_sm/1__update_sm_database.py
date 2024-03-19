import json
import ssl

import pandas as pd
import requests

from DataIngestion.utils.helper import readCSV


def update_new_players():
    sM_df_unique = readCSV(
        f"/DataIngestion/player_mapper_parser/sM_df.csv"
    )
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    append_innings_df = pd.DataFrame(
        columns=['item', 'season', 'match', 'cricketer', 'cricinfo_id', 'name', 'short_name', 'full_name'])
    for index, row in sM_df_unique.iterrows():
        print(index, row)
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
        sM_df_unique.at[index, 'name'] = player['name'].replace("'", "''").strip()
        sM_df_unique.at[index, 'short_name'] = player['mobileName'].replace("'", "''").strip()
        sM_df_unique.at[index, 'full_name'] = player['longName'].replace("'", "''").strip()
        sM_df_unique.to_csv("sm_with_names.csv")


if __name__ == "__main__":
    # update_sm()
    update_new_players()
