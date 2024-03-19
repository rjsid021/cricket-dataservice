import sys

import pandas as pd

sys.path.append("./../../")
sys.path.append("./")
sys.path.append("./../../")
sys.path.append("./")

import json
import ssl

import requests

from DataIngestion.utils.helper import readCSV


def update_new_players():
    sm_df = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/DataIngestion/player_mapper_parser/nvplay/0__parse_raw_file.csv"
    )
    x = sm_df[['key_cricinfo']]
    x = x.drop_duplicates()
    x = x.dropna()
    x['cricinfo_id'] = x['cricinfo_id'].astype(int)

    final_list = []
    max_key_val = 0
    # Create an empty DataFrame with columns
    pdddd = pd.DataFrame(columns=['cricinfo_id', 'name', 'short_name', 'full_name'])
    for index, row in x.iterrows():
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        try:
            url = f"https://hs-consumer-api.espncricinfo.com/v1/pages/player/home?playerId={row[0]}"
            payload = {}
            headers = {}
            response = requests.request("GET", url, headers=headers, data=payload)
            response.raise_for_status()
            response = json.loads(response.text)
        except Exception as e:
            print(f"{row[0]}")
            continue

        player = response['player']
        new_row = {
            'cricinfo_id': int(row['cricinfo_id']),
            'name': player['longName'].replace("'", "''").strip(),
            'short_name': player['name'].replace("'", "''").strip(),
            'full_name': player['fullName'].replace("'", "''").strip()
        }

        # Append the new row to the DataFrame
        pdddd = pdddd.append(new_row, ignore_index=True)
    pdddd.to_csv("2___csv_and_cricinfo_parser.csv")


if __name__ == "__main__":
    update_new_players()
