import pandas as pd
import requests
import json
from pandas import json_normalize


class Smartabase:

    def __init__(self, username, password, app_name):
        self.username = username
        self.password = password
        self.app_name = app_name
        self.api_version = "2023.10.4"
        self.x_app_id = "micip"

    def login(self, login_url):
        login_data = {
            "username": f"{self.username}",
            "password": f"{self.password}",
            "loginProperties": {
                "appName": f"{self.app_name}",
                "clientTime": "2023-01-01T13:00"
            }
        }

        # Define the login headers
        login_headers = {
            "X-GWT-Permutation": "HostedMode",
            "session-header": "[session]",
            "Content-Type": "application/json",
            "Cookie": f"JSESSIONID='[session]'",
            "api-version": f"{self.api_version}",
            "X-APP-ID": f"{self.x_app_id}"
        }

        login_data_json = json.dumps(login_data)

        # getting login response
        response = requests.post(login_url, data=login_data_json, headers=login_headers)

        if response.status_code == 200:
            return response.cookies['JSESSIONID']
        else:
            session_id = None
            return session_id

    def get_player_ids(self, session_id, members_url, group_name):

        if session_id:
            payload = {
                "name": f"{group_name}"
            }

            # Define the request headers
            data_headers = {
                "Content-Type": "application/json",
                "session-header": session_id,
                "Cookie": f"JSESSIONID={session_id}",
                "api-version": self.api_version,
                "X-APP-ID": f"{self.x_app_id}"
            }
            response = requests.post(members_url, data=json.dumps(payload), headers=data_headers)
            data = response.json()['results'][0]['results']

            # adding user_ids to the list
            user_id = [d['userId'] for d in data]
            return user_id

    def fetch_data(self, data_sync_url, session_id, form_name, user_id_list, last_sync_time=0):

        payload = {
            "formName": f"{form_name}",
            "lastSynchronisationTimeOnServer": last_sync_time,
            "userIds": user_id_list
            # "userIds": [1297, 1298, 1299, 1683, 1300, 1301, 1302, 1303, 1304, 1305, 1306, 1307, 1564, 1308, 1309, 1310, 1311, 1312, 1313, 1314, 1315, 1316, 1317, 1318, 1319, 1320, 1597]
        }

        # Define the request headers
        data_headers = {
            "Content-Type": "application/json",
            "session-header": session_id,
            "Cookie": f"JSESSIONID={session_id}",
            "api-version": self.api_version,
            "X-APP-ID": self.x_app_id
        }

        # generating form data response
        response = requests.post(data_sync_url, data=json.dumps(payload), headers=data_headers)

        # Check the response status and content
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            return None

    def flatten_data(self, data, col_list):
        if data['export']:
            # Extract events from the "export" key
            events = data['export']['events']

            # Extract lastSynchronisationTimeOnServer
            last_sync_time = data['lastSynchronisationTimeOnServer']

            # Initialize an empty list to store individual rows
            rows_list = []

            # Iterate through events
            for event in events:
                form_name = event['formName']
                user_id = event['userId']
                event_id = event['id']
                # Iterate through rows in the event
                for row in event['rows']:
                    row_data = {
                        'form_name': form_name,
                        'start_date': event['startDate'],
                        'start_time': event['startTime'],
                        'finish_date': event['finishDate'],
                        'finish_time': event['finishTime'],
                        'user_id': user_id,
                        'event_id': event_id,
                        'last_sync_time': last_sync_time
                    }

                    # Extract key-value pairs from pairs
                    pairs = row['pairs']
                    row_data.update({pair['key']: pair['value'] for pair in pairs})

                    rows_list.append(row_data)

            # Use json_normalize to flatten the nested structure
            flat_df = json_normalize(rows_list)

            return flat_df[col_list]

        else:
            return pd.DataFrame()

    def logout(self, session_id, logout_url):
        payload = "{}"
        headers = {
            'session-header': f'{session_id}',
            'Cookie': f'JSESSIONID={session_id}; lastuser={self.username}; activeSession=true; JSESSIONID={session_id}',
            'X-APP-ID': f'{self.x_app_id}'
        }

        response = requests.request("POST", logout_url, headers=headers, data=payload)
        return response.text






