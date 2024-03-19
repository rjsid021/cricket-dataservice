import pandas as pd
import requests
import json
from pandas import json_normalize
from tabulate import tabulate
import re
from DataIngestion.utils.helper import dataToDF, readCSV
import uuid

# Generate a random UUID

def generate_uuid():
    random_uuid = uuid.uuid4()
    return str(random_uuid)


def clean_and_format(value, replacement=''):
    # Replace special characters, strip leading/trailing whitespaces, and convert to lowercase
    cleaned_value = re.sub(r'[^a-zA-Z0-9]', replacement, value).strip().lower()
    return cleaned_value


def getPrettyDF(df):
    return tabulate(df, headers='keys', tablefmt='psql')


# set the login data
login_data = {
    "username": "siddharth.nautiyal",
    "password": ":=Siddharth12345",
    "loginProperties": {
        "appName": "ams",
        "clientTime": "2022-01-01T13:00"
    }
}

# Define the login headers
login_headers = {
    "X-GWT-Permutation": "HostedMode",
    "session-header": "[session]",
    "Content-Type": "application/json",
    "Cookie": f"JSESSIONID='[session]'",
    "api-version": "2023.10.4"
}

# login_headers = {'Content-Type': 'application/json',
#                  'session-header': 'RyGw8LQb+IE84hBWjgVgQkSy',
#                  'Cookie': 'JSESSIONID=RyGw8LQb+IE84hBWjgVgQkSy',
#                  'api-version': '2023.10.4',
#                  'X-APP-ID': 'micip'}

# user login url
api_url = "https://mumbaiindians.smartabase.com/ams/api/v2/user/loginUser"

login_data_json = json.dumps(login_data)

# getting login response
response = requests.post(api_url, data=login_data_json, headers=login_headers)

if response.status_code == 200:
    print("Login successful")
    # fetching session id
    session_id = response.cookies['JSESSIONID']
    print(session_id)

    # form data url
    # data_url = f"https://mumbaiindians.smartabase.com/ams/api/v3/forms/event/501/data"
    data_url = "https://mumbaiindians.smartabase.com/ams/api/v1/groupmembers?informat=json&format=json"
    api_version = "2023.10.4"

    form_name = "Key Performance Markers"

    team_group_mapping = { "ALL TOURNAMENTS": "All Teams"}

    # team_group_mapping = {"ILT20": "ILT20",
    #  "WPL": "WPL Squad",
    #  "SA20": "SA20",
    #  "IPL": "IPL Squad",
    #  "MLC": "MLC Squad",
    #  "UK Tour (Mens) 2023": "UK Tour (Mens) 2023",
    #  "ALL TOURNAMENTS": "All Teams"}

    # Define the request headers
    data_headers = {
        "Content-Type": "application/json",
        "session-header": session_id,
        "Cookie": f"JSESSIONID={session_id}",
        "api-version": api_version,
        "X-APP-ID": "micip"
    }
    # data_headers = {'Content-Type': 'application/json',
    #                  'session-header': 'RyGw8LQb+IE84hBWjgVgQkSy',
    #                  'Cookie': 'JSESSIONID=RyGw8LQb+IE84hBWjgVgQkSy',
    #                  'api-version': '2023.10.4',
    #                  'X-APP-ID': 'micip'}

    # generating form data response
    # response = requests.get(data_url, headers=data_headers)
    data_dict = {}
    for key, value in team_group_mapping.items():
        payload = {
            "name": f"{value}"
        }
        response = requests.post(data_url, data=json.dumps(payload), headers=data_headers)

        # Check the response status and content
        if response.status_code == 200:
            data = response.json()['results'][0]['results']
            user_id = [d['userId'] for d in data]
            data_dict[key] = user_id
            print(user_id)
        else:
            print(response)

    # print(data_dict)
    # import json
    #
    # with open('../data/competition_group_mapping.json', 'w') as fp:
    #     json.dump(data_dict, fp)
        # json_normalize(
        #     data['export']['events'],
        #     record_path=['rows', 'pairs'],
        #     meta=[
        #         'formName',
        #         'startDate',
        #         'startTime',
        #         'finishDate',
        #         'finishTime',
        #         'userId',
        #         'enteredByUserId',
        #         'id'
        #     ]
        # )
        # data = json.dumps(data, indent=4)
        # print(f"data ---> {data}")


