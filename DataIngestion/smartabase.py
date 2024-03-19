import requests
import json
from pandas import json_normalize
from tabulate import tabulate


def getPrettyDF(df):
    return tabulate(df, headers='keys', tablefmt='psql')


# set the login data
login_data = {
    "username": "siddharth.nautiyal",
    "password": "Siddharth12345",
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
    data_url = "https://mumbaiindians.smartabase.com/ams/api/v1/synchronise?informat=json&format=json"
    api_version = "2023.10.4"

    form_name = "MI Availability"

    payload = {
        "formName": f"{form_name}",
        "lastSynchronisationTimeOnServer": 0,
        "userIds": [1300]
        # "userIds": [1297, 1298, 1299, 1683, 1300, 1301, 1302, 1303, 1304, 1305, 1306, 1307, 1564, 1308, 1309, 1310, 1311, 1312, 1313, 1314, 1315, 1316, 1317, 1318, 1319, 1320, 1597]
    }

    # Define the request headers
    data_headers = {
        "Content-Type": "application/json",
        "session-header": session_id,
        "Cookie": f"JSESSIONID={session_id}",
        "api-version": api_version,
        "X-APP-ID": "micip"
    }

    # generating form data response
    # response = requests.get(data_url, headers=data_headers)
    response = requests.post(data_url, data=json.dumps(payload), headers=data_headers)

    print(f"response --> {response}")
    # Check the response status and content
    if response.status_code == 200:
        data = response.json()
        # data = json.dumps(data, indent=4)

        import pandas as pd
        from pandas import json_normalize

        # Extract events from the "export" key
        events = data['export']['events']

        # Extract lastSynchronisationTimeOnServer
        last_sync_time = data['lastSynchronisationTimeOnServer']

        # Initialize an empty list to store individual rows
        rows_list = []

        # Iterate through events
        for event in events:
            form_name = event['formName']
            start_datetime = f"{event['startDate']} {event['startTime']}"
            finish_datetime = f"{event['finishDate']} {event['finishTime']}"
            user_id = event['userId']
            entered_by_user_id = event['enteredByUserId']
            event_id = event['id']

            # Iterate through rows in the event
            for row in event['rows']:
                row_data = {
                    'formName': form_name,
                    'startDate': event['startDate'],
                    'startTime': event['startTime'],
                    'finishDate': event['finishDate'],
                    'finishTime': event['finishTime'],
                    'userId': user_id,
                    'enteredByUserId': entered_by_user_id,
                    'id': event_id,
                    'lastSynchronisationTimeOnServer': last_sync_time
                }

                # Extract key-value pairs from pairs
                pairs = row['pairs']
                row_data.update({pair['key']: pair['value'] for pair in pairs})

                rows_list.append(row_data)

        # Use json_normalize to flatten the nested structure
        flat_df = json_normalize(rows_list)

        # Convert date columns to datetime format
        # date_columns = ['startDate', 'finishDate']
        # flat_df[date_columns] = flat_df[date_columns].apply(pd.to_datetime, format='%d/%m/%Y')

        # new_line_columns = ['Actions Status Comments', 'Comments/Actions for Management', 'Injury Status', 'Injury Status Comments', 'Physical Status Comments', 'Physical Status',
        #                     'Playing Status', 'Playing Status Comments']
        #
        # for col in new_line_columns:
        #     flat_df[col] = flat_df[col].str.replace('\n', ' ')


        ################################## MI AVailability #######################################################
        # flat_df = json_normalize(data['export']['events'], record_path=['rows', 'pairs'],
        #                          meta=['formName', 'startDate', 'startTime', 'finishDate', 'finishTime', 'userId',
        #                                'enteredByUserId', 'id'])
        # print(getPrettyDF(flat_df.head(10)))
        # # Pivot the DataFrame to have 'key' as columns
        # flat_df = flat_df.pivot(
        #     index=['formName', 'startDate', 'startTime', 'finishDate', 'finishTime', 'userId', 'enteredByUserId', 'id'],
        #     columns='key', values='value').reset_index()

        ################################# Key Performance markers ##############################################
        #
        # flat_df = json_normalize(
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
        #
        # # Pivot the DataFrame to get 'key' values as columns
        # flat_df = flat_df.pivot_table(
        #     index=['formName', 'startDate', 'startTime', 'finishDate', 'finishTime', 'userId', 'enteredByUserId', 'id'],
        #     columns='key',
        #     values='value',
        #     aggfunc='first'
        # ).reset_index()

        ##################33################# Readiness to Perform #########################################

        # flat_df = json_normalize(
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
        #
        # # Pivot the DataFrame to get 'key' values as columns
        # flat_df = flat_df.pivot_table(
        #     index=['formName', 'startDate', 'startTime', 'finishDate', 'finishTime', 'userId', 'enteredByUserId', 'id'],
        #     columns='key',
        #     values='value',
        #     aggfunc='first'
        # ).reset_index()
        #
        # new_line_columns = ['Actions Status Comments', 'Comments/Actions for Management', 'Injury Status', 'Injury Status Comments', 'Physical Status Comments', 'Physical Status',
        #                     'Playing Status', 'Playing Status Comments']
        #
        # for col in new_line_columns:
        #     flat_df[col] = flat_df[col].str.replace('\n', ' ')


        ##################################### Injury/Illness Record #########################################

        # flat_df = json_normalize(
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
        # ).pivot_table(
        #     index=['formName', 'startDate', 'startTime', 'finishDate', 'finishTime', 'userId', 'enteredByUserId', 'id'],
        #     columns='key',
        #     values='value',
        #     aggfunc='first'
        # ).reset_index()
        #
        # new_line_columns = ['Additional Comments', 'Address', 'Diagnosis Summary', 'Diagnosis Details', 'Disclaimer', 'Illness Presentation',
        #                     'Last Action Note', 'Last Entered Status', 'Last Entered Status Filtered', 'Last Objective Note', 'Last Plan Note',
        #                     'Last Subjective Note', 'Last Treatment Note', 'Main Signs and Symptoms Observed/Reported', 'Main Signs and Symptoms Observed/Reported String',
        #                     'Mechanism Details', 'OSICS Diagnosis', 'Status', 'Status Indicator', 'Where did this injury occur?']
        #
        # for col in new_line_columns:
        #     flat_df[col] = flat_df[col].str.replace('\n', ' ')
        #
        # flat_df.to_csv(f"{form_name.replace('/', '_')}.csv", index=False)
        # # Display the resulting DataFrame
        # print(flat_df.count())
        print(getPrettyDF(flat_df))
        # print(f"data ---> {data}")
else:
    print("Login failed")
