import base64
import datetime
import os
from io import BytesIO

import openpyxl
from azure.storage.blob import BlockBlobService
from openpyxl.styles import Border, Side

from DataService.src import *
from DataIngestion.config import FIELD_ANALYSIS_FILE
from common.utils.helper import getEnvVariables

account_name = getEnvVariables('STORAGE_ACCOUNT_NAME')
account_key = getEnvVariables('STORAGE_ACCOUNT_KEY')
container_name = getEnvVariables('CONTAINER_NAME')
dir_name = getEnvVariables('FIELDING_ANALYSIS_DIR_NAME')


def add_borders_to_excel(file_path, sheet_name, start_row, start_column, end_row, end_column):
    # Load the Excel file
    workbook = openpyxl.load_workbook(file_path)

    # Select the sheet you want to work with
    sheet = workbook[sheet_name]

    # Define the border style you want to apply
    border_style = Border(left=Side(style='thin'),
                          right=Side(style='thin'),
                          top=Side(style='thin'),
                          bottom=Side(style='thin'))

    # Iterate through the specified range and apply the border to each cell
    for row in sheet.iter_rows(min_row=start_row, min_col=start_column, max_row=end_row, max_col=end_column):
        for cell in row:
            cell.border = border_style

    # Save the changes back to the Excel file
    workbook.save(file_path)


def write_multiple_worksheets_to_blob(output, blob_folder_name, match_name, file_name):
    # Create a BlockBlobService object to connect to the storage account
    block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)

    # Create a container if it doesn't exist
    block_blob_service.create_container(container_name)

    # Get the current datetime
    current_datetime = datetime.now()
    blob_name = f"{dir_name}/{blob_folder_name}/{match_name}/{current_datetime}/{file_name}"

    # Upload the in-memory Excel file to Azure Blob Storage
    if blob_folder_name == 'pre-filled-template':
        # Open the local file in binary read mode and stream it to Azure Blob Storage
        with open(output, "rb") as data:
            block_blob_service.create_blob_from_stream(container_name, blob_name, data)
    else:
        block_blob_service.create_blob_from_stream(container_name, blob_name, output)

    print(f"Excel file '{file_name}' with multiple worksheets is uploaded to Azure Blob Storage.")


def writeXlData(fielder_template_df, wk_template_df, match_name):
    with pd.ExcelWriter(FIELD_ANALYSIS_FILE, engine='xlsxwriter') as writer:

        fielder_template_df.to_excel(writer, sheet_name='Fielder Data')  # Write the DataFrame to the workbook
        wk_template_df.to_excel(writer, sheet_name='Wicketkeeper Data')

        # Remove the first indexed blank row will NaN values
        writer.sheets['Fielder Data'].set_row(2, None, None, {'hidden': True})
        writer.sheets['Wicketkeeper Data'].set_row(2, None, None, {'hidden': True})

        worksheet_f = writer.sheets['Fielder Data']
        worksheet_wk = writer.sheets['Wicketkeeper Data']

        # Apply Column Width to each Column
        for idx, col in enumerate(fielder_template_df):  # loop through all columns
            series = fielder_template_df[col]
            max_len = max((
                series.astype(str).map(len).max(),  # len of largest item
                len(str(series.name))  # len of column name/header
            )) + 1  # adding a little extra space
            worksheet_f.set_column(idx, idx, max_len)  # set column width

        for idx, col in enumerate(wk_template_df):  # loop through all columns
            series = wk_template_df[col]
            max_len = max((
                series.astype(str).map(len).max(),  # len of largest item
                len(str(series.name))  # len of column name/header
            )) + 1  # adding a little extra space
            worksheet_wk.set_column(idx, idx, max_len)

        writer.save()

        add_borders_to_excel(FIELD_ANALYSIS_FILE, 'Fielder Data', start_row=1, start_column=1,
                             end_row=len(fielder_template_df) + 3, end_column=len(fielder_template_df.columns) + 1)
        add_borders_to_excel(FIELD_ANALYSIS_FILE, 'Wicketkeeper Data', start_row=1, start_column=1,
                             end_row=len(wk_template_df) + 3,
                             end_column=len(wk_template_df.columns) + 1)

        write_multiple_worksheets_to_blob(FIELD_ANALYSIS_FILE, 'pre-filled-template', match_name,
                                          'filled_template.xlsx')

    response = "File written Successfully!"
    return response


def encodeAndDeleteXl(File_Name):
    data = open(File_Name, 'rb').read()
    base64_encoded = base64.b64encode(data).decode('UTF-8')
    # Use os.remove() to delete the file
    os.remove(FIELD_ANALYSIS_FILE)
    print(f"File '{FIELD_ANALYSIS_FILE}' has been deleted successfully.")
    return base64_encoded


def decodeAndUploadXL(encode_xl):
    xlDecoded = base64.b64decode(encode_xl)
    fielder_df = pd.read_excel(xlDecoded, sheet_name='Fielder Data', header=[0, 1], index_col=None)
    wk_df = pd.read_excel(xlDecoded, sheet_name='Wicketkeeper Data', header=[0, 1], index_col=None)

    fielder_df.drop(('Unnamed: 0_level_0', 'Unnamed: 0_level_1'), axis=1, inplace=True)
    wk_df.drop(('Unnamed: 0_level_0', 'Unnamed: 0_level_1'), axis=1, inplace=True)
    fielder_df.dropna(how='all', inplace=True)
    wk_df.dropna(how='all', inplace=True)
    match_name = str(fielder_df[('GENERAL DETAILS', 'MATCH NAME')].iloc[0])

    # Sample DataFrames for each worksheet
    dataframes = {
        "Fielder Data": fielder_df,
        "Wicketkeeper Data": wk_df
    }

    # Create an in-memory Excel file using BytesIO
    output = BytesIO()

    # Create a Pandas ExcelWriter object with the XlsxWriter engine
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        # Write each DataFrame to a separate worksheet
        for sheet_name, df in dataframes.items():
            df.to_excel(writer, sheet_name=sheet_name)

        # Save the ExcelWriter to the in-memory Excel file
        writer.save()

    # Reset the BytesIO position to the beginning of the stream
    output.seek(0)

    write_multiple_worksheets_to_blob(output, 'user-data-files', match_name, 'fielding_analysis_data.xlsx')
    return fielder_df, wk_df


def convtListOfTuplesToDict(input_list):
    result_dict = {}
    for data in input_list:
        key = data[0]
        value = data[1]
        if key not in result_dict:
            result_dict[key] = []
        result_dict[key].append(value)
    # del result_dict['Unnamed: 0_level_0']
    return result_dict


def compare_dictionaries(dict1, dict2):
    # Check if the keys match
    if set(dict1.keys()) != set(dict2.keys()):
        return False

    # Compare values for each key
    for key in dict1:
        if sorted(dict1[key]) != sorted(dict2[key]):
            return False

    return True


# Function to convert non-numeric values to -1
def convert_non_numeric(column, df, exceptional_columns):
    if column not in exceptional_columns:
        df[column] = df[column].apply(lambda x: -1 if isinstance(x, str) else x)
        # .apply(lambda x: -1 if not str(x).isnumeric() else x)
    return column


def parseXl(fielder_df, wk_df, match_name_selected, player_id_mapping):
    dict_fielder_cols = {'GENERAL DETAILS': ['PLAYER NAME', 'MATCH NAME', 'SEASON', 'TEAM NAME'],
                         'GROUND FIELDING': ['CLEAN TAKES', 'MISS FIELDS', 'MISS FIELDS COST'],
                         'DIVING': ['DIVES MADE', 'RUNS SAVED', 'DIVES MISSED', 'MISSED RUNS', 'GOOD ATTEMPT'],
                         'CATCHES': ['TAKEN', 'STUMPING', 'DROPPED % DIFFICULTY', 'CAUGHT &BOWLED'],
                         'THROWING ACCURACY': ['GOOD RETURN', 'POOR RETURN', 'DIRECT HIT', 'MISSED SHY',
                                               'RUN OUT OBTAINED'], 'TEAM WORK': ['POP UPS', 'SUPPORT RUN', 'BACK UP']}
    columnDictFielder = convtListOfTuplesToDict(list(fielder_df.columns))

    if compare_dictionaries(dict_fielder_cols, columnDictFielder):
        print("success")
    else:
        sys.exit()

    fielder_df = fielder_df[~fielder_df.astype(str).apply(lambda x: x.str.contains('\n')).all(axis=1)]
    fielder_df = fielder_df.dropna(subset=[('GENERAL DETAILS', 'PLAYER NAME'), ('GENERAL DETAILS', 'MATCH NAME'),
                                           ('GENERAL DETAILS', 'SEASON'), ('GENERAL DETAILS', 'TEAM NAME')])
    fielder_df = fielder_df.reset_index(drop=True).dropna(how='all').fillna(-1).replace('\n', -1)


    #  Apply the function to each element in the DataFrame except the exceptional columns
    exceptional_columns = [('GENERAL DETAILS', 'PLAYER NAME'), ('GENERAL DETAILS', 'MATCH NAME'),
                           ('GENERAL DETAILS', 'SEASON'), ('GENERAL DETAILS', 'TEAM NAME')]
    fielder_df.columns = fielder_df.columns.map(lambda x: convert_non_numeric(x, fielder_df, exceptional_columns))

    fielder_df.columns = fielder_df.columns.get_level_values(1).str.replace(' ', '_').str.lower()
    fielder_df = fielder_df.rename(
        columns={'dropped_%_difficulty': 'dropped_percent_difficulty', 'caught_&bowled': 'caught_and_bowled'})
    print(fielder_df.columns)

    # check on selected and filled match name & Calc match_id for the record
    if len(fielder_df) > 0:
        result = fielder_df['match_name'] == match_name_selected
        if result.all():
            match_name = fielder_df['match_name'].values[0]
            f_match_name = matchNameConv(match_name)
            match_id = matches_join_data[matches_join_data['match_name'] == f_match_name]['match_id'].values[0]
            fielder_df['match_id'] = match_id
            # Assign ids to players in df1 based on the mapping
            fielder_df['player_id'] = fielder_df['player_name'].map(player_id_mapping)
        else:
            sys.exit()

    fielder_df['player_type'] = 'Fielder'
    wk_additional_cols = ['standing_back_plus', 'standing_back_minus', 'standing_up_plus', 'standing_up_minus',
                          'returns_taken_plus', 'returns_untidy']
    fielder_df[wk_additional_cols] = -1


    ## WicketKeeper Data Operation Started
    dict_wk_cols = {'GENERAL DETAILS': ['PLAYER NAME', 'MATCH NAME', 'SEASON', 'TEAM NAME'],
                    'GROUND FIELDING': ['STANDING BACK +', 'STANDING BACK -', 'STANDING UP +', 'STANDING UP -'],
                    'DIVING': ['DIVES MADE', 'RUNS SAVED', 'DIVES MISSED', 'MISSED RUNS', 'GOOD ATTEMPT'],
                    'CATCHES': ['TAKEN', 'STUMPING', 'DROPPED % DIFFICULTY', 'CAUGHT &BOWLED'],
                    'THROWING ACCURACY': ['RETURNS TAKEN +', 'RETURNS UNTIDY', 'DIRECT HIT', 'MISSED SHY',
                                          'RUN OUT OBTAINED'], 'TEAM WORK': ['POP UPS', 'SUPPORT RUN', 'BACK UP']}
    columnDictWk = convtListOfTuplesToDict(list(wk_df.columns))

    if compare_dictionaries(dict_wk_cols, columnDictWk):
        print("success")
    else:
        sys.exit()

    wk_df = wk_df[~wk_df.astype(str).apply(lambda x: x.str.contains('\n')).all(axis=1)]
    wk_df = wk_df.reset_index(drop=True).dropna(how='all').fillna(-1).replace('\n', -1)
    wk_df = wk_df.dropna(subset=[('GENERAL DETAILS', 'PLAYER NAME'), ('GENERAL DETAILS', 'MATCH NAME'),
                                 ('GENERAL DETAILS', 'SEASON'), ('GENERAL DETAILS', 'TEAM NAME')])

    #  Apply the function to each element in the DataFrame except the exceptional columns
    exceptional_columns = [('GENERAL DETAILS', 'PLAYER NAME'), ('GENERAL DETAILS', 'MATCH NAME'),
                           ('GENERAL DETAILS', 'SEASON'), ('GENERAL DETAILS', 'TEAM NAME')]
    wk_df.columns = wk_df.columns.map(lambda x: convert_non_numeric(x, wk_df, exceptional_columns))
    # print(tabulate(wk_df, headers='keys', tablefmt='psql'))

    wk_df.columns = wk_df.columns.get_level_values(1).str.replace(' ', '_').str.lower()
    wk_df = wk_df.rename(
        columns={'standing_back_+': 'standing_back_plus', 'standing_back_-': 'standing_back_minus',
                 'standing_up_+': 'standing_up_plus',
                 'standing_up_-': 'standing_up_minus', 'returns_taken_+': 'returns_taken_plus',
                 'dropped_%_difficulty': 'dropped_percent_difficulty', 'caught_&bowled': 'caught_and_bowled'})
    print(wk_df.columns)

    # check on selected and filled match name & Calc match_id for the record
    if len(wk_df) > 0:
        result = wk_df['match_name'] == match_name_selected
        if result.all():
            match_name = wk_df['match_name'].values[0]
            f_match_name = matchNameConv(match_name)
            match_id = matches_join_data[matches_join_data['match_name'] == f_match_name]['match_id'].values[0]
            wk_df['match_id'] = match_id
            # Assign ids to players in df1 based on the mapping
            wk_df['player_id'] = wk_df['player_name'].map(player_id_mapping)
        else:
            sys.exit()

    wk_df['player_type'] = 'Wicketkeeper'
    fielder_additional_cols = ['clean_takes', 'miss_fields', 'miss_fields_cost', 'good_return',
                               'poor_return']
    wk_df[fielder_additional_cols] = -1

    final_df = pd.concat([fielder_df, wk_df], ignore_index=True)
    return final_df


def sum_ignore_negative(series):
    if (series < 0).all():
        return -1
    return series[series >= 0].sum()


def matchNameConv(match_name):
    match_str = match_name.rsplit(" ", 1)
    match_date = match_str[-1]
    name = match_str[0].replace(" ", "")
    f_match_date = datetime.strptime(match_date, '%d-%B-%Y').strftime('%d%m%Y')
    f_match_name = name + f_match_date

    return f_match_name


def dupCheckBeforeInsert(filtered_fa_df, new_field_data):
    merged_df = new_field_data.merge(filtered_fa_df, on=['match_name', 'player_name'], how='left', indicator=True, suffixes=('_x', '_y'))

    columns_to_drop = merged_df.filter(regex='(_y)|(_merge)', axis=1).columns
    # Drop the rows from df1 that exist in df2
    result_df = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=columns_to_drop)
    result_df.rename(columns=lambda x: x.rstrip('_x'), inplace=True)

    return result_df
