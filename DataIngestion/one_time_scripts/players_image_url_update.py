import sys

import numpy as np
import pandas as pd
import re

from common.utils.helper import getEnvVariables

sys.path.append("./../../")
sys.path.append("./")
from azure.storage.blob import BlockBlobService
from DataIngestion.config import IMAGE_STORE_URL
from DataService.utils.helper import dropFilter, getUpdateSetValues
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session
from common.db_config import DB_NAME

container_name = getEnvVariables('CONTAINER_NAME')
block_blob_service = BlockBlobService(
    account_name=getEnvVariables('STORAGE_ACCOUNT_NAME'),
    account_key=getEnvVariables('STORAGE_ACCOUNT_KEY')
)

players = getPandasFactoryDF(session, f'''select team_id, src_player_id, player_id, competition_name, season, player_name, player_image_url  
from {DB_NAME}.players;''')


# GET_PLAYERS_DATA_2023 = getPandasFactoryDF(session, f'''select team_id, src_player_id, player_id, competition_name, season, player_name
# from {DB_NAME}.players where season >= 2023 ALLOW FILTERING;''')
# GET_PLAYERS_DATA = pd.concat([GET_PLAYERS_DATA_IPL_2022, GET_PLAYERS_DATA_2023], ignore_index=True)
#
# GET_TEAMS_NAME_DATA = getPandasFactoryDF(session, f'''select team_id, team_name from {DB_NAME}.Teams''')
# final_joined_df = GET_PLAYERS_DATA.merge(GET_TEAMS_NAME_DATA, on='team_id', how='left')
#
# FULL_PLAYERS_DATA = getPandasFactoryDF(session, f'''select team_id, src_player_id, player_id, competition_name, season, player_name
# from {DB_NAME}.players;''')
# # Merge the DataFrames with indicator=True
# merged_df = pd.merge(FULL_PLAYERS_DATA, GET_PLAYERS_DATA, how='outer', indicator=True)
# missing_records = merged_df[merged_df['_merge'] == 'left_only']
# missing_records = missing_records.drop('_merge', axis=1)
#
# final_joined_df['player_image_url'] = (
#     (IMAGE_STORE_URL + 'players_images/' + final_joined_df['season'].astype(str) + '/' + final_joined_df[
#     'competition_name'] + '/' + final_joined_df['team_name'] + '/' + final_joined_df['player_name']).apply(
#     lambda x: x.replace(' ', '-')
#     .lower()).astype(str) + ".png")
#
# # missing_records['player_image_url'] = np.NAN
#
# for i in final_joined_df.to_dict(orient='records'):
#     player_id = i['player_id']
#     competition_name = i['competition_name']
#     season = i['season']
#     player_name = i['player_name']
#     src_player_id = i['src_player_id']
#     player_update_dict = dropFilter(
#         ['team_name', 'competition_name', 'team_id', 'player_id', 'season', 'player_name', 'src_player_id'], i)
#     players_set_values = getUpdateSetValues(player_update_dict)
#     players_update_sql = f"update {DB_NAME}.players set {  ', '.join(players_set_values)} where player_id={player_id} and competition_name='{competition_name}' and season={season} and player_name='{player_name}' and src_player_id='{src_player_id}' "
#     session.execute(players_update_sql)
#
# for j in missing_records.to_dict(orient='records'):
#     player_id = j['player_id']
#     competition_name = j['competition_name']
#     season = j['season']
#     player_name = j['player_name']
#     src_player_id = j['src_player_id']
#     # player_update_dict_null = dropFilter(
#     #     ['team_name', 'competition_name', 'team_id', 'player_id', 'season', 'player_name', 'src_player_id'], j)
#     # players_set_values_null = getUpdateSetValues(player_update_dict_null)
#     null_update_sql = f"update {DB_NAME}.players set player_image_url = NULL where player_id={player_id} and competition_name='{competition_name}' and season={season} and player_name='{player_name}' and src_player_id='{src_player_id}' "
#     session.execute(null_update_sql)

def check_player_image(image_name):
    print("--------------->", image_name)
    return block_blob_service.exists(container_name, f"players_images/{image_name}.png")
    print("------------------------------------")


for index, row in players.iterrows():

    def remove_extra_spaces(input_string):
        # Use a regular expression to replace multiple spaces with a single space
        cleaned_string = re.sub(r'\s+', ' ', input_string)
        return cleaned_string


    player_id = row['player_id']
    competition_name = row['competition_name']
    season = row['season']
    player_name = row['player_name']

    print("-------------> index : ", index)
    src_player_id = row['src_player_id']
    name = remove_extra_spaces(row['player_name']).strip()
    name = name.replace("  ", " ")
    name = name.replace(" ", "-").replace("'", "").replace("é", 'e')
    name = name.replace("ö", "o").replace("ç", "c").replace("ñ", "n").replace("ü", "u").replace("à", "a")
    image_name = src_player_id + "-" + name

    if check_player_image(image_name):
        print(f"checking {image_name} to blob storage @@@@@")
        # if player image not found upload to azure blob storage
        # block_blob_service.create_blob_from_path(
        #     container_name,
        #     f"players/{name}.png",
        #     f"/Users/achintya.chaudhary/Documents/projects/CricketDataService/boot_script/placeholder.png"
        # )
        image_url = f"{IMAGE_STORE_URL}" + 'players_images/' + f"{image_name}.png"
        image_update_sql = f"update {DB_NAME}.players set player_image_url = '{image_url}' where player_id={player_id} and competition_name='{competition_name}' and season={season} and player_name='{player_name}' and src_player_id='{src_player_id}' "
        session.execute(image_update_sql)
    else:
        image_url = ''
        # image_update_sql = f"update {DB_NAME}.players set player_image_url = NULL where player_id={player_id} and competition_name='{competition_name}' and season={season} and player_name='{player_name}' and src_player_id='{src_player_id}' "
        image_update_sql = f"update {DB_NAME}.players set player_image_url = '{image_url}' where player_id={player_id} and competition_name='{competition_name}' and season={season} and player_name='{player_name}' and src_player_id='{src_player_id}' "
        session.execute(image_update_sql)

# Example usage
#
# filename_from_dataframe = '<filename_to_check>'  # Make sure to have the correct filename from the dataframe
# exists = check_blob_existence(container_name, filename_from_dataframe)
#
# if exists:
#     print(f"The file {filename_from_dataframe} exists in the Azure Blob storage.")
# else:
#     print(f"The file {filename_from_dataframe} does not exist in the Azure Blob storage.")
