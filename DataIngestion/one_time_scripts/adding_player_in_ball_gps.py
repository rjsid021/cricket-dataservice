import pandas as pd
import numpy as np

from DataIngestion.config import GPS_DELIVERY_TABLE_NAME, GPS_BOWLING_2021_TRENT_BOULT
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session
from common.dao.insert_data import insertToDB
from common.db_config import DB_NAME
from datetime import datetime, timedelta

player_gps_df = pd.read_excel(GPS_BOWLING_2021_TRENT_BOULT).drop("player_id", axis=1)
player_mapping_df = getPandasFactoryDF(session, f'''select id as player_id, name, catapult_id as athlete_id from {DB_NAME}.playermapping''')
player_mapping_df = player_mapping_df[~player_mapping_df["athlete_id"].isnull()]

player_gps_df = player_gps_df.merge(player_mapping_df, left_on="player_name", right_on="name", how="left").drop(["name"], axis=1)

player_gps_df = player_gps_df.dropna(how='all')
player_gps_df['is_match_fielding'] = np.where(
    ((player_gps_df['activity_name'].str.contains('Match') | player_gps_df['activity_name'].str.contains('Game'))
     & (~player_gps_df['activity_name'].str.contains('Practice')) & (
         player_gps_df['period_name'].str.contains('Fielding'))), 1, 0)

player_gps_df['athlete_id'] = '8861b593-f392-448e-ac4c-af034763e3c1'
player_gps_df['ball_no'] = player_gps_df['ball_no'].apply(lambda x: int(x))
player_gps_df['season'] = player_gps_df['season'].apply(lambda x: int(x))
player_gps_df['delivery_time'] = player_gps_df['delivery_time'].apply(lambda x: int(x))
player_gps_df['avg_delivery_count'] = player_gps_df['avg_delivery_count'].apply(lambda x: int(x))
player_gps_df['player_id'] = player_gps_df['player_id'].apply(lambda x: int(x))

player_gps_df['total_delivery_count'] = player_gps_df['total_delivery_count'].apply(lambda x: int(x))


# Define a function to convert Excel serial numbers to dates
def excel_to_date(serial_num):
    # Excel serial date starts from 1900-01-01
    base_date = datetime(1899, 12, 30)
    # Calculate the number of days from the base date
    delta = timedelta(days=serial_num)
    # Add the number of days to the base date
    return base_date + delta


# Apply the function to the relevant column in the DataFrame
player_gps_df['date_name'] = player_gps_df['date_name'].apply(excel_to_date).astype(str)
load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
player_gps_df["load_timestamp"] = load_timestamp
player_gps_df = player_gps_df.to_dict(orient='records')

if len(player_gps_df) > 0:
    insertToDB(session, player_gps_df, DB_NAME, GPS_DELIVERY_TABLE_NAME)
