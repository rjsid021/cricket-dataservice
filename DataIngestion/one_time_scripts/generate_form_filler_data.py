'''Script to generate data from the fitness form to see who has/hasn't filled the form for the day'''

from DataIngestion.config import DAILY_ACTIVITY_TABLE_NAME
from DataIngestion.smartabase import getPrettyDF
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session
from common.db_config import DB_NAME
import numpy as np
import pandas as pd

season = 2024
competition_name = 'WPL'
team_id = 32


FITNESS_FORM_SQL = f'''select player_name, record_date, form_filler from 
         {DB_NAME}.{DAILY_ACTIVITY_TABLE_NAME} where team_name='Mumbai Indian Womens' ALLOW FILTERING;'''

fitness_form_df = getPandasFactoryDF(session, FITNESS_FORM_SQL)
players_df = getPandasFactoryDF(session, f'''select * from {DB_NAME}.players where season={season} and competition_name='{competition_name}' and team_id={team_id} ALLOW FILTERING; ''')[["player_name"]]
fitness_form_df['player_name_lower'] = fitness_form_df['player_name'].str.lower()
players_df['player_name_lower'] = players_df['player_name'].str.lower()
# print(getPrettyDF(fitness_form_df))
#
print(getPrettyDF(players_df))
final_df = pd.DataFrame()
record_date_list = list(fitness_form_df["record_date"].unique())
for dt in record_date_list:
    fitness_form_data = fitness_form_df[fitness_form_df["record_date"] == dt]
    # print(getPrettyDF(fitness_form_data))

    players_temp_df = players_df.merge(fitness_form_data, how="left", on="player_name_lower").drop(["player_name_lower", "player_name_y"], axis=1).rename(columns={"player_name_x": "player_name"})
    players_temp_df["record_date"] = players_temp_df["record_date"].fillna(dt)
    players_temp_df["form_filler"] = players_temp_df["form_filler"].fillna("NA")
    final_df = final_df.append(players_temp_df)



final_df["form_filled_for_the_day"] = np.where(final_df["form_filler"]=="NA", "No", "Yes")

form_filler_conditions = [
    (final_df['form_filler'] == "NA"),
    (final_df['form_filler'].str.lower() == "me"),
    (final_df['form_filler'].str.lower() == "staff")
]

# Different batting phases
filler_values = ["NA", "Player", "Staff"]


final_df["form_filled_by"] =  np.select(form_filler_conditions, filler_values)
final_df[["record_date", "player_name", "form_filled_for_the_day", "form_filled_by"]].to_csv("fitness_form_details.csv", index=False)