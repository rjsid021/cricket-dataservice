from DataIngestion.config import PEAK_LOAD_TABLE_NAME, PLAYER_LOAD_TABLE_NAME, DAILY_ACTIVITY_TABLE_NAME, \
    BOWL_PLANNING_TABLE_NAME
from DataIngestion.preprocessor.fitness_gps_data import playerLoadData
from DataIngestion.utils.helper import readCSV
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao.insert_data import insertToDB
from common.dao_client import session
from common.db_config import DB_NAME
from common.utils.helper import getPrettyDF
import pandas as pd

from log.log import get_logger

# flags
logger = get_logger("Update Wellness History", "Update Wellness History")
update_peak_load = True
update_fitness_form = True
update_bowl_planning = True

player_mapping_df = getPandasFactoryDF(session, f'''select id as player_id, name, catapult_id from {DB_NAME}.playermapping''')
player_mapping_df = player_mapping_df[~player_mapping_df["catapult_id"].isnull()]


if update_peak_load:
    logger.info("Updating Match Peak Load!!")
    # read updated peak_load_data
    peak_load_df = getPandasFactoryDF(session, f'''select * from {DB_NAME}.{PEAK_LOAD_TABLE_NAME}''')
    peak_load_df["match_peak_load"] = peak_load_df["match_peak_load"].apply(pd.to_numeric, errors='coerce')

    peak_load_df = (peak_load_df.merge(player_mapping_df, left_on="player_name", right_on="name", how="left")
                    .drop(["player_id_x","name", "catapult_id"], axis=1).rename(columns={"player_id_y": "player_id"}))
    # print(getPrettyDF(peak_load_df))
    insertToDB(session, peak_load_df.to_dict(orient='records'), DB_NAME, PEAK_LOAD_TABLE_NAME)

if update_fitness_form:
    logger.info("Updating Fitness Form History!!")
    fitness_form_df = readCSV("/Users/siddharth.nautiyal/Desktop/cricket/GIT/git-master/CricketDataService/data/updated_form_data/fitness_form_updated.csv")
    fitness_form_df["player_id"] = fitness_form_df["player_id"].fillna(-1).astype(int)
    insertToDB(session, fitness_form_df.fillna("").drop(["name"], axis=1).to_dict(orient='records'), DB_NAME, DAILY_ACTIVITY_TABLE_NAME)
    player_load_data = playerLoadData(fitness_form_df)
    insertToDB(session, player_load_data, DB_NAME, PLAYER_LOAD_TABLE_NAME)


if update_bowl_planning:
    logger.info("Updating Bowl Planning History!!")
    planning_df = getPandasFactoryDF(session, f'''select * from {DB_NAME}.{BOWL_PLANNING_TABLE_NAME}''')

    planning_df = (planning_df.merge(player_mapping_df, how="left", left_on="player_name", right_on="name")
                   .rename(columns={"player_id_y": "player_id"}).drop(["name", "catapult_id", "player_id_x"], axis=1))
    # print(getPrettyDF(planning_df))
    insertToDB(session, planning_df.to_dict(orient="records"), DB_NAME, BOWL_PLANNING_TABLE_NAME)