import json
import os
import re
import sys
from datetime import datetime
from logging import Logger

import pandas as pd

from DataIngestion import config

sys.path.append("./../../")
sys.path.append("./")

from DataIngestion.config import IMAGE_STORE_URL, DUCK_DB_PATH
from DataService.fetch_sql_queries import GET_BATCARD_DATA, GET_BOWL_CARD_DATA, MATCH_PLAYING_XI, MATCHES_JOIN_DATA, \
    BATSMAN_OVERWISE_SQL, BOWLER_OVERWISE_SQL, GET_SEASON_TEAMS_DATA, JOIN_DATA_SQL, GET_TEAMS_DATA, GET_PLAYERS_DATA
from DataService.utils.helper import executeQuery, connection_duckdb, defaulting_image_url
from common.duckdb.initial_ingestion import create_duckdb
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session
from log.log import get_logger

logger = get_logger("app", "__init__")


class DuckDF:
    initiated: bool = False
    bat_card_data = None
    bowl_card_data = None
    con = None
    logger: Logger
    match_playing_xi = None
    matches_join_data = None
    mi_ball_data = None
    players_data_df = None
    teams_data = None
    teams_season_data = None
    batsman_overwise_df = None
    players_data = None
    mi_bat_data = None
    bowler_overwise_df = None

    @staticmethod
    def init(force=False):
        # Check if the blob exists
        duckdb_exists = os.path.exists(DUCK_DB_PATH)
        # check for duckdb config, if db reload is required.
        with open(config.DUCK_DB_CONFIG_PATH, "r") as f:
            data = json.load(f)
            duck_db_reload = data["duck_db_reload"]

        if duck_db_reload or not duckdb_exists:
            # for check of duck_db_reload, if file exists delete it first, and then create new db.
            if duckdb_exists:
                os.chmod(DUCK_DB_PATH, 0o777)
                os.remove(DUCK_DB_PATH)
            # In either case, create new duck db and do ingestion for all the data
            create_duckdb()

        DuckDF.logger = get_logger("root", "app")
        if force:
            DuckDF.logger.info("Force recreating DF.")
            DuckDF.initiated = False
        elif DuckDF.initiated:
            DuckDF.logger.info("Already completed DF registration.")
            return

        DuckDF.con = connection_duckdb()
        DuckDF.logger.debug(DuckDF.con)

        DuckDF.teams_data = getPandasFactoryDF(session, GET_TEAMS_DATA)

        DuckDF.players_data = getPandasFactoryDF(session, GET_PLAYERS_DATA)
        # Define the pattern to match the desired part of the URL
        pattern = rf'{re.escape(IMAGE_STORE_URL)}(.*?)\/[^/]+$'
        # Find the part of the URL matching the pattern
        match = re.search(pattern, DuckDF.players_data['player_image_url'].iloc[0])
        folder_name = match.group(1)
        logger.info(f"init.py image folder_name: {folder_name}")
        defaulting_image_url(DuckDF.players_data, 'player_image_url', 'competition_name', 'WPL', folder_name)
        logger.info("init.py image defaulting completed")

        DuckDF.players_data_df = DuckDF.players_data.copy()
        DuckDF.players_data_df["player_rank"] = DuckDF.players_data_df.groupby(
            ["player_id", "competition_name"])["season"].rank(method="first", ascending=False)
        DuckDF.players_data_df = DuckDF.players_data_df[DuckDF.players_data_df["player_rank"] == 1]

        DuckDF.bat_card_data = getPandasFactoryDF(session, GET_BATCARD_DATA)

        DuckDF.bowl_card_data = getPandasFactoryDF(session, GET_BOWL_CARD_DATA)
        DuckDF.bowl_card_data[[
            'overs', 'economy'
        ]] = DuckDF.bowl_card_data[[
            'overs', 'economy'
        ]].apply(pd.to_numeric, errors='coerce')

        DuckDF.teams_season_data = getPandasFactoryDF(session, GET_SEASON_TEAMS_DATA).rename(
            columns={"seasons_played": "season"})

        DuckDF.bowler_overwise_df = executeQuery(DuckDF.con, BOWLER_OVERWISE_SQL)
        DuckDF.bowler_overwise_df['player_image_url'] = DuckDF.bowler_overwise_df['player_image_url'].fillna(
            IMAGE_STORE_URL + '2023/placeholder.png')

        DuckDF.batsman_overwise_df = executeQuery(DuckDF.con, BATSMAN_OVERWISE_SQL)
        DuckDF.batsman_overwise_df['player_image_url'] = DuckDF.batsman_overwise_df['player_image_url'].fillna(
            IMAGE_STORE_URL + '2023/placeholder.png')

        DuckDF.match_playing_xi = executeQuery(DuckDF.con, MATCH_PLAYING_XI)

        DuckDF.matches_join_data = executeQuery(DuckDF.con, MATCHES_JOIN_DATA)

        join_data = executeQuery(DuckDF.con, JOIN_DATA_SQL)
        DuckDF.mi_ball_data = join_data[(join_data['bowler_team_name'].isin(
            ["MUMBAI INDIANS", "MI EMIRATES", "MI NEW YORK", "MI CAPE TOWN", "MUMBAI INDIANS WOMEN"]))] \
            [['id', 'match_id', 'match_name', 'src_bowler_id', 'bowler_id', 'bowler_name', 'ball_number',
              'over_number', 'bowl_length', 'match_date', 'match_date_form', 'season', 'bowler_image_url']]
        DuckDF.mi_ball_data['match_date_form'] = DuckDF.mi_ball_data['match_date_form'].apply(
            lambda x: datetime.strptime(x, '%Y-%m-%d').date())
        DuckDF.mi_bat_data = join_data[(join_data['batsman_team_name'].isin(
            ["MUMBAI INDIANS", "MI EMIRATES", "MI NEW YORK", "MI CAPE TOWN", "MUMBAI INDIANS WOMEN"]))] \
            [['id', 'match_id', 'match_name', 'src_batsman_id', 'batsman_id', 'batsman_name', 'ball_number',
              'over_number', 'bowl_length', 'match_date', 'match_date_form', 'season', 'batsman_image_url']]
        DuckDF.mi_bat_data['match_date_form'] = DuckDF.mi_bat_data['match_date_form'].apply(
            lambda x: datetime.strptime(x, '%Y-%m-%d').date())

        DuckDF.initiated = True
        logger.info('Loaded Duck DF into Memory')


DuckDF.init(force=True)
con = DuckDF.con
mi_ball_data = DuckDF.mi_ball_data
mi_bat_data = DuckDF.mi_bat_data
match_playing_xi_df = DuckDF.match_playing_xi
matches_join_data = DuckDF.matches_join_data
bowl_card_data = DuckDF.bowl_card_data
bat_card_data = DuckDF.bat_card_data
teams_data = DuckDF.teams_data
teams_season_data = DuckDF.teams_season_data
players_data_df = DuckDF.players_data_df
match_playing_xi = DuckDF.match_playing_xi
batsman_overwise_df = DuckDF.batsman_overwise_df
bowler_overwise_df = DuckDF.bowler_overwise_df
