import re
import sys

import numpy as np
import pandas as pd
import pandasql as psql

from DataService.utils.helper import connection_duckdb, defaulting_image_url
from common.duckdb.initial_ingestion_query import GET_PRESSURE_INDEX

sys.path.append("./../../")
sys.path.append("./")
sys.path.append("./../")

from log.log import get_logger

logger = get_logger("boot_script", "duck_flush_insert_module")


def ingest_duckdb(connector_object):
    from DataIngestion.config import IMAGE_STORE_URL
    from DataIngestion.utils.helper import readCSV
    from DataService.app_config import TOP_TEAMS_FILE_PATH, MAPPING_FILE_PATH
    from common.duckdb.initial_ingestion_query import GET_SEASONS_DATA, GET_MATCHES_DATA, GET_PLAYERS_DATA, \
        GET_BALL_SUMMARY_DATA, GET_BATCARD_DATA, GET_EXTRAS_DATA, GET_BOWL_CARD_DATA, GET_VENUE_DATA, \
        GET_PARTNERSHIP_DATA, \
        JOIN_DATA_SQL, TEAMS_AGGREGATED_SQL, PARTNERSHIP_SQL, BOWLER_OVERWISE_SQL, BATSMAN_OVERWISE_SQL, \
        OVERWISE_BOWLING_ORDER, POSITIONWISE_BOWLER_OVER, POSITIONWISE_BATSMAN_OVER, MATCH_PLAYER_SQL, \
        MATCHES_JOIN_DATA, \
        BATSMAN_STATS, BOWLER_STATS, GET_TEAMS_DATA, GET_AGG_CONTRIBUTION_DATA, GET_CONTRIBUTION_SCORE_DATA

    from DataService.utils.helper import registerDF, executeQuery
    from common.dao.fetch_db_data import getPandasFactoryDF
    from common.dao_client import session

    latest_team = getPandasFactoryDF(session, GET_TEAMS_DATA)
    registerDF(connector_object, latest_team, "teams_data_vw", "teams_data")

    seasons_data = getPandasFactoryDF(session, GET_SEASONS_DATA)
    registerDF(connector_object, seasons_data, "seasons_data_vw", "seasons_data")

    matches_data = getPandasFactoryDF(session, GET_MATCHES_DATA)
    match_names = matches_data['match_name'].to_list()
    competition_names = matches_data['competition_name'].drop_duplicates().to_list()
    matches_data[[
        'team1_overs', 'team2_overs'
    ]] = matches_data[[
        'team1_overs', 'team2_overs'
    ]].apply(pd.to_numeric, errors='coerce')
    registerDF(connector_object, matches_data, "matches_df_vw", "matches_df")

    players_data = getPandasFactoryDF(session, GET_PLAYERS_DATA)
    # Define the pattern to match the desired part of the URL
    pattern = rf'{re.escape(IMAGE_STORE_URL)}(.*?)\/[^/]+$'
    # Find the part of the URL matching the pattern
    match = re.search(pattern, players_data['player_image_url'].iloc[0])
    folder_name = match.group(1)
    logger.info(f"initial_ingestion.py image folder_name: {folder_name}")
    defaulting_image_url(players_data, 'player_image_url', 'competition_name', 'WPL', folder_name)
    logger.info("initial_ingestion.py image defaulting completed")
    players_data_df = players_data.copy()
    registerDF(connector_object, players_data, "players_data_vw", "players_data")

    if not players_data_df.empty:
        players_data_df["player_rank"] = players_data_df.groupby(["player_id", "competition_name"])[
            "season"].rank(method="first", ascending=False)
        players_data_df = players_data_df[players_data_df["player_rank"] == 1]
        registerDF(connector_object, players_data_df, "players_data_df_vw", "players_data_df")

    ball_summary_df = []
    logger.info("Loading match ball summary in chucks.")
    for competition in competition_names:
        logger.info(f"Processing for {competition}")
        ball_summary_df.append(getPandasFactoryDF(
            session,
            f"{GET_BALL_SUMMARY_DATA}  where competition_name = '{competition}' ALLOW FILTERING;")
        )
    ball_summary_df = pd.concat(ball_summary_df, ignore_index=True)
    ball_summary_df[[
        'x_pitch', 'y_pitch'
    ]] = ball_summary_df[[
        'x_pitch', 'y_pitch'
    ]].apply(pd.to_numeric, errors='coerce')
    registerDF(connector_object, ball_summary_df, "ball_summary_df_vw", "ball_summary_df")

    # Processing data from Match Batting Card
    bat_card_data = []
    logger.info("Loading match batting card in chucks.")
    for competition in competition_names:
        logger.info(f"Processing for {competition}")
        bat_card_data.append(getPandasFactoryDF(
            session,
            f"{GET_BATCARD_DATA}  where competition_name = '{competition}' ALLOW FILTERING;")
        )
    bat_card_data = pd.concat(bat_card_data, ignore_index=True)

    registerDF(connector_object, bat_card_data, "bat_card_data_vw", "bat_card_data")

    extras_data = getPandasFactoryDF(session, GET_EXTRAS_DATA)
    registerDF(connector_object, extras_data, "extras_data_vw", "extras_data")

    # Processing data from Match Batting Card
    bowl_card_data = []
    logger.info("Loading match bowling card in chucks.")
    for competition in competition_names:
        logger.info(f"Processing for {competition}")
        bowl_card_data.append(getPandasFactoryDF(
            session,
            f"{GET_BOWL_CARD_DATA}  where competition_name = '{competition}' ALLOW FILTERING;")
        )
    bowl_card_data = pd.concat(bowl_card_data, ignore_index=True)

    bowl_card_data[[
        'overs', 'economy'
    ]] = bowl_card_data[[
        'overs', 'economy'
    ]].apply(pd.to_numeric, errors='coerce')
    registerDF(connector_object, bowl_card_data, "bowl_card_data_vw", "bowl_card_data")

    venue_data = getPandasFactoryDF(session, GET_VENUE_DATA)
    registerDF(connector_object, venue_data, "venue_data_vw", "venue_data")

    partnership_base_data = getPandasFactoryDF(session, GET_PARTNERSHIP_DATA)
    registerDF(connector_object, partnership_base_data, "partnership_base_data_vw", "partnership_base_data")

    pi_data = []
    logger.info("Loading PI in chucks.")
    start = 1
    end = 20000
    while True:
        logger.info(f"fetching data from {start} : {end}")
        data_c = getPandasFactoryDF(
            session,
            f"{GET_PRESSURE_INDEX}  where id >= {start} AND id < {end} ALLOW FILTERING;"
        )
        if not data_c.empty:
            pi_data.append(data_c)
        else:
            break
        start = end
        end += 20000
    pi_data = pd.concat(pi_data, ignore_index=True)
    pi_data = pi_data[pi_data['is_striker'] == 1]
    registerDF(connector_object, pi_data, "pi_data_vw", "pi_data")

    join_data = executeQuery(connector_object, JOIN_DATA_SQL)
    registerDF(connector_object, join_data, "join_data_vw", "join_data")

    teams_aggregated = executeQuery(connector_object, TEAMS_AGGREGATED_SQL)
    if not teams_aggregated.empty:
        registerDF(connector_object, teams_aggregated, "teams_aggregated_vw", "teams_aggregated_data")

    partnership_data = executeQuery(connector_object, PARTNERSHIP_SQL)
    if not partnership_data.empty:
        registerDF(connector_object, partnership_data, "partnership_data_vw", "partnership_data")

    bowler_overwise_df = executeQuery(connector_object, BOWLER_OVERWISE_SQL)
    bowler_overwise_df['player_image_url'] = bowler_overwise_df['player_image_url'].fillna(
        IMAGE_STORE_URL + '2023/placeholder.png')
    if not bowler_overwise_df.empty:
        registerDF(connector_object, bowler_overwise_df, "bowler_overwise_df_vw", "bowler_overwise_df")

    batsman_overwise_df = executeQuery(connector_object, BATSMAN_OVERWISE_SQL)
    batsman_overwise_df['player_image_url'] = batsman_overwise_df['player_image_url'].fillna(
        IMAGE_STORE_URL + '2023/placeholder.png')
    if not batsman_overwise_df.empty:
        registerDF(connector_object, batsman_overwise_df, "batsman_overwise_df_vw", "batsman_overwise_df")

    bowling_order_df = executeQuery(connector_object, OVERWISE_BOWLING_ORDER)
    if not bowling_order_df.empty:
        registerDF(connector_object, bowling_order_df, "bowling_order_df_vw", "bowling_order_df")

    bowler_positionwise_df = executeQuery(connector_object, POSITIONWISE_BOWLER_OVER)
    if not bowler_positionwise_df.empty:
        registerDF(connector_object, bowler_positionwise_df, "bowler_positionwise_df_vw", "bowler_positionwise_df")

    batsman_positionwise_df = executeQuery(connector_object, POSITIONWISE_BATSMAN_OVER)
    if not batsman_positionwise_df.empty:
        registerDF(connector_object, batsman_positionwise_df, "batsman_positionwise_df_vw", "batsman_positionwise_df")

    match_data_df1 = getPandasFactoryDF(session, MATCH_PLAYER_SQL).explode('team1_players').drop(
        'team2_players', axis=1
    )

    match_data_df2 = getPandasFactoryDF(session, MATCH_PLAYER_SQL).explode('team2_players').drop(
        'team1_players', axis=1
    )

    match_data1 = psql.sqldf('''
        select 
          match_id, 
          season, 
          1 as innings, 
          team1, 
          team1_players as player_id, 
          venue, 
          match_date, 
          match_time, 
          case when team1 = winning_team then 'Winning' when winning_team =-1 then 'No Result' else 'Losing' end as winning_type, 
          team2, 
          case when team1 = winning_team then 'WIN' when winning_team =-1 then 'NO RESULT' else 'LOSS' end as match_decision, 
          competition_name 
        from 
          match_data_df1
    ''')

    match_data2 = psql.sqldf('''
        select 
          match_id, 
          season, 
          2 as innings, 
          team2 as team1, 
          team2_players as player_id, 
          venue, 
          match_date, 
          match_time, 
          case when team2 = winning_team then 'Winning' when winning_team =-1 then 'No Result' else 'Losing' end as winning_type, 
          team1 as team2, 
          case when team2 = winning_team then 'WIN' when winning_team =-1 then 'NO RESULT' else 'LOSS' end as match_decision, 
          competition_name 
        from 
          match_data_df2
    ''')

    match_playing_xi_data = match_data1.append(match_data2, ignore_index=True)
    if not match_playing_xi_data.empty:
        registerDF(
            connector_object,
            match_playing_xi_data,
            "match_playing_xi_data_vw",
            "match_playing_xi_data",
            register_once=False
        )

    # GET MATCHES JOINED DATA
    matches_join_data = executeQuery(connector_object, MATCHES_JOIN_DATA)
    if not matches_join_data.empty:
        registerDF(connector_object, matches_join_data, "matches_join_data_vw", "matches_join_data")

    mapping_data = readCSV(MAPPING_FILE_PATH).rename(columns={'Player': 'player_name'})

    batsman_stats_data = executeQuery(connector_object, BATSMAN_STATS)
    if not batsman_stats_data.empty:
        registerDF(connector_object, batsman_stats_data, "batsman_stats_data_vw", "batsman_stats_data")

    bowler_stats_data = executeQuery(connector_object, BOWLER_STATS)
    bowler_stats_data = bowler_stats_data.merge(
        mapping_data[['player_name', 'Bowling_type']],
        on='player_name',
        how='left'
    )
    bowler_stats_data['bowling_type'] = np.where(
        ((bowler_stats_data['bowling_type'] == "NA") & (~bowler_stats_data['Bowling_type'].isnull())),
        bowler_stats_data['Bowling_type'], bowler_stats_data['bowling_type'])
    bowler_stats_data = bowler_stats_data.drop(['Bowling_type'], axis=1)
    if not bowler_stats_data.empty:
        registerDF(connector_object, bowler_stats_data, "bowler_stats_data_vw", "bowler_stats_data")

    contribution_data = getPandasFactoryDF(session, GET_CONTRIBUTION_SCORE_DATA)
    col_list = [
        'overall_powerplay_contribution_score',
        'overall_7_10_overs_contribution_score',
        'overall_11_15_overs_contribution_score',
        'overall_deathovers_contribution_score',
        'overall_consistency_score',
        'overall_contribution_score',
        'bowl_powerplay_contribution_score',
        'bowl_7_10_overs_contribution_score',
        'bowl_11_15_overs_contribution_score',
        'bowl_deathovers_contribution_score',
        'bowling_contribution_score',
        'bowling_consistency_score',
        'bat_powerplay_contribution_score',
        'bat_7_10_overs_contribution_score',
        'bowl_innings',
        'bat_11_15_overs_contribution_score',
        'bat_deathovers_contribution_score',
        'wickets_taken',
        'batting_contribution_score',
        'batting_consistency_score',
        'runs_scored',
        'bat_innings',
        'actual_powerplay_over_runs',
        'actual_7_10_over_runs',
        'actual_11_15_over_runs',
        'actual_death_over_runs',
        'balls_faced',
        'batting_strike_rate',
        'runs_conceded',
        'total_overs_bowled',
        'overall_fours',
        'overall_sixes',
        'fow_during_stay',
        'non_striker_runs',
        'total_balls_bowled',
        'arrived_on',
        'actual_powerplay_over_balls',
        'actual_7_10_over_balls',
        'actual_11_15_over_balls',
        'actual_death_over_balls',
    ]

    for col in col_list:
        contribution_data[col] = contribution_data[col].apply(lambda x: round(x) if x != 9999 else np.NaN)

    contribution_data[['overall_economy', 'batting_strike_rate', 'total_overs_bowled']] = contribution_data[[
        'overall_economy', 'batting_strike_rate', 'total_overs_bowled']].apply(
        pd.to_numeric, errors='coerce'
    )
    if not contribution_data.empty:
        registerDF(connector_object, contribution_data, "contribution_data_vw", "contribution_data")

    contribution_agg_data = executeQuery(connector_object, GET_AGG_CONTRIBUTION_DATA)
    if not contribution_agg_data.empty:
        registerDF(connector_object, contribution_agg_data, "contribution_agg_data_vw", "contribution_agg_data")

    top_players_data = readCSV(TOP_TEAMS_FILE_PATH).drop("player_id", axis=1)
    if not top_players_data.empty:
        registerDF(connector_object, top_players_data, "top_players_data_vw", "top_players_data", register_once=True)


def create_duckdb():
    logger.info('Flushing and Registering Duck db Initiated')
    # create connection
    con = connection_duckdb()
    # insert data to duckdb
    ingest_duckdb(con)
    # close connection
    con.close()
    logger.info('Flushing and Registering Duck db Finished! ')
