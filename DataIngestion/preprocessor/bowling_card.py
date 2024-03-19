import sys

from DataIngestion import load_timestamp

sys.path.append("./../../")
sys.path.append("./")
from log.log import get_logger
from DataIngestion.config import BOWL_CARD_TABLE_NAME, BOWL_CARD_KEY_COL, BOWL_CARD_REQD_COLS, PLAYER_MAPPER_TABLE_NAME
from common.dao.fetch_db_data import getPandasFactoryDF, getMaxId
from DataIngestion.query import GET_MATCHES_DETAILS_SQL, GET_PLAYER_DETAILS_SQL, GET_TEAM_SQL, GET_PLAYER_MAPPER_SQL
from DataIngestion.utils.helper import readJsFile, dataToDF, getRawDict, generateSeq, overs_calculator
import pandas as pd
from common.db_config import DB_NAME
import numpy as np

from common.dao_client import session

logger = get_logger("Ingestion", "Ingestion")


def getBowlingCardData(session, root_data_files, load_timestamp):
    logger.info("Bowler Card Data Generation Started!")
    if root_data_files:
        append_bowl_card_df = pd.DataFrame()

        max_key_val = getMaxId(session, BOWL_CARD_TABLE_NAME, BOWL_CARD_KEY_COL, DB_NAME)

        path_set = set((key.split("-")[1].split(".")[0].strip(), value) for key, value in root_data_files.items()
                       if 'innings' in key.split("-")[1].split(".")[0].strip().lower())

        for tupes in path_set:
            path_key = tupes[0]
            data_path = tupes[1]

            competition_name = data_path.split("/")[-3].split(" ")[0]
            season = data_path.split("/")[-2].split(" ")[1]

            bowling_raw_data = readJsFile(data_path)[path_key]['BowlingCard']

            for rawdata in bowling_raw_data:
                rawdata['competition_name'] = competition_name
                rawdata['season'] = season

            bowling_card_list = getRawDict(bowling_raw_data, BOWL_CARD_REQD_COLS)

            bowling_card_df = dataToDF(bowling_card_list)

            append_bowl_card_df = append_bowl_card_df.append(bowling_card_df, ignore_index=True)

        # if no new data is there, return
        if append_bowl_card_df.empty:
            logger.info("No New Bowling Match Data Available!")
            return
        # Getting match_id
        matches_df = getPandasFactoryDF(session, GET_MATCHES_DETAILS_SQL)

        append_bowl_card_df = pd.merge(append_bowl_card_df, matches_df[['match_id', 'src_match_id']],
                                       left_on=append_bowl_card_df['MatchID'], \
                                       right_on='src_match_id', how='left') \
            .drop(['MatchID', 'src_match_id'], axis=1)

        append_bowl_card_df = append_bowl_card_df.dropna(axis=0, subset=['match_id'])
        append_bowl_card_df['match_id'] = append_bowl_card_df['match_id'].astype(int)

        players_df = getPandasFactoryDF(session, GET_PLAYER_DETAILS_SQL).drop_duplicates()

        players_existing_df = getPandasFactoryDF(session, GET_PLAYER_MAPPER_SQL)
        append_bowl_card_df = pd.merge(
            players_existing_df[["id", "sports_mechanics_id"]],
            append_bowl_card_df,
            left_on='sports_mechanics_id',
            right_on='PlayerID',
            how='inner'
        ).drop(['sports_mechanics_id', 'PlayerID'], axis=1).rename(
            columns={
                'id': 'PlayerID'
            }
        )
        append_bowl_card_df['PlayerID'] = append_bowl_card_df['PlayerID'].astype(str)

        # Getting bowler_id
        append_bowl_card_df = pd.merge(
            append_bowl_card_df,
            players_df[['src_player_id', 'player_id']],
            left_on=append_bowl_card_df['PlayerID'],
            right_on=players_df['src_player_id'],
            how='left'
        ).rename(
            columns={'player_id': 'bowler_id'}
        ).drop(['key_0', 'PlayerName', 'PlayerID', 'src_player_id'], axis=1)

        # Getting bowler_team_id
        teams_df = getPandasFactoryDF(session, GET_TEAM_SQL)

        append_bowl_card_df = pd.merge(append_bowl_card_df, teams_df[['team_id', 'src_team_id']], how='left',
                                       left_on=append_bowl_card_df['TeamID']
                                       , right_on='src_team_id') \
            .drop(['TeamID', 'src_team_id'], axis=1)

        append_bowl_card_df[['bowler_id', 'team_id']] = append_bowl_card_df[['bowler_id', 'team_id']].fillna(-1).astype(
            int)

        append_bowl_card_df = append_bowl_card_df.where(pd.notnull(append_bowl_card_df), None).rename(
            columns={'InningsNo': 'innings', 'Overs': 'overs', 'Maidens': 'maidens',
                     'BowlingOrder': 'bowling_order', 'Runs': 'runs', 'NoBalls': 'no_balls', 'Wickets': 'wickets',
                     'Wides': 'wides', 'Economy': 'economy', 'DotBalls': 'dot_balls', 'Ones': 'ones', 'Twos': 'twos',
                     'Threes': 'threes', 'Fours': 'fours', 'Sixes': 'sixes',
                     'StrikeRate': 'strike_rate', 'TotalLegalBallsBowled': 'total_legal_balls'})

        append_bowl_card_df[
            ['sixes', 'dot_balls', 'threes', 'twos', 'runs', 'fours', 'ones', 'total_legal_balls', 'bowling_order']] = \
            append_bowl_card_df[
                ['sixes', 'dot_balls', 'threes', 'twos', 'runs', 'fours', 'ones', 'total_legal_balls', 'bowling_order']] \
                .fillna(0).astype(int)

        append_bowl_card_df['economy'] = pd.to_numeric(append_bowl_card_df['economy'].str.replace("-", "")).fillna(0.0)

        append_bowl_card_df["load_timestamp"] = load_timestamp

        bowl_card_final_data = generateSeq(
            append_bowl_card_df.drop_duplicates(['match_id', 'innings', 'team_id', 'bowler_id']) \
                .sort_values(['match_id', 'team_id', 'bowler_id']),
            BOWL_CARD_KEY_COL, max_key_val) \
            .to_dict(orient='records')
        logger.info("Bowler Card Data Generation Completed!")
        return bowl_card_final_data
    else:
        logger.info("No New Bowling Card Data Available!")


def cricsheet_bowling_card(ball_by_ball_df):
    matches_df = getPandasFactoryDF(session, GET_MATCHES_DETAILS_SQL)
    bowling_df = pd.merge(
        ball_by_ball_df,
        matches_df[['src_match_id', 'match_id']],
        on='src_match_id',
        how='left'
    ).drop('src_match_id', axis=1)

    # Get Dataframe of existing players from database
    teams_df = getPandasFactoryDF(session, GET_TEAM_SQL)[['team_id', 'team_name']]
    bowling_df = pd.merge(
        bowling_df,
        teams_df,
        left_on='bowling_team',
        right_on='team_name',
        how='left'
    )

    bowling_df = bowling_df[[
        'innings',
        'competition_name',
        'match_id',
        'season',
        'bowler_id',
        'team_id',
        'is_wicket',
        'is_no_ball',
        'is_wide',
        'ball_runs',
        'is_dot_ball',
        'is_one',
        'is_two',
        'is_three',
        'is_four',
        'is_six',
        'over_number',
        'ball_number',
        'bowling_order'
    ]]
    bowling_df = bowling_df.rename(
        columns={
            'is_dot_ball': 'dot_balls',
            'is_one': 'ones',
            'is_two': 'twos',
            'is_three': 'threes',
            'is_four': 'fours',
            'is_six': 'sixes',
            'is_wide': 'wides',
            'is_no_ball': 'no_balls',
            'is_wicket': 'wickets',
            'ball_runs': 'runs'
        }
    )
    grouped_bowler = bowling_df.groupby(
        ['bowler_id', 'competition_name', 'innings', 'team_id', 'match_id', 'season', 'over_number']
    ).aggregate({
        'ball_number': 'count',
        'wides': 'sum',
        'no_balls': 'sum',
        'runs': 'sum'
    }).reset_index()
    grouped_bowler['maidens'] = np.where(grouped_bowler['runs'] == 0, 1, 0)
    grouped_bowler = grouped_bowler.groupby('bowler_id').aggregate({
        'ball_number': 'sum',
        'wides': 'sum',
        'no_balls': 'sum',
        'maidens': 'sum'
    }).reset_index()
    grouped_bowler['total_legal_balls'] = grouped_bowler['ball_number'] - grouped_bowler['no_balls'] - grouped_bowler[
        'wides']

    grouped_bowler['overs'] = grouped_bowler['total_legal_balls'].apply(overs_calculator)
    bowling_df = bowling_df.groupby(
        ['bowler_id', 'competition_name', 'innings', 'team_id', 'match_id', 'season', 'bowling_order']
    ).aggregate({
        'fours': 'sum',
        'sixes': 'sum',
        'threes': 'sum',
        'twos': 'sum',
        'ones': 'sum',
        'runs': 'sum',
        'dot_balls': 'sum',
        'wides': 'sum',
        'wickets': 'sum',
        'no_balls': 'sum'
    }).reset_index()
    bowling_df = pd.merge(
        bowling_df,
        grouped_bowler[['bowler_id', 'maidens', 'overs', 'total_legal_balls']],
        on='bowler_id',
        how='left'
    )
    for key in ['sixes', 'dot_balls', 'threes', 'twos', 'runs', 'fours', 'ones']:
        bowling_df[key] = pd.to_numeric(bowling_df[key]).fillna(0).astype(int)
    bowling_df['economy'] = pd.to_numeric(bowling_df['runs'] / bowling_df['overs'])
    bowling_df['economy'] = bowling_df['economy'].replace(np.inf, 0)
    bowling_df['economy'] = bowling_df['economy'].round(2)
    bowling_df['economy'] = bowling_df['economy'].fillna(0)
    bowling_df['strike_rate'] = np.where(
        bowling_df['wickets'] != 0,
        bowling_df['total_legal_balls'] / bowling_df['wickets'],
        0
    )
    bowling_df['strike_rate'] = bowling_df['strike_rate'].round(2)
    bowling_df["load_timestamp"] = load_timestamp
    bowling_card = generateSeq(
        bowling_df,
        BOWL_CARD_KEY_COL,
        getMaxId(session, BOWL_CARD_TABLE_NAME, BOWL_CARD_KEY_COL, DB_NAME, False)
    ).to_dict(orient='records')
    return bowling_card
