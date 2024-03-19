import sys

from DataIngestion import load_timestamp

sys.path.append("./../../")
sys.path.append("./")
from log.log import get_logger
from DataIngestion.config import BAT_CARD_TABLE_NAME, BAT_CARD_KEY_COL, BAT_CARD_REQD_COLS
from common.dao.fetch_db_data import getPandasFactoryDF, getMaxId
from DataIngestion.query import GET_MATCHES_DETAILS_SQL, GET_PLAYER_DETAILS_SQL, GET_TEAM_SQL, GET_PLAYER_MAPPER_SQL
from common.db_config import DB_NAME
from DataIngestion.utils.helper import readJsFile, dataToDF, getRawDict, generateSeq
import pandas as pd
from common.dao_client import session

logger = get_logger("Ingestion", "Ingestion")


def getBattingCardData(session, root_data_files, load_timestamp):
    logger.info("Batting Card Data Generation Started!")
    if root_data_files:
        append_bat_card_df = pd.DataFrame()

        max_key_val = getMaxId(session, BAT_CARD_TABLE_NAME, BAT_CARD_KEY_COL, DB_NAME)

        path_set = set((key.split("-")[1].split(".")[0].strip(), value) for key, value in root_data_files.items()
                       if 'innings' in key.split("-")[1].split(".")[0].strip().lower())

        for tupes in path_set:
            path_key = tupes[0]
            data_path = tupes[1]

            competition_name = data_path.split("/")[-3].split(" ")[0]
            season = data_path.split("/")[-2].split(" ")[1]

            batting_raw_data = readJsFile(data_path)[path_key]['BattingCard']

            for rawdata in batting_raw_data:
                rawdata['competition_name'] = competition_name
                rawdata['season'] = season

            batting_card_list = getRawDict(batting_raw_data, BAT_CARD_REQD_COLS)

            batting_card_df = dataToDF(batting_card_list)

            batting_card_df['PlayingOrder'] = batting_card_df.index + 1

            append_bat_card_df = append_bat_card_df.append(batting_card_df, ignore_index=True)

        # if no new data is there, return
        if append_bat_card_df.empty:
            logger.info("No New Batting Match Data Available!")
            return
        matches_df = getPandasFactoryDF(session, GET_MATCHES_DETAILS_SQL)

        append_bat_card_df = pd.merge(append_bat_card_df, matches_df[['match_id', 'src_match_id']],
                                      left_on=append_bat_card_df['MatchID'], \
                                      right_on='src_match_id', how='left') \
            .drop(['MatchID', 'src_match_id'], axis=1)
        append_bat_card_df = append_bat_card_df.dropna(axis=0, subset=['match_id'])
        append_bat_card_df['match_id'] = append_bat_card_df['match_id'].astype(int)

        players_df = getPandasFactoryDF(session, GET_PLAYER_DETAILS_SQL).drop_duplicates()
        players_existing_df = getPandasFactoryDF(session, GET_PLAYER_MAPPER_SQL)
        append_bat_card_df = pd.merge(
            players_existing_df[["id", "sports_mechanics_id"]],
            append_bat_card_df,
            left_on='sports_mechanics_id',
            right_on='PlayerID',
            how='inner'
        ).drop(['sports_mechanics_id', 'PlayerID'], axis=1).rename(
            columns={
                'id': 'PlayerID'
            }
        )
        append_bat_card_df['PlayerID'] = append_bat_card_df['PlayerID'].astype(str)
        # Getting batsman_id
        append_bat_card_df = pd.merge(
            append_bat_card_df,
            players_df,
            left_on=append_bat_card_df['PlayerID'],
            right_on=players_df['src_player_id'],
            how='left'
        ).rename(
            columns={'player_id': 'batsman_id'}
        ).drop(['key_0', 'PlayerName', 'PlayerID', 'src_player_id', 'player_name'], axis=1)

        append_bat_card_df['bowler_id'] = -1

        # Getting batsman_team_id
        teams_df = getPandasFactoryDF(session, GET_TEAM_SQL)

        append_bat_card_df = pd.merge(append_bat_card_df, teams_df[['team_id', 'src_team_id']], how='left',
                                      left_on=append_bat_card_df['TeamID']
                                      , right_on='src_team_id') \
            .rename(columns={'team_id': 'batting_team_id'}).drop(['TeamID', 'src_team_id'], axis=1)

        append_bat_card_df[['bowler_id', 'batsman_id', 'batting_team_id']] = \
            append_bat_card_df[['bowler_id', 'batsman_id', 'batting_team_id']].fillna(-1).astype(int)

        append_bat_card_df['OutDesc'] = append_bat_card_df['OutDesc'].apply(
            lambda x: x.replace("'", "").replace('Akshar Patel', 'Axar Patel')).replace("",
                                                                                        "NA")

        append_bat_card_df['StrikeRate'] = pd.to_numeric(append_bat_card_df['StrikeRate'].str.replace("-", "")).fillna(
            0.0)

        append_bat_card_df["load_timestamp"] = load_timestamp

        append_bat_card_df = append_bat_card_df.rename(
            columns={'InningsNo': 'innings', 'PlayingOrder': 'batting_position',
                     'OutDesc': 'out_desc', 'Runs': 'runs', 'Balls': 'balls',
                     'DotBalls': 'dot_balls', 'Ones': 'ones', 'Twos': 'twos',
                     'Threes': 'threes', 'Fours': 'fours', 'Sixes': 'sixes',
                     'StrikeRate': 'strike_rate'})

        for key in ['sixes', 'dot_balls', 'threes', 'balls', 'twos', 'runs', 'fours', 'ones']:
            append_bat_card_df[key] = pd.to_numeric(append_bat_card_df[key]).fillna(0).astype(int)

        bat_card_final_data = generateSeq(
            append_bat_card_df.drop_duplicates(['match_id', 'batsman_id', 'bowler_id', 'innings']) \
                .sort_values(['match_id', 'batting_team_id', 'batsman_id', 'bowler_id']), BAT_CARD_KEY_COL, max_key_val) \
            .to_dict(orient='records')

        logger.info("Batting Card Data Generation Completed!")
        return bat_card_final_data

    else:
        logger.info("No New Teams Data Available!")


def get_cricsheet_batting_card(ball_by_ball_df):
    max_key_val = getMaxId(session, BAT_CARD_TABLE_NAME, BAT_CARD_KEY_COL, DB_NAME, False)
    matches_df = getPandasFactoryDF(session, GET_MATCHES_DETAILS_SQL)
    # Merge with Matches to get match id.
    ball_by_ball_df = pd.merge(
        ball_by_ball_df,
        matches_df[['match_id', 'src_match_id']],
        left_on=ball_by_ball_df['src_match_id'],
        right_on='src_match_id',
        how='left'
    ).drop(
        ['src_match_id_x', 'src_match_id'], axis=1
    ).rename(
        columns={
            "match_id_y": "match_id"
        }
    ).dropna(axis=0, subset=['match_id'])

    ball_by_ball_df['match_id'] = ball_by_ball_df['match_id'].astype(int)

    # Getting batsman_team_id
    teams_df = getPandasFactoryDF(session, GET_TEAM_SQL)
    ball_by_ball_df = pd.merge(
        ball_by_ball_df,
        teams_df[['team_name', 'team_id']],
        how='left',
        left_on='batting_team',
        right_on='team_name'
    )
    ball_by_ball_df = ball_by_ball_df.rename(
        columns={
            'team_id': 'batting_team_id'
        }
    )

    ball_by_ball_df['out_desc'] = ball_by_ball_df['out_desc'].apply(lambda x: str(x).replace("'", ""))
    ball_by_ball_df["load_timestamp"] = load_timestamp

    ball_by_ball_df = ball_by_ball_df.rename(
        columns={
            'ball_number': 'balls',
            'is_dot_ball': 'dot_balls',
            'is_one': 'ones',
            'is_two': 'twos',
            'is_three': 'threes',
            'is_four': 'fours',
            'is_six': 'sixes',
        }
    )

    for key in ['sixes', 'dot_balls', 'threes', 'balls', 'twos', 'runs', 'fours', 'ones']:
        ball_by_ball_df[key] = pd.to_numeric(ball_by_ball_df[key]).fillna(0).astype(int)
    grouped = ball_by_ball_df.groupby('batsman_id').aggregate({
        'fours': 'sum',
        'sixes': 'sum',
        'threes': 'sum',
        'twos': 'sum',
        'ones': 'sum',
        'ball_runs': 'sum',
        'dot_balls': 'sum',
        'balls': 'count',
        'is_wide': 'sum',
        'is_no_ball': 'sum'
    })
    final_df = pd.merge(
        ball_by_ball_df.drop_duplicates('batsman_id')[[
            'match_id',
            'innings',
            'competition_name',
            'season',
            'batting_team_id',
            'batsman_id',
            'batting_position',
            'out_desc',
            'load_timestamp'
        ]],
        grouped,
        on='batsman_id',
        how='right'
    ).rename(columns={
        'ball_runs': 'runs'
    })
    out_desc = ball_by_ball_df[ball_by_ball_df['out_desc'] != ""][['out_batsman_id', 'out_desc']]

    final_df = pd.merge(
        final_df,
        out_desc,
        left_on='batsman_id',
        right_on='out_batsman_id',
        how='left'
    ).drop(['out_desc_x', 'out_batsman_id'], axis=1).rename(columns={
        'out_desc_y': 'out_desc'
    })
    final_df['balls'] = final_df['balls'] - final_df['is_wide'] - final_df['is_no_ball']
    final_df['strike_rate_balls'] = final_df['balls']
    final_df['strike_rate_balls'] = final_df['strike_rate_balls'].replace(0, 1)
    final_df['strike_rate'] = (final_df['runs'] / final_df['strike_rate_balls']) * 100
    final_df['strike_rate'] = final_df['strike_rate'].round(2)
    final_df = final_df.drop(['is_wide', 'is_no_ball'], axis=1)
    final_df[[
        'out_desc'
    ]] = final_df[[
        'out_desc'
    ]].fillna("not out")
    final_df['bowler_id'] = -1
    final_df['strike_rate'] = final_df['strike_rate'].fillna(-1)
    final_df = final_df.drop(['strike_rate_balls'], axis=1)
    bat_card_final_data = generateSeq(
        final_df.sort_values(
            ['match_id', 'batting_team_id', 'batsman_id']
        ), BAT_CARD_KEY_COL, max_key_val
    ).to_dict(orient='records')
    return bat_card_final_data
