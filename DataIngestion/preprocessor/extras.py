import sys

from DataIngestion import load_timestamp

sys.path.append("./../../")
sys.path.append("./")
from log.log import get_logger
import pandas as pd
from DataIngestion.config import EXTRAS_REQD_COLS, EXTRAS_KEY_COL, EXTRAS_TABLE_NAME
from DataIngestion.query import GET_MATCHES_DETAILS_SQL, GET_TEAM_SQL
from common.dao.fetch_db_data import getMaxId, getPandasFactoryDF
from common.db_config import DB_NAME
from DataIngestion.utils.helper import readJsFile, getRawDict, dataToDF, generateSeq
from common.dao_client import session

logger = get_logger("Ingestion", "Ingestion")


def getExtrasData(session, root_data_files, load_timestamp):
    logger.info("Extras Data Generation Started!")
    if root_data_files:
        # Getting max match_id from target table
        max_key_val = getMaxId(session, EXTRAS_TABLE_NAME, EXTRAS_KEY_COL, DB_NAME)

        # Initializing empty DF
        append_extras_df = pd.DataFrame()

        path_set = set((key.split("-")[1].split(".")[0].strip(), value) for key, value in root_data_files.items()
                       if 'innings' in key.split("-")[1].split(".")[0].strip().lower())

        for tupes in path_set:
            path_key = tupes[0]
            data_path = tupes[1]

            extras_raw_data = readJsFile(data_path)[path_key]['Extras']

            extras_list = getRawDict(extras_raw_data, EXTRAS_REQD_COLS)

            extras_df = dataToDF(extras_list)

            append_extras_df = append_extras_df.append(extras_df, ignore_index=True)

        # if no new data is there return
        if append_extras_df.empty:
            logger.info("No New Match Extra Data Available!")
            return

        # GET MATCH_ID
        matches_df = getPandasFactoryDF(session, GET_MATCHES_DETAILS_SQL)
        append_extras_df = pd.merge(append_extras_df, matches_df[['match_id', 'src_match_id']],
                                    left_on=append_extras_df['MatchID'], \
                                    right_on='src_match_id', how='left') \
            .drop(['MatchID', 'src_match_id'], axis=1)

        append_extras_df = append_extras_df.dropna(axis=0, subset=['match_id'])
        append_extras_df['match_id'] = append_extras_df['match_id'].astype(int)

        teams_df = getPandasFactoryDF(session, GET_TEAM_SQL)

        # GET TEAM_ID
        append_extras_df = pd.merge(append_extras_df, teams_df[['team_id', 'src_team_id']], how='left',
                                    left_on=append_extras_df['TeamID']
                                    , right_on='src_team_id') \
            .drop(['TeamID', 'src_team_id'], axis=1)

        append_extras_df['team_id'] = append_extras_df['team_id'].fillna(-1).astype(int)

        append_extras_df = append_extras_df.rename(
            columns={"Wides": "wides", "TotalExtras": "total_extras", "Byes": "byes",
                     "NoBalls": "no_balls", "InningsNo": "innings",
                     "LegByes": "leg_byes"})

        append_extras_df[['wides', 'total_extras', 'byes', 'no_balls', 'leg_byes']] = \
            append_extras_df[['wides', 'total_extras', 'byes', 'no_balls', 'leg_byes']].fillna(0).astype(int)

        append_extras_df['load_timestamp'] = load_timestamp

        final_extras_data = generateSeq(append_extras_df.where(pd.notnull(append_extras_df), 0) \
                                        .sort_values(['match_id', 'innings', 'team_id']), EXTRAS_KEY_COL,
                                        max_key_val) \
            .to_dict(orient='records')
        logger.info("Extras Data Generation Completed!")
        return final_extras_data

    else:
        logger.info("No New Extras Data Available!")


def cricsheet_extras(ball_by_ball_df):
    # logger.info("Extras Data Generation Started!")
    matches_df = getPandasFactoryDF(session, GET_MATCHES_DETAILS_SQL)
    ball_by_ball_df = ball_by_ball_df[[
        'src_match_id',
        'is_wide',
        'extras',
        'is_bye',
        'is_no_ball',
        'innings',
        'batting_team',
        'is_leg_bye'
    ]]
    extras_df = pd.merge(
        ball_by_ball_df,
        matches_df[['src_match_id', 'match_id']],
        left_on='src_match_id',
        right_on='src_match_id',
        how='left'
    ).drop(['src_match_id'], axis=1)

    teams_df = getPandasFactoryDF(session, GET_TEAM_SQL)
    extras_df = pd.merge(
        extras_df,
        teams_df[['team_id', 'team_name']],
        how='left',
        left_on=extras_df['batting_team'],
        right_on='team_name'
    ).drop(['batting_team', 'team_name'], axis=1)
    extras_df = extras_df.groupby(['innings', 'match_id', 'team_id']).sum().reset_index()
    extras_df = extras_df.rename(columns={
        "is_wide": "wides",
        "extras": "total_extras",
        "is_bye": "byes",
        "is_no_ball": "no_balls",
        "innings": "innings",
        "is_leg_bye": "leg_byes",
        "match_id_y": "match_id"
    })

    extras_df[[
        'wides',
        'total_extras',
        'byes',
        'no_balls',
        'leg_byes'
    ]] = extras_df[[
        'wides',
        'total_extras',
        'byes',
        'no_balls',
        'leg_byes'
    ]].fillna(0).astype(int)
    extras_df['load_timestamp'] = load_timestamp
    extras_df = generateSeq(
        extras_df.sort_values(['match_id', 'innings', 'team_id']),
        EXTRAS_KEY_COL,
        getMaxId(session, EXTRAS_TABLE_NAME, EXTRAS_KEY_COL, DB_NAME, False)
    ).to_dict(orient='records')
    # logger.info("Extras Data Generation Completed!")
    return extras_df
