import sys

from DataIngestion import load_timestamp

sys.path.append("./../../")
sys.path.append("./")
from log.log import get_logger
from DataIngestion.config import PARTNERSHIP_TABLE_NAME, PARTNERSHIP_KEY_COL, PARTNERSHIP_REQD_COLS, \
    PLAYER_MAPPER_TABLE_NAME
from common.dao.fetch_db_data import getPandasFactoryDF, getMaxId
from DataIngestion.query import GET_MATCHES_DETAILS_SQL, GET_PLAYER_DETAILS_SQL, GET_TEAM_SQL, GET_PLAYER_MAPPER_SQL
from DataIngestion.utils.helper import readJsFile, dataToDF, getRawDict, generateSeq
import pandas as pd
from common.db_config import DB_NAME
from common.dao_client import session

logger = get_logger("Ingestion", "Ingestion")


def getPartnershipData(session, root_data_files, load_timestamp):
    logger.info("Partnership Data Generation Started!")
    if root_data_files:
        append_partnership_df = pd.DataFrame()

        max_key_val = getMaxId(session, PARTNERSHIP_TABLE_NAME, PARTNERSHIP_KEY_COL, DB_NAME)

        path_set = set((key.split("-")[1].split(".")[0].strip(), value) for key, value in root_data_files.items()
                       if 'innings' in key.split("-")[1].split(".")[0].strip().lower())

        for tupes in path_set:
            path_key = tupes[0]
            data_path = tupes[1]

            competition_name = data_path.split("/")[-3].split(" ")[0]
            season = data_path.split("/")[-2].split(" ")[1]

            partnership_raw_data = readJsFile(data_path)[path_key]['PartnershipScores']

            partnership_list = getRawDict(partnership_raw_data, PARTNERSHIP_REQD_COLS)

            partnership_df = dataToDF(partnership_list)
            partnership_df['competition_name'] = competition_name
            partnership_df['season'] = season

            append_partnership_df = append_partnership_df.append(partnership_df, ignore_index=True)
        # if no new data is there return
        if append_partnership_df.empty:
            logger.info("No New Match Partnership Data Available!")
            return
        matches_df = getPandasFactoryDF(session, GET_MATCHES_DETAILS_SQL)

        # Get Match ID
        append_partnership_df = pd.merge(append_partnership_df, matches_df[['match_id', 'src_match_id']],
                                         left_on=append_partnership_df['MatchID'], \
                                         right_on='src_match_id', how='left') \
            .drop(['MatchID', 'src_match_id'], axis=1)

        append_partnership_df = append_partnership_df.dropna(axis=0, subset=['match_id'])
        append_partnership_df['match_id'] = append_partnership_df['match_id'].astype(int)

        # Get Batting team ID
        teams_df = getPandasFactoryDF(session, GET_TEAM_SQL)

        append_partnership_df = pd.merge(append_partnership_df, teams_df[['team_id', 'src_team_id']], how='left',
                                         left_on=append_partnership_df['BattingTeamID']
                                         , right_on='src_team_id') \
            .rename(columns={'team_id': 'team_id'}).drop(['BattingTeamID', 'src_team_id'], axis=1)

        # Get Striker  Player ID
        players_df = getPandasFactoryDF(session, GET_PLAYER_DETAILS_SQL).drop_duplicates()
        players_existing_df = getPandasFactoryDF(session, GET_PLAYER_MAPPER_SQL)
        append_partnership_df = pd.merge(
            players_existing_df[["id", "sports_mechanics_id"]],
            append_partnership_df,
            left_on='sports_mechanics_id',
            right_on='StrikerID',
            how='inner'
        ).drop(['sports_mechanics_id', 'StrikerID'], axis=1).rename(
            columns={
                'id': 'StrikerID'
            }
        )
        append_partnership_df['StrikerID'] = append_partnership_df['StrikerID'].astype(str)

        append_partnership_df = pd.merge(
            players_existing_df[["id", "sports_mechanics_id"]],
            append_partnership_df,
            left_on='sports_mechanics_id',
            right_on='NonStrikerID',
            how='inner'
        ).drop(['sports_mechanics_id', 'NonStrikerID'], axis=1).rename(
            columns={
                'id': 'NonStrikerID'
            }
        )
        append_partnership_df['NonStrikerID'] = append_partnership_df['NonStrikerID'].astype(str)

        append_partnership_df = pd.merge(
            append_partnership_df,
            players_df[['src_player_id', 'player_id']],
            left_on=append_partnership_df['StrikerID'],
            right_on=players_df['src_player_id'],
            how='left'
        ).rename(
            columns={'player_id': 'striker'}
        ).drop(['Striker', 'StrikerID', 'src_player_id', 'key_0'], axis=1)

        # GET Non-Striker Player ID
        append_partnership_df = pd.merge(
            append_partnership_df, players_df[['src_player_id', 'player_id']],
            left_on=append_partnership_df['NonStrikerID'],
            right_on=players_df['src_player_id'],
            how='left'
        ).rename(
            columns={'player_id': 'non_striker'}
        ).drop(['NonStriker', 'NonStrikerID', 'src_player_id', 'key_0'], axis=1)

        append_partnership_df[['striker', 'non_striker', 'team_id']] = \
            append_partnership_df[['striker', 'non_striker', 'team_id']].fillna(-1).astype(int)

        append_partnership_df = append_partnership_df.rename(
            columns={
                'InningsNo': 'innings', 'PartnershipTotal': 'partnership_total',
                'StrikerRuns': 'striker_runs', 'StrikerBalls': 'striker_balls',
                'Extras': 'extras', 'NonStrikerRuns': 'non_striker_runs',
                'NonStrikerBalls': 'non_striker_balls'
            }
        )

        append_partnership_df[['striker_runs', 'striker_balls', 'extras', 'non_striker_runs', 'non_striker_balls',
                               'partnership_total']] = \
            append_partnership_df[['striker_runs', 'striker_balls', 'extras', 'non_striker_runs', 'non_striker_balls',
                                   'partnership_total']].fillna(0).astype(int)

        append_partnership_df['load_timestamp'] = load_timestamp

        partnership_final_data = generateSeq(
            append_partnership_df.sort_values(['match_id', 'team_id', 'striker', 'non_striker']),
            PARTNERSHIP_KEY_COL, max_key_val) \
            .to_dict(orient='records')
        logger.info("Partnership Data Generation Completed!")
        return partnership_final_data
    else:
        logger.info("No New Partnership Data Available!")


def cricsheet_partnership(ball_by_ball_df):
    # Get batter team id
    teams_df = getPandasFactoryDF(session, GET_TEAM_SQL)
    partnership_df = pd.merge(
        ball_by_ball_df,
        teams_df[['team_id', 'team_name']],
        left_on=ball_by_ball_df['batting_team'],
        right_on='team_name',
        how='left'
    ).drop(['team_name', 'batting_team'], axis=1)
    partnership_df.rename(columns={'batsman_id': 'striker'}, inplace=True)
    partnership_df = partnership_df.drop(['non_striker'], axis=1)
    partnership_df.rename(columns={'non_striker_id': 'non_striker'}, inplace=True)
    matches_df = getPandasFactoryDF(session, GET_MATCHES_DETAILS_SQL)
    partnership_df = pd.merge(
        partnership_df,
        matches_df[['src_match_id', 'match_id']],
        left_on='src_match_id',
        right_on='src_match_id',
        how='left'
    )

    partnership_df = partnership_df[[
        'match_id',
        'competition_name',
        'season',
        'team_id',
        'innings',
        'striker',
        'extras',
        'non_striker',
        'is_four',
        'is_six',
        'is_one',
        'is_two',
        'is_three'
    ]]
    grouped_df = partnership_df.groupby(
        ['striker', 'non_striker', 'innings', 'match_id', 'season', 'team_id', 'competition_name']
    )
    size_group_df = grouped_df.size().reset_index()
    partnership_df = grouped_df.sum().reset_index()
    merged_df = pd.merge(
        partnership_df,
        size_group_df,
        on=['striker', 'non_striker', 'innings', 'match_id', 'season', 'team_id']
    )
    merged_df['runs'] = merged_df['is_four'] * 4 + merged_df['is_six'] * 6 + merged_df['is_three'] * 3 + merged_df[
        'is_two'] * 2 + merged_df['is_one']
    merged_df = merged_df.drop(['is_six', 'is_three', 'is_four', 'is_two', 'is_one'], axis=1)
    merged_df = pd.merge(
        merged_df,
        merged_df,
        left_on=['striker', 'non_striker'],
        right_on=['non_striker', 'striker'],
        how='left'
    )
    merged_df['extras'] = merged_df['extras_x'] + merged_df['extras_y']
    merged_df = merged_df.rename(
        columns={
            'innings_x': 'innings',
            'match_id_x': 'match_id',
            'season_x': 'season',
            'team_id_x': 'team_id',
            'striker_x': 'striker',
            'non_striker_x': 'non_striker',
            '0_x': 'striker_balls',
            '0_y': 'non_striker_balls',
            'runs_x': 'striker_runs',
            'runs_y': 'non_striker_runs',
            'competition_name_x_x': 'competition_name'
        }
    ).drop(['innings_y', 'match_id_y', 'season_y', 'team_id_y', 'extras_x', 'extras_y', 'striker_y', 'non_striker_y',
            'competition_name_x_y', 'competition_name_y_x', 'competition_name_y_y'], axis=1)

    merged_df = merged_df.fillna(0)
    merged_df[["non_striker_balls", 'non_striker_runs', 'extras']] = merged_df[[
        "non_striker_balls", 'non_striker_runs', 'extras']].astype(int)
    merged_df['partnership_total'] = merged_df['striker_runs'] + merged_df['non_striker_runs'] + merged_df['extras']
    merged_df['load_timestamp'] = load_timestamp
    merged_reverse_df = merged_df.rename(columns={
        'striker': 'non_striker',
        'non_striker': 'striker'
    })
    merged_diff_df = pd.merge(
        merged_df,
        merged_reverse_df,
        on=['striker', 'non_striker'],
        how='outer',
        indicator=True
    )

    merged_reverse_df = merged_diff_df[merged_diff_df['_merge'] == 'right_only'].rename(
        columns={
            'load_timestamp_y': 'load_timestamp',
            'extras_y': 'extras',
            'partnership_total_y': 'partnership_total',
            'non_striker_runs_y': 'striker_runs',
            'non_striker_balls_y': 'striker_balls',
            'striker_runs_y': 'non_striker_runs',
            'striker_y': 'striker',
            'non_striker_y': 'non_striker',
            'striker_balls_y': 'non_striker_balls',
            'competition_name_y': 'competition_name',
            'team_id_y': 'team_id',
            'season_y': 'season',
            'match_id_y': 'match_id',
            'innings_y': 'innings'
        }
    )[[
        'extras',
        'load_timestamp',
        'partnership_total',
        'striker_runs',
        'striker_balls',
        'non_striker_runs',
        'non_striker_balls',
        'competition_name',
        'team_id',
        'season',
        'match_id',
        'innings',
        'non_striker',
        'striker'
    ]]
    merged_reverse_df[[
        "extras",
        'partnership_total',
        'striker_runs',
        'striker_balls',
        'non_striker_runs',
        'non_striker_balls',
        'team_id',
        'season',
        'match_id',
        'innings'
    ]] = merged_reverse_df[[
        "extras",
        'partnership_total',
        'striker_runs',
        'striker_balls',
        'non_striker_runs',
        'non_striker_balls',
        'team_id',
        'season',
        'match_id',
        'innings'
    ]].astype(int)
    merged_df = merged_df.append(merged_reverse_df)
    partnership_final_data = generateSeq(
        merged_df.sort_values(['match_id', 'team_id', 'striker']),
        PARTNERSHIP_KEY_COL,
        getMaxId(session, PARTNERSHIP_TABLE_NAME, PARTNERSHIP_KEY_COL, DB_NAME, False)
    ).to_dict(orient='records')
    return partnership_final_data
