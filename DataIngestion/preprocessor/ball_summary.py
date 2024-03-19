import sys

from DataIngestion import load_timestamp

sys.path.append("./../../")
sys.path.append("./")
from common.dao_client import session
from log.log import get_logger
from DataIngestion.config import INNINGS_REQD_COLS, WAGONWHEEL_REQD_COLS, INNINGS_KEY_COL, INNINGS_TABLE_NAME, \
    PLAYER_MAPPER_TABLE_NAME
from common.dao.fetch_db_data import getPandasFactoryDF, getMaxId
from DataIngestion.query import GET_MATCHES_DETAILS_SQL, GET_PLAYER_DETAILS_SQL, GET_BAT_CARD_DATA, \
    GET_BOWL_CARD_DATA, GET_TEAM_SQL, GET_PLAYER_MAPPER_SQL
from DataIngestion.utils.helper import readJsFile, dataToDF, generateSeq, getRawDict
import pandas as pd
import numpy as np
from common.db_config import DB_NAME

logger = get_logger("Ingestion", "Ingestion")


def getMatchBallSummaryData(session, root_data_files, load_timestamp):
    logger.info("Ball Summary Data Generation Started!")
    if root_data_files:
        # Getting max match_id from target table
        max_key_val = getMaxId(session, INNINGS_TABLE_NAME, INNINGS_KEY_COL, DB_NAME, True)

        # Initializing empty DF
        append_innings_df = pd.DataFrame()

        path_set = set((key.split("-")[1].split(".")[0].strip(), value) for key, value in root_data_files.items()
                       if 'innings' in key.split("-")[1].split(".")[0].strip().lower())

        for tupes in path_set:
            path_key = tupes[0]
            data_path = tupes[1]

            competition_name = data_path.split("/")[-3].split(" ")[0]
            season = data_path.split("/")[-2].split(" ")[1]

            innings_raw_data = readJsFile(data_path)[path_key]['OverHistory']

            for rawdata in innings_raw_data:
                rawdata['competition_name'] = competition_name
                rawdata['season'] = season

            innings_list = getRawDict(innings_raw_data, INNINGS_REQD_COLS)

            innings_df = dataToDF(innings_list)

            wagonwheel_raw_data = readJsFile(data_path)[path_key]['WagonWheel']
            wagonwheel_list = getRawDict(wagonwheel_raw_data, WAGONWHEEL_REQD_COLS)
            wagonwheel_df = dataToDF(wagonwheel_list)

            if wagonwheel_df.empty:
                wagonwheel_df = pd.DataFrame(columns=['BallID', 'FielderAngle', 'FielderLengthRatio'])
                wagonwheel_df['BallID'] = wagonwheel_df['BallID'].fillna('XX')
                wagonwheel_df['FielderAngle'] = wagonwheel_df['FielderAngle'].fillna(0)
                wagonwheel_df['FielderLengthRatio'] = wagonwheel_df['FielderLengthRatio'].fillna(0)

            innings_df = pd.merge(innings_df, wagonwheel_df, on='BallID', how='left') \
                .rename(columns={'FielderLengthRatio': 'fielder_length_ratio', 'FielderAngle': 'fielder_angle'}) \
                .drop(['BallID'], axis=1)

            append_innings_df = append_innings_df.append(innings_df, ignore_index=True)

        # if no data is there, return
        if append_innings_df.empty:
            logger.info("No New Match Ball by Ball Data Available!")
            return
        matches_df = getPandasFactoryDF(session, GET_MATCHES_DETAILS_SQL)

        append_innings_df = pd.merge(
            append_innings_df,
            matches_df[['match_id', 'src_match_id']],
            left_on=append_innings_df['MatchID'],
            right_on='src_match_id', how='left'
        ).drop(['MatchID', 'src_match_id'], axis=1)

        append_innings_df = append_innings_df.dropna(axis=0, subset=['match_id'])

        append_innings_df = append_innings_df[append_innings_df['match_id'].notna()]
        players_df = getPandasFactoryDF(session, GET_PLAYER_DETAILS_SQL).drop_duplicates()
        players_existing_df = getPandasFactoryDF(session, GET_PLAYER_MAPPER_SQL)[["id", "sports_mechanics_id"]]
        players_existing_mapping = dict(zip(players_existing_df['sports_mechanics_id'], players_existing_df['id']))
        append_innings_df['StrikerID'] = append_innings_df['StrikerID'].replace(players_existing_mapping).astype(str)
        append_innings_df['OutBatsManID'] = append_innings_df['OutBatsManID'].replace(players_existing_mapping).astype(str)
        append_innings_df['BowlerID'] = append_innings_df['BowlerID'].replace(players_existing_mapping).astype(str)
        append_innings_df['NonStrikerID'] = append_innings_df['NonStrikerID'].replace(players_existing_mapping).astype(str)
        # Getting Striker ID
        append_innings_df = pd.merge(
            append_innings_df,
            players_df[['src_player_id', 'player_id']],
            left_on=append_innings_df['StrikerID'],
            right_on=players_df['src_player_id'],
            how='left'
        ).rename(
            columns={'player_id': 'batsman_id'}
        ).drop(['StrikerID', 'key_0', 'src_player_id'], axis=1)

        # Getting Non Striker ID
        append_innings_df = pd.merge(
            append_innings_df,
            players_df[['src_player_id', 'player_id']],
            left_on=append_innings_df['NonStrikerID'],
            right_on=players_df['src_player_id'],
            how='left'
        ).rename(
            columns={'player_id': 'non_striker_id'}
        ).drop(['NonStrikerID', 'key_0', 'src_player_id'], axis=1)

        append_innings_df = pd.merge(
            append_innings_df,
            players_df[['src_player_id', 'player_id']],
            left_on=append_innings_df['OutBatsManID'],
            right_on=players_df['src_player_id'],
            how='left'
        ).rename(
            columns={'player_id': 'out_batsman_id'}
        ).drop(['OutBatsManID', 'key_0', 'src_player_id'], axis=1)

        append_innings_df = pd.merge(
            append_innings_df,
            players_df[['src_player_id', 'player_id']],
            left_on=append_innings_df['BowlerID'],
            right_on=players_df['src_player_id'],
            how='left'
        ).rename(
            columns={'player_id': 'against_bowler'}
        ).drop(['BowlerID', 'key_0', 'src_player_id'], axis=1)

        bat_card_df = getPandasFactoryDF(session, GET_BAT_CARD_DATA)[[
            'match_id', 'innings', 'batsman_id', 'batting_team_id', 'batting_position'
        ]]

        append_innings_df['InningsNo'] = append_innings_df['InningsNo'].astype(int)

        append_innings_df = pd.merge(
            append_innings_df,
            bat_card_df,
            how="left",
            left_on=['match_id', 'InningsNo', 'batsman_id'],
            right_on=['match_id', 'innings', 'batsman_id']
        ).rename(columns={'batting_team_id': 'batsman_team_id'}).drop(['InningsNo'], axis=1)
        append_innings_df['batsman_team_id'] = append_innings_df['batsman_team_id'].astype(int)
        bowl_card_df = getPandasFactoryDF(session, GET_BOWL_CARD_DATA)[['match_id', 'innings', 'bowler_id', 'team_id']]

        append_innings_df = pd.merge(
            append_innings_df,
            bowl_card_df,
            how="left",
            left_on=['match_id', 'innings', 'against_bowler'],
            right_on=['match_id', 'innings', 'bowler_id']
        ).rename(columns={'team_id': 'bowler_team_id'}).drop(['bowler_id'], axis=1)

        append_innings_df['over_number'] = append_innings_df['OverNo'].astype(int)
        # conditions for batting phases
        phase_conditions = [
            (append_innings_df['over_number'] <= 6),
            (append_innings_df['over_number'] > 6) & (append_innings_df['over_number'] <= 10),
            (append_innings_df['over_number'] > 10) & (append_innings_df['over_number'] <= 15),
            (append_innings_df['over_number'] > 15)
        ]

        # different batting phases
        phase_values = [1, 2, 3, 4]

        # create batting_phase column
        append_innings_df['batting_phase'] = np.select(phase_conditions, phase_values)

        append_innings_df['over_text'] = append_innings_df['CommentOver'].str.split(" ").map(lambda x: x[1])

        append_innings_df['Runs'] = append_innings_df['Runs'].apply(lambda x: x.split(" ")[0])
        append_innings_df['BallRuns'] = append_innings_df['BallRuns'].fillna(0).astype(int)

        append_innings_df = append_innings_df.rename(
            columns={
                'BallNo': 'ball_number',
                'Runs': 'runs',
                'IsOne': 'is_one',
                'IsTwo': 'is_two',
                'IsThree': 'is_three',
                'IsDotball': 'is_dot_ball',
                'Extras': 'extras',
                'BallRuns': 'ball_runs',
                'IsExtra': 'is_extra',
                'IsWide': 'is_wide',
                'BowlingDirection': 'bowling_direction',
                'IsNoBall': 'is_no_ball',
                'IsBye': 'is_bye',
                'IsUncomfortable': 'is_uncomfortable',
                'IsLegBye': 'is_leg_bye',
                'IsFour': 'is_four',
                'IsBeaten': 'is_beaten',
                'IsSix': 'is_six',
                'IsWicket': 'is_wicket',
                'VideoFile': 'video_file',
                'WicketType': 'wicket_type',
                'IsBowlerWicket': 'is_bowler_wicket',
                'Xpitch': 'x_pitch',
                'Ypitch': 'y_pitch',
                'IsMaiden': 'is_maiden',
                'BowlType': 'bowl_type',
                'Line': 'bowl_line',
                'ShotType': 'shot_type',
                'Length': 'bowl_length'
            }
        ).drop(['OverNo', 'CommentOver'], axis=1)

        append_innings_df[[
            'against_bowler', 'batsman_id', 'match_id', 'out_batsman_id'
        ]] = append_innings_df[[
            'against_bowler', 'batsman_id', 'match_id', 'out_batsman_id'
        ]].fillna(-1).astype(int)
        append_innings_df[[
            'wicket_type', 'bowl_type', 'shot_type', 'bowl_line', 'bowl_length', 'video_file'
        ]] = append_innings_df[[
            'wicket_type', 'bowl_type', 'shot_type', 'bowl_line', 'bowl_length', 'video_file'
        ]].fillna("NA")
        append_innings_df[[
            'x_pitch', 'y_pitch', 'fielder_length_ratio', 'fielder_angle'
        ]] = append_innings_df[[
            'x_pitch', 'y_pitch', 'fielder_length_ratio', 'fielder_angle'
        ]].fillna(0)
        append_innings_df['bowling_direction'] = append_innings_df['bowling_direction'].fillna("NA")
        append_innings_df['is_bye'] = append_innings_df['is_bye'].apply(lambda x: 1 if x == 'True' else 0)
        append_innings_df['is_leg_bye'] = append_innings_df['is_leg_bye'].apply(lambda x: 1 if x == 'True' else 0)
        append_innings_df['is_wide'] = append_innings_df['is_wide'].apply(lambda x: 1 if x == 'True' else 0)
        append_innings_df['is_no_ball'] = append_innings_df['is_no_ball'].apply(lambda x: 1 if x == 'True' else 0)
        append_innings_df['is_four'] = append_innings_df['is_four'].apply(lambda x: 1 if x == 'True' else 0)
        append_innings_df['is_six'] = append_innings_df['is_six'].apply(lambda x: 1 if x == 'True' else 0)
        append_innings_df['is_beaten'] = append_innings_df['is_beaten'].apply(lambda x: 1 if x == 'True' else 0)
        append_innings_df['is_uncomfortable'] = append_innings_df['is_uncomfortable'].apply(
            lambda x: 1 if x == 'True' else 0
        )

        append_innings_df['bowl_line'] = np.where(
            append_innings_df['bowl_line'] == 'Middle',
            'Middle Stump',
            append_innings_df['bowl_line']
        )

        append_innings_df['bowl_length'] = np.where(
            append_innings_df['bowl_length'] == 'Short Of Good Length',
            'Good Length',
            append_innings_df['bowl_length']
        )

        append_innings_df['load_timestamp'] = load_timestamp

        final_innings_data = generateSeq(
            append_innings_df.where(
                pd.notnull(append_innings_df), None
            ).sort_values(['match_id', 'innings', 'over_number', 'ball_number']),
            INNINGS_KEY_COL,
            max_key_val
        ).to_dict(orient='records')
        logger.info("Ball Summary Data Generation Completed!")
        return final_innings_data
    else:
        logger.info("No New Teams Data Available!")


def get_cricsheet_match_ball_summary(ball_by_ball_df):
    matches_df = getPandasFactoryDF(session, GET_MATCHES_DETAILS_SQL)
    match_ball_summary = pd.merge(
        ball_by_ball_df,
        matches_df[['match_id', 'src_match_id']],
        on='src_match_id',
        how='left'
    ).drop(['src_match_id'], axis=1)
    match_ball_summary.rename(columns={'bowler_id': 'against_bowler'}, inplace=True)
    # Get Batsman Team id
    teams_df = getPandasFactoryDF(session, GET_TEAM_SQL)[['team_id', 'team_short_name', 'team_name']]
    match_ball_summary = pd.merge(
        match_ball_summary,
        teams_df,
        left_on='batting_team',
        right_on='team_name',
        how="left"
    ).rename(
        columns={'team_id': 'batsman_team_id'}
    ).drop(['team_short_name_x', 'team_short_name'], axis=1)

    # Get Bowler Team id
    match_ball_summary = pd.merge(
        match_ball_summary,
        teams_df,
        left_on='bowling_team',
        right_on='team_name',
        how="left"
    ).rename(
        columns={'team_id': 'bowler_team_id'}
    ).drop(['team_short_name_y', 'team_short_name'], axis=1)

    # conditions for batting phases
    phase_conditions = [
        (match_ball_summary['over_number'] <= 6),
        (match_ball_summary['over_number'] > 6) & (match_ball_summary['over_number'] <= 10),
        (match_ball_summary['over_number'] > 10) & (match_ball_summary['over_number'] <= 15),
        (match_ball_summary['over_number'] > 15)
    ]

    # Different batting phases
    phase_values = [1, 2, 3, 4]

    # Create batting_phase column
    match_ball_summary['batting_phase'] = np.select(phase_conditions, phase_values)
    match_ball_summary['over_text'] = match_ball_summary['over_text'].astype(str)

    match_ball_summary[[
        'against_bowler', 'batsman_id', 'match_id', 'out_batsman_id']
    ] = match_ball_summary[[
        'against_bowler', 'batsman_id', 'match_id', 'out_batsman_id'
    ]].fillna(-1).astype(int)
    match_ball_summary[[
        'wicket_type'
    ]] = match_ball_summary[[
        'wicket_type'
    ]].fillna("NA")
    match_ball_summary[[
        'bowling_direction',
        "video_file",
    ]] = "NA"
    match_ball_summary[[
        'fielder_angle',
        "fielder_length_ratio",
        "is_beaten",
        "is_uncomfortable",
        "x_pitch",
        "y_pitch",
    ]] = -1

    match_ball_summary['load_timestamp'] = load_timestamp
    maiden_df = match_ball_summary[[
        'innings', 'over_number', 'ball_number', 'is_one', 'is_two', 'is_three', 'is_four', 'is_six', 'is_dot_ball',
        'is_wide', 'is_no_ball'
    ]]
    grouped = maiden_df.groupby(['innings', 'over_number']).aggregate({
        'ball_number': 'count'
    })
    grouped = grouped[grouped['ball_number'] == 6].reset_index()
    grouped = pd.merge(
        grouped,
        match_ball_summary,
        on=['innings', 'over_number'],
        how='left'
    )
    grouped = grouped[grouped['is_dot_ball'] == 1]
    grouped = grouped.groupby(['innings', 'over_number']).aggregate({
        'ball_number_y': 'count'
    })
    grouped = grouped[grouped['ball_number_y'] == 6].reset_index()
    if not grouped.empty:
        grouped['is_maiden'] = 1
        match_ball_summary = pd.merge(
            match_ball_summary,
            grouped[['innings', 'over_number', 'is_maiden']],
            on=['innings', 'over_number'],
            how='left'
        )
        match_ball_summary['is_maiden'] = match_ball_summary['is_maiden'].fillna(0).astype(int)
    else:
        match_ball_summary['is_maiden'] = 0

    match_ball_summary = match_ball_summary[[
        'competition_name',
        'season',
        'match_id',
        'over_number',
        'ball_number',
        'over_text',
        'batsman_id',
        'non_striker_id',
        'batting_position',
        'batsman_team_id',
        'batting_phase',
        'against_bowler',
        'bowler_team_id',
        'innings',
        'runs',
        'ball_runs',
        'is_one',
        'is_two',
        'is_three',
        'is_four',
        'is_six',
        'is_dot_ball',
        'extras',
        'is_extra',
        'is_wide',
        'is_no_ball',
        'is_bye',
        'is_leg_bye',
        'is_wicket',
        'wicket_type',
        'is_bowler_wicket',
        'out_batsman_id',
        'is_maiden',
        'bowling_direction',
        'load_timestamp',
        'bowling_direction',
        "bowl_line",
        "bowl_length",
        "bowl_type",
        'fielder_angle',
        "fielder_length_ratio",
        "is_beaten",
        "is_uncomfortable",
        "shot_type",
        "video_file",
        "x_pitch",
        "y_pitch",
        'over_text'
    ]]
    match_ball_summary = generateSeq(
        match_ball_summary.where(
            pd.notnull(match_ball_summary),
            None
        ).sort_values(['match_id', 'innings', 'over_number', 'ball_number']),
        INNINGS_KEY_COL,
        getMaxId(session, INNINGS_TABLE_NAME, INNINGS_KEY_COL, DB_NAME, False)
    ).to_dict(orient='records')
    return match_ball_summary
