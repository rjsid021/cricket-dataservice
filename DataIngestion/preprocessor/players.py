import numpy as np
import pandas as pd

from DataIngestion import load_timestamp, config
from DataIngestion.config import (
    SQUAD_KEY_LIST,
    PLAYERS_TABLE_NAME,
    PLAYERS_KEY_COL,
    PLAYERS_REQD_COLS,
    IMAGE_STORE_URL,
    PLAYER_MAPPER_TABLE_NAME
)
from DataIngestion.player_ethnicity import PlayerEthnicity
from DataIngestion.query import (GET_TEAM_SQL, GET_PLAYERS_SQL, GET_PLAYER_MAPPER_SQL)
from DataIngestion.sources.cricsheet.parser import bowl_major_type_dict
from DataIngestion.utils.helper import getSquadRawData
from common.dao.fetch_db_data import getMaxId, getPandasFactoryDF
from common.dao_client import session
from common.db_config import DB_NAME
from log.log import get_logger

logger = get_logger("Ingestion", "Ingestion")


def player_ethnicity(players):
    """
    :param players: players dataframe
    :return: players dataframe which contain extra column of ethnicity (Domestic/Overseas)
    """
    if not players.empty:
        player_country_df = PlayerEthnicity.player_country()
        players['src_player_id'] = players['src_player_id'].astype(int)
        players = pd.merge(
            players,
            player_country_df,
            left_on='src_player_id',
            right_on='player_mapper_id'
        )

        players["player_type"] = np.where(
            players['country'] == 'India',
            "Domestic",
            "Overseas"
        )
        players = players.drop(['country', 'player_mapper_id'], axis=1)
        players['src_player_id'] = players['src_player_id'].astype(str)
        return players


def getPlayersData(session, squad_data_files, load_timestamp):
    logger.info("Players Data Generation Started!")
    if squad_data_files:
        players_df = getSquadRawData(
            squad_data_files, SQUAD_KEY_LIST, PLAYERS_REQD_COLS
        ).drop_duplicates(
            subset=["TeamID", "PlayerName", "PlayerID", "season", "competition_name"],
            keep='last'
        ).reset_index()

        players_df['player_skill'] = players_df['PlayerSkill'].map(
            lambda x: x.strip().upper().replace('ALLRONDER', 'ALLROUNDER')
        )
        players_df['is_batsman'] = np.where(
            (players_df['player_skill'] == 'BATSMAN') |
            (players_df['player_skill'] == 'ALLROUNDER') |
            (players_df['player_skill'] == 'WICKETKEEPER'),
            1,
            0
        )

        players_df['is_bowler'] = np.where(
            (players_df['player_skill'] == 'BOWLER') |
            (players_df['player_skill'] == 'ALLROUNDER'),
            1,
            0
        )

        players_df['is_wicket_keeper'] = np.where(
            players_df['player_skill'] == 'WICKETKEEPER',
            1,
            0
        )

        players_df['batting_type'] = players_df['BattingType'].map(lambda x: x.strip().upper())
        players_df['bowling_type'] = players_df['BowlingProficiency'].map(lambda x: x.strip().upper())
        players_df['bowling_type'] = players_df['bowling_type'].apply(
            lambda
                x: "LEFT ARM FAST" if x == "LEFT ARM KNUCKLEBALL" else "RIGHT ARM FAST" if x == "RIGHT ARM KNUCKLEBALL" else x
        )

        players_df['bowl_major_type'] = np.where(
            (players_df['bowling_type'] == 'LEFT ARM FAST') |
            (players_df['bowling_type'] == 'RIGHT ARM FAST'),
            'SEAM',
            'SPIN'
        )

        players_df["player_name"] = players_df["PlayerName"].apply(
            lambda x: x.replace(
                "'", ""
            ).replace(
                'Akshar Patel', 'Axar Patel'
            ).replace(
                'Jason Behrendroff',
                'Jason Behrendorff'
            )
        )

        # adding column load timestamp
        players_df["load_timestamp"] = load_timestamp

        players_df = players_df.drop(
            [
                "BattingType", "BowlingProficiency", "PlayerSkill", "index", "PlayerName",
                "src_match_id", "TeamID"
            ], axis=1
        ).rename(columns={"PlayerID": "src_player_id", "IsCaptain": "is_captain", "TeamName": "team_name"})
        players_df['team_name'].replace(config.TEAM_SPELL_MAPPING, inplace=True)
        players_df['season'] = players_df['season'].astype(int)

        teams_df = getPandasFactoryDF(session, GET_TEAM_SQL)
        players_df['team_name'] = players_df['team_name'].str.upper()
        players_df = players_df.merge(
            teams_df[["team_name", "team_id"]],
            on='team_name',
            how='left'
        )
        # Get player mapping for Sports mechanics
        players_existing_df = getPandasFactoryDF(session, GET_PLAYER_MAPPER_SQL)
        before_length = len(players_df)
        unmapped_player = players_df.copy()
        players_df = pd.merge(
            players_existing_df[["id", "sports_mechanics_id", "name"]],
            players_df,
            left_on='sports_mechanics_id',
            right_on='src_player_id',
            how='inner'
        ).drop(['sports_mechanics_id', 'src_player_id', 'player_name'], axis=1).rename(
            columns={
                'id': 'src_player_id',
                'name': 'player_name'
            }
        )

        players_df['player_image_url'] = (
                IMAGE_STORE_URL + 'players/' + players_df['player_name'].apply(
            lambda x: x.replace(' ', '-').lower()
        ).astype(str) + ".png"
        )

        players_df['src_player_id'] = players_df['src_player_id'].astype(str)
        if len(players_df) != before_length:
            # Merge DataFrames with a left join
            mixed_player = pd.merge(
                players_existing_df[["id", "sports_mechanics_id", "name"]],
                unmapped_player,
                left_on='sports_mechanics_id',
                right_on='src_player_id',
                how='right'
            )
            unmapped_player_df = mixed_player[mixed_player['id'].isnull()]
            unmapped_player_df = (unmapped_player_df[['src_player_id', 'team_name', 'player_name']])
            print(unmapped_player_df.to_dict(orient="record"))
            raise Exception("No sports mech id found")

        # Get existing df from target table
        players_existing_df = getPandasFactoryDF(session, GET_PLAYERS_SQL)

        players_latest_df = pd.merge(
            players_df,
            players_existing_df[['src_player_id', 'season', 'competition_name', 'team_id']],
            how='left',
            on=['src_player_id', 'season', 'competition_name', 'team_id'],
            indicator=True
        )
        # Don't do duplicate ingestion for same player across 'src_player_id', 'season', 'competition_name', 'team_id'
        players_df = players_latest_df[players_latest_df['_merge'] == "left_only"]

        if not players_df.empty:
            players_df = pd.merge(
                players_df,
                players_existing_df[['src_player_id', 'player_id']].drop_duplicates(),
                how='left',
                on=['src_player_id']
            )

            if 'player_id_y' in players_df.columns:
                players_df = players_df.drop('player_id_y', axis=1).rename(columns={'player_id_x': 'player_id'})
            # Don't use player skill from mapping table, use it from Sports Mechanics
            # players_df['player_skill'] = "NA"
            # player_mapping_df = getPandasFactoryDF(
            #     session,
            #     f"select id, is_wicket_keeper, is_batsman, is_bowler from {PLAYER_MAPPER_TABLE_NAME}"
            # )
            # player_mapping_df['id'] = player_mapping_df['id'].astype(str)
            # players_df = players_df.drop(['is_wicket_keeper', 'is_batsman', 'is_bowler'], axis=1)
            # players_df = pd.merge(
            #     players_df,
            #     player_mapping_df,
            #     left_on='src_player_id',
            #     right_on='id',
            #     how='inner'
            # )
            # players_df['player_skill'] = np.where(
            #     players_df['is_wicket_keeper'] == 1,
            #     'WICKETKEEPER',
            #     players_df['player_skill']
            # )
            # players_df['player_skill'] = np.where(
            #     (players_df['is_batsman'] == 1) & (players_df['is_bowler'] == 1),
            #     'ALLROUNDER',
            #     players_df['player_skill']
            # )
            # players_df['player_skill'] = np.where(
            #     (players_df['is_batsman'] == 1) & (players_df['is_bowler'] == 0),
            #     'BATSMAN',
            #     players_df['player_skill']
            # )
            # players_df['player_skill'] = np.where(
            #     (players_df['is_batsman'] == 0) & (players_df['is_bowler'] == 1),
            #     'BOWLER',
            #     players_df['player_skill']
            # )

            players_df = player_ethnicity(players_df)[[
                "player_id",
                "src_player_id",
                "player_name",
                "batting_type",
                "bowling_type",
                "player_skill",
                "team_id",
                "season",
                "competition_name",
                "is_captain",
                "is_batsman",
                "is_bowler",
                "is_wicket_keeper",
                "player_type",
                "bowl_major_type",
                "player_image_url",
                "load_timestamp"
            ]]

            new_players_df = players_df.loc[players_df['player_id'].isnull()]
            max_key_val = getMaxId(session, PLAYERS_TABLE_NAME, PLAYERS_KEY_COL, DB_NAME, False)
            # Add player id to new players
            new_players_df['player_id'] = new_players_df['src_player_id'].rank(
                method='dense', ascending=False
            ).apply(lambda x: x + max_key_val).astype(int)

            updated_players_df = players_df.loc[players_df['player_id'].notnull()].copy()
            # Players who have src_player_id already in db, will have same player_id
            updated_players_df['player_id'] = updated_players_df['player_id'].astype(int)
            players_final_df = new_players_df.append(updated_players_df)
            players_final_df['team_id'] = players_final_df['team_id'].astype(int)
            logger.info("Players Data Generation Completed!")
            return players_final_df.to_dict(orient='records')
    else:
        logger.info("No New Players Data Available!")


def get_cricsheet_players(ball_by_ball_df, players_existing_mapping, squad):
    if squad:
        players_df = pd.DataFrame(columns=['season', 'team_name', 'competition_name', 'src_player_id'])
        season = ball_by_ball_df['season'].iloc[0]
        batting_team = ball_by_ball_df['batting_team'].iloc[0].upper()
        bowling_team = ball_by_ball_df['bowling_team'].iloc[0].upper()
        competition_name = ball_by_ball_df['competition_name'].iloc[0]
        batting_team_players = [
            str(players_existing_mapping[player]) for player in squad.get(batting_team)
        ]
        bowling_team_players = [
            str(players_existing_mapping[player]) for player in squad.get(bowling_team)
        ]

        for player in batting_team_players:
            players_df = players_df.append(
                {
                    'src_player_id': player,
                    'team_name': batting_team
                },
                ignore_index=True
            )
        for player in bowling_team_players:
            players_df = players_df.append(
                {
                    'src_player_id': player,
                    'team_name': bowling_team
                },
                ignore_index=True
            )
        players_df['competition_name'] = competition_name
        players_df['season'] = season
        players_existing_df = getPandasFactoryDF(session, GET_PLAYER_MAPPER_SQL)
        players_existing_df['id'] = players_existing_df['id'].astype(str)
        players_cricsheet_df = pd.merge(
            players_df,
            players_existing_df[['id', 'striker_batting_type', 'bowler_sub_type', 'name']],
            left_on='src_player_id',
            right_on='id',
            how='inner'
        ).drop(['id'], axis=1).rename(
            columns={
                'striker_batting_type': 'batting_type',
                'bowler_sub_type': 'bowling_type',
                'name': 'player_name'
            })
    else:
        ball_by_ball_df['season'] = ball_by_ball_df['season'].apply(lambda x: int(str(x).split('/')[0]))
        # Rename to get player_name and team_name
        players_striker_df = ball_by_ball_df.rename(
            columns={
                'batsman': 'player_name',
                'batting_team': 'team_name',
                'batsman_id': 'src_player_id'
            }
        )
        players_non_striker_df = ball_by_ball_df.rename(
            columns={
                'non_striker': 'player_name',
                'batting_team': 'team_name',
                'non_striker_id': 'src_player_id'
            }
        )
        players_bowler_df = ball_by_ball_df.rename(
            columns={
                'bowler': 'player_name',
                'bowling_team': 'team_name',
                'bowler_id': 'src_player_id'
            }
        )
        players_existing_df = getPandasFactoryDF(session, GET_PLAYER_MAPPER_SQL)
        players_striker_df['src_player_id'] = players_striker_df['src_player_id'].replace(
            players_existing_mapping
        ).astype(str)
        players_non_striker_df['src_player_id'] = players_non_striker_df['src_player_id'].replace(
            players_existing_mapping
        ).astype(str)
        players_bowler_df['src_player_id'] = players_bowler_df['src_player_id'].replace(
            players_existing_mapping
        ).astype(str)

        # Append the data for all 3 types of player
        players_df = players_striker_df.append(players_non_striker_df, ignore_index=True)
        players_df = players_df.append(players_bowler_df, ignore_index=True)
        players_df = players_df[[
            "season",
            "player_name",
            "src_player_id",
            "team_name",
            "competition_name"
        ]].drop_duplicates()
        players_existing_df = getPandasFactoryDF(session, GET_PLAYER_MAPPER_SQL)
        players_existing_df['id'] = players_existing_df['id'].astype(str)
        players_cricsheet_df = pd.merge(
            players_df,
            players_existing_df[['id', 'striker_batting_type', 'bowler_sub_type', 'name']],
            left_on='src_player_id',
            right_on='id',
            how='inner'
        ).rename(
            columns={
                'striker_batting_type': 'batting_type',
                'bowler_sub_type': 'bowling_type'
            })
    players_cricsheet_df["bowl_major_type"] = players_cricsheet_df["bowling_type"].map(bowl_major_type_dict)
    players_cricsheet_df['bowling_type'].replace('', 'NA', inplace=True)
    players_cricsheet_df[[
        "bowl_major_type", "batting_type", "bowling_type"
    ]] = players_cricsheet_df[[
        "bowl_major_type", "batting_type", "bowling_type"
    ]].fillna("NA")
    players_cricsheet_df.replace(to_replace='nan', value='NA', inplace=True)
    players_cricsheet_df["player_name"] = players_cricsheet_df["player_name"].apply(lambda x: x.replace("'", ""))
    players_cricsheet_df['player_image_url'] = (
            IMAGE_STORE_URL +
            'players/' +
            players_cricsheet_df['player_name'].apply(lambda x: x.replace(' ', '-').lower()).astype(str) + ".png"
    )
    players_cricsheet_df['load_timestamp'] = load_timestamp

    # Merge players_merged_df with Teams Dataframe
    teams_df = getPandasFactoryDF(session, GET_TEAM_SQL)
    players_df = players_cricsheet_df.merge(
        teams_df[["team_name", "team_id"]],
        on='team_name',
        how='left'
    ).drop('team_name', axis=1)
    # Get existing df from target table
    players_existing_df = getPandasFactoryDF(session, GET_PLAYERS_SQL)
    players_df['season'] = players_df['season'].astype(int)
    merged_players = pd.merge(
        players_df,
        players_existing_df[['src_player_id', 'season', 'competition_name', 'team_id']],
        how='left',
        on=['src_player_id', 'season', 'competition_name', 'team_id'],
        indicator=True
    )

    merged_players = merged_players[merged_players['_merge'] == "left_only"]

    if not merged_players.empty:
        players_df = pd.merge(
            merged_players,
            players_existing_df[['src_player_id', 'player_id']].drop_duplicates(),
            how='left',
            on=['src_player_id']
        )

        if 'player_id_y' in players_df.columns:
            players_df = players_df.drop('player_id_y', axis=1).rename(columns={'player_id_x': 'player_id'})

        players_df['player_skill'] = "NA"

        player_mapping_df = getPandasFactoryDF(
            session,
            f"select id, is_wicket_keeper, is_batsman, is_bowler from {PLAYER_MAPPER_TABLE_NAME}"
        )
        player_mapping_df['id'] = player_mapping_df['id'].astype(str)
        players_df = pd.merge(
            players_df,
            player_mapping_df,
            left_on='src_player_id',
            right_on='id',
            how='inner'
        )
        players_df['player_skill'] = np.where(
            players_df['is_wicket_keeper'] == 1,
            'WICKETKEEPER',
            players_df['player_skill']
        )
        players_df['player_skill'] = np.where(
            (players_df['is_batsman'] == 1) & (players_df['is_bowler'] == 1),
            'ALLROUNDER',
            players_df['player_skill']
        )
        players_df['player_skill'] = np.where(
            (players_df['is_batsman'] == 1) & (players_df['is_bowler'] == 0),
            'BATSMAN',
            players_df['player_skill']
        )
        players_df['player_skill'] = np.where(
            (players_df['is_batsman'] == 0) & (players_df['is_bowler'] == 1),
            'BOWLER',
            players_df['player_skill']
        )
        players_df = player_ethnicity(players_df)[[
            "player_id",
            "src_player_id",
            "player_name",
            "batting_type",
            "bowling_type",
            "player_skill",
            "team_id",
            "season",
            "competition_name",
            "is_batsman",
            "is_bowler",
            "is_wicket_keeper",
            "player_type",
            "bowl_major_type",
            "player_image_url",
            "load_timestamp"
        ]]
        # Extract out player whose player id is not, that means they are new players to be ingested to database
        new_players_df = players_df.loc[players_df['player_id'].isnull()]
        # Give id to players based on dense method, choose minimum fir
        max_key_val = getMaxId(session, PLAYERS_TABLE_NAME, PLAYERS_KEY_COL, DB_NAME, False)
        new_players_df['player_id'] = new_players_df['src_player_id'].rank(
            method='dense',
            ascending=False
        ).apply(
            lambda x: x + max_key_val
        ).astype(int)
        updated_players_df = players_df.loc[players_df['player_id'].notnull()].copy()
        updated_players_df['player_id'] = updated_players_df['player_id'].astype(int)
        # Append Existing player dataframe to New Players
        players_final_df = new_players_df.append(updated_players_df)
        players_final_df[[
            'team_id', 'is_batsman', 'is_wicket_keeper', 'is_bowler'
        ]] = players_final_df[[
            'team_id', 'is_batsman', 'is_wicket_keeper', 'is_bowler'
        ]].fillna(-1).astype(int)
        players_final_df['is_captain'] = 0
        players_final_df['is_batsman'] = players_final_df['is_batsman'].astype(int)
        players_final_df['is_bowler'] = players_final_df['is_bowler'].astype(int)
        players_final_df['is_wicket_keeper'] = players_final_df['is_wicket_keeper'].astype(int)
        return players_final_df.to_dict(orient='records')
