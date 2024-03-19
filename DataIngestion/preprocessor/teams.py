import os
import sys

import numpy as np
import pandasql as psql

from DataIngestion import load_timestamp, config

sys.path.append("./../../")
sys.path.append("./")

from common.dao_client import session

import pandas as pd
from DataIngestion.query import GET_TEAM_SQL
from log.log import get_logger
from common.dao.fetch_db_data import getMaxId, getPandasFactoryDF
from common.db_config import DB_NAME
from DataIngestion.utils.helper import generateSeq, getMatchScheduleData, checkPlayoff, checkTitle, getListTill, random_string_generator, readCSV
from DataIngestion.config import (
    TEAMS_TABLE_NAME,
    TEAMS_KEY_COL,
    IMAGE_STORE_URL, TEAM_COLOR_PATH, FILE_SHARE_PATH, TEAM_MAPPING_PATH
)

pd.set_option('display.max_columns', 50)

logger = get_logger("Ingestion", "Ingestion")


def getTeamsData(session, root_data_files, squad_data_files, load_timestamp):
    logger.info("Teams Data Generation Started!")
    current_teams_df = pd.DataFrame()
    if root_data_files:
        path_set = set(value for key, value in root_data_files.items()
                       if 'matchschedule' in key.split("-")[1].split(".")[0].strip().lower())

        teams_df = getMatchScheduleData(path_set)

        # Replace spelling mistakes in the 'team_name' column of teams_df
        teams_df['FirstBattingTeamName'].replace(config.TEAM_SPELL_MAPPING, inplace=True)
        teams_df['SecondBattingTeamName'].replace(config.TEAM_SPELL_MAPPING, inplace=True)
        teams_df['TossTeam'].replace(config.TEAM_SPELL_MAPPING, inplace=True)

        # Now teams_df has corrected team names

        teams_df["is_playoff"] = teams_df["MatchDateOrder"].apply(checkPlayoff)

        # getting is_title from MatchDateOrder key
        teams_df["is_title"] = teams_df["MatchDateOrder"].apply(checkTitle)

        # getting team who won the title match
        teams_df.loc[teams_df['is_title'] == 1, 'title_won'] = teams_df["Comments"].str.split(' ') \
            .apply(getListTill).str.join(" ")

        # getting distinct teams
        teams1 = psql.sqldf('''select distinct FirstBattingTeamID as src_team_id, FirstBattingTeamName as team_name, 
        competition_name from teams_df ''')

        # getting distinct teams
        teams2 = psql.sqldf('''select distinct SecondBattingTeamID as src_team_id, SecondBattingTeamName as team_name, 
        competition_name from teams_df ''')
        teams = (teams1.append(teams2, ignore_index=True)).drop_duplicates()
        teams['team_name'] = teams['team_name'].str.upper()

        # getting seasons each team played
        season = psql.sqldf('''select distinct FirstBattingTeamID as src_team_id, seasons as seasons_played 
        from teams_df''')
        season_sec = psql.sqldf('''select distinct SecondBattingTeamID as src_team_id, seasons as seasons_played
                from teams_df''')

        season = (season.append(season_sec, ignore_index=True)).drop_duplicates()

        # # getting list of seasons for a team
        season = season.groupby(["src_team_id"])[["seasons_played"]].agg(set).reset_index()
        season["seasons_played"] = season["seasons_played"].apply(list)

        # getting no. of titles won by teams
        titles = psql.sqldf('''select title_won as team,count(title_won) as titles from teams_df where title_won<>"None" 
        group by title_won ''')

        # getting no. of times a team has qualified for playoffs
        playoffs1 = psql.sqldf('''select FirstBattingTeamID as team, FirstBattingTeamName as team_name, seasons as 
        seasons_played from teams_df where is_playoff=1 ''')

        playoffs2 = psql.sqldf('''select SecondBattingTeamID as team, SecondBattingTeamName as team_name, seasons as 
        seasons_played from teams_df where is_playoff=1 ''')

        playoffs = playoffs1.append(playoffs2)

        playoffs = psql.sqldf('''select team, team_name, count(distinct seasons_played) as playoffs from playoffs 
        group by team, team_name''')

        # merging all the dataframes
        current_teams_df = pd.merge(teams, season, how="left", on="src_team_id")

        current_teams_df = pd.merge(current_teams_df, titles, how="left", left_on="team_name", right_on="team") \
            .drop(["team"], axis=1)

        current_teams_df = pd.merge(current_teams_df, playoffs, how="left", left_on="src_team_id", right_on="team") \
            .rename(columns={"team_name_x": "team_name"}) \
            .drop(["team", "team_name_y"], axis=1)

        # Add team short name
        team_mapping = readCSV(TEAM_MAPPING_PATH)
        current_teams_df = pd.merge(
            current_teams_df,
            team_mapping[["team_name", "team_short_name"]],
            how="left",
            on="team_name"
        )

        current_teams_df['team_short_name'] = np.where(current_teams_df['team_name'] == 'Deccan Chargers', 'DCH',
                                                       current_teams_df['team_short_name'])

        current_teams_df[["titles", "playoffs"]] = current_teams_df[["titles", "playoffs"]].fillna(0).astype(int)

        # getting existing data from the target
        existing_df = getPandasFactoryDF(session, GET_TEAM_SQL)

        old_ipl_teams = ['PWI', 'DCH', 'KTK', 'GL', 'PSG']

        final_df = pd.merge(
            current_teams_df,
            existing_df,
            how='left',
            on=['team_name', 'competition_name'],
            sort=False
        )

        final_df['seasons_played'] = final_df['seasons_played_x'] + final_df['seasons_played_y'].fillna("").apply(list)
        final_df['seasons_played'] = final_df['seasons_played'].apply(lambda x: list(set(x)))

        final_df['titles'] = final_df['titles_x'] + final_df['titles_y'].fillna(0).astype(int)
        final_df['playoffs'] = final_df['playoffs_x'] + final_df['playoffs_y'].fillna(0).astype(int)

        final_df = final_df.rename(
            columns={'competition_name_x': 'competition_name', 'team_short_name_x': 'team_short_name',
                     'src_team_id_x': 'src_team_id'}) \
            .drop(columns=['seasons_played_x', 'seasons_played_y', 'team_short_name_y',
                           'src_team_id_y', 'titles_x', 'titles_y', 'playoffs_x', 'playoffs_y'], axis=1)

        final_df = final_df.drop(['team_short_name'], axis=1)
        final_df['team_name'] = final_df['team_name'].str.upper()
        # Adding column load timestamp
        final_df["load_timestamp"] = load_timestamp
        final_df['team_image_url'] = (
                IMAGE_STORE_URL +
                'teams/' +
                final_df['competition_name'] +
                '/' +
                final_df['team_name'].apply(lambda x: x.replace(' ', '-').lower()).astype(str) + ".png"
        )
        # Merge with teams_color with new_df
        teams_color = readCSV(TEAM_COLOR_PATH)
        final_df = pd.merge(
            final_df,
            teams_color[['color_code', 'color_code_gradient', 'team_short_name', 'team_name']],
            on='team_name',
            how='left'
        )
        new_df = final_df.loc[final_df[TEAMS_KEY_COL].isnull()]
        new_df = new_df.drop(['team_short_name'], axis=1)
        new_df['team_name'] = new_df['team_name'].str.upper()
        team_mapping = readCSV(os.path.join(FILE_SHARE_PATH, "data/team_mapping.csv"))
        new_df = pd.merge(
            new_df,
            team_mapping,
            on='team_name',
            how='inner'
        )
        max_key_val = getMaxId(session, TEAMS_TABLE_NAME, TEAMS_KEY_COL, DB_NAME)
        new_data = generateSeq(new_df.drop(TEAMS_KEY_COL, axis=1).sort_values(["competition_name", "team_name"]),
                               TEAMS_KEY_COL, max_key_val).to_dict(orient='records')

        update_df = final_df.loc[final_df[TEAMS_KEY_COL].notnull()].copy()
        update_df['team_id'] = update_df['team_id'].astype(int)
        updated_data = update_df.to_dict(orient='records')
        logger.info("Teams Data Generation Completed!")
        return updated_data, new_data
    else:
        logger.info("No New Teams Data Available!")


def get_cricsheet_team_data(ball_by_ball_df):
    team_mapping = readCSV(os.path.join(FILE_SHARE_PATH, "data/team_mapping.csv"))
    ball_by_ball_df['season'] = ball_by_ball_df['season'].apply(lambda x: int(str(x).split('/')[0]))

    teams_bat_df = ball_by_ball_df[['season', 'batting_team', 'competition_name']].rename(
        columns={
            'batting_team': 'team_name',
            'season': 'seasons_played'
        }
    )
    teams_bowl_df = ball_by_ball_df[['season', 'bowling_team', 'competition_name']].rename(
        columns={
            'bowling_team': 'team_name',
            'season': 'seasons_played'
        }
    )
    teams_df = teams_bat_df.append(teams_bowl_df, ignore_index=True).drop_duplicates()
    teams_df["seasons_played"] = teams_df["seasons_played"].astype(int)
    teams_df = teams_df.groupby(["team_name", "competition_name"])[["seasons_played"]].agg(set).reset_index()
    teams_df["seasons_played"] = teams_df["seasons_played"].apply(list)
    teams_df["src_team_id"] = teams_df["team_name"].apply(lambda x: random_string_generator(x))
    teams_df["team_name"] = teams_df["team_name"].str.upper()
    teams_df = pd.merge(
        teams_df,
        team_mapping,
        on='team_name',
        how='inner'
    )
    teams_df[['playoffs', 'titles']] = 0

    existing_df = getPandasFactoryDF(session, GET_TEAM_SQL)
    old_ipl_teams = ['PWI', 'DCH', 'KTK', 'GL', 'PSG']
    final_df = pd.merge(
        teams_df,
        existing_df,
        how='left',
        on=['team_name', 'competition_name'],
        sort=False
    )
    # final_df['seasons_played_x'] = np.where(
    #     (final_df['competition_name_x'] == 'IPL') & (final_df['team_short_name_x'].isin(old_ipl_teams) == False),
    #     final_df['seasons_played_x'].apply(lambda x: x + []),
    #     final_df['seasons_played_x']
    # )
    # Combine both seasons of cricsheet and existing teams table dataframe
    final_df['seasons_played'] = final_df['seasons_played_x'] + final_df['seasons_played_y'].fillna("").apply(list)
    final_df['seasons_played'] = final_df['seasons_played'].apply(lambda x: list(set(x)))

    final_df['titles'] = final_df['titles_x'] + final_df['titles_y'].fillna(0).astype(int)
    final_df['playoffs'] = final_df['playoffs_x'] + final_df['playoffs_y'].fillna(0).astype(int)

    final_df = final_df.rename(
        columns={
            'competition_name_x': 'competition_name',
            'team_short_name_x': 'team_short_name',
            'src_team_id_x': 'src_team_id'
        }
    ).drop(
        columns=[
            'seasons_played_x',
            'seasons_played_y',
            'team_short_name_y',
            'src_team_id_y',
            'titles_x',
            'titles_y',
            'playoffs_x',
            'playoffs_y'
        ], axis=1
    )

    final_df = final_df[final_df['team_short_name'].notna()]
    # adding column load timestamp
    final_df["load_timestamp"] = load_timestamp
    final_df['team_image_url'] = (
            IMAGE_STORE_URL +
            'teams/' +
            final_df['competition_name'] +
            '/' +
            final_df['team_name'].apply(lambda x: x.replace(' ', '-').lower()).astype(str) + ".png"
    )
    # Merge with teams_color with new_df
    teams_color = readCSV(TEAM_COLOR_PATH)
    final_df = pd.merge(
        final_df,
        teams_color[['color_code', 'color_code_gradient', 'team_name']],
        on='team_name',
        how='left'
    )
    new_df = final_df.loc[final_df[TEAMS_KEY_COL].isnull()]
    new_data = generateSeq(
        new_df.drop(TEAMS_KEY_COL, axis=1).sort_values(["competition_name", "team_name"]),
        TEAMS_KEY_COL,
        getMaxId(session, TEAMS_TABLE_NAME, TEAMS_KEY_COL, DB_NAME, False)
    ).to_dict(orient='records')

    update_df = final_df.loc[final_df[TEAMS_KEY_COL].notnull()].copy()
    update_df['team_id'] = update_df['team_id'].astype(int)
    updated_data = update_df.to_dict(orient='records')
    return updated_data, new_data
