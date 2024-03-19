import json
import os
import sys

import numpy as np
import pandas as pd

from DataIngestion import load_timestamp

sys.path.append("./../../")
sys.path.append("./")
from log.log import get_logger

from DataIngestion.config import MATCHES_TABLE_NAME, MATCHES_KEY_COL, MATCHES_REQD_COLS, SQUAD_KEY_LIST, \
    FILE_SHARE_PATH
from common.dao.fetch_db_data import getPandasFactoryDF, getMaxId
from DataIngestion.query import GET_TEAM_SQL, GET_EXISTING_MATCHES_SQL, GET_PLAYER_MAPPER_SQL, \
    GET_PLAYERS_SQL
from DataIngestion.utils.helper import (
    readJsFile,
    dataToDF,
    generateSeq,
    getSquadRawData,
    checkPlayoff,
    checkTitle,
    getListTill,
    getPitchTypeData,
    getMatchScheduleData,
    readCSV
)
from DataIngestion.query import (GET_PLAYER_DETAILS_SQL, GET_VENUE_DETAILS_SQL)
from common.db_config import DB_NAME
import pandasql as psql
from datetime import datetime

from common.dao_client import session

pd.options.mode.chained_assignment = None

logger = get_logger("Ingestion", "Ingestion")


def getMatchesData(session, root_data_files, squad_data_files, PITCH_TYPE_DATA_PATH, load_timestamp,
                   pitch_data_path_2019_to_2021):
    logger.info("Matches Data Generation Started!")
    if root_data_files:
        path_set = set(value for key, value in root_data_files.items()
                       if 'matchschedule' in key.split("-")[1].split(".")[0].strip().lower())

        match_data = getMatchScheduleData(path_set)[MATCHES_REQD_COLS]

        existing_matches_data = getPandasFactoryDF(session, GET_EXISTING_MATCHES_SQL)
        existing_matches_list = existing_matches_data['src_match_id'].tolist()

        match_data = match_data[~match_data['MatchID'].isin(existing_matches_list)]

        if not match_data.empty:

            teams_df = getPandasFactoryDF(session, GET_TEAM_SQL)[['team_id', 'src_team_id', 'team_name']]

            matches_df = pd.merge(
                match_data,
                teams_df[['team_id', 'src_team_id']],
                how='left',
                left_on='FirstBattingTeamID',
                right_on='src_team_id'
            ).rename(
                columns={'team_id': 'team1'}
            ).drop(['FirstBattingTeamID', 'src_team_id'], axis=1)

            matches_df = pd.merge(
                matches_df,
                teams_df[['team_id', 'src_team_id']],
                how='left',
                left_on='SecondBattingTeamID',
                right_on='src_team_id'
            ).rename(
                columns={'team_id': 'team2'}
            ).drop(['SecondBattingTeamID', 'src_team_id'], axis=1)

            matches_df = pd.merge(
                matches_df,
                teams_df[['team_id', 'team_name']],
                how='left',
                left_on=matches_df['TossTeam'].str.replace(" ", "").str.strip().str.lower(),
                right_on=teams_df['team_name'].str.replace(" ", "").str.strip().str.lower()
            ).rename(
                columns={'team_id': 'toss_team'}
            ).drop(['TossTeam', 'team_name', 'key_0'], axis=1)

            matches_df["is_playoff"] = matches_df["MatchDateOrder"].apply(checkPlayoff)
            # getting is_title from MatchDateOrder key
            matches_df["is_title"] = matches_df["MatchDateOrder"].apply(checkTitle)

            # Generating a set of source files
            match_path_set = set(
                value for key, value in root_data_files.items()
                if 'matchsummary' in key.split("-")[1].split(".")[0].strip().lower()
            )
            # Initializing empty DF
            append_match_summary_df = pd.DataFrame()

            # Reading each file one by one from path set
            for path in match_path_set:
                # Reading the JS file
                matches_data = readJsFile(path)['MatchSummary'][0]

                # Getting specific required keys
                matches_dict = {
                    key: str(matches_data[key]).split('(')[0].strip() for key in
                    matches_data.keys() & {'MatchID', 'WinningTeamID', 'Target'}
                }

                match_summary_df = dataToDF([matches_dict])
                append_match_summary_df = append_match_summary_df.append(match_summary_df)

            matches_df = pd.merge(matches_df, append_match_summary_df, how='left', on="MatchID")

            null_result_df = matches_df[matches_df["WinningTeamID"].isnull()][["MatchID", "Comments"]]
            null_result_df["winning_team_name"] = null_result_df["Comments"].str.split(' ').apply(getListTill).str.join(
                " ")
            null_result_df = pd.merge(
                null_result_df,
                teams_df[['team_id', 'team_name']],
                how='left',
                left_on=null_result_df['winning_team_name'].str.replace(
                    " ", "").str.strip().str.lower(),
                right_on=teams_df['team_name'].str.replace(" ", "").str.strip().str.lower()
            ).rename(
                columns={'team_id': 'winning_team'}
            ).drop(['winning_team_name', 'team_name', 'key_0'], axis=1)

            matches_df = psql.sqldf('''
            select 
              mdf.MatchID as src_match_id, 
              mdf.MatchDate as match_date, 
              mdf.MatchTime as match_time, 
              mdf.GroundName, 
              mdf.TossDetails as toss_decision, 
              mdf.FirstBattingSummary, 
              mdf.SecondBattingSummary, 
              mdf.is_playoff, 
              mdf.competition_name, 
              mdf.seasons as season, 
              mdf.team1, 
              mdf.team2, 
              mdf.toss_team, 
              mdf.Comments as match_result, 
              coalesce(
                coalesce(tdf.team_id, nrdf.winning_team), 
                -1
              ) as winning_team, 
              mdf.is_title, 
              mdf.MatchName as match_name, 
              Target as team2_target 
            from 
              matches_df mdf 
              left join teams_df tdf on (
                mdf.WinningTeamID = tdf.src_team_id
              ) 
              left join null_result_df nrdf on (mdf.MatchID = nrdf.MatchID)
            ''')

            matches_df["winning_team"] = matches_df["winning_team"].astype(int)

            venues_df = getPandasFactoryDF(session, GET_VENUE_DETAILS_SQL)[['stadium_name', 'venue_id']]
            venue_mapping = os.path.join(FILE_SHARE_PATH, "data/venue_mapping.json")
            with open(venue_mapping, 'r', encoding='utf-8') as file:
                venue_mapping_content = json.loads(file.read())
            matches_df['GroundName'] = matches_df['GroundName'].str.upper()
            matches_df['GroundName'] = matches_df['GroundName'].replace(venue_mapping_content)
            matches_df = pd.merge(
                matches_df,
                venues_df,
                how='left',
                left_on=matches_df['GroundName'].str.replace(" ", "").str.strip().str.lower(),
                right_on=venues_df['stadium_name'].str.replace(" ", "").str.strip().str.lower()
            ).rename(
                columns={'venue_id': 'venue'}
            ).drop(['stadium_name', 'GroundName', 'key_0'], axis=1)
            matches_df['venue'] = matches_df['venue'].astype(int)
            matches_df["team1_score"] = matches_df["FirstBattingSummary"].apply(
                lambda x: int(x.split("/")[0]) if (x != "") else 0
            )
            matches_df["team1_wickets"] = matches_df["FirstBattingSummary"].apply(
                lambda x: int(x.split("/")[1].split(" ")[0]) if (x != "") else 0
            )
            matches_df["team1_overs"] = matches_df["FirstBattingSummary"].apply(
                lambda x: x.split(" ")[1].strip().replace("(", "") if (x != "") else 0
            )
            matches_df["team2_score"] = matches_df["SecondBattingSummary"].apply(
                lambda x: int(x.split("/")[0]) if (x != "") else 0
            )
            matches_df["team2_wickets"] = matches_df["SecondBattingSummary"].apply(
                lambda x: int(x.split("/")[1].split(" ")[0]) if (x != "") else 0
            )
            matches_df["team2_overs"] = matches_df["SecondBattingSummary"].apply(
                lambda x: x.split(" ")[1].strip().replace("(", "") if (x != "") else 0
            )

            players_df = getPandasFactoryDF(session, GET_PLAYER_DETAILS_SQL).drop_duplicates()
            squad_key_cols = {"TeamID", "src_match_id", "PlayerID"}
            squad_df = getSquadRawData(squad_data_files, SQUAD_KEY_LIST, squad_key_cols)
            squad_df = pd.merge(
                squad_df,
                teams_df[['team_id', 'src_team_id']],
                how="left",
                left_on="TeamID",
                right_on="src_team_id"
            ).drop("TeamID", axis=1)
            players_existing_df = getPandasFactoryDF(session, GET_PLAYER_MAPPER_SQL)
            squad_df = pd.merge(
                players_existing_df[["id", "sports_mechanics_id"]],
                squad_df,
                left_on='sports_mechanics_id',
                right_on='PlayerID',
                how='inner'
            ).drop(['sports_mechanics_id', 'PlayerID'], axis=1).rename(
                columns={
                    'id': 'PlayerID'
                }
            )
            squad_df['PlayerID'] = squad_df['PlayerID'].astype(str)
            squad_df = pd.merge(
                squad_df,
                players_df[['src_player_id', 'player_id']],
                how="left",
                left_on="PlayerID",
                right_on="src_player_id"
            ).drop("PlayerID", axis=1)[["src_match_id", "player_id", "team_id"]]

            squad_df["src_match_id"] = squad_df["src_match_id"].str.strip().astype(str)

            squad_df = squad_df.groupby(["src_match_id", "team_id"])["player_id"].agg(list).reset_index()

            matches_df = pd.merge(
                matches_df,
                squad_df,
                how="left",
                left_on=["team1", "src_match_id"],
                right_on=["team_id", "src_match_id"]
            ).rename(
                columns={"player_id": "team1_players"}
            ).drop(["team_id", "FirstBattingSummary", "SecondBattingSummary"], axis=1)

            matches_df = pd.merge(
                matches_df,
                squad_df,
                how="left",
                left_on=["team2", "src_match_id"],
                right_on=["team_id", "src_match_id"]
            ).rename(
                columns={"player_id": "team2_players"}
            ).drop(["team_id"], axis=1)

            matches_df = pd.merge(
                matches_df,
                getPitchTypeData(PITCH_TYPE_DATA_PATH), how="left",
                left_on="src_match_id",
                right_on="match_id"
            ).drop(["match_id"], axis=1)

            pitch_data_2019_to_2021 = readCSV(pitch_data_path_2019_to_2021)[['match_name', 'Wicket Type']].rename(
                columns={"Wicket Type": "wicket_type"}
            )

            matches_df = pd.merge(
                matches_df,
                pitch_data_2019_to_2021,
                how='left',
                on='match_name'
            )

            matches_df['overall_nature'] = np.where(
                matches_df['overall_nature'].isnull(),
                matches_df['wicket_type'],
                matches_df['overall_nature']
            )

            matches_df['nature_of_wicket'] = np.where(
                matches_df['nature_of_wicket'].isnull(),
                matches_df['wicket_type'],
                matches_df['nature_of_wicket']
            )

            matches_df[[
                "nature_of_wicket", "overall_nature", "dew"
            ]] = matches_df[[
                "nature_of_wicket", "overall_nature", "dew"
            ]].fillna("NA").astype(str)
            matches_df[[
                "nature_of_wicket", "overall_nature", "dew"
            ]] = matches_df[[
                "nature_of_wicket", "overall_nature", "dew"
            ]]
            matches_df = matches_df.drop('wicket_type', axis=1)

            matches_df["team1_players"].loc[
                matches_df["team1_players"].isnull()
            ] = matches_df["team1_players"].loc[
                matches_df["team1_players"].isnull()
            ].apply(lambda x: [])
            matches_df["team2_players"].loc[
                matches_df["team2_players"].isnull()
            ] = matches_df["team2_players"].loc[
                matches_df["team2_players"].isnull()
            ].apply(lambda x: [])

            matches_df["match_date_form"] = matches_df["match_date"].apply(
                lambda x: datetime.strptime(x, '%d %b %Y').strftime('%Y-%m-%d')
            )

            matches_df["load_timestamp"] = load_timestamp
            matches_df['toss_team'] = matches_df['toss_team'].astype(int)
            max_key_val = getMaxId(session, MATCHES_TABLE_NAME, MATCHES_KEY_COL, DB_NAME)
            final_matches_data = generateSeq(
                matches_df.sort_values(['competition_name', 'match_date_form']),
                MATCHES_KEY_COL, max_key_val
            ).to_dict(orient='records')
            logger.info("Matches Data Generation Completed!")
            return final_matches_data
    else:
        logger.info("No New Matches Data Available!")


def get_cricsheet_matches(ball_by_ball_df, players_existing_mapping, squad=None):
    ball_by_ball_df['season'] = ball_by_ball_df['season'].apply(lambda x: int(str(x).split('/')[0]))
    cricsheet_matches_df = ball_by_ball_df.rename(columns={
        "is_extra": "extras",
        "is_wide": "wides",
        "is_no_ball": "noballs",
        "is_bye": "byes",
        "is_leg_bye": "legbyes",
        "batsman": "striker",
        "ball_runs": "runs_off_bat",
        "match_date": "start_date",
        "stadium_name": "venue",
        "over_text": "ball"
    })
    # To check if matches are already in the database.
    existing_matches_data = getPandasFactoryDF(session, GET_EXISTING_MATCHES_SQL)
    existing_matches_list = existing_matches_data['src_match_id'].tolist()
    if ball_by_ball_df['src_match_id'].iloc[0] not in existing_matches_list:
        ############################################################################################################
        #                                   Matches DataFrame Team 1
        ############################################################################################################
        matches_df_t1 = cricsheet_matches_df[[
            'src_match_id',
            'season',
            'start_date',
            'venue',
            'innings',
            'ball',
            'runs_off_bat',
            'extras',
            'wicket_type',
            'competition_name',
            'batting_team',
            'bowling_team',
            'match_name',
            'winning_team',
            'toss_team',
            'toss_decision',
            'match_result',
            'is_playoff'
        ]][cricsheet_matches_df['innings'] == 1].drop('innings', axis=1)
        matches_df_t1['ball'] = matches_df_t1['ball'].astype(float)
        matches_df_t1 = psql.sqldf('''
        select 
          src_match_id as src_match_id, 
          match_name,
          winning_team,
          toss_team,
          toss_decision,
          match_result,
          season, 
          start_date as match_date, 
          venue, 
          competition_name, 
          batting_team as team1, 
          max(
            case 
                when ball == 49.6 then 50
                when ball == 19.6 then 20 
                else ball end
          ) as team1_overs, 
          sum(
            coalesce(runs_off_bat, 0)
          )+ sum(
            coalesce(extras, 0)
          ) as team1_score, 
          sum(
            case when (
              wicket_type is null 
              or wicket_type = ''
            ) then 0 else 1 end
          ) as team1_wickets, 
          bowling_team as inn1_bowl_team, 
          (
            sum(
              coalesce(runs_off_bat, 0)
            )+ sum(
              coalesce(extras, 0)
            ) + 1
          ) as team2_target 
        from 
          matches_df_t1 
        group by 
          src_match_id, 
          season, 
          start_date, 
          venue, 
          competition_name, 
          batting_team, 
          bowling_team
        ''')
        venue_mapping = os.path.join(FILE_SHARE_PATH, "data/venue_mapping.json")
        with open(venue_mapping, 'r', encoding='utf-8') as file:
            venue_mapping_content = json.loads(file.read())
        matches_df_t1['venue'] = matches_df_t1['venue'].str.upper()
        matches_df_t1['venue'] = matches_df_t1['venue'].replace(venue_mapping_content)
        venues_df = getPandasFactoryDF(session, GET_VENUE_DETAILS_SQL)[['stadium_name', 'venue_id']]
        matches_df_t1 = pd.merge(
            matches_df_t1,
            venues_df,
            how='left',
            left_on=matches_df_t1['venue'],
            right_on=venues_df['stadium_name']
        ).drop(
            ['stadium_name', 'venue', 'key_0'], axis=1
        ).rename(
            columns={'venue_id': 'venue'}
        )
        ############################################################################################################
        #                                   Matches DataFrame Team 2
        ############################################################################################################
        matches_df_t2 = cricsheet_matches_df[[
            'src_match_id',
            'innings',
            'ball',
            'runs_off_bat',
            'extras',
            'wicket_type',
            'competition_name',
            'batting_team',
            'match_name',
            'winning_team',
            'toss_team',
            'toss_decision',
            'match_result'
        ]][cricsheet_matches_df['innings'] == 2].drop('innings', axis=1)
        matches_df_t2['ball'] = matches_df_t2['ball'].astype(float)
        matches_df_t2 = psql.sqldf('''
            select 
              src_match_id as src_match_id, 
              match_name,
              winning_team,
              toss_team,
              toss_decision,
              match_result,
              batting_team as team2, 
              max(
                case 
                    when ball == 49.6 then 50
                    when ball == 19.6 then 20 
                    else ball end
              ) as team2_overs, 
              sum(
                coalesce(runs_off_bat, 0)
              )+ sum(
                coalesce(extras, 0)
              ) as team2_score, 
              sum(
                case when (
                  wicket_type is null 
                  or wicket_type = ''
                ) then 0 else 1 end
              ) as team2_wickets 
            from 
              matches_df_t2 
            group by 
              src_match_id, 
              batting_team
        ''')
        teams_df = getPandasFactoryDF(session, GET_TEAM_SQL)[['team_id', 'src_team_id', 'team_name']]
        if squad:
            matches_df_t1['team1_players'] = None
            matches_df_t2['team2_players'] = None
            outer_mapping_team1, outer_mapping_team2 = [], []
            if len(matches_df_t1) != 0:
                outer_mapping_team1 = [
                    str(players_existing_mapping[player]) for player in
                    squad.get((matches_df_t1['team1'].iloc[0]).upper())
                ]
            else:
                try:
                    matches_df_t1 = pd.DataFrame(columns=[
                        'src_match_id', 'match_name', 'winning_team', 'toss_team',
                        'toss_decision', 'match_result', 'match_date',
                        'competition_name', 'team1', 'team1_overs', 'team1_score',
                        'team1_wickets', 'team2_target', 'venue',
                        'team1_players', 'team2', 'team2_overs', 'team2_score', 'team2_wickets',
                        'team2_players']
                    )
                    matches_df_t1['src_match_id'] = ball_by_ball_df['src_match_id'].iloc[0]
                    matches_df_t1['match_name'] = ball_by_ball_df['src_match_id'].iloc[0]
                    matches_df_t1['competition_name'] = ball_by_ball_df['src_match_id'].iloc[0]
                    outer_mapping_team1 = [
                        str(players_existing_mapping[player]) for player in
                        squad.get((ball_by_ball_df['batting_team'].iloc[0]).upper())
                    ]
                    matches_df_t1['team1'] = outer_mapping_team1
                except Exception as empty_df:
                    pass
            if len(matches_df_t2) != 0:
                outer_mapping_team2 = [
                    str(players_existing_mapping[player]) for player in
                    squad.get((matches_df_t2['team2'].iloc[0]).upper())
                ]
            else:
                try:
                    matches_df_t2 = pd.DataFrame(columns=[
                        'src_match_id', 'match_name', 'winning_team', 'toss_team',
                        'toss_decision', 'match_result',
                        'team2', 'team2_overs', 'team2_score', 'team2_wickets',
                        'team2_players']
                    )
                    matches_df_t2.at[0, 'src_match_id'] = ball_by_ball_df['src_match_id'].iloc[0]
                    matches_df_t2.at[0, 'match_name'] = ball_by_ball_df['match_name'].iloc[0]
                    outer_mapping_team2 = [
                        str(players_existing_mapping[player]) for player in
                        squad.get((ball_by_ball_df['bowling_team'].iloc[0]).upper())
                    ]
                except Exception as empty_df:
                    pass
            players_existing_df = getPandasFactoryDF(session, GET_PLAYERS_SQL)
            players_existing_mapping = dict(zip(players_existing_df['src_player_id'], players_existing_df['player_id']))
            matches_df_t1.at[0, 'team1_players'] = [
                players_existing_mapping[player] for player in outer_mapping_team1
            ]
            matches_df_t2.at[0, 'team2_players'] = [
                players_existing_mapping[player] for player in outer_mapping_team2
            ]
        else:
            ############################################################################################################
            #                                  Generating squad for team 1
            ############################################################################################################

            team1_squad_striker = cricsheet_matches_df[[
                'src_match_id',
                'striker',
                'batting_team',
                'batsman_id'
            ]][cricsheet_matches_df['innings'] == 1].rename(
                columns={
                    'striker': 'player_name',
                    'batting_team': 'team_name',
                    'batsman_id': 'player_id'
                }
            )

            team1_squad_non_striker = cricsheet_matches_df[[
                'src_match_id',
                'non_striker',
                'batting_team',
                'non_striker_id'
            ]][cricsheet_matches_df['innings'] == 1].rename(
                columns={
                    'non_striker': 'player_name',
                    'batting_team': 'team_name',
                    'non_striker_id': 'player_id'
                }
            )
            team1_squad_bowler = cricsheet_matches_df[[
                'src_match_id',
                'bowler',
                'bowling_team',
                'bowler_id'
            ]][cricsheet_matches_df['innings'] == 2].rename(
                columns={
                    'bowler': 'player_name',
                    'bowling_team': 'team_name',
                    'bowler_id': 'player_id'
                }
            )

            team_squad = team1_squad_striker.append(
                team1_squad_non_striker, ignore_index=True
            ).append(
                team1_squad_bowler, ignore_index=True
            ).drop_duplicates()
            team_squad['player_name'] = team_squad['player_name'].apply(lambda x: x.replace("'", ""))
            team1_squad = team_squad.groupby(['src_match_id', 'team_name'])['player_id'].agg(list).reset_index()
            matches_df_t1 = matches_df_t1.merge(
                team1_squad,
                left_on=['src_match_id', 'team1'],
                right_on=['src_match_id', 'team_name'],
                how='left'
            ).rename(
                columns={'player_id': 'team1_players'}
            ).drop('team_name', axis=1)

            ############################################################################################################
            #                                  Generating squad for team 2
            ############################################################################################################

            team2_squad_striker = cricsheet_matches_df[[
                'src_match_id',
                'striker',
                'batting_team',
                'batsman_id'
            ]][cricsheet_matches_df['innings'] == 2].rename(
                columns={
                    'src_match_id': 'src_match_id',
                    'striker': 'player_name',
                    'batting_team': 'team_name',
                    'batsman_id': 'player_id'
                }
            )

            team2_squad_non_striker = cricsheet_matches_df[[
                'src_match_id',
                'non_striker',
                'batting_team',
                'non_striker_id'
            ]][cricsheet_matches_df['innings'] == 2].rename(
                columns={
                    'src_match_id': 'src_match_id',
                    'non_striker': 'player_name',
                    'batting_team': 'team_name',
                    'non_striker_id': 'player_id'
                }
            )
            team2_squad_bowler = cricsheet_matches_df[[
                'src_match_id',
                'bowler',
                'bowling_team',
                'bowler_id'
            ]][cricsheet_matches_df['innings'] == 1].rename(
                columns={
                    'src_match_id': 'src_match_id',
                    'bowler': 'player_name',
                    'bowling_team': 'team_name',
                    'bowler_id': 'player_id'
                }
            )

            team_squad = team2_squad_striker.append(
                team2_squad_non_striker, ignore_index=True
            ).append(
                team2_squad_bowler,
                ignore_index=True
            ).drop_duplicates()
            team_squad['player_name'] = team_squad['player_name'].apply(lambda x: x.replace("'", ""))
            team2_squad = team_squad.groupby(['src_match_id', 'team_name'])['player_id'].agg(list).reset_index()
            matches_df_t2 = matches_df_t2.merge(
                team2_squad,
                left_on=['src_match_id', 'team2'],
                right_on=['src_match_id', 'team_name'],
                how='left'
            ).rename(
                columns={'player_id': 'team2_players'}
            ).drop('team_name', axis=1)

        ############################################################################################################
        #                                  Merging team1 and team2 dataframe
        ############################################################################################################
        matches_df = matches_df_t1.merge(matches_df_t2, on='src_match_id', how='left').rename(
            columns={
                'match_name_x': 'match_name',
                'winning_team_x': 'winning_team',
                'toss_decision_x': 'toss_decision',
                'toss_team_x': 'toss_team',
                'match_result_x': 'match_result'
            }
        ).drop(['match_name_y', 'winning_team_y', 'toss_team_y', 'toss_decision_y', 'match_result_y'], axis=1)

        matches_df['team2'] = matches_df['team2'].fillna(matches_df['inn1_bowl_team'])
        teams_mapping = dict(zip(teams_df['team_name'], teams_df['team_id']))
        matches_df['team1'] = matches_df['team1'].str.upper().replace(teams_mapping)
        matches_df['team2'] = matches_df['team2'].str.upper().replace(teams_mapping)
        matches_df['winning_team'] = matches_df['winning_team'].str.upper().replace(teams_mapping)
        matches_df['toss_team'] = matches_df['toss_team'].str.upper().replace(teams_mapping)
        matches_df['src_match_id'] = matches_df['src_match_id'].astype(str)
        matches_df[['is_playoff', 'is_title']] = 0
        matches_df[['match_time', 'overall_nature', 'dew']] = 'NA'
        matches_df['nature_of_wicket'] = 'TRUE'
        matches_df[[
            'team2_score',
            'team2_wickets',
            'team1_score',
            'team1_wickets'
        ]] = matches_df[[
            'team2_score',
            'team2_wickets',
            'team1_score',
            'team1_wickets'
        ]].fillna(0).astype(int)

        matches_df[[
            'team1_players',
            'team2_players'
        ]] = matches_df[[
            'team1_players',
            'team2_players'
        ]].fillna(str([]))
        matches_df["team1_players"].loc[matches_df["team1_players"].isnull()] = matches_df["team1_players"].loc[
            matches_df["team1_players"].isnull()].apply(lambda x: [])

        matches_df["team2_players"].loc[matches_df["team2_players"].isnull()] = matches_df["team2_players"].loc[
            (matches_df["team2_players"].isnull()) | (matches_df["team2_players"] == "")].apply(lambda x: [])

        matches_df[[
            'team1_overs',
            'team2_overs'
        ]] = matches_df[[
            'team1_overs',
            'team2_overs'
        ]].fillna(0.0)

        matches_df['match_date_form'] = matches_df['match_date'].apply(lambda x: x.split(" ")[0])
        matches_df['match_date'] = pd.to_datetime(matches_df['match_date'], format='%Y-%m-%d').apply(
            lambda x: x.strftime('%d %b %Y'))
        matches_df["load_timestamp"] = load_timestamp
        matches_df[["winning_team"]] = matches_df[['winning_team']].replace("NONE", -1)
        max_key_val = getMaxId(session, MATCHES_TABLE_NAME, MATCHES_KEY_COL, DB_NAME, False)
        final_matches_data = generateSeq(
            matches_df.drop_duplicates(
                subset=['match_name'], keep='first'
            ).drop(
                'inn1_bowl_team', axis=1
            ).sort_values(
                ['competition_name', 'match_date_form']
            ),
            MATCHES_KEY_COL, max_key_val
        ).to_dict(orient='records')
        return final_matches_data
