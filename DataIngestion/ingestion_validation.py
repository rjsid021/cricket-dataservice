import json

from DataIngestion import config
from DataIngestion.config import BUILD_ENV
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session
from common.db_config import DB_NAME
from log.log import get_logger
from third_party_service.smtp import SMTPMailer

logger = get_logger("Ingestion", "IngestionValidation")


class IngestionValidation:
    def __init__(self, load_timestamp):
        self.message = []
        self.load_timestamp = load_timestamp
        self.match_ingested = 0

    def validate_matches(self):
        GET_MATCHES_DATA = f'''
        select 
          match_id, 
          match_name, 
          team1, 
          team2, 
          team1_score, 
          team1_wickets, 
          team1_players,
          team2_players,
          team2_score, 
          team2_wickets, 
          season, 
          competition_name, 
          winning_team, 
          toss_team,
          venue, 
          match_result, 
          team1_overs, 
          team2_overs, 
          match_date, 
          overall_nature, 
          match_date_form, 
          load_timestamp
        from 
          {DB_NAME}.Matches
        where load_timestamp = '{str(self.load_timestamp)}' ALLOW FILTERING;
        '''
        matches_df = getPandasFactoryDF(session, GET_MATCHES_DATA)
        self.match_ingested = len(matches_df)

        # check for duplicates
        if (matches_df.duplicated(subset=['venue', 'team1', 'team2', 'match_date']) == True).sum() != 0:
            error_data = matches_df[matches_df.duplicated(
                subset=['venue', 'team1', 'team2', 'match_date'], keep=False
            )]
            error_data[['team1_overs']] = error_data[['team1_overs']].astype(float)
            error_data[['team2_overs']] = error_data[['team2_overs']].astype(float)
            error_data['load_timestamp'] = error_data['load_timestamp'].astype(str)
            error_data['match_date_form'] = error_data['match_date_form'].astype(str)
            self.message.append(
                {
                    "table": "matches",
                    "load_timestamp": self.load_timestamp,
                    "error": "Duplicate match found",
                    "error_data": error_data.to_dict(orient='records')
                }
            )

        if matches_df["venue"].isnull().values.any():
            self.message.append(
                {
                    "table": "matches",
                    "load_timestamp": self.load_timestamp,
                    "error": "Venue is null"
                }
            )
        # toss_team nulls check
        if matches_df["toss_team"].isnull().values.any():
            self.message.append(
                {
                    "table": "matches",
                    "load_timestamp": self.load_timestamp,
                    "error": "toss team is null"
                }
            )

        # team1 nulls check
        if matches_df["team1"].isnull().values.any():
            self.message.append(
                {
                    "table": "matches",
                    "load_timestamp": self.load_timestamp,
                    "error": "team 1 is null"
                }
            )

        # team2 nulls check
        if matches_df["team2"].isnull().values.any():
            self.message.append(
                {
                    "table": "matches",
                    "load_timestamp": self.load_timestamp,
                    "error": "team 2 is null"
                }
            )
        # team1_players nulls check
        if matches_df["team1_players"].isnull().values.any():
            self.message.append(
                {
                    "table": "matches",
                    "load_timestamp": self.load_timestamp,
                    "error": "team1_players is null"
                }
            )
        # team1_players nulls check
        if matches_df["team2_players"].isnull().values.any():
            self.message.append(
                {
                    "table": "matches",
                    "load_timestamp": self.load_timestamp,
                    "error": "team1_players is null"
                }
            )

    def validate_matchballsummary(self):
        GET_BALL_SUMMARY_DATA = f'''
        select 
          id, 
          match_id, 
          over_number, 
          ball_number, 
          batsman_id, 
          batsman_team_id, 
          is_four, 
          is_six, 
          against_bowler,
          shot_type, 
          bowler_team_id, 
          is_extra, 
          is_wide, 
          is_no_ball, 
          is_dot_ball, 
          batting_position, 
          over_text, 
          innings, 
          runs, 
          extras, 
          is_wicket, 
          wicket_type, 
          is_bowler_wicket, 
          is_leg_bye, 
          batting_phase, 
          x_pitch, 
          y_pitch, 
          out_batsman_id, 
          ball_runs, 
          competition_name, 
          season, 
          bowl_line, 
          bowl_length, 
          is_bye, 
          non_striker_id, 
          load_timestamp
        from 
          {DB_NAME}.MatchBallSummary
        where load_timestamp = '{str(self.load_timestamp)}' ALLOW FILTERING;
        '''
        ball_summary_df = getPandasFactoryDF(session, GET_BALL_SUMMARY_DATA)
        column_subset = [
            'match_id',
            'over_number',
            'ball_number',
            'batsman_id',
            'batsman_team_id',
            'is_four',
            'is_six',
            'against_bowler',
            'shot_type',
            'bowler_team_id',
            'is_extra',
            'is_wide',
            'is_no_ball',
            'is_dot_ball',
            'batting_position',
            'over_text',
            'innings',
            'runs',
            'extras',
            'is_wicket',
            'wicket_type',
            'is_bowler_wicket',
            'is_leg_bye',
            'batting_phase',
            'x_pitch',
            'y_pitch',
            'out_batsman_id',
            'ball_runs',
            'competition_name',
            'season',
            'bowl_line',
            'bowl_length',
            'is_bye',
            'non_striker_id',
            'load_timestamp'
        ]

        # check for duplicates
        if (ball_summary_df.duplicated(subset=column_subset) == True).sum() != 0:
            error_data = ball_summary_df[ball_summary_df.duplicated(
                subset=column_subset, keep=False
            )]
            error_data[['x_pitch']] = error_data[['x_pitch']].astype(float)
            error_data[['y_pitch']] = error_data[['y_pitch']].astype(float)
            error_data['load_timestamp'] = error_data['load_timestamp'].astype(str)
            self.message.append(
                {
                    "table": "matchballsummary",
                    "load_timestamp": self.load_timestamp,
                    "error": "Duplicate matchballsummary found",
                    "error_data": error_data.to_dict(orient='records')
                }
            )

        # check if ball count > 300:
        match_rows_df = ball_summary_df.groupby(['match_id']).size()
        for match_id, count in match_rows_df.items():
            if count > 300:
                self.message.append(
                    {
                        "table": "matchballsummary",
                        "load_timestamp": self.load_timestamp,
                        "error": "match ball by ball data count is greater than 300",
                        "match_id": match_id,
                        "count": count
                    }
                )

    def validate_teams(self):
        GET_TEAMS_DATA = f'''
        select 
          team_id, 
          team_name, 
          src_team_id,
          team_short_name, 
          competition_name, 
          titles, 
          team_image_url, 
          seasons_played,
          load_timestamp
        from 
          {DB_NAME}.Teams
        where load_timestamp = '{str(self.load_timestamp)}' ALLOW FILTERING;
        '''
        teams_df = getPandasFactoryDF(session, GET_TEAMS_DATA)
        teams_df["team_name"] = teams_df["team_name"].apply(lambda x: x.strip().replace(" ", "").lower())
        if (teams_df.duplicated(subset=['team_name']) == True).sum() != 0:
            error_data = teams_df[teams_df.duplicated(subset=['team_name'], keep=False)]
            error_data['load_timestamp'] = error_data['load_timestamp'].astype(str)
            self.message.append(
                {
                    "table": "teams",
                    "load_timestamp": self.load_timestamp,
                    "error": "Duplicate team name found",
                    "error_data": error_data.to_dict(orient='records')
                }
            )

        # duplicate id check
        if (teams_df.duplicated(subset=['src_team_id']) == True).sum() != 0:
            error_data = teams_df[teams_df.duplicated(subset=['src_team_id'], keep=False)]
            error_data['load_timestamp'] = error_data['load_timestamp'].astype(str)
            self.message.append(
                {
                    "table": "teams",
                    "load_timestamp": self.load_timestamp,
                    "error": "Duplicate src_team_id found",
                    "error_data": error_data.to_dict(orient='records')
                }
            )
        # team_short_name nulls check
        if teams_df["team_short_name"].isnull().values.any():
            error_data = teams_df[teams_df.duplicated(subset=['team_short_name'], keep=False)]
            error_data['load_timestamp'] = error_data['load_timestamp'].astype(str)
            self.message.append(
                {
                    "table": "teams",
                    "load_timestamp": self.load_timestamp,
                    "error": "team_short_name is null",
                    "error_data": error_data.to_dict(orient='records')
                }
            )

    def validate_players(self):
        GET_PLAYERS_DATA = f'''
        select 
          player_id, 
          src_player_id,
          player_name, 
          batting_type, 
          bowling_type, 
          player_skill, 
          team_id, 
          season, 
          competition_name, 
          is_captain, 
          is_batsman, 
          is_bowler, 
          is_wicket_keeper, 
          player_type, 
          bowl_major_type, 
          player_image_url,
          load_timestamp
        from 
          {DB_NAME}.Players
        '''

        players_df = getPandasFactoryDF(session, GET_PLAYERS_DATA)

        # duplicate id check
        if (players_df.duplicated(subset=['player_id', 'competition_name', 'season']) == True).sum() != 0:
            error_data = players_df[
                players_df.duplicated(subset=['player_id', 'competition_name', 'season'], keep=False)]
            error_data['load_timestamp'] = error_data['load_timestamp'].astype(str)
            self.message.append(
                {
                    "table": "players",
                    "load_timestamp": self.load_timestamp,
                    "error": "Duplicate player found",
                    "error_data": error_data.to_dict(orient='records')
                }
            )

        # team_id nulls check
        if players_df["team_id"].isnull().values.any():
            self.message.append(
                {
                    "table": "players",
                    "load_timestamp": self.load_timestamp,
                    "error": "Team id null found for player"
                }
            )

    def validate_ingestion(self):
        try:
            # Test Scenarios
            # 1. matches column to check if data is proper
            # 1.1 - duplicate entry
            # 1.2 - team1 and team2 names
            # 1.3 - others columns
            self.validate_matches()

            # 2. matchballsummary has proper data
            # 2.1 - duplicate entry
            # 2.2 - not more than 300 entry
            # 2.3 - others columns
            self.validate_matchballsummary()

            # 3.1 check if there is duplicate entry
            # 3.2 check if team id is null
            self.validate_players()

            # 4.1 team_name duplicate check
            # 4.2 src_team_id duplicate check
            # 4.3 team_short_name null check
            self.validate_teams()

            subject = f"Micip Ingestion Report for environment : {BUILD_ENV} ❗"
            if not self.message:
                subject = f"Micip Ingestion Report for environment : {BUILD_ENV} ✅"
                self.message = {
                    "timestamp": self.load_timestamp,
                    "message": "No issue with ingestion",
                    "matches ingested": self.match_ingested
                }

            logger.info("Validation done for ingestion.")
        except Exception as err:
            logger.error(err)
            subject = "Micip Ingestion Report failed to check, please check K8s logs"
            self.message = "Please see K8s logs for more info. Something went wrong during ingestion"

        # Open and read the JSON file for NV Play config
        with open(config.NVPLAY_INGESTION_CONFIG, 'r') as json_file:
            ingestion_config = json.load(json_file)
        recipients = ingestion_config['recipients']
        # post validation send out mail
        SMTPMailer().send_bulk_email(
            recipient_emails=recipients,
            subject=subject,
            message=json.dumps(self.message)
        )
