from DataIngestion.config import PLAYER_MAPPER_TABLE_NAME, GPS_DELIVERY_TABLE_NAME, GPS_TABLE_NAME
from common.db_config import DB_NAME

GET_EXISTING_FILES = f'''
select 
  file_name 
from 
  {DB_NAME}.fileLogs
'''

GET_TEAM_SQL = f'''Select team_id, src_team_id, team_name, competition_name, seasons_played, titles, playoffs, 
team_short_name from {DB_NAME}.Teams;'''

GET_PLAYER_DETAILS_SQL = f'''Select player_name, src_player_id, player_id 
from {DB_NAME}.Players;'''

GET_PLAYERS_SQL = f'''Select player_id, src_player_id, season, competition_name, player_type, team_id, 
player_name, bowling_type, batting_type from {DB_NAME}.Players;'''

GET_VENUE_DETAILS_SQL = f'''Select stadium_name, src_venue_id, venue_id from {DB_NAME}.Venue;'''

GET_MATCHES_DETAILS_SQL = f'''Select match_id, src_match_id, team1, team2, team1_players, team2_players
                            from {DB_NAME}.Matches;'''

GET_EXISTING_MATCHES_SQL = f'''Select src_match_id, match_name from {DB_NAME}.Matches;'''

GET_MATCH_SUMMARY = f'''select src_match_id ,match_id, team1, team2, team1_players, team2_players, season, competition_name, 
load_timestamp, match_date, is_playoff, match_name, venue, winning_team from {DB_NAME}.Matches'''

GET_MATCH_DATA_SQL = f'''select match_id, batsman_id, innings, batting_position, batting_phase, against_bowler, runs,
is_four, is_six,is_one, is_two, is_three , is_dot_ball, extras, is_wicket, wicket_type, ball_runs, season, 
competition_name, load_timestamp, over_number,batsman_team_id, is_wide, is_no_ball, ball_number, bowler_team_id, out_batsman_id, 
is_bye, is_leg_bye, is_bowler_wicket, non_striker_id from {DB_NAME}.MatchBallSummary '''

GET_BAT_CARD_DATA = f'''select match_id, innings, batsman_id, batting_team_id, batting_position, runs, fours, sixes, 
balls, out_desc, competition_name, season, load_timestamp from {DB_NAME}.MatchBattingCard'''

GET_BOWL_CARD_DATA = f'''select match_id, team_id, bowler_id, innings, overs, total_legal_balls, runs, wides, no_balls, 
wickets, competition_name, season, load_timestamp from {DB_NAME}.MatchBowlingCard'''

GET_BAT_BOWL_STATS_TIMESTAMP = f'''select max(load_timestamp) as max_ts from 
{DB_NAME}.BatsmanBowlerMatchStatistics;'''

GET_CS_TIMESTAMP = f'''select max(load_timestamp) as max_ts from {DB_NAME}.contributionScore;'''

GET_EXISTING_CS_MATCH_NAMES_SQL = f'''Select game_id from {DB_NAME}.contributionScore;'''

GET_BAT_GLOBAL_STATS_TIMESTAMP = f'''select max(load_timestamp) as max_ts from {DB_NAME}.BatsmanGlobalStats;'''

GET_BAT_GLOBAL_STATS_DATA = f'''select id,batsman_id,total_matches_played,total_innings_batted,total_runs,hundreds,
fifties,duck_outs,highest_score,not_outs,num_fours,num_sixes,competition_name,season,batting_average,total_balls_faced,
strike_rate from {DB_NAME}.BatsmanGlobalStats;'''

GET_BOWL_GLOBAL_STATS_TIMESTAMP = f'''select max(load_timestamp) as max_ts from {DB_NAME}.BowlerGlobalStats;'''

GET_BOWL_GLOBAL_STATS_DATA = f'''Select id,bowler_id,total_matches_played,total_innings_played,total_balls_bowled,
total_runs_conceded,num_four_wkt_hauls,num_five_wkt_hauls,num_ten_wkt_hauls,num_extras_conceded,total_wickets,
bowling_average,bowling_strike_rate,bowling_economy,total_overs_bowled,season,competition_name,best_figure  
from {DB_NAME}.BowlerGlobalStats;'''

GET_GPS_AGG_MAX_DATE = f'''select max(date_name) as max_ts from {DB_NAME}.{GPS_TABLE_NAME} '''

GET_GPS_BALL_MAX_DATE = f'''select max(date_name) as max_ts from {DB_NAME}.{GPS_DELIVERY_TABLE_NAME} '''

GET_EXISTING_FORM_ENTRIES_SQL = f'''select id from {DB_NAME}.fitnessForm;'''

GET_EXISTING_PLAYER_LOAD_ID = f'''select id from {DB_NAME}.playerLoad;'''

GET_ALREADY_EXISTING_GPS_DATA = f'''select athlete_id, date_name, activity_name, period_name, team_name from {DB_NAME}.{GPS_TABLE_NAME};'''

GET_ALREADY_EXISTING_GPS_BALL_DATA = f'''select athlete_id, date_name, activity_name, period_name,delivery_name, delivery_time, team_name from {DB_NAME}.{GPS_DELIVERY_TABLE_NAME};'''

GET_ALREADY_EXISTING_PLANNED_SQL = f'''select player_name, record_date, team_name from {DB_NAME}.bowlPlanning;'''

GET_PLAYER_MAPPER_SQL = f'''Select * from {DB_NAME}.{PLAYER_MAPPER_TABLE_NAME};'''

SELECTED_PLAYER_MAPPER_SQL = f'''Select id, cricsheet_id, nv_play_id, cricinfo_id from {DB_NAME}.{PLAYER_MAPPER_TABLE_NAME};'''

GET_READINESS_MAX_SYNC_TIME = f'''select max(last_sync_time) as max_sync_time from {DB_NAME}.readiness_to_perform; '''

GET_AVAILABILITY_MAX_SYNC_TIME = f'''select max(last_sync_time) as max_sync_time from {DB_NAME}.mi_availability; '''

GET_PLAYER_MAPPER_DETAILS_SQL = f'''select id as src_player_id, name as player_name, smartabase_id from {DB_NAME}.{PLAYER_MAPPER_TABLE_NAME};'''

