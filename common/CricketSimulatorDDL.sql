CREATE KEYSPACE IF NOT EXISTS CricketSimulatorDB
WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3};


DROP TABLE IF EXISTS CricketSimulatorDB.Teams;
CREATE TABLE IF NOT EXISTS CricketSimulatorDB.Teams (
  team_id INT,
  src_team_id VARCHAR,
  team_name VARCHAR,
  color_code varchar,
  color_code_gradient varchar,
  team_short_name varchar,
  competition_name varchar,
  team_image_url VARCHAR,
  playoffs INT,
  seasons_played LIST<INT>,
  titles INT,
  load_timestamp timestamp,
  PRIMARY KEY (team_id));


DROP TABLE IF EXISTS CricketSimulatorDB.Players;
CREATE TABLE IF NOT EXISTS CricketSimulatorDB.Players(
  player_id INT,
  src_player_id VARCHAR,
  player_name VARCHAR,
  player_image_url VARCHAR,
  team_id  INT,
  competition_name VARCHAR,
  season int,
  player_type varchar,
  batting_type varchar,
  bowling_type varchar,
  bowl_major_type varchar,
  player_skill varchar,
  is_captain TINYINT,
  is_batsman TINYINT,
  is_bowler TINYINT,
  is_wicket_keeper TINYINT,
  load_timestamp timestamp,
  PRIMARY KEY ((competition_name, season), player_id, player_name, src_player_id));


DROP TABLE IF EXISTS CricketSimulatorDB.Venue;
CREATE TABLE IF NOT EXISTS CricketSimulatorDB.Venue (
  venue_id INT,
  src_venue_id varchar,
  stadium_name VARCHAR,
  city VARCHAR,
  country VARCHAR,
  load_timestamp timestamp,
  PRIMARY KEY (venue_id));


DROP TABLE IF EXISTS CricketSimulatorDB.Matches;
CREATE TABLE IF NOT EXISTS CricketSimulatorDB.Matches (
  match_id INT,
  src_match_id VARCHAR,
  match_name VARCHAR,
  competition_name VARCHAR,
  venue INT,
  is_playoff TINYINT,
  is_title TINYINT,
  team1 INT,
  team2 INT,
  toss_team INT,
  toss_decision VARCHAR,
  match_date VARCHAR,
  match_time varchar,
  season int,
  team1_players list<INT>,
  team2_players list<INT>,
  team1_score INT,
  team1_wickets INT,
  team1_overs DECIMAL,
  team2_score INT,
  team2_wickets INT,
  team2_overs DECIMAL,
  team2_target INT,
  winning_team INT,
  match_result VARCHAR,
  nature_of_wicket VARCHAR,
  overall_nature VARCHAR,
  dew VARCHAR,
  match_date_form DATE,
  load_timestamp timestamp,
  PRIMARY KEY ((competition_name, season, team1, team2), src_match_id));


DROP TABLE IF EXISTS cricketsimulatordb.MatchBallSummary;
CREATE TABLE IF NOT EXISTS cricketsimulatordb.MatchBallSummary (
  id INT,
  competition_name VARCHAR,
  season int,
  match_id INT,
  over_number INT,
  ball_number INT,
  over_text VARCHAR,
  batsman_id INT,
  non_striker_id INT,
  batting_position SMALLINT,
  batsman_team_id INT,
  batting_phase SMALLINT,
  against_bowler INT,
  bowler_team_id INT,
  innings SMALLINT,
  runs SMALLINT,
  ball_runs SMALLINT,
  is_one TINYINT,
  is_two TINYINT,
  is_three TINYINT,
  is_four TINYINT,
  is_six TINYINT,
  is_dot_ball TINYINT,
  extras SMALLINT,
  is_extra TINYINT,
  is_wide TINYINT,
  is_no_ball TINYINT,
  is_bye TINYINT,
  is_leg_bye TINYINT,
  is_wicket TINYINT,
  wicket_type VARCHAR,
  is_bowler_wicket TINYINT,
  out_batsman_id INT,
  is_maiden TINYINT,
  bowl_type VARCHAR,
  shot_type VARCHAR,
  bowl_line VARCHAR,
  bowl_length VARCHAR,
  x_pitch decimal,
  y_pitch decimal,
  fielder_angle decimal,
  fielder_length_ratio decimal,
  is_beaten TINYINT,
  is_uncomfortable TINYINT,
  bowling_direction VARCHAR,
  video_file varchar,
  load_timestamp timestamp,
  PRIMARY KEY ((competition_name, season, match_id), id));


DROP TABLE IF EXISTS CricketSimulatorDB.BatsmanBowlerMatchStatistics;
CREATE TABLE IF NOT EXISTS CricketSimulatorDB.BatsmanBowlerMatchStatistics (
  id INT,
  competition_name VARCHAR,
  season int,
  batsman_id INT,
  match_id INT,
  innings SMALLINT,
  batting_position SMALLINT,
  batting_phase SMALLINT,
  against_bowler INT,
  runs_scored SMALLINT,
  num_fours SMALLINT,
  num_sixes SMALLINT,
  num_singles SMALLINT,
  num_dotballs SMALLINT,
  num_doubles SMALLINT,
  num_triples SMALLINT,
  num_extras SMALLINT,
  wicket SMALLINT,
  dismissal_type varchar,
  load_timestamp timestamp,
  PRIMARY KEY ((competition_name, season, match_id), id));


DROP TABLE IF EXISTS CricketSimulatorDB.MatchBattingCard;
CREATE TABLE IF NOT EXISTS CricketSimulatorDB.MatchBattingCard (
id INT,
match_id INT,
innings SMALLINT,
competition_name VARCHAR,
season INT,
batting_team_id INT,
batsman_id INT,
batting_position SMALLINT,
bowler_id INT,
out_desc VARCHAR,
runs INT,
balls INT,
dot_balls INT,
ones INT,
twos INT,
threes INT,
fours INT,
sixes INT,
strike_rate DECIMAL,
load_timestamp timestamp,
  PRIMARY KEY ((competition_name, season, match_id), id));


DROP TABLE IF EXISTS CricketSimulatorDB.MatchExtras;
CREATE TABLE IF NOT EXISTS CricketSimulatorDB.MatchExtras (
id INT,
match_id INT,
team_id INT,
innings INT,
total_extras INT,
no_balls INT,
byes INT,
wides INT,
leg_byes INT,
load_timestamp timestamp,
PRIMARY KEY(id));


DROP TABLE IF EXISTS CricketSimulatorDB.MatchBowlingCard;
CREATE TABLE IF NOT EXISTS CricketSimulatorDB.MatchBowlingCard (
id  INT,
match_id  INT,
innings SMALLINT,
competition_name VARCHAR,
season INT,
team_id SMALLINT,
bowler_id INT,
overs DECIMAL,
total_legal_balls INT,
maidens INT,
runs  INT,
wickets INT,
wides INT,
no_balls  INT,
economy DECIMAL,
bowling_order SMALLINT,
dot_balls INT,
ones  INT,
twos  INT,
threes  INT,
fours INT,
sixes INT,
strike_rate DECIMAL,
load_timestamp timestamp,
  PRIMARY KEY ((competition_name, season, match_id), id));


DROP TABLE IF EXISTS CricketSimulatorDB.MatchPartnership;
CREATE TABLE IF NOT EXISTS CricketSimulatorDB.MatchPartnership (
id  INT,
match_id INT,
competition_name VARCHAR,
season INT,
team_id SMALLINT,
innings SMALLINT,
striker INT,
partnership_total INT,
striker_runs  INT,
striker_balls INT,
extras  INT,
non_striker_runs  INT,
non_striker_balls INT,
non_striker INT,
load_timestamp timestamp,
  PRIMARY KEY ((competition_name, season, match_id), id));


DROP TABLE IF EXISTS CricketSimulatorDB.BatsmanGlobalStats;
CREATE TABLE IF NOT EXISTS CricketSimulatorDB.BatsmanGlobalStats (
  id INT,
  batsman_id INT,
  competition_name VARCHAR,
  season INT,
  total_matches_played INT,
  total_innings_batted INT,
  total_runs INT,
  total_balls_faced INT,
  batting_average decimal,
  strike_rate decimal,
  hundreds INT,
  fifties INT,
  not_outs INT,
  duck_outs INT,
  num_sixes INT,
  num_fours INT,
  highest_score INT,
  load_timestamp timestamp,
  PRIMARY KEY (id));


DROP TABLE IF EXISTS CricketSimulatorDB.BowlerGlobalStats;
CREATE TABLE IF NOT EXISTS CricketSimulatorDB.BowlerGlobalStats (
  id INT ,
  bowler_id INT ,
  competition_name VARCHAR,
  season INT,
  total_matches_played INT ,
  total_innings_played INT ,
  total_balls_bowled INT ,
  total_overs_bowled decimal,
  total_runs_conceded INT ,
  bowling_average decimal ,
  bowling_economy decimal ,
  bowling_strike_rate decimal,
  num_four_wkt_hauls INT ,
  num_five_wkt_hauls INT ,
  num_ten_wkt_hauls INT ,
  num_extras_conceded INT ,
  total_wickets INT ,
  best_figure VARCHAR,
  load_timestamp timestamp,
  PRIMARY KEY (id));


DROP TABLE IF EXISTS CricketSimulatorDB.fileLogs;
CREATE TABLE IF NOT EXISTS CricketSimulatorDB.fileLogs (
  file_id INT ,
  competition_name VARCHAR,
  season INT,
  file_name VARCHAR ,
  file_path VARCHAR,
  load_timestamp timestamp,
  PRIMARY KEY (file_id));


DROP TABLE IF EXISTS CricketSimulatorDB.contributionScore;
CREATE TABLE IF NOT EXISTS CricketSimulatorDB.contributionScore (
match_date varchar,
season int,
competition_name varchar,
venue_id int,
team_id int,
player_id int,
game_id varchar,
player varchar,
player_type varchar,
team  varchar,
firsthalf_secondhalf  varchar,
home_away varchar,
play_off int,
stadium varchar,
position  varchar,
balls_faced  int,
runs_scored int,
overall_fours int,
overall_sixes  int,
batting_strike_rate  decimal,
arrived_on  int,
dismissed_on  int,
batting_type varchar,
actual_powerplay_over_balls int,
actual_7_10_over_balls int,
actual_11_15_over_balls int,
actual_death_over_balls int,
bat_powerplay_contribution_score  int,
bat_7_10_overs_contribution_score  int,
bat_11_15_overs_contribution_score  int,
bat_deathovers_contribution_score  int,
batting_contribution_score  int,
bat_expectations  varchar,
batting_consistency_score  int,
bat_consistency_score_powerplay int,
bat_consistency_score_7_10 int,
bat_consistency_score_11_15 int,
bat_consistency_score_deathovers int,
bat_innings  int,
total_overs_bowled  decimal,
total_balls_bowled  int,
runs_conceded int,
total_wickets int,
overall_economy decimal,
bowl_powerplay_contribution_score  int,
bowl_7_10_overs_contribution_score   int,
bowl_11_15_overs_contribution_score  int,
bowl_deathovers_contribution_score  int,
bowling_contribution_score  int,
bowl_expectations  varchar,
bowling_consistency_score  int,
bowl_consistency_score_powerplay int,
bowl_consistency_score_7_10 int,
bowl_consistency_score_11_15 int,
bowl_consistency_score_deathovers int,
bowl_innings  int,
bowling_type varchar,
overall_powerplay_contribution_score  int,
overall_7_10_overs_contribution_score  int,
overall_11_15_overs_contribution_score  int,
overall_deathovers_contribution_score  int,
overall_consistency_score  int,
overall_contribution_score int,
batted_more_than_5_innings int,
bowled_more_than_5_innings  int,
batted_more_than_10_innings int,
bowled_more_than_10_innings  int,
bowled_3_or_4_overs  int,
bowled_4_overs  int,
speciality varchar,
retained TINYINT,
in_auction varchar,
actual_powerplay_over_runs int,
actual_7_10_over_runs int,
actual_11_15_over_runs int,
actual_death_over_runs int,
is_hatrick smallint,
is_won smallint,
is_out smallint,
fow_during_stay int,
non_striker_runs int,
load_timestamp timestamp,
 PRIMARY KEY ((season, competition_name, game_id, player_id)));


DROP TABLE IF EXISTS CricketSimulatorDB.fitnessGPSData;
CREATE TABLE IF NOT EXISTS CricketSimulatorDB.fitnessGPSData (
date_name date,
record_date varchar,
period_name varchar,
activity_name varchar,
team_name varchar,
player_id int,
athlete_id varchar,
player_name varchar,
max_velocity_kmh decimal,
total_accel_load decimal,
total_distance_m decimal,
total_player_load decimal,
vel_b1_tot_dist_m decimal,
vel_b2_tot_dist_m decimal,
vel_b3_tot_dist_m decimal,
vel_b4_tot_dist_m decimal,
vel_b5_tot_dist_m decimal,
accel_b3_eff_gen2 int,
decel_b3_eff_gen2 int,
total_dives int,
total_jumps int,
vel_b3_max_eff_dist_m decimal,
vel_b3_max_eff_duration decimal,
vel_b3_total_eff_count int,
vel_b4_avg_distance_m decimal,
vel_b4_max_eff_dist_m decimal,
vel_b4_total_eff_count int,
vel_b5_avg_distance_m decimal,
vel_b5_max_eff_dist_m decimal,
vel_b5_total_eff_count int,
accel_b1_eff_gen2 int,
accel_b13_total_eff_gen2 int,
accel_b2_eff_gen2 int,
decel_b1_eff_gen2 int,
decel_b13_total_eff_gen2 int,
decel_b2_eff_gen2 int,
load_timestamp timestamp,
PRIMARY KEY ((record_date), player_name, period_name, activity_name));

DROP TABLE IF EXISTS CricketSimulatorDB.fitnessGPSBallData;
CREATE TABLE IF NOT EXISTS CricketSimulatorDB.fitnessGPSBallData (
date_name date,
season int,
record_date varchar,
period_name varchar,
activity_name varchar,
athlete_id varchar,
team_name varchar,
is_match_fielding TINYINT,
player_id int,
player_name varchar,
delivery_name varchar,
ball_no int,
delivery_runup_distance decimal,
max_runup_velocity decimal,
peak_player_load decimal,
raw_peak_roll decimal,
raw_peak_yaw decimal,
delivery_speed decimal,
raw_peak_resultant decimal,
relative_peak_player_load decimal,
relative_peak_resultant decimal,
relative_peak_roll decimal,
relative_peak_yaw decimal,
avg_delivery_count int,
total_delivery_count int,
total_runup_distance decimal,
vel_at_pod decimal,
max_raw_resultant decimal,
avg_runup_velocity decimal,
avg_runup_distance decimal,
delivery_time bigint,
max_raw_player_load decimal,
max_raw_roll decimal,
max_raw_yaw decimal,
load_timestamp timestamp,
PRIMARY KEY ((season, record_date), period_name, activity_name, player_name, ball_no, delivery_time));


DROP TABLE IF EXISTS CricketSimulatorDB.fitnessForm;
CREATE TABLE IF NOT EXISTS CricketSimulatorDB.fitnessForm (
id bigint,
start_time varchar,
completion_time varchar,
record_date varchar,
player_name varchar,
player_id bigint,
team_name varchar,
form_filler varchar,
trained_today varchar,
played_today varchar,
reason_noplay_or_train  varchar,
batting_train_mins int,
batting_train_rpe int,
bowling_train_mins int,
bowling_train_rpe int,
fielding_train_mins int,
fielding_train_rpe int,
strength_mins int,
strength_rpe int,
running_mins int,
running_rpe int,
cross_training_mins int,
cross_training_rpe int,
rehab_mins int,
rehab_rpe int,
batting_match_mins int,
bowling_match_balls int,
bowling_train_balls int,
bowling_match_rpe int,
batting_match_rpe int,
bowling_match_mins int,
fielding_match_mins int,
fielding_match_rpe int,
fatigue_level_rating int,
sleep_rating int,
muscle_soreness_rating int,
stress_levels_rating int,
wellness_rating int,
PRIMARY KEY (id));


DROP TABLE IF EXISTS CricketSimulatorDB.playerLoad;
CREATE TABLE IF NOT EXISTS CricketSimulatorDB.playerLoad (
id bigint,
record_date varchar,
player_name varchar,
player_id bigint,
team_name varchar,
bat_load int,
bowl_load int,
field_load int,
run_load  int,
bat_match_load int,
bowl_match_load int,
field_match_load int,
match_load int,
x_train_load int,
strength_load int,
rehab_load int,
total_snc_load int,
total_trn_load int,
total_load int,
bowling_match_balls int,
PRIMARY KEY (id));


DROP TABLE IF EXISTS CricketSimulatorDB.NotificationScheduler;
CREATE TABLE IF NOT EXISTS CricketSimulatorDB.NotificationScheduler (
id INT,
start_date DATE,
recipient LIST<VARCHAR>,
end_date DATE,
schedule_time TIME,
token_roles LIST<VARCHAR>
team_name varchar,
user_name varchar,
load_timestamp timestamp,
is_active BOOLEAN,
is_whatsapp BOOLEAN,
is_text BOOLEAN,
is_mail BOOLEAN,
module VARCHAR,
PRIMARY KEY(id));


DROP TABLE IF EXISTS CricketSimulatorDB.schedulerLog;
CREATE TABLE IF NOT EXISTS CricketSimulatorDB.schedulerLog (
id INT,
recipient VARCHAR,
load_timestamp timestamp,
message VARCHAR,
module VARCHAR,
PRIMARY KEY(id)
);


DROP TABLE IF EXISTS CricketSimulatorDB.matchPeakLoad;
CREATE TABLE IF NOT EXISTS CricketSimulatorDB.matchPeakLoad (
id bigint,
record_date varchar,
player_id bigint,
player_name varchar,
team_name varchar,
match_peak_load decimal,
PRIMARY KEY (id));

DROP TABLE IF EXISTS CricketSimulatorDB.bowlPlanning;
CREATE TABLE IF NOT EXISTS CricketSimulatorDB.bowlPlanning (
id bigint,
record_date varchar,
player_name varchar,
team_name varchar,
match_balls bigint,
train_balls bigint,
PRIMARY KEY (id));


DROP TABLE IF EXISTS cricketsimulatordb.userQueries;
CREATE TABLE IF NOT EXISTS cricketsimulatordb.userQueries (
  s_no BIGINT,
  uuid VARCHAR,
  user_name VARCHAR,
  title varchar,
  module_name varchar,
  description text,
  resolution text,
  state varchar,
  update_ts timestamp,
  resolution_image varchar,
  desc_image varchar,
  PRIMARY KEY (s_no));


DROP TABLE IF EXISTS cricketsimulatordb.calendarEvents;
CREATE TABLE IF NOT EXISTS cricketsimulatordb.calendarEvents (
  id BIGINT,
  event_type VARCHAR,
  event_title VARCHAR,
  venue VARCHAR,
  description VARCHAR,
  event_dates DATE,
  start_time TIME,
  end_time TIME,
  all_day BOOLEAN,
  reminder_time TIME,
  token_roles LIST<VARCHAR>,
  reminder_processed BOOLEAN,
  src_player_id LIST<INT>,
  recipient_uuid LIST<VARCHAR>,
  recipient_name LIST<VARCHAR>,
  event_creator_uuid VARCHAR,
  load_timestamp timestamp,
  PRIMARY KEY (id)
);


DROP TABLE IF EXISTS cricketsimulatordb.recipientGroup;
CREATE TABLE IF NOT EXISTS cricketsimulatordb.recipientGroup (
  id bigint,
  name VARCHAR,
  recipients LIST<VARCHAR>,
  PRIMARY KEY (id));


DROP TABLE IF EXISTS CricketSimulatorDB.fieldingAnalysis;
CREATE TABLE IF NOT EXISTS CricketSimulatorDB.fieldingAnalysis (
  id bigint,
  match_name varchar,
  player_name varchar,
  player_id int,
  team_name varchar,
  match_id int,
  season int,
  player_type varchar,
  clean_takes bigint,
  miss_fields bigint,
  miss_fields_cost bigint,
  dives_made bigint,
  runs_saved bigint,
  dives_missed bigint,
  missed_runs bigint,
  good_attempt bigint,
  taken bigint,
  stumping bigint,
  dropped_percent_difficulty bigint,
  caught_and_bowled bigint,
  good_return bigint,
  poor_return bigint,
  direct_hit bigint,
  missed_shy bigint,
  run_out_obtained bigint,
  pop_ups bigint,
  support_run bigint,
  back_up bigint,
  standing_back_plus bigint,
  standing_back_minus bigint,
  standing_up_plus bigint,
  standing_up_minus bigint,
  returns_taken_plus bigint,
  returns_untidy bigint,
  load_timestamp timestamp,
  PRIMARY KEY (id));


DROP TABLE IF EXISTS cricketsimulatordb.playerMapping;
CREATE TABLE IF NOT EXISTS cricketsimulatordb.playerMapping (
  id bigint,
  name varchar,
  short_name varchar,
  full_name varchar,
  source_name varchar,
  cricinfo_id bigint,
  cricsheet_id varchar,
  sports_mechanics_id varchar,
  nvplay_id varchar,
  nv_play_id varchar,
  country varchar,
  born varchar,
  age varchar,
  bowler_sub_type varchar,
  striker_batting_type varchar,
  is_wicket_keeper TINYINT,
  is_batsman TINYINT,
  is_bowler TINYINT,
  smartabase_id bigint,
  load_timestamp timestamp,
  PRIMARY KEY (id)
);


DROP TABLE IF EXISTS cricketsimulatordb.ingestionLog;
CREATE TABLE IF NOT EXISTS cricketsimulatordb.ingestionLog (
  id bigint,
  match_name varchar,
  player_id LIST<VARCHAR>,
  match_id varchar,
  message varchar,
  load_timestamp timestamp,
  PRIMARY KEY (id)
 );


DROP TABLE IF EXISTS CricketSimulatorDB.searchQueryFeedback;
CREATE TABLE IF NOT EXISTS CricketSimulatorDB.searchQueryFeedback (
  id bigint,
  user_query VARCHAR,
  output_api list<text>,
  ai_api_response TEXT,
  feedback_satisfaction VARCHAR,
  not_relevant INT,
  not_accurate INT,
  user_comment VARCHAR,
  user_uuid VARCHAR,
  user_role list<text>,
  load_timestamp timestamp,
  PRIMARY KEY (id));


DROP TABLE IF EXISTS CricketSimulatorDB.PressureIndex;
CREATE TABLE IF NOT EXISTS CricketSimulatorDB.PressureIndex (
  id INT,
  byb_id INT,
  match_date VARCHAR,
  match_name VARCHAR,
  innings INT,
  raw_ball_no INT,
  req_batsman_id INT,
  req_batsman VARCHAR,
  striker_id INT,
  striker VARCHAR,
  non_striker_id INT,
  non_striker VARCHAR,
  bowler_id INT,
  bowler VARCHAR,
  entry_point VARCHAR,
  match_phase VARCHAR,
  ingame_CRR_after DECIMAL,
  ingame_RRR_after DECIMAL,
  ingame_RSR_after DECIMAL,
  ingame_totalwkts_after INT,
  ingame_resource_utilized_after DECIMAL,
  ingame_BatsamnSR_after DECIMAL,
  ingame_Ewm_BatsmanSR_after DECIMAL,
  ingame_BatsamnSR_w_impute_after DECIMAL,
  ingame_Ewm_BatsmanSR_w_impute_after DECIMAL,
  ingame_total_BatsmanRuns_after DECIMAL,
  ingame_total_BatsmanBalls_after DECIMAL,
  ingame_NonStrikerSR_after DECIMAL,
  ingame_NonStrikerSR_w_impute_after DECIMAL,
  ingame_nonstriker_balls_tillnow_after DECIMAL,
  ingame_cum_non_striker_sr_after DECIMAL,
  ingame_nonstriker_balls_with_wicket_reset_after INT,
  ingame_nonstriker_runs_with_wicket_reset_after INT,
  ingame_nonstriker_sr_with_wicket_reset_after FLOAT,
  ingame_emw_nonstriker_sr_with_wicket_reset_after DECIMAL,
  ingame_BowlerEco_after DECIMAL,
  ingame_Ewm_BowlerEco_after DECIMAL,
  ingame_BowlerEco_w_impute_after DECIMAL,
  ingame_Ewm_BowlerEco_w_impute_after DECIMAL,
  ingame_BowlerSR_after DECIMAL,
  ingame_BowlerSR_w_impute_after DECIMAL,
  ingame_total_BowlerRuns_after INT,
  ingame_total_BowlerWkts_after INT,
  ingame_total_BowlerBalls_after INT,
  prev_bowler_raw_ball_no_after INT,
  ingame_Other_BowlerEco_after DECIMAL,
  ingame_Other_BowlerEco_w_impute_after DECIMAL,
  ingame_otherbowler_balls_tillnow_after DECIMAL,
  ingame_otherbowler_cum_eco_after DECIMAL,
  ingame_CRR DECIMAL,
  ingame_RRR DECIMAL,
  ingame_RSR DECIMAL,
  ingame_totalwkts INT,
  ingame_totalruns INT,
  is_striker INT,
  ingame_resource_utilized DECIMAL,
  pregame_batsman_entrySR DECIMAL,
  is_pregame_entrySR_available INT,
  pregame_bowler_eco DECIMAL,
  pregame_bowler_sr DECIMAL,
  is_pregame_bowler_phaseEco_available INT,
  pregame_h2h_eco DECIMAL,
  pregame_h2h_bowler_sr DECIMAL,
  is_pregame_h2h_available DECIMAL,
  pregame_h2h_type DECIMAL,
  prev_ball_runs DECIMAL,
  previous_ball_number DECIMAL,
  previous_raw_ball_number DECIMAL,
  ingame_BatsamnSR DECIMAL,
  ingame_Ewm_BatsmanSR DECIMAL,
  ingame_BatsamnSR_w_impute DECIMAL,
  ingame_Ewm_BatsmanSR_w_impute DECIMAL,
  ingame_total_BatsmanRuns DECIMAL,
  prev_ball_BatsmanRuns DECIMAL,
  ingame_total_BatsmanBalls DECIMAL,
  ingame_NonStrikerSR DECIMAL,
  ingame_NonStrikerSR_w_impute DECIMAL,
  ingame_nonstriker_balls_tillnow DECIMAL,
  ingame_cum_non_striker_sr DECIMAL,
  ingame_nonstriker_balls_with_wicket_reset DECIMAL,
  ingame_nonstriker_runs_with_wicket_reset DECIMAL,
  ingame_nonstriker_sr_with_wicket_reset DECIMAL,
  ingame_emw_nonstriker_sr_with_wicket_reset DECIMAL,
  ingame_BowlerEco DECIMAL,
  ingame_Ewm_BowlerEco DECIMAL,
  ingame_BowlerEco_w_impute DECIMAL,
  ingame_Ewm_BowlerEco_w_impute DECIMAL,
  ingame_BowlerSR DECIMAL,
  ingame_BowlerSR_w_impute DECIMAL,
  ingame_total_BowlerRuns DECIMAL,
  ingame_total_BowlerWkts DECIMAL,
  ingame_total_BowlerBalls DECIMAL,
  prev_bowler_raw_ball_no DECIMAL,
  ingame_Other_BowlerEco DECIMAL,
  ingame_Other_BowlerEco_w_impute DECIMAL,
  ingame_otherbowler_balls_tillnow DECIMAL,
  ingame_otherbowler_cum_eco DECIMAL,
  ingame_h2hEco DECIMAL,
  ingame_h2hEco_w_impute DECIMAL,
  ingame_h2hBowlerSR DECIMAL,
  ingame_h2hBowlerSR_w_impute DECIMAL,
  ingame_total_h2hRuns DECIMAL,
  ingame_total_h2hBalls DECIMAL,
  RRR_BY_CRR DECIMAL,
  RRR_BY_CRR_after DECIMAL,
  wickets_by_10 DECIMAL,
  wickets_by_10_after DECIMAL,
  wickets_resource_utilized DECIMAL,
  wickets_resource_utilized_after DECIMAL,
  RRR_BY_CRR_wickets_resource_utilized DECIMAL,
  RRR_BY_CRR_wickets_resource_utilized_after DECIMAL,
  BatSR_raw DECIMAL,
  BatSR_raw_after DECIMAL,
  is_BatSR_filled_random INT,
  is_BatSR_filled_random_after INT,
  BatSR DECIMAL,
  BatSR_after DECIMAL,
  NonstrikerSR_raw DECIMAL,
  NonstrikerSR_raw_after DECIMAL,
  is_NonstrikerSR_filled_random INT,
  is_NonstrikerSR_filled_random_after INT,
  NonstrikerSR DECIMAL,
  NonstrikerSR_after DECIMAL,
  w_progress_BowlerEco DECIMAL,
  w_progress_BowlerEco_after DECIMAL,
  BowlerEco_raw DECIMAL,
  BowlerEco_raw_after DECIMAL,
  is_BowlerEco_filled_random INT,
  is_BowlerEco_filled_random_after INT,
  BowlerEco DECIMAL,
  BowlerEco_after DECIMAL,
  OtherBowlersEco_raw DECIMAL,
  OtherBowlersEco_raw_after DECIMAL,
  is_OtherBowlersEco_filled_random INT,
  is_OtherBowlersEco_filled_random_after INT,
  OtherBowlersEco DECIMAL,
  OtherBowlersEco_after DECIMAL,
  RRR_BY_CRR_wickets_resource_utilized_scaled DECIMAL,
  RRR_BY_CRR_wickets_resource_utilized_after_scaled DECIMAL,
  BatSR_scaled DECIMAL,
  BatSR_after_scaled DECIMAL,
  NonstrikerSR_scaled DECIMAL,
  NonstrikerSR_after_scaled DECIMAL,
  BowlerEco_SR_scaled DECIMAL,
  BowlerEco_SR_after_scaled DECIMAL,
  OtherBowlersEco_SR_scaled DECIMAL,
  OtherBowlersEco_SR_after_scaled DECIMAL,
  H2HEco_scaled DECIMAL,
  BatSR_mulfactor DECIMAL,
  BatSR_mulfactor_after DECIMAL,
  NonstrikerSR_mulfactor DECIMAL,
  NonstrikerSR_mulfactor_after DECIMAL,
  BowlerEco_SR_mulfactor DECIMAL,
  BowlerEco_SR_mulfactor_after DECIMAL,
  OtherBowlersEco_SR_mulfactor DECIMAL,
  OtherBowlersEco_SR_mulfactor_after DECIMAL,
  H2HEco_mulfactor DECIMAL,
  RRR_BY_CRR_wickets_resource_utilized_mulfactor DECIMAL,
  RRR_BY_CRR_wickets_resource_utilized_mulfactor_after DECIMAL,
  RRR_BY_CRR_wickets_resource_utilized_final DECIMAL,
  BatSR_final DECIMAL,
  NonstrikerSR_final DECIMAL,
  BowlerEco_SR_final DECIMAL,
  OtherBowlersEco_SR_final DECIMAL,
  H2HEco_final DECIMAL,
  BatsmanPI_w_h2h DECIMAL,
  RRR_BY_CRR_wickets_resource_utilized_final_match_pi_batsman DECIMAL,
  BowlerEco_SR_final_match_pi_batsman DECIMAL,
  OtherBowlersEco_SR_final_match_pi_batsman DECIMAL,
  NonstrikerSR_final_match_pi_batsman DECIMAL,
  BatsmanMatchPI DECIMAL,
  RRR_BY_CRR_wickets_resource_utilized_final_match_pi_batsman_after DECIMAL,
  BowlerEco_SR_final_match_pi_batsman_after DECIMAL,
  OtherBowlersEco_SR_final_match_pi_batsman_after DECIMAL,
  NonstrikerSR_final_match_pi_batsman_after DECIMAL,
  BatsmanMatchPI_after DECIMAL,
  RRR_BY_CRR_wickets_resource_utilized_final_match_pi_bowler DECIMAL,
  BatSR_final_match_pi_bowler DECIMAL,
  NonstrikerSR_final_match_pi_bowler DECIMAL,
  OtherBowlersEco_SR_final_match_pi_bowler DECIMAL,
  BowlerMatchPI DECIMAL,
  RRR_BY_CRR_wickets_resource_utilized_final_match_pi_bowler_after DECIMAL,
  BatSR_final_match_pi_bowler_after DECIMAL,
  NonstrikerSR_final_match_pi_bowler_after DECIMAL,
  OtherBowlersEco_SR_final_match_pi_bowler_after DECIMAL,
  BowlerMatchPI_after DECIMAL,
  MatchPI DECIMAL,
  MatchPI_after DECIMAL,
  BatsmanPI_w_h2h_ratio DECIMAL,
  BatsmanMatchPI_ratio DECIMAL,
  BowlerMatchPI_ratio DECIMAL,
  MatchPI_ratio DECIMAL,
  MatchPI_Cat VARCHAR,
  BatsmanMatchPI_Cat VARCHAR,
  BowlerMatchPI_Cat VARCHAR,
  BatsmanPI_wh2h_Cat VARCHAR,
  load_timestamp TIMESTAMP,
  PRIMARY KEY (
    (match_name, innings, raw_ball_no),
    id
  )
);

DROP TABLE IF EXISTS CricketSimulatorDB.readiness_to_perform;
CREATE TABLE IF NOT EXISTS cricketsimulatordb.readiness_to_perform (
  start_date varchar,
  player_name varchar,
  id bigint,
  event_id bigint,
  smartabase_id bigint,
  src_player_id bigint,
  start_time varchar,
  finish_date varchar,
  finish_time varchar,
  playing_status text,
  physical_status text,
  injury_status text,
  action_status_comments text,
  last_sync_time bigint,
  load_timestamp TIMESTAMP,
  PRIMARY KEY (start_date, player_name, id)
);


DROP TABLE IF EXISTS CricketSimulatorDB.mi_availability;
CREATE TABLE IF NOT EXISTS CricketSimulatorDB.mi_availability (
  id bigint,
  event_id bigint,
  smartabase_id bigint,
  player_name varchar,
  src_player_id bigint,
  start_date varchar,
  start_time varchar,
  finish_date varchar,
  finish_time varchar,
  overall_status varchar,
  linked_medical_status text,
  comments text,
  last_sync_time bigint,
  load_timestamp TIMESTAMP,
 PRIMARY KEY (start_date, player_name, id)
 );


DROP TABLE IF EXISTS cricketsimulatordb.playerMapping;
CREATE TABLE IF NOT EXISTS cricketsimulatordb.playerMapping (
  id bigint,
  name varchar,
  short_name varchar,
  full_name varchar,
  source_name varchar,
  cricinfo_id bigint,
  cricsheet_id varchar,
  sports_mechanics_id varchar,
  nvplay_id varchar,
  nv_play_id varchar,
  smartabase_id bigint,
  catapult_id LIST<varchar>,
  country varchar,
  born varchar,
  age varchar,
  bowler_sub_type varchar,
  striker_batting_type varchar,
  is_wicket_keeper TINYINT,
  is_batsman TINYINT,
  is_bowler TINYINT,
  load_timestamp timestamp,
  PRIMARY KEY (id)
);


truncate table CricketSimulatorDB.Teams;
truncate table CricketSimulatorDB.Players;
truncate table CricketSimulatorDB.Venue;
truncate table CricketSimulatorDB.Matches;
truncate table CricketSimulatorDB.MatchBattingCard;
truncate table CricketSimulatorDB.MatchExtras;
truncate table CricketSimulatorDB.MatchBowlingCard;
truncate table CricketSimulatorDB.MatchPartnership;
truncate table CricketSimulatorDB.MatchBallSummary;
truncate table CricketSimulatorDB.BatsmanBowlerMatchStatistics;
truncate table CricketSimulatorDB.BatsmanGlobalStats;
truncate table CricketSimulatorDB.BowlerGlobalStats;
truncate table CricketSimulatorDB.fileLogs;
truncate table CricketSimulatorDB.contributionScore;
truncate table CricketSimulatorDB.fitnessGPSData;
truncate table CricketSimulatorDB.fitnessGPSBallData;
truncate table CricketSimulatorDB.fitnessForm;
truncate table CricketSimulatorDB.playerLoad;
truncate table CricketSimulatorDB.NotificationScheduler;
truncate table CricketSimulatorDB.schedulerLog;
truncate table CricketSimulatorDB.eventSchedule;
truncate table CricketSimulatorDB.recipientGroup;
truncate table CricketSimulatorDB.fieldingAnalysis;
truncate table CricketSimulatorDB.ingestionLog;
truncate table CricketSimulatorDB.searchQueryFeedback;
truncate table CricketSimulatorDB.PressureIndex;
truncate table CricketSimulatorDB.readiness_to_perform;
truncate table CricketSimulatorDB.mi_availability;