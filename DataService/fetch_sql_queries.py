from DataIngestion.config import IMAGE_STORE_URL, GPS_TABLE_NAME, GPS_DELIVERY_TABLE_NAME
from common.db_config import DB_NAME

GET_MATCHES_DATA = f'''select match_id, match_name, team1, team2, team1_score, team1_wickets, team2_score, team2_wickets, season, 
  competition_name, winning_team, venue, match_result, team1_overs, team2_overs, match_date, overall_nature, match_date_form
   from  {DB_NAME}.Matches;  '''

GET_BATCARD_DATA = f'''select match_id, innings, batting_team_id, batsman_id, out_desc, runs, balls, batting_position,
 competition_name, season, bowler_id, fours, sixes, strike_rate, dot_balls, ones, twos, threes 
  from  {DB_NAME}.MatchBattingCard; '''

GET_BOWL_CARD_DATA = f'''select match_id, innings, team_id, bowler_id, wickets, overs, runs, no_balls, wides, economy,
 bowling_order, competition_name, season, total_legal_balls, strike_rate from  {DB_NAME}.MatchBowlingCard;  '''

GET_EXTRAS_DATA = "select match_id, innings, team_id, total_extras, no_balls, byes,wides, leg_byes " \
                  f"from  {DB_NAME}.MatchExtras;"

GET_TEAMS_DATA = f"select team_id, team_name, team_short_name, competition_name, titles, team_image_url, seasons_played from  {DB_NAME}.Teams; "

GET_FITNESS_DATA = f'''select date_name, team_name, record_date, player_id, player_name, vel_b1_tot_dist_m, 
vel_b2_tot_dist_m, vel_b3_tot_dist_m, vel_b4_tot_dist_m, vel_b5_tot_dist_m, total_distance_m, max_velocity_kmh, 
 accel_b2_eff_gen2, decel_b2_eff_gen2, total_player_load from  {DB_NAME}.{GPS_TABLE_NAME} '''

GET_SEASONS_DATA = f"select season, team_id from  {DB_NAME}.Players;"

GET_SEASON_TEAMS_DATA = f'''
select 
  team_id, 
  team_name, 
  team_image_url,
  color_code, 
  color_code_gradient,
  team_short_name, 
  seasons_played as season, 
  competition_name 
from 
  {DB_NAME}.Teams;
'''

GET_PLAYERS_DATA = f'''select player_id, src_player_id, player_name, batting_type, bowling_type, player_skill, team_id, season, 
 competition_name, is_captain, is_batsman, is_bowler, is_wicket_keeper, player_type, bowl_major_type, player_image_url  
 from  {DB_NAME}.Players; '''

GET_PARTNERSHIP_DATA = f'''select match_id, team_id, innings, striker, striker_runs, striker_balls, partnership_total,
non_striker, non_striker_runs, non_striker_balls from  {DB_NAME}.MatchPartnership'''

GET_BALL_SUMMARY_DATA = f'''select id, match_id, over_number, ball_number, batsman_id, batsman_team_id, is_four, is_six, 
against_bowler as bowler_id,bowler_team_id, is_extra, is_wide, is_no_ball, is_dot_ball, batting_position,over_text,
 innings, runs, extras, is_wicket, wicket_type, is_bowler_wicket, is_leg_bye, batting_phase, x_pitch, y_pitch, 
  out_batsman_id, ball_runs, competition_name, season, bowl_line, bowl_length, is_bye, non_striker_id from  {DB_NAME}.MatchBallSummary'''

GET_VENUE_DATA = f'''select venue_id, stadium_name from  {DB_NAME}.Venue'''

GET_PRESSURE_INDEX = f'''
select 
  id, 
  match_name, 
  innings, 
  raw_ball_no, 
  byb_id, 
  req_batsman_id, 
  req_batsman, 
  bowler_id, 
  bowler, 
  is_striker, 
  MatchPI_Cat, 
  BatsmanMatchPI_Cat as batsman_pressure_cat, 
  BowlerMatchPI_Cat as bowler_pressure_cat, 
  BatsmanPI_wh2h_Cat, 
  entry_point 
from 
  {DB_NAME}.PressureIndex
where
  is_striker = 1 ALLOW FILTERING;
'''

JOIN_DATA_SQL = f'''
select 
  bdf.id, 
  bdf.match_id, 
  md.match_name, 
  md.season, 
  md.venue, 
  bdf.batsman_id, 
  pd.player_name as batsman_name, 
  cast(pd.src_player_id as int) as src_batsman_id,
  bdf.batsman_team_id, 
  td.team_name as batsman_team_name, 
  bdf.batting_position, 
  pd.batting_type, 
  pd.player_skill as batsman_skill,
  pd.player_type as batsman_player_type, 
  bdf.bowler_id, 
  pld.player_name as bowler_name, 
  cast(pld.src_player_id as int) as src_bowler_id,
  bdf.bowler_team_id, 
  td1.team_name as bowler_team_name, 
  bdf.over_number, 
  bdf.over_text, 
  bdf.out_batsman_id, 
  bdf.ball_runs, 
  opd.player_skill as out_batsman_skill, 
  pld.bowling_type, 
  pld.player_skill as bowler_skill,
  pld.player_type as bowler_player_type,  
  bdf.innings, 
  bdf.is_four, 
  bdf.is_six, 
  td.team_short_name as bat_team_short_name, 
  td1.team_short_name as bowl_team_short_name, 
  bdf.runs, 
  bdf.ball_number, 
  bdf.is_leg_bye, 
  bdf.is_extra, 
  bdf.is_no_ball, 
  bdf.is_dot_ball, 
  bdf.bowl_line, 
  bdf.bowl_length, 
  vd.stadium_name, 
  bdf.extras, 
  bdf.is_wide, 
  bdf.is_wicket, 
  bdf.is_bowler_wicket, 
  md.winning_team, 
  md.match_result, 
  bdf.batting_phase, 
  bdf.x_pitch, 
  bdf.y_pitch, 
  bdf.wicket_type, 
  md.match_date, 
  md.competition_name, 
  bdf.load_timestamp as load_timestamp,
  upper(md.overall_nature) as pitch_type, 
  case when bdf.batting_phase = 1 then 'POWERPLAY' when bdf.batting_phase = 2 then 'Middle_first_half' when bdf.batting_phase = 3 then 'Middle_second_half' else 'Death' end as match_phase, 
  td2.team_name as bat_current_team, 
  td2.team_id as bat_current_team_id,
  td3.team_name as bowl_current_team, 
  md.match_date_form, 
  bdf.is_bye, 
  pld.player_image_url as bowler_image_url, 
  pd.player_image_url as batsman_image_url, 
  td.team_image_url as batting_team_image_url, 
  td1.team_image_url as bowling_team_image_url, 
  case when batsman_team_id = winning_team then 'Winning' when winning_team =-1 then 'No Result' else 'Losing' end as winning_type,
  pi.entry_point,
  pi.batsman_pressure_cat,
  pi.bowler_pressure_cat, 
  bdf.non_striker_id,
  nspd.player_name as non_striker_name
from 
  ball_summary_df bdf 
  inner join matches_df md on (bdf.match_id = md.match_id) 
  left join players_data_df pd on (
    bdf.batsman_id = pd.player_id 
    and bdf.competition_name = pd.competition_name
  ) 
  left join players_data_df pld on (
    bdf.bowler_id = pld.player_id 
    and bdf.competition_name = pld.competition_name
  ) 
  left join players_data_df opd on (
    bdf.out_batsman_id = opd.player_id 
    and bdf.competition_name = opd.competition_name
  ) 
  left join players_data_df nspd on (
    bdf.non_striker_id = nspd.player_id 
    and bdf.competition_name = nspd.competition_name
  ) 
  inner join teams_data td on (bdf.batsman_team_id = td.team_id) 
  inner join teams_data td1 on (bdf.bowler_team_id = td1.team_id) 
  left join teams_data td2 on (pd.team_id = td2.team_id) 
  left join teams_data td3 on (pld.team_id = td3.team_id) 
  left join venue_data vd on (vd.venue_id = md.venue) 
  left join pi_data pi on (pi.match_name = md.match_name and pi.byb_id = bdf.id)
where 
  bdf.innings not in (3, 4);
'''

TEAMS_AGGREGATED_SQL = '''select season, match_id, batsman_team_id as team_id, batsman_team_name as team1, 
   bowler_team_name as team2, venue,innings, cast(sum(cast(runs as int)) as int) as total_runs, bowler_team_id, over_number, 
   cast(sum(cast(is_wicket as int)) as int) as total_wickets, winning_type, competition_name, batsman_image_url as player_image_url
   from join_data group by season, venue, match_id, batsman_team_id, batsman_team_name, bowler_team_name, 
   bowler_team_id, winning_team, innings, over_number, competition_name, batsman_image_url, winning_type'''

PARTNERSHIP_SQL = '''select mp.match_id, mp.team_id, td.team_name, mp.innings, md.season, md.venue, mp.striker, 
pd.player_name as striker_name , mp.striker_runs, mp.striker_balls, mp.partnership_total, 
mp.striker_balls + mp.non_striker_balls as partnership_balls, mp.non_striker, md.competition_name,
pd1.player_name as non_striker_name, mp.non_striker_runs, mp.non_striker_balls, case when mp.team_id=md.winning_team 
then 'Winning' when winning_team=-1 then 'No Result' else 'Losing' end as winning_type, tds.team_name as striker_current_team,
tdns.team_name as non_striker_current_team, md.match_date_form as match_date from partnership_base_data 
mp inner join teams_data td on (mp.team_id=td.team_id)  
inner join matches_df md on (mp.match_id=md.match_id)
left join players_data_df pd on (mp.striker=pd.player_id and pd.competition_name=md.competition_name) 
left join players_data_df pd1 on (mp.non_striker=pd1.player_id and pd1.competition_name=md.competition_name) 
left join teams_data tds on (pd.team_id=tds.team_id)  
left join teams_data tdns on (pd1.team_id=tdns.team_id); '''

BOWLER_OVERWISE_SQL = '''
select 
  match_name, 
  match_id, 
  bowler_id as player_id, 
  over_number, 
  season, 
  innings, 
  venue, 
  batting_type, 
  bowling_type, 
  bowler_player_type as player_nationality, 
  winning_type, 
  competition_name, 
  match_phase, 
  stadium_name, 
  bowler_skill as player_skill, 
  cast(
    sum(is_wide) as int
  ) as wides, 
  cast(
    sum(is_no_ball) as int
  ) as no_balls, 
  bowler_team_name as team_name, 
  cast(
    (
      count(ball_number)-(
        sum(is_wide)+ sum(is_no_ball)
      )
    ) as int
  ) as balls, 
  bowler_name as player_name, 
  cast(
    sum(
        case 
          when is_no_ball = 1 and is_leg_bye = 1 then 1 
          when is_no_ball = 1 and is_bye = 1 then 1 
          when is_leg_bye = 1 then 0 
          when is_bye = 1 then 0 
          else runs 
        end
    ) as int
  ) as runs, 
  cast(
    sum(runs) as int
  ) as team_runs, 
  bowler_team_id as team_id, 
  cast(
    sum(is_dot_ball) as int
  ) as dot_balls, 
  cast(
    sum(
      case when is_wicket = 1 
      and is_bowler_wicket = 1 then 1 else 0 end
    ) as int
  ) as wickets, 
  batting_phase, 
  batsman_name, 
  pitch_type, 
  bowl_current_team, 
  batsman_team_name, 
  bat_current_team, 
  cast(
    sum(
      case when is_four = 1 
      and is_leg_bye = 0 
      and is_bye = 0 then 1 else 0 end
    ) as int
  ) as fours, 
  batsman_id, 
  batsman_team_id, 
  cast(
    sum(
      case when is_six = 1 
      and is_leg_bye = 0 
      and is_bye = 0 then 1 else 0 end
    ) as int
  ) as sixes, 
  match_date_form as match_date, 
  bowler_image_url as player_image_url, 
  batsman_image_url, 
  bowling_team_image_url as team_image_url, 
  match_result,
  winning_team
from 
  join_data 
where 
  innings not in (3, 4) 
group by 
  match_id, 
  over_number, 
  player_id, 
  season, 
  innings, 
  venue, 
  batting_type, 
  bowling_type, 
  batsman_id, 
  batsman_team_id, 
  batting_phase, 
  bowler_team_id, 
  bowler_team_name, 
  bowler_name, 
  batsman_name, 
  winning_team, 
  competition_name, 
  match_phase, 
  match_name, 
  pitch_type, 
  stadium_name, 
  bowl_current_team, 
  match_date_form, 
  bowler_image_url, 
  batsman_image_url, 
  batsman_team_name, 
  bat_current_team, 
  winning_type, 
  bowler_skill, 
  bowling_team_image_url, 
  bowler_player_type,
  match_result
'''

# query to get over wise batsman stats
BATSMAN_OVERWISE_SQL = '''
select 
  jd.match_name, 
  jd.match_id, 
  jd.batsman_name as player_name, 
  jd.batsman_id as player_id, 
  jd.over_number, 
  jd.batsman_player_type as player_nationality, 
  jd.batsman_team_id as team_id, 
  jd.season, 
  jd.innings, 
  jd.venue, 
  jd.stadium_name, 
  jd.batting_type, 
  jd.bowling_type, 
  jd.batting_phase, 
  jd.winning_type, 
  jd.batsman_team_name as team_name, 
  jd.bat_team_short_name,
  jd.competition_name, 
  round(
    (
      count(jd.ball_number)- (
        sum(jd.is_wide)
      )
    )
  ) as balls, 
  round(
    sum(is_dot_ball)
  ) as dot_balls, 
  round(
    sum(ball_runs)
  ) as runs, 
  round(
    sum(jd.runs)
  ) as team_runs, 
  coalesce(out.wicket_cnt, 0) as wickets, 
  jd.bowler_id, 
  jd.bowler_name, 
  jd.bowler_team_name, 
  jd.bowler_team_id, 
  jd.bowling_type, 
  round(
    sum(
      case when jd.is_four = 1 
      and jd.is_leg_bye = 0 
      and jd.is_bye = 0 
      and jd.is_wide = 0 then 1 else 0 end
    )
  ) as fours, 
  round(
    sum(
      case when jd.is_six = 1 
      and jd.is_leg_bye = 0 
      and jd.is_bye = 0 
      and jd.is_wide = 0 then 1 else 0 end
    )
  ) as sixes, 
  round(
    sum(
      case when lower(
        replace(bd.out_desc, ' ', '')
      )= 'notout' then 1 else 0 end
    )/ count(jd.ball_number)
  ) as not_out, 
  match_result, 
  jd.batting_position, 
  out.wicket_type as wicket_type, 
  jd.winning_team, 
  jd.match_phase, 
  jd.pitch_type, 
  jd.bat_current_team, 
  jd.bat_current_team_id, 
  jd.bowl_current_team, 
  jd.match_date_form as match_date, 
  jd.batsman_image_url as player_image_url, 
  jd.bowler_image_url as bowl_image_url, 
  jd.batsman_skill as player_skill, 
  sum(
    case when jd.out_batsman_id = jd.batsman_id then 1 else 0 end
  ) as is_batsman_out, 
  jd.batting_team_image_url as team_image_url
from 
  join_data jd 
  left join bat_card_data bd on (
    jd.match_id = bd.match_id 
    and jd.innings = bd.innings 
    and bd.batsman_id = jd.batsman_id
  ) 
  left join (
    select 
      match_id, 
      out_batsman_id, 
      over_number, 
      wicket_type, 
      round(
        count(out_batsman_id)
      ) as wicket_cnt 
    from 
      join_data 
    where 
      out_batsman_id <>-1 
      and innings not in (3, 4) 
    group by 
      match_id, 
      out_batsman_id, 
      over_number, 
      wicket_type
  ) out on (
    out.match_id = jd.match_id 
    and jd.batsman_id = out.out_batsman_id 
    and out.over_number = jd.over_number
  )
where 
  jd.innings not in (3, 4) 
group by 
  jd.batsman_image_url, 
  bowl_image_url, 
  jd.batting_team_image_url, 
  jd.match_id, 
  jd.batsman_id, 
  jd.bowler_id, 
  jd.over_number, 
  jd.batting_position, 
  jd.season, 
  jd.innings, 
  jd.venue, 
  jd.batting_type, 
  jd.bowling_type, 
  jd.batting_phase, 
  jd.winning_team, 
  out.wicket_type, 
  jd.batsman_name, 
  jd.stadium_name, 
  jd.winning_team, 
  jd.batsman_team_id, 
  jd.bowler_team_name, 
  jd.bowler_team_id, 
  jd.batsman_team_name, 
  jd.bat_team_short_name,
  out.wicket_cnt, 
  jd.bowler_name, 
  match_result, 
  jd.competition_name, 
  jd.match_phase, 
  jd.match_name, 
  jd.pitch_type, 
  jd.batsman_player_type, 
  jd.bat_current_team, 
  jd.bat_current_team_id, 
  jd.bowl_current_team, 
  jd.match_date_form, 
  jd.winning_type, 
  jd.batsman_skill
order by 
  jd.match_id
'''

# query to get over wise bowler stats
OVERWISE_BOWLING_ORDER = '''select match_id, bowler_id as player_id, over_number, season, innings, venue, batting_type,
bowling_type, case when bowler_team_id = winning_team then 'WIN' when winning_team=-1 then 'NO RESULT' else 'LOSS' end
 as match_decision, match_date, winning_type, bowler_team_name as team_name, batsman_team_name as team2, 
 cast((count(ball_number)-(sum(is_Wide)+sum(is_no_ball))) as int) as balls, competition_name
 ,bowler_name as player_name, cast(sum(case when is_leg_bye=1 then 0 else runs end) as int) as runs, bowler_skill as player_skill,
  bowler_team_id as team_id, cast(sum(case when is_wicket=1 and is_bowler_wicket=1 then 1 else 0 end) as int) as wickets
 , bowler_image_url as player_image_url, batsman_team_id  
 from join_data group by over_number, match_id, bowler_team_id, bowler_id, season, innings, venue, batting_type, 
 bowling_type, winning_team, match_date, bowler_team_name, batsman_team_name, bowler_name, competition_name, 
  bowler_image_url,batsman_team_id, winning_type,bowler_skill '''

# query to get batting position wise bowler stats per over/multiple overs
POSITIONWISE_BOWLER_OVER = '''select match_id, bowler_id as player_id, batting_position, over_number as overs,
 season, innings, venue, batting_type,bowling_type, winning_type, bowler_team_name as team_name, 
 bowler_team_id as team_id, cast((count(ball_number)-(sum(is_Wide)+sum(is_no_ball))) as int) as balls, 
 bowler_name as player_name, cast(sum(case when is_leg_bye=1 then 0 else runs end) as int) as runs,
cast(sum(case when is_wicket=1 and is_bowler_wicket=1 then 1 else 0 end) as int) as wickets, competition_name, 
bowler_skill as player_skill,
bowl_current_team from join_data group by over_number, batting_position, match_id, bowler_id, season, innings, venue, 
batting_type, bowling_type, bowler_team_id, winning_team, bowler_team_name, bowler_name, competition_name, 
bowl_current_team, winning_type,bowler_skill'''

# query to get batting position wise batsman stats per over/multiple overs
POSITIONWISE_BATSMAN_OVER = '''select match_id, season,venue,batsman_team_id as team_id,batsman_skill as player_skill,
case when batting_position in (1,2) then 'Opening' else batting_position end as batting_position,innings, 
 winning_type, bowling_type, batting_type, over_number as overs, 
  cast((count(ball_number)- (sum(is_wide))) as int) as balls,batsman_team_name as team_name, 
 bat_team_short_name as team_short_name, case when bowler_team_id = winning_team then 'Win' when winning_team=-1 
 then 'No Result' else 'Lose' end as result, cast(sum(ball_runs) as int) as runs, competition_name from  
 join_data group by match_id, season, venue, batsman_team_id, winning_team, bowler_team_id, batsman_team_name, 
 batting_position, innings, winning_type, bowling_type, batting_type, over_number, result, bat_team_short_name, 
 competition_name,batsman_skill'''

# Query to get match playing XI

MATCH_PLAYER_SQL = f'''select match_id, match_date, match_time, venue, season, team1, team1_players, team2_players,
winning_team,competition_name, team2 from  {DB_NAME}.Matches'''

MATCH_PLAYING_XI = '''
select 
  mp.match_id, 
  mp.innings, 
  mp.season, 
  mp.team1 as team_id, 
  td.team_name as team1, 
  td.team_short_name as team1_short_name, 
  mp.player_id, 
  pd.player_skill, 
  pd.player_name, 
  pd.src_player_id, 
  mp.venue, 
  mp.winning_type, 
  mp.team2 as team2_id, 
  td1.team_name as team2, 
  td1.team_short_name as team2_short_name, 
  mp.match_decision, 
  pd.batting_type, 
  pd.bowling_type, 
  mp.match_date, 
  mp.match_time, 
  mp.competition_name, 
  pd.player_image_url, 
  bd.batting_position 
from 
  match_playing_xi_data mp 
  left join players_data_df pd on (
    mp.player_id = pd.player_id 
    and pd.competition_name = mp.competition_name
  ) 
  inner join teams_data td on (td.team_id = mp.team1) 
  inner join teams_data td1 on (td1.team_id = mp.team2) 
  left join bat_card_data bd on (
    bd.match_id = mp.match_id 
    and mp.player_id = bd.batsman_id
  ) 
order by 
  mp.match_id, 
  mp.player_id
'''

GET_GLOBAL_BAT_STATS = f'''select batsman_id as player_id, total_matches_played as matches, total_runs as runs, 
batting_average as average, strike_rate, hundreds, fifties, not_outs, num_sixes as sixes, num_fours as fours, 
highest_score from  {DB_NAME}.BatsmanGlobalStats'''

MATCHES_JOIN_DATA = '''select md.season, md.venue, vd.stadium_name, md.match_id, md.match_name, md.match_date, md.team1, 
td.team_name as team1_name, td1.team_name as team2_name, md.team2, case when team1 = md.winning_team 
then 'Winning' when winning_team=-1 then 'No Result' else 'Losing' end as winning_type,
md.winning_team as winning_team_id, td2.team_short_name as winning_team, td.team_short_name as team1_short_name, 
td1.team_short_name as team2_short_name, md.competition_name, 
md.team1_score, md.team1_wickets, md.team1_overs, md.team2_score, md.team2_wickets, md.team2_overs, md.match_result, 
md.match_date from matches_df md left join teams_data td on (md.team1=td.team_id) left join teams_data td1 on (
md.team2=td1.team_id) left join teams_data td2 on (md.winning_team=td2.team_id) left join venue_data vd on (
vd.venue_id=md.venue) '''


BATSMAN_STATS = '''select match_date, over_number as overs, cast(sum(wickets) as int) as num_dismissals, cast(sum(dot_balls) as int) as num_dots, 
cast(sum(case when runs=1 then 1 else 0 end) as int) as num_singles, cast(sum(case when runs=2 then 1 else 0 end) 
as int) as num_doubles, cast(sum(case when runs=3 then 1 else 0 end) as int) as num_triples, 
cast(sum(coalesce(fours,0)) as int) as num_fours, cast(sum(coalesce(sixes,0)) as int) as num_sixes, cast(sum(wickets) as int) as wickets
, cast(sum(balls) as int) as balls_faced, cast(sum(runs) as int) as runs, cast(sum(is_batsman_out) as int) as is_batsman_out,
batting_phase as phase, season as year,
innings, batting_type, bowling_type, match_id, player_name, match_name, cast(player_id as int) as player_id, 
cast(bowler_id as int) as bowler_id,bowler_name as bowler, stadium_name, pitch_type, venue,
match_phase, cast(sum(team_runs) as int) as team_runs, team_id, team_name from batsman_overwise_df group by match_id, 
player_id, bowler_id, batting_phase, season, innings,batting_type, bowling_type, player_name, match_name, match_phase,
 bowler_name, venue, stadium_name, pitch_type, over_number, match_date, team_id, team_name'''

BOWLER_STATS = '''select match_date, over_number as overs, cast(player_id as int) as player_id, player_name, 
cast(sum(case when runs=1 then 1 else 0 end) as int) as num_singles_conceded,
cast(sum(case when runs=2 then 1 else 0 end) as int) as num_doubles_conceded, 
cast(sum(case when runs=3 then 1 else 0 end) as int) as num_triples_conceded, 
cast(sum(coalesce(fours,0)) as int) as num_fours_conceded, cast(sum(coalesce(sixes,0)) as int) as num_sixes_conceded, 
cast(sum(dot_balls) as int) as num_dots_bowled, cast((sum(wides)+sum(no_balls)) as int) as num_extras_conceded, 
cast(sum(balls) as int) as total_balls_bowled, cast(sum(wides) as int) as wides, cast(sum(no_balls) as int) as no_balls,
cast(sum(runs) as int) as total_runs_conceded, cast(sum(wickets) as int) as total_wickets_taken, 
match_id, batting_phase as phase, season as year, cast(batsman_id as int) as batsman_id, match_name, venue,
innings, batting_type, bowling_type, match_phase, batsman_name as batsman, stadium_name, pitch_type, competition_name, 
batsman_team_id, team_id, winning_type from bowler_overwise_df group by match_id, match_name, batting_phase, season, player_id, stadium_name,competition_name,
innings, batting_type, bowling_type,batsman_id, match_phase, batsman_name, player_name, pitch_type, match_date, over_number,
batsman_team_id, team_id, venue, winning_type'''

GET_GPS_DELIVERY_DATA = f'''select season, player_id, player_name, ball_no, date_name, delivery_runup_distance, peak_player_load,
     raw_peak_roll, raw_peak_yaw, team_name from  {DB_NAME}.{GPS_DELIVERY_TABLE_NAME} where is_match_fielding=1 '''

GET_CONTRIBUTION_SCORE_DATA = f'''select match_date, game_id, season, competition_name, team, team_id, player_id, 
player, player_type, position, bowling_type, retained, speciality, batting_contribution_score, batting_type,
runs_scored, bat_innings, batting_consistency_score, bowling_contribution_score, overall_economy,  
total_wickets as wickets_taken, bowl_innings, bowling_consistency_score, overall_contribution_score, overall_consistency_score,
bowl_powerplay_contribution_score, bowl_7_10_overs_contribution_score, bowl_11_15_overs_contribution_score
, bowl_deathovers_contribution_score, bat_powerplay_contribution_score, bat_7_10_overs_contribution_score, bat_11_15_overs_contribution_score,
actual_powerplay_over_balls,actual_7_10_over_balls,actual_11_15_over_balls,actual_death_over_balls,
bat_deathovers_contribution_score, overall_powerplay_contribution_score, overall_7_10_overs_contribution_score, 
overall_11_15_overs_contribution_score, overall_deathovers_contribution_score, in_auction, actual_powerplay_over_runs, 
actual_7_10_over_runs, actual_11_15_over_runs, actual_death_over_runs, bat_expectations, bowl_expectations, balls_faced, 
batting_strike_rate,dismissed_on, total_overs_bowled, runs_conceded,overall_fours, overall_sixes, fow_during_stay, non_striker_runs,
arrived_on,is_hatrick, bat_consistency_score_powerplay, bat_consistency_score_7_10, bat_consistency_score_11_15, 
bat_consistency_score_deathovers, bowl_consistency_score_powerplay, bowl_consistency_score_7_10, bowl_consistency_score_11_15, 
bowl_consistency_score_deathovers,is_won, is_out, venue_id as venue, total_balls_bowled  from  {DB_NAME}.contributionScore; '''

GET_AGG_CONTRIBUTION_DATA = '''select season, is_won, competition_name, team, team_id, player_id, player, player_type, 
 position, bowling_type,batting_type, retained,venue, in_auction, speciality, count(distinct game_id) as matches_played,
 sum(coalesce(batting_contribution_score,0)) as batting_contribution_score, sum(coalesce(balls_faced,0)) as balls_played,
sum(runs_scored) as runs_scored, sum(coalesce(bat_innings,0)) as bat_innings, sum(coalesce(batting_consistency_score,0)) as
 batting_consistency_score, sum(coalesce(bowling_contribution_score,0)) as bowling_contribution_score, 
 sum(overall_economy) as overall_economy, cast(sum(coalesce(wickets_taken,0)) as int) as wickets_taken, sum(coalesce(bowl_innings,0)) as bowl_innings, 
 sum(coalesce(bowling_consistency_score,0)) as bowling_consistency_score, 
 sum(coalesce(overall_contribution_score,0)) as overall_contribution_score, 
 sum(coalesce(overall_consistency_score,0)) as overall_consistency_score,
 sum(actual_powerplay_over_balls) as actual_powerplay_over_balls, 
 sum(actual_7_10_over_balls) as actual_7_10_over_balls, 
 sum(actual_11_15_over_balls) as actual_11_15_over_balls, 
 sum(actual_death_over_balls) as actual_death_over_balls,
 sum(coalesce(bowl_powerplay_contribution_score,0)) as bowl_powerplay_contribution_score, 
 sum(coalesce(bowl_7_10_overs_contribution_score,0)) as bowl_7_10_overs_contribution_score, 
 sum(coalesce(bowl_11_15_overs_contribution_score,0)) as bowl_11_15_overs_contribution_score
, sum(coalesce(bowl_deathovers_contribution_score,0)) as bowl_deathovers_contribution_score, 
sum(coalesce(bat_powerplay_contribution_score,0)) as bat_powerplay_contribution_score, 
sum(coalesce(bat_7_10_overs_contribution_score,0)) as bat_7_10_overs_contribution_score, 
sum(coalesce(bat_11_15_overs_contribution_score,0)) as bat_11_15_overs_contribution_score,
sum(coalesce(bat_deathovers_contribution_score,0)) as bat_deathovers_contribution_score, 
sum(coalesce(overall_powerplay_contribution_score,0)) as overall_powerplay_contribution_score, 
sum(coalesce(overall_7_10_overs_contribution_score,0)) as overall_7_10_overs_contribution_score, 
sum(coalesce(overall_11_15_overs_contribution_score,0)) as overall_11_15_overs_contribution_score, 
sum(coalesce(overall_deathovers_contribution_score,0)) as overall_deathovers_contribution_score,
sum(actual_powerplay_over_runs) as actual_powerplay_over_runs, sum(actual_7_10_over_runs) as actual_7_10_over_runs, 
sum(actual_11_15_over_runs) as actual_11_15_over_runs, sum(actual_death_over_runs) as actual_death_over_runs,
sum(actual_powerplay_over_balls) as actual_powerplay_over_balls, sum(actual_7_10_over_balls) as actual_7_10_over_balls, 
sum(actual_11_15_over_balls) as actual_11_15_over_balls, sum(actual_death_over_balls) as actual_death_over_balls,
 sum(batting_strike_rate) as batting_strike_rate,  sum(case when coalesce(runs_scored,0)>=30 then 1 else 0 end) as total_thirty_plus_scores,
  sum(case when is_out=0 then 0 else 1 end) as dismissal,
 sum(coalesce(cast(total_balls_bowled/6 as int) + cast(total_balls_bowled%6 as decimal)/10,0)) as total_overs_bowled,
 sum(coalesce(total_balls_bowled,0)) as total_balls_bowled, 
 sum(coalesce(runs_conceded,0)) as runs_conceded, sum(case when coalesce(wickets_taken,0)>=3 then 1 else 0 end) as total_three_plus_wickets,
 sum(coalesce(bat_consistency_score_powerplay,0)) as bat_consistency_score_powerplay, 
 sum(coalesce(bat_consistency_score_7_10,0)) as bat_consistency_score_7_10, 
 sum(coalesce(bat_consistency_score_11_15,0)) as bat_consistency_score_11_15, 
 sum(coalesce(bat_consistency_score_deathovers,0)) as  bat_consistency_score_deathovers, 
 sum(coalesce(bowl_consistency_score_powerplay,0)) as bowl_consistency_score_powerplay, 
 sum(coalesce(bowl_consistency_score_7_10,0)) as bowl_consistency_score_7_10, 
 sum(coalesce(bowl_consistency_score_11_15,0)) as bowl_consistency_score_11_15, 
 sum(coalesce(bowl_consistency_score_deathovers,0)) as  bowl_consistency_score_deathovers  
 from contribution_data group by season, competition_name, team_id, player_id, player, player_type, batting_type, 
 position, bowling_type, retained, speciality, in_auction,is_won,team, venue ;'''

GET_FIELD_ANALYSIS = f'''select id, match_name, team_name, player_name, season, player_type, clean_takes, 
miss_fields, miss_fields_cost, dives_made, runs_saved, dives_missed, missed_runs, good_attempt, taken, stumping, 
dropped_percent_difficulty, caught_and_bowled, good_return, poor_return, direct_hit, missed_shy, run_out_obtained, 
pop_ups, support_run, back_up, standing_back_plus, standing_back_minus, standing_up_plus, standing_up_minus, 
returns_taken_plus, returns_untidy, player_id from {DB_NAME}.fieldingAnalysis'''

