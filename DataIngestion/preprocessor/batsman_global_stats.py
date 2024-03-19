import sys

sys.path.append("./../../")
sys.path.append("./")
from log.log import get_logger
from DataIngestion.config import BATSMAN_GLOBAL_STATS_TABLE, BATSMAN_GLOBAL_STATS_COL
from common.dao.fetch_db_data import getPandasFactoryDF, getMaxId
import pandasql as psql
import pandas as pd
from common.db_config import DB_NAME
from DataIngestion.utils.helper import generateSeq
from DataIngestion.query import GET_MATCH_SUMMARY, GET_BAT_CARD_DATA, GET_BAT_GLOBAL_STATS_TIMESTAMP, \
    GET_BAT_GLOBAL_STATS_DATA

logger = get_logger("Ingestion", "Ingestion")


def getBatsmanGlobalData(session, load_timestamp):
    logger.info("Batsman Global Stats Data Generation Started!")

    max_timestamp = getPandasFactoryDF(session, GET_BAT_GLOBAL_STATS_TIMESTAMP).iloc[0, 0]

    match_data_df1 = getPandasFactoryDF(session, GET_MATCH_SUMMARY).explode('team1_players').drop('team2_players',
                                                                                                  axis=1)
    match_data_df2 = getPandasFactoryDF(session, GET_MATCH_SUMMARY).explode('team2_players').drop('team1_players',
                                                                                                  axis=1)

    bat_card_df = getPandasFactoryDF(session, GET_BAT_CARD_DATA)

    if max_timestamp:
        match_data_df1 = match_data_df1[match_data_df1['load_timestamp'] > max_timestamp]
        match_data_df2 = match_data_df2[match_data_df2['load_timestamp'] > max_timestamp]
        bat_card_df = bat_card_df[bat_card_df['load_timestamp'] > max_timestamp]
    else:
        match_data_df1 = match_data_df1[match_data_df1['load_timestamp'] > '01-01-2000 00:00:00']
        match_data_df2 = match_data_df2[match_data_df2['load_timestamp'] > '01-01-2000 00:00:00']
        bat_card_df = bat_card_df[bat_card_df['load_timestamp'] > '01-01-2000 00:00:00']

    pd.set_option('display.max_columns', 20)

    matches_played_df1 = psql.sqldf('''select team1_players as player_id, season,competition_name,count(match_id) as cnt 
                                    from match_data_df1 group by team1_players, season,competition_name''')

    matches_played_df2 = psql.sqldf('''select team2_players as player_id, season,competition_name,count(match_id) as cnt 
                                     from match_data_df2 group by team2_players, season,competition_name''')

    batsman_stats_df = psql.sqldf('''select batsman_id, competition_name, season, sum(case when out_desc='' then 0 else 
    1 end) as total_innings_batted, sum(runs) as total_runs,sum(case when runs>=100 then 1 else 0 end) 
 as hundreds, sum(case when runs between 50 and 99 then 1 else 0 end) as fifties, 
sum(case when runs=0 then 1 else 0 end) as duck_outs, max(runs) as 
highest_score, sum(case when lower(replace(out_desc, ' ', ''))='notout' then 1 else 0 end) 
as not_outs, sum(fours) as num_fours, sum(sixes) as num_sixes, sum(balls) as balls_faced 
from bat_card_df where innings not in (3,4) group by batsman_id, competition_name, season''')

    batting_df = psql.sqldf('''select bdf.batsman_id, coalesce(mp1.cnt,0) + coalesce(mp2.cnt,0) as
total_matches_played, bdf.total_innings_batted, bdf.total_runs, bdf.hundreds, bdf.fifties, bdf.duck_outs,
bdf.highest_score, bdf.not_outs, bdf.num_fours, bdf.num_sixes, bdf.competition_name, bdf.season,
round(coalesce(((bdf.total_runs*1.00)/(bdf.total_innings_batted-bdf.not_outs)),0),2) as batting_average, 
round(coalesce(((bdf.total_runs*100.00)/bdf.balls_faced),0),2) as strike_rate, bdf.balls_faced  as total_balls_faced 
from batsman_stats_df bdf left join matches_played_df1 mp1 on
(bdf.batsman_id=mp1.player_id and bdf.season=mp1.season and bdf.competition_name=mp1.competition_name) 
left join matches_played_df2 mp2 on (bdf.batsman_id=mp2.player_id and bdf.season=mp2.season and 
bdf.competition_name=mp2.competition_name)''')

    batsman_global_stats = psql.sqldf('''select  batsman_id, sum(total_matches_played) as total_matches_played, 
  sum(total_innings_batted) as total_innings_batted, sum(total_runs) as total_runs, sum(hundreds) as hundreds,
   sum(fifties) as  fifties, sum(duck_outs) as duck_outs, max(highest_score) as highest_score, sum(not_outs) as not_outs,
  sum(num_fours) as num_fours, sum(num_sixes) as num_sixes, 'IPL' as competition_name, 0000 as season,
  round(coalesce(((sum(total_runs)*1.00)/(sum(total_innings_batted)-sum(not_outs))),0),2) as batting_average, 
  round(coalesce(((sum(total_runs)*100.00)/sum(total_balls_faced)),0),2) as strike_rate, 
  sum(total_balls_faced) as total_balls_faced from batting_df where competition_name='IPL' group by batsman_id''')

    batsman_global_stats = batsman_global_stats.append(batting_df, ignore_index=True)

    existing_global_stats = getPandasFactoryDF(session, GET_BAT_GLOBAL_STATS_DATA)
    existing_global_stats[['batting_average', 'strike_rate']] = existing_global_stats[['batting_average', 'strike_rate']] \
        .apply(pd.to_numeric, errors='coerce')

    final_global_data = psql.sqldf('''select egs.id, bgs.batsman_id, 
    bgs.total_matches_played+coalesce(egs.total_matches_played,0) as total_matches_played, 
    bgs.total_innings_batted+coalesce(egs.total_innings_batted,0) as total_innings_batted, 
    bgs.total_runs+coalesce(egs.total_runs,0) as total_runs, bgs.hundreds+coalesce(egs.hundreds,0) as hundreds,
    bgs.fifties+coalesce(egs.fifties,0) as fifties, bgs.duck_outs+coalesce(egs.duck_outs,0) as duck_outs, 
    case when bgs.highest_score>coalesce(egs.highest_score,0) then bgs.highest_score else 
    coalesce(egs.highest_score,0) end as highest_score, bgs.not_outs+coalesce(egs.not_outs,0) as not_outs, 
    bgs.num_fours+coalesce(egs.num_fours,0) as num_fours, bgs.num_sixes+coalesce(egs.num_sixes,0) as num_sixes,
    bgs.competition_name, bgs.season, case when egs.batting_average is null then bgs.batting_average else 
    round(coalesce((((bgs.total_runs+coalesce(egs.total_runs,0))*1.00)/((bgs.total_innings_batted+
    coalesce(egs.total_innings_batted,0))-(bgs.not_outs+coalesce(egs.not_outs,0)))),0),2) end as batting_average, 
    case when egs.strike_rate is null then bgs.strike_rate else 
    round(coalesce((((bgs.total_runs+coalesce(egs.total_runs,0))*100.00)/(bgs.total_balls_faced+
    coalesce(egs.total_balls_faced,0))),0),2) end as strike_rate, 
    bgs.total_balls_faced+coalesce(egs.total_balls_faced,0) as total_balls_faced from batsman_global_stats bgs 
    left join existing_global_stats egs on (bgs.batsman_id=egs.batsman_id and bgs.competition_name=egs.competition_name 
    and egs.season=bgs.season)''')

    final_global_data["load_timestamp"] = load_timestamp

    new_df = final_global_data.loc[final_global_data[BATSMAN_GLOBAL_STATS_COL].isnull()]
    max_key_val = getMaxId(session, BATSMAN_GLOBAL_STATS_TABLE, BATSMAN_GLOBAL_STATS_COL, DB_NAME, False)
    new_data = generateSeq(new_df.drop(BATSMAN_GLOBAL_STATS_COL, axis=1)
                           .sort_values(["batsman_id", "competition_name", "season"]),
                           BATSMAN_GLOBAL_STATS_COL, max_key_val).to_dict(orient='records')

    update_df = final_global_data.loc[final_global_data[BATSMAN_GLOBAL_STATS_COL].notnull()].copy()
    update_df[BATSMAN_GLOBAL_STATS_COL] = update_df[BATSMAN_GLOBAL_STATS_COL].astype(int)
    updated_data = update_df.to_dict(orient='records')

    logger.info("Batsman Global Stats Data Generation Completed!")
    return updated_data, new_data
