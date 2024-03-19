import sys
sys.path.append("./../../")
sys.path.append("./")
from log.log import get_logger
from DataIngestion.config import BOWLER_GLOBAL_STATS_TABLE, BOWLER_GLOBAL_STATS_COL
from DataIngestion.query import GET_MATCH_SUMMARY, GET_BOWL_CARD_DATA, GET_BOWL_GLOBAL_STATS_TIMESTAMP, \
    GET_BOWL_GLOBAL_STATS_DATA
from common.dao.fetch_db_data import getPandasFactoryDF, getMaxId
import pandasql as psql
import pandas as pd
from common.db_config import DB_NAME
from DataIngestion.utils.helper import generateSeq

logger = get_logger("Ingestion", "Ingestion")


def getBowlerGlobalData(session, load_timestamp):
    logger.info("Bowler Global Stats Data Generation Started!")
    max_timestamp = getPandasFactoryDF(session, GET_BOWL_GLOBAL_STATS_TIMESTAMP).iloc[0, 0]

    match_data_df1 = getPandasFactoryDF(session, GET_MATCH_SUMMARY).explode('team1_players').drop('team2_players',
                                                                                                  axis=1)
    match_data_df2 = getPandasFactoryDF(session, GET_MATCH_SUMMARY).explode('team2_players').drop('team1_players',
                                                                                                  axis=1)

    bowl_card_df = getPandasFactoryDF(session, GET_BOWL_CARD_DATA)

    bowl_card_df['overs'] = pd.to_numeric(bowl_card_df['overs'])

    if max_timestamp:
        match_data_df1 = match_data_df1[match_data_df1['load_timestamp'] > max_timestamp]
        match_data_df2 = match_data_df2[match_data_df2['load_timestamp'] > max_timestamp]
        bowl_card_df = bowl_card_df[bowl_card_df['load_timestamp'] > max_timestamp]
    else:
        match_data_df1 = match_data_df1[match_data_df1['load_timestamp'] > '01-01-2000 00:00:00']
        match_data_df2 = match_data_df2[match_data_df2['load_timestamp'] > '01-01-2000 00:00:00']
        bowl_card_df = bowl_card_df[bowl_card_df['load_timestamp'] > '01-01-2000 00:00:00']

    pd.set_option('display.max_columns', 20)

    matches_played_df1 = psql.sqldf('''select team1_players as player_id, season,competition_name, 
    count(match_id) as cnt from match_data_df1 group by team1_players,season,competition_name''')

    matches_played_df2 = psql.sqldf('''select team2_players as player_id, season,competition_name, 
    count(match_id) as cnt from match_data_df2 group by team2_players,season,competition_name''')

    bowler_stats_df = psql.sqldf('''select bowler_id, count(innings) as total_innings_played, 
sum(total_legal_balls) as total_balls_bowled, 
sum(runs) as total_runs_conceded, season,competition_name, 
sum(case when wickets between 5 and 9 then 1 else 0 end) as num_five_wkt_hauls,
sum(case when wickets=10 then 1 else 0 end) as num_ten_wkt_hauls,
sum(case when wickets=4 then 1 else 0 end) as num_four_wkt_hauls, sum(wides+no_balls) as
 num_extras_conceded, sum(wickets) as total_wickets, case when (sum(total_legal_balls)%6)==0 
then ((sum(total_legal_balls)*1.00)/6) else (sum(total_legal_balls)/6) + 
((sum(total_legal_balls)%6)/10.00) end as total_overs_bowled from 
 bowl_card_df group by bowler_id, season, competition_name''')

    # get best bowling figure

    bowl_card_df['bbw_rank'] = bowl_card_df.groupby(['bowler_id', 'season', 'competition_name'])['wickets'] \
        .rank(method='first', ascending=False).astype(int)
    bb_figures_df = bowl_card_df[bowl_card_df['bbw_rank'] == 1][
        ['bowler_id', 'runs', 'wickets', 'season', 'competition_name']]
    bb_figures_df['bbr_rank'] = bowl_card_df.groupby(['bowler_id', 'season', 'wickets', 'competition_name'])['runs'] \
        .rank(method='first', ascending=True).astype(int)
    bb_figures_df = bb_figures_df[bb_figures_df['bbr_rank'] == 1].drop('bbr_rank', axis=1)

    # get overall best bowling figure
    bb_overall_figures_df = bb_figures_df.copy()
    bb_overall_figures_df['bbw_rank'] = bb_overall_figures_df.groupby(['bowler_id'])['wickets'] \
        .rank(method='first', ascending=False).astype(int)
    bb_overall_figures_df = bb_overall_figures_df[bb_overall_figures_df['bbw_rank'] == 1][
        ['bowler_id', 'runs', 'wickets', 'competition_name']]
    bb_overall_figures_df['bbr_rank'] = bb_overall_figures_df.groupby(['bowler_id', 'wickets', 'competition_name'])['runs'] \
        .rank(method='first', ascending=True).astype(int)
    bb_overall_figures_df = bb_overall_figures_df[bb_overall_figures_df['bbr_rank'] == 1].drop('bbr_rank', axis=1)
    bb_overall_figures_df = bb_overall_figures_df[bb_overall_figures_df['competition_name'] == 'IPL']

    bowling_df = psql.sqldf(''' select bowl.bowler_id, coalesce(mp1.cnt,0) + coalesce(mp2.cnt,0) as 
total_matches_played, bowl.total_innings_played, bowl.total_balls_bowled, 
bowl.total_runs_conceded, bowl.num_four_wkt_hauls, bowl.num_five_wkt_hauls, bowl.num_ten_wkt_hauls, 
bowl.num_extras_conceded, bowl.total_wickets, round(coalesce(((bowl.total_runs_conceded*1.00)/bowl.total_wickets),0.0),2) 
as bowling_average, round(coalesce(((bowl.total_balls_bowled*1.00)/bowl.total_wickets),0.0),2) as bowling_strike_rate, 
round(coalesce(((bowl.total_runs_conceded*1.00)/bowl.total_overs_bowled),0.0),2) as bowling_economy, 
bowl.total_overs_bowled, bowl.season, bowl.competition_name, bb.runs as bbr, bb.wickets as bbw from bowler_stats_df bowl 
left join bb_figures_df bb on 
(bowl.bowler_id=bb.bowler_id and bowl.season=bb.season and bowl.competition_name=bb.competition_name) 
left join matches_played_df1 mp1 on (bowl.bowler_id=mp1.player_id and bowl.season=mp1.season 
and bowl.competition_name=mp1.competition_name) 
left join matches_played_df2 mp2 on (bowl.bowler_id=mp2.player_id and bowl.season=mp2.season 
and bowl.competition_name=mp2.competition_name) ''')

    bowling_stats_df = psql.sqldf(''' select bowl.bowler_id, sum(bowl.total_matches_played) as 
total_matches_played, sum(bowl.total_innings_played) as total_innings_played, 
sum(bowl.total_balls_bowled) as total_balls_bowled, 
sum(bowl.total_runs_conceded) as total_runs_conceded, sum(bowl.num_four_wkt_hauls) as num_four_wkt_hauls, 
sum(bowl.num_five_wkt_hauls) as num_five_wkt_hauls, sum(bowl.num_ten_wkt_hauls) as num_ten_wkt_hauls, 
sum(bowl.num_extras_conceded) as num_extras_conceded, sum(bowl.total_wickets) as total_wickets,
 round(coalesce(((sum(bowl.total_runs_conceded)*1.00)/sum(bowl.total_wickets)),0.0),2) as bowling_average,
round(coalesce(((sum(bowl.total_balls_bowled)*1.00)/sum(bowl.total_wickets)),0.0),2) as bowling_strike_rate, 
round(coalesce(((sum(bowl.total_runs_conceded)*1.00)/sum(bowl.total_overs_bowled)),0.0),2) as bowling_economy, 
case when (sum(total_balls_bowled)%6)==0 then ((sum(total_balls_bowled)*1.00)/6) else (sum(total_balls_bowled)/6) + 
((sum(total_balls_bowled)%6)/10.00) end as total_overs_bowled, 0000 as season, 'IPL' as competition_name, 
bb.runs as bbr, bb.wickets as bbw from bowling_df bowl 
left join bb_overall_figures_df bb on 
(bowl.bowler_id=bb.bowler_id and bowl.competition_name=bb.competition_name) where bowl.competition_name='IPL' 
group by bowl.bowler_id''')

    bowler_global_stats_df = bowling_stats_df.append(bowling_df)

    existing_global_stats = getPandasFactoryDF(session, GET_BOWL_GLOBAL_STATS_DATA)
    existing_global_stats[['bowling_average', 'bowling_strike_rate', 'bowling_economy', 'total_overs_bowled']] = \
        existing_global_stats[['bowling_average', 'bowling_strike_rate', 'bowling_economy', 'total_overs_bowled']] \
            .apply(pd.to_numeric, errors='coerce')
    existing_global_stats['bbw'] = existing_global_stats['best_figure'].apply(lambda x: x.split('/')[0])
    existing_global_stats['bbr'] = existing_global_stats['best_figure'].apply(lambda x: x.split('/')[1])

    final_global_data = psql.sqldf('''select egs.id, bgs.bowler_id, 
    bgs.total_matches_played+coalesce(egs.total_matches_played,0) as total_matches_played ,
    bgs.total_innings_played+coalesce(egs.total_innings_played,0) as total_innings_played,
    bgs.total_balls_bowled+coalesce(egs.total_balls_bowled,0) as total_balls_bowled ,
    bgs.total_runs_conceded+coalesce(egs.total_runs_conceded,0) as total_runs_conceded,
    bgs.num_four_wkt_hauls+coalesce(egs.num_four_wkt_hauls,0) as num_four_wkt_hauls,
    bgs.num_five_wkt_hauls+coalesce(egs.num_five_wkt_hauls,0) as num_five_wkt_hauls,
    bgs.num_ten_wkt_hauls+coalesce(egs.num_ten_wkt_hauls,0) as num_ten_wkt_hauls,
    bgs.num_extras_conceded+coalesce(egs.num_extras_conceded,0) as num_extras_conceded,
    bgs.total_wickets+coalesce(egs.total_wickets,0) as total_wickets, 
    case when ((bgs.total_balls_bowled+coalesce(egs.total_balls_bowled,0))%6)==0 then 
    (((bgs.total_balls_bowled+coalesce(egs.total_balls_bowled,0))*1.00)/6) else 
    ((bgs.total_balls_bowled+coalesce(egs.total_balls_bowled,0))/6) + 
    (((bgs.total_balls_bowled+coalesce(egs.total_balls_bowled,0))%6)/10.00) end as total_overs_bowled,
    case when egs.bowling_average is null then bgs.bowling_average else 
    round(coalesce((((bgs.total_runs_conceded+coalesce(egs.total_runs_conceded,0))*1.00)/
    (bgs.total_wickets+coalesce(egs.total_wickets,0))),0.0),2) end as bowling_average,
    case when egs.bowling_strike_rate is null then bgs.bowling_strike_rate else 
    round(coalesce((((bgs.total_balls_bowled+coalesce(egs.total_balls_bowled,0))*1.00)/
    (bgs.total_wickets+coalesce(egs.total_wickets,0))),0.0),2) end as bowling_strike_rate,
    case when egs.bowling_economy is null then bgs.bowling_economy else 
    round(coalesce((((bgs.total_runs_conceded+coalesce(egs.total_runs_conceded,0))*1.00)/
    (case when ((bgs.total_balls_bowled+coalesce(egs.total_balls_bowled,0))%6)==0 then 
    (((bgs.total_balls_bowled+coalesce(egs.total_balls_bowled,0))*1.00)/6) else 
    ((bgs.total_balls_bowled+coalesce(egs.total_balls_bowled,0))/6) + 
    (((bgs.total_balls_bowled+coalesce(egs.total_balls_bowled,0))%6)/10.00) end)),0.0),2) end as bowling_economy,
    case when bgs.bbw>coalesce(egs.bbw,0) then bgs.bbw else egs.bbw end as bbw, 
    case when bgs.bbw>coalesce(egs.bbw,0) then bgs.bbr else egs.bbr end as bbr, bgs.season,bgs.competition_name 
    from bowler_global_stats_df bgs left join existing_global_stats egs on (bgs.bowler_id=egs.bowler_id and 
    bgs.competition_name=egs.competition_name and egs.season=bgs.season)''')

    final_global_data['best_figure'] = final_global_data['bbw'].fillna(0).astype(int).map(str) + '/' + \
                                       final_global_data['bbr'].fillna(0).astype(int).map(str)

    final_global_data['load_timestamp'] = load_timestamp

    new_df = final_global_data.loc[final_global_data[BOWLER_GLOBAL_STATS_COL].isnull()].drop(['bbw', 'bbr'], axis=1)
    max_key_val = getMaxId(session, BOWLER_GLOBAL_STATS_TABLE, BOWLER_GLOBAL_STATS_COL, DB_NAME)
    new_data = generateSeq(new_df.drop(BOWLER_GLOBAL_STATS_COL, axis=1)
                           .sort_values(["bowler_id", "competition_name", "season"]),
                           BOWLER_GLOBAL_STATS_COL, max_key_val).to_dict(orient='records')

    update_df = final_global_data.loc[final_global_data[BOWLER_GLOBAL_STATS_COL].notnull()].drop(['bbw', 'bbr'],
                                                                                                 axis=1).copy()
    update_df[BOWLER_GLOBAL_STATS_COL] = update_df[BOWLER_GLOBAL_STATS_COL].astype(int)
    updated_data = update_df.to_dict(orient='records')
    logger.info("Bowler Global Stats Data Generation Completed!")
    return updated_data, new_data

