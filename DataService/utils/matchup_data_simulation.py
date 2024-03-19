import os
import sys

import numpy as np
import pandas as pd

sys.path.append("./../../")
sys.path.append("./")
from DataService.utils.helper import executeQuery, connection_duckdb
from DataIngestion.utils.helper import readCSV
from DataIngestion.config import FILE_SHARE_PATH


# to run this file separately, uncomment the below line 15 and comment line 16. Also, in src/run.py,
# uncomment the last line of the file i.e., return DFR
# con = DFR.init().con
con = connection_duckdb()
from log.log import get_logger

logger = get_logger("Matchup_Data", "Matchup_Data")


def getPartnershipStay(connection, striker, non_striker, match_date):
    if match_date:
        partnership_stay_sql = f'''select striker as batsman_id, striker_name as batsman, non_striker_name as non_striker, 
                                    non_striker as non_striker_id,
                                    cast(round(((6.00*sum(striker_balls))/sum(partnership_balls))) as int) as ball_percent 
                                    from partnership_data where season in (2019,2020,2021,2022) and striker_name='{striker}' and non_striker_name='{non_striker}' and match_date<'{match_date}' 
                                    group by striker, striker_name, non_striker_name, non_striker; '''
    else:
        partnership_stay_sql = f'''select striker as batsman_id, striker_name as batsman, non_striker_name as non_striker, 
                                    non_striker as non_striker_id,
                                    cast(round(((6.00*sum(striker_balls))/sum(partnership_balls))) as int) as ball_percent 
                                    from partnership_data where striker_name='{striker}' and non_striker_name='{non_striker}' and season in (2019,2020,2021,2022)  
                                    group by striker, striker_name, non_striker_name, non_striker; '''
    partnership_df = executeQuery(connection, partnership_stay_sql)
    partnership_df[['batsman_id', 'non_striker_id']] = partnership_df[['batsman_id', 'non_striker_id']].astype(int)
    partnership_df = partnership_df.groupby(
        ['batsman', 'batsman_id', 'non_striker_id', 'non_striker']).agg(
        {'ball_percent': 'mean'}).reset_index()

    return partnership_df


def getMatchWiseBatsmanStats(connection, batsman_name, match_date):
    if match_date:
        matchwise_bat_stats = f'''select bd.match_id, md.match_name, bd.batsman_id, pd.player_name as batsman,
         bd.runs as ball_runs, bd.balls as ball_number from bat_card_data bd inner join matches_df md 
         on (md.match_id=bd.match_id) left join players_data_df pd on (bd.batsman_id=pd.player_id and bd.competition_name=pd.competition_name) 
         where bd.season in (2019,2020,2021,2022) and pd.player_name='{batsman_name}' and md.match_date_form<'{match_date}';'''
    else:
        matchwise_bat_stats = f'''select bd.match_id, md.match_name, bd.batsman_id, pd.player_name as batsman,
         bd.runs as ball_runs, bd.balls as ball_number from bat_card_data bd inner join matches_df md 
         on (md.match_id=bd.match_id) left join players_data_df pd on (bd.batsman_id=pd.player_id and bd.competition_name=pd.competition_name) 
         where pd.player_name='{batsman_name}' and bd.season in (2019,2020,2021,2022);'''

    matchwise_bat_stats = executeQuery(connection, matchwise_bat_stats)
    matchwise_bat_stats = matchwise_bat_stats.groupby(
        ['match_id', 'match_name', 'batsman_id', 'batsman']
    ).agg({'ball_runs': 'sum', 'ball_number': 'sum'}).reset_index()

    return matchwise_bat_stats


def getMatchWiseBowlerStats(connection, match_date):
    if match_date:
        matchwise_bowl_stats = f'''select bd.match_id, md.match_name, bd.bowler_id, pd.player_name as bowler, 
        bd.runs as ball_runs, bd.total_legal_balls as ball_number, bd.wickets as is_wicket, bd.no_balls as is_no_ball, bd.wides as is_wide, 
        bd.economy from bowl_card_data bd inner join matches_df md
         on (md.match_id=bd.match_id) left join players_data_df pd on (bd.bowler_id=pd.player_id and bd.competition_name=pd.competition_name)
         where bd.season in (2019,2020,2021,2022) and md.match_date_form<'{match_date}';'''
    else:
        matchwise_bowl_stats = '''select bd.match_id, md.match_name, bd.bowler_id, pd.player_name as bowler, 
        bd.runs as ball_runs, bd.total_legal_balls as ball_number, bd.wickets as is_wicket, bd.no_balls as is_no_ball, bd.wides as is_wide, 
        bd.economy from bowl_card_data bd inner join matches_df md
         on (md.match_id=bd.match_id) left join players_data_df pd on (bd.bowler_id=pd.player_id and bd.competition_name=pd.competition_name)
         where bd.season in (2019,2020,2021,2022);'''
    matchwise_bowl_stats_df = executeQuery(connection, matchwise_bowl_stats)
    matchwise_bowl_stats_df = matchwise_bowl_stats_df.groupby(
        ['match_id', 'match_name', 'bowler_id', 'bowler']
    ).agg({
        'ball_runs': 'sum', 'ball_number': 'sum', 'is_wicket': 'sum', 'is_no_ball': 'sum',
        'economy': 'mean', 'is_wide': 'sum'
    }).reset_index()

    return matchwise_bowl_stats_df


def getOverallBowlerStats(connection):
    overall_bowl_stats = '''select bd.bowler_id, pd.player_name as bowler, 
    sum(bd.runs) as ball_runs, sum(bd.total_legal_balls) as ball_number, sum(bd.wickets) as total_wickets, 
    sum(bd.no_balls) as is_no_ball, sum(bd.wides) as is_wide, count(distinct match_id) as total_matches,
    round(coalesce(sum(bd.economy)/count(distinct match_id),0),2) as bowling_economy from bowl_card_data bd  
    left join players_data_df pd on (bd.bowler_id=pd.player_id and bd.competition_name=pd.competition_name)
     where bd.season in (2019,2020,2021,2022) group by bd.bowler_id, pd.player_name;'''
    overall_bowl_df = executeQuery(connection, overall_bowl_stats)
    overall_bowl_df = overall_bowl_df.groupby(
        ['bowler_id', 'bowler']
    ).agg(
        {
            'total_matches': 'sum', 'ball_number': 'sum',
            'ball_runs': 'sum', 'total_wickets': 'sum', 'is_wide': 'sum',
            'is_no_ball': 'sum', 'bowling_economy': 'avg'
        }
    ).reset_index()

    return overall_bowl_df


def getOverwiseBowlerStats(connection):
    overwise_bowler_stats = '''select bd.bowler_id as player_id, pd.player_name, bd.over_number, count(distinct bd.match_id) as total_matches, 
    cast((count(bd.ball_number)-(sum(bd.is_wide)+sum(bd.is_no_ball))) as int) as ball_number,
    cast(sum(case when bd.is_leg_bye=1 then 0 else bd.runs end) as int) as ball_runs, cast(sum(case when bd.is_wicket=1 and bd.is_bowler_wicket=1
     then 1 else 0 end) as int) as total_wickets, cast(sum(bd.is_wide) as int) as is_wide, cast(sum(bd.is_no_ball) as int) as 
     is_no_ball, round(coalesce(((sum(runs)*1.00)/(case when ((cast((count(bd.ball_number)-(sum(bd.is_wide)+sum(bd.is_no_ball))) as int))%6)==0 
     then (((cast((count(bd.ball_number)-(sum(bd.is_wide)+sum(bd.is_no_ball))) as int))*1.00)/6) else 
     ((cast((count(bd.ball_number)-(sum(bd.is_wide)+sum(bd.is_no_ball))) as int))/6) + 
     (((cast((count(bd.ball_number)-(sum(bd.is_wide)+sum(bd.is_no_ball))) as int))%6)/10.00) end)),0.0),2) as bowling_economy 
     from ball_summary_df bd left join players_data_df pd on (bd.bowler_id=pd.player_id and bd.competition_name=pd.competition_name)
     where bd.season in (2019,2020,2021,2022) group by bd.bowler_id, bd.over_number, pd.player_name;'''

    overwise_bowler_df = executeQuery(connection, overwise_bowler_stats)
    overwise_bowler_df = overwise_bowler_df \
        .groupby(['player_id', 'player_name', 'over_number']).agg({'total_matches': 'sum', 'ball_number': 'sum',
                                                                   'ball_runs': 'sum', 'total_wickets': 'sum',
                                                                   'is_wide': 'sum',
                                                                   'is_no_ball': 'sum',
                                                                   'bowling_economy': 'avg'}).reset_index()

    return overwise_bowler_df


def getMatchesData(connection, match_date, players_list):
    if match_date:
        ball_data_query_batsman = '''select bdf.batsman_id, pd.player_name as batsman, pd.batting_type as striker_batting_type,
         bdf.bowler_id, pld.player_name as bowler, pld.bowling_type as bowler_sub_type,
         case when bdf.batting_phase=1 then 'POWERPLAY' when bdf.batting_phase=2 then 'MIDDLE FIRST HALF' 
          when bdf.batting_phase=3 then 'MIDDLE SECOND HALF'  else 'DEATH' end
         as match_phase, bdf.ball_runs, bdf.ball_number, bdf.is_wicket, bdf.is_wide, bdf.over_number from
         ball_summary_df bdf left join players_data_df pd
        on (bdf.batsman_id=pd.player_id and bdf.competition_name=pd.competition_name)
        left join players_data_df pld on (bdf.bowler_id=pld.player_id and bdf.competition_name=pd.competition_name) 
        left join matches_df md on (md.match_id=bdf.match_id) 
         where pd.player_name in {} and md.match_date_form < '{}' and is_wide=0 '''.format(str(tuple(players_list)),
                                                                                           str(match_date)) + ''' ;'''

        ball_data_query_bowler = '''select bdf.batsman_id, pd.player_name as batsman, pd.batting_type as striker_batting_type,
                 bdf.bowler_id, pld.player_name as bowler, pld.bowling_type as bowler_sub_type,
                 case when bdf.batting_phase=1 then 'POWERPLAY' when bdf.batting_phase=2 then 'MIDDLE FIRST HALF' 
                  when bdf.batting_phase=3 then 'MIDDLE SECOND HALF'  else 'DEATH' end
                 as match_phase, bdf.ball_runs, bdf.ball_number, bdf.is_wicket, bdf.is_wide, bdf.over_number from
                 ball_summary_df bdf left join players_data_df pd
                on (bdf.batsman_id=pd.player_id and bdf.competition_name=pd.competition_name)
                left join players_data_df pld on (bdf.bowler_id=pld.player_id and bdf.competition_name=pd.competition_name) 
                left join matches_df md on (md.match_id=bdf.match_id) where pld.player_name in {} and 
                md.match_date_form < '{}' and is_wide=0 '''.format(str(tuple(players_list)), str(match_date)) + ''' ;'''
    else:
        ball_data_query_batsman = '''select bdf.batsman_id, pd.player_name as batsman, pd.batting_type as striker_batting_type,
        bdf.bowler_id, pld.player_name as bowler, pld.bowling_type as bowler_sub_type,
        case when bdf.batting_phase=1 then 'POWERPLAY' when bdf.batting_phase=2 then 'MIDDLE FIRST HALF' 
        when bdf.batting_phase=3 then 'MIDDLE SECOND HALF'  else 'DEATH' end 
        as match_phase, bdf.ball_runs, bdf.ball_number, bdf.is_wicket, bdf.is_wide, bdf.over_number from 
        ball_summary_df bdf left join players_data_df pd 
        on (bdf.batsman_id=pd.player_id and bdf.competition_name=pd.competition_name) 
        left join players_data_df pld on (bdf.bowler_id=pld.player_id and bdf.competition_name=pd.competition_name)
         where is_wide=0 and pd.player_name in {};'''.format(str(tuple(players_list)))

        ball_data_query_bowler = '''select bdf.batsman_id, pd.player_name as batsman, pd.batting_type as striker_batting_type,
                bdf.bowler_id, pld.player_name as bowler, pld.bowling_type as bowler_sub_type,
                case when bdf.batting_phase=1 then 'POWERPLAY' when bdf.batting_phase=2 then 'MIDDLE FIRST HALF' 
                when bdf.batting_phase=3 then 'MIDDLE SECOND HALF'  else 'DEATH' end 
                as match_phase, bdf.ball_runs, bdf.ball_number, bdf.is_wicket, bdf.is_wide, bdf.over_number from 
                ball_summary_df bdf left join players_data_df pd 
                on (bdf.batsman_id=pd.player_id and bdf.competition_name=pd.competition_name) 
                left join players_data_df pld on (bdf.bowler_id=pld.player_id and bdf.competition_name=pd.competition_name)
                 where is_wide=0 and pld.player_name in {};'''.format(str(tuple(players_list)))

    ball_stats = executeQuery(connection, ball_data_query_batsman).append(
        executeQuery(connection, ball_data_query_bowler), ignore_index=True).drop_duplicates()
    return ball_stats


def getContributionData(connection, match_date, players_list):
    players_list = [x.upper() for x in players_list]
    if match_date:
        contribution_sql = '''select player, bat_powerplay_contribution_score, bat_7_10_overs_contribution_score,
                               bat_11_15_overs_contribution_score, bat_deathovers_contribution_score, 
                               bowl_powerplay_contribution_score, bowl_7_10_overs_contribution_score, 
                               bowl_11_15_overs_contribution_score, bowl_deathovers_contribution_score, bat_innings, 
                               bowl_innings from contribution_data where match_date < '{}' and player in {}'''.format(
            str(match_date), str(tuple(players_list))) + ''';'''
    else:
        contribution_sql = '''select player, bat_powerplay_contribution_score, bat_7_10_overs_contribution_score,
                               bat_11_15_overs_contribution_score, bat_deathovers_contribution_score, 
                               bowl_powerplay_contribution_score, bowl_7_10_overs_contribution_score, 
                               bowl_11_15_overs_contribution_score, bowl_deathovers_contribution_score, bat_innings, 
                               bowl_innings from contribution_data where player in {};'''.format(
            str(tuple(players_list)))

    contribution_df = executeQuery(connection, contribution_sql) \
        .rename(columns={'player': 'Player', 'bat_powerplay_contribution_score': 'Bat_powerplay_contribution_score',
                         'bat_7_10_overs_contribution_score': 'Bat_7_10_Overs_contribution_score',
                         'bat_11_15_overs_contribution_score': 'Bat_11_15_Overs_contribution_score',
                         'bat_deathovers_contribution_score': 'Bat_deathovers_contribution_score',
                         'bowl_powerplay_contribution_score': 'Bowl_powerplay_contribution_score',
                         'bowl_7_10_overs_contribution_score': 'Bowl_7_10_Overs_contribution_score',
                         'bowl_11_15_overs_contribution_score': 'Bowl_11_15_Overs_contribution_score',
                         'bowl_deathovers_contribution_score': 'Bowl_deathovers_contribution_score',
                         'bat_innings': 'Bat_innings', 'bowl_innings': 'Bowl_innings'})
    return contribution_df


def get_match_phase(over):
    if (over <= 6 and over >= 1):
        return "POWERPLAY"
    elif (over <= 10):
        return "MIDDLE FIRST HALF"
    elif (over <= 15):
        return "MIDDLE SECOND HALF"
    else:
        return "DEATH"


def get_batsman_classes(mapping, contribution):
    # Fixed constraints
    min_phase_inns, t1, t2 = 5, 0.7, 0.3

    # ball_data1['phase'] = ball_data1['over_number'].apply(get_match_phase)
    # ball_data = ball_data1[ball_data1['is_wide'] == 0].copy()

    contribution['Player'] = contribution['Player'].str.title()
    contribution[['Bat_innings', 'Bowl_innings']] = contribution[['Bat_innings', 'Bowl_innings']].fillna(value=0.0)
    contribution['Bat_innings'] = contribution['Bat_innings'].apply(lambda x: 0 if (x == 0.0) else 1)
    contribution['Bowl_innings'] = contribution['Bowl_innings'].apply(lambda x: 0 if (x == 0.0) else 1)

    # Calculating batsman stats
    batsman_details = contribution[(contribution['Player'].isin(mapping['Player'].values))].groupby('Player').agg(
        {'Bat_powerplay_contribution_score': ['mean', 'count'],
         'Bat_7_10_Overs_contribution_score': ['mean', 'count'],
         'Bat_11_15_Overs_contribution_score': ['mean', 'count'],
         'Bat_deathovers_contribution_score': ['mean', 'count'],
         'Bat_innings': 'sum'}).reset_index()

    batsman_details.columns = ['Player', 'Bat_powerplay_contribution_score', 'pp_inns',
                               'Bat_7_10_Overs_contribution_score', '7_10_overs_inns',
                               'Bat_11_15_Overs_contribution_score', '11_15_overs_inns',
                               'Bat_deathovers_contribution_score', 'death_inns', 'Bat_innings']

    # Deriving thresholds for determining batsman classes
    pp_80_pct = batsman_details[batsman_details['pp_inns'] >= min_phase_inns][
        'Bat_powerplay_contribution_score'].quantile(t1)
    pp_50_pct = batsman_details[batsman_details['pp_inns'] >= min_phase_inns][
        'Bat_powerplay_contribution_score'].quantile(t2)

    middle_first_80_pct = batsman_details[batsman_details['7_10_overs_inns'] >= min_phase_inns][
        'Bat_7_10_Overs_contribution_score'].quantile(t1)
    middle_first_50_pct = batsman_details[batsman_details['7_10_overs_inns'] >= min_phase_inns][
        'Bat_7_10_Overs_contribution_score'].quantile(t2)

    middle_second_80_pct = batsman_details[batsman_details['11_15_overs_inns'] >= min_phase_inns][
        'Bat_11_15_Overs_contribution_score'].quantile(t1)
    middle_second_50_pct = batsman_details[batsman_details['11_15_overs_inns'] >= min_phase_inns][
        'Bat_11_15_Overs_contribution_score'].quantile(t2)

    death_80_pct = batsman_details[batsman_details['death_inns'] >= min_phase_inns][
        'Bat_deathovers_contribution_score'].quantile(t1)
    death_50_pct = batsman_details[batsman_details['death_inns'] >= min_phase_inns][
        'Bat_deathovers_contribution_score'].quantile(t2)

    # Function to assign batsman classes
    def my_fn(x):
        res = []

        if (x['pp_inns'] >= min_phase_inns):
            if (x['Bat_powerplay_contribution_score'] >= pp_80_pct):
                res.append('CLASS A')
            elif (x['Bat_powerplay_contribution_score'] >= pp_50_pct):
                res.append('CLASS B')
            else:
                res.append('CLASS C')
        else:
            res.append('CLASS C')

        if (x['7_10_overs_inns'] >= min_phase_inns):
            if (x['Bat_7_10_Overs_contribution_score'] >= middle_first_80_pct):
                res.append('CLASS A')
            elif (x['Bat_7_10_Overs_contribution_score'] >= middle_first_50_pct):
                res.append('CLASS B')
            else:
                res.append('CLASS C')
        else:
            res.append('CLASS C')

        if (x['11_15_overs_inns'] >= min_phase_inns):
            if (x['Bat_11_15_Overs_contribution_score'] >= middle_second_80_pct):
                res.append('CLASS A')
            elif (x['Bat_11_15_Overs_contribution_score'] >= middle_second_50_pct):
                res.append('CLASS B')
            else:
                res.append('CLASS C')
        else:
            res.append('CLASS C')

        if (x['death_inns'] >= min_phase_inns):
            if (x['Bat_deathovers_contribution_score'] >= death_80_pct):
                res.append('CLASS A')
            elif (x['Bat_deathovers_contribution_score'] >= death_50_pct):
                res.append('CLASS B')
            else:
                res.append('CLASS C')
        else:
            res.append('CLASS C')
        return res

    batsman_details[['bat_Powerplay_class', 'bat_Middle_first_class', 'bat_Middle_second_class',
                     'bat_Death_class']] = batsman_details.apply(lambda x: my_fn(x), axis='columns',
                                                                 result_type='expand')

    final_bat = batsman_details.merge(mapping[['Player', 'Batting_type', 'Team']], on='Player', how='right')
    final_bat.rename(columns={'Team': 'bat_Team'}, inplace=True)
    return final_bat


def get_bowler_classes(mapping, contribution):
    # Fixed constraints
    min_phase_inns, t1, t2 = 5, 0.7, 0.3

    # Calculating bowler stats
    contribution['Player'] = contribution['Player'].str.title()
    bowler_details = contribution[(contribution['Player'].isin(mapping['Player'].values))].groupby('Player').agg(
        {'Bowl_powerplay_contribution_score': ['mean', 'count'],
         'Bowl_7_10_Overs_contribution_score': ['mean', 'count'],
         'Bowl_11_15_Overs_contribution_score': ['mean', 'count'],
         'Bowl_deathovers_contribution_score': ['mean', 'count'],
         'Bowl_innings': 'sum'}).reset_index()

    bowler_details.columns = ['Player', 'Bowl_powerplay_contribution_score', 'pp_inns',
                              'Bowl_7_10_Overs_contribution_score', '7_10_overs_inns',
                              'Bowl_11_15_Overs_contribution_score', '11_15_overs_inns',
                              'Bowl_deathovers_contribution_score', 'death_inns', 'Bowl_innings']

    # Deriving thresholds for determining bowler classes
    bowl_pp_80_pct = bowler_details[bowler_details['pp_inns'] >= min_phase_inns][
        'Bowl_powerplay_contribution_score'].quantile(t1)
    bowl_pp_50_pct = bowler_details[bowler_details['pp_inns'] >= min_phase_inns][
        'Bowl_powerplay_contribution_score'].quantile(t2)

    bowl_middle_first_80_pct = bowler_details[bowler_details['7_10_overs_inns'] >= min_phase_inns][
        'Bowl_7_10_Overs_contribution_score'].quantile(t1)
    bowl_middle_first_50_pct = bowler_details[bowler_details['7_10_overs_inns'] >= min_phase_inns][
        'Bowl_7_10_Overs_contribution_score'].quantile(t2)

    bowl_middle_second_80_pct = bowler_details[bowler_details['11_15_overs_inns'] >= min_phase_inns][
        'Bowl_11_15_Overs_contribution_score'].quantile(t1)
    bowl_middle_second_50_pct = bowler_details[bowler_details['11_15_overs_inns'] >= min_phase_inns][
        'Bowl_11_15_Overs_contribution_score'].quantile(t2)

    bowl_death_80_pct = bowler_details[bowler_details['death_inns'] >= min_phase_inns][
        'Bowl_deathovers_contribution_score'].quantile(t1)
    bowl_death_50_pct = bowler_details[bowler_details['death_inns'] >= min_phase_inns][
        'Bowl_deathovers_contribution_score'].quantile(t2)

    # Function to add bowler class
    def my_fn_bowl(x):
        res = []
        if (x['pp_inns'] >= min_phase_inns):
            if (x['Bowl_powerplay_contribution_score'] >= bowl_pp_80_pct):
                res.append('CLASS A')
            elif (x['Bowl_powerplay_contribution_score'] >= bowl_pp_50_pct):
                res.append('CLASS B')
            else:
                res.append('CLASS C')
        else:
            res.append('CLASS C')

        if (x['7_10_overs_inns'] >= min_phase_inns):
            if (x['Bowl_7_10_Overs_contribution_score'] >= bowl_middle_first_80_pct):
                res.append('CLASS A')
            elif (x['Bowl_7_10_Overs_contribution_score'] >= bowl_middle_first_50_pct):
                res.append('CLASS B')
            else:
                res.append('CLASS C')
        else:
            res.append('CLASS C')

        if (x['11_15_overs_inns'] >= min_phase_inns):
            if (x['Bowl_11_15_Overs_contribution_score'] >= bowl_middle_second_80_pct):
                res.append('CLASS A')
            elif (x['Bowl_11_15_Overs_contribution_score'] >= bowl_middle_second_50_pct):
                res.append('CLASS B')
            else:
                res.append('CLASS C')
        else:
            res.append('CLASS C')

        if (x['death_inns'] >= min_phase_inns):
            if (x['Bowl_deathovers_contribution_score'] >= bowl_death_80_pct):
                res.append('CLASS A')
            elif (x['Bowl_deathovers_contribution_score'] >= bowl_death_50_pct):
                res.append('CLASS B')
            else:
                res.append('CLASS C')
        else:
            res.append('CLASS C')

        return res

    bowler_details[['bowl_Powerplay_class', 'bowl_Middle_first_class', 'bowl_Middle_second_class',
                    'bowl_Death_class']] = bowler_details.apply(lambda x: my_fn_bowl(x), axis='columns',
                                                                result_type='expand')
    final_bowl = bowler_details.merge(mapping[['Player', 'Bowling_type', 'Team']], on='Player', how='right')

    # fill class c for players which are not present in contribution but present in mapping file
    # df['DataFrame Column'] = df['DataFrame Column'].fillna(0)
    final_bowl['bowl_Powerplay_class'] = final_bowl['bowl_Powerplay_class'].fillna('CLASS C')
    final_bowl['bowl_Middle_first_class'] = final_bowl['bowl_Middle_first_class'].fillna('CLASS C')
    final_bowl['bowl_Middle_second_class'] = final_bowl['bowl_Middle_second_class'].fillna('CLASS C')
    final_bowl['bowl_Death_class'] = final_bowl['bowl_Death_class'].fillna('CLASS C')
    # np.where(condition, value if true, value if false)
    final_bowl.rename(columns={'Team': 'bowl_Team'}, inplace=True)
    return final_bowl


def get_matchup_details(ball_data, mapping, batsman_classes, bowler_classes):
    ball_data['phase'] = ball_data['over_number'].apply(get_match_phase)
    ball_data = ball_data[~ball_data['bowler'].isin(['Rohit Sharma', 'Jofra Archer'])].copy()
    df = ball_data[ball_data['is_wide'] == 0].copy()
    min_ball_criteria = 30

    # batsman stats
    batsman_details = df[df['batsman'].isin(mapping['Player'].values)].groupby(
        ['batsman', 'bowler_sub_type', 'striker_batting_type', 'phase']).agg({'ball_runs': 'sum',
                                                                              'ball_number': 'count',
                                                                              'is_wicket': 'sum'
                                                                              }).reset_index()
    batsman_details['Batting_strike_rate'] = round(batsman_details['ball_runs'] / batsman_details['ball_number'] * 100,
                                                   2)
    batsman_details['Batting_average'] = round(batsman_details['ball_runs'] / batsman_details['is_wicket'], 2)
    # Adding classes to batsman stats
    batsman_details = batsman_details.merge(
        batsman_classes[['Player', 'bat_Powerplay_class', 'bat_Middle_first_class', 'bat_Middle_second_class',
                         'bat_Death_class', 'bat_Team']], left_on='batsman', right_on='Player', how='left').drop(
        columns=['Player'])

    batsman_details.rename(
        columns={'ball_runs': 'runs_sum_bat', 'ball_number': 'balls_sum_bat', 'is_wicket': 'wickets_bat'
                 }, inplace=True)

    # Bowler stats
    bowler_details = ball_data[ball_data['bowler'].isin(mapping['Player'].values)].groupby(
        ['bowler', 'bowler_sub_type', 'striker_batting_type', 'phase']).agg({'ball_runs': 'sum',
                                                                             'ball_number': 'count',
                                                                             'is_wicket': 'sum'}).reset_index()

    bowler_details['Bowling_strike_rate'] = round(bowler_details['ball_number'] / bowler_details['is_wicket'], 2)
    bowler_details['Bowling_average'] = round(bowler_details['ball_runs'] / bowler_details['is_wicket'], 2)
    bowler_details['Bowling_economy'] = round(6 * bowler_details['ball_runs'] / bowler_details['ball_number'], 2)

    # Add bowler class to bowler stats
    bowler_details = bowler_details.merge(bowler_classes[['Player', 'bowl_Powerplay_class', 'bowl_Middle_first_class',
                                                          'bowl_Middle_second_class', 'bowl_Death_class', 'bowl_Team']],
                                          left_on='bowler', right_on='Player', how='left').drop(columns=['Player'])

    bowler_details.rename(
        columns={'ball_runs': 'runs_sum_ball', 'ball_number': 'balls_sum_ball', 'is_wicket': 'wickets_ball'},
        inplace=True)

    # Compute mean data for batsman class B
    mean_details_pp = batsman_details[
        (batsman_details['phase'] == 'POWERPLAY') & (batsman_details['bat_Powerplay_class'] == 'CLASS B')].groupby(
        ['bowler_sub_type', 'striker_batting_type', 'phase']).agg({'runs_sum_bat': 'mean',
                                                                   'balls_sum_bat': 'mean',
                                                                   'wickets_bat': 'mean'}).reset_index()

    mean_details_pp['Batting_strike_rate'] = round(
        mean_details_pp['runs_sum_bat'] / mean_details_pp['balls_sum_bat'] * 100, 2)
    mean_details_pp['Batting_average'] = round(mean_details_pp['runs_sum_bat'] / mean_details_pp['wickets_bat'], 2)

    mean_details_middle_first = batsman_details[(batsman_details['phase'] == 'MIDDLE FIRST HALF') & (
            batsman_details['bat_Middle_first_class'] == 'CLASS B')].groupby(
        ['bowler_sub_type', 'striker_batting_type', 'phase']).agg({'runs_sum_bat': 'mean',
                                                                   'balls_sum_bat': 'mean',
                                                                   'wickets_bat': 'mean'}).reset_index()

    mean_details_middle_first['Batting_strike_rate'] = round(
        mean_details_middle_first['runs_sum_bat'] / mean_details_middle_first['balls_sum_bat'] * 100, 2)
    mean_details_middle_first['Batting_average'] = round(
        mean_details_middle_first['runs_sum_bat'] / mean_details_middle_first['wickets_bat'], 2)

    mean_details_middle_second = batsman_details[(batsman_details['phase'] == 'MIDDLE SECOND HALF') & (
            batsman_details['bat_Middle_second_class'] == 'CLASS B')].groupby(
        ['bowler_sub_type', 'striker_batting_type', 'phase']).agg({'runs_sum_bat': 'mean',
                                                                   'balls_sum_bat': 'mean',
                                                                   'wickets_bat': 'mean'}).reset_index()

    mean_details_middle_second['Batting_strike_rate'] = round(
        mean_details_middle_second['runs_sum_bat'] / mean_details_middle_second['balls_sum_bat'] * 100, 2)
    mean_details_middle_second['Batting_average'] = round(
        mean_details_middle_second['runs_sum_bat'] / mean_details_middle_second['wickets_bat'], 2)

    mean_details_death = batsman_details[
        (batsman_details['phase'] == 'DEATH') & (batsman_details['bat_Death_class'] == 'CLASS B')].groupby(
        ['bowler_sub_type', 'striker_batting_type', 'phase']).agg({'runs_sum_bat': 'mean',
                                                                   'balls_sum_bat': 'mean',
                                                                   'wickets_bat': 'mean'}).reset_index()

    mean_details_death['Batting_strike_rate'] = round(
        mean_details_death['runs_sum_bat'] / mean_details_death['balls_sum_bat'] * 100, 2)
    mean_details_death['Batting_average'] = round(
        mean_details_death['runs_sum_bat'] / mean_details_death['wickets_bat'], 2)

    mean_details = pd.concat(
        [mean_details_pp, mean_details_middle_first, mean_details_middle_second, mean_details_death], axis=0)

    # Compute mean data for bowler class B
    ball_mean_details_pp = bowler_details[
        (bowler_details['phase'] == 'POWERPLAY') & (bowler_details['bowl_Powerplay_class'] == 'CLASS B')].groupby(
        ['bowler_sub_type', 'striker_batting_type', 'phase']).agg({'runs_sum_ball': 'mean',
                                                                   'balls_sum_ball': 'mean',
                                                                   'wickets_ball': 'mean'}).reset_index()

    ball_mean_details_pp['Bowling_strike_rate'] = round(
        ball_mean_details_pp['balls_sum_ball'] / ball_mean_details_pp['wickets_ball'], 2)
    ball_mean_details_pp['Bowling_average'] = round(
        ball_mean_details_pp['runs_sum_ball'] / ball_mean_details_pp['wickets_ball'], 2)
    ball_mean_details_pp['Bowling_economy'] = round(
        6 * ball_mean_details_pp['runs_sum_ball'] / ball_mean_details_pp['balls_sum_ball'], 2)

    ball_mean_details_middle_first = bowler_details[(bowler_details['phase'] == 'MIDDLE FIRST HALF') & (
            bowler_details['bowl_Middle_first_class'] == 'CLASS B')].groupby(
        ['bowler_sub_type', 'striker_batting_type', 'phase']).agg({'runs_sum_ball': 'mean',
                                                                   'balls_sum_ball': 'mean',
                                                                   'wickets_ball': 'mean'}).reset_index()

    ball_mean_details_middle_first['Bowling_strike_rate'] = round(
        ball_mean_details_middle_first['balls_sum_ball'] / ball_mean_details_middle_first['wickets_ball'], 2)
    ball_mean_details_middle_first['Bowling_average'] = round(
        ball_mean_details_middle_first['runs_sum_ball'] / ball_mean_details_middle_first['wickets_ball'], 2)
    ball_mean_details_middle_first['Bowling_economy'] = round(
        6 * ball_mean_details_middle_first['runs_sum_ball'] / ball_mean_details_middle_first['balls_sum_ball'], 2)

    ball_mean_details_middle_second = bowler_details[(bowler_details['phase'] == 'MIDDLE SECOND HALF') & (
            bowler_details['bowl_Middle_second_class'] == 'CLASS B')].groupby(
        ['bowler_sub_type', 'striker_batting_type', 'phase']).agg({'runs_sum_ball': 'mean',
                                                                   'balls_sum_ball': 'mean',
                                                                   'wickets_ball': 'mean'}).reset_index()

    ball_mean_details_middle_second['Bowling_strike_rate'] = round(
        ball_mean_details_middle_second['balls_sum_ball'] / ball_mean_details_middle_second['wickets_ball'], 2)
    ball_mean_details_middle_second['Bowling_average'] = round(
        ball_mean_details_middle_second['runs_sum_ball'] / ball_mean_details_middle_second['wickets_ball'], 2)
    ball_mean_details_middle_second['Bowling_economy'] = round(
        6 * ball_mean_details_middle_second['runs_sum_ball'] / ball_mean_details_middle_second['balls_sum_ball'], 2)

    ball_mean_details_death = bowler_details[
        (bowler_details['phase'] == 'DEATH') & (bowler_details['bowl_Death_class'] == 'CLASS B')].groupby(
        ['bowler_sub_type', 'striker_batting_type', 'phase']).agg({'runs_sum_ball': 'mean',
                                                                   'balls_sum_ball': 'mean',
                                                                   'wickets_ball': 'mean'}).reset_index()

    ball_mean_details_death['Bowling_strike_rate'] = round(
        ball_mean_details_death['balls_sum_ball'] / ball_mean_details_death['wickets_ball'], 2)
    ball_mean_details_death['Bowling_average'] = round(
        ball_mean_details_death['runs_sum_ball'] / ball_mean_details_death['wickets_ball'], 2)
    ball_mean_details_death['Bowling_economy'] = round(
        6 * ball_mean_details_death['runs_sum_ball'] / ball_mean_details_death['balls_sum_ball'], 2)

    ball_mean_details = pd.concat(
        [ball_mean_details_pp, ball_mean_details_middle_first, ball_mean_details_middle_second,
         ball_mean_details_death], axis=0)

    # Update batsmen not meeting criteria with mean details
    batsman_details['min_ball_criteria_met'] = (batsman_details['balls_sum_bat'] >= min_ball_criteria).astype(int)

    batsman_sig = batsman_details.groupby(['batsman']).agg({'min_ball_criteria_met': 'sum',
                                                            'striker_batting_type': 'count'}).reset_index()

    insignificant_batsmen = batsman_sig[batsman_sig['min_ball_criteria_met'] == 0]['batsman'].values

    new_batsmen = set(mapping['Player'].values) - set(batsman_details['batsman'].values)

    batsmen_with_mod = list(new_batsmen) + list(insignificant_batsmen)
    batsmen_with_mod_df = mapping[mapping['Player'].isin(batsmen_with_mod)][['Player', 'Batting_type', 'Team']].copy()

    batsmen_with_mod_df = batsmen_with_mod_df.merge(mean_details, left_on=['Batting_type'],
                                                    right_on=['striker_batting_type'], how='left')
    batsmen_with_mod_df = batsmen_with_mod_df.drop(columns=['Batting_type', ]).rename(
        columns={'Team': 'bat_Team', 'Player': 'batsman'})

    batsmen_with_mod_df['bat_Powerplay_class'] = "CLASS B"
    batsmen_with_mod_df['bat_Middle_first_class'] = "CLASS B"
    batsmen_with_mod_df['bat_Middle_second_class'] = "CLASS B"
    batsmen_with_mod_df['bat_Death_class'] = "CLASS B"

    batsman_details.drop(columns=['min_ball_criteria_met'], inplace=True)
    batsman_details = batsman_details[~batsman_details['batsman'].isin(batsmen_with_mod)].copy()

    batsman_details = pd.concat([batsman_details, batsmen_with_mod_df], axis=0)

    # Update bowlers not meeting criteria with mean details
    bowler_details['min_ball_criteria_met'] = (bowler_details['balls_sum_ball'] >= min_ball_criteria).astype(int)

    bowler_sig = bowler_details.groupby(['bowler']).agg({'min_ball_criteria_met': 'sum',
                                                         'bowler_sub_type': 'count'}).reset_index()

    insignificant_bowlers = bowler_sig[bowler_sig['min_ball_criteria_met'] == 0]['bowler'].values

    new_bowlers = set(mapping['Player'].values) - set(bowler_details['bowler'].values)

    bowlers_with_mod = list(new_bowlers) + list(insignificant_bowlers)
    bowlers_with_mod_df = mapping[mapping['Player'].isin(bowlers_with_mod)][['Player', 'Bowling_type', 'Team']].copy()

    bowlers_with_mod_df = bowlers_with_mod_df.merge(ball_mean_details, left_on=['Bowling_type'],
                                                    right_on=['bowler_sub_type'], how='left')
    bowlers_with_mod_df = bowlers_with_mod_df.drop(columns=['Bowling_type', ]).rename(
        columns={'Team': 'bowl_Team', 'Player': 'bowler'})

    bowlers_with_mod_df['bowl_Powerplay_class'] = "CLASS B"
    bowlers_with_mod_df['bowl_Middle_first_class'] = "CLASS B"
    bowlers_with_mod_df['bowl_Middle_second_class'] = "CLASS B"
    bowlers_with_mod_df['bowl_Death_class'] = "CLASS B"

    bowler_details.drop(columns=['min_ball_criteria_met'], inplace=True)
    bowler_details = bowler_details[~bowler_details['bowler'].isin(bowlers_with_mod)].copy()

    bowler_details = pd.concat([bowler_details, bowlers_with_mod_df], axis=0)

    final_df = batsman_details.merge(bowler_details, on=['bowler_sub_type', 'striker_batting_type', 'phase'],
                                     how='left')

    return final_df


def get_considered_matches(player, performance, flag, match_date):
    df_match_bat = getMatchWiseBatsmanStats(con, player, match_date)
    df_match_bat = df_match_bat[df_match_bat['ball_number'] > 0]
    # df_match_bowl = getMatchWiseBowlerStats(con, match_date)

    if (flag == 'bat'):
        if (performance == 'best'):
            player_matches = \
                df_match_bat[df_match_bat['batsman'] == player].sort_values(by=['ball_runs', 'ball_number'],
                                                                            ascending=[False, True]).head()[
                    'match_name'].values
        elif (performance == 'worst'):
            player_matches = \
                df_match_bat[df_match_bat['batsman'] == player].sort_values(by=['ball_runs', 'ball_number'],
                                                                            ascending=[True, False]).head()[
                    'match_name'].values
        elif (performance == 'avg'):
            player_matches = df_match_bat[df_match_bat['batsman'] == player]['match_name'].values
    elif (flag == 'bowl'):
        if (performance == 'best'):
            player_matches = df_match_bowl[df_match_bowl['bowler'] == player].sort_values(by=['is_wicket', 'economy'],
                                                                                          ascending=[False,
                                                                                                     True]).head()[
                'match_name'].values
        elif (performance == 'worst'):
            player_matches = df_match_bowl[df_match_bowl['bowler'] == player].sort_values(by=['is_wicket', 'economy'],
                                                                                          ascending=[True,
                                                                                                     False]).head()[
                'match_name'].values
        elif (performance == 'avg'):
            player_matches = df_match_bowl[df_match_bowl['bowler'] == player]['match_name'].values

    return player_matches


def get_stay_at_crease(bat, match_date, performance='avg'):
    df_stay = getMatchWiseBatsmanStats(con, bat, match_date)[['match_name', 'batsman', 'ball_number']]
    df_stay = df_stay[df_stay['ball_number'] > 0]
    player_matches = get_considered_matches(bat, performance, "bat", match_date)
    val = df_stay[(df_stay['match_name'].isin(player_matches)) & (df_stay['batsman'] == bat)]
    if (val.empty or len(player_matches) < 5):
        return 6.0
    else:
        return round(val['ball_number'].mean())


def get_partnership_data(striker, non_striker, match_date):
    df_partner = getPartnershipStay(con, striker, non_striker, match_date)

    val = df_partner[(df_partner['batsman'] == striker) & (df_partner['non_striker'] == non_striker)]
    if (val.empty or np.isnan(val['ball_percent'].values[0])):
        return 3.0
    else:
        return val['ball_percent'].values[0]


def stay_at_crease_over(rem_stay):
    if rem_stay == 0.0:
        return True
    else:
        return False


def get_batting_order(start_over, bat_lineup, match_date, perf_list=['avg'] * 11):
    wicket_list = []
    batting_lineup = bat_lineup.copy()
    perf_li = perf_list.copy()

    bat_1 = batting_lineup.pop(0)
    bat_1_perf = perf_li.pop(0)
    bat_1_stay = get_stay_at_crease(bat_1, match_date, bat_1_perf)
    bat_2 = batting_lineup.pop(0)
    bat_2_perf = perf_li.pop(0)
    bat_2_stay = get_stay_at_crease(bat_2, match_date, bat_2_perf)
    batting_order = []
    for over in range(start_over, 20):
        phase = get_match_phase(over)
        bat_1_balls = get_partnership_data(bat_1, bat_2, match_date)
        bat_2_balls = (6 - bat_1_balls)
        batsmen_at_crease = []
        batsmen_at_crease.append(bat_1)
        batsmen_at_crease.append(bat_2)
        batting_order.append(batsmen_at_crease)
        if ((bat_1_balls <= bat_1_stay) and (bat_2_balls <= bat_2_stay)):
            bat_1_stay = bat_1_stay - bat_1_balls
            bat_2_stay = bat_2_stay - bat_2_balls
            if (stay_at_crease_over(bat_1_stay)):
                wicket_list.append(over)
                try:
                    bat_1 = batting_lineup.pop(0)
                except:
                    break
                bat_1_perf = perf_li.pop(0)
                bat_1_stay = get_stay_at_crease(bat_1, match_date, bat_1_perf)

            if (stay_at_crease_over(bat_2_stay)):
                wicket_list.append(over)
                try:
                    bat_2 = batting_lineup.pop(0)
                except:
                    break
                bat_2_perf = perf_li.pop(0)
                bat_2_stay = get_stay_at_crease(bat_2, match_date, bat_2_perf)


        elif ((bat_1_balls <= bat_1_stay) and (bat_2_balls > bat_2_stay)):
            bat_1_stay = bat_1_stay - bat_1_balls
            if (stay_at_crease_over(bat_1_stay)):
                wicket_list.append(over)
                try:
                    bat_1 = batting_lineup.pop(0)
                except:
                    break
                bat_1_perf = perf_li.pop(0)
                bat_1_stay = get_stay_at_crease(bat_1, match_date, bat_1_perf)

            tmp = bat_2_stay
            wicket_list.append(over)
            try:
                bat_2 = batting_lineup.pop(0)
            except:
                break
            bat_2_perf = perf_li.pop(0)
            bat_2_stay = get_stay_at_crease(bat_2, match_date, bat_2_perf)

            bat_2_stay = bat_2_stay - (bat_2_balls - tmp)


        elif ((bat_1_balls > bat_1_stay) and (bat_2_balls <= bat_2_stay)):
            bat_2_stay = bat_2_stay - bat_2_balls
            tmp = bat_1_stay
            if (stay_at_crease_over(bat_2_stay)):
                wicket_list.append(over)
                try:
                    bat_2 = batting_lineup.pop(0)
                except:
                    break
                bat_2_perf = perf_li.pop(0)
                bat_2_stay = get_stay_at_crease(bat_2, match_date, bat_2_perf)

            wicket_list.append(over)
            try:
                bat_1 = batting_lineup.pop(0)
            except:
                break
            bat_1_perf = perf_li.pop(0)
            bat_1_stay = get_stay_at_crease(bat_1, match_date, bat_1_perf)

            bat_1_stay = bat_1_stay - (bat_1_balls - tmp)

        elif ((bat_1_balls > bat_1_stay) and (bat_2_balls > bat_2_stay)):
            wicket_list.append(over)
            tmp1 = bat_1_stay
            tmp2 = bat_2_stay
            try:
                bat_1 = batting_lineup.pop(0)
            except:
                break
            bat_1_perf = perf_li.pop(0)
            bat_1_stay = get_stay_at_crease(bat_1, match_date, bat_1_perf)

            bat_1_stay = bat_1_stay - (bat_1_balls - tmp1)

            wicket_list.append(over)
            try:
                bat_2 = batting_lineup.pop(0)
            except:
                break
            bat_2_perf = perf_li.pop(0)
            bat_2_stay = get_stay_at_crease(bat_2, match_date, bat_2_perf)

            bat_2_stay = bat_2_stay - (bat_2_balls - tmp2)

    return batting_order


def get_pivoted_matchup(mapping, eco_df, wkt_df, avg_df, include_batsman, perf_list, match_date, start_over,
                        replaced_over_no):
    mapping = mapping.rename(columns={'batsman': 'Player'})

    eco_df.fillna(method='ffill', inplace=True)
    wkt_df.fillna(method='ffill', inplace=True)
    avg_df.fillna(method='ffill', inplace=True)

    bat_lineup = include_batsman

    batting_order = get_batting_order(start_over, bat_lineup, match_date, perf_list)
    bat1 = []
    bat2 = []
    # condition for impact player
    if replaced_over_no == 0:
        for i in range(len(batting_order)):
            bat1.append(batting_order[i][0])
            bat2.append(batting_order[i][1])

        wicket_taking1 = []
        economy1 = []
        for i in range(len(batting_order)):
            economy1.append(eco_df[(eco_df['batsman'].isin([batting_order[i][0]])) & (
                    eco_df['phase'] == get_match_phase(i))].sort_values(by='Bowling_economy', ascending=True)[
                                'bowler'].unique())
            wicket_taking1.append(wkt_df[(wkt_df['batsman'].isin([batting_order[i][0]])) & (
                    wkt_df['phase'] == get_match_phase(i))].sort_values(by='Bowling_strike_rate', ascending=True)[
                                      'bowler'].unique())

        wicket_taking2 = []
        economy2 = []
        for i in range(len(batting_order)):
            economy2.append(eco_df[(eco_df['batsman'].isin([batting_order[i][1]])) & (
                    eco_df['phase'] == get_match_phase(i))].sort_values(by='Bowling_economy', ascending=True)[
                                'bowler'].unique())
            wicket_taking2.append(wkt_df[(wkt_df['batsman'].isin([batting_order[i][1]])) & (
                    wkt_df['phase'] == get_match_phase(i))].sort_values(by='Bowling_strike_rate', ascending=True)[
                                      'bowler'].unique())
    else:
        if replaced_over_no > len(batting_order):
            replaced_over_no = len(batting_order)
        for i in range(replaced_over_no):
            bat1.append(batting_order[i][0])
            bat2.append(batting_order[i][1])

        wicket_taking1 = []
        economy1 = []
        for i in range(replaced_over_no):
            economy1.append(eco_df[(eco_df['batsman'].isin([batting_order[i][0]])) & (
                    eco_df['phase'] == get_match_phase(i))].sort_values(by='Bowling_economy', ascending=True)[
                                'bowler'].unique())
            wicket_taking1.append(wkt_df[(wkt_df['batsman'].isin([batting_order[i][0]])) & (
                    wkt_df['phase'] == get_match_phase(i))].sort_values(by='Bowling_strike_rate', ascending=True)[
                                      'bowler'].unique())

        wicket_taking2 = []
        economy2 = []
        for i in range(replaced_over_no):
            economy2.append(eco_df[(eco_df['batsman'].isin([batting_order[i][1]])) & (
                    eco_df['phase'] == get_match_phase(i))].sort_values(by='Bowling_economy', ascending=True)[
                                'bowler'].unique())
            wicket_taking2.append(wkt_df[(wkt_df['batsman'].isin([batting_order[i][1]])) & (
                    wkt_df['phase'] == get_match_phase(i))].sort_values(by='Bowling_strike_rate', ascending=True)[
                                      'bowler'].unique())

    for i in range(len(wicket_taking1)):
        if (len(wicket_taking1[i]) == 0):
            batting_type = mapping[mapping['Player'] == bat1[i]]['Batting_type'].values[0]
            phase = get_match_phase(i + 1)

            wicket_taking1[i] = list(
                avg_df[(avg_df['striker_batting_type'] == batting_type) & (avg_df['phase'] == phase)][
                    ['bowler_wkt_1', 'bowler_wkt_2', 'bowler_wkt_3']].values[0])
        if (len(wicket_taking2[i]) == 0):
            batting_type = mapping[mapping['Player'] == bat2[i]]['Batting_type'].values[0]
            phase = get_match_phase(i + 1)
            wicket_taking2[i] = list(
                avg_df[(avg_df['striker_batting_type'] == batting_type) & (avg_df['phase'] == phase)][
                    ['bowler_wkt_1', 'bowler_wkt_2', 'bowler_wkt_3']].values[0])
        if (len(economy1[i]) == 0):
            batting_type = mapping[mapping['Player'] == bat1[i]]['Batting_type'].values[0]
            phase = get_match_phase(i + 1)
            economy1[i] = list(avg_df[(avg_df['striker_batting_type'] == batting_type) & (avg_df['phase'] == phase)][
                                   ['bowler_eco_1', 'bowler_eco_2', 'bowler_eco_3']].values[0])
        if (len(economy2[i]) == 0):
            batting_type = mapping[mapping['Player'] == bat2[i]]['Batting_type'].values[0]
            phase = get_match_phase(i + 1)
            economy2[i] = list(avg_df[(avg_df['striker_batting_type'] == batting_type) & (avg_df['phase'] == phase)][
                                   ['bowler_eco_1', 'bowler_eco_2', 'bowler_eco_3']].values[0])

    result = []
    n = 3
    if replaced_over_no == 0:
        for i in range(start_over, len(wicket_taking1) + start_over):
            bat = []
            bat.append(i + 1)
            bat.append(get_match_phase(i))
            bat.append(bat1[i - start_over])
            bat.append(bat2[i - start_over])
            result.append(bat + list(economy1[i - start_over]) + [' '] * (n - len(economy1[i - start_over])) + list(
                wicket_taking1[i - start_over]) + [' '] * (n - len(wicket_taking1[i - start_over])) +
                          list(economy2[i - start_over]) + [' '] * (n - len(economy2[i - start_over])) + list(
                wicket_taking2[i - start_over]) + [' '] * (n - len(wicket_taking2[i - start_over])))
    else:
        for i in range(start_over, replaced_over_no):
            bat = []
            bat.append(i + 1)
            bat.append(get_match_phase(i))
            bat.append(bat1[i - start_over])
            bat.append(bat2[i - start_over])
            result.append(bat + list(economy1[i - start_over]) + [' '] * (n - len(economy1[i - start_over])) + list(
                wicket_taking1[i - start_over]) + [' '] * (n - len(wicket_taking1[i - start_over])) +
                          list(economy2[i - start_over]) + [' '] * (n - len(economy2[i - start_over])) + list(
                wicket_taking2[i - start_over]) + [' '] * (n - len(wicket_taking2[i - start_over])))

    result_df = pd.DataFrame(result)
    result_df.columns = ['Over No', 'Match Phase', 'Batsman1', 'Batsman2', 'Bat1_Economy1', 'Bat1_Economy2',
                         'Bat1_Economy3', 'Bat1_Wicket_taking1', 'Bat1_Wicket_taking2', 'Bat1_Wicket_taking3',
                         'Bat2_Economy1', 'Bat2_Economy2', 'Bat2_Economy3', 'Bat2_Wicket_taking1',
                         'Bat2_Wicket_taking2', 'Bat2_Wicket_taking3']

    return result_df, batting_order


def get_matchup_calculation(include_batsman: list, exclude_bowlers: list, include_bowlers: list = [],
                            best_list: list = [], worst_list: list = [], match_date=None, home_team="MI", start_over=0,
                            replaced_over_no=0):
    logger.info(f"home team ----> {home_team}")
    logger.info(f"start over ---> {start_over}")

    include_batsman = [batsman.title() for batsman in include_batsman]
    exclude_bowlers = [bowler.title() for bowler in exclude_bowlers]
    include_bowlers = [bowler.title() for bowler in include_bowlers]

    MAPPING_FILE_PATH = os.path.join(FILE_SHARE_PATH, "data/mapping_new.csv")
    mapping = readCSV(MAPPING_FILE_PATH)
    mapping['Player'] = mapping['Player'].str.title()
    mapping['season'] = mapping['season'].apply(lambda x: f'{x:.0f}').astype(str)

    # QA and higher env code - handling mapping file according to season

    #########################
    # Dev code T20 simulation is available only in Dev currently.
    def get_competition(value):
        team_name_dict = {'BBL': ['HEAT', 'STARS', 'REN', 'SIX', 'THUN', 'HUR', 'STR', 'SCOR'],
                          'SA20': ['DSG', 'JSK', 'MICT', 'PR', 'PRC', 'SES'] \
            , 'CPL': ['SKN', 'BT', 'GAW', 'TKR', 'JT', 'STLS'],
                          'IPL': ['CSK', 'MI', 'DC', 'RCB', 'KKR', 'SRH', 'KXIP', 'RR', 'LKSG', 'GT', 'PWI', 'PSG',
                                  'KTK', 'DCH', 'GL']}
        for key, val in team_name_dict.items():
            # if isinstance(val, list) and value in val:
            if value in val:
                return key
        return 'T20'

    competition_name = get_competition(home_team)

    if match_date == None:
        if competition_name == 'T20':
            mapping = mapping[(mapping['competition_name'] == 'T20')]
            # mapping = mapping[mapping['season'] == str(pd.to_numeric(mapping['season']).max())]
        else:
            mapping = mapping[(mapping['competition_name'] == competition_name)]
            mapping = mapping[mapping['season'] == str(pd.to_numeric(mapping['season']).max())]
    else:
        if competition_name == 'T20':
            mapping = mapping[mapping['competition_name'] == 'T20']
            # mapping = mapping[mapping['season'] == str(pd.to_datetime(match_date).year)]
        else:
            mapping = mapping[mapping['competition_name'] == competition_name]
            mapping = mapping[mapping['season'] == str(pd.to_datetime(match_date).year)]

    mapping.drop(columns=['season', 'competition_name'], inplace=True)

    players_list = list(mapping['Player'].unique())

    ball_data = getMatchesData(con, match_date, players_list)
    contribution = getContributionData(con, match_date, players_list)

    batsman_classes = get_batsman_classes(mapping, contribution)
    bowler_classes = get_bowler_classes(mapping, contribution)

    # calculating bowler class of playing 11 for validation report
    playing_eleven_bowler_class = bowler_classes[bowler_classes['Player'].isin(include_bowlers)].copy()
    playing_eleven_bowler_class.drop(
        columns=['Bowl_powerplay_contribution_score', 'pp_inns', 'Bowl_7_10_Overs_contribution_score',
                 '7_10_overs_inns', 'Bowl_11_15_Overs_contribution_score', '11_15_overs_inns',
                 'Bowl_deathovers_contribution_score', 'death_inns', 'Bowl_innings', 'Bowling_type', 'bowl_Team'],
        inplace=True)
    playing_eleven_bowler_class.drop_duplicates(subset=['Player'], inplace=True)
    playing_eleven_bowler_class = playing_eleven_bowler_class.reset_index(drop=True)
    playing_eleven_bowler_class.rename(columns={'Player': 'bowler', 'bowl_Death_class': 'death_overs_class',
                                                'bowl_Middle_first_class': 'middle_first_half_class',
                                                'bowl_Middle_second_class': 'middle_second_half_class',
                                                'bowl_Powerplay_class': 'power_play_class'}, inplace=True)

    ############

    matchup = get_matchup_details(ball_data, mapping, batsman_classes, bowler_classes)
    matchup = matchup[matchup['batsman'].isin(include_batsman)]
    matchup = matchup.replace({'POWERPLAY': '1 POWERPLAY', 'MIDDLE FIRST HALF': '2 MIDDLE FIRST HALF',
                               'MIDDLE SECOND HALF': '3 MIDDLE SECOND HALF'})
    home_team_bowlers = list(bowler_classes[(bowler_classes['bowl_Team'] == home_team)]['Player'])
    ball_data = ball_data[(ball_data['batsman'].isin(include_batsman)) & (ball_data['bowler'].isin(home_team_bowlers))]

    def bowler_class(row):
        if row['phase'] == '1 POWERPLAY':
            return row['bowl_Powerplay_class']
        if row['phase'] == '2 MIDDLE FIRST HALF':
            return row['bowl_Middle_first_class']
        if row['phase'] == '3 MIDDLE SECOND HALF':
            return row['bowl_Middle_second_class']
        if row['phase'] == 'DEATH':
            return row['bowl_Death_class']
        return np.nan

    # Calculate Bowler class for respective Phase
    matchup['Bowler Class'] = matchup.apply(lambda row: bowler_class(row), axis=1)

    # Join matchup with mapping to get preferred order
    mapping = mapping.rename(columns={'Player': 'batsman'})
    data = matchup.merge(mapping[['batsman', 'Preferred Position', 'Batting_type']], left_on='batsman',
                         right_on='batsman', how='left')
    data['striker_batting_type'] = np.where(data['striker_batting_type'] == "NA", data['Batting_type'],
                                            data['striker_batting_type'])

    # Calculate H2H Economy & Wicket_pct
    bbb = ball_data.replace({'POWERPLAY': '1 POWERPLAY', 'MIDDLE FIRST HALF': '2 MIDDLE FIRST HALF',
                             'MIDDLE SECOND HALF': '3 MIDDLE SECOND HALF'})
    bbb_grouped = bbb[bbb['is_wide'] == 0].groupby(['phase', 'batsman', 'bowler']).agg(
        {'ball_number': 'count', 'ball_runs': 'sum', 'is_wicket': 'sum'}).reset_index()
    bbb_grouped = bbb_grouped[(bbb_grouped['ball_number'] >= 10) | (
            (bbb_grouped['ball_number'] < 10) & (bbb_grouped['is_wicket'] > 1))].sort_values(by=['phase'],
                                                                                             ascending=False)
    bbb_grouped['H2H_Economy'] = round(((bbb_grouped['ball_runs'] / bbb_grouped['ball_number']) * 6), 2)
    bbb_grouped['H2H_Wicket_pct'] = round(((bbb_grouped['is_wicket'] / bbb_grouped['ball_number']) * 100), 2)
    bbb_grouped.reset_index(drop=True, inplace=True)

    # Columns to Drop
    drop_cols = ['Batting_average', 'bowl_Powerplay_class', 'bowl_Middle_first_class', 'bowl_Middle_second_class',
                 'bowl_Death_class', 'bat_Powerplay_class',
                 'bat_Middle_first_class', 'bat_Middle_second_class', 'bat_Team', 'bat_Death_class']

    # Calculate Economy Matchup
    logger.info('Calculating Economy Matchup')
    batsman_data = data[(data['batsman'].isin(include_batsman))]
    dict_lookup = dict(zip(mapping['batsman'], mapping['Batting_type']))
    batsman_data['striker_batting_type'] = [dict_lookup[item] for item in list(batsman_data['batsman'])]

    if (len(include_bowlers) > 3):
        batsman_data = batsman_data[(batsman_data['bowler'].isin(include_bowlers))].groupby(
            ['batsman', 'striker_batting_type', 'phase', 'bowler_sub_type', 'bowler']).last()
    else:
        batsman_data = batsman_data[(~batsman_data['bowler'].isin(exclude_bowlers))].groupby(
            ['batsman', 'striker_batting_type', 'phase', 'bowler_sub_type', 'bowler']).last()
    batsman_data_f = batsman_data.drop(columns=drop_cols)[
        (batsman_data['bowl_Team'] == home_team) & (batsman_data['balls_sum_bat'] > 15) & (
                batsman_data['balls_sum_ball'] > 30)]
    valid_keys = [key for key in include_batsman if key in batsman_data_f.index.get_level_values(0)]

    # batsman_data_rg = \
    #     batsman_data_f.reset_index().groupby(['batsman', 'Preferred Position', 'striker_batting_type', 'phase']).apply(
    #         lambda x: x.sort_values(["Bowling_economy"]).head(3)).loc[valid_keys]

    batsman_data_rg = \
        batsman_data_f.reset_index().groupby(['batsman', 'striker_batting_type', 'phase']).apply(
            lambda x: x.sort_values(["Bowling_economy"]).head(3)).loc[valid_keys]

    # bt1 = batsman_data_rg.droplevel(4).drop(
    #     columns=['batsman', 'striker_batting_type', 'phase', 'Preferred Position']).reset_index()

    bt1 = batsman_data_rg.droplevel(3).drop(
        columns=['batsman', 'striker_batting_type', 'phase']).reset_index()
    btt1 = bt1.merge(bbb_grouped[['phase', 'batsman', 'bowler', 'H2H_Economy', 'H2H_Wicket_pct']],
                     left_on=['phase', 'batsman', 'bowler'], right_on=['phase', 'batsman', 'bowler'], how='left')
    # btt1_grp = btt1.groupby(['batsman', 'Preferred Position', 'striker_batting_type', 'phase']).apply(
    #     lambda x: x.sort_values(["Bowling_economy"])).loc[valid_keys]
    btt1_grp = btt1.groupby(['batsman', 'striker_batting_type', 'phase']).apply(
        lambda x: x.sort_values(["Bowling_economy"])).loc[valid_keys]
    # bt1_res = btt1_grp.drop(columns=['batsman', 'striker_batting_type', 'phase', 'Preferred Position']).rename(
    #     index={'1 POWERPLAY': 'POWERPLAY', '2 MIDDLE FIRST HALF': 'MIDDLE FIRST HALF',
    #            '3 MIDDLE SECOND HALF': 'MIDDLE SECOND HALF'},
    #     columns={
    #         'runs_sum_bat': 'Bat Total Runs', 'balls_sum_bat': 'Balls Faced', 'wickets_bat': 'Bat Wicket Lost',
    #         'bowler': 'bowler',
    #         'runs_sum_ball': 'Bowl Total  Runs', 'balls_sum_ball': 'Balls  Thrown', 'wickets_ball': 'Wicket Taken'}
    # ).reset_index().drop(columns=['level_4'])
    bt1_res = btt1_grp.drop(columns=['batsman', 'striker_batting_type', 'phase']).rename(
        index={'1 POWERPLAY': 'POWERPLAY', '2 MIDDLE FIRST HALF': 'MIDDLE FIRST HALF',
               '3 MIDDLE SECOND HALF': 'MIDDLE SECOND HALF'},
        columns={
            'runs_sum_bat': 'Bat Total Runs', 'balls_sum_bat': 'Balls Faced', 'wickets_bat': 'Bat Wicket Lost',
            'bowler': 'bowler',
            'runs_sum_ball': 'Bowl Total  Runs', 'balls_sum_ball': 'Balls  Thrown', 'wickets_ball': 'Wicket Taken'}
    ).reset_index().drop(columns=['level_3'])
    bt1_res['bowling_economy_bucket'] = pd.cut(bt1_res['Bowling_economy'], bins=[4, 5, 6, 7, 8, np.inf],
                                               labels=['Very Low', 'Low', 'Mid', 'High', 'Very High'])
    eco_df = bt1_res

    int_cols = ['Bat Total Runs', 'Balls Faced', 'Bat Wicket Lost', 'Bowl Total  Runs', 'Balls  Thrown', 'Wicket Taken']
    for column in eco_df:
        if (column in int_cols):
            eco_df[column] = eco_df[column].astype(int)

    # Calculate Wicket Taking Matchup
    logger.info('Calculating Wicket Taking Matchup')
    batsman_data = data[(data['batsman'].isin(include_batsman))]
    dict_lookup = dict(zip(mapping['batsman'], mapping['Batting_type']))
    batsman_data['striker_batting_type'] = [dict_lookup[item] for item in list(batsman_data['batsman'])]

    if (len(include_bowlers) > 3):
        batsman_data = batsman_data[(batsman_data['bowler'].isin(include_bowlers))].groupby(
            ['batsman', 'striker_batting_type', 'phase', 'bowler_sub_type', 'bowler']).last()
    else:
        batsman_data = batsman_data[(~batsman_data['bowler'].isin(exclude_bowlers))].groupby(
            ['batsman', 'striker_batting_type', 'phase', 'bowler_sub_type', 'bowler']).last()
    batsman_data_f = batsman_data.drop(columns=drop_cols)[
        (batsman_data['bowl_Team'] == home_team) & (batsman_data['balls_sum_bat'] > 15) & (
                batsman_data['balls_sum_ball'] > 30)]
    valid_keys = [key for key in include_batsman if key in batsman_data_f.index.get_level_values(0)]

    # batsman_data_rg = \
    #     batsman_data_f.reset_index().groupby(['batsman', 'Preferred Position', 'striker_batting_type', 'phase']).apply(
    #         lambda x: x.sort_values(["Bowling_strike_rate"]).head(3)).loc[valid_keys]
    # bt2 = batsman_data_rg.droplevel(4).drop(
    #     columns=['batsman', 'striker_batting_type', 'phase', 'Preferred Position']).reset_index()

    batsman_data_rg = \
        batsman_data_f.reset_index().groupby(['batsman', 'striker_batting_type', 'phase']).apply(
            lambda x: x.sort_values(["Bowling_strike_rate"]).head(3)).loc[valid_keys]
    bt2 = batsman_data_rg.droplevel(3).drop(
        columns=['batsman', 'striker_batting_type', 'phase']).reset_index()
    btt2 = bt2.merge(bbb_grouped[['phase', 'batsman', 'bowler', 'H2H_Economy', 'H2H_Wicket_pct']],
                     left_on=['phase', 'batsman', 'bowler'], right_on=['phase', 'batsman', 'bowler'], how='left')
    # btt2_grp = btt2.groupby(['batsman', 'Preferred Position', 'striker_batting_type', 'phase']).apply(
    #     lambda x: x.sort_values(["Bowling_strike_rate"])).loc[valid_keys]
    # bt2_res = btt2_grp.drop(
    #     columns=['batsman', 'striker_batting_type', 'phase', 'Preferred Position']
    # ).rename(
    #     index={'1 POWERPLAY': 'POWERPLAY', '2 MIDDLE FIRST HALF': 'MIDDLE FIRST HALF',
    #            '3 MIDDLE SECOND HALF': 'MIDDLE SECOND HALF'},
    #     columns={
    #         'runs_sum_bat': 'Bat Total Runs', 'balls_sum_bat': 'Balls Faced', 'wickets_bat': 'Bat Wicket Lost',
    #         'bowler': 'bowler',
    #         'runs_sum_ball': 'Bowl Total  Runs', 'balls_sum_ball': 'Balls  Thrown', 'wickets_ball': 'Wicket Taken'}
    # ).reset_index().drop(columns=['level_4'])
    btt2_grp = btt2.groupby(['batsman', 'striker_batting_type', 'phase']).apply(
        lambda x: x.sort_values(["Bowling_strike_rate"])).loc[valid_keys]
    bt2_res = btt2_grp.drop(
        columns=['batsman', 'striker_batting_type', 'phase']
    ).rename(
        index={'1 POWERPLAY': 'POWERPLAY', '2 MIDDLE FIRST HALF': 'MIDDLE FIRST HALF',
               '3 MIDDLE SECOND HALF': 'MIDDLE SECOND HALF'},
        columns={
            'runs_sum_bat': 'Bat Total Runs', 'balls_sum_bat': 'Balls Faced', 'wickets_bat': 'Bat Wicket Lost',
            'bowler': 'bowler',
            'runs_sum_ball': 'Bowl Total  Runs', 'balls_sum_ball': 'Balls  Thrown', 'wickets_ball': 'Wicket Taken'}
    ).reset_index().drop(columns=['level_3'])
    bt2_res['Bowling_strike_rate_bucket'] = pd.cut(bt2_res['Bowling_strike_rate'],
                                                   bins=[-np.inf, 10, 20, 30, 40, np.inf],
                                                   labels=['Very Low', 'Low', 'Mid', 'High', 'Very High'])
    wkt_df = bt2_res
    for column in wkt_df:
        if (column in int_cols):
            wkt_df[column] = wkt_df[column].astype(int)

    # Calculate Extra Matchup
    logger.info('Calculating Extra Matchup')

    if include_bowlers:
        bb = data[(data['batsman'].isin(include_batsman)) & (data['bowl_Team'] == home_team) & (
            ~data['bowler'].isin(exclude_bowlers)) & (data['bowler'].isin(include_bowlers))].groupby(
            ['phase', 'striker_batting_type', 'bowler_sub_type', 'bowler']).last()
    else:
        bb = data[(data['batsman'].isin(include_batsman)) & (data['bowl_Team'] == home_team) & (
            ~data['bowler'].isin(exclude_bowlers))].groupby(
            ['phase', 'striker_batting_type', 'bowler_sub_type', 'bowler']).last()
    bb1 = bb.reset_index().groupby(['phase', 'striker_batting_type']).apply(
        lambda x: x.sort_values(["Bowling_economy"]).head(3))
    bb1 = bb1[['bowler', 'Bowling_economy']].droplevel(2).rename(columns={'bowler': 'bowler_eco'}).reset_index()
    bb1 = bb1.groupby(['phase', 'striker_batting_type'])['bowler_eco'].apply(list).reset_index()
    bb2 = bb.reset_index().groupby(['phase', 'striker_batting_type']).apply(
        lambda x: x.sort_values(["Bowling_strike_rate"]).head(3))
    bb2 = bb2[['bowler', 'Bowling_strike_rate']].droplevel(2).rename(columns={'bowler': 'bowler_wkt'}).reset_index()
    bb2 = bb2.groupby(['phase', 'striker_batting_type'])['bowler_wkt'].apply(list).reset_index()
    res = bb1.merge(bb2, left_on=['phase', 'striker_batting_type'], right_on=['phase', 'striker_batting_type'])
    split_df = pd.DataFrame(res['bowler_eco'].tolist(), columns=['bowler_eco_1', 'bowler_eco_2', 'bowler_eco_3'])
    res = pd.concat([res, split_df], axis=1)

    split_df = pd.DataFrame(res['bowler_wkt'].tolist(), columns=['bowler_wkt_1', 'bowler_wkt_2', 'bowler_wkt_3'])
    res = pd.concat([res, split_df], axis=1)

    res = pd.pivot_table(
        res.reset_index(),
        values=['bowler_eco_1', 'bowler_eco_2', 'bowler_eco_3', 'bowler_wkt_1', 'bowler_wkt_2', 'bowler_wkt_3'],
        index=['phase', 'striker_batting_type'],
        aggfunc='last')

    res.rename(index={'1 POWERPLAY': 'POWERPLAY', '2 MIDDLE FIRST HALF': 'MIDDLE FIRST HALF',
                      '3 MIDDLE SECOND HALF': 'MIDDLE SECOND HALF'}, inplace=True)
    avg_df = res.reset_index()

    ## Calculate Combined Matchup
    logger.info('Calculating combined matchup')
    perf_list = ['avg', 'avg', 'avg', 'avg', 'avg', 'avg', 'avg', 'avg', 'avg', 'avg', 'avg']
    comb_df, batting_order = get_pivoted_matchup(mapping, eco_df, wkt_df, avg_df, include_batsman, perf_list,
                                                 match_date, start_over, replaced_over_no)

    best_df = []

    for player in best_list:
        logger.info('Calculating Best of {0}'.format(player))
        perf_list = ['avg', 'avg', 'avg', 'avg', 'avg', 'avg', 'avg', 'avg', 'avg', 'avg', 'avg']
        perf_list[include_batsman.index(player)] = "best"
        tmp1, tmp2 = get_pivoted_matchup(mapping, eco_df, wkt_df, avg_df, include_batsman, perf_list, match_date,
                                         start_over, replaced_over_no)
        best_df.append(tmp1)

    worst_df = []

    for player in worst_list:
        logger.info('Calculating Worst of {0}'.format(player))
        perf_list = ['avg', 'avg', 'avg', 'avg', 'avg', 'avg', 'avg', 'avg', 'avg', 'avg', 'avg']
        perf_list[include_batsman.index(player)] = "worst"
        tmp3, tmp4 = get_pivoted_matchup(mapping, eco_df, wkt_df, avg_df, include_batsman, perf_list, match_date,
                                         start_over, replaced_over_no)
        # worst_df.append(get_pivoted_matchup(mapping, eco_df, wkt_df, avg_df, include_batsman, perf_list, match_date, start_over, replaced_over_no))
        worst_df.append(tmp3)

    return (eco_df, wkt_df, comb_df, best_df, worst_df, playing_eleven_bowler_class, batting_order)

# include_batsman = ['Kl Rahul', 'Quinton De Kock', 'Ayush Badoni', 'Manish Pandey', 'Marcus Stoinis', 'Deepak Hooda', 'Krunal Pandya',
# 'Jason Holder', 'Krishnappa Gowtham', 'Dushmantha Chameera', 'Avesh Khan', 'Ravi Bishnoi']

# CSK Batting Lineup
# include_batsman = ['Ruturaj Gaikwad', 'Robin Uthappa', 'Moeen Ali', 'Ambati Rayadu', 'Shivam Dube', 'Ravindra Jadeja', 'Ms Dhoni',
# 'Dwayne Bravo', 'Chris Jordan', 'Maheesh Theekshana', 'Mukesh Choudhary']

# exclude_bowlers = ['Arjun Tendulkar','Arshad Khan','Dewald Brevis','Hrithik Shokeen','Rahul Buddhi','Rohit Sharma','Sanjay Yadav','Jofra Archer','Tim David',
# 'Ramandeep Singh','Tilak Varma','Tim David','Riley Meredith','Fabian Allen','Daniel Sams']
#
# include_bowlers = ['Kieron Pollard', 'Jasprit Bumrah', 'Murugan Ashwin', 'Basil Thampi', 'Jaydev Unadkat', 'Tymal Mills']

# best_list = ['Ruturaj Gaikwad', 'Shivam Dube']

# a, b, c, d, e = get_matchup_calculation(include_batsman, exclude_bowlers, include_bowlers)
# print(f"Value of a --> {a}")
# print(f"Value of b --> {b}")
# print(f"Value of c --> {c}")


# %%
# a.head(20)
# %%
# a[a['batsman']=='Chris Jordan']
# %%
# d[0]

# %%
