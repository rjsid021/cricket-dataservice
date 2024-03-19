import json
import sys
sys.path.append("./../../")
sys.path.append("./")
from DataIngestion.utils.helper import readCSV
from DataService.app_config import IPL_RETAINED_LIST_PATH, IPL_AUCTION_LIST_PATH
from flask import Blueprint, request, Response, jsonify

from common.authentication.auth import token_required
from werkzeug.exceptions import HTTPException
from marshmallow import ValidationError
from DataService.utils.helper import globalFilters, generateWhereClause, dropFilter, generate_api_function, open_api_spec
from DataService.src import *


app_auction = Blueprint("app_auction", __name__)
open_api_spec = open_api_spec()

pd.options.mode.chained_assignment = None


# @app_auction.route("/perfVsDiffBowlersAndBatsman", methods=['POST'])
@token_required
def perfVsDiffBowlersAndBatsman():
    logger = get_logger("perfVsDiffBowlersAndBatsman", "perfVsDiffBowlersAndBatsman")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        response = {}
        if request.json:
            if "overs" in request.json:
                filter_dict['over_number'] = request.json.get('overs')
        filters, params = generateWhereClause(filter_dict)
        if filters:
            performance_diff_bowlers_sql = '''
            select 
              player_id, 
              player_name, 
              bowling_type as bowler_type, 
              count(distinct match_id) as innings, 
              match_phase, 
              cast(
                sum(balls) as int
              ) as balls, 
              cast(
                sum(runs) as int
              ) as runs, 
              round(
                sum(runs)* 100.00 / sum(balls), 
                2
              ) as batting_strike_rate, 
              cast(
                sum(wickets) as int
              ) as wickets, 
              coalesce(
                round(
                  (
                    sum(runs)* 1.00
                  )/ sum(wickets), 
                  2
                ), 
                0.0
              ) as batting_average, 
              coalesce(
                round(
                  (
                    (
                      (
                        sum(sixes)* 6
                      )+(
                        sum(fours * 4)
                      )
                    )* 100.00
                  )/ sum(runs), 
                  2
                ), 
                0.0
              ) as boundary_percent, 
              round(
                (
                  sum(dot_balls)* 100.00 / sum(balls)
                ), 
                2
              ) as dot_ball_percent 
            from 
              batsman_overwise_df
            ''' + filters + ''' 
            group by 
              bowling_type, 
              player_id, 
              player_name, 
              match_phase
            '''

            out_batsman_sql = '''
            select 
              out_batsman_id, 
              bowling_type, 
              match_phase,
              round(
                count(out_batsman_id)
              ) as wicket_cnt 
            from 
              join_data 
            ''' + filters.replace(
                'player_id', 'out_batsman_id'
            ).replace(
                ' team_id', ' batsman_team_id'
            ).replace(
                'player_skill', 'out_batsman_skill'
            ) + ''' 
              and out_batsman_id<>-1 
              and innings not in (3,4) 
            group by 
              out_batsman_id, 
              bowling_type, 
              match_phase
            '''

            performance_against_diff_batsman_sql = '''
            select 
              player_id, 
              player_name,
              batting_type as batsman_type,
              count(distinct match_id) as innings, 
              match_phase, 
              cast(
                sum(runs) as int
              ) as runs_conceded, 
              cast(
                sum(balls) as int
              ) as balls, 
              cast(
                sum(wickets) as int
              ) as wickets,
              round(
                  coalesce(
                    (
                      (
                        sum(runs)* 1.00
                      )/ sum(wickets)
                    ), 
                    0.0
                  ), 
                  2
                ) as average, 
              round(
                  coalesce(
                    (
                      (
                        sum(balls)* 1.00
                      )/ sum(wickets)
                    ), 
                    0.0
                  ), 
                  2
                ) as bowling_strike_rate, 
              round(
                (
                  sum(dot_balls)* 100.00 / sum(balls)
                ), 
                2
              ) as dot_ball_percent 
            from 
              bowler_overwise_df ''' + filters + '''
            group by  
              player_name, 
              player_id,
              batting_type,
              match_phase 
            '''

        else:
            params = []
            performance_diff_bowlers_sql = '''
            select 
              player_id, 
              player_name, 
              bowling_type as bowler_type, 
              count(distinct match_id) as innings, 
              match_phase, 
              cast(
                sum(balls) as int
              ) as balls, 
              cast(
                sum(runs) as int
              ) as runs, 
              round(
                sum(runs)* 100.00 / sum(balls), 
                2
              ) as batting_strike_rate, 
              cast(
                sum(wickets) as int
              ) as wickets, 
              coalesce(
                round(
                  (
                    sum(runs)* 1.00
                  )/ sum(wickets), 
                  2
                ), 
                0.0
              ) as batting_average, 
              coalesce(
                round(
                  (
                    (
                      (
                        sum(sixes)* 6
                      )+(
                        sum(fours * 4)
                      )
                    )* 100.00
                  )/ sum(runs), 
                  2
                ), 
                0.0
              ) as boundary_percent, 
              round(
                (
                  sum(dot_balls)* 100.00 / sum(balls)
                ), 
                2
              ) as dot_ball_percent 
            from 
              batsman_overwise_df
            group by
              bowling_type, 
              player_id, 
              player_name, 
              match_phase
            '''

            out_batsman_sql = '''
            select 
              out_batsman_id, 
              bowling_type, 
              match_phase,
              round(
                count(out_batsman_id)
              ) as wicket_cnt 
            from 
              join_data 
            where 
              out_batsman_id<>-1 
              and innings not in (3,4) 
            group by 
              out_batsman_id, 
              bowling_type, 
              match_phase
            '''

            performance_against_diff_batsman_sql = '''
            select 
              player_id, 
              player_name,
              batting_type as batsman_type,
              count(distinct match_id) as innings, 
              match_phase, 
              cast(
                sum(runs) as int
              ) as runs_conceded, 
              cast(
                sum(balls) as int
              ) as balls, 
              cast(
                sum(wickets) as int
              ) as wickets,
              round(
                  coalesce(
                    (
                      (
                        sum(runs)* 1.00
                      )/ sum(wickets)
                    ), 
                    0.0
                  ), 
                  2
                ) as average, 
              round(
                  coalesce(
                    (
                      (
                        sum(balls)* 1.00
                      )/ sum(wickets)
                    ), 
                    0.0
                  ), 
                  2
                ) as bowling_strike_rate, 
              round(
                (
                  sum(dot_balls)* 100.00 / sum(balls)
                ), 
                2
              ) as dot_ball_percent 
            from 
              bowler_overwise_df
            group by  
              player_name, 
              player_id,
              batting_type,
              match_phase 
            '''

        performance_against_bowlers = executeQuery(con, performance_diff_bowlers_sql, params)
        out_batsman_df = executeQuery(con, out_batsman_sql, params)

        performance_against_diff_bowlers = '''
        select 
          player_id,
          player_name,
          bowler_type, 
          pdf.match_phase,
          innings, 
          balls, 
          runs, 
          batting_strike_rate, 
          cast(
            coalesce(wicket_cnt, 0) as int
          ) as wickets, 
          round(
            coalesce(runs / wicket_cnt, 0), 
            2
          ) as batting_average, 
          boundary_percent, 
          dot_ball_percent 
        from 
          performance_against_bowlers pdf 
          left join out_batsman_df odf on (
            odf.out_batsman_id = pdf.player_id 
            and pdf.bowler_type = odf.bowling_type
            and pdf.match_phase = odf.match_phase
          )
        '''

        batsman_against_diff_bowlers = executeQuery(con, performance_against_diff_bowlers)
        bowler_against_diff_batsman = executeQuery(con, performance_against_diff_batsman_sql, params)

        batsman_against_diff_bowlers['batsman_data'] = batsman_against_diff_bowlers[[
            'player_id',
            'player_name',
            'innings',
            'balls',
            'runs',
            'batting_strike_rate',
            'wickets',
            'batting_average',
            'boundary_percent',
            'dot_ball_percent',
            'match_phase'
        ]].to_dict(orient='records')

        bowler_against_diff_batsman['bowler_data'] = bowler_against_diff_batsman[[
            'player_id',
            'player_name',
            'innings',
            'match_phase',
            'runs_conceded',
            'balls',
            'wickets',
            'average',
            'bowling_strike_rate',
            'dot_ball_percent'
        ]].to_dict(orient='records')

        response['batting_stats'] = batsman_against_diff_bowlers.drop(
            [
                'player_id',
                'player_name',
                'innings',
                'balls',
                'runs',
                'batting_strike_rate',
                'wickets',
                'batting_average',
                'boundary_percent',
                'dot_ball_percent',
                'match_phase'
            ],
            axis=1
        ).groupby(['bowler_type'])['batsman_data'].agg(list).reset_index().to_dict(orient='records')

        response['bowling_stats'] = bowler_against_diff_batsman.drop(
            [
                'player_id',
                'player_name',
                'innings',
                'match_phase',
                'runs_conceded',
                'balls',
                'wickets',
                'average',
                'bowling_strike_rate',
                'dot_ball_percent'
            ],
            axis=1
        ).groupby(['batsman_type'])['bowler_data'].agg(list).reset_index().to_dict(orient='records')

        return jsonify(response), logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app_auction.route("/performanceByPositionAndPhase", methods=['POST'])
@token_required
def performanceByPositionAndPhase():
    logger = get_logger("performanceByPositionAndPhase", "performanceByPositionAndPhase")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        response = {}
        data_df = pd.DataFrame()

        if request.json:
            if request.json.get('overs'):
                filter_dict['over_number'] = request.json.get('overs')

        filters, params = generateWhereClause(filter_dict)

        out_params = params
        out_filter_dict = filter_dict.copy()
        del_keys = ['venue', 'player_skill', 'bowling_type', 'batting_type', 'winning_type', 'over_number']
        out_filters, inner_params = generateWhereClause(dropFilter(del_keys, out_filter_dict))
        inner_params.extend(out_params)
        del_key_bowling = ['over_number']
        bowl_filters, bowl_params = generateWhereClause(dropFilter(del_key_bowling, out_filter_dict))

        if filters:
            positionwise_batsman_stats = '''
            select  
              batting_position, 
              cast(
                sum(runs) as int
              ) as total_runs_scored, 
              cast(
                sum(balls) as int
              ) as balls_played, 
              count(distinct match_id) as innings_played, 
              cast(
                sum(wickets) as int
              ) as dismissals, 
              player_id,
              player_name, 
              round(
                coalesce(
                  (
                    (
                      sum(runs)* 100.00
                    )/ sum(balls)
                  ), 
                  0.0
                ), 
                2
              ) as strike_rate, 
              round(
                coalesce(
                  (
                    sum(runs)* 1.00
                  )/ sum(wickets), 
                  0.0
                ), 
                2
              ) as batting_average,
              round(coalesce(((sum(fours)*4 + sum(sixes)*6)*100.00)/sum(runs),0.0),2) as boundary_percent,
              round(coalesce((sum(dot_balls)*100.00/sum(balls)),0.0),2) as dot_ball_percent,
            from 
              (
                select 
                  match_id, 
                  player_id, 
                  bowler_team_id as against_team_id, 
                  bowler_team_name as against_team, 
                  team_name, 
                  season, 
                  innings, 
                  venue, 
                  fours,
                  sixes,
                  dot_balls,
                  batting_type, 
                  bowling_type, 
                  winning_type, 
                  batting_position, 
                  over_number, 
                  runs, 
                  balls, 
                  not_out, 
                  player_name, 
                  wickets, 
                  dense_rank() over (
                    order by 
                      match_id desc
                  ) as match_rank 
                from 
                  batsman_overwise_df
            ''' + filters + ''')  
            group by 
              player_id, 
              batting_position, 
              player_name 
            order by 
              batting_position desc
            '''
            out_batsman_sql = f'''
            select 
              bdf.batting_position, 
              out_batsman_id, 
              count(out_batsman_id) as wicket_cnt 
            from 
              join_data jd 
              left join (
                select 
                  match_id, 
                  batting_position, 
                  batsman_id 
                from 
                  bat_card_data {out_filters.replace('player_id', 'batsman_id').replace(' team_id', ' batting_team_id')} 
                  and innings not in (3, 4)
              ) bdf on (
                jd.match_id = bdf.match_id 
                and bdf.batsman_id = jd.out_batsman_id
              ) {filters.replace('player_id', 'out_batsman_id').replace(' team_id', ' batsman_team_id').replace(
                'player_skill', 'out_batsman_skill'
            )} 
              and out_batsman_id <>-1 
              and jd.innings not in (3, 4) 
            group by 
              bdf.batting_position, 
              out_batsman_id
            '''

            phasewise_bowler_stats = '''
            SELECT
                player_id,
                player_name,
                match_phase,
                count(distinct match_id) as innings_played, 
                cast(
                  sum(runs) as int
                ) as total_runs_conceded,
                cast(
                  sum(wickets) as int
                ) as wickets, 
                round(
                  coalesce(
                    (
                      (
                        sum(runs)* 1.00
                      )/ sum(wickets)
                    ), 
                    0.0
                  ), 
                  2
                ) as average, 
                round(
                  coalesce(
                    (
                      (
                        sum(balls)* 1.00
                      )/ sum(wickets)
                    ), 
                    0.0
                  ), 
                  2
                ) as strike_rate, 
                round(
                  coalesce(
                    (
                      (
                        sum(runs)* 1.00
                      )/(
                        sum(balls)* 1.00 / 6
                      )
                    ), 
                    0.0
                  ), 
                  2
                ) as economy,
              round(
                (
                  sum(dot_balls)* 100.00
                )/ sum(balls), 
                2
              ) as dot_percent
            FROM
                bowler_overwise_df ''' + bowl_filters + '''
            GROUP BY
                player_id,
                player_name,
                match_phase
            '''
        else:
            positionwise_batsman_stats = '''
            select  
              batting_position, 
              cast(
                sum(runs) as int
              ) as total_runs_scored, 
              cast(
                sum(balls) as int
              ) as balls_played, 
              count(distinct match_id) as innings_played, 
              cast(
                sum(wickets) as int
              ) as dismissals,
              player_name,
              player_id, 
              round(
                coalesce(
                  (
                    (
                      sum(runs)* 100.00
                    )/ sum(balls)
                  ), 
                  0.0
                ), 
                2
              ) as strike_rate, 
              round(
                coalesce(
                  (
                    sum(runs)* 1.00
                  )/ sum(wickets), 
                  0.0
                ), 
                2
              ) as batting_average,
              round(coalesce(((sum(fours)*4 + sum(sixes)*6)*100.00)/sum(runs),0.0),2) as boundary_percent,
              round(coalesce((sum(dot_balls)*100.00/sum(balls)),0.0),2) as dot_ball_percent,
            from 
              (
                select 
                  match_id, 
                  player_id, 
                  bowler_team_id as against_team_id, 
                  bowler_team_name as against_team, 
                  team_name, 
                  season, 
                  innings, 
                  venue, 
                  fours,
                  sixes,
                  dot_balls,
                  batting_type, 
                  bowling_type, 
                  winning_type, 
                  batting_position, 
                  over_number, 
                  runs, 
                  balls, 
                  not_out, 
                  player_name, 
                  wickets, 
                  dense_rank() over (
                    order by 
                      match_id desc
                  ) as match_rank 
                from 
                  batsman_overwise_df
                  )  
            group by 
              player_id, 
              batting_position, 
              player_name 
            order by 
              batting_position desc
            '''
            out_batsman_sql = f'''
            select 
              bdf.batting_position, 
              out_batsman_id, 
              count(out_batsman_id) as wicket_cnt 
            from 
              join_data jd 
              left join (
                select 
                  match_id, 
                  batting_position, 
                  batsman_id 
                from 
                  bat_card_data 
                  where innings not in (3, 4)
              ) bdf on (
                jd.match_id = bdf.match_id 
                and bdf.batsman_id = jd.out_batsman_id
              ) 
              where out_batsman_id <>-1 
              and jd.innings not in (3, 4) 
            group by 
              bdf.batting_position, 
              out_batsman_id
            '''

            phasewise_bowler_stats = '''
            SELECT
                player_id,
                player_name,
                match_phase,
                count(distinct match_id) as innings_played,
                cast(
                  sum(runs) as int
                ) as total_runs_conceded,  
                cast(
                  sum(wickets) as int
                ) as wickets, 
                round(
                  coalesce(
                    (
                      (
                        sum(runs)* 1.00
                      )/ sum(wickets)
                    ), 
                    0.0
                  ), 
                  2
                ) as average, 
                round(
                  coalesce(
                    (
                      (
                        sum(balls)* 1.00
                      )/ sum(wickets)
                    ), 
                    0.0
                  ), 
                  2
                ) as strike_rate, 
                round(
                  coalesce(
                    (
                      (
                        sum(runs)* 1.00
                      )/(
                        sum(balls)* 1.00 / 6
                      )
                    ), 
                    0.0
                  ), 
                  2
                ) as economy,
              round(
                (
                  sum(dot_balls)* 100.00
                )/ sum(balls), 
                2
              ) as dot_percent
            FROM 
                bowler_overwise_df ''' + bowl_filters + '''
            GROUP BY
                player_id,
                player_name,
                match_phase
                '''

        positionwise_batsman_df = executeQuery(con, positionwise_batsman_stats, params)
        out_batsman_df = executeQuery(con, out_batsman_sql, inner_params)
        # phasewise_bowler_df = executeQuery(con, phasewise_bowler_stats, bowl_params)

        positionwise_batsman_stats_final = '''
        select 
          pdf.batting_position, 
          total_runs_scored, 
          innings_played, 
          player_id, 
          player_name, 
          boundary_percent,
          dot_ball_percent,
          strike_rate, 
          round(
            coalesce(
              (total_runs_scored * 1.00)/ wicket_cnt, 
              0.0
            ), 
            2
          ) as batting_average 
        from 
          positionwise_batsman_df pdf 
          left join out_batsman_df odf on (
            odf.out_batsman_id = pdf.player_id 
            and odf.batting_position = pdf.batting_position
          )
        '''

        response['batting_stats'] = executeQuery(con, positionwise_batsman_stats_final).to_dict(orient='records')
        response['bowling_stats'] = executeQuery(con, phasewise_bowler_stats, bowl_params).to_dict(orient='records')

        return jsonify(response), logger.info("Status - 200")
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"{e}", 500))


# @app_auction.route("/latestPlayerPerformances", methods=['POST'])
@token_required
def latestPlayerPerformances():
    logger = get_logger("latestPlayerPerformances", "latestPlayerPerformances")

    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        response = {}
        del_keys = ['batting_type', 'bowling_type']

        filters, params = generateWhereClause(dropFilter(del_keys, filter_dict))

        if filters:
            batsman_performance = '''
            select 
              match_id,
              player_id, 
              player_name, 
              bat_current_team as team_name, 
              bowler_team_name as against_team, 
              match_result, 
              cast(coalesce(winning_team,0) as int) as winning_team, 
              cast(
                sum(runs) as int
              ) as runs, 
              cast(
                sum(balls) as int
              ) as balls, 
              cast(not_out as int) as is_not_out,
              match_date
            from 
              batsman_overwise_df ''' + filters.replace("team_id", "bowler_team_id") + ''' 
            group by 
              season, 
              match_id, 
              venue, 
              player_name,
              player_id, 
              bat_current_team, 
              bowler_team_name, 
              match_result, 
              winning_team, 
              not_out,
              match_date
            '''

            bowler_performance = '''
            select 
              match_id, 
              player_id, 
              player_name, 
              batsman_team_name as against_team, 
              bowl_current_team as team_name,
              match_result, 
              cast(coalesce(winning_team,0) as int) as winning_team, 
              cast(
                sum(runs) as int
              ) as runs, 
              cast(
                sum(balls) as int
              ) as balls, 
              case when (
                sum(balls)% 6
              )== 0 then (
                (
                  sum(balls)* 1.00
                )/ 6
              ) else (
                sum(balls)/ 6
              ) + (
                (
                  sum(balls)% 6
                )/ 10.00
              ) end as overs, 
              cast(
                sum(wickets) as int
              ) as wickets,
              match_date
            from 
              bowler_overwise_df ''' + filters.replace("team_id", "batsman_team_id") + ''' 
            group by 
              season, 
              match_id, 
              venue,
              player_name, 
              player_id, 
              batsman_team_name, 
              bowl_current_team, 
              match_result, 
              winning_team,
              match_date
            '''
        else:
            batsman_performance = '''
            select 
              match_id,
              player_id, 
              player_name, 
              bat_current_team as team_name, 
              bowler_team_name as against_team, 
              match_result, 
              cast(coalesce(winning_team,0) as int) as winning_team, 
              cast(
                sum(runs) as int
              ) as runs, 
              cast(
                sum(balls) as int
              ) as balls, 
              cast(not_out as int) as is_not_out,
              match_date
            from 
              batsman_overwise_df 
            group by 
              season, 
              match_id, 
              venue,
              player_id, 
              player_name, 
              bat_current_team, 
              bowler_team_name, 
              match_result, 
              winning_team, 
              not_out,
              match_date
            '''

            bowler_performance = '''
            select 
              match_id, 
              player_id, 
              player_name, 
              batsman_team_name as against_team, 
              bowl_current_team as team_name, 
              match_result, 
              cast(coalesce(winning_team,0) as int) as winning_team, 
              cast(
                sum(runs) as int
              ) as runs, 
              cast(
                sum(balls) as int
              ) as balls, 
              case when (
                sum(balls)% 6
              )== 0 then (
                (
                  sum(balls)* 1.00
                )/ 6
              ) else (
                sum(balls)/ 6
              ) + (
                (
                  sum(balls)% 6
                )/ 10.00
              ) end as overs, 
              cast(
                sum(wickets) as int
              ) as wickets,
            match_date
            from 
              bowler_overwise_df 
            group by 
              season, 
              match_id, 
              venue, 
              player_name, 
              player_id, 
              batsman_team_name, 
              bowl_current_team, 
              match_result, 
              winning_team,
              match_date
            '''
        # sort by match_date in batsman performance
        BatsmanPerformance = executeQuery(con, batsman_performance, params)
        BatsmanPerformance['match_date'] = pd.to_datetime(BatsmanPerformance['match_date'], format='%Y-%m-%d')
        BatsmanPerformance.sort_values(by='match_date', ascending=False, inplace=True)
        # sort by match_date in bowler performance
        BowlerPerformance = executeQuery(con, bowler_performance, params)
        BowlerPerformance['match_date'] = pd.to_datetime(BowlerPerformance['match_date'], format='%Y-%m-%d')
        BowlerPerformance.sort_values(by='match_date', ascending=False, inplace=True)
        # drop match_date column
        BatsmanPerformance = BatsmanPerformance.drop(columns=['match_date'], axis=1)
        BowlerPerformance = BowlerPerformance.drop(columns=['match_date'], axis=1)
        response['batsman_performance'] = BatsmanPerformance.to_dict(orient='records')
        response['bowler_performance'] = BowlerPerformance.to_dict(orient='records')

        return jsonify(response), logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app_auction.route("/pressureIndexDashboard", methods=['POST'])
@token_required
def pressureIndexDashboard():
    logger = get_logger("pressureIndexDashboard", "pressureIndexDashboard")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        response = {}
        if "overs" in request.json:
            filter_dict['over_number'] = request.json['overs']

        if "entry_point" in request.json:
            filter_dict['entry_point'] = request.json['entry_point']

        if "batting_position" in request.json:
            filter_dict['batting_position'] = request.json['batting_position']

        if "pressure_cat" in request.json:
            filter_dict['pressure_cat'] = request.json['pressure_cat']

        if "asc" in request.json:
            asc = request.json['asc']
        else:
            asc = False

        if "sort_key" in request.json:
            sort_key = request.json['sort_key']
            sort_key_bowling = "strike_rate"
        elif "sort_key_bowling" in request.json:
            sort_key_bowling = request.json['sort_key_bowling']
            sort_key = "strike_rate"
        else:
            sort_key = "strike_rate"
            sort_key_bowling = "strike_rate"

        if "min_innings" in request.json:
            min_innings = request.json['min_innings']
        else:
            min_innings = 1

        if "total_balls" in request.json:
            total_balls = request.json['total_balls']
        else:
            total_balls = 1

        filters, params = generateWhereClause(filter_dict)
        bowl_filter_dict = filter_dict.copy()

        del_key_bowling = ['batting_position']
        bowl_filters, bowl_params = generateWhereClause(dropFilter(del_key_bowling, bowl_filter_dict))

        # if "match_type" in request.json:
        #     match_type = request.json['match_type']
        #     if match_type.upper() == 'ODI' or match_type.upper() == 'TEST':
        #         join_data_df = join_data[
        #             join_data['competition_name'].str.upper() == match_type.upper()]
        #     else:
        #         join_data_df = join_data[(join_data['competition_name'].str.upper() != 'ODI') & (
        #                 join_data['competition_name'].str.upper() != 'TEST')]
        # else:
        #     join_data_df = join_data

        if filters:
            batsmanAggDetailSQL = '''
            select 
              player_id, 
              player_name, 
              cast(
                sum(runs) as int
              ) as runs_scored, 
              count(distinct match_id) as matches, 
              round(
                coalesce(
                  (
                    (
                      sum(runs)* 100.00
                    )/ sum(balls)
                  ), 
                  0.0
                ), 
                2
              ) as strike_rate, 
              cast(
                sum(balls) as int
              ) as balls_played, 
              cast(
                max(runs) as int
              ) as best_score, 
              player_image_url 
            from 
              (
                select 
                  batsman_name as player_name, 
                  batsman_image_url as player_image_url, 
                  batsman_team_name as bat_current_team, 
                  batsman_id as player_id, 
                  innings, 
                  round(
                    sum(ball_runs)
                  ) as runs, 
                  round(
                    (
                      count(jd.ball_number)- (
                      sum(jd.is_wide)
                      )
                    )
                  ) as balls, 
                  match_id, 
                  competition_name 
                from 
                  join_data jd ''' + \
                                  filters.replace('player_id', 'batsman_id').replace('team_id',
                                                                                     'batsman_team_id').replace(
                                      'pressure_cat', 'batsman_pressure_cat'
                                  ) + f''' 
                group by 
                  batsman_id, 
                  match_id, 
                  innings, 
                  batsman_name, 
                  batsman_team_name, 
                  competition_name, 
                  batsman_image_url
              ) 
            group by 
              player_id, 
              player_name, 
              player_image_url 
            having 
              count(distinct match_id) >= {min_innings} 
              and cast(
                sum(balls) as int
              ) >= {total_balls}
            '''

            outBatsmanSQL = '''
            select 
              out_batsman_id, 
              round(
                count(out_batsman_id)
              ) as wicket_cnt 
            from 
              join_data ''' + filters.replace('player_id', 'out_batsman_id').replace('team_id',
                                                                                         'batsman_team_id').replace(
                'player_skill', 'out_batsman_skill').replace('pressure_cat', 'batsman_pressure_cat') + \
                            ''' 
                            and out_batsman_id <>-1 
                            and innings not in (3, 4) 
            group by 
              out_batsman_id
            '''

            bowlerAggDetailSQL = '''
            select 
              player_name, 
              player_id, 
              count(distinct match_id) as matches, 
              cast(
                sum(wickets) as int
              ) as wickets, 
              cast(
                sum(runs) as int
              ) as runs, 
              round(
                case when (
                  sum(balls)% 6
                )== 0 then (
                  (
                    sum(balls)* 1.00
                  )/ 6
                ) else (
                  sum(balls)/ 6
                )+(
                  (
                    sum(balls)% 6
                  )/ 10.00
                ) end, 
                2
              ) as overs, 
              round(
                coalesce(
                  (
                    (
                      sum(balls)* 1.00
                    )/ sum(wickets)
                  ), 
                  0.0
                ), 
                2
              ) as strike_rate, 
              cast(
                sum(dot_balls) as int
              ) as dot_balls, 
              round(
                coalesce(
                  (
                    (
                      sum(runs)* 1.00
                    )/(
                      sum(balls)* 1.00 / 6
                    )
                  ), 
                  0.0
                ), 
                2
              ) as economy, 
              player_image_url 
            from 
              (
                select 
                  match_id, 
                  bowler_name as player_name, 
                  bowler_id as player_id, 
                  bowler_team_name as team_name, 
                  innings, 
                  competition_name, 
                  cast(
                      (
                        count(ball_number)-(
                          sum(is_wide)+ sum(is_no_ball)
                        )
                      ) as int
                    ) as balls, 
                  cast(
                      sum(
                        case when is_leg_bye = 1 then 0 when is_bye = 1 then 0 else runs end
                      ) as int
                  ) as runs, 
                  cast(
                      sum(
                        case when is_wicket = 1 
                        and is_bowler_wicket = 1 then 1 else 0 end
                      ) as int
                    ) as wickets, 
                  cast(
                      sum(is_dot_ball) as int
                    ) as dot_balls, 
                  bowler_image_url as player_image_url 
                from 
                  join_data ''' + bowl_filters.replace('player_id', 'bowler_id').replace('team_id',
                                                                                         'bowler_team_id').replace(
                'pressure_cat', 'bowler_pressure_cat'
            ) + f''' 
                group by 
                  match_id, 
                  bowler_id,  
                  bowler_team_name, 
                  competition_name, 
                  bowler_name, 
                  innings, 
                  bowler_image_url
              ) 
            group by 
              player_id, 
              player_name, 
              player_image_url 
            having 
              count(distinct match_id) >= {min_innings} 
              and cast(
                sum(balls) as int
              ) >= {total_balls}
            '''

        else:
            batsmanAggDetailSQL = '''
            select 
              player_id, 
              player_name, 
              cast(
                sum(runs) as int
              ) as runs_scored, 
              count(distinct match_id) as matches, 
              round(
                coalesce(
                  (
                    (
                      sum(runs)* 100.00
                    )/ sum(balls)
                  ), 
                  0.0
                ), 
                2
              ) as strike_rate, 
              cast(
                sum(balls) as int
              ) as balls_played, 
              cast(
                max(runs) as int
              ) as best_score, 
              player_image_url 
            from 
              (
                select 
                  batsman_name as player_name, 
                  batsman_image_url as player_image_url, 
                  batsman_team_name as bat_current_team, 
                  batsman_id as player_id, 
                  innings, 
                  round(
                    sum(ball_runs)
                  ) as runs, 
                  round(
                      (
                        count(jd.ball_number)- (
                          sum(jd.is_wide)
                        )
                      )
                  ) as balls, 
                  match_id, 
                  competition_name 
                from 
                  join_data jd 
                group by 
                  batsman_id, 
                  match_id, 
                  innings, 
                  batsman_name, 
                  batsman_team_name, 
                  competition_name, 
                  batsman_image_url
              ) 
            group by 
              player_id, 
              player_name, 
              player_image_url 
            '''

            outBatsmanSQL = '''
            select 
              out_batsman_id, 
              round(
                count(out_batsman_id)
              ) as wicket_cnt 
            from 
              join_data
            where
              out_batsman_id <>-1 
              and innings not in (3, 4) 
            group by 
              out_batsman_id
            '''

            bowlerAggDetailSQL = '''select 
              player_name, 
              player_id, 
              count(distinct match_id) as matches, 
              cast(
                sum(wickets) as int
              ) as wickets, 
              cast(
                sum(runs) as int
              ) as runs, 
              round(
                case when (
                  sum(balls)% 6
                )== 0 then (
                  (
                    sum(balls)* 1.00
                  )/ 6
                ) else (
                  sum(balls)/ 6
                )+(
                  (
                    sum(balls)% 6
                  )/ 10.00
                ) end, 
                2
              ) as overs, 
              round(
                coalesce(
                  (
                    (
                      sum(balls)* 1.00
                    )/ sum(wickets)
                  ), 
                  0.0
                ), 
                2
              ) as strike_rate, 
              cast(
                sum(dot_balls) as int
              ) as dot_balls, 
              round(
                coalesce(
                  (
                    (
                      sum(runs)* 1.00
                    )/(
                      sum(balls)* 1.00 / 6
                    )
                  ), 
                  0.0
                ), 
                2
              ) as economy, 
              player_image_url 
            from 
              (
                select 
                  match_id, 
                  bowler_name as player_name, 
                  bowler_id as player_id, 
                  bowler_team_name as team_name, 
                  innings, 
                  competition_name, 
                  cast(
                      (
                        count(ball_number)-(
                          sum(is_wide)+ sum(is_no_ball)
                        )
                      ) as int
                  ) as balls, 
                  cast(
                      sum(
                        case when is_leg_bye = 1 then 0 when is_bye = 1 then 0 else runs end
                      ) as int
                    ) as runs, 
                  cast(
                      sum(
                        case when is_wicket = 1 
                        and is_bowler_wicket = 1 then 1 else 0 end
                      ) as int
                    ) as wickets, 
                  cast(
                      sum(is_dot_ball) as int
                    ) as dot_balls, 
                  bowler_image_url as player_image_url 
                from 
                  join_data 
                group by 
                  match_id, 
                  bowler_id,  
                  bowler_team_name, 
                  competition_name, 
                  bowler_name, 
                  innings, 
                  bowler_image_url
              ) 
            group by 
              player_id, 
              player_name, 
              player_image_url 
            '''

        finalBatsmanAggDetailSQL = '''select player_id, player_name, runs_scored, matches, strike_rate, cast(coalesce(obd.wicket_cnt, 0) as int) as dismissals, 
        balls_played, player_image_url, round(coalesce((hsd.runs_scored*1.00)/obd.wicket_cnt,0.0),2) as average  
        from batsmanAggDetailDF hsd left join OutBatsmanDf obd on (obd.out_batsman_id=hsd.player_id)'''

        batsmanAggDetailDF = executeQuery(con, batsmanAggDetailSQL, params)
        OutBatsmanDf = executeQuery(con, outBatsmanSQL, params)
        BatsmanStatsDf = executeQuery(con, finalBatsmanAggDetailSQL)
        BowlerStatsDf = executeQuery(con, bowlerAggDetailSQL, bowl_params)

        # filtering data based on Auction filter

        if "auction" in request.json:
            if request.json['auction'] == "Retained":
                retained_players_list = list(readCSV(IPL_RETAINED_LIST_PATH)['player_name'])
                BatsmanStatsDf = BatsmanStatsDf[BatsmanStatsDf['player_name'].isin(retained_players_list)]
                BowlerStatsDf = BowlerStatsDf[BowlerStatsDf['player_name'].isin(retained_players_list)]
            elif request.json['auction'] == "In-Auction":
                in_auction_list = list(readCSV(IPL_AUCTION_LIST_PATH)['player_name'])
                BatsmanStatsDf = BatsmanStatsDf[BatsmanStatsDf['player_name'].isin(in_auction_list)]
                BowlerStatsDf = BowlerStatsDf[BowlerStatsDf['player_name'].isin(in_auction_list)]
            elif request.json['auction'] == "Others":
                retained_players_list = list(readCSV(IPL_RETAINED_LIST_PATH)['player_name'])
                in_auction_list = list(readCSV(IPL_AUCTION_LIST_PATH)['player_name'])
                retained_players_list.extend(in_auction_list)
                BatsmanStatsDf = BatsmanStatsDf[~BatsmanStatsDf['player_name'].isin(retained_players_list)]
                BowlerStatsDf = BowlerStatsDf[~BowlerStatsDf['player_name'].isin(retained_players_list)]
            else:
                BatsmanStatsDf = BatsmanStatsDf
                BowlerStatsDf = BowlerStatsDf

        if sort_key != "player_name":
            BatsmanStatsDf = BatsmanStatsDf[BatsmanStatsDf[sort_key] >= 0]
        elif sort_key_bowling != "player_name":
            BowlerStatsDf = BowlerStatsDf[BowlerStatsDf[sort_key_bowling] >= 0]

        if "page" in request.json:
            page = request.json['page']
        else:
            page = 1

        total_count_batting = int(BatsmanStatsDf['player_id'].count())
        total_count_bowling = int(BowlerStatsDf['player_id'].count())

        if "record_count" in request.json:
            record_count = request.json['record_count']
        else:
            record_count = total_count_batting

        response['batting_stats'] = json.loads(BatsmanStatsDf.sort_values(sort_key, ascending=asc)
                                               .iloc[((page - 1) * record_count):(page * record_count)].to_json(
            orient='records'))
        response['bowling_stats'] = json.loads(BowlerStatsDf.sort_values(sort_key_bowling, ascending=asc)
                                               .iloc[((page - 1) * record_count):(page * record_count)].to_json(
            orient='records'))
        response['total_count_batting'] = total_count_batting
        response['total_count_bowling'] = total_count_bowling

        return jsonify(response), logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app_auction.route("/pressureBasedAnalytics", methods=['POST'])
@token_required
def pressureBasedAnalytics():
    logger = get_logger("pressureBasedAnalytics", "pressureBasedAnalytics")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        response = {}

        if "overs" in request.json:
            filter_dict['over_number'] = request.json['overs']

        filters, params = generateWhereClause(filter_dict)

        if filters:
            batsmanAggDetailSQL = '''
            select 
              player_id, 
              player_name, 
              pressure_cat, 
              cast(
                sum(runs) as int
              ) as runs_scored, 
              count(distinct match_id) as matches, 
              round(
                coalesce(
                  (
                    (
                      sum(runs)* 100.00
                    )/ sum(balls)
                  ), 
                  0.0
                ), 
                2
              ) as strike_rate, 
              cast(
                sum(balls) as int
              ) as balls_played 
            from 
              (
                select 
                  batsman_name as player_name, 
                  batsman_id as player_id,
                  batsman_pressure_cat as pressure_cat, 
                  innings, 
                  round(
                    sum(ball_runs)
                  ) as runs, 
                  round(
                    (
                      count(jd.ball_number)- (
                        sum(jd.is_wide)
                      )
                    )
                  ) as balls, 
                  match_id 
                from 
                  join_data jd ''' + \
                                              filters.replace(' player_id ', ' batsman_id ').replace(' team_id ',
                                                                                                 ' batsman_team_id ').replace(
                                                  ' pressure_cat ', ' batsman_pressure_cat '
                                              ) + f''' 
                group by 
                  batsman_id, 
                  match_id, 
                  innings, 
                  batsman_name,
                  batsman_pressure_cat
              ) 
            group by 
              player_id, 
              player_name, 
              pressure_cat
            '''

            outBatsmanSQL = '''
            select 
              out_batsman_id, 
              batsman_pressure_cat,
              round(
                count(out_batsman_id)
              ) as wicket_cnt 
            from 
              join_data 
            ''' + filters.replace(
                'player_id', 'out_batsman_id'
            ).replace(
                ' team_id', ' batsman_team_id'
            ).replace(
                'player_skill', 'out_batsman_skill'
            ).replace(
                'pressure_cat', 'batsman_pressure_cat'
            ) + ''' and out_batsman_id<>-1 and innings not in (3,4) 
            group by 
              out_batsman_id, 
              batsman_pressure_cat
            '''

            bowlerAggDetailSQL = '''
            select 
              player_name, 
              player_id, 
              pressure_cat, 
              count(distinct match_id) as matches, 
              cast(
                sum(wickets) as int
              ) as wickets, 
              cast(
                sum(runs) as int
              ) as runs, 
              cast(
                sum(balls) as int
              ) as balls, 
              round(
                coalesce(
                  (
                    (
                      sum(runs)* 1.00
                    )/ sum(wickets)
                  ), 
                  0.0
                ), 
                2
              ) as average, 
              round(
                coalesce(
                  (
                    (
                      sum(balls)* 1.00
                    )/ sum(wickets)
                  ), 
                  0.0
                ), 
                2
              ) as strike_rate, 
              round(
                coalesce(
                  (
                    (
                      sum(runs)* 1.00
                    )/(
                      sum(balls)* 1.00 / 6
                    )
                  ), 
                  0.0
                ), 
                2
              ) as economy 
            from 
              (
                select 
                  match_id, 
                  bowler_name as player_name, 
                  bowler_id as player_id, 
                  bowler_pressure_cat as pressure_cat, 
                  innings, 
                  cast(
                    (
                      count(ball_number)-(
                        sum(is_wide)+ sum(is_no_ball)
                      )
                    ) as int
                  ) as balls, 
                  cast(
                    sum(
                      case when is_leg_bye = 1 then 0 when is_bye = 1 then 0 else runs end
                    ) as int
                  ) as runs, 
                  cast(
                    sum(
                      case when is_wicket = 1 
                      and is_bowler_wicket = 1 then 1 else 0 end
                    ) as int
                  ) as wickets, 
                  cast(
                    sum(is_dot_ball) as int
                  ) as dot_balls 
                from 
                  join_data ''' + filters.replace(' player_id ', ' bowler_id ').replace(' team_id ',
                                                                                                     ' bowler_team_id ').replace(
                            ' pressure_cat ', ' bowler_pressure_cat ') + ''' 
                group by 
                  match_id, 
                  bowler_id, 
                  bowler_name, 
                  innings,
                  bowler_pressure_cat
              ) 
            group by 
              player_id, 
              player_name, 
              pressure_cat
            '''
        else:
            batsmanAggDetailSQL = ''''
            select 
              player_id, 
              player_name, 
              pressure_cat, 
              cast(
                sum(runs) as int
              ) as runs_scored, 
              count(distinct match_id) as matches, 
              round(
                coalesce(
                  (
                    (
                      sum(runs)* 100.00
                    )/ sum(balls)
                  ), 
                  0.0
                ), 
                2
              ) as strike_rate, 
              cast(
                sum(balls) as int
              ) as balls_played 
            from 
              (
                select 
                  batsman_name as player_name, 
                  batsman_id as player_id,
                  batsman_pressure_cat as pressure_cat, 
                  innings, 
                  round(
                    sum(ball_runs)
                  ) as runs, 
                  round(
                    (
                      count(jd.ball_number)- (
                        sum(jd.is_wide)
                      )
                    )
                  ) as balls, 
                  match_id 
                from 
                  join_data jd 
                group by 
                  batsman_id, 
                  match_id, 
                  innings, 
                  batsman_name,
                  batsman_pressure_cat
              ) 
            group by 
              player_id, 
              player_name, 
              pressure_cat
            '''

            outBatsmanSQL = '''
            select 
              out_batsman_id, 
              batsman_pressure_cat,
              round(
                count(out_batsman_id)
              ) as wicket_cnt 
            from 
              join_data 
            where
              out_batsman_id <> -1 and innings not in (3,4) 
            group by 
              out_batsman_id,
              batsman_pressure_cat
            '''

            bowlerAggDetailSQL = '''
            select 
              player_name, 
              player_id, 
              pressure_cat, 
              count(distinct match_id) as matches, 
              cast(
                sum(wickets) as int
              ) as wickets, 
              cast(
                sum(runs) as int
              ) as runs, 
              cast(
                sum(balls) as int
              ) as balls, 
              round(
                coalesce(
                  (
                    (
                      sum(runs)* 1.00
                    )/ sum(wickets)
                  ), 
                  0.0
                ), 
                2
              ) as average, 
              round(
                coalesce(
                  (
                    (
                      sum(balls)* 1.00
                    )/ sum(wickets)
                  ), 
                  0.0
                ), 
                2
              ) as strike_rate, 
              round(
                coalesce(
                  (
                    (
                      sum(runs)* 1.00
                    )/(
                      sum(balls)* 1.00 / 6
                    )
                  ), 
                  0.0
                ), 
                2
              ) as economy 
            from 
              (
                select 
                  match_id, 
                  bowler_name as player_name, 
                  bowler_id as player_id, 
                  bowler_pressure_cat as pressure_cat, 
                  innings, 
                  cast(
                    (
                      count(ball_number)-(
                        sum(is_wide)+ sum(is_no_ball)
                      )
                    ) as int
                  ) as balls, 
                  cast(
                    sum(
                      case when is_leg_bye = 1 then 0 when is_bye = 1 then 0 else runs end
                    ) as int
                  ) as runs, 
                  cast(
                    sum(
                      case when is_wicket = 1 
                      and is_bowler_wicket = 1 then 1 else 0 end
                    ) as int
                  ) as wickets, 
                  cast(
                    sum(is_dot_ball) as int
                  ) as dot_balls 
                from 
                  join_data 
                group by 
                  match_id, 
                  bowler_id, 
                  bowler_name, 
                  innings,
                  bowler_pressure_cat
              ) 
            group by 
              player_id, 
              player_name, 
              pressure_cat
            '''
        batsmanAggDetail = executeQuery(con, batsmanAggDetailSQL, params)
        outBatsman = executeQuery(con, outBatsmanSQL, params)

        batsmanAggDetailFinalSQL = '''
        select 
          distinct
          player_id,
          player_name,
          runs_scored, 
          balls_played,
          strike_rate, 
          cast(coalesce(odf.wicket_cnt, 0) as int) as dismissals,
          pressure_cat, 
          round(
            coalesce(runs_scored / wicket_cnt, 0), 
            2
          ) as batting_average, 
        from 
          batsmanAggDetail pdf 
          left join outBatsman odf on (
            odf.out_batsman_id = pdf.player_id 
            and odf.batsman_pressure_cat = pdf.pressure_cat
          )
        '''

        response['batting_stats'] = executeQuery(con, batsmanAggDetailFinalSQL).to_dict(orient='records')
        response['bowling_stats'] = executeQuery(con, bowlerAggDetailSQL, params).to_dict(orient='records')

        return jsonify(response), logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))

generate_api_function(open_api_spec, app_auction, '/perfVsDiffBowlersAndBatsman', 'post', perfVsDiffBowlersAndBatsman, 'perfVsDiffBowlersAndBatsman')
generate_api_function(open_api_spec, app_auction, '/performanceByPositionAndPhase', 'post', performanceByPositionAndPhase, 'performanceByPositionAndPhase')
generate_api_function(open_api_spec, app_auction, '/latestPlayerPerformances', 'post', latestPlayerPerformances, 'latestPlayerPerformances')
generate_api_function(open_api_spec, app_auction, '/pressureIndexDashboard', 'post', pressureIndexDashboard, 'pressureIndexDashboard')
generate_api_function(open_api_spec, app_auction, '/pressureBasedAnalytics', 'post', pressureBasedAnalytics, 'pressureBasedAnalytics')