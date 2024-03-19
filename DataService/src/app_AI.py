import json
import re

from flask import Blueprint, Response, request, jsonify
from marshmallow import ValidationError
from werkzeug.exceptions import HTTPException

from DataIngestion.utils.helper import readCSV
from DataService.app_config import IPL_RETAINED_LIST_PATH, IPL_AUCTION_LIST_PATH
from DataService.src import *
from DataService.utils.contribution_score import getBatsmanAggContributionDF, getBowlerAggContributionDF, \
    getAllRounderAggContributionDF, getBatsmanContributionDF, getBowlerContributionDF, getAllRounderContributionDF
from DataService.utils.helper import filtersAI, generateWhereClause, get_optimal_squad, dropFilter, validateRequest, \
    globalFilters, filters_cs, generate_api_function, open_api_spec
from DataService.utils.matchup_data_simulation import get_matchup_calculation
from common.authentication.auth import token_required

app_ai = Blueprint("app_ai", __name__)
open_api_spec = open_api_spec()


# @app_ai.route("/getPlayerBowlingStats", methods=['POST'])
@token_required
def getPlayerBowlingStats():
    logger = get_logger("getPlayerBowlingStats", "getPlayerBowlingStats")
    try:
        filter_dict = filtersAI()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        map_dict = {"player_id": ["player_id", "player_name"], "batsman_id": ["batsman_id", "batsman"]}
        output_keys = sorted(
            set([item for key in filter_dict for item in (map_dict[key] if key in map_dict else [key])]))
        key_cols = ", ".join(output_keys)
        filters, params = generateWhereClause(filter_dict)

        if filters:
            bowler_stats_df = executeQuery(con, '''select ''' + key_cols + ''', match_date, match_name, 
    cast(sum(total_balls_bowled) as int) as balls,
    cast(sum(total_runs_conceded) as int) as runs,
    round(coalesce(((sum(total_runs_conceded)*1.00)/(sum(total_balls_bowled)*1.00/6)),0.0),2) as economy
     from bowler_stats_data ''' + filters + ''' group by match_date, match_name,''' + key_cols, params)

            return bowler_stats_df.to_json(orient='records'), 200, logger.info("Status - 200")
        else:
            return []
    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app_ai.route("/getBatsmanStats", methods=['POST'])
# @token_required
# def getBatsmanStats():
#     logger = get_logger("getBatsmanStats", "getBatsmanStats")
#     try:
#         filter_dict = filtersAI()
#     except ValidationError as e:
#         logger.error(e.messages)
#         logger.error(e.valid_data)
#         raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
#     try:
#         key_cols = ','.join([key for key, value in filter_dict.items()])
#
#         filters, params = generateWhereClause(filter_dict)
#
#         if "match_date" in request.json:
#             match_date = request.json.get("match_date")
#             match_id = request.json.get("match_id")
#             overs = request.json.get("overs", [0])
#             filters = filters + f''' and case when match_date < ?
#         then overs in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)
#                     when match_id=?  then overs  in (''' + ','.join('?' * len(overs)) + ''') end '''
#             params.extend([match_date, match_id])
#             params.extend(overs)
#
#         if filters:
#             batsman_stats_query = '''select ''' + key_cols + ''', cast(sum(num_dismissals) as int) as
#         num_dismissals, cast(sum(num_dots) as int) as num_dots, cast(sum(num_singles) as int) as num_singles,
#         cast(sum(num_doubles) as int) as num_doubles, cast(sum(num_triples) as int) as num_triples,
#         cast(sum(num_fours) as int) as num_fours, cast(sum(coalesce(num_sixes,0)) as int) as num_sixes,
#         count(distinct match_id) as innings_played, count(distinct match_id)- cast(sum(num_dismissals) as int) as not_outs,
#          cast(sum(balls_faced) as int) as balls_faced, cast(sum(case when stadium_name in ('SHARJAH CRICKET STADIUM','MA CHIDAMBARAM STADIUM')
#     then (1.3*runs) else runs end) as int) as total_runs,
#         round(coalesce(((sum(runs)*100.00)/sum(balls_faced)),0.0),2) as strike_rate,
#         round(coalesce((sum(runs)*1.00)/sum(num_dismissals),0.0),2) as batting_average from batsman_stats_data ''' + filters \
#                                   + ''' group by ''' + key_cols
#
#             batsman_stats_df = executeQuery(con, batsman_stats_query, params)
#
#             return batsman_stats_df.to_json(orient='records'), 200, logger.info("Status - 200")
#         else:
#             return []
#     except Exception as e:
#         logger.info(e)
#         raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))
#
#
# @app_ai.route("/getBowlerStats", methods=['POST'])
# @token_required
# def getBowlerStats():
#     logger = get_logger("getBowlerStats", "getBowlerStats")
#     try:
#         filter_dict = filtersAI()
#     except ValidationError as e:
#         logger.error(e.messages)
#         logger.error(e.valid_data)
#         raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
#     try:
#         key_cols = ','.join([key for key, value in filter_dict.items()])
#         filters, params = generateWhereClause(filter_dict)
#
#         if "match_date" in request.json:
#             match_date = request.json.get("match_date")
#             match_id = request.json.get("match_id")
#             overs = request.json.get("overs", [0])
#             filters = filters + f''' and case when match_date < ?
#             then overs in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)
#                         when match_id=?  then overs  in (''' + ','.join('?' * len(overs)) + ''') end '''
#             params.extend([match_date, match_id])
#             params.extend(overs)
#
#         if filters:
#             bowler_stats_df = executeQuery(con, '''select ''' + key_cols + ''', cast(sum(num_singles_conceded) as int) as
#     num_singles_conceded, cast(sum(num_doubles_conceded) as int) as num_doubles_conceded,
#     cast(sum(num_triples_conceded) as int) as num_triples_conceded,
#     cast(sum(num_fours_conceded) as int) as num_fours_conceded,
#     cast(sum(num_sixes_conceded) as int) as num_sixes_conceded, cast(sum(num_dots_bowled) as int) as num_dots_bowled,
#     cast(sum(num_extras_conceded) as int) as num_extras_conceded,cast(sum(total_balls_bowled) as int) as total_balls_bowled,
#     cast(sum(case when stadium_name in ('SHARJAH CRICKET STADIUM','MA CHIDAMBARAM STADIUM')
#     then (1.3*total_runs_conceded) else total_runs_conceded end) as int) as total_runs_conceded,
#     cast(sum(no_balls) as int) as no_balls,
#     cast(sum(total_wickets_taken) as int) as total_wickets_taken, cast(sum(wides) as int) as wides,
#     round(coalesce(((sum(total_balls_bowled)*1.00)/sum(total_wickets_taken)),0),2)
#     as bowling_strike_rate,round(coalesce(((sum(total_runs_conceded)*1.00)/(sum(total_balls_bowled)*1.00/6)),0.0),2) as bowling_economy,
#     round(coalesce(((sum(total_runs_conceded)*1.00)/sum(total_wickets_taken)),0.0),2)
#     as bowling_average, count(distinct match_id) as innings_played from bowler_stats_data ''' + filters +
#                                            ''' group by ''' + key_cols, params)
#
#             return bowler_stats_df.to_json(orient='records'), 200, logger.info("Status - 200")
#         else:
#             return []
#     except Exception as e:
#         logger.info(e)
#         raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))
#
#
# @app_ai.route("/getBatsmanCareerStats", methods=['POST'])
# @token_required
# def getBatsmanCareerStats():
#     logger = get_logger("getBatsmanCareerStats", "getBatsmanCareerStats")
#     try:
#         filter_dict = filtersAI()
#     except ValidationError as e:
#         logger.error(e.messages)
#         logger.error(e.valid_data)
#         raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
#     try:
#
#         filters, params = generateWhereClause(filter_dict)
#
#         if filters:
#             batsman_stats_query = f'''select * from batsman_stats_data {filters};'''
#         else:
#             batsman_stats_query = f'''select * from batsman_stats_data;'''
#
#         batsman_stats_df = executeQuery(con, batsman_stats_query, params)
#
#         return batsman_stats_df.to_json(orient='records'), 200, logger.info("Status - 200")
#
#     except Exception as e:
#         logger.info(e)
#         raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))
#
#
# @app_ai.route("/getBowlerCareerStats", methods=['POST'])
# @token_required
# def getBowlerCareerStats():
#     logger = get_logger("getBowlerCareerStats", "getBowlerCareerStats")
#     try:
#         filter_dict = filtersAI()
#     except ValidationError as e:
#         logger.error(e.messages)
#         logger.error(e.valid_data)
#         raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
#     try:
#         filters, params = generateWhereClause(filter_dict)
#
#         if filters:
#             bowler_stats_sql = f'''select * from bowler_stats_data {filters};'''
#         else:
#             bowler_stats_sql = f'''select * from bowler_stats_data;'''
#
#         bowler_stats_df = executeQuery(con, bowler_stats_sql, params)
#
#         return bowler_stats_df.to_json(orient='records'), 200, logger.info("Status - 200")
#
#     except Exception as e:
#         logger.info(e)
#         raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app_ai.route("/getAvgStay", methods=['POST'])
# @token_required
# def getAvgStay():
#     logger = get_logger("getAvgStay", "getAvgStay")
#     try:
#         filter_dict = filtersAI()
#     except ValidationError as e:
#         logger.error(e.messages)
#         logger.error(e.valid_data)
#         raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
#     try:
#         filters, params = generateWhereClause(filter_dict)
#
#         if "match_date" in request.json:
#             match_date = request.json.get("match_date")
#             match_id = request.json.get("match_id")
#             overs = request.json.get("overs", [0])
#             filters = filters + f''' and case when match_date < ?
#             then over_number in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)
#                         when match_id=?  then over_number  in (''' + ','.join('?' * len(overs)) + ''') end '''
#             params.extend([match_date, match_id])
#             params.extend(overs)
#
#         if filters:
#             avg_stay_sql = '''select player_name, player_id,count(distinct match_id) as total_innings,
#      cast(round((sum(balls)/count(distinct match_id))) as int) as avg_stay from batsman_overwise_df ''' + filters + \
#                            ''' group by player_name, player_id'''
#         else:
#             params = []
#             avg_stay_sql = '''select player_name, player_id, count(distinct match_id) as total_innings,
#     cast(round((sum(balls)/count(distinct match_id))) as int) as avg_stay from batsman_overwise_df group by player_name,
#      player_id'''
#
#         return executeQuery(con, avg_stay_sql, params).to_json(orient='records'), 200, logger.info("Status - 200")
#
#     except Exception as e:
#         logger.info(e)
#         raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app_ai.route("/getPartnershipStay", methods=['POST'])
@token_required
def getPartnershipStay():
    logger = get_logger("getPartnershipStay", "getPartnershipStay")
    try:
        filter_dict = filtersAI()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        filters, params = generateWhereClause(filter_dict)

        if "match_date" in request.json:
            match_date = request.json.get("match_date")
            filters = filters + f" and match_date < ? "
            params.append(match_date)

        if filters:
            partnership_stay_sql = '''select striker, striker_name, non_striker_name, non_striker, 
        cast(round(((6.00*sum(striker_balls))/sum(partnership_balls))) as int) as ball_percent from partnership_data ''' + \
                                   filters + ''' group by striker, striker_name, non_striker_name, non_striker '''
        else:
            partnership_stay_sql = '''select striker, striker_name, non_striker_name, non_striker, 
                cast(round(((6.00*sum(striker_balls))/sum(partnership_balls))) as int) as ball_percent from partnership_data 
                 group by striker, striker_name, non_striker_name, non_striker '''

        partnership_df = executeQuery(con, partnership_stay_sql, params)
        partnership_df[['striker', 'non_striker']] = partnership_df[['striker', 'non_striker']].astype(int)
        partnership_df = partnership_df.groupby(
            ['striker_name', 'striker', 'non_striker', 'non_striker_name']).agg(
            {'ball_percent': 'mean'}).reset_index()

        return partnership_df.to_json(orient='records'), 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app_ai.route("/getOverwiseBowler", methods=['POST'])
# @token_required
# def getOverwiseBowler():
#     logger = get_logger("getOverwiseBowler", "getOverwiseBowler")
#     try:
#         filter_dict = filtersAI()
#     except ValidationError as e:
#         logger.error(e.messages)
#         logger.error(e.valid_data)
#         raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
#     try:
#         filters, params = generateWhereClause(filter_dict)
#
#         if "match_date" in request.json:
#             match_date = request.json.get("match_date")
#             match_id = request.json.get("match_id")
#             overs = request.json.get("overs", [0])
#             filters = filters + f''' and case when match_date < ?
#             then over_number in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)
#                         when match_id=?  then over_number  in (''' + ','.join('?' * len(overs)) + ''') end '''
#             params.extend([match_date, match_id])
#             params.extend(overs)
#
#         bowler_stats_df = executeQuery(con, '''select player_id, player_name,
#         round(coalesce(((sum(runs)*1.00)/(sum(balls)*1.00/6)),0.0),2) as bowling_economy,
#          count(distinct match_id) as total_matches from bowler_overwise_df ''' + filters + \
#                                        ''' group by player_id, player_name''', params)
#
#         bowler_overwise_stats_df = executeQuery(con, '''select player_id, player_name,over_number,
#         round(coalesce(((sum(runs)*1.00)/(sum(balls)*1.00/6)),0.0),2) as bowling_economy,
#          count(distinct match_id) as total_matches from bowler_overwise_df ''' + filters + \
#                                                 ''' group by player_id, player_name, over_number''', params)
#
#         penalty = 99.0
#         bowlers_list = filter_dict['player_id']
#
#         eco_score, final_output = get_optimal_squad(bowler_overwise_stats_df, bowler_stats_df, bowlers_list, penalty)
#         final_output = pd.DataFrame(final_output, columns=['over', 'player_id', 'flag'])
#         final_output['player'] = final_output[['player_id', 'flag']].apply(
#             lambda x: {"player_id": x['player_id'], "flag": x["flag"]}, axis=1)
#         final_output = final_output.set_index("over")[["player"]].to_dict()["player"]
#
#         if 'data' in request.json:
#             if request.json['data'] == "bowlers":
#                 response = jsonify(final_output)
#             elif request.json['data'] == "economy":
#                 response = dict()
#                 response["economy_score"] = eco_score
#             else:
#                 response = final_output
#         else:
#             response = final_output
#
#         return response, 200, logger.info("Status - 200")
#
#     except Exception as e:
#         logger.info(e)
#         raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app_ai.route("/getTopPlayers", methods=['GET'])
@token_required
def getTopPlayers():
    logger = get_logger("getTopPlayers", "getTopPlayers")
    try:
        req = dict()
        req['team_id'] = int(request.args.get('TeamID'))
        validateRequest(req)
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Bad Request --> Invalid Input!!", 400))
    try:
        team_id = int(request.args.get('TeamID'))

        players_sql = '''select pd.player_id, tpd.player_name, pd.team_id, td.team_name, pd.batting_type, 
        pd.bowling_type, 
            tpd.position, case when pd.player_skill in ('ALLROUNDER', 'WICKETKEEPER') then 'BATSMAN' 
            else pd.player_skill end as skill_name,
            case when pd.player_skill='ALLROUNDER' then 'ALLROUNDER' when pd.player_skill='WICKETKEEPER' then 
            'WICKETKEEPER' else null end as additional_skill, pd.is_captain, pd.player_type, 
            pd.player_image_url as player_image_url,
            pd.bowl_major_type from top_players_data tpd 
            inner join players_data_df pd on (pd.player_name=tpd.player_name and tpd.competition_name = pd.competition_name) 
            inner join teams_data td on (tpd.team=td.team_short_name and tpd.competition_name = td.competition_name) where td.team_id=? 
            order by tpd.position'''

        players_df = executeQuery(con, players_sql, [team_id]) \
            .rename(
            columns={'player_id': 'PlayerID', 'player_name': 'PlayerName', 'team_id': 'TeamID', 'team_name': 'Team',
                     'batting_type': 'BattingType', 'bowling_type': 'BowlingType', 'player_type': 'PlayerType',
                     'additional_skill': 'AdditionalSkill', 'is_captain': 'Is_Captain'})

        response = players_df.to_json(orient='records')

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app_ai.route("/getMatchupCalculation", methods=['POST'])
# @token_required
# def getMatchupCalculation():
#     logger = get_logger("getMatchupCalculation", "getMatchupCalculation")
#     try:
#         request_json = request.json
#         validateRequest(request_json)
#     except ValidationError as e:
#         logger.error(e.messages)
#         logger.error(e.valid_data)
#         raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
#
#     include_batsman, a, b, c, d, e = None, None, None, None, None, None
#
#     # Default Exclude Players
#     exclude_bowlers = ['Arjun Tendulkar', 'Arshad Khan', 'Dewald Brevis', 'Rahul Buddhi',
#                        'Rohit Sharma', 'Sanjay Yadav', 'Jofra Archer', 'Tim David']
#
#     logger.info(f"request ---> {request.json}")
#     try:
#
#         if request.json:
#
#             if "include_batsman" in request.json:
#                 include_batsman = request.json['include_batsman']
#                 # logger.info('include_batsman {0}'.format(include_batsman))
#             if "best_performing" in request.json:
#                 best_list = request.json['best_performing']
#             else:
#                 best_list = []
#
#             if "worst_performing" in request.json:
#                 worst_list = request.json['worst_performing']
#             else:
#                 worst_list = []
#
#             if "playing_xi" in request.json:
#                 playing_xi = request.json['playing_xi']
#             else:
#                 playing_xi = []
#                 # logger.info('best_performing {0}'.format(best_list))
#
#             if "match_date" in request.json:
#                 match_date = request.json['match_date']
#             else:
#                 match_date = None
#
#             if "home_team" in request.json:
#                 home_team = request.json['home_team']
#             else:
#                 home_team = "MI"
#
#             if "start_over" in request.json:
#                 start_over = request.json['start_over']
#             else:
#                 start_over = 0
#
#             # changes for impact player
#             if "replaced_over_no" in request.json:
#                 replaced_over_no = request.json['replaced_over_no']
#             else:
#                 replaced_over_no = 0
#
#             if "replaced_player" in request.json:
#                 replaced_player = request.json['replaced_player']
#             else:
#                 replaced_player = None
#
#             if "impact_player" in request.json:
#                 impact_player = request.json['impact_player']
#             else:
#                 impact_player = None
#             #########
#
#             if (include_batsman == None):
#                 raise Exception('include_batsman not specified')
#             else:
#                 if (replaced_over_no == 0):
#                     a, b, c, d, e, f, batting_order = get_matchup_calculation(include_batsman, exclude_bowlers,
#                                                                               include_bowlers=playing_xi,
#                                                                               best_list=best_list,
#                                                                               worst_list=worst_list,
#                                                                               match_date=match_date,
#                                                                               home_team=home_team,
#                                                                               start_over=start_over)
#                     response = {
#                         'eco': json.loads(re.sub(r'\bnull\b', '\"\"', a.to_json(orient='records'))),
#                         'wkt': json.loads(re.sub(r'\bnull\b', '\"\"', b.to_json(orient='records'))),
#                         'combined': json.loads(re.sub(r'\bnull\b', '\"\"', c.to_json(orient='records'))),
#                         'best': [json.loads(re.sub(r'\bnull\b', '\"\"', x.to_json(orient='records'))) for x in d],
#                         'worst': [json.loads(re.sub(r'\bnull\b', '\"\"', x.to_json(orient='records'))) for x in e],
#                         'bowler_class': json.loads(re.sub(r'\bnull\b', '\"\"', f.to_json(orient='records'))),
#                         'batting_order': batting_order
#                     }
#                 else:
#                     if (start_over >= replaced_over_no):
#                         include_bowlers = playing_xi
#                         if replaced_player in include_bowlers:
#                             include_bowlers.remove(replaced_player)
#                         if impact_player is not None:
#                             include_bowlers.append(impact_player)
#                         a, b, c, d, e, f, batting_order = get_matchup_calculation(include_batsman, exclude_bowlers,
#                                                                                   include_bowlers=playing_xi,
#                                                                                   best_list=best_list,
#                                                                                   worst_list=worst_list,
#                                                                                   match_date=match_date,
#                                                                                   home_team=home_team,
#                                                                                   start_over=start_over)
#                         response = {
#                             'eco': json.loads(re.sub(r'\bnull\b', '\"\"', a.to_json(orient='records'))),
#                             'wkt': json.loads(re.sub(r'\bnull\b', '\"\"', b.to_json(orient='records'))),
#                             'combined': json.loads(re.sub(r'\bnull\b', '\"\"', c.to_json(orient='records'))),
#                             'best': [json.loads(re.sub(r'\bnull\b', '\"\"', x.to_json(orient='records'))) for x in d],
#                             'worst': [json.loads(re.sub(r'\bnull\b', '\"\"', x.to_json(orient='records'))) for x in e],
#                             'bowler_class': json.loads(re.sub(r'\bnull\b', '\"\"', f.to_json(orient='records'))),
#                             'batting_order': batting_order
#                         }
#                     # print("in else statement")
#                     else:
#                         a, b, c, d, e, f, batting_order = get_matchup_calculation(include_batsman, exclude_bowlers,
#                                                                                   include_bowlers=playing_xi,
#                                                                                   best_list=best_list,
#                                                                                   worst_list=worst_list,
#                                                                                   match_date=match_date,
#                                                                                   home_team=home_team,
#                                                                                   start_over=start_over,
#                                                                                   replaced_over_no=replaced_over_no)
#                         if len(batting_order) < replaced_over_no:
#                             response = {
#                                 'eco': json.loads(re.sub(r'\bnull\b', '\"\"', a.to_json(orient='records'))),
#                                 'wkt': json.loads(re.sub(r'\bnull\b', '\"\"', b.to_json(orient='records'))),
#                                 'combined': json.loads(re.sub(r'\bnull\b', '\"\"', c.to_json(orient='records'))),
#                                 'best': [json.loads(re.sub(r'\bnull\b', '\"\"', x.to_json(orient='records'))) for x in
#                                          d],
#                                 'worst': [json.loads(re.sub(r'\bnull\b', '\"\"', x.to_json(orient='records'))) for x in
#                                           e],
#                                 'bowler_class': json.loads(re.sub(r'\bnull\b', '\"\"', f.to_json(orient='records'))),
#                                 'batting_order': batting_order
#                             }
#                         else:
#                             include_bowlers = playing_xi
#                             if replaced_player in include_bowlers:
#                                 include_bowlers.remove(replaced_player)
#                             if impact_player is not None:
#                                 include_bowlers.append(impact_player)
#                             a1, b1, c1, d, e, f1, batting_order1 = get_matchup_calculation(include_batsman,
#                                                                                            exclude_bowlers,
#                                                                                            include_bowlers=playing_xi,
#                                                                                            best_list=best_list,
#                                                                                            worst_list=worst_list,
#                                                                                            match_date=match_date,
#                                                                                            home_team=home_team,
#                                                                                            start_over=replaced_over_no)
#
#                             c = pd.concat([c, c1])
#                             a = pd.concat([a, a1]).drop_duplicates()
#                             b = pd.concat([b, b1]).drop_duplicates()
#                             f = pd.concat([f, f1]).drop_duplicates()
#                             response = {
#                                 'eco': json.loads(re.sub(r'\bnull\b', '\"\"', a.to_json(orient='records'))),
#                                 'wkt': json.loads(re.sub(r'\bnull\b', '\"\"', b.to_json(orient='records'))),
#                                 'combined': json.loads(re.sub(r'\bnull\b', '\"\"', c.to_json(orient='records'))),
#                                 'best': [json.loads(re.sub(r'\bnull\b', '\"\"', x.to_json(orient='records'))) for x in
#                                          d],
#                                 'worst': [json.loads(re.sub(r'\bnull\b', '\"\"', x.to_json(orient='records'))) for x in
#                                           e],
#                                 'bowler_class': json.loads(re.sub(r'\bnull\b', '\"\"', f.to_json(orient='records'))),
#                                 'batting_order': batting_order
#                                 # 'best1': [json.loads(re.sub(r'\bnull\b', '\"\"', x.to_json(orient='records'))) for x in d1],
#                                 # 'worst1': [json.loads(re.sub(r'\bnull\b', '\"\"', x.to_json(orient='records'))) for x in e1]
#                             }
#
#             return response, 200, logger.info("Status - 200")
#
#     except Exception as e:
#         logger.info(e)
#         raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))
#
#
# @app_ai.route("/matchWiseBatsmanStats", methods=['POST'])
# @token_required
# def matchWiseBatsmanStats():
#     logger = get_logger("matchWiseBatsmanStats", "matchWiseBatsmanStats")
#     try:
#         filter_dict = filtersAI()
#     except ValidationError as e:
#         logger.error(e.messages)
#         logger.error(e.valid_data)
#         raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
#     try:
#
#         pre_filter_keys = ['player_id', 'year', 'player_name']
#         pre_filter_dict = {key: value for key, value in filtersAI().items() if key in pre_filter_keys}
#         pre_key_cols = ','.join([key for key, value in pre_filter_dict.items()])
#         pre_filters, pre_params = generateWhereClause(pre_filter_dict)
#
#         if "match_date" in request.json:
#             match_date = request.json.get("match_date")
#             match_id = request.json.get("match_id")
#             overs = request.json.get("overs", [0])
#             pre_filters = pre_filters + f''' and case when match_date < ?
#             then overs in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)
#                         when match_id=?  then overs  in (''' + ','.join('?' * len(overs)) + ''') end '''
#             pre_params.extend([match_date, match_id])
#             pre_params.extend(overs)
#
#         batsman_match_stats_df = executeQuery(con, '''select match_id, match_name, ''' + pre_key_cols + ''',
#                     cast(sum(runs) as int) as runs, cast(sum(balls_faced) as int) as balls,
#                     cast(sum(wickets) as int) as wickets from batsman_stats_data ''' + pre_filters +
#                                               ''' group by match_id, match_name, ''' + pre_key_cols, pre_params)
#         if 'perf_sort' in request.json:
#             perf_sort = request.json['perf_sort']
#             if perf_sort == 'best':
#                 batsman_match_stats_df = batsman_match_stats_df.sort_values(by=['runs', 'balls'],
#                                                                             ascending=[False, True])
#             elif perf_sort == 'worst':
#                 batsman_match_stats_df = batsman_match_stats_df.sort_values(by=['runs', 'balls'],
#                                                                             ascending=[True, False])
#             elif perf_sort == 'avg':
#                 batsman_match_stats_df = batsman_match_stats_df
#             else:
#                 batsman_match_stats_df = batsman_match_stats_df
#
#         if 'max_rows' in request.json:
#             max_rows = request.json['max_rows']
#             batsman_match_stats_df = batsman_match_stats_df.head(max_rows)
#
#         if 'upper' in request.json:
#             upper = request.json['upper']
#             lower = request.json['lower']
#             batsman_match_stats_df = batsman_match_stats_df.iloc[lower:upper]
#
#         batsman_matches = list(batsman_match_stats_df['match_id'].unique())
#
#         del_keys = ['batsman_id', 'striker', 'non_striker', 'striker_name', 'non_striker_name']
#         filter_dict = dropFilter(del_keys, filter_dict)
#
#         if len(batsman_matches) > 0:
#             filter_dict['match_id'] = [x.item() for x in batsman_matches]
#
#         key_cols = ','.join([key for key, value in filter_dict.items()])
#
#         filters, params = generateWhereClause(filter_dict)
#
#         if filters:
#
#             batsman_stats_df = executeQuery(con, '''select match_name, match_phase, ''' + key_cols + ''',
#             cast(sum(case when stadium_name in ('SHARJAH CRICKET STADIUM','MA CHIDAMBARAM STADIUM')
#     then (1.3*runs) else runs end) as int) as runs, cast(sum(balls_faced) as int) as balls,
#             cast(sum(wickets) as int) as wickets from batsman_stats_data ''' + filters +
#                                             ''' group by match_name, match_phase, ''' + key_cols, params)
#
#             return batsman_stats_df.to_json(orient='records'), 200, logger.info("Status - 200")
#         else:
#             return []
#     except Exception as e:
#         logger.info(e)
#         raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))
#
#
# @app_ai.route("/matchWiseBowlerStats", methods=['POST'])
# @token_required
# def matchWiseBowlerStats():
#     logger = get_logger("matchWiseBowlerStats", "matchWiseBowlerStats")
#     try:
#         filter_dict = filtersAI()
#     except ValidationError as e:
#         logger.error(e.messages)
#         logger.error(e.valid_data)
#         raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
#     try:
#         pre_filter_keys = ['player_id', 'player_name', 'year']
#         pre_filter_dict = {key: value for key, value in filtersAI().items() if key in pre_filter_keys}
#         pre_key_cols = ','.join([key for key, value in pre_filter_dict.items()])
#         pre_filters, pre_params = generateWhereClause(pre_filter_dict)
#
#         if "match_date" in request.json:
#             match_date = request.json.get("match_date")
#             match_id = request.json.get("match_id")
#             overs = request.json.get("overs", [0])
#             pre_filters = pre_filters + f''' and case when match_date < ?
#             then overs in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)
#                         when match_id=?  then overs  in (''' + ','.join('?' * len(overs)) + ''') end '''
#             pre_params.extend([match_date, match_id])
#             pre_params.extend(overs)
#
#         bowler_match_stats_df = executeQuery(con, '''select match_id, match_name, match_phase,''' + pre_key_cols + ''',
#                 cast(sum(total_balls_bowled) as int) as balls,
#         cast(sum(case when stadium_name in ('SHARJAH CRICKET STADIUM','MA CHIDAMBARAM STADIUM')
#     then (1.3*total_runs_conceded) else total_runs_conceded end) as int) as runs,
#         cast(sum(total_wickets_taken) as int) as wickets,
#         cast(sum(wides) as int) as wides, cast(sum(no_balls) as int) as no_balls,
#          round(coalesce(((sum(total_runs_conceded)*1.00)/(sum(total_balls_bowled)*1.00/6)),0.0),2) as bowling_economy
#           from bowler_stats_data ''' + pre_filters +
#                                              ''' group by match_id, match_name, match_phase, ''' + pre_key_cols,
#                                              pre_params)
#
#         if 'perf_sort' in request.json:
#             perf_sort = request.json['perf_sort']
#             if perf_sort == 'best':
#                 bowler_match_stats_df = bowler_match_stats_df.sort_values(by=['wickets', 'bowling_economy'],
#                                                                           ascending=[False, True])
#             elif perf_sort == 'worst':
#                 bowler_match_stats_df = bowler_match_stats_df.sort_values(by=['wickets', 'bowling_economy'],
#                                                                           ascending=[True, False])
#             elif perf_sort == 'avg':
#                 bowler_match_stats_df = bowler_match_stats_df
#             else:
#                 bowler_match_stats_df = bowler_match_stats_df
#
#         if 'max_rows' in request.json:
#             max_rows = request.json['max_rows']
#             bowler_match_stats_df = bowler_match_stats_df.head(max_rows)
#
#         if 'upper' in request.json:
#             upper = request.json['upper']
#             lower = request.json['lower']
#             bowler_match_stats_df = bowler_match_stats_df.iloc[lower:upper]
#
#         bowler_matches = list(bowler_match_stats_df['match_id'].unique())
#
#         del_keys = ['bowler_id', 'striker', 'non_striker', 'striker_name', 'non_striker_name']
#         filter_dict = dropFilter(del_keys, filter_dict)
#
#         if len(bowler_matches) > 0:
#             filter_dict['match_id'] = [x.item() for x in bowler_matches]
#
#         key_cols = ','.join([key for key, value in filter_dict.items()])
#
#         filters, params = generateWhereClause(filter_dict)
#
#         if filters:
#             bowler_stats_df = executeQuery(con, '''select match_name, match_phase,''' + key_cols + ''',
#             cast(sum(total_balls_bowled) as int) as balls,
#     cast(sum(total_runs_conceded) as int) as runs,
#     cast(sum(total_wickets_taken) as int) as wickets,
#     cast(sum(wides) as int) as wides, cast(sum(no_balls) as int) as no_balls,
#      round(coalesce(((sum(total_runs_conceded)*1.00)/(sum(total_balls_bowled)*1.00/6)),0.0),2) as bowling_economy
#      from bowler_stats_data ''' + filters + ''' group by match_name, match_phase, ''' + key_cols, params)
#
#             return bowler_stats_df.to_json(orient='records'), 200, logger.info("Status - 200")
#         else:
#             return []
#     except Exception as e:
#         logger.info(e)
#         raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app_ai.route("/bowlerOverwiseStats", methods=['POST'])
# @token_required
# def bowlerOverwiseStats():
#     logger = get_logger("bowlerOverwiseStats", "bowlerOverwiseStats")
#     try:
#         filter_dict = filtersAI()
#     except ValidationError as e:
#         logger.error(e.messages)
#         logger.error(e.valid_data)
#         raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
#     try:
#         filters, params = generateWhereClause(filter_dict)
#
#         if "match_date" in request.json:
#             match_date = request.json.get("match_date")
#             match_id = request.json.get("match_id")
#             overs = request.json.get("overs", [0])
#             filters = filters + f''' and case when match_date < ?
#             then over_number in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)
#                         when match_id=?  then over_number  in (''' + ','.join('?' * len(overs)) + ''') end '''
#             params.extend([match_date, match_id])
#             params.extend(overs)
#
#         if filters:
#             bowler_stats = '''select player_id, player_name, over_number,
#                 cast(sum(balls) as int) as balls, cast(sum(wickets) as int) as wickets,
#                 round(coalesce(((sum(runs)*1.00)/(sum(balls)*1.00/6)),0.0),2) as bowling_economy,
#                  count(distinct match_id) as total_matches
#                 , cast(sum(case when stadium_name in ('SHARJAH CRICKET STADIUM','MA CHIDAMBARAM STADIUM')
#     then (1.3*runs) else runs end) as int) as runs, cast(sum(no_balls) as int) as no_balls,
#     cast(sum(wides) as int) as wides, round((cast(sum(wickets) as int)*1.00/count(distinct match_id)),2) as wicket_percent
#                   from bowler_overwise_df ''' + filters + ''' group by over_number, player_id, player_name'''
#
#             overwise_bowler_df = executeQuery(con, bowler_stats, params)
#             overwise_bowler_df = overwise_bowler_df.groupby(
#                 ['player_id', 'player_name', 'over_number']
#             ).agg(
#                 {'total_matches': 'sum', 'balls': 'sum',
#                  'runs': 'sum', 'wickets': 'sum',
#                  'wides': 'sum',
#                  'no_balls': 'sum',
#                  'wicket_percent': 'mean',
#                  'bowling_economy': 'mean'
#                  }
#             ).reset_index()
#             if 'total_matches_gt' in request.json:
#                 total_matches_gt = request.json.get('total_matches_gt')
#                 overwise_bowler_df = overwise_bowler_df[overwise_bowler_df['total_matches'] > total_matches_gt]
#
#             return overwise_bowler_df.to_json(orient='records'), 200, logger.info("Status - 200")
#
#     except Exception as e:
#         logger.info(e)
#         raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app_ai.route("/getStayAtCrease", methods=['POST'])
@token_required
def getStayAtCrease():
    logger = get_logger("getStayAtCrease", "getStayAtCrease")
    try:
        filter_dict = filtersAI()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        filters, params = generateWhereClause(filter_dict)

        if "match_date" in request.json:
            match_date = request.json.get("match_date")
            match_id = request.json.get("match_id")
            overs = request.json.get("overs", [0])
            filters = filters + f''' and case when match_date < ? 
            then overs in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20) 
                        when match_id=?  then overs  in (''' + ','.join('?' * len(overs)) + ''') end '''
            params.extend([match_date, match_id])
            params.extend(overs)

        batsman_stay = executeQuery(con, '''select match_name, player_id, player_name, cast(sum(balls_faced) as int) as balls,
        cast(sum(runs) as int) as runs from batsman_stats_data ''' + filters +
                                    ''' group by match_name, player_id, player_name;''', params)

        batsman_stay["player_best_rank"] = \
            batsman_stay.sort_values(by=['runs', 'balls'], ascending=[False, True]).groupby("player_id")[
                "player_id"].rank(
                method="first", ascending=True)
        best_stay = batsman_stay[batsman_stay['player_best_rank'] <= 5][['player_name', 'player_id', 'balls']].groupby(
            ['player_name', 'player_id']).mean().reset_index()
        best_stay['best_stay'] = round(best_stay['balls'])

        avg_stay = batsman_stay[['player_name', 'player_id', 'balls']].groupby(
            ['player_name', 'player_id']).mean().reset_index()
        avg_stay['avg_stay'] = round(avg_stay['balls'])

        final_stay_df = pd.merge(best_stay[['player_id', 'player_name', 'best_stay']],
                                 avg_stay[['player_id', 'avg_stay']],
                                 on='player_id', how='left')

        batsman_stay["player_worst_rank"] = \
            batsman_stay.sort_values(by=['runs', 'balls'], ascending=[True, False]).groupby("player_id")[
                "player_id"].rank(
                method="first", ascending=False)
        worst_stay = batsman_stay[batsman_stay['player_worst_rank'] <= 5][
            ['player_name', 'player_id', 'balls']].groupby(
            ['player_name', 'player_id']).mean().reset_index()
        worst_stay['worst_stay'] = round(worst_stay['balls'])
        final_stay_df = pd.merge(final_stay_df, worst_stay[['player_id', 'worst_stay']], on='player_id', how='left')

        final_stay_df[['best_stay', 'avg_stay', 'worst_stay']] = final_stay_df[
            ['best_stay', 'avg_stay', 'worst_stay']].astype(int)

        return final_stay_df.to_json(orient='records'), 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app_ai.route("/getMatchOverScoreCard", methods=['GET'])
@token_required
def getMatchOverScoreCard():
    logger = get_logger("getMatchOverScoreCard", "getMatchOverScoreCard")
    try:
        req = dict()
        req['match_id'] = int(request.args.get('match_id'))
        validateRequest(req)
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Bad Request --> Invalid Input!!", 400))
    try:
        response = {}
        match_id = request.args.get('match_id')
        first_ball_striker_non_striker = '''select innings, over_number, batsman_id as striker_id, batsman_name as striker_name, non_striker_id, non_striker_name from join_data
        where match_id=? and ball_number = 1 and is_wide=0 and is_no_ball =0 group by match_id, innings, over_number, batsman_id, batsman_name, non_striker_id, non_striker_name'''
        over_data_sql = f''' select innings, over_number, cast(runs as int) as runs, cast(wickets as int) as wickets, 
cast(sum(runs) over (partition by innings order by over_number) as int) as cumulative_runs, 
cast(sum(wickets) over (partition by innings order by over_number) as int) as cumulative_wickets, 
bowler_id, bowler_name, cast(is_batsman_out as int) as is_batsman_out, batsman_name, batsman_id, 
cast(balls_played as int) as balls_played from
(select match_id, innings, overs as over_number, bowler_id, bowler as bowler_name, sum(team_runs) as runs, sum(num_dismissals) as wickets,
sum(num_dismissals) as is_batsman_out, player_name as batsman_name, player_id as batsman_id, sum(balls_faced) as balls_played   
 from batsman_stats_data where match_id=? group by match_id, innings, overs, bowler_id, bowler,
 player_name, player_id order by innings, overs) A'''
        bowler_details = '''select bd.innings,pdf.player_name as bowler,
case when ((sum(total_balls_bowled)*1.00)%6)==0 then ((sum(total_balls_bowled)*1.00)/6) else (sum(cast(total_balls_bowled as int))/6) + 
(((sum(total_balls_bowled)*1.00)%6)/10.00) end as overs_bowled  from  
bowler_stats_data bd left join players_data_df pdf on (pdf.player_id=bd.player_id and bd.competition_name=pdf.competition_name) 
where bd.match_id=? group by bd.innings, pdf.player_name'''
        over_data_df = executeQuery(con, over_data_sql, [match_id])
        first_ball_striker_non_striker_df = executeQuery(con, first_ball_striker_non_striker, [match_id])
        over_data_df['batsman_stats'] = over_data_df[
            ['batsman_name', 'batsman_id', 'is_batsman_out', 'balls_played']].to_dict(orient='records')
        over_data_df['innings'] = over_data_df['innings'].apply(lambda x: "inning_" + str(x))
        first_ball_striker_non_striker_df['innings'] = first_ball_striker_non_striker_df['innings'].apply(lambda x: "inning_" + str(x))
        batsman_over_data_df = over_data_df.groupby(['innings', 'over_number'])['batsman_stats'].agg(list).reset_index()
        bowler_over_data_df = over_data_df.groupby(
            ['innings', 'over_number', 'cumulative_runs', 'cumulative_wickets', 'bowler_id', 'bowler_name']) \
            .agg({'runs': 'sum', 'wickets': 'sum'}).reset_index()
        bowler_over_data_df['bowler_stats'] = bowler_over_data_df[
            ['bowler_id', 'bowler_name', 'runs', 'wickets']].to_dict(orient='records')
        bowler_over_data_df = bowler_over_data_df[
            ['innings', 'over_number', 'bowler_stats', 'cumulative_runs', 'cumulative_wickets', ]].groupby(
            ['innings', 'over_number', 'cumulative_runs', 'cumulative_wickets'])[
            'bowler_stats'].agg(list).reset_index()
        final_over_data = bowler_over_data_df.merge(batsman_over_data_df[['innings', 'over_number', 'batsman_stats']],
                                                    on=['innings', 'over_number'],
                                                    how='inner').merge(first_ball_striker_non_striker_df[
                                                                           ['innings', 'over_number', 'striker_id',
                                                                            'striker_name', 'non_striker_id',
                                                                            'non_striker_name']],
                                                                       on=['innings', 'over_number'], how='left')
        bowler_details_df = executeQuery(con, bowler_details, [match_id]).drop_duplicates()
        bowler_details_df['innings'] = bowler_details_df['innings'].apply(lambda x: "inning_" + str(x))
        bowler_details_df['bowler_details'] = bowler_details_df[['bowler', 'overs_bowled']].to_dict(orient='records')
        response['ball_data'] = final_over_data.groupby('innings') \
            .apply(lambda x: x.set_index('over_number').to_dict(orient='index')).to_dict()
        response.update(bowler_details_df[['innings', 'bowler_details']].groupby("innings").agg(
            {"bowler_details": lambda x: list(x)}).to_dict())
        team1 = executeQuery(con, f'''select coalesce(team_id, -1) as team1, coalesce(team_name, 'NA') as team1_name from batsman_stats_data 
        where match_id=? and innings=1''', [match_id])
        response['team1'] = int(team1.iloc[0, 0])
        response['team1_name'] = team1.iloc[0, 1]
        team2 = executeQuery(con, f'''select coalesce(team_id, -1) as team2, coalesce(team_name, 'NA') as team2_name from batsman_stats_data 
                where match_id=? and innings=2''', [match_id])
        response['team2'] = int(team2.iloc[0, 0])
        response['team2_name'] = team2.iloc[0, 1]
        return response, 200, logger.info("Status - 200")
    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app_ai.route("/overVSBowlerTypeStats", methods=['POST'])
# @token_required
# def overVSBowlerTypeStats():
#     logger = get_logger("overVSBowlerTypeStats", "overVSBowlerTypeStats")
#     try:
#         filter_dict = globalFilters()
#     except ValidationError as e:
#         logger.error(e.messages)
#         logger.error(e.valid_data)
#         raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
#     try:
#         response = {}
#         if request.json:
#             if request.json.get('batsman_team_id'):
#                 filter_dict['batsman_team_id'] = request.json.get('batsman_team_id')
#             if "overs" in request.json:
#                 filter_dict['overs'] = request.json.get('overs')
#
#         filters, params = generateWhereClause(filter_dict)
#         eco_params = params.copy()
#         eco_params.extend(eco_params)
#
#         eco_percent = '''select bowling_type, bsd.overs, round(count(innings)*100.00/cnt.match_cnt,2) as match_percent,
#         round(coalesce(((sum(total_runs_conceded)*1.00)/(sum(total_balls_bowled)*1.00/6)),0.0),2) as economy from bowler_stats_data bsd
#         left join (select count(innings) as match_cnt, overs from bowler_stats_data ''' + filters.replace('season',
#                                                                                                           'year') + ''' and innings not in (3,4) and
#          bowling_type<>'NA' group by overs) cnt on
#          (bsd.overs=cnt.overs) ''' + filters.replace('season', 'year').replace('overs', 'bsd.overs') + ''' and innings not in (3,4) and bowling_type<>'NA'
#          group by bowling_type, bsd.overs, cnt.match_cnt order by bsd.overs'''
#
#         eco_percent_df = executeQuery(con, eco_percent, eco_params)
#
#         response['match_percent'] = eco_percent_df.groupby('overs') \
#             .apply(
#             lambda x: x.set_index('bowling_type')[['match_percent', 'economy']].to_dict(orient='index')).to_dict()
#
#         overwise_runs = '''select bowling_type, bsd.overs, cast(sum(total_runs_conceded) as int) as runs,
#         cast(sum(total_wickets_taken) as int) as wickets from bowler_stats_data bsd
#          ''' + filters.replace('season', 'year') + '''and innings not in (3,4) and bowling_type<>'NA' group by bowling_type, bsd.overs order by
#          bsd.overs '''
#
#         overwise_runs_df = executeQuery(con, overwise_runs, params)
#
#         response['match_stats'] = overwise_runs_df.groupby('overs') \
#             .apply(lambda x: x.set_index('bowling_type')[['runs', 'wickets']].to_dict(orient='index')).to_dict()
#
#         return response, 200, logger.info("Status - 200")
#     except Exception as e:
#         logger.info(e)
#         raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app_ai.route("/getContributionScore", methods=['POST'])
@token_required
def getContributionScore():
    logger = get_logger("getContributionScore", "getContributionScore")
    try:
        filter_dict = filters_cs()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        filters, params = generateWhereClause(filter_dict)
        response = {}
        total_count = 0
        if request.json:

            if "match_phase" in request.json:
                match_phase = request.json['match_phase'][0]
            else:
                match_phase = None

            if "min_innings" in request.json:
                innings = request.json['min_innings']
            else:
                innings = None

            if "asc" in request.json:
                asc = request.json['asc']
            else:
                asc = False

            retained_players_list = list(readCSV(IPL_RETAINED_LIST_PATH)['player_name'])
            retained_players_list = list(map(str.upper, retained_players_list))
            in_auction_list = list(readCSV(IPL_AUCTION_LIST_PATH)['player_name'])
            in_auction_list = list(map(str.upper, in_auction_list))

            if "auction" in request.json:
                auction = request.json['auction']
            else:
                auction = None

            if "stat_type" in request.json:
                stat_type = request.json['stat_type']
                if stat_type.lower() == "batsman":
                    if "sort_key" in request.json:
                        sort_key = request.json['sort_key']
                    else:
                        sort_key = "batting_contribution_score"
                    contribution_df = getBatsmanAggContributionDF(filters, match_phase, innings, params, sort_key, asc, auction, retained_players_list, in_auction_list)
                    total_count = int(contribution_df['player_id'].count())
                elif stat_type.lower() == "bowler":
                    if "sort_key" in request.json:
                        sort_key = request.json['sort_key']
                    else:
                        sort_key = "bowling_contribution_score"
                    contribution_df = getBowlerAggContributionDF(filters, match_phase, innings, params, sort_key, asc,auction, retained_players_list, in_auction_list)
                    total_count = int(contribution_df['player_id'].count())
                elif stat_type.lower() == "allrounder":
                    if "sort_key" in request.json:
                        sort_key = request.json['sort_key']
                    else:
                        sort_key = "batting_contribution_score"
                    contribution_df = getAllRounderAggContributionDF(filters, match_phase, innings, params, sort_key,
                                                                     asc)
                    total_count = int(contribution_df['player_id'].count())
        else:
            contribution_df = getBatsmanAggContributionDF(filters, match_phase=None, innings=None, params=[],
                                                          sort_key="batting_contribution_score", asc=False)
            total_count = int(contribution_df['player_id'].count())

        if "page" in request.json:
            page = request.json['page']
        else:
            page = 1

        if "record_count" in request.json:
            record_count = request.json['record_count']
        else:
            record_count = total_count
        response['total_count'] = total_count
        response['data'] = json.loads(re.sub(r'\bnull\b', '\"\"', contribution_df.iloc[((page - 1) * record_count):(
                page * record_count)].to_json(orient='records')))

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app_ai.route("/getCSFilters", methods=['GET'])
@token_required
def getCSFilters():
    logger = get_logger("getCSFilters", "getCSFilters")
    try:
        response = dict()
        # competition_name_filter = '''select distinct competition_name from contribution_agg_data; '''
        player_type_filter = '''select distinct player_type from contribution_agg_data; '''
        bowling_type_filter = '''select distinct bowling_type from contribution_agg_data where bowling_type<>'NA' ; '''
        position_filter = '''select distinct position from contribution_agg_data where position<>'NA' order by 
        position; '''
        retained_filter = '''select distinct(case when retained=0 then 'Not Retained' else 'Retained' end) as retained from
        contribution_agg_data; '''
        speciality_filter = '''select distinct speciality from contribution_data; '''
        is_won_filter = '''select distinct is_won from contribution_data; '''

        batting_type_filter = '''select distinct batting_type from contribution_agg_data where batting_type<>'NA' ; '''
        in_auction_filter = '''select distinct in_auction from contribution_agg_data; '''

        response["player_type"] = sorted(
            [value for i in executeQuery(con, player_type_filter).to_dict("records") for key, value in i.items()])
        response["bowling_type"] = [value for i in executeQuery(con, bowling_type_filter).to_dict("records") for
                                    key, value
                                    in i.items()]
        position_list = [value for i in executeQuery(con, position_filter).to_dict("records") for key, value in
                         i.items()]
        position_list.remove("Openers")
        position_list = sorted([int(x) for x in position_list])
        position_list.insert(0, "Openers")
        response["position"] = position_list
        response["retained"] = [value for i in executeQuery(con, retained_filter).to_dict("records") for key, value in
                                i.items()]
        response["speciality"] = sorted(
            [value for i in executeQuery(con, speciality_filter).to_dict("records") for key, value in i.items()])

        response["batting_type"] = sorted(
            [value for i in executeQuery(con, batting_type_filter).to_dict("records") for key, value in i.items()])
        response["match_phase"] = ["POWERPLAY", "7-10 OVERS", "11-15 OVERS", "DEATH OVERS"]
        response["in_auction"] = sorted(
            [value for i in executeQuery(con, in_auction_filter).to_dict("records") for key, value in i.items()])
        response["is_won"] = sorted(
            [value for i in executeQuery(con, is_won_filter).to_dict("records") for key, value in i.items()])
        response["auction"] = ["Retained", "In-Auction", "Others"]
        cs_teams_data = f'''select distinct team, team_id, season, competition_name from contribution_data 
                group by team_id, team, season, competition_name order by competition_name, season, team;'''

        cs_teams_df = executeQuery(con, cs_teams_data)
        cs_teams_df['team_details'] = cs_teams_df[['team', 'team_id']].to_dict(orient='records')
        cs_teams_df = cs_teams_df.groupby(['competition_name', 'season'])['team_details'].agg(list).reset_index()
        response['competition_name'] = cs_teams_df.groupby('competition_name') \
            .apply(lambda x: x.set_index('season').to_dict(orient='index')).to_dict()

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app_ai.route("/getPlayerContributionScore", methods=['POST'])
@token_required
def getPlayerContributionScore():
    logger = get_logger("getPlayerContributionScore", "getPlayerContributionScore")
    try:
        filter_dict = filters_cs()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        filters, params = generateWhereClause(filter_dict)

        retained_players_list = list(readCSV(IPL_RETAINED_LIST_PATH)['player_name'])
        retained_players_list = list(map(str.upper, retained_players_list))
        in_auction_list = list(readCSV(IPL_AUCTION_LIST_PATH)['player_name'])
        in_auction_list = list(map(str.upper, in_auction_list))

        if request.json:

            if "match_phase" in request.json:
                match_phase = request.json['match_phase'][0]
            else:
                match_phase = None

            if "auction" in request.json:
                auction = request.json['auction']
            else:
                auction = None

            if "stat_type" in request.json:
                stat_type = request.json['stat_type']
                if stat_type.lower() == "batsman":
                    contribution_df = getBatsmanContributionDF(filters, match_phase, params, auction, retained_players_list, in_auction_list)
                elif stat_type.lower() == "bowler":
                    contribution_df = getBowlerContributionDF(filters, match_phase, params, auction, retained_players_list, in_auction_list)
                elif stat_type.lower() == "allrounder":
                    contribution_df = getAllRounderContributionDF(filters, match_phase, params)
        else:
            contribution_df = getBatsmanContributionDF(filters, retained_players_list= retained_players_list,
                                                       in_auction_list= in_auction_list, match_phase=None, auction=None)

        contribution_df['match_date_form'] = contribution_df['match_date'].apply(
            lambda x: datetime.strptime(x, '%d %b %Y').strftime('%Y-%m-%d'))

        contribution_df['match_rank'] = contribution_df['match_date_form'].rank(method='dense', ascending=False).astype(
            int)

        response = contribution_df.sort_values(by=['match_rank']).drop(['match_rank', 'match_date_form'],
                                                                       axis=1).to_json(orient='records')

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


generate_api_function(open_api_spec, app_ai, '/getPlayerBowlingStats', 'post', getPlayerBowlingStats, 'getPlayerBowlingStats')
generate_api_function(open_api_spec, app_ai, '/getPartnershipStay', 'post', getPartnershipStay, 'getPartnershipStay')
generate_api_function(open_api_spec, app_ai, '/getTopPlayers', 'get', getTopPlayers, 'getTopPlayers')
generate_api_function(open_api_spec, app_ai, '/getStayAtCrease', 'post', getStayAtCrease, 'getStayAtCrease')
generate_api_function(open_api_spec, app_ai, '/getMatchOverScoreCard', 'get', getMatchOverScoreCard, 'getMatchOverScoreCard')
generate_api_function(open_api_spec, app_ai, '/getCSFilters', 'get', getCSFilters, 'getCSFilters')
generate_api_function(open_api_spec, app_ai, '/getContributionScore', 'post', getContributionScore, 'getContributionScore')
generate_api_function(open_api_spec, app_ai, '/getPlayerContributionScore', 'post', getPlayerContributionScore, 'getPlayerContributionScore')
