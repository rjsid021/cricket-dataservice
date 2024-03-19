import ast
import sys
import warnings
from flask import Flask, request, jsonify, Response
from flask_cors import CORS
from werkzeug.exceptions import HTTPException
from DataIngestion import load_timestamp
from common.dao.fetch_db_data import getMaxId
from common.dao.insert_data import insertToDB
from common.db_config import DB_NAME

sys.path.append("./../../")
sys.path.append("./")
from DataIngestion.utils.helper import generateSeq
from DataService.src import *
import numpy as np
from DataIngestion.config import QUERY_FEEDBACK_TABLE_NAME, PLAYER_MAPPER_TABLE_NAME, PLAYER_MAPPER_KEY_COL, BUILD_ENV
from DataService.utils.helper import globalFilters, generateWhereClause, dropFilter, executeQuery, validateRequest, \
    getListIndexDF, getJoinCondition, transform_matchName, open_api_spec, generate_api_function
from common.authentication.auth import token_required
from marshmallow import ValidationError
from DataService.app_config import HOST, PORT, ALLOWED_HOSTS
from log.log import get_logger
from app_AI import app_ai
from DataService.src.app_gps import app_gps
from DataService.src.app_smartabase import app_smartabase
from DataService.src.app_auction import app_auction
from flask_restful import Api
from flasgger import Swagger

app = Flask(__name__)
app.config['SWAGGER'] = {
    'title': 'Cricket Data Service API Doc',
}


if BUILD_ENV.upper() in ["PROD", "REPLICA", "QA"]:
    CORS(app, resources={r"*": {
        "origins": ALLOWED_HOSTS,
        "supports_credentials": True,
        "allow_headers":  ["Content-Type", "Authorization"],
        "methods": ["GET", "POST", "PUT", "DELETE"]
    }})
else:
    CORS(app, support_credentials=True)

warnings.simplefilter(action='ignore', category=FutureWarning)

app.register_blueprint(app_ai)
app.register_blueprint(app_gps)
app.register_blueprint(app_auction)
app.register_blueprint(app_smartabase)
pd.set_option('display.max_columns', 50)

api = Api(app)

swagger = Swagger(app)

open_api_spec = open_api_spec()


@app.before_request
def check_origin():
    logger = get_logger("check_origin", "check_origin")
    if BUILD_ENV.upper() in ["PROD", "REPLICA", "QA"]:
        if 'Origin' in request.headers:
            origin = request.headers['Origin']
            if origin not in ALLOWED_HOSTS:
                logger.error("Origin not allowed")
                return jsonify({'error': 'Origin not allowed'}), 403


@app.after_request
def after_request(response):
    response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Content-Security-Policy'] = "default-src 'self'"
    response.headers['X-Frame-Options'] = 'SAMEORIGIN'
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0, private'

    if BUILD_ENV.upper() in ["PROD", "REPLICA", "QA"]:
        if 'Origin' in request.headers:
            origin = request.headers['Origin']
            if origin in ALLOWED_HOSTS:
                response.headers['Access-Control-Allow-Origin'] = origin
                response.headers[
                    'Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
                response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE'

    return response


def home():
    return jsonify(["home"])


@token_required
def batsmanType():
    logger = get_logger("batsmanType", "batsmanType")
    try:
        batsman = executeQuery(con, '''select distinct batting_type as batsman from players_data where batting_type<>'NA' and batting_type<>''
         order by batting_type''')
        response = batsman['batsman'].to_json(orient='records')
        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/playerSkill")
@token_required
def playerSkill():
    logger = get_logger("playerSkill", "playerSkill")
    try:
        player_skill = executeQuery(
            con,
            '''
            select 
              distinct player_skill as player_skill 
            from 
              players_data 
            where 
              player_skill <> 'NA' 
            order by 
              player_skill
            '''
        )
        response = player_skill['player_skill'].to_json(orient='records')
        return response, 200, logger.info("Status - 200")
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/bowlerType")
@token_required
def bowlerType():
    logger = get_logger("bowlerType", "bowlerType")
    try:
        bowler = executeQuery(con, '''select distinct bowling_type as bowler from players_data where bowling_type<>'NA' and bowling_type<>'' 
         order by bowling_type''')
        response = bowler['bowler'].to_json(orient='records')
        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/seasons")
@token_required
def seasons():
    logger = get_logger("seasons", "seasons")
    try:
        season = executeQuery(con, '''select distinct season from seasons_data order by season desc''')
        response = season['season'].to_json(orient='records')
        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/getGround")
@token_required
def getGround():
    logger = get_logger("getGround", "getGround")
    try:
        venue = executeQuery(con, '''select venue_id as VenueID, stadium_name as venue from venue_data 
        order by stadium_name''')
        response = venue.to_json(orient='records')
        return response, 200, logger.info("Status - 200")
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/getTeams")
@token_required
def getTeams():
    logger = get_logger("getTeams", "getTeams")
    try:
        req = dict()
        req['competition_name'] = ast.literal_eval(request.args.get('competition_name'))
        validateRequest(req)
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        teams_season_df = teams_season_data.rename(
            columns={
                "team_id": "TeamID",
                "team_name": "Team",
                "team_short_name": "TeamShortName"
            }
        )

        if "competition_name" in request.args:
            competition_name = request.args.get('competition_name')
            competition_name = ast.literal_eval(competition_name)
            if type(competition_name) == list:
                teams_season_df = teams_season_df[teams_season_df['competition_name'].isin(competition_name)]
            else:
                teams_season_df = teams_season_df[teams_season_df['competition_name'] == competition_name]

        response = teams_season_df.sort_values('Team', ascending=True).to_json(orient='records')
        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/getPlayersForTeam")
@token_required
def getPlayersForTeam():
    logger = get_logger("getPlayersForTeam", "getPlayersForTeam")
    try:
        req = dict()
        req['team_id'] = int(request.args.get('TeamID'))
        req['season'] = int(request.args.get('season', 2022))
        req['competition_name'] = request.args.get('competition_name', 'IPL')
        validateRequest(req)
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Bad Request --> Invalid Input!!", 400))
    try:
        team_id = request.args.get('TeamID')
        if "season" in request.args:
            season = request.args.get('season')
        else:
            seasons_df = executeQuery(
                con,
                '''select coalesce(max(season), 2022) as season from seasons_data where team_id=?''',
                [team_id]
            )
            season = int(seasons_df["season"].iloc[0])

        players_sql = '''
        select 
          pd.player_id, 
          pd.player_name, 
          pd.team_id, 
          td.team_name, 
          pd.batting_type, 
          pd.bowling_type, 
          pd.player_skill as skill_name,
          case when pd.player_skill = 'ALLROUNDER' then 'ALLROUNDER' when pd.player_skill = 'WICKETKEEPER' then 'WICKETKEEPER' else null end as additional_skill, 
          pd.is_captain, 
          pd.player_type, 
          pd.season, 
          pd.bowl_major_type, 
          pd.player_image_url 
        from 
          players_data pd 
          inner join teams_data td on (pd.team_id = td.team_id) 
        where 
          pd.team_id = ? 
          and pd.season = ?
        '''
        params = [team_id, season]

        if "competition_name" in request.args:
            competition_name = request.args.get('competition_name')
            players_sql = players_sql + f"and pd.competition_name=?; "
            params.append(competition_name)

        players_df = executeQuery(con, players_sql, params).drop_duplicates(
            subset=['player_id', 'player_name', 'team_id', 'team_name'], keep='last'
        ).rename(
            columns={
                'player_id': 'PlayerID',
                'player_name': 'PlayerName',
                'team_id': 'TeamID',
                'team_name': 'Team',
                'batting_type': 'BattingType',
                'bowling_type': 'BowlingType',
                'player_type': 'PlayerType',
                'additional_skill': 'AdditionalSkill',
                'is_captain': 'Is_Captain'
            }
        )
        return players_df.to_json(orient='records'), 200, logger.info("Status - 200")
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/highlightStatsCard", methods=['POST'])
# @token_required
# def highlightStatsCard():
#     logger = get_logger("highlightStatsCard", "highlightStatsCard")
#     response = {}
#     try:
#         filter_dict = globalFilters()
#     except ValidationError as e:
#         logger.error(e.messages)
#         logger.error(e.valid_data)
#         raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
#     try:
#         filters, params = generateWhereClause(filter_dict)
#
#         if "min_innings" in request.json:
#             min_innings = request.json['min_innings']
#         else:
#             min_innings = 1
#
#         if filters:
#             highest_wicket_taker = '''select player_name, cast(sum(wickets) as int) as total_wickets, count(distinct match_id) as innings_played, player_image_url from
#             bowler_overwise_df ''' + filters + ''' group by player_id, player_name, player_image_url''' + f" having count(distinct match_id) >= {min_innings} order by total_wickets desc limit 1"
#
#             best_bowling_average = '''select player_name, bowl_current_team,
#             round(coalesce(((sum(runs)*1.00)/sum(wickets)),0.0),2)
#             as bowling_average, count(distinct match_id) as innings_played, player_image_url from bowler_overwise_df ''' + filters + ''' group by player_id, player_name,  bowl_current_team, player_image_url
#             having sum(balls)>10 and bowling_average>0''' + f" and count(distinct match_id) >=  {min_innings} order by bowling_average asc limit 1"
#
#             out_batsman_sql = '''select out_batsman_id, round(count(out_batsman_id)) as wicket_cnt
#                     from join_data ''' + filters.replace('player_id', 'out_batsman_id').replace(' team_id',
#                                                                                                 ' batsman_team_id') + ''' and out_batsman_id<>-1
#                     and innings not in (3,4) group by out_batsman_id'''
#
#             out_batsman_df = executeQuery(con, out_batsman_sql, params)
#
#             best_batting_average = '''select player_id, player_name, bat_current_team as team_name, count(distinct match_id) as innings_played,
#                    sum(runs) as runs, player_image_url from batsman_overwise_df ''' + filters + ''' group by player_id,
#                     bat_current_team, player_name, player_image_url''' + f" having count(distinct match_id) >= {min_innings}"
#
#             best_batting_average_df = executeQuery(con, best_batting_average, params)
#
#             best_batting_average = '''select player_name, team_name, innings_played,player_image_url,
#             round(coalesce((bdf.runs/odf.wicket_cnt),0),2) as batting_average from
#              best_batting_average_df bdf left join out_batsman_df odf on (odf.out_batsman_id=bdf.player_id) where odf.wicket_cnt>0
#              order by batting_average desc limit 1'''
#
#         else:
#             params = []
#             highest_wicket_taker = '''select player_name, cast(sum(wickets) as int) as total_wickets, count(distinct match_id) as innings_played, player_image_url from
#             bowler_overwise_df group by player_id, player_name, player_image_url''' + f" having count(distinct match_id) >= {min_innings} order by total_wickets desc limit 1"
#
#             best_bowling_average = '''select player_name, bowl_current_team as team_name,
#             round(coalesce(((sum(runs)*1.00)/sum(wickets)),0.0),2)
#             as bowling_average, count(distinct match_id) as innings_played, player_image_url from bowler_overwise_df group by player_id, player_name, player_image_url, bowl_current_team
#             having sum(balls)>10 and bowling_average>0''' + f" and count(distinct match_id) >= {min_innings} order by bowling_average asc limit 1"
#
#             out_batsman_sql = '''select out_batsman_id, round(count(out_batsman_id)) as wicket_cnt
#                     from join_data where out_batsman_id<>-1
#                     and innings not in (3,4) group by out_batsman_id '''
#
#             out_batsman_df = executeQuery(con, out_batsman_sql)
#
#             best_batting_average = '''select player_id, player_name, bat_current_team as team_name, count(distinct match_id) as innings_played,
#                    sum(runs) as runs, player_image_url from batsman_overwise_df group by player_id,
#                     bat_current_team, player_name, player_image_url''' + f" having count(distinct match_id) >= {min_innings}"
#
#             best_batting_average_df = executeQuery(con, best_batting_average)
#
#             best_batting_average = '''select player_name, team_name, innings_played,player_image_url,
#             round(coalesce((bdf.runs/odf.wicket_cnt),0),2) as batting_average from
#              best_batting_average_df bdf left join out_batsman_df odf on (odf.out_batsman_id=bdf.player_id) where odf.wicket_cnt>0
#              order by batting_average desc limit 1'''
#
#         # Get Highest Score
#         del_keys = ['batting_type', 'bowling_type', 'player_id']
#         filters_score, params_score = generateWhereClause(dropFilter(del_keys, filter_dict))
#
#         if filters_score:
#             highest_score = '''select match_id, team1, team2, cast(sum(total_runs) as int) as total_runs, count(distinct match_id) as innings_played,
#         cast(sum(total_wickets) as int) as wickets from teams_aggregated_data ''' + filters_score + ''' group by match_id,
#             innings, team1,team2 order by total_runs desc limit 1'''
#         else:
#             params_score = []
#             highest_score = '''select match_id, team1, team2, cast(sum(total_runs) as int) as total_runs, count(distinct match_id) as innings_played,
#             cast(sum(total_wickets) as int) as wickets from teams_aggregated_data group by match_id, innings,
#             team1,team2 order by total_runs desc limit 1'''
#
#         response["highestWickets"] = executeQuery(con, highest_wicket_taker, params).to_dict("records")
#         response["bestBowlingAverage"] = executeQuery(con, best_bowling_average, params).to_dict("records")
#         response["bestBattingAverage"] = executeQuery(con, best_batting_average).to_dict("records")
#         response["highestTeamTotal"] = executeQuery(con, highest_score, params_score).to_dict("records")
#         return response, 200, logger.info("Status - 200")
#
#     except Exception as e:
#         logger.error(e)
#         raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# #@app.route("/latestPerformances", methods=['POST'])
@token_required
def latestPerformances():
    logger = get_logger("latestPerformances", "latestPerformances")

    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        del_keys = ['batting_type', 'bowling_type']

        filters, params = generateWhereClause(dropFilter(del_keys, filter_dict))

        if filters:
            player_matches = '''select match_id,player_name, bat_current_team as team_name, bowler_team_name as against_team, match_result,
    winning_team, cast(sum(runs) as int) as runs, cast(sum(balls) as int) as balls, cast(not_out as int) as is_not_out from 
    batsman_overwise_df ''' + filters.replace("team_id", "bowler_team_id") + ''' group by season, match_id, venue, 
                             player_name, bat_current_team, bowler_team_name, match_result, winning_team, not_out  
     order by match_id desc'''

        response = executeQuery(con, player_matches, params).to_json(orient="records")
        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/playerProfile", methods=['POST'])
# @token_required
def playerProfile():
    logger = get_logger("playerProfile", "playerProfile")
    page = request.json.get('page', 1)
    record_count = request.json.get('record_count')
    is_pagination = request.json.get('is_pagination')
    search_query = request.json.get('search_query')
    filter_body = request.json
    if request.json.get('user_name'):
        filter_body.pop('user_name')
    if request.json.get('page'):
        filter_body.pop('page')
    if record_count:
        filter_body.pop('record_count')
    if is_pagination:
        filter_body.pop('is_pagination')
    if search_query:
        filter_body.pop('search_query')

    # Check if the old key exists in the dictionary
    if 'teams' in filter_body:
        filter_body['team_id'] = filter_body['teams']
        del filter_body['teams']
    if 'type' in filter_body:
        batting_type = []
        if "RIGHT HAND BATSMAN" in filter_body['type'] or "LEFT HAND BATSMAN" in filter_body['type']:
            if "RIGHT HAND BATSMAN" in filter_body['type'] and "LEFT HAND BATSMAN" in filter_body['type']:
                batting_type = ["RIGHT HAND BATSMAN", "LEFT HAND BATSMAN"]
            elif "LEFT HAND BATSMAN" in filter_body['type']:
                batting_type = ["LEFT HAND BATSMAN"]
            else:
                batting_type = ["RIGHT HAND BATSMAN"]
            filter_body['batting_type'] = batting_type
        if 'RIGHT HAND BATSMAN' in filter_body['type']:
            filter_body['type'].remove('RIGHT HAND BATSMAN')
        if 'LEFT HAND BATSMAN' in filter_body['type']:
            filter_body['type'].remove('LEFT HAND BATSMAN')

        if filter_body['type']:
            filter_body['bowling_type'] = filter_body['type']
        del filter_body['type']
    filters, params = generateWhereClause(filter_body)
    try:
        if filters:
            player_sql = '''
             select 
              player_id, 
              src_player_id,
              player_name, 
              batting_type, 
              bowling_type, 
              player_skill, 
              case when player_skill in ('ALLROUNDER', 'WICKETKEEPER') then 'BATSMAN' else player_skill end as skill_name, 
              case when player_skill = 'ALLROUNDER' then 'ALLROUNDER' when player_skill = 'WICKETKEEPER' then 'WICKETKEEPER' else null end as additional_skill, 
              is_captain, 
              is_batsman, 
              is_bowler, 
              is_wicket_keeper, 
              player_type, 
              bowl_major_type, 
              player_image_url 
            from 
              players_data 
            ''' + filters + ''' order by player_name asc'''
            player_final_df = executeQuery(con, player_sql, params)

        elif ~(request.json is None) & ('player_name' in request.json):
            player_name = request.json.get('player_name')
            li = players_data_df["player_name"].unique()
            final_data = tuple(x for x in li if player_name.replace(" ", "").lower() in x.replace(" ", "").lower())
            if len(final_data) == 1:
                player_name = "='" + final_data[0] + "'"
            elif len(final_data) == 0:
                player_name = "='" + player_name + "'"
            else:
                player_name = " in " + str(final_data)

            player_sql = '''
            select 
              player_id, 
              src_player_id,
              player_name, 
              batting_type, 
              bowling_type, 
              player_skill, 
              is_captain, 
              is_batsman, 
              is_bowler, 
              is_wicket_keeper, 
              player_type, 
              case when player_skill in ('ALLROUNDER', 'WICKETKEEPER') then 'BATSMAN' else player_skill end as skill_name, 
              case when player_skill = 'ALLROUNDER' then 'ALLROUNDER' when player_skill = 'WICKETKEEPER' then 'WICKETKEEPER' else null end as additional_skill, 
              bowl_major_type, 
              player_image_url 
            from 
              players_data 
            where 
              player_name {} 
            order by 
              player_name asc
            '''.format(player_name)
            player_final_df = executeQuery(con, player_sql)

        else:
            player_sql = '''
            select 
              player_id, 
              src_player_id,
              player_name, 
              batting_type, 
              bowling_type, 
              player_skill, 
              case when player_skill in ('ALLROUNDER', 'WICKETKEEPER') then 'BATSMAN' else player_skill end as skill_name, 
              case when player_skill = 'ALLROUNDER' then 'ALLROUNDER' when player_skill = 'WICKETKEEPER' then 'WICKETKEEPER' else null end as additional_skill, 
              is_captain, 
              is_batsman, 
              is_bowler, 
              is_wicket_keeper, 
              player_type, 
              bowl_major_type, 
              player_image_url 
            from 
              players_data 
            order by 
              player_name asc
            '''
            player_final_df = executeQuery(con, player_sql)

        if is_pagination:
            player_final_df = player_final_df.drop_duplicates(
                subset='player_id',
                keep="last"
            )
            if search_query:
                player_final_df = player_final_df[player_final_df['player_name'].str.contains(search_query, case=False)]
            total_count = int(player_final_df['player_id'].count())
            response = {}
            response['total_count'] = total_count
            response['data'] = json.loads(
                player_final_df.iloc[((page - 1) * record_count):(page * record_count)].to_json(
                    orient='records'
                ))
            return response
        if len(player_final_df) > 0:
            response = player_final_df.sort_values('player_image_url', ascending=True).drop_duplicates(
                subset='player_id',
                keep="last"
            ).to_json(orient="records")
        else:
            if "player_name" in request.json:
                response = jsonify([{'player_name': request.json.get('player_name')}])
            else:
                response = jsonify([])
        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/playerSeasonStats", methods=['POST'])
@token_required
def playerSeasonStats():
    logger = get_logger("playerSeasonStats", "playerSeasonStats")
    response = dict()
    try:
        filter_dict = globalFilters()
        filter_dict_bowling = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        if request.json:
            if request.json.get('overs'):
                filter_dict['over_number'] = request.json.get('overs')
            if request.json.get('innings'):
                if type(request.json.get('innings')) != list:
                    innings_mapper = {
                        1: 2,
                        2: 1
                    }
                    filter_dict_bowling['innings'] = innings_mapper[request.json.get('innings')]
            if request.json.get('winning_type'):
                if type(request.json.get('winning_type')) != list:
                    winning_type_mapper = {
                        "Losing": "Winning",
                        "Winning": "Losing"
                    }
                    filter_dict_bowling['winning_type'] = winning_type_mapper[request.json.get('winning_type')]
        filters, params = generateWhereClause(filter_dict)
        filters_bowling, params_bowling = generateWhereClause(filter_dict_bowling)

        if filters:
            batsman_season_stats = f"""
            select 
              season, 
              player_id, 
              player_name, 
              bat_current_team as team_name,
              competition_name, 
              count(distinct match_id) as innings_played, 
              cast(
                sum(balls) as int
              ) as balls_played, 
              cast(
                sum(runs) as int
              ) as total_runs_scored, 
              cast(
                sum(wickets) as int
              ) as dismissals, 
              round(
                coalesce(
                  (
                    sum(runs)* 100.00
                  )/ sum(balls), 
                  0.0
                ), 
                2
              ) as strike_rate, 
              round(
                coalesce(
                  (
                    sum(dot_balls)* 100.00 / sum(balls)
                  ), 
                  0.0
                ), 
                2
              ) as dot_ball_percent, 
              round(
                coalesce(
                  (
                    (
                      sum(fours)* 4 + sum(sixes)* 6
                    )* 100.00
                  )/ sum(runs), 
                  0.0
                ), 
                2
              ) as boundary_percent 
            from 
              batsman_overwise_df {filters} 
            group by 
              player_id, 
              season, 
              player_name, 
              bat_current_team,
              competition_name 
            order by 
              season desc
            """

            batsman_season_stats_df = executeQuery(con, batsman_season_stats, params)

            out_batsman_sql = f'''
            select 
              out_batsman_id, 
              season, 
              round(
                count(out_batsman_id)
              ) as wicket_cnt 
            from 
              join_data {
            filters.replace(
                'player_id', 'out_batsman_id'
            ).replace(
                ' team_id', ' batsman_team_id'
            ).replace(
                'player_skill', 'out_batsman_skill'
            )} 
              and out_batsman_id <>-1 
              and innings not in (3, 4) 
            group by 
              out_batsman_id, 
              season
            '''

            out_batsman_df = executeQuery(con, out_batsman_sql, params)

            final_batsman_stats_sql = '''
            select 
              bdf.season, 
              player_name, 
              team_name,
              competition_name, 
              innings_played, 
              balls_played, 
              total_runs_scored, 
              strike_rate, 
              dot_ball_percent, 
              boundary_percent, 
              round(
                coalesce((total_runs_scored*1.00)/wicket_cnt,0.0),
                2
              ) as batting_average, 
              dismissals 
            from 
              batsman_season_stats_df bdf 
              left join out_batsman_df odf on (
                odf.out_batsman_id = bdf.player_id 
                and odf.season = bdf.season
              )
            '''

            bowler_season_stats = f'''
            select 
              season, 
              player_name, 
              bowl_current_team as team_name, 
              competition_name, 
              count(distinct match_id) as innings_played, 
              cast(
                sum(wickets) as int
              ) as dismissals,
              cast(
                sum(wickets) as int
              ) as wickets,
              round(
                coalesce(
                  (
                    (
                      sum(balls)* 1.00
                    )/ sum(wickets)
                  ), 
                  0
                ), 
                2
              ) as bowling_strike_rate, 
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
              ) as bowling_economy, 
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
              ) as bowling_avg, 
              cast(
                sum(runs) as int
              ) as total_runs_conceded, 
              round(
                coalesce(
                  (
                    sum(dot_balls)* 100.00 / sum(balls)
                  ), 
                  0.0
                ), 
                2
              ) as dot_ball_percent, 
              round(
                coalesce(
                  (
                    (
                      sum(fours)* 4 + sum(sixes)* 6
                    )* 100.00
                  )/ sum(runs), 
                  0.0
                ), 
                2
              ) as boundary_percent 
            from 
              bowler_overwise_df {filters_bowling} 
            group by 
              player_id, 
              season, 
              player_name, 
              bowl_current_team, 
              competition_name
            order by 
              season desc
            '''

            response["battingStats"] = executeQuery(con, final_batsman_stats_sql).to_dict("records")
            response["bowlingStats"] = executeQuery(con, bowler_season_stats, params_bowling).to_dict("records")
            return response, 200, logger.info("Status - 200")
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/bestPartnerships", methods=['POST'])
@token_required
def bestPartnerships():
    logger = get_logger("bestPartnerships", "bestPartnerships")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        response = {}
        del_keys = ['batting_type', 'bowling_type', 'player_skill']

        filters, params = generateWhereClause(dropFilter(del_keys, filter_dict))

        if filters:
            striker_df = executeQuery(con, '''select partnership_total as runs, partnership_balls as balls, striker_name 
        as player1, striker_runs as player1_runs, striker_balls as player1_balls, non_striker_name as player2, 
        non_striker_runs as player2_runs, non_striker_balls as player2_balls, team_name from partnership_data ''' +
                                      filters.replace('player_id', 'striker') + ''' order by partnership_total desc''',
                                      params) \
                .head(10)

            striker_agg_df = executeQuery(con, '''select non_striker_name as player_name,team_name, 
            cast(sum(partnership_total) as int) as runs, count(distinct match_id) as innings from partnership_data '''
                                          + filters.replace('player_id', 'striker') +
                                          ''' group by non_striker_name, team_name order by runs desc''', params).head(
                10)

        if len(striker_df) > 0:
            response['bestPartnerships'] = striker_df.to_dict("records")

        if len(striker_agg_df) > 0:
            response['AggPartnerships'] = striker_agg_df.to_dict("records")

        return jsonify(response), 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/overwiseBowlingStats", methods=['POST'])
@token_required
def overwiseBowlingStats():
    logger = get_logger("overwiseBowlingStats", "overwiseBowlingStats")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))

    try:
        if request.json:
            if request.json.get('overs'):
                filter_dict['over_number'] = request.json.get('overs')

        filters, params = generateWhereClause(filter_dict)

        if filters:
            params.extend(params)
            overwise_stats_df = executeQuery(con, '''select oa.player_name,oa.over_number,
            cast(sum(oa.balls) as int) as balls, round((sum(oa.balls)*100.00)/bs.balls_sum,2) as percent_times,
            cast(sum(oa.wickets) as int) as wickets, round(coalesce(((sum(oa.balls)*1.00)/sum(oa.wickets)),0.0),2) as 
            bowling_strike_rate, round(coalesce(((sum(oa.runs)*1.00)/(sum(oa.balls)*1.00/6)),0.0),2) as bowling_economy,
             round(coalesce(((sum(oa.runs)*1.00)/sum(oa.wickets)),0.0),2) as bowling_avg
            , cast(sum(oa.runs) as int) as runs from bowler_overwise_df oa 
            left join (select player_id, cast(sum(balls) as int) as balls_sum from 
            bowler_overwise_df ''' + filters + ''' group by player_id) bs on 
            (oa.player_id=bs.player_id) ''' + filters.replace("player_id", "oa.player_id") + ''' group by oa.over_number, 
            oa.player_id, oa.player_name, balls_sum''', params)

        else:
            overwise_stats_df = executeQuery(con, '''select oa.player_name,oa.over_number, 
            cast(sum(oa.balls) as int) as balls, round((sum(oa.balls)*100.00)/bs.balls_sum,2) as percent_times, 
            cast(sum(oa.wickets) as int) as wickets,
            round(coalesce(((sum(oa.balls)*1.00)/sum(oa.wickets)),0.0),2) as bowling_strike_rate,
            round(coalesce(((sum(oa.runs)*1.00)/(sum(oa.balls)*1.00/6)),0.0),2) as bowling_economy,
             round(coalesce(((sum(oa.runs)*1.00)/sum(oa.wickets)),0.0),2) as bowling_avg
            , cast(sum(oa.runs) as int) as runs from bowler_overwise_df oa left join 
            (select player_id, cast(sum(balls) as int) as balls_sum from bowler_overwise_df group by player_id) bs on 
            (oa.player_id=bs.player_id) group by oa.over_number, oa.player_id, oa.player_name, balls_sum''')

        response = overwise_stats_df.groupby('player_name') \
            .apply(lambda x: x.set_index('over_number').to_dict(orient='index')).to_dict()

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/overwiseStrikeRate", methods=['POST'])
@token_required
def overwiseStrikeRate():
    logger = get_logger("overwiseStrikeRate", "overwiseStrikeRate")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        if request.json:
            if request.json.get('overs'):
                filter_dict['over_number'] = request.json.get('overs')

        filters, params = generateWhereClause(filter_dict)

        if filters:
            overwise_sr_df = executeQuery(con, '''select  over_number, player_name, 
            round(coalesce(((sum(runs)*100.00)/sum(balls)),0.0),2) as strike_rate from batsman_overwise_df '''
                                          + filters + ''' group by over_number,player_id, player_name''', params)

            response = overwise_sr_df.groupby('player_name') \
                .apply(lambda x: x.set_index('over_number').to_dict(orient='index')).to_dict()

            return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/overwiseBowlingOrder", methods=['POST'])
@token_required
def overwiseBowlingOrder():
    logger = get_logger("overwiseBowlingOrder", "overwiseBowlingOrder")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        if "match_id" in request.json:
            filter_dict['match_id'] = request.json.get('match_id')
        if "team_id" in request.json:
            filter_dict['team_id'] = request.json.get('team_id')
        if "batsman_team_id" in request.json:
            filter_dict['batsman_team_id'] = request.json.get('batsman_team_id')

        del_keys = ['player_id', 'batting_type', 'bowling_type']

        filters, params = generateWhereClause(dropFilter(del_keys, filter_dict))

        if filters:
            overwise_bowlers = '''select match_id, match_date, over_number, player_name, bowling_type, team_name as team1, 
            team2, match_decision, player_image_url from bowling_order_df ''' + filters \
                               + ''' group by player_image_url, match_id, over_number, match_date, bowling_type, team_name, team2, 
            match_decision, player_name order by match_id desc, over_number asc '''

        else:
            params = []
            overwise_bowlers = '''select match_id, match_date, over_number, player_name, bowling_type, team_name as team1, 
            team2, match_decision, player_image_url from bowling_order_df group by player_image_url, match_id, over_number, match_date, bowling_type, team_name,
            team2, match_decision, player_name order by match_id desc, over_number asc '''

        overwise_bowlers_df = executeQuery(con, overwise_bowlers, params)

        if len(overwise_bowlers_df) > 0:
            overwise_bowlers_df['player_details'] = overwise_bowlers_df[
                ['team1', 'player_name', 'over_number', 'bowling_type', 'player_image_url']] \
                .to_dict(orient='records')

            response = \
                overwise_bowlers_df.drop(['over_number', 'player_name', 'bowling_type', 'player_image_url'], axis=1) \
                    .groupby(['match_id', 'match_date', 'team1', 'team2', 'match_decision'])['player_details'] \
                    .agg(list).reset_index().sort_values('match_id', ascending=False).head(40).to_json(orient='records')

        else:
            response = jsonify([])

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/positionWiseAvgRuns", methods=['POST'])
@token_required
def positionWiseAvgRuns():
    logger = get_logger("positionWiseAvgRuns", "positionWiseAvgRuns")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        data_df = pd.DataFrame()

        if request.json:
            if request.json.get('overs'):
                filter_dict['over_number'] = request.json.get('overs')

        filters, params = generateWhereClause(filter_dict)

        if filters:

            last_matches = [5, 10, 'Overall']
            for val in last_matches:
                if val != "Overall":
                    key = 'last' + str(val) + 'Matches'

                    positionwise_avg_matches_sql = '''select '{}' as against_team, batting_position,
    cast(sum(runs) as int) as total_runs_scored, cast(sum(balls) as int) as balls_played,
    count(distinct match_id) as innings_played, cast(sum(wickets) as int) as dismissals, player_name,
    round(coalesce(((sum(runs)*100.00)/sum(balls)),0.0),2) as strike_rate,
    round(coalesce((sum(runs)*1.00)/sum(wickets),0.0),2) as batting_average from 
     (select match_id, player_id, bowler_team_id as against_team_id,
        bowler_team_name as against_team, team_name, season, innings, venue, batting_type, bowling_type, winning_type,
        batting_position, over_number, runs, balls, not_out, player_name, wickets, 
        dense_rank() over (order by match_id desc) as match_rank 
        from  batsman_overwise_df '''.format(key) + filters + ''') where match_rank<=''' + str(val) \
                                                   + ''' group by player_id,batting_position, player_name 
                                                   order by batting_position desc'''

                    data_df = data_df.append(executeQuery(con, positionwise_avg_matches_sql, params), ignore_index=True)

                else:

                    key = 'Overall'

                    positionwise_avg_all_sql = '''select '{}' as against_team, player_id, batting_position,
    cast(sum(runs) as int) as total_runs_scored, cast(sum(balls) as int) as balls_played,
    count(distinct match_id) as innings_played, player_name, 
    round(coalesce(((sum(runs)*100.00)/sum(balls)),0.0),2) as strike_rate from batsman_overwise_df '''.format(key) \
                                               + filters + ''' group by player_id, batting_position,player_name 
                                               order by batting_position desc'''
                    positionwise_avg_teams_df = executeQuery(con, positionwise_avg_all_sql, params)

                    out_params = params
                    out_filter_dict = filter_dict.copy()
                    del_keys = ['venue', 'player_skill', 'bowling_type', 'batting_type', 'winning_type', 'over_number']
                    out_filters, inner_params = generateWhereClause(dropFilter(del_keys, out_filter_dict))
                    inner_params.extend(out_params)

                    out_batsman_sql = f'''select bdf.batting_position, out_batsman_id, count(out_batsman_id) as wicket_cnt
                     from join_data jd left join (select match_id, batting_position, batsman_id from bat_card_data 
                    {out_filters.replace('player_id', 'batsman_id').replace(' team_id', ' batting_team_id')} and innings not in (3,4) ) bdf 
                      on (jd.match_id=bdf.match_id and bdf.batsman_id=jd.out_batsman_id)
    {filters.replace('player_id', 'out_batsman_id').replace(' team_id', ' batsman_team_id').replace('player_skill', 'out_batsman_skill')} and out_batsman_id<>-1 
    and jd.innings not in (3,4) group by bdf.batting_position, out_batsman_id'''

                    out_batsman_df = executeQuery(con, out_batsman_sql, inner_params)

                    positionwise_avg_teams = '''select against_team, pdf.batting_position,
                        total_runs_scored, balls_played, innings_played, coalesce(wicket_cnt, 0) as dismissals, player_name,
                         strike_rate, round(coalesce((total_runs_scored*1.00)/wicket_cnt,0.0),2) as batting_average from positionwise_avg_teams_df
                         pdf left join out_batsman_df odf on (odf.out_batsman_id=pdf.player_id and odf.batting_position=pdf.batting_position)'''

                    data_df = data_df.append(executeQuery(con, positionwise_avg_teams), ignore_index=True)

            positionwise_avg_teams_sql = '''select bowler_team_name as against_team, batting_position,
    cast(sum(runs) as int) as total_runs_scored, cast(sum(balls) as int) as balls_played,
    count(distinct match_id) as innings_played, cast(sum(wickets) as int) as dismissals, player_name,
     round(coalesce(((sum(runs)*100.00)/sum(balls)),0.0),2) as strike_rate,
    round(coalesce((sum(runs)*1.00)/sum(wickets),0.0),2) as batting_average from batsman_overwise_df ''' + filters + '''
     group by player_id, bowler_team_name, batting_position, player_name order by batting_position desc'''

            data_df = data_df.append(executeQuery(con, positionwise_avg_teams_sql, params), ignore_index=True)

            data_df['player_details'] = data_df[
                ['batting_position', 'total_runs_scored', 'dismissals', 'batting_average',
                 'balls_played', 'innings_played', 'strike_rate']] \
                .to_dict(orient='records')

            data_df = data_df.drop(['batting_position', 'total_runs_scored', 'dismissals', 'batting_average',
                                    'balls_played', 'innings_played', 'strike_rate'], axis=1)

            response = data_df.groupby(['against_team', 'player_name'])['player_details'] \
                .agg(list).reset_index().to_json(orient='records')

            return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/positionWiseBowler", methods=['POST'])
@token_required
def positionWiseBowler():
    logger = get_logger("positionWiseBowler", "positionWiseBowler")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        filter_dict['overs'] = request.json.get('overs')

        filters, params = generateWhereClause(filter_dict)

        if filters:
            positionwise_bowling_sql = '''select bowl_current_team as team_name, batting_position,player_id, player_name,
            count(distinct match_id) as innings_played,
        cast(sum(balls) as int) as balls, cast(sum(runs) as int)  as runs, cast(sum(wickets) as int) as wickets,
         round(coalesce(((sum(runs)*1.00)/(sum(balls)*1.00/6)),0.0),2) as bowling_economy,
          round(coalesce(((sum(runs)*1.00)/sum(wickets)),0.0),2) as bowling_avg,
        sum(balls)/count(distinct match_id) as avg_balls_innings, round(coalesce(sum(runs)/count(distinct match_id),0.0),1)
        as avg_runs_innings from bowler_positionwise_df ''' + filters + ''' group by batting_position,
        player_id, bowl_current_team, player_name order by batting_position'''

        response = executeQuery(con, positionwise_bowling_sql, params).to_json(orient="records")

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/positionWiseTeamsPerOver", methods=['POST'])
@token_required
def positionWiseTeamsPerOver():
    logger = get_logger("positionWiseTeamsPerOver", "positionWiseTeamsPerOver")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        response = {}
        filter_dict['overs'] = request.json.get('overs')
        del_keys = ['player_id']

        filters, params = generateWhereClause(dropFilter(del_keys, filter_dict))

        if filters:
            positionwise_avg_teams = '''select batting_position,team_short_name,result,
cast(coalesce(sum(runs)/count(distinct match_id),0) as int) as runs,
cast(coalesce(sum(balls)/count(distinct match_id),0) as int) as balls from batsman_positionwise_df ''' \
                                     + filters + ''' group by team_id, team_short_name, batting_position, result'''

            positionwise_avg_per_over_team = executeQuery(con, positionwise_avg_teams, params)

            positionwise_avg_per_over_team['match_result'] = positionwise_avg_per_over_team[
                ['team_short_name', 'result']].agg(''.join, axis=1)

            del_keys_all = ['player_id', 'team_id']
            filters_all, params_all = generateWhereClause(dropFilter(del_keys_all, filter_dict))

            positionwise_avg_all = '''select batting_position, result as match_result,
cast(coalesce(sum(runs)/count(distinct match_id),0) as int) as runs,
cast(coalesce(sum(balls)/count(distinct match_id),0) as int)
as balls from batsman_positionwise_df ''' + filters_all + ''' group by batting_position, result'''

            positionwise_avg_per_over_all = executeQuery(con, positionwise_avg_all, params_all)

            response['team'] = positionwise_avg_per_over_team.groupby('match_result') \
                [['batting_position', 'runs', 'balls']] \
                .apply(lambda x: x.set_index(['batting_position']).to_dict(orient='index')).to_dict()

            response['overall'] = positionwise_avg_per_over_all.groupby('match_result') \
                [['batting_position', 'runs', 'balls']] \
                .apply(lambda x: x.set_index(['batting_position']).to_dict(orient='index')).to_dict()

            return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/matchPlayingXI", methods=['POST'])
@token_required
def matchPlayingXI():
    logger = get_logger("matchPlayingXI", "matchPlayingXI")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        if request.json:
            if 'match_id' in request.json:
                filter_dict['match_id'] = request.json.get('match_id')

        del_keys = ['player_id', 'batting_type', 'bowling_type']

        filters, params = generateWhereClause(dropFilter(del_keys, filter_dict))

        if filters:
            playing_xi_sql = '''
            select 
              match_id, 
              match_date, 
              team_id as team1_id, 
              team1, 
              team1_short_name, 
              team2_id, 
              team2, 
              team2_short_name, 
              match_decision, 
              player_id, 
              player_name, 
              batting_type as batting_style, 
              bowling_type as bowling_style, 
              player_image_url, 
              cast(batting_position as int) as batting_position, 
              innings 
            from 
              match_playing_xi 
            ''' + filters

        else:
            playing_xi_sql = '''select match_id, match_date, team_id as team1_id, team1, team1_short_name, team2_id, 
            team2, team2_short_name, match_decision, 
            player_id, player_name, batting_type as batting_style, bowling_type as bowling_style,player_image_url, 
            cast(batting_position as int) as batting_position, innings from match_playing_xi '''

        playing_xi_df = executeQuery(con, playing_xi_sql, params)
        if len(playing_xi_df) > 0:

            if request.json:
                if 'last_#matches' in request.json:
                    match_rank = request.json.get('last_#matches')

                    playing_xi_df['match_rank'] = playing_xi_df['match_id'].rank(method='dense',
                                                                                 ascending=False).astype(int)

                    playing_xi_df = playing_xi_df[playing_xi_df['match_rank'] <= match_rank]

            playing_xi_df['batting_position'] = playing_xi_df['batting_position'].fillna(0).astype(int)
            playing_xi_df = playing_xi_df.drop_duplicates()
            playing_xi_df['player_details'] = playing_xi_df[
                ['player_id', 'player_name', 'team1', 'batting_style', 'bowling_style',
                 'player_image_url', 'batting_position']].to_dict(orient='records')

            playing_xi_df = playing_xi_df.drop(
                ['player_id', 'batting_style', 'player_name', 'bowling_style', 'player_image_url', 'batting_position'],
                axis=1)

            playing_xi_df = \
                playing_xi_df.groupby(['match_id', 'team1_id', 'team1', 'team1_short_name', 'team2_id', 'team2',
                                       'team2_short_name', 'match_decision', 'match_date', 'innings'])[
                    'player_details'].agg(list).reset_index()

            response = playing_xi_df.sort_values('match_id', ascending=False).to_json(orient='records')

        else:
            response = jsonify([])

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/overWiseStats", methods=['POST'])
@token_required
def overWiseStats():
    logger = get_logger("overWiseStats", "overWiseStats")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        response = {}
        filter_dict['over_number'] = request.json.get('overs')

        filters, params = generateWhereClause(filter_dict)

        if filters:
            overwise_batsman_sql = '''select bo.overs, bo.player_name, round(sum(bo.total_runs)*1.00/sum(inn.innings),2)
    as runs, round(sum(bo.total_wickets)*1.00/sum(inn.innings),2) as wickets, cast(sum(bo.fours) as int) as fours,
    cast(sum(bo.sixes) as int) as sixes,
    round(coalesce((((sum(bo.total_runs)*1.00/sum(inn.innings))*100.00)/(sum(bo.total_balls)*1.00/sum(inn.innings))),0.0),2)
    as strike_rate from (select player_id, season, player_name,sum(runs) as total_runs, sum(balls) as total_balls,
    sum(wickets) as total_wickets, sum(sixes) as sixes, sum(fours) as fours, over_number as overs from batsman_overwise_df ''' \
                                   + filters + ''' group by season, player_id, over_number, player_name) bo left join
    (select season, count(distinct match_id) as innings,player_id,over_number as overs
    from batsman_overwise_df ''' + filters + ''' group by season, player_id, over_number) inn on
    (bo.player_id=inn.player_id and bo.season=inn.season and bo.overs=inn.overs) group by bo.player_id, bo.overs,
    bo.player_name order by bo.overs'''
            params.extend(params)
            overwise_batsman_stats = executeQuery(con, overwise_batsman_sql, params)
            overwise_batsman_stats['player_details'] = overwise_batsman_stats[
                ['overs', 'runs', 'wickets', 'fours', 'sixes',
                 'strike_rate']].to_dict(orient='records')

            response['batsman_details'] = overwise_batsman_stats.drop(
                ['overs', 'runs', 'wickets', 'fours', 'sixes', 'strike_rate'], axis=1).groupby(['player_name'])[
                'player_details'] \
                .agg(list).reset_index().to_dict('records')

            overwise_bowler_sql = '''select bo.overs, bo.player_name, round(sum(bo.total_runs)*1.00/sum(inn.innings),2) as
    runs, round(sum(bo.total_wickets)*1.00/sum(inn.innings),2) as wickets, cast(sum(fours) as int) as fours,
    cast(sum(sixes) as int) as sixes,
    round(coalesce(((sum(bo.total_balls)*1.00/sum(inn.innings))/(sum(bo.total_wickets)*1.00/sum(inn.innings))),0.0),2)
    as strike_rate from
     (select player_id, season, player_name, sum(runs) as total_runs, sum(balls) as total_balls,
    sum(wickets) as total_wickets, sum(sixes) as sixes,sum(fours) as fours, over_number as overs from bowler_overwise_df ''' \
                                  + filters + ''' group by season, player_id, over_number, player_name) bo left join
                                   (select season,count(distinct match_id) as innings,player_id,over_number from
                                   bowler_overwise_df ''' + filters + ''' group by season, player_id, over_number)
    inn on (bo.player_id=inn.player_id and bo.season=inn.season and bo.overs=inn.over_number)
    group by bo.player_id,bo.overs, bo.player_name order by bo.overs'''

            overwise_bowler_stats = executeQuery(con, overwise_bowler_sql, params)

            overwise_bowler_stats['player_details'] = overwise_bowler_stats[
                ['overs', 'runs', 'wickets', 'fours', 'sixes',
                 'strike_rate']].to_dict(orient='records')

            response['bowler_details'] = overwise_bowler_stats.drop(
                ['overs', 'runs', 'wickets', 'fours', 'sixes', 'strike_rate'], axis=1).groupby(['player_name'])[
                'player_details'] \
                .agg(list).reset_index().to_dict('records')

            return response, 200, logger.info("Status - 200")
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# #@app.route("/batsmanVSbowlerStats", methods=['POST'])
# #@token_required
# def batsmanVSbowlerStats():
#     logger = get_logger("batsmanVSbowlerStats", "batsmanVSbowlerStats")
#     try:
#         filter_dict = globalFilters()
#     except ValidationError as e:
#         logger.error(e.messages)
#         logger.error(e.valid_data)
#         raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
#     try:
#         if 'team_id' in filter_dict:
#             del_keys = ['team_id']
#             filter_dict = dropFilter(del_keys, filter_dict)
#
#         if "team1" in request.json:
#             filter_dict['team_id'] = request.json.get('team1')
#         if "team2" in request.json:
#             filter_dict['bowler_team_id'] = request.json.get('team2')
#         if "bowler_id" in request.json:
#             filter_dict['bowler_id'] = request.json.get('bowler_id')
#
#         filter_dict['over_number'] = request.json.get('overs')
#
#         filters, params = generateWhereClause(filter_dict)
#
#         batsmanVSbowler_sql = '''select player_name,bat_current_team as team_name,batting_type,bowler_name,
#                 bowl_current_team as bowler_team_name,
#             bowling_type, cast(sum(runs) as int) as runs,cast(sum(balls) as int) as balls, cast(sum(wickets) as int) as wickets,
#             round(coalesce(((sum(runs)*100.00)/sum(balls)),0.0),2) as strike_rate from batsman_overwise_df ''' + filters + ''' group by player_id,bowler_id,team_id,bowler_team_id,
#             player_name,bat_current_team, batting_type, bowl_current_team, bowling_type,bowler_name  '''
#
#         if "min_innings" in request.json:
#             min_innings = request.json['min_innings']
#             batsmanVSbowler_sql = batsmanVSbowler_sql + f" having count(distinct match_id) >= {min_innings}"
#
#         batsmanVSbowler = executeQuery(con, batsmanVSbowler_sql, params)
#
#         batsmanVSbowler['bowler_details'] = batsmanVSbowler[
#             ['bowler_name', 'bowler_team_name', 'bowling_type', 'runs', 'balls',
#              'strike_rate', 'wickets']].to_dict(orient='records')
#
#         batsmanVSbowler = batsmanVSbowler.drop(['bowler_name', 'bowler_team_name', 'bowling_type', 'runs', 'balls',
#                                                 'strike_rate', 'wickets'], axis=1)
#
#         response = batsmanVSbowler.groupby(['player_name', 'team_name', 'batting_type'])['bowler_details'] \
#             .agg(list).reset_index().to_json(orient='records')
#
#         return response, 200, logger.info("Status - 200")
#     except Exception as e:
#         logger.error(e)
#         raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/battingVSbowlerType", methods=['POST'])
@token_required
def battingVSbowlerType():
    logger = get_logger("battingVSbowlerType", "battingVSbowlerType")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        filter_dict['over_number'] = request.json.get('overs')

        filters, params = generateWhereClause(filter_dict)

        battingBowlerType = executeQuery(con, '''select player_name, bowling_type as bowler_type,
        coalesce(round(sum(balls)/sum(wickets),2),0.0) as dismissal_rate,
        round(coalesce(((sum(runs)*1.00)/(case when (sum(balls)%6)==0
        then ((sum(balls)*1.00)/6) else (sum(balls)/6) + ((sum(balls)%6)/10.00) end)),0.0),2) as run_rate
        from batsman_overwise_df ''' + filters + ''' group by player_name, bowling_type''', params)

        battingBowlerType['bowler_details'] = battingBowlerType[['bowler_type', 'dismissal_rate', 'run_rate']] \
            .to_dict(orient='records')

        battingBowlerType = battingBowlerType.drop(['bowler_type', 'dismissal_rate', 'run_rate'], axis=1)

        response = battingBowlerType.groupby(['player_name'])['bowler_details'] \
            .agg(list).reset_index().to_json(orient='records')

        return response, 200, logger.info("Status - 200")
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/strikeRateVSdismissals", methods=['POST'])
# #@token_required
# def strikeRateVSdismissals():
#     logger = get_logger("strikeRateVSdismissals", "strikeRateVSdismissals")
#     try:
#         filter_dict = globalFilters()
#     except ValidationError as e:
#         logger.error(e.messages)
#         logger.error(e.valid_data)
#         raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
#     try:
#         if 'team_id' in filter_dict:
#             del_keys = ['team_id']
#             filter_dict = dropFilter(del_keys, filter_dict)
#
#         if "team1" in request.json:
#             filter_dict['team_id'] = request.json.get('team1')
#         if "team2" in request.json:
#             filter_dict['bowler_team_id'] = request.json.get('team2')
#
#         filter_dict['over_number'] = request.json.get('overs')
#         filters, params = generateWhereClause(filter_dict)
#
#         battingBowlerType_sql = '''select player_name, bat_current_team as team_name, bowling_type as bowler_type,
#             cast(sum(wickets) as int) as dismissals, cast(sum(runs) as int) as runs, cast(sum(balls) as int) as balls,
#             round(coalesce(sum(runs)*100.00/sum(balls),0.0),2) as strike_rate from batsman_overwise_df ''' + filters + '''
#             group by player_name, bat_current_team, bowling_type'''
#
#         if "min_innings" in request.json:
#             min_innings = request.json['min_innings']
#             battingBowlerType_sql = battingBowlerType_sql + f" having count(distinct match_id) >= {min_innings}"
#
#         battingBowlerType = executeQuery(con, battingBowlerType_sql, params)
#
#         battingBowlerType['bowler_details'] = battingBowlerType[['bowler_type', 'dismissals', 'strike_rate']] \
#             .to_dict(orient='records')
#
#         battingBowlerType = battingBowlerType.drop(['bowler_type', 'dismissals', 'strike_rate'], axis=1)
#
#         response = battingBowlerType.groupby(['player_name', 'team_name'])['bowler_details'] \
#             .agg(list).reset_index().to_json(orient='records')
#         return response, 200, logger.info("Status - 200")
#     except Exception as e:
#         logger.error(e)
#         raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/highestIndividualScores", methods=['POST'])
@token_required
def highestIndividualScores():
    logger = get_logger("highestIndividualScores", "highestIndividualScores")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        response = {}

        if request.json:
            if request.json.get('overs'):
                filter_dict['over_number'] = request.json.get('overs')

        filters, params = generateWhereClause(filter_dict)

        if filters:
            highestScoresSQL = '''select player_id, player_name,bat_current_team as team_name, cast(sum(runs) as int) as runs, count(distinct match_id) as 
                matches, round(coalesce(((sum(runs)*100.00)/sum(balls)),0.0),2) as strike_rate,
                round(coalesce((sum(runs)*1.00)/sum(wickets),0.0),2) as average, cast(sum(case when runs>=100 then 1 else 0 end) as int)
                as hundreds, cast(sum(fours) as int) as fours, cast(sum(sixes) as int) as sixes,
                cast(sum(case when runs between 50 and 99 then 1 else 0 end) as int) as fifties
                , player_image_url from
                 (select  player_name, player_image_url, bat_current_team, player_id,innings, sum(runs) as runs, sum(balls) as balls,
                 match_id, sum(fours) as fours, sum(sixes) as sixes, sum(wickets) as wickets, not_out from batsman_overwise_df ''' + \
                               filters + ''' group by player_id, not_out, match_id, innings, player_name, bat_current_team, player_image_url)
                                           group by player_id, player_name, bat_current_team, player_image_url'''

            out_batsman_sql = '''select out_batsman_id, round(count(out_batsman_id)) as wicket_cnt
                    from join_data ''' + filters.replace('player_id', 'out_batsman_id').replace(' team_id',
                                                                                                ' batsman_team_id').replace \
                ('player_skill', 'out_batsman_skill') + \
                              ''' and out_batsman_id<>-1 and innings not in (3,4) group by out_batsman_id'''

            top_scorer_sql = ''' select player_name, player_image_url, team_name, MAX(highest_score) as highest_score,
                        count(distinct match_id) as innings_played from (select  player_name, player_image_url,
                        bat_current_team as team_name, cast(sum(runs) as int) as highest_score, match_id from batsman_overwise_df ''' + filters + '''
                        group by player_id, not_out, match_id, innings, player_name, bat_current_team, player_image_url
                        order by highest_score desc, player_name asc) group by player_name, player_image_url, team_name'''

            first_last_ball_boundary_sql = '''select player_name, team_name, player_image_url, cast(sum(first_ball_boundary) as int) as 
            first_ball_boundary, cast(sum(last_ball_boundary) as int) as last_ball_boundary, count(distinct match_id) as innings_played 
            from (select batsman_id, batsman_name as player_name, match_id, over_number, batsman_team_id, batsman_team_name as team_name, 
            MIN(ball_number) as FirstBall, MAX(ball_number) as LastBall, cast(sum(case when ball_number= 1 and is_extra <> 1 and 
            (is_four = 1 or is_six = 1)then 1 else 0 end) as int) as first_ball_boundary, cast(sum(case when ball_number= 6 
            and is_extra <> 1 and (is_four = 1 or is_six = 1)then 1 else 0 end) as int) as last_ball_boundary, batsman_image_url as player_image_url 
            from join_data''' + filters.replace('player_id', 'batsman_id').replace(' team_id',
                                                                                   ' batsman_team_id').replace \
                ('player_skill', 'batsman_skill') + \
                                           ''' and innings not in (3,4) group by batsman_id, batsman_name, over_number, match_id, batsman_team_id, batsman_team_name, batsman_image_url) 
                                           group by player_name, team_name, player_image_url'''

        else:
            highestScoresSQL = '''select player_name,bat_current_team as team_name, cast(sum(runs) as int) as runs, count(distinct match_id)
    as matches, round(coalesce(((sum(runs)*100.00)/sum(balls)),0.0),2) as strike_rate,
    round(coalesce((sum(runs)*1.00)/sum(wickets),0.0),2) as average, cast(sum(case when runs>=100 then 1 else 0 end) as int)
    as hundreds, cast(sum(fours) as int) as fours, cast(sum(sixes) as int) as sixes,
    cast(sum(case when runs between 50 and 99 then 1 else 0 end) as int) as fifties, player_image_url
    from (select  player_name, player_image_url, bat_current_team, player_id,innings, sum(runs) as runs, sum(balls) as balls,
     match_id, sum(fours) as fours, sum(sixes) as sixes, sum(wickets) as wickets, not_out from batsman_overwise_df
     group by player_id, not_out, match_id, innings, player_name, bat_current_team, player_image_url) group by 
     player_id, player_name, bat_current_team, player_image_url'''

            out_batsman_sql = '''select out_batsman_id, round(count(out_batsman_id)) as wicket_cnt
                            from join_data and out_batsman_id<>-1 and innings not in (3,4) group by out_batsman_id'''

            top_scorer_sql = ''' select player_name, player_image_url, team_name, MAX(highest_score) as highest_score,
                        count(distinct match_id) as innings_played from (select  player_name, player_image_url,
                        bat_current_team as team_name, cast(sum(runs) as int) as highest_score, match_id from batsman_overwise_df 
                        group by player_id, not_out, match_id, innings, player_name, bat_current_team, player_image_url
                        order by highest_score desc, player_name asc) group by player_name, player_image_url, team_name'''

            first_last_ball_boundary_sql = '''select player_name, team_name, player_image_url, cast(sum(first_ball_boundary) as int) as 
            first_ball_boundary, cast(sum(last_ball_boundary) as int) as last_ball_boundary, count(distinct match_id) as innings_played 
            from (select batsman_id, batsman_name as player_name, match_id, over_number, batsman_team_id, batsman_team_name as team_name, 
            MIN(ball_number) as first_ball, MAX(ball_number) as last_ball, cast(sum(case when ball_number= 1 and is_extra <> 1 and 
            (is_four = 1 or is_six = 1)then 1 else 0 end) as int) as first_ball_boundary, cast(sum(case when ball_number= 6 
            and is_extra <> 1 and (is_four = 1 or is_six = 1)then 1 else 0 end) as int) as last_ball_boundary, batsman_image_url as player_image_url from 
            join_data where  innings not in (3,4) group by batsman_id, batsman_name, over_number, match_id, batsman_team_id, batsman_team_name, batsman_image_url) 
            group by player_name, team_name, player_image_url'''

        final_highest_score_sql = '''select player_name, team_name, runs, matches, strike_rate, hundreds, fours, 
                    sixes, fifties, coalesce(matches-obd.wicket_cnt,0),  player_image_url, 
                    round(coalesce((hsd.runs*1.00)/obd.wicket_cnt,0.0),2) as average from highest_score_df hsd 
                    left join out_batsman_df obd on (obd.out_batsman_id=hsd.player_id) order by player_name'''

        highest_score_df = executeQuery(con, highestScoresSQL, params)
        out_batsman_df = executeQuery(con, out_batsman_sql, params)
        highestScoresDF = executeQuery(con, final_highest_score_sql)
        top_scorer_df = executeQuery(con, top_scorer_sql, params)
        firstLastBallBoundaryDF = executeQuery(con, first_last_ball_boundary_sql, params)

        if "min_innings" in request.json:
            min_innings = request.json['min_innings']
            highestScoresDF = highestScoresDF[highestScoresDF['matches'] >= min_innings]
            top_scorer_df = top_scorer_df[top_scorer_df['innings_played'] >= min_innings]
            firstLastBallBoundaryDF = firstLastBallBoundaryDF[firstLastBallBoundaryDF['innings_played'] >= min_innings]

        stat_list = ['runs', 'average', 'strike_rate', 'hundreds', 'fifties', 'sixes', 'fours']

        for stat in stat_list:
            highestScoresDF['batsman_rank'] = highestScoresDF[stat].rank(method='first', ascending=False).astype(int)

            highestScoresDF['batsman_details'] = highestScoresDF[
                ['matches', 'runs', 'average', 'strike_rate', 'batsman_rank',
                 'hundreds', 'fifties', 'sixes', 'fours']].to_dict(orient='records')

            response[stat] = highestScoresDF.sort_values(['batsman_rank']).head(10) \
                .groupby(['player_name', 'team_name', 'player_image_url'])['batsman_details'] \
                .agg(list).reset_index().to_dict('records')

            top_scorer_df['batsman_rank'] = top_scorer_df['highest_score'].rank(method='first', ascending=False).astype(
                int)

            top_scorer_df['batsman_details'] = top_scorer_df[['highest_score', 'batsman_rank']].to_dict(
                orient='records')
            response['highest_score'] = top_scorer_df.sort_values(['batsman_rank']).head(10) \
                .groupby(['player_name', 'team_name', 'player_image_url', 'batsman_rank'])['batsman_details'] \
                .agg(list).reset_index().to_dict('records')

        boundary_stat_list = ['first_ball_boundary', 'last_ball_boundary']

        for stats in boundary_stat_list:
            firstLastBallBoundaryDF['batsman_rank'] = firstLastBallBoundaryDF[stats].rank(method='first',
                                                                                          ascending=False).astype(
                int)

            firstLastBallBoundaryDF['batsman_details'] = firstLastBallBoundaryDF[
                ['first_ball_boundary', 'last_ball_boundary', 'batsman_rank']].to_dict(orient='records')

            response[stats] = firstLastBallBoundaryDF.sort_values(['batsman_rank']).head(10) \
                .groupby(['player_name', 'team_name', 'player_image_url'])['batsman_details'] \
                .agg(list).reset_index().to_dict('records')

        return jsonify(response), 200, logger.info("Status - 200")
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/seasonWiseBattingStats", methods=['POST'])
@token_required
def seasonWiseBattingStats():
    logger = get_logger("seasonWiseBattingStats", "seasonWiseBattingStats")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        filter_dict['over_number'] = request.json.get('overs')

        filters, params = generateWhereClause(filter_dict)

        season_batsman_sql = '''select player_id, season, player_name,sum(runs) as total_runs_scored,
        round((sum(runs)*100.00)/sum(balls),2) as strike_rate,
     round(coalesce(sum(runs*1.00)/sum(wickets),0.0),2) as average
     from batsman_overwise_df ''' + filters + ''' group by player_id, season, player_name order by season'''

        season_batsman_df = executeQuery(con, season_batsman_sql, params)

        out_batsman_sql = '''select out_batsman_id, season, round(count(out_batsman_id)) as wicket_cnt
                            from join_data ''' + filters.replace('player_id', 'out_batsman_id').replace(' team_id',
                                                                                                        ' batsman_team_id') \
            .replace('player_skill', 'out_batsman_skill') + ''' and 
                            out_batsman_id<>-1 and innings not in (3,4)  group by out_batsman_id, season '''
        out_batsman_df = executeQuery(con, out_batsman_sql, params)

        final_batsman_stats_sql = '''select bdf.season, player_name, strike_rate, 
                    round(coalesce(total_runs_scored/wicket_cnt,0),2) as average from season_batsman_df bdf 
                    left join out_batsman_df odf on (odf.out_batsman_id=bdf.player_id and odf.season=bdf.season)'''

        season_batsman_stats = executeQuery(con, final_batsman_stats_sql)

        season_batsman_stats['player_details'] = season_batsman_stats[['season', 'average', 'strike_rate']] \
            .to_dict(orient='records')

        response = season_batsman_stats.drop(['season', 'average', 'strike_rate'], axis=1) \
            .groupby(['player_name'])['player_details'] \
            .agg(list).reset_index().to_json(orient='records')

        return response, 200, logger.info("Status - 200")
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/overSlabWiseRunRate", methods=['POST'])
@token_required
def overSlabWiseRunRate():
    logger = get_logger("overSlabWiseRunRate", "overSlabWiseRunRate")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        final_phase_wise_df = pd.DataFrame()

        slabs = ['slab1', 'slab2', 'slab3', 'slab4']

        for slab in slabs:

            if slab in request.json:
                filter_dict['over_number'] = request.json.get(slab)
                filters, params = generateWhereClause(filter_dict)

                phaseWiseRunRateSQL = '''select '{}' as batting_phase, team_name,
            case when (sum(balls)%6)==0 then ((sum(balls)*1.00)/6) else (sum(balls)/6) +
            ((sum(balls)%6)/10.00) end as overs, round((sum(runs)/(case when (sum(balls)%6)==0 then ((sum(balls)*1.00)/6)
            else (sum(balls)/6) + ((sum(balls)%6)/10.00) end)),2) as run_rate
             from batsman_overwise_df '''.format(slab) + filters + ''' group by team_name'''

                final_phase_wise_df = final_phase_wise_df.append(executeQuery(con, phaseWiseRunRateSQL, params))

        final_phase_wise_df['team_run_rate'] = final_phase_wise_df[['batting_phase', 'run_rate']] \
            .to_dict(orient='records')

        response = final_phase_wise_df.drop(['batting_phase', 'run_rate'], axis=1).groupby(['team_name'])[
            'team_run_rate'] \
            .agg(list).reset_index().to_json(orient='records')

        return response, 200, logger.info("Status - 200")
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/batsmanAvgRankingRuns", methods=['POST'])
# @token_required
# def batsmanAvgRankingRuns():
#     logger = get_logger("batsmanAvgRankingRuns", "batsmanAvgRankingRuns")
#     try:
#         filter_dict = globalFilters()
#     except ValidationError as e:
#         logger.error(e.messages)
#         logger.error(e.valid_data)
#         raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
#     try:
#
#         filter_dict['over_number'] = request.json.get('overs')
#
#         if "min_innings" in request.json:
#             min_innings = request.json['min_innings']
#         else:
#             min_innings = 1
#
#         filters, params = generateWhereClause(filter_dict)
#         params.extend(params)
#         batsman_avg_runs = executeQuery(con, '''select player_name, team_name, player_image_url,
#         round(sum(bdf.runs)/sum(coalesce(wicket_cnt,0)),2) as batting_average, MAX(innings) as innings_played from
#         (select player_id, over_number, player_name, bat_current_team as team_name,
#               count(distinct match_id) as innings ,sum(wickets) as wickets,
#                 sum(runs) as runs, player_image_url from batsman_overwise_df ''' + filters +
#                                         ''' group by player_id, player_name,bat_current_team, player_image_url,over_number) bdf
#          left join (select out_batsman_id, over_number, round(count(out_batsman_id)) as wicket_cnt
# from join_data  ''' + filters.replace('player_id', 'out_batsman_id').replace(' team_id', ' batsman_team_id')
#                                         .replace('player_skill', 'out_batsman_skill') + ''' and out_batsman_id<>-1 and
#         innings not in (3,4) group by out_batsman_id, over_number) out on
#          (bdf.player_id=out.out_batsman_id and out.over_number=bdf.over_number)
#                  group by player_name, team_name, player_image_url having sum(coalesce(wicket_cnt,0))>0''' + f" and innings_played >= {min_innings} order by batting_average desc  limit 10",
#                                         params)
#
#         response = batsman_avg_runs.to_json(orient='records')
#         return response, 200, logger.info("Status - 200")
#     except Exception as e:
#         logger.error(e)
#         raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/averageStatsByGround", methods=['POST'])
@token_required
def averageStatsByGround():
    logger = get_logger("averageStatsByGround", "averageStatsByGround")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        filter_dict['over_number'] = request.json.get('overs')
        filters, params = generateWhereClause(filter_dict)

        stats_by_venue_sql = '''select venue as venue_id, stadium_name as venue,
        round((sum(runs)/(case when (sum(balls)%6)==0 then ((sum(balls)*1.00)/6) else (sum(balls)/6) +
        ((sum(balls)%6)/10.00) end)),2) as run_rate, 
        cast(round(cast((sum(fours) + sum(sixes)) as int)/(count(distinct match_id)*count(distinct innings))) as int) as boundaries,
        sum(runs) as runs, count(distinct innings) as innings,
        round(coalesce((sum(runs)*100.00)/sum(balls),0.0),1) as strike_rate, count(distinct match_id) as innings_played,
        cast(coalesce((sum(runs)/(count(distinct match_id)*count(distinct innings))),0) as int) as avg_score 
         from batsman_overwise_df ''' + filters + ''' group by venue, stadium_name'''

        stats_by_venue_df = executeQuery(con, stats_by_venue_sql, params)

        stats_wicket_sql = '''select venue as venue_id, round(count(out_batsman_id)) as wickets 
        from join_data ''' + filters.replace('player_id', 'out_batsman_id').replace(' team_id',
                                                                                    ' batsman_team_id').replace(
            'player_skill', 'out_batsman_skill') + ''' and out_batsman_id<>-1 and 
        innings not in (3,4) group by venue'''

        stats_wickets_df = executeQuery(con, stats_wicket_sql, params)

        venue_sql = '''select venue, run_rate, boundaries, innings_played,
                cast(round(coalesce(swd.wickets,0)/(coalesce(svd.innings_played,0) * coalesce(svd.innings,0)),1) as int) as wickets, 
                avg_score,strike_rate from stats_by_venue_df svd left join stats_wickets_df swd on (svd.venue_id=swd.venue_id)'''

        venue_df = executeQuery(con, venue_sql)
        venue_df['boundaries'] = round(venue_df['boundaries'], 2)
        if "min_innings" in request.json:
            min_innings = request.json['min_innings']
            venue_df = venue_df[venue_df['innings_played'] >= min_innings]

        venue_df['venue_stats'] = venue_df[['avg_score', 'run_rate', 'strike_rate', 'boundaries', 'wickets']] \
            .to_dict(orient='records')

        response = venue_df.drop(['avg_score', 'run_rate', 'strike_rate', 'boundaries', 'wickets'], axis=1) \
            .groupby(['venue'])['venue_stats'].agg(list).reset_index().to_json(orient='records')

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/teamStrikeRate", methods=['POST'])
@token_required
def teamStrikeRate():
    logger = get_logger("teamStrikeRate", "teamStrikeRate")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        filter_dict['over_number'] = request.json.get('overs')
        filters, params = generateWhereClause(filter_dict)

        team_sr_df = executeQuery(con, '''select team_id, team_name, count(distinct match_id) as innings,
        round(coalesce(sum(team_runs)*100.00/sum(balls),0.0),2) as strike_rate 
         from batsman_overwise_df ''' + filters + ''' group by team_id, team_name''', params)

        team_wickets_sql = '''select batsman_team_id, cast(count(out_batsman_id) as int) as wickets 
from join_data ''' + filters.replace('player_id', 'out_batsman_id').replace(' team_id', ' batsman_team_id') \
            .replace('player_skill', 'out_batsman_skill') + ''' and out_batsman_id<>-1 and 
innings not in (3,4) group by batsman_team_id'''

        team_wickets_df = executeQuery(con, team_wickets_sql, params).rename(columns={'batsman_team_id': 'team_id'})
        team_sr_df = team_sr_df.merge(team_wickets_df, on='team_id', how='left')

        response = team_sr_df.to_json(orient='records')

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/playerStrikeRate", methods=['POST'])
# @token_required
# def playerStrikeRate():
#     logger = get_logger("playerStrikeRate", "playerStrikeRate")
#     try:
#         filter_dict = globalFilters()
#     except ValidationError as e:
#         logger.error(e.messages)
#         logger.error(e.valid_data)
#         raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
#     try:
#         if request.json:
#             if "overs" in request.json:
#                 filter_dict['over_number'] = request.json.get('overs')
#
#         filters, params = generateWhereClause(filter_dict)
#
#         if "min_innings" in request.json:
#             min_innings = request.json['min_innings']
#         else:
#             min_innings = 1
#
#         team_sr_df = executeQuery(con, '''select player_name, bat_current_team as team_name, round(coalesce(sum(runs)*100.00/sum(balls),0.0),2)
#         as strike_rate, count(distinct match_id) as innings_played, player_image_url from batsman_overwise_df ''' + filters + '''
#         group by player_id,player_name, bat_current_team, player_image_url''' + f" having count(distinct match_id) >= {min_innings} order by strike_rate desc limit 10",
#                                   params)
#
#         response = team_sr_df.to_json(orient='records')
#
#         return response, 200, logger.info("Status - 200")
#
#     except Exception as e:
#         logger.error(e)
#         raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/mostWickets", methods=['POST'])
@token_required
def mostWickets():
    logger = get_logger("mostWickets", "mostWickets")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        sort_key = "wickets"
        asc = False
        if request.json:
            if "overs" in request.json:
                filter_dict['over_number'] = request.json.get('overs')

                if "asc" in request.json:
                    asc = request.json['asc']

                if "sort_key" in request.json:
                    sort_key = request.json['sort_key']

        filters, params = generateWhereClause(filter_dict)

        if filters:
            params.extend(params)
            most_wickets_sql = '''
            select 
              player_name, 
              bo.player_id, 
              team_name, 
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
                  sum(balls)// 6
                ) + (
                  (
                    sum(balls)% 6
                  )/ 10.00
                ) end, 
                2
              ) as overs, 
              cast(
                sum(dot_balls) as int
              ) as dot_balls, 
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
              cast(
                sum(
                  case when wickets between 5 
                  and 9 then 1 else 0 end
                ) as int
              ) as five_wkt_hauls, 
              mw.bbi_wicket, 
              mw.bbi_runs, 
              player_image_url 
            from 
              (
                select 
                  match_id, 
                  player_name, 
                  player_id, 
                  bowl_current_team as team_name, 
                  sum(balls) as balls, 
                  sum(runs) as runs, 
                  cast(
                    sum(dot_balls) as int
                  ) as dot_balls, 
                  sum(wickets) as wickets, 
                  player_image_url 
                from 
                  bowler_overwise_df ''' + filters + ''' 
                group by 
                  match_id, 
                  player_id, 
                  team_id, 
                  bowl_current_team, 
                  player_name, 
                  player_image_url
              ) bo 
              left join (
                select 
                  player_id, 
                  cast(wickets as int) as bbi_wicket, 
                  cast(runs as int) as bbi_runs 
                from 
                  (
                    select 
                      player_id, 
                      runs, 
                      wickets, 
                      row_number() over (
                        partition by player_id 
                        order by 
                          runs asc
                      ) as rnk 
                    from 
                      (
                        select 
                          player_id, 
                          sum(runs) as runs, 
                          sum(wickets) as wickets, 
                          dense_rank() over (
                            partition by player_id 
                            order by 
                              sum(wickets) desc
                          ) as rnk 
                        from 
                          bowler_overwise_df ''' + filters + ''' 
                        group by 
                          player_id, 
                          match_id
                      ) 
                    where 
                      rnk = 1
                  ) 
                where 
                  rnk = 1
              ) mw on (bo.player_id = mw.player_id) 
            group by 
              bo.player_id, 
              team_name, 
              player_name, 
              mw.bbi_runs, 
              mw.bbi_wicket, 
              player_image_url
            '''

        else:
            params = []
            most_wickets_sql = '''
            select 
              player_name, 
              bo.player_id, 
              team_name, 
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
              cast(
                sum(dot_balls) as int
              ) as dot_balls, 
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
              cast(
                sum(
                  case when wickets between 5 
                  and 9 then 1 else 0 end
                ) as int
              ) as five_wkt_hauls, 
              cast(mw.bbi_wicket as int) as bbi_wicket, 
              cast(mw.bbi_runs as int) as bbi_runs, 
              player_image_url 
            from 
              (
                select 
                  match_id, 
                  player_name, 
                  player_id, 
                  bowl_current_team as team_name, 
                  cast(
                    sum(dot_balls) as int
                  ) as dot_balls, 
                  sum(balls) as balls, 
                  sum(runs) as runs, 
                  cast(
                    sum(dot_balls) as int
                  ) as dot_balls, 
                  sum(wickets) as wickets, 
                  player_image_url 
                from 
                  bowler_overwise_df 
                group by 
                  match_id, 
                  player_id, 
                  team_id, 
                  bowl_current_team, 
                  player_name, 
                  player_image_url
              ) bo 
              left join (
                select 
                  player_id, 
                  cast(wickets as int) as bbi_wicket, 
                  cast(runs as int) as bbi_runs 
                from 
                  (
                    select 
                      player_id, 
                      sum(runs) as runs, 
                      sum(wickets) as wickets, 
                      row_number() over (
                        partition by player_id 
                        order by 
                          sum(wickets) desc
                      ) as rnk 
                    from 
                      bowler_overwise_df 
                    group by 
                      player_id, 
                      match_id
                  ) 
                where 
                  rnk = 1
              ) mw on (bo.player_id = mw.player_id) 
            group by 
              bo.player_id, 
              team_name, 
              player_name, 
              mw.bbi_runs, 
              player_image_url, 
              mw.bbi_wicket
            '''

        if "min_innings" in request.json:
            min_innings = request.json['min_innings']
            most_wickets_sql = most_wickets_sql + f" having matches >= {min_innings}"

        most_wickets_df = executeQuery(con, most_wickets_sql, params)

        if sort_key not in ["player_name", "bbi"]:
            most_wickets_df = most_wickets_df[most_wickets_df[sort_key] > 0].sort_values(sort_key, ascending=asc)
        elif sort_key == "bbi":
            asc2 = True
            if not asc:
                asc2 = True
            elif asc:
                asc2 = False
            most_wickets_df = most_wickets_df[most_wickets_df['bbi_wicket'] > 0].sort_values(
                by=['bbi_wicket', 'bbi_runs'], ascending=[asc, asc2])
        else:
            most_wickets_df = most_wickets_df.sort_values(sort_key, ascending=asc)

        most_wickets_df['bbi'] = most_wickets_df['bbi_wicket'].fillna(0).astype(int).map(str) + '/' + most_wickets_df[
            'bbi_runs'].fillna(0).astype(int).map(str)

        response = most_wickets_df.to_json(orient='records')

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/mostEconomicalBowler", methods=['POST'])
@token_required
def mostEconomicalBowler():
    logger = get_logger("mostEconomicalBowler", "mostEconomicalBowler")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        filter_dict['over_number'] = request.json.get('overs')
        filters, params = generateWhereClause(filter_dict)

        if "min_innings" in request.json:
            min_innings = request.json['min_innings']
        else:
            min_innings = 1

        economical_bowler_df = executeQuery(
            con, '''
            select 
              player_name, 
              bowl_current_team as team_name, 
              count(distinct match_id) as innings_played, 
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
              bowler_overwise_df 
            ''' + filters
                 + ''' group by player_image_url, player_name, bowl_current_team having economy>0 '''
                 + f" and count(distinct match_id) >= {min_innings} order by economy limit 10",
            params
        )
        response = economical_bowler_df.to_json(orient='records')
        return response, 200, logger.info("Status - 200")
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/playerWith750Runs", methods=['POST'])
@token_required
def playerWith750Runs():
    logger = get_logger("playerWith750Runs", "playerWith750Runs")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        del_keys = ['player_skill']
        filter_dict = dropFilter(del_keys, filter_dict)
        if "overs" in request.json:
            filter_dict['over_number'] = request.json.get('overs')
        filters, params = generateWhereClause(filter_dict)
        if "total_runs" in request.json:
            total_runs = request.json['total_runs']
        else:
            total_runs = 250

        player_runs_df = executeQuery(con, '''select  player_id, player_name, bat_current_team as team_name,
        sum(runs) as runs, round(sum(runs)*100.00/sum(balls),2) as strike_rate, count(distinct match_id) as innings_played, player_image_url
     from batsman_overwise_df ''' + filters + f''' group by player_id, player_name, player_image_url, 
     bat_current_team having sum(runs) >= {total_runs} ''', params)

        out_batsman_sql = '''select out_batsman_id, round(count(out_batsman_id)) as wicket_cnt
                from join_data  ''' + filters.replace('player_id', 'out_batsman_id').replace(' team_id',
                                                                                             ' batsman_team_id') + ''' and out_batsman_id<>-1 and innings not in (3,4) 
                 group by out_batsman_id'''
        out_batsman_df = executeQuery(con, out_batsman_sql, params)

        final_player_runs_sql = '''select player_name, team_name, player_image_url, 
                round(coalesce(runs/wicket_cnt, 0), 2) as average, strike_rate, innings_played from player_runs_df pdf left join out_batsman_df odf 
                on (odf.out_batsman_id=pdf.player_id)'''

        if "min_innings" in request.json:
            min_innings = request.json['min_innings']
            final_player_runs_sql = final_player_runs_sql + f" where innings_played >= {min_innings}"

        final_player_runs_df = executeQuery(con, final_player_runs_sql)

        response = final_player_runs_df.to_json(orient='records')

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/overWiseBowlerPerformance", methods=['POST'])
# #@token_required
# def overWiseBowlerPerformance():
#     logger = get_logger("overWiseBowlerPerformance", "overWiseBowlerPerformance")
#     try:
#         filter_dict = globalFilters()
#     except ValidationError as e:
#         logger.error(e.messages)
#         logger.error(e.valid_data)
#         raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
#     try:
#         if 'team_id' in filter_dict:
#             del_keys = ['team_id']
#             filter_dict = dropFilter(del_keys, filter_dict)
#
#         if "team1" in request.json:
#             filter_dict['team_id'] = request.json.get('team1')
#         if "team2" in request.json:
#             filter_dict['batsman_team_id'] = request.json.get('team2')
#
#         filter_dict['over_number'] = request.json.get('overs')
#         filters, params = generateWhereClause(filter_dict)
#
#         overwise_bowler_performance_sql = '''select player_name, bowl_current_team as team_name, over_number as overs,
#             cast(sum(runs) as int) as runs, cast(sum(balls) as int) as balls, cast(sum(wickets) as int) as wickets, bowling_type,
#                 round((sum(dot_balls)*100.00)/sum(balls),2) as dot_percent,
#                 round(sum(runs)/(case when (sum(balls)%6)==0 then ((sum(balls)*1.00)/6) else
#                 (sum(balls)/6) + ((sum(balls)%6)/10.00) end),2) as run_rate
#                 from bowler_overwise_df ''' + filters + '''
#                 group by over_number, player_name, bowl_current_team, bowling_type'''
#
#         if "min_innings" in request.json:
#             min_innings = request.json['min_innings']
#             overwise_bowler_performance_sql = overwise_bowler_performance_sql + f" having count(distinct match_id) >= {min_innings}"
#
#         overwise_bowler_performance = executeQuery(con, overwise_bowler_performance_sql, params)
#
#         overwise_bowler_performance['bowling_stats'] = overwise_bowler_performance[['overs', 'runs', 'balls', 'wickets',
#                                                                                     'bowling_type', 'dot_percent',
#                                                                                     'run_rate']] \
#             .to_dict(orient='records')
#
#         response = overwise_bowler_performance.drop(['overs', 'runs', 'balls', 'wickets', 'bowling_type', 'dot_percent',
#                                                      'run_rate'], axis=1).groupby(['player_name', 'team_name'])[
#             'bowling_stats'].agg(list).reset_index().to_json(orient='records')
#
#         return response, 200, logger.info("Status - 200")
#
#     except Exception as e:
#         logger.error(e)
#         raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/powerPlayBowler", methods=['POST'])
@token_required
def powerPlayBowler():
    logger = get_logger("powerPlayBowler", "powerPlayBowler")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        if "overs" in request.json:
            filter_dict['over_number'] = request.json.get('overs')
        filters, params = generateWhereClause(filter_dict)

        if "min_innings" in request.json:
            min_innings = request.json['min_innings']
        else:
            min_innings = 1

        if "total_balls" in request.json:
            total_balls = request.json['total_balls']
        else:
            total_balls = 60

        if filters:
            pp_bowler_performance_sql = '''select player_name,bowl_current_team as team_name, count(distinct match_id) as innings_played,
    cast(sum(wickets) as int) as wickets, bowling_type, round(coalesce(((sum(runs)*1.00)/sum(wickets)),0.0),2) as average,
    round(coalesce(((sum(balls)*1.00)/sum(wickets)),0.0),2) as strike_rate, player_image_url
    from bowler_overwise_df ''' + filters + ''' and over_number between 1 and 6
    group by player_id, player_image_url, player_name, bowl_current_team, bowling_type''' + f" having sum(balls) >= {total_balls} and count(distinct match_id) >= {min_innings} order by wickets desc limit 10"

        else:
            params = []
            pp_bowler_performance_sql = '''select player_name, bowl_current_team as team_name, count(distinct match_id) as innings_played,
    cast(sum(wickets) as int) as wickets, bowling_type, round(coalesce(((sum(runs)*1.00)/sum(wickets)),0.0),2) as average,
    round(coalesce(((sum(balls)*1.00)/sum(wickets)),0.0),2) as strike_rate, player_image_url
    from bowler_overwise_df where over_number between 1 and 6
    group by player_id, player_image_url, player_name, bowl_current_team,bowling_type''' + f" having sum(balls) >= {total_balls} and count(distinct match_id) >= {min_innings} order by wickets desc limit 10"

        pp_bowler_performance_df = executeQuery(con, pp_bowler_performance_sql, params)

        pp_bowler_performance_df['bowling_stats'] = pp_bowler_performance_df[['wickets', 'average', 'strike_rate']] \
            .to_dict(orient='records')

        pp_bowler_performance_df = pp_bowler_performance_df.drop(
            ['wickets', 'average', 'strike_rate'], axis=1)

        response = pp_bowler_performance_df.groupby(['player_name', 'team_name', 'player_image_url'])['bowling_stats'] \
            .agg(list).reset_index().to_json(orient='records')

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/performanceVSdiffBowlers", methods=['POST'])
@token_required
def performanceVSdiffBowlers():
    logger = get_logger("performanceVSdiffBowlers", "performanceVSdiffBowlers")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        if request.json:
            if "overs" in request.json:
                filter_dict['over_number'] = request.json.get('overs')

        filters, params = generateWhereClause(filter_dict)

        if filters:

            performance_diff_bowlers_sql = '''select player_id, bowling_type as bowler_type,
    count(distinct match_id) as innings, cast(sum(balls) as int) as balls, cast(sum(runs) as int) as runs,
    round(sum(runs)*100.00/sum(balls),2) as batting_strike_rate, cast(sum(wickets) as int) as wickets,
    coalesce(round((sum(runs)*1.00)/sum(wickets),2),0.0) as batting_average,
    round((((sum(sixes)*6)+(sum(fours*4)))*100.00)/sum(runs),2) as boundary_percent,
    round((sum(dot_balls)*100.00/sum(balls)),2) as dot_ball_percent
    from batsman_overwise_df ''' + filters + ''' group by bowling_type, player_id'''

            out_batsman_sql = '''select out_batsman_id, bowling_type, round(count(out_batsman_id)) as wicket_cnt
                            from join_data  ''' + filters.replace('player_id', 'out_batsman_id').replace(' team_id',
                                                                                                         ' batsman_team_id') \
                .replace('player_skill', 'out_batsman_skill') + ''' and out_batsman_id<>-1 and innings not in (3,4) 
                             group by out_batsman_id, bowling_type'''

        else:
            params = []
            performance_diff_bowlers_sql = '''select bowling_type as bowler_type,
    count(distinct match_id) as innings, cast(sum(balls) as int) as balls, sum(runs) as runs,
    round(sum(runs)*100.00/sum(balls),2) as batting_strike_rate, cast(sum(wickets) as wickets as int),
    coalesce(round((sum(runs)*1.00)/sum(wickets),2),0.0) as batting_average,
    round((((sum(sixes)*6)+(sum(fours*4)))*100.00)/sum(runs),2) as boundary_percent,
    round((sum(dot_balls)*100.00/sum(balls)),2) as dot_ball_percent
    from batsman_overwise_df group by bowling_type'''

            out_batsman_sql = '''select out_batsman_id, bowling_type, round(count(out_batsman_id)) as wicket_cnt
                                        from join_data where out_batsman_id<>-1 and innings not in (3,4) 
                                         group by out_batsman_id, bowling_type'''

        performance_bowlers = executeQuery(con, performance_diff_bowlers_sql, params)
        out_batsman_df = executeQuery(con, out_batsman_sql, params)

        performance_diff_bowlers = executeQuery(con, '''select bowler_type, innings, balls, runs, batting_strike_rate, 
                    cast(coalesce(wicket_cnt,0) as int) as wickets, round(coalesce(runs/wicket_cnt, 0), 2) as batting_average, boundary_percent, 
                    dot_ball_percent from performance_bowlers pdf left join out_batsman_df odf 
                            on (odf.out_batsman_id=pdf.player_id and pdf.bowler_type=odf.bowling_type)''')

        performance_diff_bowlers['bowling_stats'] = performance_diff_bowlers[['innings', 'balls',
                                                                              'runs', 'batting_strike_rate', 'wickets',
                                                                              'batting_average',
                                                                              'boundary_percent', 'dot_ball_percent']] \
            .to_dict(orient='records')

        response = performance_diff_bowlers.drop(['innings', 'balls', 'runs', 'batting_strike_rate', 'wickets',
                                                  'batting_average', 'boundary_percent', 'dot_ball_percent'], axis=1) \
            .groupby(['bowler_type'])['bowling_stats'].agg(list).reset_index().to_json(orient='records')

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/multiTeamComparison", methods=['POST'])
# @token_required
# def multiTeamComparison():
#     logger = get_logger("multiTeamComparison", "multiTeamComparison")
#     try:
#         filter_dict = globalFilters()
#     except ValidationError as e:
#         logger.error(e.messages)
#         logger.error(e.valid_data)
#         raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
#     try:
#         filter_dict['over_number'] = request.json.get('overs')
#
#         filters, params = generateWhereClause(filter_dict)
#
#         team_comparison_sql = '''select team_name, team_id, count(distinct match_id) as innings,
#     round(sum(runs)*100.00/sum(balls),2) as strike_rate,cast(round(sum(runs)/count(distinct match_id)) as int) as avg_runs,
#     cast(round(sum(wickets)/count(distinct match_id)) as int) as avg_wickets,
#      round(coalesce(((sum(runs)*1.00)/(case when (sum(balls)%6)==0 then ((sum(balls)*1.00)/6)
#      else (sum(balls)/6) + ((sum(balls)%6)/10.00) end)),0.0),2) as run_rate,
#      cast(sum(dot_balls) as int) as dot_balls from batsman_overwise_df ''' + filters + ''' group by team_id, team_name'''
#
#         team_comparison_df = executeQuery(con, team_comparison_sql, params)
#
#         team_comparison_df['avg_score'] = team_comparison_df['avg_runs'].fillna(0).astype(int).map(str) + '/' + \
#                                           team_comparison_df['avg_wickets'].fillna(0).astype(int).map(str)
#
#         highest_wicket_sql = '''select team_id, player_name, cast(sum(wickets) as int) as wickets
#     from bowler_overwise_df ''' + filters + ''' group by team_id, player_id, player_name'''
#
#         highest_wicket = executeQuery(con, highest_wicket_sql, params)
#
#         if highest_wicket.empty:
#             highest_wicket_df = highest_wicket
#
#         else:
#             highest_wicket['bowler_rank'] = highest_wicket.groupby('team_id')['wickets'].rank(method='first',
#                                                                                               ascending=False).astype(
#                 int)
#             highest_wicket_df = highest_wicket[highest_wicket['bowler_rank'] == 1]
#
#         final_comparison_sql = '''select tc.team_name, tc.innings, tc.strike_rate, tc.run_rate,
#         tc.avg_score, tc.dot_balls, hd.player_name, hd.wickets from team_comparison_df tc
#         left join highest_wicket_df hd on (tc.team_id=hd.team_id)'''
#
#         final_comparison_df = executeQuery(con, final_comparison_sql)
#
#         final_comparison_df['bowler_stats'] = final_comparison_df[['player_name', 'wickets']].to_dict(orient='records')
#         final_comparison_df['team_details'] = final_comparison_df[
#             ['innings', 'strike_rate', 'run_rate', 'avg_score', 'dot_balls', 'bowler_stats']].to_dict(orient='records')
#
#         response = final_comparison_df.drop(['innings', 'strike_rate', 'run_rate', 'avg_score', 'dot_balls',
#                                              'bowler_stats'], axis=1).groupby(['team_name'])['team_details'] \
#             .agg(list).reset_index().to_json(orient='records')
#
#         return response, 200, logger.info("Status - 200")
#
#     except Exception as e:
#         logger.error(e)
#         raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/bestBattingStrikeRate", methods=['POST'])
# @token_required
# def bestBattingStrikeRate():
#     logger = get_logger("bestBattingStrikeRate", "bestBattingStrikeRate")
#     try:
#         filter_dict = globalFilters()
#     except ValidationError as e:
#         logger.error(e.messages)
#         logger.error(e.valid_data)
#         raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
#     try:
#         if request.json:
#             if "overs" in request.json:
#                 filter_dict['over_number'] = request.json.get('overs')
#
#         filters, params = generateWhereClause(filter_dict)
#
#         if "min_innings" in request.json:
#             min_innings = request.json['min_innings']
#         else:
#             min_innings = 1
#
#         if filters:
#             batting_sr_sql = '''select player_name, bat_current_team as team_name, round((sum(runs)*100.00)/sum(balls),2) as strike_rate, count(distinct match_id) as innings_played,
#     cast(max(runs) as int) as runs_in_innings, player_image_url from ( select player_name, bat_current_team, sum(runs) as runs, sum(balls) as balls, match_id, player_image_url
#     from batsman_overwise_df ''' + filters + ''' group by match_id, player_id, player_name, bat_current_team, player_image_url)
#     group by player_name,bat_current_team, player_image_url''' + f" having count(distinct match_id) >= {min_innings} order by strike_rate desc limit 10"
#
#         else:
#             params = []
#             batting_sr_sql = '''select player_name, bat_current_team as team_name, round((sum(runs)*100.00)/sum(balls),2) as strike_rate, count(distinct match_id) as innings_played,
#     cast(max(runs) as int) as runs_in_innings, player_image_url from ( select player_name, bat_current_team, sum(runs) as runs, sum(balls) as balls, match_id, player_image_url
#     from batsman_overwise_df group by match_id, player_id, player_name, bat_current_team) group by player_name, bat_current_team, player_image_url''' + \
#                              f"having count(distinct match_id) >= {min_innings} order by strike_rate desc limit 10"
#
#         response = executeQuery(con, batting_sr_sql, params).to_json(orient='records')
#         return response, 200, logger.info("Status - 200")
#
#     except Exception as e:
#         logger.error(e)
#         raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/matchTimelinePlayerVSTeam", methods=['POST'])
# @token_required
# def matchTimelinePlayerVSTeam():
#     logger = get_logger("matchTimelinePlayerVSTeam", "matchTimelinePlayerVSTeam")
#     try:
#         filter_dict = globalFilters()
#     except ValidationError as e:
#         logger.error(e.messages)
#         logger.error(e.valid_data)
#         raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
#     try:
#         filters_overall, params_overall = generateWhereClause(filter_dict)
#         params_overall.extend(params_overall)
#         if 'player_id' in filter_dict:
#
#             bat_stats_overall_agg = executeQuery(con, '''select player_id as id,
#     round((sum(runs)*100.00)/sum(balls),2) as overall_strike_rate, cast(round(sum(runs)/count(distinct match_id)) as int) as
#     overall_avg_runs, cast(round(sum(balls)/count(distinct match_id)) as int) as overall_avg_balls,
#     coalesce(round((sum(runs)*1.00)/sum(wickets),2),0.0) as overall_average,
#     (select cast(sum(runs) as int) as runs from batsman_overwise_df ''' + filters_overall + ''' group by match_id,
#     player_id order by runs desc limit 1) as overall_highest_score from batsman_overwise_df '''
#                                                  + filters_overall + ''' group by player_id''', params_overall)
#
#         elif 'team_id' in filter_dict:
#             bat_stats_overall_agg = executeQuery(con, '''select team_id as id,
#     round((sum(runs)*100.00)/sum(balls),2) as overall_strike_rate, cast(round(sum(runs)/count(distinct match_id)) as int) as
#     overall_avg_runs, cast(round(sum(balls)/count(distinct match_id)) as int) as overall_avg_balls,
#     coalesce(round((sum(runs)*1.00)/sum(wickets),2),0.0) as overall_average,
#     (select cast(sum(runs) as int) as runs from batsman_overwise_df ''' + filters_overall + ''' group by match_id,
#     team_id order by runs desc limit 1) as overall_highest_score
#      from batsman_overwise_df ''' + filters_overall + ''' group by team_id''', params_overall)
#
#         if "bowler_id" in request.json:
#             filter_dict['bowler_id'] = request.json.get('bowler_id')
#         if "bowler_team_id" in request.json:
#             filter_dict['bowler_team_id'] = request.json.get('bowler_team_id')
#
#         filters, params = generateWhereClause(filter_dict)
#
#         bat_stats_against_sql = ''' select match_id, player_name,player_image_url, bowl_image_url, player_id,team_id, team_name,bowler_id, bowler_name,
#         bowler_team_id, bowler_team_name,bat_current_team, bowl_current_team,
#         count(distinct match_id) as innings, cast(sum(runs) as int) as runs, cast(sum(balls) as int) as balls,
#         cast(sum(wickets) as int) as wickets, winning_type from batsman_overwise_df ''' + filters + \
#                                 ''' group by match_id, player_id, player_name, player_image_url, bowl_image_url, team_name,
#         team_id, bowler_id, bowler_team_id, bowler_name, bowler_team_name, winning_type, bat_current_team, bowl_current_team'''
#
#         bat_stats_against = executeQuery(con, bat_stats_against_sql, params)
#
#         if all(key in filter_dict for key in ('team_id', 'bowler_id')):
#             bat_stats_against_agg = executeQuery(con, '''select team_name, bowl_image_url as bowler_image_url,
#     team_id as id, bowler_name, bowl_current_team as bowler_team_name, count(distinct match_id) as total_matches,
#     round((sum(runs)*100.00)/sum(balls),2) as strike_rate,
#     cast(round(sum(runs)/count(distinct match_id)) as int) as avg_runs,
#     cast(round(sum(balls)/count(distinct match_id)) as int) as avg_balls,
#     coalesce(round((sum(runs)*1.00)/sum(wickets),2),0.0) as average,
#     cast((select sum(runs) from bat_stats_against group by match_id, team_id order by sum(runs) desc limit 1) as int) as highest_score,
#     (select coalesce(count(distinct match_id),0) from  bat_stats_against where winning_type='Winning' group by team_id) as team1_wins,
#     (select coalesce(count(distinct match_id),0) from  bat_stats_against where winning_type='Losing' group by bowler_name)
#     as team2_wins from bat_stats_against group by team_id,bowler_id, team_name, bowl_image_url, bowler_name, bowl_current_team ''')
#
#             bat_stats_against_agg['primary'] = bat_stats_against_agg['team_name']
#             bat_stats_against_agg['secondary'] = bat_stats_against_agg['bowler_name']
#
#         elif all(key in filter_dict for key in ('player_id', 'bowler_team_id')):
#             bat_stats_against_agg = executeQuery(con, '''select  player_name, player_image_url, bat_current_team as team_name, player_id as id,
# bowler_team_name, count(distinct match_id) as total_matches,
# round((sum(runs)*100.00)/sum(balls),2) as strike_rate,
# cast(round(sum(runs)/count(distinct match_id)) as int) as avg_runs,
# cast(round(sum(balls)/count(distinct match_id)) as int) as avg_balls,
# coalesce(round((sum(runs)*1.00)/sum(wickets),2),0.0) as average,
# (select cast(sum(runs) as int) from bat_stats_against group by match_id, player_id order by sum(runs) desc limit 1) as highest_score,
# (select coalesce(count(distinct match_id),0) from  bat_stats_against where winning_type='Winning' group by player_name)
# as team1_wins, (select coalesce(count(distinct match_id),0) from  bat_stats_against where winning_type='Losing'
# group by bowler_team_name) as team2_wins
#  from bat_stats_against group by player_id,bowler_team_id, player_name, player_image_url, bat_current_team, bowler_team_name''')
#
#             bat_stats_against_agg['primary'] = bat_stats_against_agg['player_name']
#             bat_stats_against_agg['secondary'] = bat_stats_against_agg['bowler_team_name']
#
#         elif all(key in filter_dict for key in ('player_id', 'bowler_id')):
#             bat_stats_against_agg = executeQuery(con, '''select  player_name, player_image_url, bowl_image_url as bowler_image_url, bat_current_team as team_name, player_id as id,
# bowler_name, bowl_current_team as bowler_team_name, count(distinct match_id) as total_matches,
# round((sum(runs)*100.00)/sum(balls),2) as strike_rate,
# cast(round(sum(runs)/count(distinct match_id)) as int) as avg_runs,
# cast(round(sum(balls)/count(distinct match_id)) as int) as avg_balls,
# coalesce(round((sum(runs)*1.00)/sum(wickets),2),0.0) as average,
# (select cast(sum(runs) as int) from bat_stats_against group by match_id, player_id order by sum(runs) desc limit 1) as highest_score,
# (select coalesce(count(distinct match_id),0) from  bat_stats_against where winning_type='Winning' group by player_name)
# as team1_wins, (select coalesce(count(distinct match_id),0) from  bat_stats_against where winning_type='Losing'
#  group by bowler_name) as team2_wins from bat_stats_against group by player_id, bowler_id, player_name, player_image_url,bowl_image_url,
#  bowler_name, bat_current_team, bowl_current_team''')
#             bat_stats_against_agg['primary'] = bat_stats_against_agg['player_name']
#             bat_stats_against_agg['secondary'] = bat_stats_against_agg['bowler_name']
#
#         merge_stats_df = pd.merge(bat_stats_against_agg, bat_stats_overall_agg, on='id', how='left')
#         merge_stats_df[['team1_wins', 'team2_wins']] = merge_stats_df[['team1_wins', 'team2_wins']].fillna(0).astype(
#             int)
#
#         merge_stats_df['strike_rate'] = merge_stats_df[['strike_rate', 'overall_strike_rate']].to_dict(orient='records')
#         merge_stats_df['avg_runs'] = merge_stats_df[['avg_runs', 'overall_avg_runs']].astype(int).to_dict(
#             orient='records')
#         merge_stats_df['avg_balls'] = merge_stats_df[['avg_balls', 'overall_avg_balls']].astype(int).to_dict(
#             orient='records')
#         merge_stats_df['average'] = merge_stats_df[['average', 'overall_average']].to_dict(orient='records')
#         merge_stats_df['highest_score'] = merge_stats_df[['highest_score', 'overall_highest_score']].to_dict(
#             orient='records')
#
#         response = merge_stats_df.rename(columns={"team_name": "team1", "bowler_team_name": "team2"}) \
#             .drop(
#             ['overall_strike_rate', 'overall_avg_runs', 'overall_avg_balls',
#              'overall_highest_score', 'overall_average', 'id'], axis=1).to_json(orient='records')
#
#         return response, 200, logger.info("Status - 200")
#
#     except Exception as e:
#         logger.error(e)
#         raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/matchTimelineTeamVSTeam", methods=['POST'])
# @token_required
# def matchTimelineTeamVSTeam():
#     logger = get_logger("matchTimelineTeamVSTeam", "matchTimelineTeamVSTeam")
#     try:
#         filter_dict = globalFilters()
#     except ValidationError as e:
#         logger.error(e.messages)
#         logger.error(e.valid_data)
#         raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
#     try:
#         if "team1" in request.json:
#             filter_dict['team1'] = request.json.get('team1')
#         if "team2" in request.json:
#             filter_dict['team2'] = request.json.get('team2')
#
#         del_keys = ['batting_type', 'bowling_type', 'player_id', 'team_id']
#
#         filters, params = generateWhereClause(dropFilter(del_keys, filter_dict))
#
#         head2head_stats_df1 = executeQuery(con, '''select season, venue, stadium_name, match_id, match_date, team1,
#         team1_name, team2, team2_name, winning_team_id, winning_team, team1_short_name, team2_short_name,team1_score,
#         team1_wickets, team1_overs, team2_score, team2_wickets, team2_overs, match_result from matches_join_data ''' \
#                                            + filters, params)
#
#         filter_dict1 = filter_dict
#         filter_dict1['team1'], filter_dict1['team2'] = filter_dict1['team2'], filter_dict1['team1']
#         filters1, params1 = generateWhereClause(dropFilter(del_keys, filter_dict1))
#
#         head2head_stats_df2 = executeQuery(con, '''select season, venue, stadium_name, match_id, match_date, team1 as team2,
#         team1_name as team2_name, team1_short_name as team2_short_name, team2 as team1,team2_name as team1_name,
#         team2_short_name as team1_short_name, team1_score as team2_score, team1_wickets as team2_wickets, team1_overs as
#         team2_overs, team2_score as team1_score, team2_wickets as team1_wickets, team2_overs as team1_overs,
#         match_result, winning_team_id, winning_team from matches_join_data ''' + filters1, params1)
#
#         head2head_stats_df = head2head_stats_df1.append(head2head_stats_df2, ignore_index=True)
#
#         team_total_stats_df = executeQuery(con, '''select team1 as team1_id, team1_name as team1, team2 as team2_id,
#         team2_name as team2, count(match_id) as total_matches,
#          cast(sum(case when team1=winning_team_id then 1 else 0 end) as int) as
#         team1_wins, cast(sum(case when team2=winning_team_id then 1 else 0 end) as int) as  team2_wins
#         from head2head_stats_df group by team1, team1_name, team2, team2_name''')
#
#         # Getting batsman with highest score in a match from team1
#         bat_card_data['batsman_rank'] = bat_card_data.groupby(['match_id', 'batting_team_id'])['runs'] \
#             .rank(method='first', ascending=False).astype(int)
#
#         highest_score_df = bat_card_data[bat_card_data['batsman_rank'] == 1][
#             ['match_id', 'batsman_id', 'batting_team_id',
#              'runs', 'balls']]
#
#         ipl_players_data = players_data_df[players_data_df['competition_name'] == 'IPL']
#
#         highest_score_df = pd.merge(highest_score_df, ipl_players_data[['player_id', 'player_name']],
#                                     left_on='batsman_id', right_on='player_id',
#                                     how='left').drop(['batsman_id'], axis=1).rename(
#             columns={'batting_team_id': 'team_id'})
#
#         highest_score_df = pd.merge(highest_score_df, teams_data[['team_id', 'team_name']],
#                                     on='team_id', how='left')
#
#         highest_score_df['best_batting'] = highest_score_df[['player_name', 'runs', 'balls', 'team_name']].to_dict(
#             orient='records')
#
#         # Getting bowler with highest wickets in a match from team2
#
#         bowl_card_data['extras'] = bowl_card_data['no_balls'] + bowl_card_data['wides']
#
#         bowl_card_data['bowler_rank'] = bowl_card_data.groupby(['match_id', 'team_id'])['wickets'].rank(method='first',
#                                                                                                         ascending=False).astype(
#             int)
#         highest_wickets_df = bowl_card_data[bowl_card_data['bowler_rank'] == 1][['match_id', 'bowler_id', 'team_id',
#                                                                                  'wickets', 'economy', 'extras', 'runs',
#                                                                                  'overs']]
#
#         highest_wickets_df = pd.merge(highest_wickets_df, ipl_players_data[['player_id', 'player_name']],
#                                       left_on='bowler_id', right_on='player_id',
#                                       how='left').drop(['bowler_id'], axis=1)
#
#         highest_wickets_df = pd.merge(highest_wickets_df, teams_data[['team_id', 'team_name']],
#                                       on='team_id', how='left')
#
#         highest_wickets_df['best_bowling'] = highest_wickets_df[
#             ['player_name', 'team_name', 'runs', 'overs', 'extras', 'economy', 'wickets']] \
#             .to_dict(orient='records')
#
#         final_stats_df = pd.merge(head2head_stats_df, highest_score_df[['match_id', 'team_id', 'best_batting']],
#                                   left_on=['match_id', 'team1'], right_on=['match_id', 'team_id']) \
#             .drop(['team_id'], axis=1)
#
#         final_stats_df = pd.merge(final_stats_df, highest_wickets_df[['match_id', 'team_id', 'best_bowling']],
#                                   left_on=['match_id', 'team2'], right_on=['match_id', 'team_id']).drop(['team_id'],
#                                                                                                         axis=1)
#
#         final_stats_df = pd.merge(final_stats_df, team_total_stats_df[['total_matches', 'team1_wins', 'team2_wins'
#             , 'team1_id', 'team2_id']],
#                                   left_on=['team1', 'team2'],
#                                   right_on=['team1_id', 'team2_id']).drop(['team1_id', 'team2_id'], axis=1)
#
#         final_stats_df['team1_stats'] = final_stats_df[
#             ['team1_short_name', 'team1_score', 'team1_overs', 'team1_wickets']] \
#             .to_dict(orient='records')
#         final_stats_df['team2_stats'] = final_stats_df[
#             ['team2_short_name', 'team2_score', 'team2_overs', 'team2_wickets']] \
#             .to_dict(orient='records')
#
#         final_stats_df['match_details'] = final_stats_df[
#             ['match_id', 'match_date', 'season', 'stadium_name', 'team1_wickets',
#              'winning_team', 'match_result', 'team1_stats', 'team2_stats',
#              'best_batting', 'best_bowling']].sort_values(by='match_id') \
#             .to_dict(orient='records')
#
#         response = final_stats_df.drop(['team1_score', 'team1_overs', 'team1_wickets', 'venue', 'team2_stats',
#                                         'team2_score', 'team2_overs', 'team2_wickets', 'match_id', 'team1_stats',
#                                         'match_date', 'season', 'stadium_name', 'team1_wickets', 'team1_short_name',
#                                         'winning_team_id', 'team2_short_name', 'winning_team', 'match_result'],
#                                        axis=1).groupby(
#             ['total_matches', 'team1_wins', 'team2_wins', 'team1_name', 'team2_name']) \
#             ['match_details'].agg(list).reset_index().to_json(orient='records')
#
#         return response, 200, logger.info("Status - 200")
#
#     except Exception as e:
#         logger.error(e)
#         raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/matchStats", methods=['GET'])
@token_required
def matchStats():
    logger = get_logger("matchStats", "matchStats")
    try:
        req = dict()
        req['match_id'] = int(request.args.get('match_id'))
        validateRequest(req)
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Bad Request --> Invalid Input!!", 400))
    try:
        match_id = request.args.get('match_id')
        response = {}
        matches_data_df1 = executeQuery(
            con, '''select match_id, match_date, team1 as team, team1_score as team_score,
         team1_overs as team_overs, team1_wickets as team_wickets, match_result from matches_df where match_id=?''',
            [match_id]
        )
        matches_data_df2 = executeQuery(
            con,
            '''select match_id, match_date, team2 as team, team2_score as team_score,
         team2_overs as team_overs, team2_wickets as team_wickets, match_result from matches_df where match_id=?''',
            [match_id]
        )

        final_matches_data = matches_data_df1.append(matches_data_df2)

        bat_card_df = executeQuery(
            con,
            '''
            select 
              bd.match_id, 
              bd.innings, 
              bd.batting_team_id, 
              td.team_name as batting_team_name, 
              bd.batsman_id, 
              pd.player_name as batsman_name, 
              bd.out_desc, 
              bd.runs, 
              bd.batting_position, 
              bd.balls, 
              pd.player_image_url as batsman_image_url, 
              bd.fours, 
              bd.sixes, 
              bd.strike_rate, 
              bd.dot_balls, 
              bd.ones, 
              bd.twos, 
              bd.threes 
            from 
              bat_card_data bd 
              left join players_data_df pd on (
                bd.batsman_id = pd.player_id 
                and bd.competition_name = pd.competition_name
              ) 
              left join teams_data td on (bd.batting_team_id = td.team_id) 
            where 
              match_id = ?
            ''',
            [match_id]
        )

        bat_card_unique = bat_card_df[['batting_team_id', 'innings']].drop_duplicates()

        final_matches_data = pd.merge(
            final_matches_data[['match_date', 'team', 'team_score', 'team_overs', 'team_wickets', 'match_result']],
            bat_card_unique,
            how='left',
            left_on='team',
            right_on='batting_team_id'
        ).drop('batting_team_id', axis=1)

        final_matches_data = pd.merge(
            final_matches_data,
            teams_data[['team_id', 'team_name', 'team_short_name']],
            how='left',
            left_on='team',
            right_on='team_id'
        ).drop('team_id', axis=1)

        final_matches_data['team_total'] = final_matches_data[[
            'team_score', 'team_name', 'team_short_name', 'team_overs', 'team_wickets'
        ]].to_dict('records')
        bat_card_df['out_desc'] = bat_card_df['out_desc'].apply(lambda x: x.replace("NA", ""))
        bat_card_df['bat_stats'] = bat_card_df[[
            'batsman_name', 'batsman_id', 'batting_team_name', 'batting_position', 'runs', 'balls', 'out_desc',
            'batsman_image_url', 'fours', 'sixes', 'strike_rate', 'dot_balls', 'ones', 'twos', 'threes'
        ]].to_dict(orient='records')
        bat_card_df = bat_card_df.drop(
            [
                'batting_team_name', 'batsman_id', 'batting_position', 'runs', 'balls', 'batsman_id',
                'batsman_image_url',
                'fours', 'sixes', 'strike_rate', 'dot_balls', 'ones', 'twos', 'threes'
            ], axis=1
        )

        bowl_card_df = executeQuery(
            con,
            '''
            select 
              bd.match_id, 
              bd.team_id, 
              td.team_name as bowling_team_name, 
              bd.bowler_id, 
              pd.player_name as bowler_name, 
              bd.innings, 
              bd.wickets, 
              bd.overs, 
              bd.runs, 
              bd.economy, 
              bd.bowling_order, 
              pd.player_image_url as bowler_image_url 
            from 
              bowl_card_data bd 
              left join players_data_df pd on (
                bd.bowler_id = pd.player_id 
                and bd.competition_name = pd.competition_name
              ) 
              left join teams_data td on (bd.team_id = td.team_id) 
            where 
              match_id = ?
            ''',
            [match_id]
        )

        bowl_card_df['bowl_stats'] = bowl_card_df[[
            'bowler_name', 'bowling_team_name', 'bowler_id', 'bowling_order', 'wickets', 'overs', 'runs', 'economy',
            'bowler_image_url'
        ]].to_dict(orient='records')
        bowl_card_df = bowl_card_df.drop([
            'bowler_name', 'bowling_team_name', 'bowler_id', 'wickets', 'overs', 'runs', 'economy', 'bowler_id',
            'bowler_image_url'
        ], axis=1
        )

        extras_data_df = executeQuery(
            con,
            '''select match_id, innings, team_id, total_extras, no_balls, byes,wides, 
        leg_byes from extras_data where match_id=?''',
            [match_id]
        )
        extras_data_df['innings_extras'] = extras_data_df[[
            'total_extras', 'no_balls', 'byes', 'wides', 'leg_byes'
        ]].to_dict(orient='records')

        ball_by_ball_df = executeQuery(
            con,
            '''
            select 
              match_id, 
              batsman_name, 
              bowler_name, 
              innings, 
              over_number, 
              ball_number, 
              over_text, 
              runs, 
              extras, 
              is_wicket as wicket, 
              is_bowler_wicket, 
              coalesce(x_pitch, 0.0) as x_pitch, 
              coalesce(y_pitch, 0.0) as y_pitch, 
              is_wide, 
              is_no_ball, 
              is_leg_bye, 
              is_bye 
            from 
              join_data 
            where 
              match_id = ?
            ''',
            [match_id]
        )

        ball_by_ball_df['ball_stats'] = ball_by_ball_df[[
            'runs', 'extras', 'wicket', 'x_pitch', 'y_pitch', 'is_wide', 'is_no_ball', 'is_leg_bye', 'is_bye',
            'is_bowler_wicket'
        ]].to_dict('records')
        ball_by_ball_df['over_details'] = ball_by_ball_df[[
            'over_number',
            'ball_number',
            'batsman_name',
            'over_text',
            'ball_stats'
        ]].to_dict('records')

        ball_by_ball_df = ball_by_ball_df.sort_values(['innings', 'over_number', 'ball_number'])

        response['innings1_batting'] = bat_card_df[bat_card_df['innings'] == 1].groupby(
            ['match_id', 'innings', 'batting_team_id']
        )['bat_stats'].agg(list).reset_index().to_dict('records')

        response['innings1_total'] = final_matches_data[final_matches_data['innings'] == 1]['team_total'].to_list()

        response['innings1_bowling'] = bowl_card_df[bowl_card_df['innings'] == 1].groupby(
            ['match_id', 'innings', 'team_id']
        )['bowl_stats'].agg(list).reset_index().to_dict('records')

        response['innings1_dnb'] = bat_card_df[
            (bat_card_df['out_desc'] == '') & (bat_card_df['innings'] == 1)
            ].groupby(['innings'])[['batsman_name']].agg(list).reset_index().to_dict('records')

        response['innings1_extras'] = extras_data_df[extras_data_df['innings'] == 1][[
            'innings_extras'
        ]].to_dict('records')

        response['innings1_overs'] = ball_by_ball_df[ball_by_ball_df['innings'] == 1].set_index(
            'bowler_name'
        ).groupby(['bowler_name'])['over_details'].agg(list).to_dict()

        response['innings2_batting'] = bat_card_df[bat_card_df['innings'] == 2].groupby(
            ['match_id', 'innings', 'batting_team_id']
        )['bat_stats'].agg(list).reset_index().to_dict('records')

        response['innings2_total'] = final_matches_data[final_matches_data['innings'] == 2]['team_total'].to_list()

        response['innings2_bowling'] = bowl_card_df[bowl_card_df['innings'] == 2].groupby(
            ['match_id', 'innings', 'team_id']
        )['bowl_stats'].agg(list).reset_index().to_dict('records')

        response['innings2_dnb'] = bat_card_df[
            (bat_card_df['out_desc'] == '') & (bat_card_df['innings'] == 2)
            ].groupby(['innings'])[['batsman_name']].agg(list).reset_index().to_dict('records')

        response['innings2_extras'] = extras_data_df[extras_data_df['innings'] == 2][[
            'innings_extras'
        ]].to_dict('records')

        response['innings2_overs'] = ball_by_ball_df[ball_by_ball_df['innings'] == 2].set_index(
            'bowler_name'
        ).groupby(['bowler_name'])['over_details'].agg(list).to_dict()

        response['match_result'] = final_matches_data['match_result'].head(1).to_list()
        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/getPastMatches", methods=['POST'])
@token_required
def getPastMatches():
    logger = get_logger("getPastMatches", "getPastMatches")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        over_mapping = {
            1.6: 2.0,
            2.6: 3.0,
            3.6: 4.0,
            4.6: 5.0,
            5.6: 6.0,
            6.6: 7.0,
            7.6: 8.0,
            8.6: 9.0,
            9.6: 10.0,
            10.6: 11.0,
            11.6: 12.0,
            12.6: 13.0,
            13.6: 14.0,
            14.6: 15.0,
            15.6: 16.0,
            16.6: 17.0,
            17.6: 18.0,
            18.6: 19.0,
            19.6: 20.0,
            20.6: 21.0,
            21.6: 22.0,
            22.6: 23.0,
            23.6: 24.0,
            24.6: 25.0,
            25.6: 26.0,
            26.6: 27.0,
            27.6: 28.0,
            28.6: 29.0,
            29.6: 30.0,
            30.6: 31.0,
            31.6: 32.0,
            32.6: 33.0,
            33.6: 34.0,
            34.6: 35.0,
            35.6: 36.0,
            36.6: 37.0,
            37.6: 38.0,
            38.6: 39.0,
            39.6: 40.0,
            40.6: 41.0,
            41.6: 42.0,
            42.6: 43.0,
            43.6: 44.0,
            44.6: 45.0,
            45.6: 46.0,
            46.6: 47.0,
            47.6: 48.0,
            48.6: 49.0,
            49.6: 50.0
        }
        if filter_dict:
            if "season" not in filter_dict:
                filter_dict["season"] = matches_join_data['season'].max()

        if request.json:
            if "match_id" in request.json:
                filter_dict['match_id'] = request.json.get('match_id')

        del_keys = ['batting_type', 'bowling_type', 'player_id', 'innings', 'winning_type', 'player_skill']
        filters, params = generateWhereClause(dropFilter(del_keys, filter_dict))

        if filters:
            matches_data_df1 = executeQuery(con, '''select season, match_id, stadium_name, match_date, team1 as team, team1_name
        as team_name, team1_short_name as team_short_name, team1_score as team_score, team1_overs as team_overs,case when
        winning_team_id=team1 then 1 else 0 end as is_won,team1_wickets as team_wickets, match_result
        from matches_join_data ''' + filters, params)

            matches_data_df2 = executeQuery(con, '''select season,match_id, stadium_name, match_date, team2 as team, team2_score as
        team_score, team2_name as team_name, team2_short_name as team_short_name, team2_overs as team_overs,
        case when winning_team_id=team2 then 1 else 0 end as is_won , team2_wickets as team_wickets,
        match_result from matches_join_data ''' + filters, params)

        else:
            matches_data_df1 = executeQuery(con, '''select season, match_id, stadium_name, match_date, team1 as team, team1_name as
        team_name, team1_short_name as team_short_name, team1_score as team_score, team1_overs as team_overs,case when
        winning_team_id=team1 then 1 else 0 end as is_won,team1_wickets as team_wickets, match_result
        from matches_join_data ''')
            matches_data_df2 = executeQuery(con, '''select season,match_id, stadium_name, match_date, team2 as team, 
        team2_score as team_score, team2_name as team_name, team2_short_name as team_short_name, team2_overs as team_overs,
        case when winning_team_id=team2 then 1 else 0 end as is_won , team2_wickets as team_wickets,
        match_result from matches_join_data ''')

        matches_data_df1['team_overs'] = matches_data_df1['team_overs'].replace(over_mapping)
        matches_data_df2['team_overs'] = matches_data_df2['team_overs'].replace(over_mapping)
        matches_data_df = matches_data_df1.append(matches_data_df2)

        matches_data_df['summary'] = matches_data_df[
            ['team', 'team_name', 'team_short_name', 'team_score', 'team_overs',
             'team_wickets', 'is_won']].to_dict('records')

        response = matches_data_df.groupby(['match_id', 'season', 'stadium_name', 'match_date', 'match_result'])[
            'summary'] \
            .agg(list).reset_index().to_json(orient='records')

        return response, 200, logger.info("Status - 200")
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/getPlayerLastMatches", methods=['POST'])
# @token_required
# def getPlayerLastMatches():
#     logger = get_logger("getPlayerLastMatches", "getPlayerLastMatches")
#     try:
#         filter_dict = globalFilters()
#     except ValidationError as e:
#         logger.error(e.messages)
#         logger.error(e.valid_data)
#         raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
#     try:
#         filters, params = generateWhereClause(filter_dict)
#         batsman_stats = executeQuery(con, '''select match_id, player_id, player_name, team_id, team_name, bowler_team_id as
#     against_team_id, bowler_team_name as against_team, match_result, case when winning_type='Winning' then team_id when
#     winning_type='No Result' then 0 else bowler_team_id end as winning_team, cast(sum(runs) as int) as runs,
#     cast(sum(balls) as int) as balls, cast(not_out as int) as not_out from batsman_overwise_df ''' +
#                                      filters + ''' group by match_id, player_id, player_name, team_id, team_name,
#                                      bowler_team_id, bowler_team_name, match_result, winning_type, not_out''', params)
#
#         batsman_stats['match_rank'] = batsman_stats['match_id'].rank(method='dense', ascending=False).astype(int)
#
#         batsman_stats = batsman_stats[batsman_stats['match_rank'] <= 20].sort_values(['match_rank'])
#
#         response = batsman_stats.to_json(orient='records')
#
#         return response, 200, logger.info("Status - 200")
#     except Exception as e:
#         logger.error(e)
#         raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/dismissalType", methods=['POST'])
@token_required
def dismissalType():
    logger = get_logger("dismissalType", "dismissalType")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        if request.json:
            if "match_id" in request.json:
                filter_dict['match_id'] = request.json.get('match_id')
            if "overs" in request.json:
                filter_dict['over_number'] = request.json.get('overs')

        filters, params = generateWhereClause(filter_dict)

        if filters:
            dismissal_type_df = executeQuery(con, '''select  wicket_type, count(wicket_type) as count from 
            join_data ''' + filters.replace('player_id', 'out_batsman_id').replace(' team_id', ' batsman_team_id') + '''and wicket_type<> '' group by 
            wicket_type''', params)

        else:
            dismissal_type_df = executeQuery(con, '''select  wicket_type, count(wicket_type) as count from 
            join_data where  wicket_type<> '' group by wicket_type''')

        response = dismissal_type_df.to_json(orient='records')
        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/powerPlayAttackingShot", methods=['POST'])
@token_required
def powerPlayAttackingShot():
    logger = get_logger("powerPlayAttackingShot", "powerPlayAttackingShot")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        if "overs" in request.json:
            filter_dict['over_number'] = request.json.get('overs')
        filters, params = generateWhereClause(filter_dict)

        if filters:
            attacking_shot_sql = '''
            select 
              bat_current_team_id as team_id, 
              bat_current_team as team_name, 
              round(
                (
                  sum(fours)+ sum(sixes)
                )* 100.00 / sum(balls), 
                2
              ) as attacking_per, 
              round(
                coalesce(
                  (
                    (
                      sum(runs)* 1.00
                    )/(
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
                      ) end
                    )
                  ), 
                  0.0
                ), 
                2
              ) as run_rate, 
              count(distinct match_id) as innings_played, 
              cast(
                sum(team_runs) as int
              ) as team_runs, 
              count(distinct innings) as innings, 
            from 
              batsman_overwise_df ''' + filters + ''' 
            group by 
              bat_current_team_id, 
              bat_current_team
            '''

            out_batsman_sql = '''
            select 
              batsman_team_id, 
              round(
                count(out_batsman_id)
              ) as wicket_cnt 
            from 
              join_data ''' + filters.replace(
                'team_id',
                'batsman_team_id'
            ).replace(
                "player_id",
                "out_batsman_id"
            ).replace(
                'player_skill',
                'out_batsman_skill'
            ) + ''' 
              and out_batsman_id <>-1 
              and innings not in (3, 4) 
            group by 
              batsman_team_id
            '''
        else:
            params = []
            attacking_shot_sql = '''
            select 
              team_id, 
              bat_current_team as team_name, 
              round(
                (
                  sum(fours)+ sum(sixes)
                )* 100.00 / sum(balls), 
                2
              ) as attacking_per, 
              round(
                coalesce(
                  (
                    (
                      sum(team_runs)* 1.00
                    )/(
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
                      ) end
                    )
                  ), 
                  0.0
                ), 
                2
              ) as run_rate, 
              count(distinct match_id) as innings_played, 
              cast(
                sum(team_runs) as int
              ) as team_runs, 
              count(distinct innings) as innings, 
            from 
              batsman_overwise_df 
            group by 
              team_id, 
              bat_current_team
            '''

            out_batsman_sql = '''
            select 
              batsman_team_id, 
              round(
                count(out_batsman_id)
              ) as wicket_cnt 
            from 
              join_data 
            where 
              out_batsman_id <>-1 
              and innings not in (3, 4) 
            group by 
              batsman_team_id
            '''

        out_batsman_df = executeQuery(con, out_batsman_sql, params)
        attacking_shot_df = executeQuery(con, attacking_shot_sql, params)

        final_attacking_shot_sql = '''
        select 
          team_name, 
          attacking_per, 
          innings_played, 
          team_runs, 
          round(
            coalesce(
              (team_runs * 1.00)/ coalesce(wicket_cnt, 0), 
              0.0
            ), 
            2
          ) as average, 
          run_rate, 
          cast(
            coalesce(wicket_cnt, 0) as int
          ) as no_of_dismissals 
        from 
          attacking_shot_df asd 
          left join out_batsman_df odf on (
            asd.team_id = odf.batsman_team_id
          )
        '''

        final_attacking_shot_df = executeQuery(con, final_attacking_shot_sql)

        if "min_innings" in request.json:
            min_innings = request.json['min_innings']
            final_attacking_shot_df = final_attacking_shot_df[
                final_attacking_shot_df['innings_played'] >= min_innings
                ]

        final_attacking_shot_df['team_details'] = final_attacking_shot_df[[
            'team_name',
            'attacking_per',
            'run_rate',
            'average',
            'no_of_dismissals'
        ]].to_dict('records')
        response = final_attacking_shot_df.set_index('team_name')['team_details'].to_dict()
        return jsonify(response), 200, logger.info("Status - 200")
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/generalStats", methods=['GET'])
# @token_required
# def generalStats():
#     logger = get_logger("generalStats", "generalStats")
#     try:
#         req = dict()
#         req['season'] = int(request.args.get('season'))
#         validateRequest(req)
#     except Exception as e:
#         logger.error(e)
#         raise HTTPException(response=Response(f"Bad Request --> Invalid Input!!", 400))
#     try:
#         season = request.args.get('season')
#         gen_stats_sql1 = f'''select md.team1 as team, count(md.match_id) as matches,
#         sum(case when md.team1=md.winning_team then 1 else 0 end) as team_wins,
#         sum(case when (md.team1<>md.winning_team and md.winning_team<>-1) then 1 else 0 end) as team_losses, sum(team1_score) as total_score,
#         sum(coalesce(floor(team1_overs)*6 + floor((team1_overs%1)*10), 0)) as total_balls, sum(team2_wickets) as wickets_taken
#          from matches_df md where md.competition_name='IPL' and md.season=? group by md.team1'''
#
#         gen_stats_df1 = executeQuery(con, gen_stats_sql1, [season])
#
#         gen_stats_sql2 = f'''select md.team2 as team, count(md.match_id) as matches,
#         sum(case when md.team2=md.winning_team then 1 else 0 end) as team_wins,
#         sum(case when (md.team2<>md.winning_team and md.winning_team<>-1) then 1 else 0 end) as team_losses,
#         sum(team2_score) as total_score,
#          sum(coalesce(floor(team2_overs)*6 + floor((team2_overs%1)*10), 0)) as total_balls, sum(team1_wickets) as wickets_taken
#           from matches_df md where md.competition_name='IPL' and md.season=? group by md.team2'''
#
#         gen_stats_df2 = executeQuery(con, gen_stats_sql2, [season])
#
#         general_stats_query = f'''select td.team_short_name, cast((gs1.matches+gs2.matches) as int) as matches,
#         cast((gs1.team_wins+gs2.team_wins) as int) as team_wins, cast((gs1.team_losses+gs2.team_losses) as int) as team_losses,
#         round(((gs1.team_wins+gs2.team_wins)*1.00/(gs1.matches+gs2.matches))*100.00,2) as win_percent,
#         cast(round((gs1.total_score+gs2.total_score)*1.00/(gs1.matches+gs2.matches),0) as int) as avg_score,
#         round(((gs1.total_score+gs2.total_score)*1.00/(gs1.total_balls+gs2.total_balls)*100.00),2) as strike_rate,
#         cast((gs1.wickets_taken+gs2.wickets_taken) as int) as wickets_taken, cast(td.titles as int) as titles,
#          cast(bd.fours as int) as fours, cast(bd.sixes as int) as sixes from gen_stats_df1 gs1 inner join gen_stats_df2 gs2 on
#          (gs1.team=gs2.team) inner join (select team_id, team_name, team_short_name, titles from teams_data) td on
#          (gs1.team=td.team_id) inner join (select batting_team_id as team_id, sum(fours) as fours, sum(sixes) as sixes from bat_card_data
#         where competition_name='IPL' and season=? group by team_id) bd on (bd.team_id=gs1.team)'''
#
#         response = executeQuery(con, general_stats_query, [season]).to_json(orient='records')
#
#         return response, logger.info("Status - 200")
#
#     except Exception as e:
#         logger.error(e)
#         raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/head2HeadStats", methods=['POST'])
@token_required
def head2HeadStats():
    logger = get_logger("head2HeadStats", "head2HeadStats")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:

        map_dict = {
            "player_id": ["player_name", "player_image_url"],
            "team_id": ["team_name"],
            "batsman_id": ["batsman_name", "batsman_image_url"],
            "bowler_id": ["bowler_name", "bowl_image_url"],
            "bowler_team_id": ["bowler_team_name"],
            "batsman_team_id": ["batsman_team_name"]
        }

        exclude_group_list = ['over_number', 'comparison_type', 'venue', 'bowling_type', 'batting_type',
                              'innings', 'winning_type', 'season', 'competition_name']

        if request.json:
            if "bowler_id" in request.json:
                filter_dict['bowler_id'] = request.json.get('bowler_id')
            if "bowler_team_id" in request.json:
                filter_dict['bowler_team_id'] = request.json.get('bowler_team_id')
            if "batsman_id" in request.json:
                filter_dict['batsman_id'] = request.json.get('batsman_id')
            if "batsman_team_id" in request.json:
                filter_dict['batsman_team_id'] = request.json.get('batsman_team_id')
            if "overs" in request.json:
                filter_dict['over_number'] = request.json.get('overs')

        key_cols = ','.join(
            [
                key +
                ',' +
                ','.join(map_dict.get(key, '')) for key, value in filter_dict.items() if key not in exclude_group_list
            ]
        )

        out_key_cols = ' , '.join(
            [' ' + key for key, value in filter_dict.items() if key not in exclude_group_list]
        )

        filters, params = generateWhereClause(filter_dict)

        if request.json.get('comparison_type') in ['batter', 'batVbowl']:

            if 'player_id' in key_cols.split(','):
                batter_sql = f'''
                select 
                  {key_cols}, 
                  count(
                    distinct(match_id)
                  ) as total_matches, 
                  cast(
                    sum(runs) as int
                  ) as runs, 
                  cast(
                    sum(sixes) as int
                  ) as sixes, 
                  cast(
                    sum(fours) as int
                  ) as fours, 
                  round(
                    (
                      sum(runs)* 100.00
                    )/ sum(balls), 
                    2
                  ) as strike_rate 
                from 
                  batsman_overwise_df {filters} 
                group by 
                  {key_cols}
                '''

            elif 'team_id' in key_cols.split(','):
                batter_sql = f'''
                select 
                  {key_cols}, 
                  count(
                    distinct(match_id)
                  ) as total_matches, 
                  cast(
                    sum(team_runs) as int
                  ) as runs, 
                  cast(
                    sum(sixes) as int
                  ) as sixes, 
                  cast(
                    sum(fours) as int
                  ) as fours, 
                  round(
                    (
                      sum(team_runs)* 100.00
                    )/ sum(balls), 
                    2
                  ) as strike_rate 
                from 
                  batsman_overwise_df {filters} 
                group by 
                  {key_cols}
                '''

            if 'bowler_id' in out_key_cols.split(','):
                filters = filters + " and is_bowler_wicket = 1 "

            out_batsman_sql = f'''
            select 
              {out_key_cols.replace(' team_id', ' batsman_team_id').replace("player_id", "out_batsman_id")}, round(
                count(out_batsman_id)
              ) as wicket_cnt 
            from 
              join_data {filters.replace(' team_id', ' batsman_team_id').replace("player_id", "out_batsman_id")} 
              and out_batsman_id <>-1 
              and innings not in (3, 4) 
            group by 
              {out_key_cols.replace(' team_id', ' batsman_team_id').replace("player_id", "out_batsman_id")}
            '''

            out_batsman_df = executeQuery(con, out_batsman_sql, params)
            batter_df = executeQuery(con, batter_sql.replace(',,', ','), params)

            join_condition = getJoinCondition(out_key_cols).replace(
                'odf.team_id', 'odf.batsman_team_id'
            ).replace(
                "odf.player_id", "odf.out_batsman_id"
            )

            final_batter_sql = f'''
            select 
              {','.join(
                'pdf.' + i for i in key_cols.split(',')
            )}, 
              total_matches, 
              runs, 
              round(
                coalesce(
                  (runs * 1.00)/ coalesce(wicket_cnt, 0), 
                  0.0
                ), 
                2
              ) as average, 
              sixes, 
              fours, 
              strike_rate, 
              cast(
                total_matches - coalesce(wicket_cnt, 0) as int
              ) as not_outs, 
              cast(
                coalesce(wicket_cnt, 0) as int
              ) as dismissals 
            from 
              batter_df pdf 
              left join out_batsman_df odf on {join_condition}
            '''

            final_df = executeQuery(con, final_batter_sql.replace(',,', ',')).rename(
                columns={
                    'bowler_id': 'against_player_id',
                    'bowler_name': 'against_player_name',
                    'bowl_image_url': 'against_player_image',
                    'bowler_team_id': 'against_team_id',
                    'bowler_team_name': 'against_team_name'
                }
            )

            if "team_id" in request.json:
                final_df = final_df.drop(["not_outs"], axis=1)

            if (request.json.get('comparison_type') in ['batter']) & ("player_id" in request.json):
                del_keys = ['over_number', 'winning_type', 'innings']
                filters_cs, param_cs = generateWhereClause(dropFilter(del_keys, filter_dict))

                batter_contribution_sql = f'''
                select 
                  player_id, 
                  cast(
                    coalesce(
                      round(
                        sum(batting_consistency_score)/ sum(bat_innings)
                      ), 
                      0
                    ) as int
                  ) as batting_consistency_score, 
                  cast(
                    coalesce(
                      round(
                        sum(batting_contribution_score)/ sum(bat_innings)
                      ), 
                      0
                    ) as int
                  ) as batting_contribution_score 
                from 
                  contribution_agg_data {filters_cs} 
                group by 
                  player_id
                '''

                batter_contribution_df = executeQuery(con, batter_contribution_sql, param_cs)
                final_df = final_df.merge(batter_contribution_df, how='left', on='player_id')
        elif request.json.get('comparison_type') in ['bowler', 'bowlVbat']:
            if 'player_id' in key_cols.split(','):
                # bowler comparison
                bowler_sql = f'''
                select 
                  {key_cols}, 
                  count(
                    distinct(match_id)
                  ) as total_matches, 
                  cast(
                    sum(runs) as int
                  ) as runs_conceded, 
                  coalesce(
                    round(
                      (
                        sum(runs)* 1.00
                      )/ sum(wickets), 
                      2
                    ), 
                    0.0
                  ) as average, 
                  cast(
                    sum(sixes) as int
                  ) as sixes_conceded, 
                  cast(
                    sum(fours) as int
                  ) as fours_conceded, 
                  cast(
                    sum(dot_balls) as int
                  ) as dot_balls, 
                  round(
                    coalesce(
                      (
                        (
                          sum(balls)* 1.00
                        )/ sum(wickets)
                      ), 
                      0
                    ), 
                    2
                  ) as strike_rate, 
                  cast(
                    sum(wickets) as int
                  ) as wickets, 
                  cast(
                    (
                      sum(wides)+ sum(no_balls)
                    ) as int
                  ) as extras, 
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
                  bowler_overwise_df {filters} 
                group by 
                  {key_cols}
                '''

            elif 'team_id' in key_cols.split(','):
                bowler_sql = f'''
                select 
                  {key_cols}, 
                  count(
                    distinct(match_id)
                  ) as total_matches, 
                  cast(
                    sum(team_runs) as int
                  ) as runs_conceded, 
                  coalesce(
                    round(
                      (
                        sum(team_runs)* 1.00
                      )/ sum(wickets), 
                      2
                    ), 
                    0.0
                  ) as average, 
                  cast(
                    sum(sixes) as int
                  ) as sixes_conceded, 
                  cast(
                    sum(fours) as int
                  ) as fours_conceded, 
                  cast(
                    sum(dot_balls) as int
                  ) as dot_balls, 
                  round(
                    coalesce(
                      (
                        (
                          sum(balls)* 1.00
                        )/ sum(wickets)
                      ), 
                      0
                    ), 
                    2
                  ) as strike_rate, 
                  cast(
                    sum(wickets) as int
                  ) as wickets, 
                  cast(
                    (
                      sum(wides)+ sum(no_balls)
                    ) as int
                  ) as extras, 
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
                  bowler_overwise_df {filters} 
                group by 
                  {key_cols}
                '''

            final_df = executeQuery(
                con, bowler_sql.replace(',,', ','), params
            ).rename(
                columns={'batsman_id': 'against_player_id',
                         'batsman_name': 'against_player_name',
                         'batsman_image_url': 'against_player_image',
                         'batsman_team_id': 'against_team_id',
                         'batsman_team_name': 'against_team_name'}
            )

            if (request.json.get('comparison_type') in ['bowler']) & ("player_id" in request.json):
                del_keys = ['over_number', 'winning_type', 'innings']
                filters_cs, param_cs = generateWhereClause(dropFilter(del_keys, filter_dict))
                bowler_contribution_sql = f'''
                select 
                  player_id, 
                  cast(
                    coalesce(
                      round(
                        sum(bowling_consistency_score)/ sum(bowl_innings)
                      ), 
                      0
                    ) as int
                  ) as bowling_consistency_score, 
                  cast(
                    coalesce(
                      round(
                        sum(bowling_contribution_score)/ sum(bowl_innings)
                      ), 
                      0
                    ) as int
                  ) as bowling_contribution_score 
                from 
                  contribution_agg_data {filters_cs} 
                group by 
                  player_id
                '''
                bowler_contribution_df = executeQuery(con, bowler_contribution_sql, param_cs)
                final_df = final_df.merge(bowler_contribution_df, how='left', on='player_id')

        if ~final_df.empty:
            if request.json.get('comparison_type') in ['batter', 'bowler']:
                if 'player_id' in request.json:
                    if isinstance(filter_dict['player_id'], list):
                        ind_df = getListIndexDF(filter_dict['player_id'], ['ind', 'player_id'])
                        final_df = final_df.merge(ind_df, how='left', on='player_id').sort_values(by=['ind'])
                elif 'team_id' in request.json:
                    if isinstance(filter_dict['team_id'], list):
                        ind_df = getListIndexDF(filter_dict['team_id'], ['ind', 'team_id'])
                        final_df = final_df.merge(ind_df, how='left', on='team_id').sort_values(by=['ind'])

            elif request.json.get('comparison_type') == 'batVbowl':
                if 'bowler_id' in request.json:
                    if isinstance(filter_dict['bowler_id'], list):
                        ind_df = getListIndexDF(filter_dict['bowler_id'], ['ind', 'against_player_id'])
                        final_df = final_df.merge(ind_df, how='left', on='against_player_id').sort_values(by=['ind'])

                elif 'bowler_team_id' in request.json:
                    if isinstance(filter_dict['bowler_team_id'], list):
                        ind_df = getListIndexDF(filter_dict['bowler_team_id'], ['ind', 'against_team_id'])
                        final_df = final_df.merge(ind_df, how='left', on='against_team_id').sort_values(by=['ind'])

            elif request.json.get('comparison_type') == 'bowlVbat':
                if 'batsman_id' in request.json:
                    if isinstance(filter_dict['batsman_id'], list):
                        ind_df = getListIndexDF(filter_dict['batsman_id'], ['ind', 'against_player_id'])
                        final_df = final_df.merge(ind_df, how='left', on='against_player_id').sort_values(by=['ind'])
                elif 'batsman_team_id' in request.json:
                    if isinstance(filter_dict['batsman_team_id'], list):
                        ind_df = getListIndexDF(filter_dict['batsman_team_id'], ['ind', 'against_team_id'])
                        final_df = final_df.merge(ind_df, how='left', on='against_team_id').sort_values(by=['ind'])
            else:
                final_df = final_df
        response = final_df.to_json(orient='records')
        return response, logger.info("Status - 200")
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/getPlayerMatchupVSPlayer", methods=['POST'])
@token_required
def getPlayerMatchupVSPlayer():
    logger = get_logger("getPlayerMatchupVSPlayer", "getPlayerMatchupVSPlayer")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        if "overs" in request.json:
            filter_dict['over_number'] = request.json.get('overs')
        del_keys = ["player_skill"]
        dropFilter(del_keys, filter_dict)
        filters, params = generateWhereClause(filter_dict)

        response = {'batsman_matchup': {'best_matchup': {}, 'worst_matchup': {}},
                    'bowler_matchup': {'best_matchup': {}, 'worst_matchup': {}}}

        batsman_matchup = '''select player_id, player_name, bat_current_team as team_name, bowler_id, bowler_name as against_player_name, 
        bowl_current_team as against_player_team_name, player_image_url, bowl_image_url as against_player_image_url,
        cast(sum(runs) as int) as runs, round(coalesce((sum(runs)*100.00)/sum(balls),0.0),2) as strike_rate
         from batsman_overwise_df ''' + filters + ''' group by bowler_id, player_id, player_name, bat_current_team, bowler_name, 
        bowl_current_team, player_image_url, bowl_image_url'''

        batsman_matchup_stats = executeQuery(con, batsman_matchup, params)

        out_batsman_sql = '''select out_batsman_id, bowler_id, round(count(out_batsman_id)) as wicket_cnt
                from join_data ''' + filters.replace("player_id", "out_batsman_id").replace(' team_id',
                                                                                            ' batsman_team_id') + ''' and  out_batsman_id<>-1 
                and innings not in (3,4) and is_bowler_wicket=1 group by out_batsman_id, bowler_id'''
        out_batsman_df = executeQuery(con, out_batsman_sql, params)

        batsman_matchup_sql = '''select player_id, player_name, team_name, against_player_name, 
        against_player_team_name, player_image_url, against_player_image_url
        , runs, cast(coalesce(wicket_cnt,0) as int) as dismissals, strike_rate,
         round(coalesce((runs*1.00)/coalesce(wicket_cnt,0),0.0),2) as average from batsman_matchup_stats 
             pdf left join out_batsman_df odf on (odf.out_batsman_id=pdf.player_id and 
             odf.bowler_id=pdf.bowler_id)'''

        batsman_matchup_df = executeQuery(con, batsman_matchup_sql)

        bat_stat_list = ['runs', 'strike_rate', 'average', 'dismissals']

        for stat in bat_stat_list:
            # best matchup
            if stat == "dismissals":
                batsman_matchup_df['against_player_rank'] = batsman_matchup_df[batsman_matchup_df[stat] > 0][stat].rank(
                    method='first', ascending=True).astype(int)
            else:
                batsman_matchup_df['against_player_rank'] = batsman_matchup_df[batsman_matchup_df[stat] > 0][stat].rank(
                    method='first', ascending=False).astype(int)

            best_matchup_df = batsman_matchup_df[batsman_matchup_df['against_player_rank'] <= 5] \
                [['player_name', 'against_player_name', stat, 'against_player_rank', 'against_player_team_name',
                  'player_image_url',
                  'against_player_image_url', 'team_name']]
            best_matchup_df['player_details'] = best_matchup_df[
                ['against_player_name', stat, 'against_player_rank', 'against_player_team_name',
                 'against_player_image_url']].to_dict(orient='records')
            response['batsman_matchup']['best_matchup'][stat] = \
                best_matchup_df.groupby(['player_name', 'player_image_url', 'team_name'])[
                    'player_details'].agg(list).reset_index().to_dict('records')

            # worst matchup
            if stat == "dismissals":
                batsman_matchup_df['against_player_rank'] = batsman_matchup_df[batsman_matchup_df[stat] > 0][stat].rank(
                    method='first', ascending=False).astype(int)
            else:
                batsman_matchup_df['against_player_rank'] = batsman_matchup_df[batsman_matchup_df[stat] > 0][stat].rank(
                    method='first', ascending=True).astype(int)

            worst_matchup_df = batsman_matchup_df[batsman_matchup_df['against_player_rank'] <= 5] \
                [['player_name', 'against_player_name', stat, 'against_player_rank', 'against_player_team_name',
                  'player_image_url', 'against_player_image_url', 'team_name']]
            worst_matchup_df['player_details'] = worst_matchup_df[
                ['against_player_name', stat, 'against_player_rank', 'against_player_team_name',
                 'against_player_image_url']].to_dict(orient='records')
            response['batsman_matchup']['worst_matchup'][stat] = \
                worst_matchup_df.groupby(['player_name', 'player_image_url', 'team_name'])[
                    'player_details'].agg(list).reset_index().to_dict('records')

        # bowler matchup
        bowl_stat_list = ['strike_rate', 'average', 'dismissals', 'economy']

        bowler_matchup = '''select player_id, player_name, bowl_current_team as team_name, batsman_name as 
        against_player_name, player_image_url, batsman_image_url as against_player_image_url, 
        bat_current_team as against_player_team_name ,cast(sum(wickets) as int) as dismissals, 
        round(coalesce(((sum(balls)*1.00)/sum(wickets)),0),2) as strike_rate, 
        round(coalesce(((sum(runs)*1.00)/(sum(balls)*1.00/6)),0.0),2) as economy, 
        round(coalesce(((sum(runs)*1.00)/sum(wickets)),0.0),2) as average 
         from bowler_overwise_df ''' + filters + ''' group by player_id, player_name, bowl_current_team, batsman_name, 
         player_image_url, batsman_image_url, bat_current_team'''

        bowler_matchup_df = executeQuery(con, bowler_matchup, params)

        for stat in bowl_stat_list:
            # best matchup
            if stat == "dismissals":
                bowler_matchup_df['against_player_rank'] = bowler_matchup_df[bowler_matchup_df[stat] > 0][stat].rank(
                    method='first', ascending=False).astype(int)
            else:
                bowler_matchup_df['against_player_rank'] = bowler_matchup_df[bowler_matchup_df[stat] > 0][stat].rank(
                    method='first', ascending=True).astype(int)

            best_matchup_df = bowler_matchup_df[bowler_matchup_df['against_player_rank'] <= 5] \
                [['player_name', 'against_player_name', stat, 'against_player_rank', 'against_player_team_name',
                  'player_image_url',
                  'against_player_image_url', 'team_name']]
            best_matchup_df['player_details'] = best_matchup_df[
                ['against_player_name', stat, 'against_player_rank', 'against_player_team_name',
                 'against_player_image_url']].to_dict(orient='records')
            response['bowler_matchup']['best_matchup'][stat] = \
                best_matchup_df.groupby(['player_name', 'player_image_url', 'team_name'])[
                    'player_details'].agg(list).reset_index().to_dict('records')

            # worst matchup
            if stat == "dismissals":
                bowler_matchup_df['against_player_rank'] = bowler_matchup_df[bowler_matchup_df[stat] > 0][stat].rank(
                    method='first', ascending=True).astype(int)
            else:
                bowler_matchup_df['against_player_rank'] = bowler_matchup_df[bowler_matchup_df[stat] > 0][stat].rank(
                    method='first', ascending=False).astype(int)

            worst_matchup_df = bowler_matchup_df[bowler_matchup_df['against_player_rank'] <= 5] \
                [['player_name', 'against_player_name', stat, 'against_player_rank', 'against_player_team_name',
                  'player_image_url', 'against_player_image_url', 'team_name']]
            worst_matchup_df['player_details'] = worst_matchup_df[
                ['against_player_name', stat, 'against_player_rank', 'against_player_team_name',
                 'against_player_image_url']].to_dict(orient='records')
            response['bowler_matchup']['worst_matchup'][stat] = \
                worst_matchup_df.groupby(['player_name', 'player_image_url', 'team_name'])[
                    'player_details'].agg(list).reset_index().to_dict('records')

        return response, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/getPlayerMatchupVSTeam", methods=['POST'])
@token_required
def getPlayerMatchupVSTeam():
    logger = get_logger("getPlayerMatchupVSTeam", "getPlayerMatchupVSTeam")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        if "overs" in request.json:
            filter_dict['over_number'] = request.json.get('overs')
        filters, params = generateWhereClause(filter_dict)

        response = {'batsman_matchup': {'best_matchup': {}, 'worst_matchup': {}},
                    'bowler_matchup': {'best_matchup': {}, 'worst_matchup': {}}}

        batsman_matchup = '''select player_id, player_name, bat_current_team as team_name,  bowler_team_id,
                bowler_team_name as against_team_name, player_image_url, cast(sum(runs) as int) as runs,
                round(coalesce((sum(runs)*100.00)/sum(balls),0.0),2) as strike_rate
                 from batsman_overwise_df ''' + filters + ''' and bowler_team_name<>bat_current_team 
                 group by player_id, player_name, bat_current_team, bowler_team_name, player_image_url, bowler_team_id'''

        batsman_matchup_stats = executeQuery(con, batsman_matchup, params)

        out_batsman_sql = '''select out_batsman_id, bowler_team_id, round(count(out_batsman_id)) as wicket_cnt
                        from join_data ''' + filters.replace("player_id", "out_batsman_id").replace(' team_id',
                                                                                                    ' batsman_team_id') + ''' and  out_batsman_id<>-1 
                        and innings not in (3,4)  group by out_batsman_id, bowler_team_id'''
        out_batsman_df = executeQuery(con, out_batsman_sql, params)

        batsman_matchup_sql = '''select player_id, player_name, team_name, 
                against_team_name, player_image_url, runs, cast(coalesce(wicket_cnt,0) as int) as dismissals, strike_rate,
                 round(coalesce((runs*1.00)/coalesce(wicket_cnt,0),0.0),2) as average from batsman_matchup_stats 
                     pdf left join out_batsman_df odf on (odf.out_batsman_id=pdf.player_id and 
                     odf.bowler_team_id=pdf.bowler_team_id)'''

        batsman_matchup_df = executeQuery(con, batsman_matchup_sql)

        bat_stat_list = ['runs', 'strike_rate', 'average', 'dismissals']

        for stat in bat_stat_list:
            # best matchup
            if stat == "dismissals":
                batsman_matchup_df['against_team_rank'] = batsman_matchup_df[batsman_matchup_df[stat] > 0][stat].rank(
                    method='first', ascending=True).astype(int)
            else:
                batsman_matchup_df['against_team_rank'] = batsman_matchup_df[batsman_matchup_df[stat] > 0][stat].rank(
                    method='first', ascending=False).astype(int)

            best_matchup_df = batsman_matchup_df[batsman_matchup_df['against_team_rank'] <= 5] \
                [['player_name', 'against_team_name', stat, 'against_team_rank', 'team_name', 'player_image_url']]
            best_matchup_df['team_details'] = best_matchup_df[[stat, 'against_team_rank', 'against_team_name']].to_dict(
                orient='records')

            response['batsman_matchup']['best_matchup'][stat] = \
                best_matchup_df.groupby(['player_name', 'player_image_url', 'team_name'])[
                    'team_details'].agg(list).reset_index().to_dict('records')

            # worst matchup
            if stat == "dismissals":
                batsman_matchup_df['against_team_rank'] = batsman_matchup_df[batsman_matchup_df[stat] > 0][stat].rank(
                    method='first', ascending=False).astype(int)
            else:
                batsman_matchup_df['against_team_rank'] = batsman_matchup_df[batsman_matchup_df[stat] > 0][stat].rank(
                    method='first', ascending=True).astype(int)

            worst_matchup_df = batsman_matchup_df[batsman_matchup_df['against_team_rank'] <= 5] \
                [['player_name', 'against_team_name', stat, 'against_team_rank', 'team_name', 'player_image_url']]
            worst_matchup_df['team_details'] = worst_matchup_df[
                [stat, 'against_team_rank', 'against_team_name']].to_dict(orient='records')

            response['batsman_matchup']['worst_matchup'][stat] = \
                worst_matchup_df.groupby(['player_name', 'player_image_url', 'team_name'])[
                    'team_details'].agg(list).reset_index().to_dict('records')

        bowl_stat_list = ['strike_rate', 'average', 'dismissals', 'economy']

        bowler_matchup = '''select player_id, player_name, player_image_url, bowl_current_team as team_name, 
        batsman_team_name as against_team_name,cast(sum(wickets) as int) as dismissals, 
        round(coalesce(((sum(balls)*1.00)/sum(wickets)),0),2) as strike_rate, 
        round(coalesce(((sum(runs)*1.00)/(sum(balls)*1.00/6)),0.0),2) as economy, 
        round(coalesce(((sum(runs)*1.00)/sum(wickets)),0.0),2) as average 
         from bowler_overwise_df ''' + filters + ''' and batsman_team_name<>bowl_current_team group by player_id, 
         player_name, player_image_url, bowl_current_team, batsman_team_name '''

        bowler_matchup_df = executeQuery(con, bowler_matchup, params)

        for stat in bowl_stat_list:
            # best matchup
            if stat == "dismissals":
                bowler_matchup_df['against_team_rank'] = bowler_matchup_df[bowler_matchup_df[stat] > 0][stat].rank(
                    method='first', ascending=False).astype(int)
            else:
                bowler_matchup_df['against_team_rank'] = bowler_matchup_df[bowler_matchup_df[stat] > 0][stat].rank(
                    method='first', ascending=True).astype(int)

            best_matchup_df = bowler_matchup_df[bowler_matchup_df['against_team_rank'] <= 5] \
                [['player_name', stat, 'against_team_rank', 'against_team_name', 'player_image_url', 'team_name']]
            best_matchup_df['team_details'] = best_matchup_df[[stat, 'against_team_rank', 'against_team_name']].to_dict(
                orient='records')
            response['bowler_matchup']['best_matchup'][stat] = \
                best_matchup_df.groupby(['player_name', 'player_image_url', 'team_name'])[
                    'team_details'].agg(list).reset_index().to_dict('records')

            # worst matchup
            if stat == "dismissals":
                bowler_matchup_df['against_team_rank'] = bowler_matchup_df[bowler_matchup_df[stat] > 0][stat].rank(
                    method='first', ascending=True).astype(int)
            else:
                bowler_matchup_df['against_team_rank'] = bowler_matchup_df[bowler_matchup_df[stat] > 0][stat].rank(
                    method='first', ascending=False).astype(int)

            worst_matchup_df = bowler_matchup_df[bowler_matchup_df['against_team_rank'] <= 5] \
                [['player_name', stat, 'against_team_rank', 'against_team_name', 'player_image_url', 'team_name']]
            worst_matchup_df['team_details'] = worst_matchup_df[
                [stat, 'against_team_rank', 'against_team_name']].to_dict(orient='records')
            response['bowler_matchup']['worst_matchup'][stat] = \
                worst_matchup_df.groupby(['player_name', 'player_image_url', 'team_name'])[
                    'team_details'].agg(list).reset_index().to_dict('records')

        return response, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/highestIndividualBowlingStats", methods=['POST'])
@token_required
def highestIndividualBowlingStats():
    logger = get_logger("highestIndividualBowlingStats", "highestIndividualBowlingStats")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        response = {}

        if request.json:
            if request.json.get('overs'):
                filter_dict['over_number'] = request.json.get('overs')

        filters, params = generateWhereClause(filter_dict)

        if filters:
            bowler_stats_sql = '''
            select 
              player_name, 
              team_name, 
              player_image_url, 
              count(distinct match_id) as innings_played, 
              cast(
                sum(wickets) as int
              ) as wickets, 
              round(
                coalesce(
                  (
                    (
                      sum(balls)* 1.00
                    )/ sum(wickets)
                  ), 
                  0
                ), 
                2
              ) as bowling_strike_rate, 
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
                    sum(dot_balls)* 100.00 / sum(balls)
                  ), 
                  0.0
                ), 
                2
              ) as dot_ball_percent, 
              round(
                coalesce(
                  (
                    (
                      sum(fours)* 4 + sum(sixes)* 6
                    )* 100.00
                  )/ sum(runs), 
                  0.0
                ), 
                2
              ) as boundary_percent, 
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
                sum(
                  case when wickets >= 3 then 1 else 0 end
                ) as int
              ) as three_wkt_hauls, 
            from 
              (
                select 
                  match_id, 
                  player_name, 
                  player_id, 
                  bowl_current_team as team_name, 
                  sum(dot_balls) as dot_balls, 
                  sum(balls) as balls, 
                  sum(runs) as runs, 
                  sum(wickets) as wickets, 
                  sum(fours) as fours, 
                  sum(sixes) as sixes, 
                  player_image_url 
                from 
                  bowler_overwise_df 
            ''' + filters + '''
            group by 
              match_id, 
              player_id, 
              bowl_current_team, 
              player_name, 
              player_image_url
            ) bo 
            group by 
              player_name, 
              team_name, 
              player_image_url
            '''

            first_last_ball_boundary_sql = '''
            select 
              player_name, 
              team_name, 
              player_image_url, 
              cast(
                sum(first_ball_boundary) as int
              ) as first_ball_boundary, 
              cast(
                sum(last_ball_boundary) as int
              ) as last_ball_boundary, 
              count(distinct match_id) as innings_played 
            from 
              (
                select 
                  bowler_id, 
                  bowler_name as player_name, 
                  match_id, 
                  over_number, 
                  bowler_team_id, 
                  bowler_team_name as team_name, 
                  MIN(ball_number) as first_ball, 
                  MAX(ball_number) as last_ball, 
                  cast(
                    sum(
                      case when ball_number = 1 
                      and is_extra <> 1 
                      and (
                        is_four = 1 
                        or is_six = 1
                      ) then 1 else 0 end
                    ) as int
                  ) as first_ball_boundary, 
                  cast(
                    sum(
                      case when ball_number = 6 
                      and is_extra <> 1 
                      and (
                        is_four = 1 
                        or is_six = 1
                      ) then 1 else 0 end
                    ) as int
                  ) as last_ball_boundary, 
                  bowler_image_url as player_image_url 
                from 
                  join_data 
            ''' + filters.replace(
                'player_id', 'bowler_id'
            ).replace(
                ' team_id', ' bowler_team_id'
            ).replace(
                'player_skill', 'bowler_skill'
            ) + ''' 
            and innings not in (3, 4) 
            group by 
              bowler_id, 
              bowler_name, 
              over_number, 
              match_id, 
              bowler_team_id, 
              bowler_team_name, 
              bowler_image_url
            ) 
            group by 
              player_name, 
              team_name, 
              player_image_url
            '''
        else:
            bowler_stats_sql = '''
            select 
              player_name, 
              team_name, 
              player_image_url, 
              count(distinct match_id) as innings_played, 
              cast(
                sum(wickets) as int
              ) as wickets, 
              round(
                coalesce(
                  (
                    (
                      sum(balls)* 1.00
                    )/ sum(wickets)
                  ), 
                  0
                ), 
                2
              ) as bowling_strike_rate, 
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
                    sum(dot_balls)* 100.00 / sum(balls)
                  ), 
                  0.0
                ), 
                2
              ) as dot_ball_percent, 
              round(
                coalesce(
                  (
                    (
                      sum(fours)* 4 + sum(sixes)* 6
                    )* 100.00
                  )/ sum(runs), 
                  0.0
                ), 
                2
              ) as boundary_percent, 
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
                sum(
                  case when wickets >= 3 then 1 else 0 end
                ) as int
              ) as three_wkt_hauls, 
            from 
              (
                select 
                  match_id, 
                  player_name, 
                  player_id, 
                  bowl_current_team as team_name, 
                  sum(dot_balls) as dot_balls, 
                  sum(balls) as balls, 
                  sum(runs) as runs, 
                  sum(wickets) as wickets, 
                  sum(fours) as fours, 
                  sum(sixes) as sixes, 
                  player_image_url 
                from 
                  bowler_overwise_df 
                group by 
                  match_id, 
                  player_id, 
                  bowl_current_team, 
                  player_name, 
                  player_image_url
              ) bo 
            group by 
              player_name, 
              team_name, 
              player_image_url
            '''

            first_last_ball_boundary_sql = '''
            select 
              player_name, 
              team_name, 
              player_image_url, 
              cast(
                sum(first_ball_boundary) as int
              ) as first_ball_boundary, 
              cast(
                sum(last_ball_boundary) as int
              ) as last_ball_boundary, 
              count(distinct match_id) as innings_played 
            from 
              (
                select 
                  bowler_id, 
                  bowler_name as player_name, 
                  match_id, 
                  over_number, 
                  bowler_team_id, 
                  bowler_team_name as team_name, 
                  MIN(ball_number) as first_ball, 
                  MAX(ball_number) as last_ball, 
                  cast(
                    sum(
                      case when ball_number = 1 
                      and is_extra <> 1 
                      and (
                        is_four = 1 
                        or is_six = 1
                      ) then 1 else 0 end
                    ) as int
                  ) as first_ball_boundary, 
                  cast(
                    sum(
                      case when ball_number = 6 
                      and is_extra <> 1 
                      and (
                        is_four = 1 
                        or is_six = 1
                      ) then 1 else 0 end
                    ) as int
                  ) as last_ball_boundary, 
                  bowler_image_url as player_image_url 
                from 
                  join_data 
                where 
                  innings not in (3, 4) 
                group by 
                  bowler_id, 
                  bowler_name, 
                  over_number, 
                  match_id, 
                  bowler_team_id, 
                  bowler_team_name, 
                  bowler_image_url
              ) 
            group by 
              player_name, 
              team_name, 
              player_image_url
            '''
        bowler_stats_df = executeQuery(con, bowler_stats_sql, params)
        firstLastBallBoundaryDF = executeQuery(con, first_last_ball_boundary_sql, params)

        if "min_innings" in request.json:
            min_innings = request.json['min_innings']
            bowler_stats_df = bowler_stats_df[bowler_stats_df['innings_played'] >= min_innings]
            firstLastBallBoundaryDF = firstLastBallBoundaryDF[firstLastBallBoundaryDF['innings_played'] >= min_innings]

        stat_list = ['wickets', 'economy', 'average', 'dot_ball_percent', 'strike_rate', 'three_wkt_hauls']

        for stat in stat_list:
            if stat in ["average", "economy"]:
                bowler_stats_df = bowler_stats_df[bowler_stats_df[stat] > 0]

            bowler_stats_df['bowler_rank'] = bowler_stats_df[stat].rank(method='first', ascending=False).astype(int)

            bowler_stats_df['bowler_details'] = bowler_stats_df[[
                'wickets',
                'economy',
                'average',
                'strike_rate',
                'bowler_rank',
                'dot_ball_percent',
                'three_wkt_hauls'
            ]].to_dict(orient='records')

            response[stat] = bowler_stats_df.sort_values(['bowler_rank']).head(10).groupby(
                ['player_name', 'team_name', 'player_image_url']
            )['bowler_details'].agg(list).reset_index().to_dict('records')

        boundary_stat_list = ['first_ball_boundary', 'last_ball_boundary']

        for stats in boundary_stat_list:
            firstLastBallBoundaryDF['bowler_rank'] = firstLastBallBoundaryDF[stats].rank(
                method='first',
                ascending=False
            ).astype(int)

            firstLastBallBoundaryDF['bowler_details'] = firstLastBallBoundaryDF[[
                'first_ball_boundary', 'last_ball_boundary', 'bowler_rank'
            ]].to_dict(orient='records')

            response[stats] = firstLastBallBoundaryDF.sort_values(['bowler_rank']).head(10).groupby(
                ['player_name', 'team_name', 'player_image_url']
            )['bowler_details'].agg(list).reset_index().to_dict('records')
        return jsonify(response), 200, logger.info("Status - 200")
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# Function to show Batsman Over-wise Stats in BI
# @app.route("/getBatsmanOverWiseStats", methods=['POST'])
@token_required
def getBatsmanOverWiseStats():
    logger = get_logger("getBatsmanOverWiseStats", "getBatsmanOverWiseStats")
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
                filter_dict['over_number'] = request.json['overs']
            if "asc" in request.json:
                asc = request.json['asc']
            else:
                asc = False
            if "sort_key" in request.json:
                sort_key = request.json['sort_key']
            else:
                sort_key = "runs_scored"

        filters, params = generateWhereClause(filter_dict)

        if filters:
            BatsmanOverWiseStatsSQL = f'''
            select 
              player_id, 
              player_name, 
              bat_current_team as team_name, 
              bat_team_short_name as team_short_name,
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
              round(
                coalesce(
                  (
                    sum(runs)* 1.00
                  )/ sum(wickets), 
                  0.0
                ), 
                2
              ) as average, 
              cast(
                sum(case when runs >= 30 then 1 else 0 end) as int
              ) as thirty_plus, 
              cast(
                sum(fours) as int
              ) as fours, 
              cast(
                sum(sixes) as int
              ) as sixes, 
              cast(
                max(runs) as int
              ) as best_score, 
              player_image_url 
            from 
              (
                select 
                  player_name, 
                  player_image_url, 
                  bat_current_team, 
                  bat_team_short_name,
                  player_id, 
                  innings, 
                  sum(runs) as runs, 
                  sum(balls) as balls, 
                  match_id, 
                  sum(fours) as fours, 
                  sum(sixes) as sixes, 
                  sum(wickets) as wickets, 
                  not_out 
                from 
                  batsman_overwise_df {filters} 
                group by 
                  player_id, 
                  not_out, 
                  match_id, 
                  innings, 
                  player_name, 
                  bat_current_team, 
                  bat_team_short_name,
                  player_image_url
              ) 
            group by 
              player_id, 
              player_name, 
              bat_current_team,
              bat_team_short_name, 
              player_image_url
            '''
            out_batsman_sql = f'''
            select 
              out_batsman_id, 
              round(
                count(out_batsman_id)
              ) as wicket_cnt 
            from 
              join_data {filters.replace('player_id', 'out_batsman_id').replace(' team_id', ' batsman_team_id').replace(
                'player_skill', 'out_batsman_skill'
            )} 
              and out_batsman_id <>-1 
              and innings not in (3, 4) 
            group by 
              out_batsman_id
            '''
        else:
            BatsmanOverWiseStatsSQL = '''
            select 
              player_id, 
              player_name, 
              bat_current_team as team_name, 
              bat_team_short_name as team_short_name,
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
              round(
                coalesce(
                  (
                    sum(runs)* 1.00
                  )/ sum(wickets), 
                  0.0
                ), 
                2
              ) as average, 
              cast(
                sum(case when runs >= 30 then 1 else 0 end) as int
              ) as thirty_plus, 
              cast(
                sum(fours) as int
              ) as fours, 
              cast(
                sum(sixes) as int
              ) as sixes, 
              cast(
                max(runs) as int
              ) as best_score, 
              player_image_url 
            from 
              (
                select 
                  player_name, 
                  player_image_url, 
                  bat_current_team, 
                  player_id, 
                  innings, 
                  sum(runs) as runs, 
                  sum(balls) as balls, 
                  match_id, 
                  sum(fours) as fours, 
                  sum(sixes) as sixes, 
                  sum(wickets) as wickets, 
                  not_out 
                from 
                  batsman_overwise_df 
                group by 
                  player_id, 
                  not_out, 
                  match_id, 
                  innings, 
                  player_name, 
                  bat_current_team, 
                  player_image_url
              ) bdf 
              left join (
                select 
                  out_batsman_id, 
                  round(
                    count(out_batsman_id)
                  ) as wicket_cnt 
                from 
                  join_data 
                group by 
                  out_batsman_id
              ) out on (
                bdf.player_id = out.out_batsman_id
              ) 
            group by 
              player_id, 
              player_name, 
              bat_current_team, 
              player_image_url
            '''
            out_batsman_sql = '''
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

        final_batsmanOverWiseStatsSQL = '''
        select 
          player_id, 
          player_name, 
          team_name, 
          team_short_name,
          runs_scored, 
          matches, 
          strike_rate, 
          balls_played, 
          thirty_plus, 
          fours, 
          sixes, 
          best_score, 
          player_image_url, 
          round(
            coalesce(
              (hsd.runs_scored * 1.00)/ obd.wicket_cnt, 
              0.0
            ), 
            2
          ) as average 
        from 
          BatsmanOverWiseStatsDf hsd 
          left join OutBatsmanDf obd on (
            obd.out_batsman_id = hsd.player_id
          )
        '''

        BatsmanOverWiseStatsDf = executeQuery(con, BatsmanOverWiseStatsSQL, params)
        OutBatsmanDf = executeQuery(con, out_batsman_sql, params)
        OverwiseBatsmanStatsDf = executeQuery(con, final_batsmanOverWiseStatsSQL)

        if "min_innings" in request.json:
            min_innings = request.json['min_innings']
            OverwiseBatsmanStatsDf = OverwiseBatsmanStatsDf[OverwiseBatsmanStatsDf['matches'] >= min_innings]

        if sort_key not in ["player_name", "team_short_name"]:
            OverwiseBatsmanStatsDf = OverwiseBatsmanStatsDf[OverwiseBatsmanStatsDf[sort_key] > 0]
        response = OverwiseBatsmanStatsDf.sort_values(sort_key, ascending=asc).to_json(orient='records')
        return response, logger.info("Status - 200")
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/ball-video", methods=['POST'])
# @token_required
def ball_video():
    logger = get_logger("dismissalType", "dismissalType")
    try:
        filter_dict = {}
        if request.json:
            filter_dict = globalFilters()
            if "bowler_id" in request.json:
                filter_dict['bowler_id'] = request.json.get('bowler_id')
            if "bowler_team_id" in request.json:
                filter_dict['bowler_team_id'] = request.json.get('bowler_team_id')
            if "batsman_id" in request.json:
                filter_dict['batsman_id'] = request.json.get('batsman_id')
            if "batsman_team_id" in request.json:
                filter_dict['batsman_team_id'] = request.json.get('batsman_team_id')
            if "overs" in request.json:
                filter_dict['over_number'] = request.json.get('overs')
            if "match_id" in request.json:
                filter_dict['match_id'] = request.json.get('match_id')
            if "non_striker_name" in request.json:
                filter_dict['non_striker'] = request.json.get('non_striker_name')
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        if request.args.get('widget') == "dismissal_type":
            filters, params = generateWhereClause(filter_dict)
            if filters:
                dismissal_type_df = executeQuery(
                    con,
                    '''select  * from join_data ''' + filters.replace('player_id', 'out_batsman_id').replace(
                        ' team_id', ' batsman_team_id') + '''and wicket_type<> '' ''',
                    params).groupby('wicket_type')
            else:
                dismissal_type_df = executeQuery(
                    con,
                    '''select  wicket_type, count(wicket_type) as count from join_data where  wicket_type<> '' '''
                ).groupby('wicket_type')

            response = []
            for name, group in dismissal_type_df:
                json_obj = group.to_dict(orient='records')
                response.append({
                    'parameter': 'wicket_type',
                    'value': name,
                    'video': [iter for iter in json_obj]
                })
            return jsonify(response), 200, logger.info("Status - 200")

        if request.args.get('widget') == 'six_and_four':
            filters, params = generateWhereClause(filter_dict)
            filters += " and (fours=? or sixes=?)"
            params.extend([1.0, 1.0])
            if filters:
                overwise_batsman_sql = "select * from batsman_overwise_df" + filters
                overwise_batsman_stats = executeQuery(con, overwise_batsman_sql, params)
                overwise_bowler_sql = "select * from bowler_overwise_df" + filters
                overwise_bowler_stats = executeQuery(con, overwise_bowler_sql, params)
                response = {
                    "batsman_detail": json.loads(overwise_batsman_stats.to_json(orient="records")),
                    "bowler_detail": json.loads(overwise_bowler_stats.to_json(orient="records"))
                }

                return response, 200, logger.info("Status - 200")

        if request.args.get('widget') == 'latest_performance':
            filters, params = generateWhereClause(filter_dict)
            latest_performance = '''select * from batsman_overwise_df ''' + filters
            response = executeQuery(con, latest_performance, params).to_json(orient="records")
            response = json.loads(response)
            return jsonify(response), 200, logger.info("Status - 200")

        if request.args.get('widget') == 'best_partnership':
            latest_performance = f"select * from players_data_df where player_name='{filter_dict.get('non_striker')}'"
            non_striker_id = executeQuery(con, latest_performance)
            if not non_striker_id.empty:
                non_striker_id = non_striker_id.iloc[0]['player_id']
            filter_dict['non_striker_id'] = int(non_striker_id)
            del filter_dict['non_striker']
            filters, params = generateWhereClause(filter_dict)
            partnership_query = "select * from join_data" + filters.replace('player_id', 'batsman_id')
            response = executeQuery(con, partnership_query, params).to_json(orient="records")
            response = json.loads(response)
            return jsonify(response), 200, logger.info("Status - 200")

        if request.args.get('widget') == 'perf_diff_type_bowler':
            filters, params = generateWhereClause(filter_dict)
            perf_diff_type_bowler_query = "select * from join_data" + filters.replace('player_id', 'batsman_id')
            perf_diff_type_bowler_response = executeQuery(con, perf_diff_type_bowler_query, params)
            perf_diff_type_bowler_response = perf_diff_type_bowler_response.groupby('bowling_type')
            response = []
            for name, group in perf_diff_type_bowler_response:
                json_obj = group.to_dict(orient='records')
                response.append({
                    'parameter': 'bowler_type',
                    'value': name,
                    'video': [iter for iter in json_obj]
                })
            return jsonify(response), 200, logger.info("Status - 200")

        if request.args.get('widget') == 'batter_comparison':
            if "team_id" in filter_dict:
                filter_dict['batsman_team_id'] = filter_dict["team_id"]
                del filter_dict['team_id']
            filters, params = generateWhereClause(filter_dict)
            batter_comparison_query = "select * from join_data" + filters.replace('player_id', 'batsman_id')
            batter_comparison_result = executeQuery(con, batter_comparison_query, params)
            if "batsman_team_id" in filter_dict:
                batter_comparison_result = batter_comparison_result.groupby("batsman_team_id")
                response = []
                for name, group in batter_comparison_result:
                    response.append({
                        'parameter': 'team_id',
                        'value': name,
                        'video': {
                            "sixes": json.loads(group[group['is_six'] == 1].to_json(orient="records")),
                            "fours": json.loads(group[group['is_four'] == 1].to_json(orient="records")),
                            "wickets": json.loads(group[group['is_wicket'] == 1].to_json(orient="records"))
                        }
                    })
            else:
                batter_comparison_result = batter_comparison_result.groupby("batsman_id")
                response = []
                for name, group in batter_comparison_result:
                    response.append({
                        'parameter': 'batsman_id',
                        'value': name,
                        'video': {
                            "sixes": json.loads(group[group['is_six'] == 1].to_json(orient="records")),
                            "fours": json.loads(group[group['is_four'] == 1].to_json(orient="records")),
                            "wickets": json.loads(group[group['is_wicket'] == 1].to_json(orient="records"))
                        }
                    })
            return jsonify(response), 200, logger.info("Status - 200")

        if request.args.get('widget') == 'bowler_comparison':
            if "team_id" in filter_dict:
                filter_dict['bowler_team_id'] = filter_dict["team_id"]
                del filter_dict['team_id']
            filters, params = generateWhereClause(filter_dict)
            bowler_comparison_query = "select * from join_data" + filters.replace('player_id', 'bowler_id')
            bowler_comparison_result = executeQuery(con, bowler_comparison_query, params)
            if "bowler_team_id" in filter_dict:
                bowler_comparison_result = bowler_comparison_result.groupby("bowler_team_id")
                response = []
                for name, group in bowler_comparison_result:
                    response.append({
                        'parameter': 'bowler_team_id',
                        'value': name,
                        'video': {
                            "sixes": json.loads(group[group['is_six'] == 1].to_json(orient="records")),
                            "fours": json.loads(group[group['is_four'] == 1].to_json(orient="records")),
                            "wickets": json.loads(group[group['is_wicket'] == 1].to_json(orient="records"))
                        }
                    })
            else:
                bowler_comparison_result = bowler_comparison_result.groupby("bowler_id")
                response = []
                for name, group in bowler_comparison_result:
                    response.append({
                        'parameter': 'bowler_id',
                        'value': name,
                        'video': {
                            "sixes": json.loads(group[group['is_six'] == 1].to_json(orient="records")),
                            "fours": json.loads(group[group['is_four'] == 1].to_json(orient="records")),
                            "wickets": json.loads(group[group['is_wicket'] == 1].to_json(orient="records"))
                        }
                    })
            return jsonify(response), 200, logger.info("Status - 200")

        if request.args.get('widget') == 'batter_vs_bowler':
            if "team_id" in filter_dict:
                filter_dict['batsman_team_id'] = filter_dict["team_id"]
                del filter_dict['team_id']

            if filter_dict.get('player_id'):
                filter_dict['batsman_id'] = filter_dict.get('player_id')
                del filter_dict['player_id']
            filters, params = generateWhereClause(filter_dict)
            batter_vs_bowler_query = "select * from join_data" + filters
            batter_vs_bowler_query_response = executeQuery(con, batter_vs_bowler_query, params)
            response = []
            if filter_dict.get('bowler_team_id'):
                bowler_comparison_result = batter_vs_bowler_query_response.groupby("bowler_team_id")
                for name, group in bowler_comparison_result:
                    response.append({
                        'parameter': 'bowler_team_id',
                        'value': name,
                        'video': {
                            "sixes": json.loads(group[group['is_six'] == 1].to_json(orient="records")),
                            "fours": json.loads(group[group['is_four'] == 1].to_json(orient="records")),
                            "wickets": json.loads(group[group['is_wicket'] == 1].to_json(orient="records"))
                        }
                    })
            else:
                bowler_comparison_result = batter_vs_bowler_query_response.groupby("bowler_id")
                for name, group in bowler_comparison_result:
                    response.append({
                        'parameter': 'bowler_id',
                        'value': name,
                        'video': {
                            "sixes": json.loads(group[group['is_six'] == 1].to_json(orient="records")),
                            "fours": json.loads(group[group['is_four'] == 1].to_json(orient="records")),
                            "wickets": json.loads(group[group['is_wicket'] == 1].to_json(orient="records"))
                        }
                    })
            return jsonify(response), 200, logger.info("Status - 200")

        if request.args.get('widget') == 'bowler_vs_batter':
            if "team_id" in filter_dict:
                filter_dict['bowler_team_id'] = filter_dict["team_id"]
                del filter_dict['team_id']

            if filter_dict.get('player_id'):
                filter_dict['bowler_id'] = filter_dict.get('player_id')
                del filter_dict['player_id']
            filters, params = generateWhereClause(filter_dict)
            bowler_vs_batter_query = "select * from join_data" + filters
            bowler_vs_batter_response = executeQuery(con, bowler_vs_batter_query, params)
            if filter_dict.get('batsman_team_id'):
                bowler_vs_batter_response = bowler_vs_batter_response.groupby("batsman_team_id")
                response = []
                for name, group in bowler_vs_batter_response:
                    response.append({
                        'parameter': 'batsman_team_id',
                        'value': name,
                        'video': {
                            "sixes": json.loads(group[group['is_six'] == 1].to_json(orient="records")),
                            "fours": json.loads(group[group['is_four'] == 1].to_json(orient="records")),
                            "wickets": json.loads(group[group['is_wicket'] == 1].to_json(orient="records"))
                        }
                    })
            else:
                bowler_vs_batter_response = bowler_vs_batter_response.groupby("batsman_id")
                response = []
                for name, group in bowler_vs_batter_response:
                    response.append({
                        'parameter': 'batsman_id',
                        'value': name,
                        'video': {
                            "sixes": json.loads(group[group['is_six'] == 1].to_json(orient="records")),
                            "fours": json.loads(group[group['is_four'] == 1].to_json(orient="records")),
                            "wickets": json.loads(group[group['is_wicket'] == 1].to_json(orient="records"))
                        }
                    })
            return jsonify(response), 200, logger.info("Status - 200")

        if request.args.get('widget') == 'tournament':
            filters, params = generateWhereClause({"match_id": request.args.get('match_id')})
            ball_summary_query = "select * from ball_summary_df" + filters
            players_data_df = "select * from players_data_df"
            players_data_df = executeQuery(con, players_data_df)
            players_data_df = players_data_df.drop_duplicates(subset=['player_id'])[["player_id", "player_name"]]

            teams_data_df = "select * from teams_data"
            teams_data_df = executeQuery(con, teams_data_df)
            teams_data_df = teams_data_df.drop_duplicates(subset=['team_name'])[["team_id", "team_name"]]

            ball_summary_result = executeQuery(con, ball_summary_query, params)
            innings_group = ball_summary_result.groupby('innings')
            innings2_bowling, innings1_batting, innings2_batting, innings1_bowling = [], [], [], []
            for name, group in innings_group:
                # Innings 1
                if name == 1:
                    batting_df_innings_one = pd.merge(
                        ball_summary_result,
                        players_data_df,
                        left_on='batsman_id',
                        right_on='player_id',
                        how='left'
                    )
                    batting_df_innings_one = pd.merge(
                        batting_df_innings_one,
                        teams_data_df,
                        left_on='batsman_team_id',
                        right_on='team_id',
                        how='left'
                    )

                    batting_df_innings_one_group = batting_df_innings_one.groupby('player_name')
                    for name, group in batting_df_innings_one_group:
                        json_obj = group.to_dict(orient='records')
                        innings1_batting.append({
                            'value': name,
                            'video': [iter for iter in json_obj]
                        })
                    bowling_df_innings_one = pd.merge(
                        ball_summary_result,
                        players_data_df,
                        left_on='bowler_id',
                        right_on='player_id',
                        how='left'
                    )
                    bowling_df_innings_one = pd.merge(
                        bowling_df_innings_one,
                        teams_data_df,
                        left_on='bowler_team_id',
                        right_on='team_id',
                        how='left'
                    )

                    bowling_df_innings_one_group = bowling_df_innings_one.groupby('player_name')
                    for name, group in bowling_df_innings_one_group:
                        json_obj = group.to_dict(orient='records')
                        innings1_bowling.append({
                            'value': name,
                            'video': [iter for iter in json_obj]
                        })
                # Innings 2
                else:
                    batting_df_innings_two = pd.merge(
                        ball_summary_result,
                        players_data_df,
                        left_on='batsman_id',
                        right_on='player_id',
                        how='left'
                    )
                    batting_df_innings_two = pd.merge(
                        batting_df_innings_two,
                        teams_data_df,
                        left_on='batsman_team_id',
                        right_on='team_id',
                        how='left'
                    )
                    batting_df_innings_two_group = batting_df_innings_two.groupby('player_name')
                    for name, group in batting_df_innings_two_group:
                        json_obj = group.to_dict(orient='records')
                        innings2_batting.append({
                            'value': name,
                            'video': [iter for iter in json_obj]
                        })
                    bowling_df_innings_two = pd.merge(
                        ball_summary_result,
                        players_data_df,
                        left_on='bowler_id',
                        right_on='player_id',
                        how='left'
                    )
                    bowling_df_innings_two = pd.merge(
                        bowling_df_innings_two,
                        teams_data_df,
                        left_on='bowler_team_id',
                        right_on='team_id',
                        how='left'
                    )
                    bowling_df_innings_two_group = bowling_df_innings_two.groupby('player_name')

                    for name, group in bowling_df_innings_two_group:
                        json_obj = group.to_dict(orient='records')
                        innings2_bowling.append({
                            'value': name,
                            'video': [iter for iter in json_obj]
                        })

            return jsonify({
                "bowling": {
                    "innings1": innings1_bowling,
                    "innings2": innings2_bowling
                },
                "batting": {
                    "innings1": innings1_batting,
                    "innings2": innings2_batting
                }}
            ), 200, logger.info("Status - 200")
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/getBIFilters", methods=['POST'])
@token_required
def getBIFilters():
    logger = get_logger("getBIFilters", "getBIFilters")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        filters_df = teams_data.explode('seasons_played')[
            ["team_id", "team_name", "team_short_name", "competition_name", "team_image_url", "seasons_played"]].rename(
            columns={"seasons_played": "season"})
        filters_df['team_image_url'] = filters_df['team_image_url'].apply(
            lambda x: "" if ((len(x) == 0) or x is None) else x)

        if request.json:
            if "competition_name" in request.json:
                filters_df = filters_df[filters_df['competition_name'].isin(request.json['competition_name'])]

            if "season" in request.json:
                filters_df = filters_df[filters_df['season'].isin(request.json['season'])]

        filters_df['team_details'] = filters_df[['team_id', 'team_name', 'team_image_url']].to_dict(orient='records')

        filters_df = filters_df[['competition_name', 'season', 'team_details']].groupby(['competition_name', 'season'])[
            'team_details'].agg(list).reset_index()
        response = filters_df.groupby(['competition_name']).apply(
            lambda x: x.set_index('season').to_dict(orient='index')).to_dict()
        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/competitionName", methods=['GET'])
@token_required
def competitionName():
    logger = get_logger("competitionName", "competitionName")
    try:
        competition_name = executeQuery(
            con,
            '''select distinct competition_name from teams_data order by competition_name'''
        )
        response = set(competition_name['competition_name'])
        return jsonify(sorted(list(response))), 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/battingTeamStats", methods=['POST'])
@token_required
def battingTeamStats():
    logger = get_logger("battingTeamStats", "battingTeamStats")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        response = {}
        if request.json:
            if request.json.get('overs'):
                filter_dict['over_number'] = request.json.get('overs')

        filters, params = generateWhereClause(filter_dict)

        if filters:
            batting_team_stats_sql = '''
            select 
              team_id, 
              team_name, 
              count(
                distinct(match_id)
              ) as total_matches, 
              cast(
                sum(team_runs) as int
              ) as runs, 
              team_image_url, 
              cast(
                sum(sixes) as int
              ) as sixes, 
              cast(
                sum(fours) as int
              ) as fours, 
              round(
                (
                  sum(team_runs)* 100.00
                )/ sum(balls), 
                2
              ) as strike_rate 
            from 
              batsman_overwise_df 
            ''' + filters + ''' 
            group by 
              team_id, 
              team_name, 
              team_image_url
            '''

            out_batsman_sql = '''
            select 
              batsman_team_id, 
              round(
                count(out_batsman_id)
              ) as wicket_cnt 
            from 
              join_data
            ''' + filters.replace(
                ' team_id',
                ' batsman_team_id'
            ).replace(
                "player_id", "out_batsman_id"
            ) + ''' and  out_batsman_id<>-1 and innings not in (3,4) group by batsman_team_id'''

            highest_scorer_sql = '''
            select 
              team_name, 
              MAX(highest_score) as highest_score, 
              count(distinct match_id) as innings_played, 
              team_image_url 
            from 
              (
                select 
                  bat_current_team as team_name, 
                  cast(
                    sum(team_runs) as int
                  ) as highest_score, 
                  match_id, 
                  team_image_url 
                from 
                  batsman_overwise_df
            ''' + filters + ''' 
            group by 
              match_id, 
              bat_current_team, 
              team_image_url 
            order by 
              highest_score desc, 
              team_name asc
            ) 
            group by 
              team_name, 
              team_image_url
            '''

            first_last_ball_boundary_sql = '''
            select 
              team_name, 
              team_image_url, 
              cast(
                sum(first_ball_boundary) as int
              ) as first_ball_boundary, 
              cast(
                sum(last_ball_boundary) as int
              ) as last_ball_boundary, 
              count(distinct match_id) as innings_played 
            from 
              (
                select 
                  match_id, 
                  over_number, 
                  batsman_team_id, 
                  batsman_team_name as team_name, 
                  MIN(ball_number) as FirstBall, 
                  MAX(ball_number) as LastBall, 
                  cast(
                    sum(
                      case when ball_number = 1 
                      and is_extra <> 1 
                      and (
                        is_four = 1 
                        or is_six = 1
                      ) then 1 else 0 end
                    ) as int
                  ) as first_ball_boundary, 
                  cast(
                    sum(
                      case when ball_number = 6 
                      and is_extra <> 1 
                      and (
                        is_four = 1 
                        or is_six = 1
                      ) then 1 else 0 end
                    ) as int
                  ) as last_ball_boundary, 
                  batting_team_image_url as team_image_url 
                from 
                  join_data 
            ''' + filters.replace(
                'player_id', 'batsman_id'
            ).replace(
                ' team_id',
                ' batsman_team_id'
            ).replace(
                'player_skill',
                'batsman_skill'
            ) + '''
            and innings not in (3, 4) 
                group by 
                  over_number, 
                  match_id, 
                  batsman_team_id, 
                  batsman_team_name, 
                  batting_team_image_url
                ) 
                group by 
                  team_name, 
                  team_image_url
                '''
        else:
            batting_team_stats_sql = '''
            select 
              team_id, 
              team_name, 
              count(
                distinct(match_id)
              ) as total_matches, 
              cast(
                sum(team_runs) as int
              ) as runs, 
              team_image_url, 
              cast(
                sum(sixes) as int
              ) as sixes, 
              cast(
                sum(fours) as int
              ) as fours, 
              round(
                (
                  sum(team_runs)* 100.00
                )/ sum(balls), 
                2
              ) as strike_rate 
            from 
              batsman_overwise_df 
            group by 
              team_id, 
              team_name, 
              team_image_url
            '''

            out_batsman_sql = '''
            select 
              batsman_team_id, 
              round(
                count(out_batsman_id)
              ) as wicket_cnt 
            from 
              join_data 
            where 
              out_batsman_id <>-1 
              and innings not in (3, 4) 
            group by 
              batsman_team_id
            '''

            highest_scorer_sql = '''
            select 
              team_name, 
              MAX(highest_score) as highest_score, 
              team_image_url, 
              count(distinct match_id) as innings_played 
            from 
              (
                select 
                  team_image_url, 
                  bat_current_team as team_name, 
                  cast(
                    sum(team_runs) as int
                  ) as highest_score, 
                  match_id 
                from 
                  batsman_overwise_df 
                group by 
                  match_id, 
                  bat_current_team, 
                  team_image_url 
                order by 
                  highest_score desc, 
                  team_name asc
              ) 
            group by 
              team_name, 
              team_image_url
            '''

            first_last_ball_boundary_sql = '''
            select 
              team_name, 
              team_image_url, 
              cast(
                sum(first_ball_boundary) as int
              ) as first_ball_boundary, 
              cast(
                sum(last_ball_boundary) as int
              ) as last_ball_boundary, 
              count(distinct match_id) as innings_played 
            from 
              (
                select 
                  match_id, 
                  over_number, 
                  batsman_team_id, 
                  batsman_team_name as team_name, 
                  batting_team_image_url as team_image_url, 
                  MIN(ball_number) as FirstBall, 
                  MAX(ball_number) as LastBall, 
                  cast(
                    sum(
                      case when ball_number = 1 
                      and is_extra <> 1 
                      and (
                        is_four = 1 
                        or is_six = 1
                      ) then 1 else 0 end
                    ) as int
                  ) as first_ball_boundary, 
                  cast(
                    sum(
                      case when ball_number = 6 
                      and is_extra <> 1 
                      and (
                        is_four = 1 
                        or is_six = 1
                      ) then 1 else 0 end
                    ) as int
                  ) as last_ball_boundary 
                from 
                  join_data 
                where 
                  innings not in (3, 4) 
                group by 
                  over_number, 
                  match_id, 
                  batsman_team_id, 
                  batsman_team_name, 
                  batting_team_image_url
              ) 
            group by 
              team_name, 
              team_image_url
                        '''

        final_team_stat_sql = '''
        select 
          team_name, 
          team_image_url, 
          runs, 
          total_matches, 
          strike_rate, 
          fours, 
          sixes, 
          round(
            coalesce(
              (tsd.runs * 1.00)/ obd.wicket_cnt, 
              0.0
            ), 
            2
          ) as average 
        from 
          team_stat_df tsd 
          left join out_batsman_df obd on (
            obd.batsman_team_id = tsd.team_id
          )
        '''
        team_stat_df = executeQuery(con, batting_team_stats_sql, params)
        out_batsman_df = executeQuery(con, out_batsman_sql, params)
        teamStatsDF = executeQuery(con, final_team_stat_sql)
        highest_scorer_df = executeQuery(con, highest_scorer_sql, params)
        firstLastBallBoundaryDF = executeQuery(con, first_last_ball_boundary_sql, params)

        if "min_innings" in request.json:
            min_innings = request.json['min_innings']
            teamStatsDF = teamStatsDF[teamStatsDF['total_matches'] >= min_innings]
            highest_scorer_df = highest_scorer_df[highest_scorer_df['innings_played'] >= min_innings]
            firstLastBallBoundaryDF = firstLastBallBoundaryDF[firstLastBallBoundaryDF['innings_played'] >= min_innings]

        stat_list = ['runs', 'average', 'strike_rate', 'sixes', 'fours']

        for stat in stat_list:
            teamStatsDF['team_rank'] = teamStatsDF[stat].rank(method='first', ascending=False).astype(int)

            teamStatsDF['team_details'] = teamStatsDF[[
                'total_matches', 'runs', 'average', 'strike_rate', 'team_rank', 'sixes', 'fours'
            ]].to_dict(orient='records')

            response[stat] = teamStatsDF.sort_values(['team_rank']).head(10).groupby(
                ['team_name', 'team_image_url']
            )['team_details'].agg(list).reset_index().to_dict('records')

            highest_scorer_df['team_rank'] = highest_scorer_df['highest_score'].rank(
                method='first', ascending=False).astype(int)

            highest_scorer_df['team_details'] = highest_scorer_df[[
                'highest_score', 'team_rank'
            ]].to_dict(orient='records')
            response['highest_score'] = highest_scorer_df.sort_values(['team_rank']).head(10).groupby(
                ['team_name', 'team_image_url']
            )['team_details'].agg(list).reset_index().to_dict('records')
        boundary_stat_list = ['first_ball_boundary', 'last_ball_boundary']

        for stats in boundary_stat_list:
            firstLastBallBoundaryDF['team_rank'] = firstLastBallBoundaryDF[stats].rank(
                method='first', ascending=False).astype(int)

            firstLastBallBoundaryDF['team_details'] = firstLastBallBoundaryDF[[
                'first_ball_boundary', 'last_ball_boundary', 'team_rank'
            ]].to_dict(orient='records')

            response[stats] = firstLastBallBoundaryDF.sort_values(['team_rank']).head(10).groupby(
                ['team_name', 'team_image_url']
            )['team_details'].agg(list).reset_index().to_dict('records')
        return jsonify(response), 200, logger.info("Status - 200")
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/bowlingTeamStats", methods=['POST'])
@token_required
def bowlingTeamStats():
    logger = get_logger("bowlingTeamStats", "bowlingTeamStats")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        response = {}
        if request.json:
            if request.json.get('overs'):
                filter_dict['over_number'] = request.json.get('overs')
            if request.json.get('winning_type'):
                winning_change = {
                    "Winning": "Losing",
                    "Losing": "Winning"
                }
                filter_dict['winning_type'] = winning_change[request.json.get('winning_type')]
        filters, params = generateWhereClause(filter_dict)

        if filters:
            bowling_team_stats_sql = '''
            select 
              team_id, 
              team_name, 
              count(
                distinct(match_id)
              ) as innings_played, 
              team_image_url, 
              cast(
                sum(team_runs) as int
              ) as runs_conceded, 
              coalesce(
                round(
                  (
                    sum(team_runs)* 1.00
                  )/ sum(wickets), 
                  2
                ), 
                0.0
              ) as average, 
              cast(
                sum(sixes) as int
              ) as sixes_conceded, 
              cast(
                sum(fours) as int
              ) as fours_conceded, 
              cast(
                sum(dot_balls) as int
              ) as dot_balls, 
              round(
                coalesce(
                  (
                    (
                      sum(balls)* 1.00
                    )/ sum(wickets)
                  ), 
                  0
                ), 
                2
              ) as strike_rate, 
              cast(
                sum(wickets) as int
              ) as wickets, 
              cast(
                (
                  sum(wides)+ sum(no_balls)
                ) as int
              ) as extras, 
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
              bowler_overwise_df 
            ''' + filters + ''' group by team_id, team_name, team_image_url'''

            first_last_ball_boundary_sql = '''
            select 
              team_name, 
              team_image_url, 
              cast(
                sum(first_ball_boundary) as int
              ) as first_ball_boundary, 
              cast(
                sum(last_ball_boundary) as int
              ) as last_ball_boundary, 
              count(distinct match_id) as innings_played 
            from 
              (
                select 
                  match_id, 
                  over_number, 
                  bowler_team_id, 
                  bowler_team_name as team_name, 
                  bowling_team_image_url as team_image_url, 
                  MIN(ball_number) as first_ball, 
                  MAX(ball_number) as last_ball, 
                  cast(
                    sum(
                      case when ball_number = 1 
                      and is_extra <> 1 
                      and (
                        is_four = 1 
                        or is_six = 1
                      ) then 1 else 0 end
                    ) as int
                  ) as first_ball_boundary, 
                  cast(
                    sum(
                      case when ball_number = 6 
                      and is_extra <> 1 
                      and (
                        is_four = 1 
                        or is_six = 1
                      ) then 1 else 0 end
                    ) as int
                  ) as last_ball_boundary 
                from 
                  join_data 
            ''' + filters.replace(
                'player_id', 'bowler_id'
            ).replace(
                ' team_id', ' bowler_team_id'
            ).replace('player_skill', 'bowler_skill') + ''' 
            and innings not in (3, 4) 
                group by 
                  over_number, 
                  match_id, 
                  bowler_team_id, 
                  bowler_team_name, 
                  bowling_team_image_url
            ) 
                group by 
                  team_name, 
                  team_image_url
                '''
            if "min_innings" in request.json:
                min_innings = request.json['min_innings']
                first_last_ball_boundary_sql = first_last_ball_boundary_sql + f" having innings_played >= {min_innings}"
                bowling_team_stats_sql = bowling_team_stats_sql + f" having innings_played >= {min_innings}"
        else:
            bowling_team_stats_sql = '''
            select 
              team_id, 
              team_name, 
              team_image_url, 
              count(
                distinct(match_id)
              ) as total_matches, 
              cast(
                sum(team_runs) as int
              ) as runs_conceded, 
              coalesce(
                round(
                  (
                    sum(team_runs)* 1.00
                  )/ sum(wickets), 
                  2
                ), 
                0.0
              ) as average, 
              cast(
                sum(sixes) as int
              ) as sixes_conceded, 
              cast(
                sum(fours) as int
              ) as fours_conceded, 
              cast(
                sum(dot_balls) as int
              ) as dot_balls, 
              round(
                coalesce(
                  (
                    (
                      sum(balls)* 1.00
                    )/ sum(wickets)
                  ), 
                  0
                ), 
                2
              ) as strike_rate, 
              cast(
                sum(wickets) as int
              ) as wickets, 
              cast(
                (
                  sum(wides)+ sum(no_balls)
                ) as int
              ) as extras, 
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
              bowler_overwise_df 
            group by 
              team_id, 
              team_name, 
              team_image_url
            '''

            first_last_ball_boundary_sql = '''
            select 
              team_name, 
              team_image_url, 
              cast(
                sum(first_ball_boundary) as int
              ) as first_ball_boundary, 
              cast(
                sum(last_ball_boundary) as int
              ) as last_ball_boundary, 
              count(distinct match_id) as innings_played 
            from 
              (
                select 
                  match_id, 
                  over_number, 
                  bowler_team_id, 
                  bowler_team_name as team_name, 
                  bowling_team_image_url as team_image_url, 
                  MIN(ball_number) as first_ball, 
                  MAX(ball_number) as last_ball, 
                  cast(
                    sum(
                      case when ball_number = 1 
                      and is_extra <> 1 
                      and (
                        is_four = 1 
                        or is_six = 1
                      ) then 1 else 0 end
                    ) as int
                  ) as first_ball_boundary, 
                  cast(
                    sum(
                      case when ball_number = 6 
                      and is_extra <> 1 
                      and (
                        is_four = 1 
                        or is_six = 1
                      ) then 1 else 0 end
                    ) as int
                  ) as last_ball_boundary 
                from 
                  join_data 
                where 
                  innings not in (3, 4) 
                group by 
                  over_number, 
                  match_id, 
                  bowler_team_id, 
                  bowler_team_name, 
                  bowling_team_image_url
              ) 
            group by 
              team_name, 
              team_image_url
            '''

        bowler_stats_df = executeQuery(con, bowling_team_stats_sql, params)
        firstLastBallBoundaryDF = executeQuery(con, first_last_ball_boundary_sql, params)

        stat_list = [
            'runs_conceded',
            'wickets',
            'economy',
            'average',
            'dot_balls',
            'strike_rate',
            'sixes_conceded',
            'fours_conceded',
            'extras'
        ]

        for stat in stat_list:
            if stat in [
                "average",
                "economy",
                "runs_conceded",
                "sixes_conceded",
                "fours_conceded",
                "extras",
                "strike_rate"
            ]:
                bowler_stats_df['team_rank'] = bowler_stats_df[bowler_stats_df[stat] >= 0][stat].rank(
                    method='first', ascending=False
                ).astype(int)
            else:
                bowler_stats_df['team_rank'] = bowler_stats_df[stat].rank(method='first', ascending=False).astype(int)

            bowler_stats_df['team_details'] = bowler_stats_df[[
                "average",
                "economy",
                "runs_conceded",
                "sixes_conceded",
                "fours_conceded",
                "extras",
                "strike_rate",
                "team_rank"
            ]].to_dict(orient='records')

            response[stat] = bowler_stats_df.sort_values(['team_rank']).head(10).groupby(
                ['team_name', 'team_image_url']
            )['team_details'].agg(list).reset_index().to_dict('records')
        boundary_stat_list = ['first_ball_boundary', 'last_ball_boundary']
        for stats in boundary_stat_list:
            firstLastBallBoundaryDF['team_rank'] = firstLastBallBoundaryDF[stats].rank(
                method='first', ascending=False).astype(int)
            firstLastBallBoundaryDF['team_details'] = firstLastBallBoundaryDF[[
                'first_ball_boundary',
                'last_ball_boundary',
                'team_rank'
            ]].to_dict(orient='records')
            response[stats] = firstLastBallBoundaryDF.sort_values(['team_rank']).head(10).groupby(
                ['team_name', 'team_image_url']
            )['team_details'].agg(list).reset_index().to_dict('records')
        return jsonify(response), 200, logger.info("Status - 200")
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/putSearchQueryFeedback", methods=['POST'])
@token_required
def putSearchQueryFeedback():
    logger = get_logger("putSearchQueryFeedback", "putSearchQueryFeedback")
    try:
        json_data = request.json
        search_feedback_Df = pd.DataFrame(json_data)
        search_feedback_Df['ai_api_response'] = search_feedback_Df['ai_api_response'].apply(
            lambda x: str(x).replace("'", "''"))
        search_feedback_Df['load_timestamp'] = load_timestamp

        max_key_val = getMaxId(session, QUERY_FEEDBACK_TABLE_NAME, "id", DB_NAME)
        searchQueryFeedbackDF = generateSeq(search_feedback_Df, "id", max_key_val)

        if len(searchQueryFeedbackDF) > 0:
            insertToDB(session, searchQueryFeedbackDF.to_dict(orient='records'), DB_NAME, QUERY_FEEDBACK_TABLE_NAME)

            response = jsonify("Data Inserted Successfully!")
        else:
            response = jsonify("No New Data!")

        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.info(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/playerTrackingDashboard", methods=['POST'])
@token_required
def playerTrackingDashboard():
    logger = get_logger("playerTrackingDashboard", "playerTrackingDashboard")
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

        if "asc" in request.json:
            asc = request.json['asc']
        else:
            asc = False

        if "sort_key" in request.json:
            sort_key = request.json['sort_key']
            sort_key_bowling = "wickets"
        elif "sort_key_bowling" in request.json:
            sort_key_bowling = request.json['sort_key_bowling']
            sort_key = "runs_scored"
        else:
            sort_key = "runs_scored"
            sort_key_bowling = "wickets"

        filters, params = generateWhereClause(filter_dict)

        if "from_date" and "to_date" in request.json:
            from_date = request.json['from_date']
            f_from_date = datetime.strptime(
                datetime.strptime(str(from_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                '%Y-%m-%d').date()
            to_date = request.json['to_date']
            f_to_date = datetime.strptime(datetime.strptime(str(to_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                                          '%Y-%m-%d').date()
        else:
            f_from_date = '2000-01-01'
            f_to_date = datetime.now().strftime("%Y-%m-%d")

        if "match_type" in request.json:
            match_type = request.json['match_type']
            if match_type.upper() == 'ODI' or match_type.upper() == 'TEST':
                batsman_details = batsman_overwise_df[
                    batsman_overwise_df['competition_name'].str.upper() == match_type.upper()]
                bowler_details = bowler_overwise_df[
                    bowler_overwise_df['competition_name'].str.upper() == match_type.upper()]
            else:
                batsman_details = batsman_overwise_df[(batsman_overwise_df['competition_name'].str.upper() != 'ODI') & (
                        batsman_overwise_df['competition_name'].str.upper() != 'TEST')]
                bowler_details = bowler_overwise_df[(bowler_overwise_df['competition_name'].str.upper() != 'ODI') & (
                        bowler_overwise_df['competition_name'].str.upper() != 'TEST')]
        else:
            batsman_details = batsman_overwise_df
            bowler_details = bowler_overwise_df

        if filters:
            batsmanAggDetailSQL = '''select player_id, player_name, player_nationality, 
                cast(sum(runs) as int) as runs_scored, count(distinct match_id) as matches,
                round(coalesce(((sum(runs)*100.00)/sum(balls)),0.0),2) as strike_rate, cast(sum(balls) as int) as balls_played,
                round(coalesce((sum(runs)*1.00)/sum(wickets),0.0),2) as average, cast(sum(fours) as int) as fours,
                cast(sum(sixes) as int) as sixes, cast(max(runs) as int) as best_score, cast(sum(not_out) as int) as not_out, player_image_url from
                 (select  player_name, player_image_url, player_nationality, bat_current_team, player_id, innings, sum(runs) as runs, sum(balls) as balls,
                 match_id, sum(fours) as fours, sum(sixes) as sixes, sum(wickets) as wickets, not_out, competition_name from batsman_details ''' + \
                                  filters + f''' and match_date >= '{f_from_date}' and match_date <= '{f_to_date}' group by player_id, player_nationality, not_out, match_id, innings,
                                  player_name, bat_current_team, competition_name, player_image_url)
                                           group by player_id, player_name, player_nationality, player_image_url'''

            outBatsmanSQL = '''select out_batsman_id, round(count(out_batsman_id)) as wicket_cnt 
                                from join_data ''' + filters.replace('player_id', 'out_batsman_id').replace('team_id',
                                                                                                            'batsman_team_id').replace(
                'player_skill', 'out_batsman_skill') + \
                            f''' and out_batsman_id<>-1 and innings not in (3,4) and match_date_form >= '{f_from_date}' and match_date_form <= '{f_to_date}'
                              group by out_batsman_id'''

            bowlerAggDetailSQL = '''select player_name, player_id, player_nationality, count(distinct match_id) as matches,
                cast(sum(wickets) as int) as wickets, cast(sum(runs) as int) as runs, 
                round(case when (sum(balls)%6)==0 then ((sum(balls)*1.00)/6) else (sum(balls)/6)+((sum(balls)%6)/10.00) end,2) as overs, 
                round(coalesce(((sum(balls)*1.00)/sum(wickets)),0.0),2) as strike_rate, cast(sum(dot_balls) as int) as dot_balls,
                round(coalesce(((sum(runs)*1.00)/(sum(balls)*1.00/6)),0.0),2) as economy, player_image_url  
                from ( select match_id, player_name, player_id, player_nationality, bowl_current_team as team_name, innings, competition_name, 
                sum(balls) as balls, sum(runs) as runs, sum(wickets) as wickets, sum(dot_balls) as dot_balls, player_image_url 
                from bowler_details ''' + filters + f''' and match_date >= '{f_from_date}' and match_date <= '{f_to_date}' group by match_id, player_id, player_nationality, team_id, bowl_current_team, competition_name, 
                player_name, innings, player_image_url) group by player_id, player_nationality, player_name, player_image_url'''

        else:
            batsmanAggDetailSQL = f'''select player_id, player_name, player_nationality,   
                cast(sum(runs) as int) as runs_scored, count(distinct match_id) as matches, 
                round(coalesce(((sum(runs)*100.00)/sum(balls)),0.0),2) as strike_rate, cast(sum(balls) as int) as balls_played, 
                round(coalesce((sum(runs)*1.00)/sum(wickets),0.0),2) as average, cast(sum(fours) as int) as fours, 
                cast(sum(sixes) as int) as sixes, cast(max(runs) as int) as best_score, cast(sum(not_out) as int) as not_out, player_image_url from
                 (select  player_name, player_image_url, player_nationality, bat_current_team, player_id, innings, sum(runs) as runs, sum(balls) as balls,
                 match_id, sum(fours) as fours, sum(sixes) as sixes, sum(wickets) as wickets, not_out, competition_name from batsman_details 
                 where match_date >= '{f_from_date}' and match_date <= '{f_to_date}' group by player_id, player_nationality, not_out, match_id, innings, 
                player_name, bat_current_team, competition_name, player_image_url) group by player_id, player_name, player_nationality, player_image_url'''

            outBatsmanSQL = '''select out_batsman_id, round(count(out_batsman_id)) as wicket_cnt  
                                            from join_data where out_batsman_id<>-1 and innings not in (3,4)''' + f''' and match_date_form >= '{f_from_date}' and match_date_form <= '{f_to_date}'
                                            group by out_batsman_id'''

            bowlerAggDetailSQL = f'''select player_name, player_id, player_nationality, count(distinct match_id) as matches,
                cast(sum(wickets) as int) as wickets, cast(sum(runs) as int) as runs, 
                round(case when (sum(balls)%6)==0 then ((sum(balls)*1.00)/6) else (sum(balls)/6)+((sum(balls)%6)/10.00) end,2) as overs, 
                round(coalesce(((sum(balls)*1.00)/sum(wickets)),0.0),2) as strike_rate, cast(sum(dot_balls) as int) as dot_balls,
                round(coalesce(((sum(runs)*1.00)/(sum(balls)*1.00/6)),0.0),2) as economy, player_image_url  
                from ( select match_id, player_name, player_id, player_nationality, bowl_current_team as team_name, innings, competition_name, 
                sum(balls) as balls, sum(runs) as runs, sum(wickets) as wickets, sum(dot_balls) as dot_balls, player_image_url 
                from bowler_details where match_date >= '{f_from_date}' and match_date <= '{f_to_date}' group by match_id, player_id, player_nationality, team_id, bowl_current_team, competition_name, 
                player_name, innings, player_image_url) group by player_id, player_nationality, player_name, player_image_url'''

        finalBatsmanAggDetailSQL = '''select player_id, player_name, player_nationality, runs_scored, matches, strike_rate, 
        balls_played, fours, sixes, best_score, player_image_url, round(coalesce((hsd.runs_scored*1.00)/obd.wicket_cnt,0.0),2) as average, 
        not_out from batsmanAggDetailDF hsd left join OutBatsmanDf obd on (obd.out_batsman_id=hsd.player_id)'''

        batsmanAggDetailDF = executeQuery(con, batsmanAggDetailSQL, params)
        OutBatsmanDf = executeQuery(con, outBatsmanSQL, params)
        BatsmanStatsDf = executeQuery(con, finalBatsmanAggDetailSQL)
        BowlerStatsDf = executeQuery(con, bowlerAggDetailSQL, params)

        if sort_key != "player_name":
            BatsmanStatsDf = BatsmanStatsDf[BatsmanStatsDf[sort_key] >= 0]
        elif sort_key_bowling != "player_name":
            BowlerStatsDf = BowlerStatsDf[BowlerStatsDf[sort_key_bowling] >= 0]

        response['batting_stats'] = BatsmanStatsDf.sort_values(sort_key, ascending=asc).to_dict(orient='records')
        response['bowling_stats'] = BowlerStatsDf.sort_values(sort_key_bowling, ascending=asc).to_dict(orient='records')

        return jsonify(response), logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/playerMatchDetails", methods=['POST'])
@token_required
def playerMatchDetails():
    logger = get_logger("playerMatchDetails", "playerMatchDetails")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        response = {}
        filters, params = generateWhereClause(filter_dict)

        if "from_date" and "to_date" in request.json:
            from_date = request.json['from_date']
            f_from_date = datetime.strptime(
                datetime.strptime(str(from_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                '%Y-%m-%d').date()
            to_date = request.json['to_date']
            f_to_date = datetime.strptime(datetime.strptime(str(to_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                                          '%Y-%m-%d').date()
        else:
            f_from_date = '2000-01-01'
            f_to_date = datetime.now().strftime("%Y-%m-%d")

        if filters:
            battingCardSQL = '''select cast(bc.batsman_id as int)as player_id, pd.player_name, bc.competition_name, bc.innings, cast(bc.runs as int) as runs, cast(bc.balls as int) as balls, 
            cast(bc.batting_position as int) as batting_position, round(coalesce(cast(bc.strike_rate as decimal), 0.0), 2) as strike_rate, cast(bc.fours as int) as fours, cast(bc.sixes as int) as sixes, 
            cast((case when bc.out_desc = 'not out' then 1 else 0 end) as int) as not_out, bc.batting_team_id as team_id, md.match_date, md.match_date_form, md.match_name, pd.player_type as player_nationality, venue from bat_card_data bc 
            left join (select match_date, match_date_form, match_name, match_id, venue from matches_df) md on (bc.match_id=md.match_id) left join (select distinct player_id, player_name, player_type from players_data) pd on (bc.batsman_id = pd.player_id) ''' \
                             + filters.replace('player_id',
                                               'batsman_id') + f''' and balls > 0 and match_date_form >= '{f_from_date}' and match_date_form <= '{f_to_date}' '''

            bowlingCardSQL = '''select bc.bowler_id as player_id, pd.player_name, bc.competition_name, bc.innings, bc.overs, cast(bc.runs as int) as runs, 
            cast(bc.wickets as int) as wickets, round(coalesce(cast(bc.strike_rate as decimal), 0.0),2) as strike_rate, round(coalesce(cast(bc.economy as decimal), 0.0),2) as economy, 
            md.match_date, md.match_date_form, md.match_name, pd.player_type as player_nationality, venue from bowl_card_data bc left join (select match_date, match_date_form, match_name, match_id, venue from matches_df) md on (bc.match_id=md.match_id) 
            left join (select distinct player_id, player_name, player_type from players_data) pd on (bc.bowler_id = pd.player_id) ''' + filters.replace(
                'player_id', 'bowler_id') + \
                             f''' and match_date_form >= '{f_from_date}' and match_date_form <= '{f_to_date}' '''
        else:
            battingCardSQL = f'''select cast(bc.batsman_id as int)as player_id, pd.player_name, bc.competition_name, bc.innings, cast(bc.runs as int) as runs, cast(bc.balls as int) as balls, 
            cast(bc.batting_position as int) as batting_position, round(coalesce(cast(bc.strike_rate as decimal),0.0),2) as strike_rate, cast(bc.fours as int) as fours, cast(bc.sixes as int) as sixes, 
            cast((case when bc.out_desc = 'not out' then 1 else 0 end) as int) as not_out, md.match_date, md.match_date_form, md.match_name, pd.player_type as player_nationality, venue from bat_card_data 
            left join (select match_date, match_date_form, match_name, match_id, venue from matches_df) md on (bc.match_id=md.match_id) left join (select distinct player_id, player_name, player_type from players_data) pd on (bc.batsman_id = pd.player_id) 
            where balls > 0 and match_date_form >= '{f_from_date}' and match_date_form <= '{f_to_date}' '''

            bowlingCardSQL = f'''select bc.bowler_id as player_id, pd.player_name, bc.competition_name, bc.innings, bc.overs, cast(bc.runs as int) as runs, 
            cast(bc.wickets as int) as wickets, round(coalesce(cast(bc.strike_rate as decimal), 0.0),2) as strike_rate, round(coalesce(cast(bc.economy as decimal), 0.0),2) as economy, 
            md.match_date, md.match_date_form, md.match_name, pd.player_type as player_nationality, venue from bowl_card_data bc left join (select match_date, match_date_form, match_name, match_id, venue from matches_df) md on (bc.match_id=md.match_id) 
            left join (select distinct player_id, player_name, player_type from players_data) pd on (bc.bowler_id = pd.player_id) 
            where match_date_form >= '{f_from_date}' and match_date_form <= '{f_to_date}' '''

        battingCardDF = executeQuery(con, battingCardSQL, params)
        bowlingCardDF = executeQuery(con, bowlingCardSQL, params)

        if "match_type" in request.json:
            match_type = request.json['match_type']
            if match_type.upper() == 'ODI' or match_type.upper() == 'TEST':
                battingCardDF = battingCardDF[battingCardDF['competition_name'].str.upper() == match_type.upper()]
                bowlingCardDF = bowlingCardDF[bowlingCardDF['competition_name'].str.upper() == match_type.upper()]
            else:
                battingCardDF = battingCardDF[(battingCardDF['competition_name'].str.upper() != 'ODI') & (
                        battingCardDF['competition_name'].str.upper() != 'TEST')]
                bowlingCardDF = bowlingCardDF[(bowlingCardDF['competition_name'].str.upper() != 'ODI') & (
                        bowlingCardDF['competition_name'].str.upper() != 'TEST')]

        battingCardDF['match_name'] = battingCardDF['match_name'].apply(transform_matchName)
        bowlingCardDF['match_name'] = bowlingCardDF['match_name'].apply(transform_matchName)

        response['batting_details'] = battingCardDF.sort_values('match_date_form', ascending=True).to_dict('records')
        response['bowling_Details'] = bowlingCardDF.sort_values('match_date_form', ascending=True).to_dict('records')

        return jsonify(response), logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/batsmanStatsOnAggKey", methods=['POST'])
@token_required
def batsmanStatsOnAggKey():
    logger = get_logger("batsmanStatsOnAggKey", "batsmanStatsOnAggKey")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        response = {}
        filters, params = generateWhereClause(filter_dict)

        if "from_date" and "to_date" in request.json:
            from_date = request.json['from_date']
            f_from_date = datetime.strptime(
                datetime.strptime(str(from_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                '%Y-%m-%d').date()
            to_date = request.json['to_date']
            f_to_date = datetime.strptime(datetime.strptime(str(to_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                                          '%Y-%m-%d').date()
        else:
            f_from_date = '2000-01-01'
            f_to_date = datetime.now().strftime("%Y-%m-%d")

        if "match_type" in request.json:
            match_type = request.json['match_type']
            if match_type.upper() == 'ODI' or match_type.upper() == 'TEST':
                batsman_details = batsman_overwise_df[
                    batsman_overwise_df['competition_name'].str.upper() == match_type.upper()]
            else:
                batsman_details = batsman_overwise_df[(batsman_overwise_df['competition_name'].str.upper() != 'ODI') & (
                        batsman_overwise_df['competition_name'].str.upper() != 'TEST')]
        else:
            batsman_details = batsman_overwise_df

        key_columns = ['winning_type', 'innings', 'bat_current_team', 'bowler_team_name', 'stadium_name',
                       'batting_position', 'competition_name', 'season']

        for key in key_columns:
            if filters:
                batsmanAggDetailSQL = f'''select player_id, player_name, cast(sum(runs) as int) as runs_scored, count(distinct match_id) as 
                matches, round(coalesce(((sum(runs)*100.00)/sum(balls)),0.0),2) as strike_rate, cast(sum(balls) as int) as balls_played, 
                cast(sum(case when runs between 30 and 49 then 1 else 0 end) as int) as thirty_plus, cast(sum(case when runs between 50 and 99 then 1 else 0 end) as int)as fifty_plus, 
                cast(sum(case when runs>=100 then 1 else 0 end) as int)as hundred_plus, cast(sum(case when runs=0 then 1 else 0 end) as int)as duck, 
                cast(sum(case when runs between 1 and 29 then 1 else 0 end) as int) as thirty_less,  
                cast(sum(fours) as int) as fours, cast(sum(sixes) as int) as sixes, {key} from
                (select  player_name, bat_current_team, player_id, innings, sum(runs) as runs, sum(fours) as fours, sum(sixes) as sixes, sum(balls) as balls, 
                match_id, competition_name, {key} from batsman_details ''' + filters + f''' and match_date >= '{f_from_date}' and match_date <= '{f_to_date}' 
                group by player_id, not_out, match_id, innings, player_name, bat_current_team, competition_name, player_image_url, {key}) 
                group by player_id, player_name, {key} '''

                outBatsmanSQL = f'''select out_batsman_id, round(count(out_batsman_id)) as wicket_cnt, {key}  
                                                from join_data ''' + filters.replace('player_id',
                                                                                     'out_batsman_id').replace(
                    'team_id', 'batsman_team_id').replace('player_skill', 'out_batsman_skill') + \
                                f''' and out_batsman_id<>-1 and innings not in (3,4) and match_date_form >= '{f_from_date}' and match_date_form <= '{f_to_date}'
                                              group by out_batsman_id, {key}'''

            else:
                batsmanAggDetailSQL = f'''select player_id, player_name, cast(sum(runs) as int) as runs_scored, count(distinct match_id) as 
                matches, round(coalesce(((sum(runs)*100.00)/sum(balls)),0.0),2) as strike_rate, cast(sum(balls) as int) as balls_played, 
                cast(sum(case when runs between 30 and 49 then 1 else 0 end) as int) as thirty_plus, cast(sum(case when runs between 50 and 99 then 1 else 0 end) as int)as fifty_plus, 
                cast(sum(case when runs>=100 then 1 else 0 end) as int)as hundred_plus, cast(sum(case when runs=0 then 1 else 0 end) as int)as duck, 
                cast(sum(case when runs between 1 and 29 then 1 else 0 end) as int) as thirty_less, 
                cast(sum(fours) as int) as fours, cast(sum(sixes) as int) as sixes, {key} from
                (select  player_name, bat_current_team, player_id, innings, sum(runs) as runs, sum(fours) as fours, sum(sixes) as sixes, sum(balls) as balls, 
                match_id, competition_name, {key} from batsman_details where match_date >= '{f_from_date}' and match_date <= '{f_to_date}' 
                group by player_id, not_out, match_id, innings, player_name, bat_current_team, competition_name, player_image_url, {key}) 
                group by player_id, player_name, {key} '''

                outBatsmanSQL = f'''select out_batsman_id, round(count(out_batsman_id)) as wicket_cnt, {key} from join_data where 
                out_batsman_id<>-1 and innings not in (3,4) and match_date_form >= '{f_from_date}' and match_date_form <= '{f_to_date}' 
                group by out_batsman_id, {key}'''

            finalBatsmanAggDetailSQL = '''select player_id, player_name, runs_scored, matches, strike_rate, thirty_plus, fifty_plus, thirty_less, 
                    balls_played, fours, sixes, round(coalesce((hsd.runs_scored*1.00)/obd.wicket_cnt,0.0),2) as average, hundred_plus, duck,  
                    hsd.''' + key.replace('bowler_team_name', 'bowler_team_name as team_against').replace(
                'bat_current_team', 'bat_current_team as team_name') + \
                                       f''' from batsmanAggDetailDF hsd left join OutBatsmanDf obd on (obd.out_batsman_id=hsd.player_id) and (obd.{key}=hsd.{key})'''

            batsmanAggDetailDF = executeQuery(con, batsmanAggDetailSQL, params)
            OutBatsmanDf = executeQuery(con, outBatsmanSQL, params)
            BatsmanStatsDf = executeQuery(con, finalBatsmanAggDetailSQL)

            response[key.replace('bowler_team_name', 'team_against').replace('bat_current_team',
                                                                             'team_name')] = BatsmanStatsDf.to_dict(
                orient='records')

        return jsonify(response), 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/bowlerStatsOnAggKey", methods=['POST'])
@token_required
def bowlerStatsOnAggKey():
    logger = get_logger("bowlerStatsOnAggKey", "bowlerStatsOnAggKey")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        response = {}
        filters, params = generateWhereClause(filter_dict)

        if "from_date" and "to_date" in request.json:
            from_date = request.json['from_date']
            f_from_date = datetime.strptime(
                datetime.strptime(str(from_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                '%Y-%m-%d').date()
            to_date = request.json['to_date']
            f_to_date = datetime.strptime(datetime.strptime(str(to_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                                          '%Y-%m-%d').date()
        else:
            f_from_date = '2000-01-01'
            f_to_date = datetime.now().strftime("%Y-%m-%d")

        if "match_type" in request.json:
            match_type = request.json['match_type']
            if match_type.upper() == 'ODI' or match_type.upper() == 'TEST':
                bowler_details = bowler_overwise_df[
                    bowler_overwise_df['competition_name'].str.upper() == match_type.upper()]
            else:
                bowler_details = bowler_overwise_df[(bowler_overwise_df['competition_name'].str.upper() != 'ODI') & (
                        bowler_overwise_df['competition_name'].str.upper() != 'TEST')]
        else:
            bowler_details = bowler_overwise_df

        key_columns = ['winning_type', 'innings', 'bowl_current_team', 'batsman_team_name', 'stadium_name',
                       'competition_name', 'season']

        for key in key_columns:
            if filters:
                bowlerAggDetailSQL = '''select player_name, player_id, count(distinct match_id) as matches, cast(sum(wickets) as int) as wickets, 
                cast(sum(runs) as int) as runs, cast(sum(wides) as int) as wides, cast(sum(case when wickets== 0 then 1 else 0 end) as int) as zero_wickets, 
                cast(sum(case when wickets between 1 and 2 then 1 else 0 end) as int)as less_than_3_wickets, cast(sum(case when wickets== 3 then 1 else 0 end) as int)as three_wickets, 
                cast(sum(case when wickets== 4 then 1 else 0 end) as int)as four_wickets, cast(sum(case when wickets >= 5 then 1 else 0 end) as int)as five_plus_wickets, 
                round(case when (sum(balls)%6)==0 then ((sum(balls)*1.00)/6) else (sum(balls)/6)+((sum(balls)%6)/10.00) end,2) as overs, 
                round(coalesce(((sum(balls)*1.00)/sum(wickets)),0.0),2) as strike_rate, cast(sum(dot_balls) as int) as dot_balls, 
                round(coalesce(((sum(runs)*1.00)/(sum(balls)*1.00/6)),0.0),2) as economy, cast(sum(no_balls) as int) as no_balls, ''' + \
                                     key.replace('bowl_current_team', 'bowl_current_team as team_name').replace(
                                         'batsman_team_name', 'batsman_team_name as team_against') + f''' from 
                (select match_id, player_name, player_id, sum(wides) as wides, sum(no_balls) as no_balls, 
                sum(balls) as balls, sum(runs) as runs, sum(wickets) as wickets, sum(dot_balls) as dot_balls, {key} 
                from bowler_details ''' + filters + f''' and match_date >= '{f_from_date}' and match_date <= '{f_to_date}' group by match_id, player_id, 
                player_name, {key}) group by player_id, player_name, {key}'''

            else:
                bowlerAggDetailSQL = '''select player_name, player_id, count(distinct match_id) as matches, cast(sum(wickets) as int) as wickets, 
                cast(sum(runs) as int) as runs, cast(sum(wides) as int) as wides, cast(sum(case when wickets== 0 then 1 else 0 end) as int)as zero_wickets, 
                cast(sum(case when wickets between 1 and 2 then 1 else 0 end) as int)as less_than_3_wickets, cast(sum(case when wickets== 3 then 1 else 0 end) as int)as three_wickets, 
                cast(sum(case when wickets== 4 then 1 else 0 end) as int)as four_wickets, cast(sum(case when wickets >= 5 then 1 else 0 end) as int)as five_plus_wickets, 
                round(case when (sum(balls)%6)==0 then ((sum(balls)*1.00)/6) else (sum(balls)/6)+((sum(balls)%6)/10.00) end,2) as overs, 
                round(coalesce(((sum(balls)*1.00)/sum(wickets)),0.0),2) as strike_rate, cast(sum(dot_balls) as int) as dot_balls, 
                round(coalesce(((sum(runs)*1.00)/(sum(balls)*1.00/6)),0.0),2) as economy, cast(sum(no_balls) as int) as no_balls, ''' + \
                                     key.replace('bowl_current_team', 'bowl_current_team as team_name').replace(
                                         'batsman_team_name', 'batsman_team_name as team_against') + f''' from 
                (select match_id, player_name, player_id, sum(wides) as wides, sum(no_balls) as no_balls,  
                sum(balls) as balls, sum(runs) as runs, sum(wickets) as wickets, sum(dot_balls) as dot_balls, {key} 
                from bowler_details where match_date >= '{f_from_date}' and match_date <= '{f_to_date}' group by match_id, player_id, 
                player_name, {key}) group by player_id, player_name, {key}'''

            bowlerAggDetailDF = executeQuery(con, bowlerAggDetailSQL, params)

            response[key.replace('bowl_current_team', 'team_name').replace('batsman_team_name',
                                                                           'team_against')] = bowlerAggDetailDF.to_dict(
                orient='records')

        # Replace values using the replace method
        for entry in response["winning_type"]:
            entry["winning_type"] = "Losing" if entry["winning_type"] == "Winning" else "Winning"
        return jsonify(response), 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/strikeRateAndAvgForInnings", methods=['POST'])
@token_required
def strikeRateAndAvgForInnings():
    logger = get_logger("strikeRateAndAvgForInnings", "strikeRateAndAvgForInnings")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        response = {}
        filters, params = generateWhereClause(filter_dict)

        if "from_date" and "to_date" in request.json:
            from_date = request.json['from_date']
            f_from_date = datetime.strptime(
                datetime.strptime(str(from_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                '%Y-%m-%d').date()
            to_date = request.json['to_date']
            f_to_date = datetime.strptime(datetime.strptime(str(to_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                                          '%Y-%m-%d').date()
        else:
            f_from_date = '2000-01-01'
            f_to_date = datetime.now().strftime("%Y-%m-%d")

        if "match_type" in request.json:
            match_type = request.json['match_type']
            if match_type.upper() == 'ODI' or match_type.upper() == 'TEST':
                batsman_details = batsman_overwise_df[
                    batsman_overwise_df['competition_name'].str.upper() == match_type.upper()]
            else:
                batsman_details = batsman_overwise_df[(batsman_overwise_df['competition_name'].str.upper() != 'ODI') & (
                        batsman_overwise_df['competition_name'].str.upper() != 'TEST')]
        else:
            batsman_details = batsman_overwise_df

        if filters:
            batsmanMatchAggDetailSQL = '''select  player_name, innings, player_id, sum(runs) as runs, sum(balls) as balls,
                 match_id, sum(wickets) as wickets, not_out, round(coalesce(((sum(runs)*100.00)/sum(balls)),0.0),2) as strike_rate, 
                  round(coalesce((sum(runs)*1.00)/sum(wickets),0.0),2) as average from batsman_details ''' + \
                                       filters + f''' and match_date >= '{f_from_date}' and match_date <= '{f_to_date}' group by player_id, innings, not_out, match_id, player_name'''
        else:
            batsmanMatchAggDetailSQL = f'''select  player_name, innings, player_id, sum(runs) as runs, sum(balls) as balls,
                         match_id, sum(wickets) as wickets, not_out, round(coalesce(((sum(runs)*100.00)/sum(balls)),0.0),2) as strike_rate, 
                          round(coalesce((sum(runs)*1.00)/sum(wickets),0.0),2) as average from batsman_details where 
                          match_date >= '{f_from_date}' and match_date <= '{f_to_date}' group by player_id, innings, not_out, match_id, player_name'''

        batsmanMatchAggDetailDF = executeQuery(con, batsmanMatchAggDetailSQL, params)

        batsmanMatchAggDetailDF['strike_rate_career'] = np.where(batsmanMatchAggDetailDF['balls'] == 0, 0.0,
                                                                 batsmanMatchAggDetailDF.groupby(
                                                                     ['player_name', 'player_id'])['runs'].transform(
                                                                     'sum') * 100 /
                                                                 batsmanMatchAggDetailDF.groupby(
                                                                     ['player_name', 'player_id'])['balls'].transform(
                                                                     'sum')).round(2)
        batsmanMatchAggDetailDF['average_career'] = np.where(
            (batsmanMatchAggDetailDF['wickets'] == 0) & (batsmanMatchAggDetailDF['not_out'] != 1), 0.0,
            batsmanMatchAggDetailDF.groupby(
                ['player_name', 'player_id'])['runs'].transform(
                'sum') * 1.00 /
            batsmanMatchAggDetailDF.groupby(
                ['player_name', 'player_id'])['wickets'].transform(
                'sum')).round(2)
        batsmanMatchAggDetailDF['average_career'] = batsmanMatchAggDetailDF['average_career'].replace([np.inf, -np.inf],
                                                                                                      0.0)
        batsmanMatchAggDetailDF['total_matches_played'] = batsmanMatchAggDetailDF.groupby(['player_name', 'player_id'])[
            'match_id'].transform('nunique')

        def count_matches_above_career(df):
            above_sr = (df['strike_rate'] >= df['strike_rate_career']).sum()
            above_avg = (df['average'] >= df['average_career']).sum()
            return above_sr, above_avg

        # Calculate counts separately for each inning
        result_1st_inning = batsmanMatchAggDetailDF[batsmanMatchAggDetailDF['innings'] == 1].groupby(
            ['player_name', 'player_id']).apply(count_matches_above_career).reset_index()
        result_2nd_inning = batsmanMatchAggDetailDF[batsmanMatchAggDetailDF['innings'] == 2].groupby(
            ['player_name', 'player_id']).apply(count_matches_above_career).reset_index()

        # Check if result_1st_inning is not empty
        if not result_1st_inning.empty:
            result_1st_inning[['Above_SR_1st_inning', 'Above_Avg_1st_inning']] = pd.DataFrame(
                result_1st_inning[0].to_list(), columns=['Above_SR_1st_inning', 'Above_Avg_1st_inning'])
            result_1st_inning = result_1st_inning.drop(columns=0)
        else:
            result_1st_inning = pd.DataFrame(columns=['player_id', 'player_name'])

        # Check if result_2nd_inning is not empty
        if not result_2nd_inning.empty:
            result_2nd_inning[['Above_SR_2nd_inning', 'Above_Avg_2nd_inning']] = pd.DataFrame(
                result_2nd_inning[0].to_list(), columns=['Above_SR_2nd_inning', 'Above_Avg_2nd_inning'])
            result_2nd_inning = result_2nd_inning.drop(columns=0)
        else:
            result_2nd_inning = pd.DataFrame(columns=['player_id', 'player_name'])

        # Check if either DataFrame is not empty before merging
        if not result_1st_inning.empty or not result_2nd_inning.empty:
            result_df = pd.merge(result_1st_inning, result_2nd_inning, on=['player_id', 'player_name'], how='outer')

            # Join career stats into the final DataFrame
            required_cols = ['player_id', 'player_name', 'strike_rate_career', 'average_career', 'total_matches_played']
            result_df = result_df.merge(batsmanMatchAggDetailDF[required_cols], on=['player_name', 'player_id'],
                                        how='inner').drop_duplicates()
        else:
            result_df = pd.DataFrame()

        response = result_df.to_dict(orient='records')
        return jsonify(response), 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


# @app.route("/batBowlCombDataOnKeys", methods=['POST'])
@token_required
def batBowlCombDataOnKeys():
    logger = get_logger("batBowlCombDataOnKeys", "batBowlCombDataOnKeys")
    try:
        filter_dict = globalFilters()
    except ValidationError as e:
        logger.error(e.messages)
        logger.error(e.valid_data)
        raise HTTPException(response=Response(f"Bad Request --> {e.messages}", 400))
    try:
        response = {}
        filters, params = generateWhereClause(filter_dict)

        if "from_date" and "to_date" in request.json:
            from_date = request.json['from_date']
            f_from_date = datetime.strptime(
                datetime.strptime(str(from_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                '%Y-%m-%d').date()
            to_date = request.json['to_date']
            f_to_date = datetime.strptime(datetime.strptime(str(to_date), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                                          '%Y-%m-%d').date()
        else:
            f_from_date = '2000-01-01'
            f_to_date = datetime.now().strftime("%Y-%m-%d")

        if "match_type" in request.json:
            match_type = request.json['match_type']
            if match_type.upper() == 'ODI' or match_type.upper() == 'TEST':
                batsman_details = batsman_overwise_df[
                    batsman_overwise_df['competition_name'].str.upper() == match_type.upper()]
                bowler_details = bowler_overwise_df[
                    bowler_overwise_df['competition_name'].str.upper() == match_type.upper()]
            else:
                batsman_details = batsman_overwise_df[(batsman_overwise_df['competition_name'].str.upper() != 'ODI') & (
                        batsman_overwise_df['competition_name'].str.upper() != 'TEST')]
                bowler_details = bowler_overwise_df[(bowler_overwise_df['competition_name'].str.upper() != 'ODI') & (
                        bowler_overwise_df['competition_name'].str.upper() != 'TEST')]
        else:
            batsman_details = batsman_overwise_df
            bowler_details = bowler_overwise_df

        if filters:
            batsmanMatchAggDetailSQL = '''select  player_name, innings, player_id, cast(sum(runs) as int) as runs_scored, cast(sum(balls) as int) as balls_played, stadium_name, 
                 cast(match_id as int) as match_played, cast(sum(wickets) as int) as wickets_gone, not_out, round(coalesce(((sum(runs)*100.00)/sum(balls)),0.0),2) as strike_rate, competition_name, season, winning_type,  
                  team_name, bowler_team_name as team_against, round(coalesce((sum(runs)*1.00)/sum(wickets),0.0),2) as average from batsman_details ''' + \
                                       filters + f''' and match_date >= '{f_from_date}' and match_date <= '{f_to_date}' group by player_id, innings, not_out, match_id, player_name, 
                                       competition_name, team_name, bowler_team_name, stadium_name, winning_type, season'''

            bowlerAggDetailSQL = '''select cast(match_id as int) as match_played, player_name, player_id, team_name, innings, competition_name, winning_type, 
                            batsman_team_name as team_against, cast(sum(balls) as int) as balls_bowled, cast(sum(runs) as int) as runs_conceded, cast(sum(wickets) as int) as wickets_taken, 
                            cast(sum(dot_balls) as int) as dot_balls, stadium_name, season from bowler_details ''' + filters + f''' and match_date >= '{f_from_date}' and match_date <= '{f_to_date}'
                            group by match_id, player_id, team_id, team_name, competition_name, season, player_name, innings, batsman_team_name, stadium_name, winning_type '''
        else:
            batsmanMatchAggDetailSQL = f'''select  player_name, innings, player_id, cast(sum(runs) as int) as runs_scored, cast(sum(balls) as int) as balls_played, stadium_name, 
                             cast(match_id as int) as match_played, cast(sum(wickets) as int) as wickets_gone, not_out, round(coalesce(((sum(runs)*100.00)/sum(balls)),0.0),2) as strike_rate, competition_name, season, winning_type,  
                              team_name, bowler_team_name as team_against, round(coalesce((sum(runs)*1.00)/sum(wickets),0.0),2) as average from batsman_details where
                               match_date >= '{f_from_date}' and match_date <= '{f_to_date}' group by player_id, innings, not_out, match_id, player_name, 
                                                   competition_name, team_name, bowler_team_name, stadium_name, winning_type, season'''

            bowlerAggDetailSQL = f'''select cast(match_id as int) as match_played, player_name, player_id, team_name, innings, competition_name, winning_type, 
                                        batsman_team_name as team_against, cast(sum(balls) as int) as balls_bowled, cast(sum(runs) as int) as runs_conceded, cast(sum(wickets) as int) as wickets_taken, 
                                        cast(sum(dot_balls) as int) as dot_balls, stadium_name, season from bowler_details where match_date >= '{f_from_date}' and match_date <= '{f_to_date}'
                                        group by match_id, player_id, team_id, team_name, competition_name, season, player_name, innings, batsman_team_name, stadium_name, winning_type '''

        batsmanMatchAggDetailDF = executeQuery(con, batsmanMatchAggDetailSQL, params)
        bowlerMatchAggDetailDF = executeQuery(con, bowlerAggDetailSQL, params)

        # Replace values using the replace method
        bowlerMatchAggDetailDF['winning_type'] = bowlerMatchAggDetailDF['winning_type'].replace(
            {'Winning': 'Losing', 'Losing': 'Winning'})

        merged_df = batsmanMatchAggDetailDF.merge(bowlerMatchAggDetailDF,
                                                  on=['match_played', 'player_id', 'player_name', 'winning_type',
                                                      'season',
                                                      'team_name', 'competition_name', 'team_against',
                                                      'stadium_name', 'innings'], how='outer')

        integer_columns = [
            'balls_played',
            'balls_bowled',
            'runs_scored',
            'runs_conceded',
            'wickets_taken',
            'wickets_gone'
        ]  # List of integer column names
        # merged_df[integer_columns] = pd.to_numeric(merged_df[integer_columns], downcast='integer', errors='coerce')
        merged_df[integer_columns] = merged_df[integer_columns].fillna(0).astype(int)

        key_columns = [
            'winning_type',
            'team_name',
            'team_against',
            'stadium_name',
            'competition_name',
            'season',
            'innings'
        ]
        for key in key_columns:
            # Group merged dataframe by Match_ID
            grouped_df = merged_df.groupby(['player_id', 'player_name', key]).agg({
                'match_played': 'nunique',
                'runs_scored': 'sum',
                'balls_played': 'sum',
                'wickets_taken': 'sum',
                'wickets_gone': 'sum',
                'runs_conceded': 'sum',
                'balls_bowled': 'sum'
            }).reset_index()

            grouped_df['strike_Rate'] = np.where(grouped_df['balls_played'] == 0, 0.0,
                                                 grouped_df['runs_scored'] * 100 / grouped_df['balls_played']).round(2)
            grouped_df['average'] = np.where(grouped_df['wickets_gone'] == 0, 0.0,
                                             grouped_df['runs_scored'] * 1.00 / grouped_df['wickets_gone']).round(2)
            grouped_df['economy'] = np.where(grouped_df['balls_bowled'] == 0, 0.0,
                                             grouped_df['runs_conceded'] * 1.00 / (
                                                     grouped_df['balls_bowled'] * 1.00 / 6)).round(2)

            response[key] = grouped_df.to_dict(orient='records')

        return jsonify(response), 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


@app.route("/player-id", methods=['GET'])
@token_required
def player_id():
    logger = get_logger("player-id", "player-id")
    try:
        json_data = request.json
        src_players_df = pd.DataFrame(json_data)
        src_players_df['src_player_id'] = src_players_df['src_player_id'].astype(str)
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Bad Request --> Invalid Input!!", 400))
    try:
        players_df = getPandasFactoryDF(
            session, f"select player_id, src_player_id from {DB_NAME}.Players;"
        ).drop_duplicates(['src_player_id'])

        merged_player = pd.merge(
            src_players_df,
            players_df,
            on='src_player_id',
            how='inner'
        )
        return jsonify(merged_player.to_dict(orient='records')), 200, logger.info("Status - 200")
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


@app.route("/player_mapping_ingestion", methods=['POST'])
@token_required
def player_mapping_ingestion():
    BATTING_STYLE_MAP = {
        'rhb': 'RIGHT HAND BATSMAN',
        'lhb': 'LEFT HAND BATSMAN'
    }

    BOWLING_STYLE_MAP = {
        'sla': 'LEFT ARM ORTHODOX',
        'ob': 'RIGHT ARM OFF SPINNER',
        'lmf': 'LEFT ARM FAST',
        'rm': 'RIGHT ARM FAST',
        'rmf': 'RIGHT ARM FAST',
        'lm': 'LEFT ARM FAST',
        'rfm': 'RIGHT ARM FAST',
        'lbg': 'RIGHT ARM LEGSPIN',
        'lb': 'RIGHT ARM LEGSPIN',
        'rsm': 'RIGHT ARM FAST',
        'rf': 'RIGHT ARM FAST',
        'lfm': 'LEFT ARM FAST',
        'rab': 'RIGHT ARM FAST',
        'ls': 'LEFT ARM FAST',
        'lsm': 'LEFT ARM FAST',
        'lws': 'LEFT ARM CHINAMAN',
        'rs': 'RIGHT ARM FAST'
    }
    logger = get_logger("player_mapping_ingestion", "player_mapping_ingestion")
    try:
        original_cricinfo_ids = request.json.get('cricinfo_ids', 1)
        cricinfo_ids = ', '.join(str(item) for item in original_cricinfo_ids)
        player_mapper_sql = getPandasFactoryDF(session,
                                               f"select * from {DB_NAME}.{PLAYER_MAPPER_TABLE_NAME} where cricinfo_id in ({cricinfo_ids}) allow filtering")
        already_exists = player_mapper_sql['cricinfo_id'].tolist()

        # Subtract already_exists from original_cricinfo_ids
        cricinfo_ids = list(set(original_cricinfo_ids) - set(already_exists))

        for id in cricinfo_ids:
            is_wicket_keeper = 0
            is_bowler = 0
            is_batsman = 0
            try:
                import ssl
                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                import requests
                url = f"https://hs-consumer-api.espncricinfo.com/v1/pages/player/home?playerId={str(id)}"
                payload = {}
                headers = {}
                response = requests.request("GET", url, headers=headers, data=payload)
                response.raise_for_status()
                response = json.loads(response.text)
                playing_role = response['player']['playingRoles']
                roles = []
                for role in playing_role:
                    roles.extend(role.split(' '))

                for role in roles:
                    if role == 'wicketkeeper':
                        is_wicket_keeper = 1
                    elif role == 'batter':
                        is_batsman = 1
                    elif role == 'bowler':
                        is_bowler = 1
                max_id = getMaxId(session, PLAYER_MAPPER_TABLE_NAME, PLAYER_MAPPER_KEY_COL, DB_NAME)
                player = response['player']
                data_to_insert = [{
                    'id': int(max_id),
                    'cricsheet_id': "",
                    'name': player['longName'].replace("'", "''"),
                    'short_name': player['longName'].replace("'", "''"),
                    'full_name': player['longName'].replace("'", "''"),
                    'cricinfo_id': str(id),
                    'sports_mechanics_id': "",
                    'country': "",
                    'born': "",
                    'age': "",
                    'bowler_sub_type': "" if len(player['bowlingStyles']) == 0 else BOWLING_STYLE_MAP.get(
                        player['bowlingStyles'][0].lower(), ""),
                    'striker_batting_type': "" if len(player['battingStyles']) == 0 else BATTING_STYLE_MAP.get(
                        player['battingStyles'][0].lower(), ""),
                    'load_timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'is_batsman': is_batsman,
                    'is_bowler': is_bowler,
                    'is_wicket_keeper': is_wicket_keeper
                }]
                insertToDB(
                    session,
                    data_to_insert,
                    DB_NAME,
                    PLAYER_MAPPER_TABLE_NAME
                )
            except Exception as err:
                raise err
        return jsonify(
            {"success": True, "ingestion_count": len(cricinfo_ids),
             "message": "Player Mapping Ingestion Successful"}
        ), 200, logger.info("Status - 200")
    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))


@app.route('/stopServer', methods=['GET'])
def stopServer():
    logger = get_logger("stopServer", "stopServer")
    import signal
    os.kill(os.getpid(), signal.SIGINT)
    logger.info("Server closed!")
    return jsonify({"success": True, "message": "Server is shutting down..."}), 200


generate_api_function(open_api_spec, app, '/batsmanType', 'get', batsmanType, 'batsmanType')
generate_api_function(open_api_spec, app, '/playerSkill', 'get', playerSkill, 'playerSkill')
generate_api_function(open_api_spec, app, '/bowlerType', 'get', bowlerType, 'bowlerType')
generate_api_function(open_api_spec, app, '/seasons', 'get', seasons, 'seasons')
generate_api_function(open_api_spec, app, '/getGround', 'get', getGround, 'getGround')
generate_api_function(open_api_spec, app, '/getTeams', 'get', getTeams, 'getTeams')
generate_api_function(open_api_spec, app, '/getBIFilters', 'post', getBIFilters, 'getBIFilters')
generate_api_function(open_api_spec, app, '/competitionName', 'get', competitionName, 'competitionName')
generate_api_function(open_api_spec, app, '/getPlayersForTeam', 'get', getPlayersForTeam, 'getPlayersForTeam')
generate_api_function(open_api_spec, app, '/latestPerformances', 'post', latestPerformances, 'latestPerformances')
generate_api_function(open_api_spec, app, '/playerProfile', 'post', playerProfile, 'playerProfile')
generate_api_function(open_api_spec, app, '/playerSeasonStats', 'post', playerSeasonStats, 'playerSeasonStats')
generate_api_function(open_api_spec, app, '/bestPartnerships', 'post', bestPartnerships, 'bestPartnerships')
generate_api_function(open_api_spec, app, '/overwiseBowlingStats', 'post', overwiseBowlingStats, 'overwiseBowlingStats')
generate_api_function(open_api_spec, app, '/overwiseStrikeRate', 'post', overwiseStrikeRate, 'overwiseStrikeRate')
generate_api_function(open_api_spec, app, '/battingVSbowlerType', 'post', battingVSbowlerType, 'battingVSbowlerType')
generate_api_function(open_api_spec, app, '/highestIndividualScores', 'post', highestIndividualScores,
                      'highestIndividualScores')
generate_api_function(open_api_spec, app, '/seasonWiseBattingStats', 'post', seasonWiseBattingStats,
                      'seasonWiseBattingStats')
generate_api_function(open_api_spec, app, '/overSlabWiseRunRate', 'post', overSlabWiseRunRate, 'overSlabWiseRunRate')
generate_api_function(open_api_spec, app, '/averageStatsByGround', 'post', averageStatsByGround, 'averageStatsByGround')
generate_api_function(open_api_spec, app, '/teamStrikeRate', 'post', teamStrikeRate, 'teamStrikeRate')
generate_api_function(open_api_spec, app, '/mostWickets', 'post', mostWickets, 'mostWickets')
generate_api_function(open_api_spec, app, '/mostEconomicalBowler', 'post', mostEconomicalBowler, 'mostEconomicalBowler')
generate_api_function(open_api_spec, app, '/playerWith750Runs', 'post', playerWith750Runs, 'playerWith750Runs')
generate_api_function(open_api_spec, app, '/powerPlayBowler', 'post', powerPlayBowler, 'powerPlayBowler')
generate_api_function(open_api_spec, app, '/performanceVSdiffBowlers', 'post', performanceVSdiffBowlers,
                      'performanceVSdiffBowlers')
generate_api_function(open_api_spec, app, '/matchStats', 'get', matchStats, 'matchStats')
generate_api_function(open_api_spec, app, '/getPastMatches', 'post', getPastMatches, 'getPastMatches')
generate_api_function(open_api_spec, app, '/dismissalType', 'post', dismissalType, 'dismissalType')
generate_api_function(open_api_spec, app, '/powerPlayAttackingShot', 'post', powerPlayAttackingShot,
                      'powerPlayAttackingShot')
generate_api_function(open_api_spec, app, '/getPlayerMatchupVSPlayer', 'post', getPlayerMatchupVSPlayer,
                      'getPlayerMatchupVSPlayer')
generate_api_function(open_api_spec, app, '/getPlayerMatchupVSTeam', 'post', getPlayerMatchupVSTeam,
                      'getPlayerMatchupVSTeam')
generate_api_function(open_api_spec, app, '/highestIndividualBowlingStats', 'post', highestIndividualBowlingStats,
                      'highestIndividualBowlingStats')
generate_api_function(open_api_spec, app, '/getBatsmanOverWiseStats', 'post', getBatsmanOverWiseStats,
                      'getBatsmanOverWiseStats')
generate_api_function(open_api_spec, app, '/battingTeamStats', 'post', battingTeamStats, 'battingTeamStats')
generate_api_function(open_api_spec, app, '/bowlingTeamStats', 'post', bowlingTeamStats, 'bowlingTeamStats')
generate_api_function(open_api_spec, app, '/head2HeadStats', 'post', head2HeadStats, 'head2HeadStats')
generate_api_function(open_api_spec, app, '/overwiseBowlingOrder', 'post', overwiseBowlingOrder, 'overwiseBowlingOrder')
generate_api_function(open_api_spec, app, '/positionWiseAvgRuns', 'post', positionWiseAvgRuns, 'positionWiseAvgRuns')
generate_api_function(open_api_spec, app, '/positionWiseBowler', 'post', positionWiseBowler, 'positionWiseBowler')
generate_api_function(open_api_spec, app, '/overWiseStats', 'post', overWiseStats, 'overWiseStats')
generate_api_function(open_api_spec, app, '/matchPlayingXI', 'post', matchPlayingXI, 'matchPlayingXI')
generate_api_function(open_api_spec, app, '/positionWiseTeamsPerOver', 'post', positionWiseTeamsPerOver,
                      'positionWiseTeamsPerOver')
generate_api_function(open_api_spec, app, '/putSearchQueryFeedback', 'post', putSearchQueryFeedback,
                      'putSearchQueryFeedback')
generate_api_function(open_api_spec, app, '/playerTrackingDashboard', 'post', playerTrackingDashboard,
                      'playerTrackingDashboard')
generate_api_function(open_api_spec, app, '/playerMatchDetails', 'post', playerMatchDetails, 'playerMatchDetails')
generate_api_function(open_api_spec, app, '/batsmanStatsOnAggKey', 'post', batsmanStatsOnAggKey, 'batsmanStatsOnAggKey')
generate_api_function(open_api_spec, app, '/bowlerStatsOnAggKey', 'post', bowlerStatsOnAggKey, 'bowlerStatsOnAggKey')
generate_api_function(open_api_spec, app, '/strikeRateAndAvgForInnings', 'post', strikeRateAndAvgForInnings,
                      'strikeRateAndAvgForInnings')
generate_api_function(open_api_spec, app, '/batBowlCombDataOnKeys', 'post', batBowlCombDataOnKeys,
                      'batBowlCombDataOnKeys')

if __name__ == "__main__":
    # for prod purpose
    from waitress import serve

    logger = get_logger("root", "app")
    serve(app, host=HOST, port=PORT, channel_timeout=20, threads=100)

    # for DEV purpose comment above 2 lines and uncomment below line
    # app.run(debug=True, host=HOST, port=PORT, threaded=True)
