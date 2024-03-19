import ast
import json
import os
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
from DataIngestion.config import QUERY_FEEDBACK_TABLE_NAME, PLAYER_MAPPER_TABLE_NAME, PLAYER_MAPPER_KEY_COL
from DataService.utils.helper import globalFilters, generateWhereClause, dropFilter, executeQuery, validateRequest, \
    getListIndexDF, getJoinCondition, transform_matchName
from common.authentication.auth import token_required
from marshmallow import ValidationError
from DataService.app_config import HOST, PORT
from log.log import get_logger
import yaml
from flask import Flask, request, jsonify, Response
from flask_cors import CORS
from werkzeug.exceptions import HTTPException
from flask_openapi3 import OpenAPI, Info
from marshmallow import ValidationError
from flask_restful import Api, Resource
from flasgger import Swagger
from flasgger.utils import swag_from
import yaml

app = Flask(__name__)
CORS(app, support_credentials=True)
warnings.simplefilter(action='ignore', category=FutureWarning)

# Load OpenAPI spec from YAML file
with open('../../openapi_spec/app_open_api_spec.yaml', 'r') as file:
    openapi_spec = yaml.safe_load(file)


api_info = Info(
    title="Cricket Data Service API Doc",
    version="1.0",
)
# Initialize Flask-OpenAPI3
openapi =  OpenAPI(
    app,
    info=api_info
)

api = Api(app)

swagger = Swagger(app)

def generate_api_function(route, method, function, endpoint_name):
    path_info = openapi_spec.get('paths', {}).get(route, {}).get(method, {})

    if not path_info:
        print(f"Missing OpenAPI spec information for route: {route}, method: {method}")
        return

    # Decorate the function with Swagger documentation
    @swag_from(path_info)
    def wrapper(*args, **kwargs):
        return function(*args, **kwargs)

    # Add the route to the app with the unique endpoint
    app.add_url_rule(route, view_func=wrapper, methods=[method.upper()], endpoint=endpoint_name)

# Your existing code for batsman_type
def batsmanType():
    logger = get_logger("batsmanType", "batsmanType")
    try:
        batsman = executeQuery(con, '''select distinct batting_type as batsman from players_data where batting_type<>'NA'
         order by batting_type''')
        response = batsman['batsman'].to_json(orient='records')
        return response, 200, logger.info("Status - 200")

    except Exception as e:
        logger.error(e)
        raise HTTPException(response=Response(f"Internal Server error --> {e}", 500))

# Your existing code for latest_performances
#@token_required
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


# Adding resources to the API with custom endpoint names
generate_api_function('/batsmanType', 'get', batsmanType, 'batsmanType')
generate_api_function('/latestPerformances', 'post', latestPerformances, 'latestPerformances')

if __name__ == "__main__":
    app.run(port=8080, debug=True)
