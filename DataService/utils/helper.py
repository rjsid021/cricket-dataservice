from collections import Counter

import duckdb
import numpy as np
import pandas as pd
import pandasql as psql
import yaml
from azure.storage.blob import BlockBlobService
from flask import request
from pulp import *

from DataService.app_config import OPEN_API_SPEC_FILE

sys.path.append('../')
sys.path.append("./../../")
sys.path.append("./")
from log.log import get_logger
from common.utils.helper import getTeamsMapping, getEnvVariables
from DataService.serializer.validator import GlobalFilters
from DataIngestion.config import PLAYER_LIST_2023_FILE, DUCK_DB_PATH, IMAGE_STORE_URL
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session
from common.db_config import DB_NAME
from datetime import datetime, timedelta
import requests

logger = get_logger("root", "app")


# Fetch Global Filters
def globalFilters():
    filter_dict = {}
    req = request.json
    if req:
        validateRequest(req)
    logger.info("Request --> {}".format(req))
    if req:
        venue = req.get('venue')
        player_id = req.get('player_id')
        team_id = req.get('team_id')
        bowling_type = req.get('bowling_type')
        batting_type = req.get('batting_type')
        innings = req.get('innings')
        winning_type = req.get('winning_type')
        season = req.get('season')
        competition_name = req.get('competition_name')
        player_skill = req.get('player_skill')

        if venue:
            filter_dict['venue'] = venue
        if player_id:
            filter_dict['player_id'] = player_id
        if team_id:
            filter_dict['team_id'] = team_id
        if bowling_type:
            filter_dict['bowling_type'] = bowling_type
        if batting_type:
            filter_dict['batting_type'] = batting_type
        if innings:
            filter_dict['innings'] = innings
        if winning_type:
            filter_dict['winning_type'] = winning_type
        if season:
            filter_dict['season'] = season
        if competition_name:
            filter_dict['competition_name'] = competition_name
        if player_skill:
            filter_dict['player_skill'] = player_skill

    else:
        filter_dict = None
        # print(filter_dict)
    return filter_dict


# fetch filter for AI APIs
def filtersAI():
    filter_dict = {}
    req = request.json
    logger.info("Request --> {}".format(req))
    validateRequest(req)

    if req:
        player_id = req.get('player_id')
        bowling_type = req.get('bowling_type')
        batting_type = req.get('batting_type')
        innings = req.get('innings')
        year = req.get('year')
        phase = req.get('phase')
        bowler_id = req.get('bowler_id')
        batsman_id = req.get('batsman_id')
        bowler = req.get('bowler')
        batsman = req.get('batsman')
        striker = req.get('striker')
        non_striker = req.get('non_striker')
        striker_name = req.get('striker_name')
        non_striker_name = req.get('non_striker_name')
        player_name = req.get('player_name')
        pitch_type = req.get('pitch_type')
        over_no = req.get('over_no')
        competition_name = req.get('competition_name')

        if player_id:
            filter_dict['player_id'] = player_id
        if player_name:
            filter_dict['player_name'] = player_name
        if bowling_type:
            filter_dict['bowling_type'] = bowling_type
        if batting_type:
            filter_dict['batting_type'] = batting_type
        if innings:
            filter_dict['innings'] = innings
        if year:
            filter_dict['year'] = year
        if phase:
            filter_dict['phase'] = phase
        if bowler_id:
            filter_dict['bowler_id'] = bowler_id
        if batsman_id:
            filter_dict['batsman_id'] = batsman_id
        if bowler:
            filter_dict['bowler'] = bowler
        if batsman:
            filter_dict['batsman'] = batsman
        if striker:
            filter_dict['striker'] = striker
        if non_striker:
            filter_dict['non_striker'] = non_striker
        if striker_name:
            filter_dict['striker_name'] = striker_name
        if non_striker_name:
            filter_dict['non_striker_name'] = non_striker_name
        if pitch_type:
            filter_dict['pitch_type'] = pitch_type
        if over_no:
            filter_dict['overs'] = over_no
        if competition_name:
            filter_dict['competition_name'] = competition_name
    else:
        filter_dict = None

    return filter_dict


# function to generate contribution score filters
def filters_cs():
    filter_dict = {}
    req = request.json
    validateRequest(req)
    logger.info("Request --> {}".format(req))
    if req:

        season = req.get('season')
        position = req.get('position')
        bowling_type = req.get('bowling_type')
        player_type = req.get('player_type')
        retained = req.get('retained')
        speciality = req.get('speciality')
        competition_name = req.get('competition_name')
        batting_type = req.get('batting_type')
        in_auction = req.get('in_auction')
        player_id = req.get('player_id')
        team_id = req.get('team_id')
        is_won = req.get('is_won')
        team = req.get('team')

        if season:
            filter_dict['season'] = [int(x) for x in season]
        if position:
            filter_dict['position'] = position
        if bowling_type:
            filter_dict['bowling_type'] = bowling_type
        if player_type:
            filter_dict['player_type'] = player_type
        if retained:
            filter_dict['retained'] = [0 if x == "Not Retained" else 1 for x in retained]
        if speciality:
            filter_dict['speciality'] = speciality
        if competition_name:
            filter_dict['competition_name'] = competition_name
        if batting_type:
            filter_dict['batting_type'] = batting_type
        if in_auction:
            filter_dict['in_auction'] = in_auction
        if player_id:
            filter_dict['player_id'] = player_id
        if team_id:
            filter_dict['team_id'] = team_id
        if is_won is not None:
            filter_dict['is_won'] = is_won
        if team:
            filter_dict['team'] = team
    else:
        filter_dict = None

    return filter_dict


# drop filter from the dict if not required
def dropFilter(list_keys, data_dict):
    if data_dict:
        for key in list_keys:
            if key in data_dict:
                data_dict.pop(key)
        return data_dict
    else:
        return None


# generate where clause from the provided dict
def generateWhereClause(filter_dict):
    param_list = []
    where_clause = ""
    if filter_dict:
        if len(filter_dict) == 0:
            pass
        elif len(filter_dict) > 0:
            for key, value in filter_dict.items():
                if isinstance(value, str):
                    where_clause = where_clause + " and {}=?".format(key) if where_clause != "" else "{}=?".format(key)
                    param_list.append(value)
                elif isinstance(value, list) and len(value) > 1:
                    where_clause = where_clause + ' and {} in ('.format(key) + ','.join(
                        '?' * len(value)) + ')' if where_clause != "" \
                        else '{} in ('.format(key) + ','.join('?' * len(value)) + ')'
                    param_list.extend(value)
                elif isinstance(value, list) and len(value) == 1 and (
                        isinstance(value[0], str) or isinstance(value[0], int)):
                    where_clause = where_clause + " and {}=?".format(key) if where_clause != "" else "{}=?".format(key)
                    param_list.append(value[0])
                else:
                    where_clause = where_clause + " and {}=?".format(key) if where_clause != "" else "{}=?".format(key)
                    param_list.append(value)

            return " where " + where_clause, param_list
    else:
        return "", []


def validateRequest(req):
    global_filters = GlobalFilters()
    global_filters.load(req)
    return


def pandasSQLDF(query):
    return psql.sqldf(query)


# creating duckdb connection
def connection_duckdb(path=DUCK_DB_PATH, read_only=False):
    return duckdb.connect(path, read_only=read_only)


# executing query using duckdb
def executeQuery(connection, sql_stmt, param_list=[]):
    connection.execute('PRAGMA threads=36')
    cursor = connection.cursor()
    return cursor.execute(sql_stmt, parameters=param_list).fetchdf()


def queryDF(df, vw_name, sql_query):
    return duckdb.query_df(df, vw_name, sql_query).df()


# register df to duckdb as view
def registerDF(connection, df, vw_name, table_name, register_once=False):
    result = connection.execute('SHOW TABLES')
    tables_list = [row[0] for row in result.fetchall()]
    if table_name in tables_list:
        logger.info(f"Updating {table_name} with new data")
        if register_once:
            logger.info(f"{table_name} doesn't need to be registered again")
            return
        duckdb.sql(query=f"INSERT INTO {table_name} SELECT * FROM df", connection=connection)
    else:
        logger.info(f"Registering {table_name} to duckdb")
        duckdb.sql(query=f"CREATE TABLE {table_name} AS SELECT * FROM df", connection=connection)


# get overwise bowler based on the selection by the user
# df- input df containing the historical data i.e., total_matches, economy per over of the bowler
# prev_bowl - previous over bowled by bowler
# bowl_dict - dictionary of the total quota of overs for each bowlers
def get_bowler(df, prev_bowl, bowl_dict):
    for j in range(len(df)):
        if prev_bowl != df.iloc[j]['player_id']:
            if bowl_dict[df.iloc[j]['player_id']] >= 1:
                if df.iloc[j]['total_matches'] >= 10:
                    bowl_dict[df.iloc[j]['player_id']] = bowl_dict[df.iloc[j]['player_id']] - 1
                    prev_bowl = df.iloc[j]['player_id']
                    return df.iloc[j]['player_id']
                else:
                    if df.iloc[j]['over_number'] >= 8:
                        bowl_dict[df.iloc[j]['player_id']] = bowl_dict[df.iloc[j]['player_id']] - 1
                        prev_bowl = df.iloc[j]['player_id']
                        return df.iloc[j]['player_id']
                    else:
                        pass
            else:
                pass
        else:
            pass


def addColumnsToSQL(sql_query, column_to_replace, required_column):
    return sql_query.replace(column_to_replace, required_column)


def optimizer(runs_matrix, bowlers_list):
    n_overs = len(runs_matrix)
    n_bowlers = len(bowlers_list)
    model = LpProblem("OverBowlerMapping", LpMinimize)
    variable_names = [str(j) + str(i) for j in range(1, n_overs + 1) for i in range(1, n_bowlers + 1)]
    DV_variables = LpVariable.matrix("X", variable_names, cat="Integer", lowBound=0, upBound=1)
    allocation = np.array(DV_variables).reshape(n_overs, n_bowlers)

    obj_func = lpSum(allocation * runs_matrix)

    model += obj_func

    # Constraints
    model += lpSum(allocation[i][j] for i in range(n_overs) for j in range(n_bowlers)) == n_overs

    # 1 over can be bowled by only 1 bowler
    for i in range(n_overs):
        model += lpSum(allocation[i][j] for j in range(n_bowlers)) == 1

    # No bowler can bowl 2 consecutive overs
    for j in range(n_bowlers):
        for i in range(n_overs - 1):
            model += lpSum(allocation[i + k][j] for k in range(2)) <= 1

    # 1 bowler can bowl maximum of 4 overs
    for j in range(n_bowlers):
        model += lpSum(allocation[i][j] for i in range(n_overs)) <= 4

    model.solve(PULP_CBC_CMD(msg=0))
    runs = 0
    for i in range(n_overs):
        runs += allocation[i][0].value() * runs_matrix[i][0]

    lineup = []
    for i in range(allocation.shape[0]):
        for j in range(allocation.shape[1]):
            if allocation[i][j].value() == 1:
                lineup.append(bowlers_list[j])

    return lineup, round(model.objective.value())


def check_replacements(over, mapping, over_dict, over_agg_df):
    replacements = []
    for item in over_dict.items():
        if (item[1] < 4.0):
            replacements.append(item[0])
    if (len(replacements) != 0):
        select_df = over_agg_df[over_agg_df['player_id'].isin(replacements)][
            ['player_id', 'total_matches', 'bowling_economy']].copy()
        select_df = select_df.sort_values(by=['bowling_economy', 'total_matches'], ascending=[True, False])
        for j in range(select_df.shape[0]):
            if (over == 0):
                if (select_df.iloc[j]['player_id'] == mapping[over + 1]):
                    continue
                else:
                    over_dict[select_df.iloc[j]['player_id']] += 1
                    over_dict[mapping[over]] -= 1
                    mapping[over] = select_df.iloc[j]['player_id']
                    break

            elif over == 19:
                if select_df.iloc[j]['player_id'] == mapping[over - 1]:
                    continue
                else:
                    over_dict[select_df.iloc[j]['player_id']] += 1
                    over_dict[mapping[over]] -= 1
                    mapping[over] = select_df.iloc[j]['player_id']
                    break

            else:
                if ((select_df.iloc[j]['player_id'] == mapping[over - 1]) | (
                        select_df.iloc[j]['player_id'] == mapping[over + 1])):
                    continue
                else:
                    over_dict[select_df.iloc[j]['player_id']] += 1
                    over_dict[mapping[over]] -= 1
                    mapping[over] = select_df.iloc[j]['player_id']
                    break


def get_optimal_squad(over_df, over_agg_df, bowling_list, penalty):
    bowlers_data = over_df[over_df['player_id'].isin(bowling_list)].copy()
    bowlers_data['bowling_economy'] = bowlers_data.apply(
        lambda x: x['bowling_economy'] if x['total_matches'] >= 5 else np.nan, axis=1)
    player_list = bowlers_data['player_id'].unique()
    missing_list = list(set(bowling_list) - set(player_list))
    bowling_squad = bowlers_data.pivot(index='over_number', columns='player_id', values='bowling_economy').reset_index()

    if bowling_squad.shape[0] != 20:
        overs = [i for i in range(1, 20)]
        overs_left = set(overs) - set(bowling_squad['over_number'].unique())
        for over in overs_left:
            li = [over] + [np.nan] * (bowling_squad.shape[1] - 1)
            bowling_squad.loc[len(bowling_squad.index)] = li
        bowling_squad = bowling_squad.sort_values(by=['over_number'], ascending=True)

    if missing_list:
        for i in missing_list:
            bowling_squad[i] = np.nan

    runs_matrix = bowling_squad[bowling_list].fillna(value=penalty).values
    # Optimal bowler to over mapping
    mapping, economy_score = optimizer(runs_matrix, bowling_list)

    over_dict = Counter(mapping)
    output = []

    # Mention rows that need to be coloured differently
    for ind in range(len(mapping)):
        if runs_matrix[ind][bowling_list.index(mapping[ind])] == penalty:
            check_replacements(ind, mapping, over_dict, over_agg_df)
            output.append((ind + 1, mapping[ind], True))
        else:
            output.append((ind + 1, mapping[ind], False))

    # Update economy score
    for val in output:
        if (val[2]):
            over_agg_data = over_agg_df[over_agg_df['player_id'] == val[1]]
            if not len(over_agg_data) == 0:
                cum_sum = over_agg_df['bowling_economy'].values[0]
            else:
                cum_sum = 9
            economy_score = economy_score - penalty + round(cum_sum)

    return economy_score, output


# used in GPS data services. Filters df based in range of lower and upper values of a particular column
def filterDF(df, col_name, lower, upper):
    return df[(df[col_name] >= lower) & (df[col_name] <= upper)]


def getNormalizedDf(df, normalization_keys, normalization_group_list=None):
    for key in normalization_keys:
        if key in df.columns:
            df[key + "_min"] = df.groupby(normalization_group_list)[key].transform('min')
            df[key + "_max"] = df.groupby(normalization_group_list)[key].transform('max')

    for key in normalization_keys:
        if key in df.columns:
            try:
                df[key] = (df[key] - df[key + "_min"]) / (df[key + "_max"] - df[key + "_min"])
            except Exception as e:
                logger.info(f"Failed normalizing for key having max val {df[key].max}")
                continue
    return df.groupby(normalization_group_list).mean().reset_index()[
        normalization_keys + normalization_group_list].fillna(0)


def getMean(df, group_list):
    return df.groupby(group_list).mean().round(decimals=2)


def getStandardDeviation(df, group_list):
    return df.groupby(group_list).std().round(decimals=2)


def getListIndexDF(li, col_list):
    df = pd.DataFrame([(i, j) for i, j in enumerate(li)], columns=col_list)
    return df


# join generation for H2H
# takes input as a string of cols generated in H2h
def getJoinCondition(key_cols):
    join_list = key_cols.split(',')
    for i in range(len(join_list)):
        if len(join_list) == 1:
            join_clause = f'odf.{join_list[i].strip()}=pdf.{join_list[i].strip()}'
        else:
            join_clause = f'(odf.{join_list[i - 1].strip()}=pdf.{join_list[i - 1].strip()} and odf.{join_list[i].strip()}=pdf.{join_list[i].strip()})'

    return join_clause


# Generates list of Players who have not filled Fitness Form
# Takes Player Names who have Filled the Form as Input
def getFormNotFilledPlayers(fitness_form_df, season, team_name):
    team_dict = {v: k for k, v in getTeamsMapping().items()}
    try:
        team_mi = getPandasFactoryDF(session,
                                     f'''select team_id from {DB_NAME}.teams where team_short_name='{team_dict[team_name]}' ALLOW FILTERING; ''').iloc[
            0, 0]
    except:
        team_mi = 0
    df_all_season = getPandasFactoryDF(session, f'''select player_name, season from 
 {DB_NAME}.players where team_id={team_mi} ALLOW FILTERING; ''')
    final_df = df_all_season[df_all_season['season'] == season]
    form_filled_players = list(fitness_form_df['player_name'].unique())
    fitness_form_not_filled = final_df[~final_df['player_name'].isin(form_filled_players)]
    return fitness_form_not_filled


def get_form_not_filled_players_list(fitness_form_df, active_players, team_name):
    team_dict = {v: k for k, v in getTeamsMapping().items()}
    df_2023 = pd.read_csv(PLAYER_LIST_2023_FILE)
    df_2023_MI = df_2023[df_2023['Team'] == team_dict.get(team_name)]
    df_2023_players = list(df_2023_MI['Player'].unique())
    df_2023_players = [_.lower() for _ in df_2023_players]
    fitness_form_filled_players = [_.lower() for _ in fitness_form_df["player_name"].to_list()]
    fitness_form_not_filled_players = [i for i in df_2023_players if i not in fitness_form_filled_players]
    scheduled_players = [player for player in fitness_form_not_filled_players if player in active_players]
    return scheduled_players


def getUpdateSetValues(update_dict):
    set_values = []
    for key, value in update_dict.items():
        if isinstance(value, str):
            set_values.append(f"{key}='{value}'")
        elif isinstance(value, int):
            set_values.append(f"{key}={value}")
        elif isinstance(value, list):
            set_values.append(f"{key}={value}")
    return set_values


def updatePlannedBallData(json_data, BOWL_PLANNING_TABLE_NAME):
    form_df = pd.DataFrame([json_data])
    form_df['record_date'] = form_df['record_date'].apply(
        lambda x: datetime.strptime(str(x).split(" ")[0], '%Y-%m-%d').strftime('%A, %b %d, %Y'))
    id = int(json_data.get('id'))

    planned_ball_update_dict = dropFilter(['record_date', 'player_id', 'player_name', 'team_name', 'id'], json_data)
    planned_ball_set_values = getUpdateSetValues(planned_ball_update_dict)

    planned_ball_update_sql = f"update {DB_NAME}.{BOWL_PLANNING_TABLE_NAME} set {', '.join(planned_ball_set_values)} where id={id}"
    session.execute(planned_ball_update_sql)


def updateFieldAnalysisData(json_data, FIELDING_ANALYSIS_TABLE_NAME):
    id = int(json_data.get('id'))

    fielding_analysis_update_dict = dropFilter(['match_name', 'player_name', 'team_name', 'category', 'id'], json_data)
    fielding_analysis_set_values = getUpdateSetValues(fielding_analysis_update_dict)

    fielding_analysis_update_sql = f"update {DB_NAME}.{FIELDING_ANALYSIS_TABLE_NAME} set {', '.join(fielding_analysis_set_values)} where id={id}"
    session.execute(fielding_analysis_update_sql)


# Function to convert match_name into 'team1 vs team2 DD MON YYYY'
def transform_matchName(s):
    # Use regular expressions to extract team names and date
    match = re.match(r'([A-Za-z]+)(?i)VS([A-Za-z]+)(\d+)', s)

    if match:
        team1 = match.group(1)
        team2 = match.group(2)
        date_str = match.group(3)

        # Determine the date format based on the length of date_str
        if len(date_str) == 6:
            date_format = '%d%m%y'
        elif len(date_str) == 8:
            date_format = '%d%m%Y'
        else:
            return s  # Invalid date format

        try:
            date_obj = datetime.strptime(date_str, date_format)
            formatted_date = date_obj.strftime('%d-%B-%Y')
            return f'{team1} VS {team2} {formatted_date}'
        except ValueError:
            return s
    else:
        return s


# function to generate dates from from_date to to_date. it takes input in %d/%m/%Y format and returns a tuple of the dates
def generate_dates(from_date, to_date):
    # Convert string dates to datetime objects
    from_date = datetime.strptime(from_date, '%d/%m/%Y')
    to_date = datetime.strptime(to_date, '%d/%m/%Y')

    # Generate dates between from_date and to_date
    current_date = from_date
    date_tuple = ()

    while current_date <= to_date:
        date_tuple += (current_date.strftime('%d/%m/%Y'),)
        current_date += timedelta(days=1)

    return date_tuple


# function to check whether the link exists or not. created for the player images url
def url_exists(url):
    try:
        response = requests.head(url)
        return response.status_code == 200
    except requests.ConnectionError:
        return False


# function to generate router for the APIs

# Load OpenAPI spec from YAML file
def open_api_spec():
    with open(OPEN_API_SPEC_FILE, 'r') as file:
        openapi_spec = yaml.safe_load(file)
    return openapi_spec


def generate_api_function(openapi_spec, app, route, method, function, endpoint_name):
    path_info = openapi_spec.get('paths', {}).get(route, {}).get(method, {})

    if not path_info:
        print(f"Missing OpenAPI spec information for route: {route}, method: {method}")
        return
    from flasgger.utils import swag_from
    # Decorate the function with Swagger documentation
    @swag_from(path_info)
    def wrapper(*args, **kwargs):
        return function(*args, **kwargs)

    # Add the route to the app with the unique endpoint
    app.add_url_rule(route, view_func=wrapper, methods=[method.upper()], endpoint=endpoint_name)


def updateCalendarEventData(json_data, CALENDAR_EVENT_TABLE_NAME):
    id = int(json_data.get('id'))
    calendar_event_update_dict = dropFilter(['id'], json_data)
    calendar_event_set_values = getUpdateSetValues(calendar_event_update_dict)
    calendar_events_update_sql = f"update {DB_NAME}.{CALENDAR_EVENT_TABLE_NAME} set {', '.join(calendar_event_set_values)} where id={id}"
    session.execute(calendar_events_update_sql)


def process_response(team_response):
    # Initialize lists for different roles
    response = {
        "players": [],
        "admins": [],
        "coaches": [],
        "analysts": [],
        "head_coaches": [],
        "support": []
    }
    default_img_url = f'{IMAGE_STORE_URL}players_images/men-default-placeholder.png'

    # Iterate through memberUsers in the data
    for user in team_response['memberUsers']:
        # Check if the user is active
        if user.get('active'):

            document = user['documents']
            profile_image = ""
            if document.get('image'):
                profile_image = document.get('image')[-1].get('docAccessUrl', default_img_url)

            recipient_meta_data = {
                "name": user['metadata'].get('db_name', user['metadata'].get('name')),
                "uuid": user['uuid'],
                "active": user['active'],
                "player_image_url": user['metadata'].get('player_details', {}).get('player_image_url', default_img_url),
                "profile_image": profile_image if profile_image else default_img_url,
                "src_player_id": user['metadata'].get('player_details', {}).get('src_player_id', '')
            }
            if 'coach' in user['roles']:
                response["coaches"].append(recipient_meta_data)
            if 'analyst' in user['roles']:
                response["analysts"].append(recipient_meta_data)
            if 'player' in user['roles']:
                response["players"].append(recipient_meta_data)
            if 'support' in user['roles']:
                response["support"].append(recipient_meta_data)
            if 'head_coach' in user['roles']:
                response["head_coaches"].append(recipient_meta_data)

    # Iterate through memberUsers in the data
    for user in team_response['adminUsers']:
        # Check if the user is active
        if user.get('active'):
            document = user['documents']
            profile_image = ""
            if document.get('image'):
                profile_image = document.get('image')[-1].get('docAccessUrl', default_img_url)

            recipient_meta_data = {
                "name": user['metadata'].get('db_name', user['metadata'].get('name')),
                "uuid": user['uuid'],
                "active": user['active'],
                "player_image_url": user['metadata'].get('player_details', {}).get('player_image_url', default_img_url),
                "profile_image": profile_image if profile_image else default_img_url,
                "src_player_id": user['metadata'].get('player_details', {}).get('src_player_id', '')
            }
            if 'admin' in user['roles']:
                response["admins"].append(recipient_meta_data)

    return response


# Function to check image existence in bulk
def check_player_images(image_names, folder_name):
    container_name = getEnvVariables('CONTAINER_NAME')
    block_blob_service = BlockBlobService(
        account_name=getEnvVariables('STORAGE_ACCOUNT_NAME'),
        account_key=getEnvVariables('STORAGE_ACCOUNT_KEY')
    )
    # Get list of existing blobs in the container
    existing_blobs = block_blob_service.list_blobs(container_name, folder_name)
    existing_blob_names = [blob.name for blob in existing_blobs]
    # Check if image name exists in the list of existing blob names
    return [image_name in existing_blob_names for image_name in image_names]


def defaulting_image_url(players_data, image_url, filter_column, filter_value, folder_name):
    missing_url_wpl = (players_data[image_url].isna() | (players_data[image_url] == '') & (
            players_data[filter_column] == filter_value))
    players_data.loc[missing_url_wpl, image_url] = f'{IMAGE_STORE_URL}players_images/women-default-placeholder.png'

    # Filter rows with missing or null player_image_url and non-'WPL' competition_name
    missing_url_non_wpl = (players_data[image_url].isna() | (players_data[image_url] == '') & (
            players_data[filter_column] != filter_value))
    players_data.loc[missing_url_non_wpl, image_url] = f'{IMAGE_STORE_URL}players_images/men-default-placeholder.png'

    # Extract unique image names from the DataFrame if 'players_images/' is present in URL
    players_data['image_name'] = np.where(
        players_data[image_url].str.contains(IMAGE_STORE_URL),
        players_data[image_url].str.split(IMAGE_STORE_URL).str[1],
        players_data[image_url]
    )

    # Check existence of images in bulk
    unique_image_names = players_data['image_name'].unique()
    image_existence = check_player_images(unique_image_names, folder_name)
    # Create a dictionary to map image names to existence status
    image_existence_dict = dict(zip(unique_image_names, image_existence))

    # Update players_data DataFrame based on image existence
    for index, row in players_data.iterrows():
        if row['image_name'] in image_existence_dict and not image_existence_dict[row['image_name']]:
            # Determine the type of default image based on the competition_name
            default_image_url = f'{IMAGE_STORE_URL}players_images/women-default-placeholder.png' if row[
                                                                                                        filter_column] == filter_value else f'{IMAGE_STORE_URL}players_images/men-default-placeholder.png'
            players_data.at[index, image_url] = default_image_url
