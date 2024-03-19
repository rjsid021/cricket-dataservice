from functools import wraps

import jwt
from flask import request, jsonify

from common.authentication.role_config import roles
from common.utils.helper import getEnvVariables
from log.log import get_logger

logger = get_logger("auth_service", "auth_service")
AUTH_SECRET_KEY_1 = getEnvVariables("AUTH_SECRET_KEY_1")
AUTH_SECRET_KEY_2 = getEnvVariables("AUTH_SECRET_KEY_2")

secret_key = "-----BEGIN PUBLIC KEY-----" + "\n" + AUTH_SECRET_KEY_1 + "\n" + AUTH_SECRET_KEY_2 + "\n" + "-----END PUBLIC KEY-----"


# decorator for verifying the JWT
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        # jwt is passed in the request header
        api_name = request.endpoint
        if 'Authorization' in request.headers:
            token = request.headers['Authorization'].replace("Bearer ", "")
        # return 401 if token is not passed
        if not token:
            response = {"message": "Token is missing !!",
                        'Status': '401'}
            return jsonify(response), 404, logger.info(f"{response}")

        try:
            # decoding the payload to fetch the stored details
            data = jwt.decode(token, secret_key, algorithms=["ES256", "ES256K"]) # , options={"verify_signature": False})
            token_roles = data['roles']

            # Add headers with token roles to be used further in application
            headers_cp = dict(request.headers)
            headers_cp["token_roles"] = token_roles
            headers_cp["token"] = token
            request.headers = headers_cp
            db_roles = roles[api_name]
            # data_map = getTeamsMapping()
            # data_map['WPL'] = data_map.pop('MIW')
            role_dict = {}

            for role in token_roles:
                if ":" in role:
                    team, position = role.split(":")
                    # if team in data_map:
                    role_dict[role] = position
                else:
                    role_dict[role] = role

            token_role_values = [value for key, value in role_dict.items()]

            if not set(token_role_values).isdisjoint(set(db_roles)):
                if api_name in ["app_gps.fetch-calendar-event", "app_gps.create-calendar-event", "app_gps.delete-calendar-event"]:
                    if request.json:
                        if isinstance(request.json, dict):
                            request.json['uuid'] = data["uuid"]
                        elif isinstance(request.json, list):
                            for item in request.json:
                                item['event_creator_uuid'] = data["uuid"]
                        else:
                            pass
                    else:
                        pass
                else:
                    pass
                if all(element == 'player' for element in token_role_values):
                    if api_name != "app_gps.putFitnessForm":
                        if request.json:
                            if "player_name" in data["metadata"]["player_details"]:
                                request.json['user_name'] = data["metadata"]["player_details"].get("player_name")
                            else:
                                request.json['user_name'] = data["metadata"].get("name")

                            if "src_player_id" in data["metadata"]["player_details"]:
                                request.json['user_id'] = int(data["metadata"]["player_details"].get("src_player_id"))
                            else:
                                pass
                    else:
                        pass
                else:
                    pass
            else:
                response = {
                    'message': 'User Unauthorised !!',
                    'Status': '403'
                }
                return jsonify(response), 403, logger.info(f"{response}")
        except Exception as err:
            response = {
                'message': f'Token is invalid for {api_name}!!',
                'Status': '401'
            }
            return jsonify(response), 401, logger.info(f"{response}")

        return f(*args, **kwargs)

    return decorated
