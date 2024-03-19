# This package contains the UMS(User Management Service) calls
import requests

from DataService.utils.helper import process_response
from common.utils.helper import getEnvVariables


class UMS:
    def __init__(self):
        self.base_url = getEnvVariables("UMS_BASE_URL")

    def get_user_leagues(self, token):
        # This method will return the list of leagues for which super_user has access
        # Add headers to the request
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }
        # Make API call to UMS Service
        response = requests.get(self.base_url + "/ums/v1/leagues_mini", headers=headers)
        # if the HTTP request returned an unsuccessful status code (i.e., 4xx or 5xx series status codes). Raise Error
        response.raise_for_status()
        # Return response in JSON format
        response = response.json()
        leagues = response.get("leagues")
        user_leagues = []
        for league in leagues:
            user_leagues.append(league.get("league_id"))
        return user_leagues

    def get_team_info(self, token, team_name):
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }

        # Make API request
        response = requests.get(self.base_url + f"ums/v1/leagues/{team_name}", headers=headers)
        response.raise_for_status()
        response_json = response.json()

        # Process the response
        team_output = process_response(response_json)

        return team_output
