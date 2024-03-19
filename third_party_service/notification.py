"""
Notification class is used to send notification to players, which can be extended for future case to send generic notification
"""

import requests
import json
from log.log import get_logger
from third_party_service import config
import re

from third_party_service.constants import EMAIL_REGEX

logger = get_logger("notification", "notification")


class Notification:
    def __init__(self, template_id, access_token, payload_sub="jiosports"):
        self.cns_url = config.CNS_URL
        self.cns_endpoint = config.CNS_ENDPOINT
        self.knight_watch_url = config.KNIGHT_WATCH_URL
        self.knight_watch_endpoint = config.KNIGHT_WATCH_ENDPOINT
        self.payload = config.CNS_URL
        self.template_id = template_id
        self.payload_sub = payload_sub
        self.access_token = access_token

    def players_info_knightwatch(self) -> object:
        """Call Knight Watch to get players metadata, we are mostly interested in phone and email"""
        payload = {
            "sub": self.payload_sub,
            "accessToken": self.access_token,
            "query": {
                "roleFilter": {
                    "valuesToMatch": [
                        "player"
                    ],
                    "searchType": "ALL"
                }
            }
        }
        req = requests.post(self.knight_watch_url + self.knight_watch_endpoint, json=payload)
        data = req.json()
        logger.info("players_info_knightwatch -> {data}")
        # TODO: exception handling for fail cases
        logger.info("Response received from CNS Service")
        return data

    def generate_payload(self, players: list) -> object:
        common_data_list = []
        players_lowercase = [player for player in map(lambda _: _.lower(), players)]
        players_metadata = self.players_info_knightwatch()
        players_metadata = players_metadata["results"]
        for player in players_metadata:
            # Skip all the players from knight watch whose names is not in players list.
            player_name = player['metadata'].get('name')
            if not player_name or (player_name.lower() not in players_lowercase):
                continue
            player_contacts = player["contacts"]
            for player_contact in player_contacts:
                contact = player_contact.get('contact')
                is_email = re.fullmatch(EMAIL_REGEX, contact)
                if is_email:
                    common_data_list.append(
                        {
                            "name": "EMAIL",
                            "value": contact
                        }
                    )
                else:
                    common_data_list.append(
                        {
                            "name": "NUMBER",
                            "value": contact
                        }
                    )

        # Generate Payload
        payload = {
            "contactMedium": {
                "characteristics": {
                    "commonDataList": common_data_list
                }
            },
            "payloadMessage": {
                "attributes": {}
            },
            "templateID": f"{self.template_id}"
        }
        return payload

    def send_notification(self, players: list) -> bool:
        auth = {
            'Cache-Control': 'no-cache',
            'content-type': 'application/json'
        }
        payload = self.generate_payload(players)
        try:
            req = requests.post(self.cns_url + self.cns_endpoint, headers=auth, data=json.dumps(payload))
            data = req.json()
        # Too broad exception narrow it down
        except Exception as err:
            return False
        logger.info(f"send_notification -> {data}")
        logger.info("Response received from CNS Service")
        return data

# def send_notification():
#     access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiJ9.eyJzdWIiOiJqaW9zcG9ydHMiLCJ1dWlkIjoiMGVjNzkxOGU5M2RkNDI5YWFiYjc2ZmVkMjlkMjliYjVjNmJmODNlZjNjMDk0MGI0YWI0YjU2ZGFlNjY0MzM5MSIsIm1ldGFkYXRhIjp7Im5hbWUiOiJ0ZXN0IiwiZW1haWwiOiIiLCJudW1iZXIiOiIifSwiY29udGFjdExpc3QiOlsiKzkxMTIzNDU2Nzg5MCJdLCJpYXQiOjE2Nzc1OTAyMTMsInJvbGVzIjpbImFkbWluIl0sInBlcm1pc3Npb25zIjpbImNyZWF0ZTphbGwiLCJkZWxldGU6YWxsIiwib25ib2FyZCIsInJlYWQ6YWxsIl0sImV4cCI6MTY3NzY3NjYxMywiaXNzIjoia25pZ2h0d2F0Y2gifQ._4I9qsbzMHMiqPDJjfW2U8uXtE5SM6Rulc0bplFhSvi7QhcDzOvwBv91EI2dO_XUsdtzLVFw9Nf4oBg0TOLTww"
#     notification = Notification(411939, access_token, "jiosports")
#     players = ["Achintya Ranjan Chaudhary", "Vaibhav Gowsami"]
#     response = notification.send_notification(players)
#     print(response)
#
#
# if __name__ == "__main__":
#     send_notification()
