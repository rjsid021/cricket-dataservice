import requests

from log.log import get_logger
from third_party_service.knightwatch import config

logger = get_logger("knightwatch", "knightwatch")


class KnightWatch:
    def __init__(self, payload_sub="jiosports"):
        self.payload_sub = payload_sub
        self.access_token = config.KNIGHT_ACCESS_TOKEN
        self.knight_watch_tenant_auth = config.KNIGHT_WATCH_TENANT_AUTH
        self.knight_watch_url = config.KNIGHT_WATCH_URL
        self.knight_watch_endpoint = config.KNIGHT_WATCH_ENDPOINT

    def players_info_knightwatch(self, token_roles) -> dict:
        """Call Knight Watch to get players metadata, we are mostly interested in phone and email"""
        header = {
            "tenantAuth": self.knight_watch_tenant_auth,
            "Content-Type": "application/json"
        }

        payload = {
            "sub": self.payload_sub,
            "accessToken": self.access_token,
            "query": {
                "roleFilter": {
                    "valuesToMatch": token_roles,
                    "searchType": "ALL"
                }
            }
        }
        try:
            req = requests.post(self.knight_watch_url + self.knight_watch_endpoint, json=payload, headers=header)
            data = req.json()
            return data
        except Exception as err:
            logger.error(err)

    def players_coaches_info_knightwatch(self, token_roles) -> dict:
        """Call Knight Watch to get players metadata, we are mostly interested in phone and email"""
        header = {
            "tenantAuth": self.knight_watch_tenant_auth,
            "Content-Type": "application/json"
        }
        payload = {
            "sub": self.payload_sub,
            "accessToken": self.access_token,
            "query": {
                "roleFilter": {
                    "valuesToMatch": token_roles,
                    "searchType": "EITHER"
                }
            }
        }
        try:
            endpoint = self.knight_watch_url + self.knight_watch_endpoint
            logger.info(f"Endpoint KW - {endpoint}")
            logger.info(f"Endpoint KW - {payload}")
            logger.info(f"Endpoint KW - {header}")
            response = requests.post(endpoint, json=payload, headers=header)
            response.raise_for_status()
            logger.info(str(response))
            logger.info(response.text)
            return response.json()
        except Exception as err:
            raise Exception(err)
