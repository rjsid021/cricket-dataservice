import json

import requests

from log.log import get_logger
from third_party_service.notification.mail import config

logger = get_logger("notification", "text_message")


class TextMessage:
    def __init__(self, template_id):
        self.cns_url = config.CNS_URL
        self.cns_endpoint = config.CNS_ENDPOINT
        self.template_id = template_id

    def send_notification(self, payload) -> object:
        auth = {
            'Cache-Control': 'no-cache',
            'content-type': 'application/json'
        }
        try:
            logger.warning(f"Endpoint of Customer notification service -> {self.cns_url + self.cns_endpoint}")
            request_data = json.dumps(payload)
            response = requests.post(self.cns_url + self.cns_endpoint, headers=auth, data=request_data)
            # raises exception when not a 2xx response
            response.raise_for_status()
            logger.warning(f"Response: {response}")
            return response
        except Exception as err:
            logger.error(f"Some error Occurred while sending request -> {err}")
            return False

    def send_bulk_notification(self, payload):
        return self.send_notification(payload)
