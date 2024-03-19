import re
import uuid

from log.log import get_logger
from third_party_service.constants import EMAIL_REGEX
from third_party_service.knightwatch.knightwatch import KnightWatch
from third_party_service.notification.whatsapp import config
from third_party_service.notification.whatsapp.templates import PayloadTemplate

logger = get_logger("third_party_service", "payload")


class Payload:
    def __init__(self, token_roles):
        self.campaign_auth_id = config.CAMPAIGN_AUTH_ID
        self.knight_watch = KnightWatch()
        self.token_roles = token_roles

    def whatsapp_bulk_payload(self, message_data):
        recipient = message_data["recipient"]
        report_name = message_data["report_name"]
        date_filter = message_data["date_filter"]
        # call knightwatch service to get phone numbers for players

        players_metadata = self.knight_watch.players_coaches_info_knightwatch(self.token_roles)
        players_metadata = players_metadata["results"]
        players_whatsapp_mapper = {}
        for player in players_metadata:
            player_name = player['metadata'].get('db_name', player['metadata'].get('name'))
            # Skip all the players from knight watch whose names is not in players list.
            if not player_name or (player_name.lower() not in recipient):
                continue
            # Iterate among all players contacts which contain phone and email
            player_contacts = player["contacts"]
            for player_contact in player_contacts:
                contact = player_contact.get('contact')
                is_email = re.fullmatch(EMAIL_REGEX, contact)
                if not is_email:
                    contact = contact.replace("+", "")
                    players_whatsapp_mapper[player_name] = contact
        payloads = []
        payload = PayloadTemplate()
        # Generating Payload for recipient
        for player_name, player_number in players_whatsapp_mapper.items():
            payloads.append(
                payload.get_template(
                    player_name,
                    report_name,
                    player_number,
                    date_filter,
                    message_data
                )
            )
        return payloads

    def whatsapp_payload(self):
        pass

    def generate_fitness_payload_text_message(self, players: list, template_id) -> object:
        common_data_list = []
        players_metadata = self.knight_watch.players_info_knightwatch(self.token_roles)
        players_metadata = players_metadata["results"]
        for player in players_metadata:
            # Skip all the players from knight watch whose names is not in players list.
            player_name = player['metadata'].get('db_name', player['metadata'].get('name'))
            if not player_name or (player_name.lower() not in players):
                continue
            player_contacts = player["contacts"]
            for player_contact in player_contacts:
                contact = player_contact.get('contact')
                is_email = re.fullmatch(EMAIL_REGEX, contact)
                if not is_email:
                    common_data_list.append(
                        {
                            "name": "SMS",
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
            "templateID": f"{template_id}",
            "referenceID": str(uuid.uuid4())
        }
        return payload

    def generate_fitness_payload_mail(self, players: list, template_id) -> object:
        common_data_list = []
        players_metadata = self.knight_watch.players_info_knightwatch(self.token_roles)
        players_metadata = players_metadata["results"]
        for player in players_metadata:
            # Skip all the players from knight watch whose names is not in players list.
            player_name = player['metadata'].get('db_name', player['metadata'].get('name'))
            if not player_name or (player_name.lower() not in players):
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
            "templateID": f"{template_id}",
            "referenceID": str(uuid.uuid4())
        }
        return payload
