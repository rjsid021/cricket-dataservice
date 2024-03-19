import json
import re

from common.authentication.role_config import SUPER_ADMIN, PLAYER, ADMIN, ANALYST, HEAD_COACH, COACH, SUPPORT
from log.log import get_logger
from third_party_service.constants import EMAIL_REGEX
from third_party_service.knightwatch.knightwatch import KnightWatch
from third_party_service.ums import UMS

logger = get_logger("notification", "utils")

def get_ums_sa_leagues(token):
    leagues = UMS().get_user_leagues(token)
    return leagues


def get_roles_token(token, token_roles):
    if SUPER_ADMIN in token_roles:
        leagues = get_ums_sa_leagues(token)
    else:
        leagues = set()
        for role in token_roles:
            leagues.add(role.split(":")[0])
    response = []
    for league in leagues:
        response.extend([
            f"{league}:{ADMIN}",
            f"{league}:{ANALYST}",
            f"{league}:{COACH}",
            f"{league}:{HEAD_COACH}",
            f"{league}:{PLAYER}",
            f"{league}:{SUPPORT}",
        ])
    return response


def get_kw_contacts(token_roles, uuids, email_only=False):
    knight_watch = KnightWatch()
    kw_recipients = knight_watch.players_coaches_info_knightwatch(token_roles)
    logger.info("KW recipients")
    logger.info(f"{kw_recipients}")
    response = []
    for recipient in kw_recipients['results']:
        if recipient['uuid'] in uuids:
            email = ''
            phone = ''
            for contacts in recipient["contacts"]:
                contact = contacts.get('contact')
                is_email = re.fullmatch(EMAIL_REGEX, contact)
                if is_email:
                    email = contact
                if not is_email:
                    phone = contact
            recipient_meta_data = {
                "name": recipient['metadata'].get('db_name', recipient['metadata'].get('name')),
                "email": email,
                "phone": phone,
                "uuid": recipient['uuid']
            }

            # Create a dictionary for the current item and append it to the output list
            if email_only:
                response.append(recipient_meta_data['email'])
            else:
                response.append(recipient_meta_data)
    logger.info("Knightwatch email id received")
    logger.info(json.dumps(response, indent=4))
    return response
