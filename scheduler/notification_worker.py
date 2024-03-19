import datetime
import inspect
import json
import os
import sys

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)
import numpy as np

from scheduler.constants import WELLNESS_NOTIFICATION_MODULE
from third_party_service.notification.notification import Notification
from third_party_service.notification.payload import Payload
from third_party_service.notification.whatsapp.config import WELLNESS_CAMPAIGN_TEMPLATE

from DataIngestion.config import SCHEDULER_LOG_TABLE_NAME, SCHEDULER_LOG_COL
from DataService.utils.constants import NOTIFICATION_TEMPLATE_ID
from DataService.utils.helper import get_form_not_filled_players_list
from common.dao.fetch_db_data import getPandasFactoryDF, getMaxId
from common.dao.insert_data import insertToDB
from common.dao_client import session
from common.db_config import DB_NAME
from log.log import get_logger

logger = get_logger("cron", "notification_worker")


def cron_notification_worker():
    # Query Notification Scheduler Table
    # Hit the database to get the team name where is_active is True
    data_frame = getPandasFactoryDF(
        session,
        f"select * from {DB_NAME}.notificationscheduler where is_active=True and module='{WELLNESS_NOTIFICATION_MODULE}' ALLOW FILTERING"
    )
    record = data_frame.to_json(orient='records')
    result = json.loads(record)[0]
    active_players = result['recipient']
    team_name = result['team_name']
    is_whatsapp = result['is_whatsapp']
    is_text = result['is_text']
    is_mail = result['is_mail']
    token_roles = result['token_roles']
    today_date = datetime.datetime.today()
    # Send notification to players
    FITNESS_FORM_SQL = f'''select player_name, record_date from {DB_NAME}.fitnessForm where record_date = '{today_date}' '''

    if team_name:
        FITNESS_FORM_SQL = FITNESS_FORM_SQL + f" and team_name='{team_name}' Allow Filtering;"
    else:
        FITNESS_FORM_SQL = FITNESS_FORM_SQL

    fitness_form_df = getPandasFactoryDF(session, FITNESS_FORM_SQL).drop_duplicates(
        subset=["record_date", "player_name"],
        keep="last"
    )
    fitness_form_df = fitness_form_df.mask(fitness_form_df == -1, np.NaN)
    fitness_form_df = fitness_form_df.mask(fitness_form_df == 'NA', np.NaN)
    recipient = get_form_not_filled_players_list(fitness_form_df, active_players, team_name)

    # Create payload for mail, whatsapp and text message to recipient.
    notification = Notification(
        is_text=is_text,
        is_mail=is_mail,
        is_whatsapp=is_whatsapp,
        text_template_id=NOTIFICATION_TEMPLATE_ID,
        mail_template_id=NOTIFICATION_TEMPLATE_ID
    )
    payloads = {}
    if is_whatsapp:
        payloads["whatsapp_payload"] = Payload(token_roles).whatsapp_bulk_payload({
            'recipient': recipient,
            'report_name': WELLNESS_NOTIFICATION_MODULE,
            'campaign_template': WELLNESS_CAMPAIGN_TEMPLATE,
            'date_filter': {}
        })
    if is_text:
        payloads["text_payload"] = Payload(token_roles).generate_fitness_payload_text_message(
            recipient,
            NOTIFICATION_TEMPLATE_ID
        )
    if is_mail:
        payloads["mail_payload"] = Payload(token_roles).generate_fitness_payload_mail(recipient, NOTIFICATION_TEMPLATE_ID)

    # Send notification to players
    notification.send_bulk_notification(payloads)

    logger.info("cron_notification_worker ran successfully")
    # Prepare data to be inserted into the Scheduler log Table
    max_id = getMaxId(session, SCHEDULER_LOG_TABLE_NAME, SCHEDULER_LOG_COL, DB_NAME)
    scheduler_data = [{
        'id': int(max_id),
        'recipient': json.dumps(recipient),
        'message': "",
        'module': WELLNESS_NOTIFICATION_MODULE,
        'load_timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }]
    insertToDB(session, scheduler_data, DB_NAME, SCHEDULER_LOG_TABLE_NAME)


if __name__ == '__main__':
    cron_notification_worker()
