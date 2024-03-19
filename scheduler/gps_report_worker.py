import datetime
import inspect
import json
import os
import sys

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)
from third_party_service.notification.whatsapp.config import GPS_CAMPAIGN_TEMPLATE

from third_party_service.notification.notification import Notification
from third_party_service.notification.payload import Payload
from DataIngestion.config import SCHEDULER_LOG_TABLE_NAME, SCHEDULER_LOG_COL
from common.dao.fetch_db_data import getPandasFactoryDF, getMaxId
from common.dao.insert_data import insertToDB
from common.dao_client import session
from common.db_config import DB_NAME
from log.log import get_logger

logger = get_logger("cron", "gps_worker")


def cron_notification_worker():
    # Query Notification Scheduler Table
    # Hit the database to get the team name where is_active is True
    report_name = sys.argv[1]
    data_frame = getPandasFactoryDF(
        session,
        f"select * from {DB_NAME}.notificationscheduler where module='{report_name}' and is_active=True ALLOW FILTERING;"
    )
    record = data_frame.to_json(orient='records')
    result = json.loads(record)[0]
    recipients = result['recipient']

    is_text = result['is_text']
    is_mail = result['is_mail']
    is_whatsapp = result['is_whatsapp']
    user_name = result['user_name']
    token_roles = result['token_roles']

    # Create payload for mail, whatsapp and text message to recipient.
    notification = Notification(
        is_text=is_text,
        is_mail=is_mail,
        is_whatsapp=is_whatsapp,
        text_template_id=None,
        mail_template_id=None
    )
    payloads = {}
    if is_whatsapp:
        payloads["whatsapp_payload"] = Payload(token_roles).whatsapp_bulk_payload({
            'recipient': recipients,
            'report_name': report_name,
            'campaign_template': GPS_CAMPAIGN_TEMPLATE,
            'date_filter': {},
            'active_tab': 0,
            'user_name': user_name
        })
    if is_text:
        payloads["text_payload"] = None
    if is_mail:
        payloads["mail_payload"] = None

    # Send notification to players
    notification.send_bulk_notification(payloads)

    # Prepare data to be inserted into the Scheduler log Table
    max_id = getMaxId(session, SCHEDULER_LOG_TABLE_NAME, SCHEDULER_LOG_COL, DB_NAME)
    scheduler_data = [{
        'id': int(max_id),
        'recipient': json.dumps(recipients),
        'message': "",
        'load_timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'module': report_name
    }]
    insertToDB(session, scheduler_data, DB_NAME, SCHEDULER_LOG_TABLE_NAME)
    logger.info("gps_worker ran successfully")


if __name__ == '__main__':
    cron_notification_worker()
