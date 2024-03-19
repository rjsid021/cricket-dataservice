import inspect
import os
import sys
from datetime import datetime

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from DataIngestion.config import NOTIFICATION_SCHEDULER_TABLE_NAME
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session
from scheduler.constants import MODULE_MAPPING, WELLNESS_NOTIFICATION_MODULE
from scheduler.gps_report_scheduler import gps_periodic_scheduler
from scheduler.notification_scheduler import schedule_players_notification_cron


def check_active_scheduler():
    for module in MODULE_MAPPING:
        scheduler = getPandasFactoryDF(
            session,
            f"""
            select 
              * 
            from 
              {NOTIFICATION_SCHEDULER_TABLE_NAME} 
            where 
              is_active = True 
              and module = '{module}' ALLOW FILTERING
            """
        )
        if scheduler.empty:
            continue
        scheduler = scheduler.to_dict(orient='records')[0]
        start_date = datetime.strptime(str(scheduler['start_date']), "%Y-%m-%d")
        end_date = datetime.strptime(str(scheduler['end_date']), "%Y-%m-%d")
        hour = scheduler['schedule_time'].hour
        minute = scheduler['schedule_time'].minute
        team_name = scheduler['team_name']
        recipient = scheduler['recipient']
        is_whatsapp = scheduler['is_whatsapp']
        is_text = scheduler['is_text']
        is_mail = scheduler['is_mail']
        token_roles = scheduler['token_roles']
        if module == WELLNESS_NOTIFICATION_MODULE:
            schedule_players_notification_cron(
                start_date,
                end_date,
                hour,
                minute,
                team_name,
                recipient,
                is_whatsapp,
                is_text,
                is_mail,
                token_roles
            )
        else:
            gps_periodic_scheduler(
                start_date,
                end_date,
                hour,
                minute,
                team_name,
                recipient,
                module,
                is_whatsapp,
                is_text,
                is_mail,
                "System Scheduler",
                token_roles
            )


if __name__ == '__main__':
    check_active_scheduler()
