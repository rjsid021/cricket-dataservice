import inspect
import os
import sys

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao.insert_data import upsertDatatoDB
from common.db_config import DB_NAME
from scheduler.constants import WELLNESS_NOTIFICATION_MODULE

from crontab import CronTab
from DataIngestion.config import NOTIFICATION_SCHEDULER_TABLE_NAME, NOTIFICATION_SCHEDULER_COL

from common.dao_client import session


def clear_all_cron_schedule():
    # Delete all existing cron first, as they are finished executing
    cron_tab = CronTab(user=True)
    clear_job = None
    for job in cron_tab:
        if job.comment == 'schedule_players_notification_cron_id':
            cron_tab.remove(job)
            cron_tab.write()
        if job.comment == 'notification_schedule_remover_id':
            clear_job = job
    cron_tab.remove(clear_job)
    cron_tab.write()

    # Mark as inactive for existing schedule
    active_scheduler = getPandasFactoryDF(
        session,
        f"select * from {NOTIFICATION_SCHEDULER_TABLE_NAME} where is_active=True and module='{WELLNESS_NOTIFICATION_MODULE}' ALLOW FILTERING;"
    )
    active_scheduler = active_scheduler.to_dict(orient='records')
    # Serialize data for existing scheduler
    for scheduler in active_scheduler:
        scheduler['start_date'] = str(scheduler['start_date'])
        scheduler['is_active'] = False
        scheduler['end_date'] = str(scheduler['end_date'])
        scheduler['load_timestamp'] = str(scheduler['load_timestamp'])
        scheduler['schedule_time'] = str(scheduler['schedule_time'])
    # update schedule which will be inactive after this new schedule
    upsertDatatoDB(
        session,
        active_scheduler,
        DB_NAME,
        NOTIFICATION_SCHEDULER_TABLE_NAME,
        NOTIFICATION_SCHEDULER_COL
    )


if __name__ == '__main__':
    clear_all_cron_schedule()
