import datetime
import inspect
import os
import sys
from datetime import datetime, time
from pathlib import Path

import croniter
from crontab import CronTab

from scheduler.constants import WELLNESS_NOTIFICATION_MODULE

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)
from DataIngestion.config import NOTIFICATION_SCHEDULER_TABLE_NAME, NOTIFICATION_SCHEDULER_COL
from common.dao.fetch_db_data import getMaxId, getPandasFactoryDF
from common.dao.insert_data import insertToDB, upsertDatatoDB
from common.dao_client import session
from common.db_config import DB_NAME


def schedule_players_notification_cron(
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
):
    # This will be called by API while scheduling
    cron_str = f"{minute} {hour} * * *"
    cron = croniter.croniter(cron_str, start_date)

    # Get all the schedule for the date between start date and end date
    daily_run_schedule = []
    while True:
        next_run = cron.get_next(datetime)
        daily_run_schedule.append(next_run)
        if next_run >= end_date:
            break
    # Delete all existing cron first
    cron_tab = CronTab(user=True)
    for job in cron_tab:
        if job.comment == 'schedule_players_notification_cron_id':
            cron_tab.remove(job)
            cron_tab.write()
        if job.comment == 'notification_schedule_remover_id':
            cron_tab.remove(job)
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

    # Give permissions to file
    os.chmod(f'{os.path.dirname(os.path.abspath(__file__))}/notification_worker.py', 0o777)
    os.chmod(f'{os.path.dirname(os.path.abspath(__file__))}/notification_schedule_remover.py', 0o777)
    file_path = '/var/log/notification_scheduler.log'
    Path(file_path).touch()
    os.chmod('/var/log/notification_scheduler.log', 0o777)
    # Add cron job for each :daily_run_schedule
    for schedule in daily_run_schedule:
        # Prepare cron job schedule
        job = cron_tab.new(
            command=f'/usr/local/bin/python3 {os.path.dirname(os.path.abspath(__file__))}/notification_worker.py >> /var/log/notification_scheduler.log 2>&1',
            comment='schedule_players_notification_cron_id'
        )
        job.minute.on(schedule.minute)
        job.hour.on(schedule.hour)
        job.day.on(schedule.day)
        job.month.on(schedule.month)

    # Prepare data to be inserted into the Scheduler table
    id = getMaxId(session, NOTIFICATION_SCHEDULER_TABLE_NAME, NOTIFICATION_SCHEDULER_COL, DB_NAME)
    scheduler_data = [{
        'id': int(id),
        'start_date': start_date.strftime("%Y-%m-%d"),
        'end_date': end_date.strftime("%Y-%m-%d"),
        'team_name': team_name,
        'recipient': recipient,
        'module': WELLNESS_NOTIFICATION_MODULE,
        'is_whatsapp': is_whatsapp,
        'is_text': is_text,
        'is_mail': is_mail,
        'token_roles': token_roles,
        'is_active': True,
        'schedule_time': time(hour=hour, minute=minute).strftime("%H:%M:%S"),
        'load_timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }]
    insertToDB(session, scheduler_data, DB_NAME, NOTIFICATION_SCHEDULER_TABLE_NAME)

    # Add new job that will delete all cron, after end date is gone for player schedule
    clear_all_executed_job = cron_tab.new(
        command=f'/usr/local/bin/python3 {os.path.dirname(os.path.abspath(__file__))}/notification_schedule_remover.py >> /var/log/notification_scheduler.log 2>&1',
        comment='notification_schedule_remover_id'
    )
    # Schedule it to run after the last schedule
    exit_schedule = daily_run_schedule[-1]
    clear_all_executed_job.minute.on(exit_schedule.minute + 1)
    clear_all_executed_job.hour.on(exit_schedule.hour)
    clear_all_executed_job.day.on(exit_schedule.day)
    clear_all_executed_job.month.on(exit_schedule.month)
    cron_tab.write()
