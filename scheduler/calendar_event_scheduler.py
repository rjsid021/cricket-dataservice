import inspect
import os
import sys
from datetime import time
from datetime import timedelta
from datetime import timezone

from common.dao.insert_data import upsertDatatoDB
from log.log import get_logger
from third_party_service.notification.utils import get_kw_contacts
from third_party_service.smtp import SMTPMailer

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)
from DataIngestion.config import CALENDAR_EVENT_TABLE_NAME, CALENDAR_EVENT_KEY_COL
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session
from common.db_config import DB_NAME

logger = get_logger("cronjob", "calendar_event_scheduler")


def calendar_event_scheduler():
    # Fetch data from db
    logger.info("calendar_event_scheduler invoked")
    calendar_events = getPandasFactoryDF(
        session,
        f"SELECT * FROM {DB_NAME}.{CALENDAR_EVENT_TABLE_NAME} where reminder_processed=False allow filtering"
    )
    if calendar_events.empty:
        # no mail to send for reminder return flow
        logger.info("No Entry in database where processing of reminder is required")
        return
    from datetime import datetime

    def calculate_reminder_time(row):
        # Define your logic here to calculate the value of the new column based on values in columns A, B, and C
        # For example, let's say we want to sum the values in columns A, B, and C
        remind_minute = row['reminder_time'].minute
        remind_hour = row['reminder_time'].hour
        event_dates = datetime.strptime(str(row['event_dates']), '%Y-%m-%d').date()
        # Extract hour, minute, and second components from the time string
        time_parts = str(row['start_time']).split(':')
        hours = int(time_parts[0])
        minutes = int(time_parts[1])
        seconds = int(time_parts[2][:2])  # Extract only the first two characters for seconds

        # Create a Python datetime.time object
        time_object = time(hours, minutes, seconds)
        # Define the number of hours and minutes to subtract

        # Create a timedelta object with the specified hours and minutes
        time_to_subtract = timedelta(hours=remind_hour, minutes=remind_minute)
        datetime_object = datetime.combine(event_dates, time_object)
        # Subtract the timedelta from the datetime_object
        datetime_object -= time_to_subtract
        utc_time_now = datetime.utcnow().replace(second=0, microsecond=0)
        # if schedule is happening send mail
        if datetime_object <= utc_time_now:
            return True

    # Apply the function to create the new column
    calendar_events['allow_mail'] = calendar_events.apply(calculate_reminder_time, axis=1)
    calendar_events = calendar_events[calendar_events['allow_mail'] == True]
    if calendar_events.empty:
        # no mail to send for reminder return flow
        logger.info("No calender Reminder to send")
        return
    # send call to Knight watch to get users email id
    for index, event in calendar_events.iterrows():

        uuids = event['recipient_uuid']
        token_roles = event['token_roles']
        recipient_emails = get_kw_contacts(token_roles=token_roles, uuids=uuids, email_only=True)
        logger.info("Got recipient_emails")
        logger.info(f"{recipient_emails}")
        event_dates = f"{event['event_dates']}"
        subject = f"MICIP Event : {event['event_type']} on {event_dates}"
        start_time = f"{event['start_time']}"
        end_time = f"{event['end_time']}"
        logger.info(
            f"Processing for event {event['event_title']} for date {event_dates} at {start_time} to {end_time}."
            f" Reminder of minutes: {event['reminder_time'].minute} and hours:{event['reminder_time'].hour}"
        )
        # Parse the input time string into a time object
        start_time_obj = datetime.strptime(start_time.split('.')[0], '%H:%M:%S').time()
        end_time_obj = datetime.strptime(end_time.split('.')[0], '%H:%M:%S').time()

        # Get the current date in UTC timezone
        utc_date = datetime.strptime(event_dates, '%Y-%m-%d').date()

        # Combine the time with the current date to create a datetime object
        utc_datetime_start = datetime.combine(utc_date, start_time_obj)
        utc_datetime_end = datetime.combine(utc_date, end_time_obj)

        # Convert the datetime object to UTC timezone
        utc_datetime_start_utc = utc_datetime_start.astimezone(timezone.utc)
        utc_datetime_end_utc = utc_datetime_end.astimezone(timezone.utc)

        # Format the UTC datetime as a string without seconds and with AM/PM
        start_time = utc_datetime_start_utc.strftime('%I:%M %p %Z')
        end_time = utc_datetime_end_utc.strftime('%I:%M %p %Z')

        event_title = f"{event['event_title']}" or f"{event['event_type']}"
        venue = f"{event['venue']}"
        event['reminder_processed'] = True
        # drop key
        event.drop('allow_mail', inplace=True)
        event['event_dates'] = str(event['event_dates'])
        event['end_time'] = str(event['end_time'])
        event['start_time'] = str(event['start_time'])
        event['reminder_time'] = str(event['reminder_time'])
        event['load_timestamp'] = str(event['load_timestamp'])
        # update calendar event back to database
        upsertDatatoDB(
            session,
            [event.to_dict()],
            DB_NAME,
            CALENDAR_EVENT_TABLE_NAME,
            CALENDAR_EVENT_KEY_COL,
            False
        )
        logger.info("upsert database done")
        # send mail to users
        html_content = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Promotion Message</title>
        <link rel="stylesheet" href="https://fonts.googleapis.com/css?family=Roboto:300,400,500,700&display=swap">
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/material-components-web/4.0.0/material-components-web.min.css">
        <style>
            body {
                font-family: 'Roboto', sans-serif;
                background-color: #f8f9fa;
                margin: 0;
                padding: 0;
            }
            .container {
                width: 80%;
                margin: 50px auto;
                background-color: #fff;
                padding: 20px;
                border-radius: 8px;
                box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
            }
            h1 {
                color: #333;
                text-align: center;
                margin-bottom: 20px;
            }
            .event-info {
                padding: 20px;
                border-radius: 8px;
                background-color: #f3f3f3; /* Cream color */
                color: #333; /* Dark text */
            }
            .event-title {
                font-size: 24px;
                margin-bottom: 10px;
            }
            .event-details {
                font-size: 18px;
                margin-bottom: 10px;
            }
            .btn {
                display: inline-block;
                background-color: #17a2b8; /* Blue */
                color: #fff;
                padding: 10px 20px;
                text-decoration: none;
                border-radius: 4px;
                transition: background-color 0.3s;
                margin-top: 20px;
            }
            .btn:hover {
                background-color: #138496; /* Darker blue on hover */
            }
        </style>
        </head>
        <body>
        <div class="container">
            <div class="event-info">
                <h1 class="event-title">MICIP Upcoming Event</h1>
                <p class="event-details">Hello MICIP member,</p>
                <p class="event-details">This is reminder for event - <strong>{{ event }}</strong> scheduled on <strong>{{ event_dates }}</strong> at <strong>{{ start_time }}</strong> to <strong>{{ end_time }} at - <strong>{{ venue }}</strong></strong>. Please attend the event.</p>
                <p class="event-details">Thanks</p>
                <p class="event-details">MICIP Team</p>
            </div>
        </div>
        </body>
        </html>
        """
        # Define variables
        variables = {
            'event': event_title,
            'event_dates': event_dates,
            'start_time': start_time,
            'end_time': end_time,
            'venue': venue
        }

        # Update HTML content with variables
        for var, value in variables.items():
            html_content = html_content.replace('{{ ' + var + ' }}', value)

        # Send it to sftp mail service
        SMTPMailer().send_bulk_email(recipient_emails, subject, html_content, True)
    logger.info("Calendar Event Scheduler Finished.")


# invoke script
calendar_event_scheduler()
