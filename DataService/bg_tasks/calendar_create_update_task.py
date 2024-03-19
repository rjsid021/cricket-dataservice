import threading
from datetime import datetime, timezone
from typing import List

from DataIngestion.config import CALENDAR_EVENT_TABLE_NAME
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session
from common.db_config import DB_NAME
from log.log import get_logger
from third_party_service.notification.utils import get_kw_contacts
from third_party_service.smtp import SMTPMailer

logger = get_logger("bg_task", "calendar_create_update_task")


class CalendarCreateUpdateBGTask(threading.Thread):
    def __init__(self, pks: List[int], is_new_event: bool):
        super().__init__()
        self.pks = pks
        self.is_new_event = is_new_event

    def new_calendar_event(self, calendar_events):
        # send call to Knight watch to get users email id
        uuids = calendar_events['recipient_uuid'].iloc[0]
        if not uuids:
            logger.warning(f"No uuid found for {self.pks}")
            return

        token_roles = calendar_events['token_roles'].iloc[0]
        recipient_emails = get_kw_contacts(token_roles=token_roles, uuids=uuids, email_only=True)
        if not recipient_emails:
            logger.info(f"No email ids to send for updated events")
            return
        event_dates = f"{calendar_events.iloc[0]['event_dates']}"
        cal_event_dates = calendar_events['event_dates'].tolist()
        cal_event_dates = [str(date) for date in cal_event_dates]
        subject = f"MICIP Event : {calendar_events.iloc[0]['event_type']} Scheduled"
        start_time = f"{calendar_events.iloc[0]['start_time']}"
        end_time = f"{calendar_events.iloc[0]['end_time']}"
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

        event = f"{calendar_events.iloc[0]['event_title']}" or f"{calendar_events.iloc[0]['event_type']}"
        venue = f"{calendar_events.iloc[0]['venue']}"

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
                                    <p class="event-details">There is an event - <strong>{{ event }}</strong> scheduled on <strong>{{ event_dates }}</strong> at <strong>{{ start_time }}</strong> to <strong>{{ end_time }} at - <strong>{{ venue }}</strong></strong>. Please attend the event.</p>
                                    <p class="event-details">Thanks</p>
                                    <p class="event-details">MICIP Team</p>
                                </div>
                            </div>
                            </body>
                            </html>
                            """
        # Define variables
        variables = {
            'event': event,
            'event_dates': str(cal_event_dates),
            'start_time': start_time,
            'end_time': end_time,
            'venue': venue
        }

        # Update HTML content with variables
        for var, value in variables.items():
            html_content = html_content.replace('{{ ' + var + ' }}', value)

        # Send it to sftp mail service
        SMTPMailer().send_bulk_email(recipient_emails, subject, html_content, True)

    def updated_calendar_event(self, calendar_events):
        # send call to Knight watch to get users email id
        uuids = calendar_events['recipient_uuid'].iloc[0]
        if not uuids:
            logger.warning(f"No uuid found for {self.pks}")
            return
        id = calendar_events['id'].iloc[0]
        token_roles = calendar_events['token_roles'].iloc[0]
        recipient_emails = get_kw_contacts(token_roles=token_roles, uuids=uuids, email_only=True)
        if not recipient_emails:
            logger.info(f"No email ids to send for {id}")
            return
        event_dates = f"{calendar_events.iloc[0]['event_dates']}"
        subject = f"MICIP Event : {calendar_events.iloc[0]['event_type']} on {event_dates} Updated!"
        start_time = f"{calendar_events.iloc[0]['start_time']}"
        end_time = f"{calendar_events.iloc[0]['end_time']}"
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

        event = f"{calendar_events.iloc[0]['event_title']}" or f"{calendar_events.iloc[0]['event_type']}"
        venue = f"{calendar_events.iloc[0]['venue']}"

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
                            <p class="event-details">There is an event - <strong>{{ event }}</strong> scheduled on <strong>{{ event_dates }}</strong> at <strong>{{ start_time }}</strong> to <strong>{{ end_time }} at - <strong>{{ venue }}</strong></strong>. Please attend the event.</p>
                            <p class="event-details">Thanks</p>
                            <p class="event-details">MICIP Team</p>
                        </div>
                    </div>
                    </body>
                    </html>
                    """
        # Define variables
        variables = {
            'event': event,
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

    def run(self):
        try:
            logger.info("BG task for sending create mail")
            # Fetch data from db
            query = f"SELECT * FROM {DB_NAME}.{CALENDAR_EVENT_TABLE_NAME} WHERE id IN ({', '.join(map(str, self.pks))}) ALLOW FILTERING"
            calendar_events = getPandasFactoryDF(
                session,
                query
            )

            # If new calendar event is created
            if self.is_new_event:
                self.new_calendar_event(calendar_events)
            # If existing calendar event get updated
            else:
                self.updated_calendar_event(calendar_events)
            logger.info("CalendarCreateUpdateBGTask Finished processing.")
        except Exception as e:
            logger.info(f"CalendarCreateUpdateBGTask Exception : {e}")
