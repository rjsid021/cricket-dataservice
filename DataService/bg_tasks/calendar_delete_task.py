import threading
from datetime import datetime, timezone

from log.log import get_logger
from third_party_service.notification.utils import get_kw_contacts
from third_party_service.smtp import SMTPMailer

logger = get_logger("bg_task", "calendar_delete_task")


class CalendarDeleteBGTask(threading.Thread):
    def __init__(self, recipient_uuid, event_dates, start_time, end_time, event_title, event_type, venue, token_roles):
        super().__init__()
        self.recipient_uuid = recipient_uuid
        self.event_dates = str(event_dates)
        self.start_time = str(start_time)
        self.end_time = str(end_time)
        self.event_title = event_title
        self.event_type = event_type
        self.venue = venue
        self.token_roles = token_roles

    def run(self):
        try:
            logger.info("BG task for deleting event mail")
            # send call to Knight watch to get users email id
            recipient_emails = get_kw_contacts(token_roles=self.token_roles, uuids=self.recipient_uuid, email_only=True)
            subject = f"MICIP Event : {self.event_type} on {self.event_dates} Cancelled!"

            # Parse the input time string into a time object
            start_time_obj = datetime.strptime(self.start_time.split('.')[0], '%H:%M:%S').time()
            end_time_obj = datetime.strptime(self.end_time.split('.')[0], '%H:%M:%S').time()

            # Get the current date in UTC timezone
            utc_date = datetime.strptime(self.event_dates, '%Y-%m-%d').date()

            # Combine the time with the current date to create a datetime object
            utc_datetime_start = datetime.combine(utc_date, start_time_obj)
            utc_datetime_end = datetime.combine(utc_date, end_time_obj)

            # Convert the datetime object to UTC timezone
            utc_datetime_start_utc = utc_datetime_start.astimezone(timezone.utc)
            utc_datetime_end_utc = utc_datetime_end.astimezone(timezone.utc)

            # Format the UTC datetime as a string without seconds and with AM/PM
            start_time = utc_datetime_start_utc.strftime('%I:%M %p %Z')
            end_time = utc_datetime_end_utc.strftime('%I:%M %p %Z')

            event = self.event_title or self.event_type

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
                    <h1 class="event-title">MICIP Event Cancelled</h1>
                    <p class="event-details">Hello MICIP member,</p>
                    <p class="event-details">The event - <strong>{{ event }}</strong> scheduled on <strong>{{ event_dates }}</strong> at <strong>{{ start_time }}</strong> to <strong>{{ end_time }} at - <strong>{{ venue }}</strong></strong> has been cancelled.</p>
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
                'event_dates': self.event_dates,
                'start_time': start_time,
                'end_time': end_time,
                'venue': self.venue
            }

            # Update HTML content with variables
            for var, value in variables.items():
                html_content = html_content.replace('{{ ' + var + ' }}', value)

            # Send it to sftp mail service
            SMTPMailer().send_bulk_email(recipient_emails, subject, html_content, True)
            print("CalendarDeleteBGTask Finished processing.")
        except Exception as e:
            print("CalendarDeleteBGTask Exception : ", e)
