import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from DataIngestion import config
from log.log import get_logger

logger = get_logger("bg_task", "smtp mailer")


class SMTPMailer:
    def __init__(self):
        self.sender_password = config.SMTP_PASSWORD
        self.sender_email = config.SMTP_EMAIL
        self.smtp_server = config.SMTP_SERVER_IP
        self.smtp_port = int(config.SMTP_PORT)

    def send_bulk_email(self, recipient_emails, subject, message, is_html=False):
        logger.info("Sending mails to recipient_emails")
        for recipient_email in recipient_emails:
            msg = MIMEMultipart()
            msg['From'] = self.sender_email
            msg['To'] = recipient_email
            msg['Subject'] = subject
            if is_html:
                msg.attach(MIMEText(message, 'html'))
            else:
                msg.attach(MIMEText(message, 'plain'))

            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                # if you want to see the SMTP communication with the server
                # server.set_debuglevel(1)
                try:
                    send_errs = server.send_message(msg)
                    logger.info(f"{recipient_email} : {send_errs}")
                except smtplib.SMTPRecipientsRefused as e:
                    logger.info(f"{recipient_email} : {e}")

