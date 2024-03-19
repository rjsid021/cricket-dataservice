from log.log import get_logger
from third_party_service.notification.mail.notifier import Mail
from third_party_service.notification.messages.notifier import TextMessage
from third_party_service.notification.whatsapp.notifier import _Whatsapp

logger = get_logger("notification", "notification")


class Notification:
    def __init__(self, **kwargs):
        """
        Kwargs can have possible keys to initialize for whatsapp, text message or mail.
        :param is_whatsapp:
        :param is_text:
        :param is_mail:
        :param kwargs:
        """
        self.is_whatsapp = kwargs.get("is_whatsapp")
        self.is_text = kwargs.get("is_text")
        self.is_mail = kwargs.get("is_mail")

        # Initialize Whatsapp Instance if passed via constructor
        if self.is_whatsapp:
            self.whatsapp = _Whatsapp()
        # Initialize Text Instance if passed via constructor
        if self.is_text:
            template_id = kwargs.get("text_template_id")
            if not template_id:
                raise Exception("Template id is not provided for message service")
            self.text_message = TextMessage(template_id)
        # Initialize Mail Instance if passed via constructor
        if self.is_mail:
            template_id = kwargs.get("mail_template_id")
            if not template_id:
                raise Exception("Template id is not provided for mail service")
            self.mail = Mail(template_id)

    def send_notification(self, payloads):
        if self.is_text:
            self.text_message.send_notification(payloads["text_payload"])
        if self.is_whatsapp:
            self.whatsapp.send_notification(payloads["whatsapp_payload"])
        if self.is_mail:
            self.mail.send_notification(payloads["mail_payload"])

    def send_bulk_notification(self, payloads):
        if self.is_text:
            self.text_message.send_bulk_notification(payloads["text_payload"])
        if self.is_whatsapp:
            self.whatsapp.send_bulk_notification(payloads["whatsapp_payload"])
        if self.is_mail:
            self.mail.send_bulk_notification(payloads["mail_payload"])
