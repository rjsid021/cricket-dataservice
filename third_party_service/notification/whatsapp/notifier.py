import base64
import time

import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.padding import PKCS7

from log.log import get_logger
from third_party_service.notification.whatsapp import config

logger = get_logger("notification", "whatsapp")


class _Whatsapp:
    def __init__(self):
        self.campaign_auth_id = config.CAMPAIGN_AUTH_ID
        self.whatsapp_endpoint = config.WHATSAPP_ENDPOINT
        self.encryption_secret_key = config.CAMPAIGN_ENCRYPTION_SECRET_KEY

    def send_notification(self, payload):
        pass

    def send_bulk_notification(self, payloads):
        def encrypt_auth_token(message):
            encryption_secret_key = bytes(self.encryption_secret_key, "utf-8")
            plaintext = bytes(message, "utf-8")
            padder = PKCS7(128).padder()
            plaintext = padder.update(plaintext) + padder.finalize()
            cipher = Cipher(algorithms.AES(encryption_secret_key), modes.ECB(), backend=default_backend())
            encryptor = cipher.encryptor()
            ciphertext = encryptor.update(plaintext) + encryptor.finalize()
            ciphertext = base64.b64encode(ciphertext).decode("ascii")
            ciphertext = ciphertext + f"|{int(time.time() * 1000)}"
            ciphertext = bytes(ciphertext, "utf-8")
            padder = PKCS7(128).padder()
            plaintext = padder.update(ciphertext) + padder.finalize()
            cipher = Cipher(algorithms.AES(encryption_secret_key), modes.ECB(), backend=default_backend())
            encryptor = cipher.encryptor()
            ciphertext = encryptor.update(plaintext) + encryptor.finalize()
            ciphertext = base64.b64encode(ciphertext).decode("ascii")
            return ciphertext

        for payload in payloads:
            header = {
                "Authorization": encrypt_auth_token(self.campaign_auth_id),
                "Content-Type": "application/json"
            }
            # Send whatsapp message.
            try:
                response = requests.post(self.whatsapp_endpoint, json=payload, headers=header)
                response.raise_for_status()
            except Exception as err:
                logger.error(err)
        return "Sent whatsapp message successfully to all recipient"
