from common.utils.helper import getEnvVariables
from log.log import get_logger

logger = get_logger("third_party_service", "blob_storage")

from azure.storage.blob import BlockBlobService


class MicipBlobStorage:
    def __init__(self, **kwargs):
        self.account_name = kwargs.get("storage_account_name") or getEnvVariables('PRIVATE_STORAGE_ACCOUNT_NAME')
        self.account_key = kwargs.get("storage_account_key") or getEnvVariables('PRIVATE_STORAGE_ACCOUNT_KEY')
        self.container_name = kwargs.get("container_name") or getEnvVariables('PRIVATE_CONTAINER_NAME')

        self.block_blob_service = BlockBlobService(
            account_name=self.account_name,
            account_key=self.account_key
        )

    def get_blob(self, remote_dir_name, local_dir_path):
        try:
            self.block_blob_service.get_blob_to_path(self.container_name, remote_dir_name, local_dir_path)
        except Exception as err:
            logger.error(err)

    def update_blob(self, remote_dir_name, local_dir_name):
        try:
            self.block_blob_service.create_blob_from_path(self.container_name, remote_dir_name, local_dir_name)
        except Exception as err:
            logger.error(err)
