import os
from azure.storage.blob import BlockBlobService
import base64
from codecs import encode
from common.utils.helper import getEnvVariables
from log.log import get_logger

dir_name = getEnvVariables('USER_QUERY_DIR_NAME')
account_name = getEnvVariables('STORAGE_ACCOUNT_NAME')
account_key = getEnvVariables('STORAGE_ACCOUNT_KEY')
container_name = getEnvVariables('CONTAINER_NAME')


def uploadImageToBlob(image_code, local_image_path, blob_folder_name, image_name):
    logger = get_logger("uploadImageToBlob", "uploadImageToBlob")
    try:
        block_blob_service = BlockBlobService(
            account_name=account_name,
            account_key=account_key
        )
        bytes_img = encode(image_code, 'utf-8')
        binary_img = base64.decodebytes(bytes_img)
        if not os.path.exists(f"{local_image_path}/{blob_folder_name}"):
            os.makedirs(f"{local_image_path}/{blob_folder_name}", exist_ok=True)
        os.chmod(f"{local_image_path}/{blob_folder_name}", 0o777)

        with open(f"{local_image_path}/{blob_folder_name}/{image_name}", "wb") as fh:
           fh.write(binary_img)

        blob_name = f"{dir_name}/{blob_folder_name}/{image_name}"
        block_blob_service.create_blob_from_path(container_name, blob_name, f"{local_image_path}/{blob_folder_name}/{image_name}")
        logger.info("Image(s) Uploaded Successfully!")

    except Exception as e:
        logger.error(e)
