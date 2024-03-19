import os
from datetime import date, timedelta

from DataIngestion.config import FILE_SHARE_PATH
from log.log import get_logger

logger = get_logger("SFTPDataIngestion", "SFTPDataIngestion")


from DataIngestion.service.sftp.sftp_services import SftpService

from common.utils.helper import getEnvVariables


class SportsMechanicsSFTPPlayerIncrementService(SftpService):
    def __init__(self):
        super().__init__()
        self.SFTP_DOWNLOAD_PATH = "sports_mechanics_downloads"
        self.host = getEnvVariables("SFTP_SM_HOST")
        self.username = getEnvVariables("SFTP_SM_USERNAME")
        self.password = getEnvVariables("SFTP_SM_PASSWORD")
        self.port = getEnvVariables("SFTP_SM_PORT")

    def download_files(self):
        try:
            remote_path = self.sftp_config['player_incremental_dump']
            sftp_connection = self.sftp_connect(
                self.host,
                self.username,
                self.password,
                self.port,
                remote_path
            )
            remote_relative_path = "mi2022/Incremental_Player_Dump/"
            player_dump_list_dir = sftp_connection.listdir(remote_relative_path)
            localpath = os.path.join(os.path.join(FILE_SHARE_PATH, "data"), "player_increment_sftp.xlsx")
            for player_dump in player_dump_list_dir:
                sftp_connection.get(remote_relative_path + player_dump, localpath, preserve_mtime=False)
        except Exception as e:
            logger.info(e)