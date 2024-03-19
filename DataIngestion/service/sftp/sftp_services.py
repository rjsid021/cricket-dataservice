import pysftp
import yaml

from DataIngestion import config
from log.log import get_logger

logger = get_logger("SFTPDataIngestion", "SFTPDataIngestion")


class SftpService:
    def __init__(self):
        with open(config.SFTP_CONFIG_PATH, 'r') as yaml_file:
            self.sftp_config = yaml.safe_load(yaml_file)

    def sftp_connect(self, sftp_host, sftp_username, sftp_password, sftp_port, cn_opts=None):
        cn_opts = pysftp.CnOpts()
        cn_opts.hostkeys = None
        return pysftp.Connection(
            host=sftp_host,
            username=sftp_username,
            password=sftp_password,
            cnopts=cn_opts,
            port=int(sftp_port)
        )
