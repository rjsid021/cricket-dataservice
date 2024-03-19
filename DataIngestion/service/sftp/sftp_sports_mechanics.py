import errno
import os
import os.path
import re
import shutil
from datetime import date, timedelta
from os import listdir
from stat import S_ISDIR, S_ISREG

from DataIngestion import config
from DataIngestion.config import ROOT_DATA_PATH, SQUAD_ROOT_PATH
from DataIngestion.service.sftp.sftp_services import SftpService
from DataIngestion.utils.helper import readJsFile
from log.log import get_logger

logger = get_logger("SFTPDataIngestion", "SFTPDataIngestion")
interval = 0.05
yesterday = date.today() - timedelta(days=1)
yesterday = yesterday.strftime("%d%m%Y")
logger.info(f"Running for date --> {yesterday}")
from common.utils.helper import getEnvVariables


class SportsMechanicsSftpService(SftpService):
    def __init__(self):
        super().__init__()
        self.SFTP_DOWNLOAD_PATH = "sports_mechanics_downloads"
        self.host = getEnvVariables("SFTP_SM_HOST")
        self.username = getEnvVariables("SFTP_SM_USERNAME")
        self.password = getEnvVariables("SFTP_SM_PASSWORD")
        self.port = getEnvVariables("SFTP_SM_PORT")

    def get_numbers_from_filename(self, filename):
        return re.search(r'\d+', filename).group(0)

    def mkdir_match_season(self, path):
        try:
            os.makedirs(path)
            os.chmod(path, 0o777)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    def download_squad_file(self, sftp_connection, remotedir, localdir, squad_prefix, preserve_mtime=False):
        squad_file_present = squad_prefix + "-squad.js" in sftp_connection.listdir(remotedir)
        if not squad_file_present:
            raise Exception(f"Squad file missing for {localdir} {squad_prefix}")
        for entry in sftp_connection.listdir(remotedir):
            remotepath = remotedir + "/" + entry
            localpath = os.path.join(localdir, entry)
            mode = sftp_connection.stat(remotepath).st_mode
            if S_ISDIR(mode):
                self.download_squad_file(sftp_connection, remotepath, localdir, squad_prefix, preserve_mtime)
            elif S_ISREG(mode) and entry.split('-')[0] == squad_prefix:
                sftp_connection.get(remotepath, localpath, preserve_mtime=preserve_mtime)

    def download_match_file(self, sftp_connection, remote_dir, local_dir, remote_rel_path, squad_path,
                            preserve_mtime=False):
        squad_processed = False
        for entry in sftp_connection.listdir(remote_dir):
            if entry == "competition.js":
                continue
            allowed_suffix = ["innings1.js", "Innings1.js", "innings2.js", "Innings2.js", "matchsummary.js",
                              "Matchsummary.js"]
            if not squad_processed and entry.split('-')[1] in allowed_suffix:
                self.download_squad_file(
                    sftp_connection,
                    remote_rel_path + '/' + squad_path,
                    local_dir,
                    entry.split('-')[0],
                    preserve_mtime=False
                )
                squad_processed = True
            remotepath = remote_dir + "/" + entry
            localpath = os.path.join(local_dir, entry)
            mode = sftp_connection.stat(remotepath).st_mode
            if S_ISDIR(mode):
                try:
                    os.mkdir(localpath)
                except OSError:
                    pass
                self.download_match_file(sftp_connection, remotepath, localpath, preserve_mtime)
            elif S_ISREG(mode):
                sftp_connection.get(remotepath, localpath, preserve_mtime=preserve_mtime)

    def download_files(
            self,
            destination_path,
            temp_path,
            ROOT_DATA_PATH,
            sftp_connection,
            remote_path,
            remote_tournament_dir,
            squad_path,
            tournament,
            season
    ):
        # Make dir if doesn't exist where the file will be stored permanently
        self.mkdir_match_season(destination_path)

        local_dir = ROOT_DATA_PATH + temp_path
        try:
            os.chdir(ROOT_DATA_PATH)
            self.mkdir_match_season(local_dir)
        except OSError:
            pass

        remote_rel_path = f"{remote_path}/{remote_tournament_dir}"
        # Read sftp_scheduler_table.json file
        import json

        # Open and read the JSON file
        with open(config.SFTP_INGESTION_CACHE, 'r') as json_file:
            sftp_ingestion_cache = json.load(json_file)

        sftp_file_dirs: list = sftp_connection.listdir(remote_rel_path)
        unprocessed_matches = [
            item for item in sftp_file_dirs if item not in sftp_ingestion_cache[f'{tournament} {season}']
        ]
        for file in unprocessed_matches:
            if file[-4:] == '.zip':
                continue
            self.mkdir_match_season(local_dir + "/" + file)
            self.download_match_file(
                sftp_connection,
                remote_rel_path + '/' + file,
                local_dir + "/" + file,
                remote_rel_path,
                squad_path,
                preserve_mtime=False
            )
        # update unprocessed matches
        sftp_ingestion_cache[f'{tournament} {season}'] = unprocessed_matches + sftp_ingestion_cache[
            f'{tournament} {season}']
        with open(config.SFTP_INGESTION_CACHE, 'w') as json_file:
            json.dump(sftp_ingestion_cache, json_file, indent=2)  # indent parameter is optional for pretty formatting

    def moveFiles(self, basePath, destination_path, squad_path):
        logger.info("Moving Files")
        try:
            for basefold in listdir(basePath):
                if basefold != ".DS_Store":
                    for filename in listdir(basePath + '/' + basefold):
                        if filename not in [".DS_Store", "competition.js"]:
                            logger.info(f"filename --> {filename}")
                            if filename.split('-')[1].split('.')[0] in ['matchsummary', 'Innings2', 'Innings1']:
                                shutil.move(basePath + '/' + basefold + '/' + filename,
                                            os.path.join(destination_path, filename))
                                logger.info(f"File Moved: {os.path.join(destination_path, filename)}")
                            elif filename.split('-')[1].split('.')[0] in ['squad']:
                                if os.path.exists(squad_path + basefold):
                                    shutil.rmtree(squad_path + basefold)
                                squad_match_dir = squad_path + basefold
                                os.makedirs(squad_match_dir)
                                os.chmod(squad_match_dir, 0o777)
                                shutil.move(basePath + '/' + basefold + '/' + filename,
                                            os.path.join(squad_match_dir, filename))
                                logger.info(f"File Moved: {os.path.join(squad_match_dir, filename)}")
                            elif filename.split('-')[1].split('.')[0] in ['matchSchedule']:
                                if os.path.exists(os.path.join(destination_path, filename)):
                                    src_file_len = len(readJsFile(os.path.join(destination_path, filename))['Result'])
                                    current_file_len = len(
                                        readJsFile(os.path.join(basePath + '/' + basefold, filename))['Result'])
                                    if current_file_len > src_file_len:
                                        shutil.move(basePath + '/' + basefold + '/' + filename,
                                                    os.path.join(destination_path, filename))
                                        logger.info(f"File Moved: {os.path.join(destination_path, filename)}")
                                    else:
                                        pass
                                else:
                                    shutil.move(basePath + '/' + basefold + '/' + filename,
                                                os.path.join(destination_path, filename))
        except Exception as err:
            logger.error(err)

    def get_match_files(
            self,
            temp_path,
            ROOT_DATA_PATH,
            destination_path,
            SQUAD_ROOT_PATH,
            sftp_connection,
            remote_tournament_dir,
            remote_path,
            squad_path,
            tournament,
            season
    ):
        try:
            # Download file from SFTP Server
            self.download_files(
                destination_path,
                temp_path,
                ROOT_DATA_PATH,
                sftp_connection,
                remote_path,
                remote_tournament_dir,
                squad_path,
                tournament,
                season
            )
            # moving files data folder
            self.moveFiles(ROOT_DATA_PATH + temp_path, destination_path, f"{SQUAD_ROOT_PATH}{tournament} {season}/")
        except Exception as e:
            logger.info(e)

    def add_match_file(self):
        try:
            tournament_info = self.sftp_config['source']['sports mechanics']
            for tournament, tournament_config in tournament_info.items():
                available = tournament_config['available']
                if not available:
                    continue
                remote_tournament_dir = tournament_config['folder']
                remote_path = tournament_config['remote path']
                squad_path = tournament_config['squad path']
                season = tournament_config['season']
                destination_path = os.path.join(ROOT_DATA_PATH, f'{tournament}/{tournament} {season}/')
                sftp_connection = self.sftp_connect(
                    self.host,
                    self.username,
                    self.password,
                    self.port,
                    remote_path
                )
                self.get_match_files(
                    self.SFTP_DOWNLOAD_PATH,
                    ROOT_DATA_PATH,
                    destination_path,
                    SQUAD_ROOT_PATH,
                    sftp_connection,
                    remote_tournament_dir,
                    remote_path,
                    squad_path,
                    tournament,
                    season
                )
                # Finally remove temp dir recursively
                try:
                    shutil.rmtree(f"{ROOT_DATA_PATH}{self.SFTP_DOWNLOAD_PATH}")
                except OSError as e:
                    print(f"Error: {e}")
        except Exception as e:
            logger.info(e)
