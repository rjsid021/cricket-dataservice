import datetime

from DataIngestion.config import INGESTION_LOG_TABLE_NAME, INGESTION_LOG_KEY_COL
from common.dao.fetch_db_data import getMaxId
from common.dao.insert_data import insertToDB
from common.dao_client import session
from common.db_config import DB_NAME


class NoResultHandler:
    def __init__(self, message, match_id, match_name):
        self.message = message
        self.match_id = match_id
        self.match_name = match_name

    def log_ingestion_table(self):
        insertToDB(session, [{
            'id': int(getMaxId(session, INGESTION_LOG_TABLE_NAME, INGESTION_LOG_KEY_COL, DB_NAME)),
            'match_id': self.match_id,
            'match_name': self.match_name,
            'player_id': [],
            'message': self.message,
            'load_timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }], DB_NAME, INGESTION_LOG_TABLE_NAME)
