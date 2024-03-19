import datetime

from DataIngestion.config import MATCHES_TABLE_NAME
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session


class ConnCreator:
    db_matches_count = 0

    def get_db_matches_count(con):
        sql = f"SELECT COUNT(*) AS row_count FROM {MATCHES_TABLE_NAME};"
        return getPandasFactoryDF(session, sql).loc[0][0]

    @staticmethod
    def initial_val(path=None, read_only=True):
        from DataService.utils.helper import connection_duckdb
        con = connection_duckdb(path, read_only=read_only)
        ConnCreator.db_matches_count = ConnCreator.get_db_matches_count(con)
        return con


load_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
load_timestamp_utc = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
