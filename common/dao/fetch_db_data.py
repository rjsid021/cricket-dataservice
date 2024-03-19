from common.utils.helper import pandas_factory
from log.log import get_logger

logger = get_logger("Ingestion", "Ingestion")


# This function takes in DB session and select statement
# and generates the DF of rows using pandas_factory function
def getPandasFactoryDF(session, select_sql, fetch_size=None, is_prepared=False, parameter_list=[]):
    # Getting teams DF from db table using pandas_factory
    session.row_factory = pandas_factory
    session.default_fetch_size = fetch_size
    if is_prepared:
        prepared_query = session.prepare(select_sql)
        res = session.execute(prepared_query, parameter_list)
    else:
        res = session.execute(select_sql, timeout=None)
    return res._current_rows


# This function fetches the max primary key value from the input DB table
def getMaxId(session, table_name, id_col, db_name, allow_logging=True):
    select_sql = "SELECT max({}) as max FROM {}.{};".format(id_col, db_name, table_name)
    rows = getPandasFactoryDF(session, select_sql)
    if rows["max"].iloc[0] is None:
        if allow_logging:
            logger.info("Max id for table={} is 1".format(table_name))
        return 1
    else:
        max_val = rows["max"].iloc[0] + 1
        if allow_logging:
            logger.info("Max id for table={} is {}".format(table_name, max_val))
        return max_val


# this function takes input as db session and SQL select statement
# and returns the data list of the selected column
def getAlreadyExistingValue(session, select_sql):
    rows = getPandasFactoryDF(session, select_sql)
    data_list = [row for row in rows.iloc[:, 0]]
    return data_list


