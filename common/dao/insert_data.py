import json

from cassandra.concurrent import execute_concurrent

from log.log import get_logger

logger = get_logger("Ingestion", "Ingestion")


# This function takes in session and data as input and insert the records to the provided db and table
def insertToDB(session, data_list, db_name, table_name, allow_logging=True):
    statements_and_params = []
    for jData in data_list:
        insert_stmt = "insert into {}.{} JSON \'{}\';".format(db_name, table_name, json.dumps(jData))
        statements_and_params.append((insert_stmt, ()))
    if allow_logging:
        logger.info("Insert Started for --> {}".format(table_name))
    execute_concurrent(session, statements_and_params, concurrency=50)
    if allow_logging:
        logger.info("Insert Completed for --> {}".format(table_name))


# this function deletes the records, taking input as list of data
def upsertDatatoDB(session, data_list, db_name, table_name, key_col, allow_logging=True):
    if data_list:
        key_li = tuple(d[key_col] for d in data_list)
        if len(key_li) > 1:
            key_li = str(key_li)
            clause = f"where {key_col} in {key_li}"
        else:
            key_li = key_li[0]
            clause = f"where {key_col} = {key_li}"
        if allow_logging:
            logger.info("Delete Started for --> {}".format(table_name))
        delete_stmt = f"delete from {db_name}.{table_name} {clause};"
        if allow_logging:
            logger.info("Delete Statement --> {}".format(delete_stmt))
        session.execute(delete_stmt)
        if allow_logging:
            logger.info("Delete Completed for --> {}".format(table_name))

        # after deleting updated records, inserting the latest records
        insertToDB(session, data_list, db_name, table_name, allow_logging)


# this function truncates the given table
def truncateTable(session, db_name, table_name):
    logger.info("Truncate started for --> {}.{}".format(db_name, table_name))
    truncate_stmt = "truncate table {}.{}".format(db_name, table_name)
    session.execute(truncate_stmt)
    logger.info("Truncate completed for --> {}.{}".format(db_name, table_name))
