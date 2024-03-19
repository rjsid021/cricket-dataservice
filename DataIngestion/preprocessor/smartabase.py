import sys
sys.path.append("./../../")
sys.path.append("./")
from log.log import get_logger
from DataIngestion.utils.helper import generateSeq
from DataIngestion.query import GET_READINESS_MAX_SYNC_TIME, GET_PLAYER_MAPPER_DETAILS_SQL, \
    GET_AVAILABILITY_MAX_SYNC_TIME
from DataIngestion.sources.smartabase.config import SMARTABASE_USER, SMARTABASE_PASSWORD, SMARTABASE_APP, LOGIN_API, \
    SMARTABASE_USERS_URL, SMARTABASE_DATA_SYNC_URL, READINESS_TO_PERFORM_COLUMN_MAPPING, READINESS_TO_PERFORM_TABLE, \
    SMARTABASE_LOGOUT_URL, MI_AVAILABILITY_COLUMN_MAPPING, MI_AVAILABILITY_TABLE, MI_AVAILABILITY_TEXT_COLS, \
    READINESS_TO_PERFORM_TEXT_COLS, MI_AVAILABILITY_FORM_NAME, READINESS_TO_PERFORM_FORM_NAME
from DataIngestion.sources.smartabase.smartabase import Smartabase
from common.dao.fetch_db_data import getPandasFactoryDF, getMaxId
from common.dao.insert_data import insertToDB
from common.dao_client import session
import datetime
from common.db_config import DB_NAME

logger = get_logger("Smartabase Ingestion", "Smartabase Ingestion")


def generate_smartabase_form_data(group_name, form_name, special_char_col_list, load_timestamp, form_col_mapping,
                                  form_table_max_query, table_name):
    smartabase = Smartabase(SMARTABASE_USER, SMARTABASE_PASSWORD, SMARTABASE_APP)
    session_id = smartabase.login(LOGIN_API)
    player_id_list = smartabase.get_player_ids(session_id, SMARTABASE_USERS_URL, group_name)

    max_sync_timestamp = getPandasFactoryDF(session, form_table_max_query).iloc[0, 0]
    if max_sync_timestamp:
        max_sync_timestamp = int(max_sync_timestamp)
    else:
        max_sync_timestamp = int(0)

    data = smartabase.fetch_data(SMARTABASE_DATA_SYNC_URL, session_id, form_name, player_id_list,
                                 last_sync_time=max_sync_timestamp)

    smartabase.logout(session_id, SMARTABASE_LOGOUT_URL)

    col_list = [key for key, value in form_col_mapping.items()]
    data_df = smartabase.flatten_data(data, col_list)

    if len(data_df) > 0:
        logger.info(f"Data Generated for the form --> {form_name} for group --> {group_name}!!!")
        data_df = data_df.rename(columns=form_col_mapping)

        player_mapping_df = getPandasFactoryDF(session, GET_PLAYER_MAPPER_DETAILS_SQL)
        data_df = data_df.merge(player_mapping_df, on="smartabase_id", how="left")
        data_df['player_name'] = data_df['player_name'].fillna(data_df['first_name'] + " " + data_df['last_name'])
        data_df['src_player_id'] = data_df['src_player_id'].fillna(-1).astype(int)
        data_df['load_timestamp'] = load_timestamp

        max_key_val = getMaxId(session, table_name, "id", DB_NAME, False)

        data_df = generateSeq(data_df.sort_values(['event_id']), "id", max_key_val)

        for col in special_char_col_list:
            data_df[col] = data_df[col].fillna("")
            data_df[col] = data_df[col].astype(str).apply(lambda x: x.replace("'", "''"))

        return data_df.drop(["first_name", "last_name"], axis=1).to_dict(orient="records")
    else:
        logger.info(f"No New Data Available for the form --> {form_name} for group --> {group_name}!!!")
        return None


if __name__ == "__main__":
    # groups = ["ILT20", "WPL Squad", "SA20", "IPL Squad", "MLC Squad", "UK Tour (Mens) 2023"]
    load_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    group_name = "All Teams"

    readiness_data = generate_smartabase_form_data(group_name, READINESS_TO_PERFORM_FORM_NAME,
                                                   READINESS_TO_PERFORM_TEXT_COLS, load_timestamp,
                                                   READINESS_TO_PERFORM_COLUMN_MAPPING, GET_READINESS_MAX_SYNC_TIME,
                                                   READINESS_TO_PERFORM_TABLE)

    if readiness_data:
        insertToDB(session, readiness_data, DB_NAME, READINESS_TO_PERFORM_TABLE)

    availability_data = generate_smartabase_form_data(group_name, MI_AVAILABILITY_FORM_NAME,
                                                      MI_AVAILABILITY_TEXT_COLS, load_timestamp,
                                                      MI_AVAILABILITY_COLUMN_MAPPING,
                                                      GET_AVAILABILITY_MAX_SYNC_TIME, MI_AVAILABILITY_TABLE)

    if availability_data:
        insertToDB(session, availability_data, DB_NAME, MI_AVAILABILITY_TABLE)
