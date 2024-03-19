import datetime

from DataIngestion.config import PLAYER_MAPPER_TABLE_NAME
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao.insert_data import insertToDB
from common.dao_client import session
from common.db_config import DB_NAME

if __name__ == "__main__":
    # get player mapper from cassandra db
    player_mapping_df = getPandasFactoryDF(
        session,
        f"select * from {DB_NAME}.playermapping;"
    )

    players_existing_mapping = {
        "RIGHT ARM FAST MEDIUM": "RIGHT ARM FAST",
        "RIGHT ARM OFFBREAK": "RIGHT ARM OFF SPINNER",
        "RIGHT ARM MEDIUM": "RIGHT ARM FAST",
        "RIGHT ARM MEDIUM FAST": "RIGHT ARM FAST",
        "SLOW LEFT ARM ORTHODOX": "LEFT ARM ORTHODOX",
        "RIGHT ARM FAST": "RIGHT ARM FAST",
        "LEFT ARM FAST MEDIUM": "LEFT ARM FAST",
        "LEFT ARM MEDIUM FAST": "LEFT ARM FAST",
        "LEGBREAK": "RIGHT ARM LEGSPIN",
        "RIGHT ARM SLOW": "RIGHT ARM FAST",
        "LEGBREAK GOOGLY": "RIGHT ARM LEGSPIN",
        "RIGHT ARM BOWLER": "RIGHT ARM FAST",
        "LEFT ARM WRIST SPIN": "LEFT ARM CHINAMAN",
        "LEFT ARM MEDIUM": "LEFT ARM FAST",
        "RIGHT ARM SLOW MEDIUM": "RIGHT ARM FAST ",
        "LEFT ARM SLOW MEDIUM": "LEFT ARM FAST",
        "LEFT ARM SLOW": "LEFT ARM FAST"
    }

    player_mapping_df['bowler_sub_type'] = player_mapping_df['bowler_sub_type'].replace(
        players_existing_mapping
    )
    player_mapping_df['load_timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    player_mapping_df = player_mapping_df.to_dict(orient='records')
    insertToDB(session, player_mapping_df, DB_NAME, PLAYER_MAPPER_TABLE_NAME)
