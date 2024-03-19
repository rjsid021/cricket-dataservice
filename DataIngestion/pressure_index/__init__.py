from DataIngestion import load_timestamp
from DataIngestion.pressure_index.extract_db_data import create_data, getPandasFactoryDF
from common.dao_client import session
from log.log import get_logger


class BybData:
    logger = get_logger("byb_data", "byb_data")

    @staticmethod
    def combine_data(load_all_data):
        BybData.logger.info("Creating Session for BybData")
        data_all = create_data(session, load_timestamp, load_all_data)
        data_all = data_all.rename(
            columns={
                "batsman_src_player_id": "batsman_source_id",
                "non_striker_src_player_id": "non_striker_source_id",
                "bowler_src_player_id": "bowler_source_id",
                "out_batsman_src_player_id": "out_batsman_source_id",
            }
        )
        BybData.logger.info("All leagues data generated.")
        return data_all
