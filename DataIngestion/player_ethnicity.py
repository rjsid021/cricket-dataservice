from DataIngestion.config import PLAYER_MAPPER_TABLE_NAME
from DataIngestion.pressure_index.extract_db_data import getPandasFactoryDF
from common.dao_client import session


class PlayerEthnicity:
    @classmethod
    def player_country(cls):
        return getPandasFactoryDF(
            session,
            f"select id as player_mapper_id, country from {PLAYER_MAPPER_TABLE_NAME}"
        )
