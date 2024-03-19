from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session

if __name__ == '__main__':
    player_mapping = getPandasFactoryDF(session, "select * from playerMapping")
