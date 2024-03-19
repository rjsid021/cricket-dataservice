from DataIngestion.utils.helper import readCSV
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session

if __name__ == "__main__":
    players = getPandasFactoryDF(session, "select * from players")
    unique = players['src_player_id'].drop_duplicates()
    unique.to_csv("1__findleft_over.csv")

    sm_df = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/DataIngestion/player_mapper_parser/leftover_sm/failed_players.csv"
    )
    xx = sm_df['PlayerID'].drop_duplicates()
    xx.to_csv("unique_failed_player.csv")