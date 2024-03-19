from common.dao.insert_data import truncateTable
from common.dao_client import session
from common.db_config import DB_NAME

db_name = DB_NAME

tables_list = ['Teams', 'Players', 'Venue', 'Matches', 'MatchBattingCard', 'MatchExtras', 'MatchBowlingCard',
               'MatchPartnership', 'MatchBallSummary', 'BatsmanBowlerMatchStatistics', 'BatsmanGlobalStats', 'BowlerGlobalStats',
               'fileLogs', 'contributionScore', 'fitnessGPSData', 'fitnessGPSBallData']

if __name__ == '__main__':

    for table in tables_list:
        truncateTable(session, db_name, table)
