import sys
sys.path.append("./../../")
sys.path.append("./")
from log.log import get_logger
import pandasql as psql
from common.db_config import DB_NAME
from DataIngestion.utils.helper import generateSeq
from common.dao.fetch_db_data import getPandasFactoryDF, getMaxId
from DataIngestion.config import MATCH_STATS_TABLE_NAME, MATCH_STATS_KEY_COL
from DataIngestion.query import GET_MATCH_DATA_SQL, GET_BAT_BOWL_STATS_TIMESTAMP

logger = get_logger("Ingestion", "Ingestion")


def getBatsmanBowlerStats(session, load_timestamp):
    logger.info("Batsman Bowler Stats Data Generation Started!")
    # Getting max match_id from target table
    max_key_val = getMaxId(session, MATCH_STATS_TABLE_NAME, MATCH_STATS_KEY_COL, DB_NAME, False)

    max_timestamp = getPandasFactoryDF(session, GET_BAT_BOWL_STATS_TIMESTAMP).iloc[0, 0]

    match_data_df = getPandasFactoryDF(session, GET_MATCH_DATA_SQL)

    if max_timestamp:
        match_data_df = match_data_df[match_data_df['load_timestamp'] > max_timestamp]
    else:
        match_data_df = match_data_df[match_data_df['load_timestamp'] > '01-01-2000 00:00:00']

    final_stats_df = psql.sqldf('''select match_id, batsman_id, innings, batting_position, batting_phase,against_bowler,  
season, competition_name, sum(ball_runs) as runs_scored,sum(is_four) as num_fours, sum(is_six) as num_sixes, 
sum(is_one) as num_singles, sum(is_two) as num_doubles,sum(is_three) as num_triples, sum(is_dot_ball) as num_dotballs, 
sum(extras) as num_extras,sum(is_wicket) as wicket, wicket_type as dismissal_type from match_data_df 
group by match_id, batsman_id, innings, batting_position, batting_phase, against_bowler, wicket_type, season, 
competition_name''')

    final_stats_df['load_timestamp'] = load_timestamp

    final_batsman_bowler_stats = generateSeq(final_stats_df.sort_values(['match_id', 'innings', 'batting_phase']),
                                             MATCH_STATS_KEY_COL, max_key_val).to_dict(orient='records')

    logger.info("Batsman Bowler Stats Data Generation Completed!")

    return final_batsman_bowler_stats
