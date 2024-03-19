import pandas as pd

from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session
from common.db_config import DB_NAME

if __name__ == "__main__":
    players = getPandasFactoryDF(session, f"SELECT * FROM {DB_NAME}.players")

    no_skill = [1, 2]

    for row, index in players.iterrows():
        if index['is_bowler'] == 0 and index['is_batsman'] == 0 and index['is_wicket_keeper'] == 0:
            no_skill.append(index['src_player_id'])
    player_m = getPandasFactoryDF(session, f"SELECT * FROM {DB_NAME}.playermapping")
    player_no_skill = pd.DataFrame(columns=['src_player_id'])
    player_no_skill['src_player_id'] = no_skill
    # merge this with player_source_mapper on
    player_m['id'] = player_m['id'].astype(int)
    xx = pd.merge(
        player_no_skill,
        player_m[['id', 'cricinfo_id']],
        left_on='src_player_id',
        right_on='id',
        how='left'
    )
    xx.to_csv("no_mapping.csv")
