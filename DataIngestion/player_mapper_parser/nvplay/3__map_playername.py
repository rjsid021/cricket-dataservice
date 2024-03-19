import hashlib

from DataIngestion.utils.helper import readCSV


def create_short_hash(player_name):
    if type(player_name) != str:
        return ""
    # Create a hash object using SHA-256
    sha256_hash = hashlib.sha256(player_name.encode()).hexdigest()
    return f"nv_{sha256_hash}"


def update_new_players():
    sm_df = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/DataIngestion/player_mapper_parser/nvplay/1__nvplay_player_unique_name_from_datadir.csv"
    )
    sm_df['src_player_id'] = sm_df['player'].apply(lambda x: create_short_hash(x))
    sm_df.to_csv("3__map_playername.csv")


if __name__ == "__main__":
    update_new_players()
