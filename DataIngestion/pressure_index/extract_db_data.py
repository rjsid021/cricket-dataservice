import warnings
from datetime import datetime

import numpy as np
import pandas as pd
from tabulate import tabulate

from DataIngestion import MATCHES_TABLE_NAME
from DataIngestion.config import VENUE_TABLE_NAME, PLAYERS_TABLE_NAME, TEAMS_TABLE_NAME, INNINGS_TABLE_NAME
from DataIngestion.pressure_index.byb_utils import create_baseline_df
from DataIngestion.pressure_index.pregame import (
    create_batsman_stats,
    create_bowler_stats,
    create_h2h_stats,
    get_stadium_data,
)
from common.db_config import DB_NAME

warnings.filterwarnings("ignore")


def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


def getPandasFactoryDF(session, select_sql, vals=None):
    # Getting teams DF from db table using pandas_factory
    session.row_factory = pandas_factory
    session.default_fetch_size = None
    if vals is not None:
        res = session.execute(select_sql, vals, timeout=None)
    else:
        res = session.execute(select_sql, timeout=None)
    # session.shutdown()
    return res._current_rows


def getPrettyDF(df):
    return tabulate(df, headers="keys", tablefmt="psql")


def create_data(
        session,
        load_ts,
        load_all_data
):
    # GET_BATCARD_DATA = """select match_id, innings, batting_team_id,
    # batsman_id, out_desc,
    # runs, balls, batting_position,
    # dot_balls, ones, twos, threes, fours,
    # sixes, strike_rate  from cricketsimulatordb_qa.MatchBattingCard
    # where competition_name={} ALLOW FILTERING; """.format(
    #     competition_name
    # )

    # bat_card_data = getPandasFactoryDF(session, GET_BATCARD_DATA)

    get_matches_sql = f"""select match_id, match_name, venue, match_date, winning_team, is_playoff, is_title, team2_target from {DB_NAME}.{MATCHES_TABLE_NAME} where season >= 2021 """
    if not load_all_data:
        get_matches_sql = get_matches_sql + f" and load_timestamp >= '{load_ts}' ALLOW FILTERING"
    else:
        get_matches_sql = get_matches_sql + " ALLOW FILTERING"
    GET_MACTHES_DATA = session.prepare(get_matches_sql)
    matches_data = getPandasFactoryDF(session, GET_MACTHES_DATA)

    GET_VENUE_DATA = f"""select venue_id, stadium_name from {DB_NAME}.{VENUE_TABLE_NAME}"""
    venue_data = getPandasFactoryDF(session, GET_VENUE_DATA)

    players_data_sql = f"""select player_id, player_name, batting_type, 
            bowling_type, bowl_major_type, player_type, src_player_id from 
            {DB_NAME}.{PLAYERS_TABLE_NAME} where season >= 2021 ALLOW FILTERING"""

    players_data = getPandasFactoryDF(session, players_data_sql)
    players_data["player_rank"] = players_data.groupby("player_id")["player_id"].rank(
        method="first", ascending=True
    )
    players_data = players_data[players_data["player_rank"] == 1]
    players_data = players_data.drop_duplicates("player_id")

    GET_TEAMS_DATA = session.prepare(
        f"""select team_id, team_name,team_short_name 
    from {DB_NAME}.{TEAMS_TABLE_NAME} """
    )
    teams_data = getPandasFactoryDF(session, GET_TEAMS_DATA)
    match_ball_summary_sql = f"""select * from {DB_NAME}.{INNINGS_TABLE_NAME} where season >= 2021 """
    if not load_all_data:
        match_ball_summary_sql = match_ball_summary_sql + f" and load_timestamp >= '{load_ts}'  ALLOW FILTERING"
    else:
        match_ball_summary_sql = match_ball_summary_sql + " ALLOW FILTERING"

    GET_BALL_SUMMARY_DATA = session.prepare(match_ball_summary_sql)

    ball_summary_df = getPandasFactoryDF(
        session, GET_BALL_SUMMARY_DATA
    )
    ball_summary_df = ball_summary_df.rename(columns={"against_bowler": "bowler_id"})

    ball_stats = pd.merge(ball_summary_df, matches_data, how="inner", on="match_id")
    ball_stats["target_runs"] = np.where(
        ball_stats["innings"] == 2, ball_stats["team2_target"], None
    )

    ball_stats = pd.merge(
        ball_stats, venue_data, how="inner", left_on="venue", right_on="venue_id"
    ).drop(["venue"], axis=1)
    ball_stats = (
        pd.merge(
            ball_stats,
            players_data[
                [
                    "player_id",
                    "player_name",
                    "batting_type",
                    "player_type",
                    "src_player_id",
                ]
            ],
            how="left",
            left_on="batsman_id",
            right_on="player_id",
        )
        .rename(
            columns={
                "batting_type": "striker_batting_type",
                "player_name": "batsman",
                "player_type": "striker_type",
                "src_player_id": "batsman_src_player_id",
            }
        )
        .drop(["player_id", "team2_target"], axis=1)
    )

    ball_stats = (
        pd.merge(
            ball_stats,
            players_data[
                [
                    "player_id",
                    "player_name",
                    "batting_type",
                    "player_type",
                    "src_player_id",
                ]
            ],
            how="left",
            left_on="non_striker_id",
            right_on="player_id",
        )
        .rename(
            columns={
                "batting_type": "non_striker_batting_type",
                "player_name": "non_striker",
                "player_type": "non_striker_type",
                "src_player_id": "non_striker_src_player_id",
            }
        )
        .drop(["player_id"], axis=1)
    )

    ball_stats = (
        pd.merge(
            ball_stats,
            players_data[
                [
                    "player_id",
                    "player_name",
                    "bowling_type",
                    "bowl_major_type",
                    "player_type",
                    "src_player_id",
                ]
            ],
            how="left",
            left_on="bowler_id",
            right_on="player_id",
        )
        .rename(
            columns={
                "bowling_type": "bowler_sub_type",
                "player_name": "bowler",
                "player_type": "bowler_type",
                "src_player_id": "bowler_src_player_id",
            }
        )
        .drop(["player_id"], axis=1)
    )

    ##Out Batsman
    ball_stats = (
        pd.merge(
            ball_stats,
            players_data[["player_id", "player_name", "src_player_id"]],
            how="left",
            left_on="out_batsman_id",
            right_on="player_id",
        )
        .rename(
            columns={
                "player_name": "out_batsman",
                "src_player_id": "out_batsman_src_player_id",
            }
        )
        .drop(["player_id"], axis=1)
    )

    ##Batsman Team ID
    ball_stats = (
        pd.merge(
            ball_stats,
            teams_data,
            how="left",
            left_on="batsman_team_id",
            right_on="team_id",
        )
        .rename(columns={"team_name": "batting_team"})
        .drop(["team_id"], axis=1)
    )

    ##Bowler Team ID
    ball_stats = (
        pd.merge(
            ball_stats,
            teams_data,
            how="left",
            left_on="bowler_team_id",
            right_on="team_id",
        )
        .rename(columns={"team_name": "bowling_team"})
        .drop(["team_id"], axis=1)
    )

    ball_stats["match_phase"] = ball_stats["batting_phase"].apply(
        lambda x: "POWERPLAY" if x == 1 else "MIDDLEOVERS" if x == 2 else "DEATHOVERS"
    )
    return ball_stats.sort_values(by=["id"])


def generate_and_load_pregame(data_all, load_ts):
    # seasons = [
    #     "2022",
    #     "2022/23",
    #     "2023",
    #     "2021",
    #     "2021/22",
    #     "2020",
    #     "2020/21",
    #     "2019",
    #     "2019/20",
    # ]

    # Convert the string to a datetime object
    load_timestamp = datetime.strptime(load_ts, '%Y-%m-%d %H:%M:%S')
    # Extract the year from the load_timestamp
    load_year = load_timestamp.year
    # Generate a list of years from two years before the load_year till the current year
    seasons = [str(year) for year in range(max(2019, load_year - 2), load_year + 1)]

    # byb_data = pd.read_csv(args.data_dir)
    data_all["season"] = data_all["season"].astype(str)
    byb_data = data_all[(data_all["season"].isin(seasons))]

    byb_data_new = create_baseline_df(byb_data)

    df_stadium_runs = get_stadium_data(byb_data_new, "Data")
    df_batsman_entrypoint, df_batsman_ip = create_batsman_stats(byb_data_new)
    df_bowler_stats = create_bowler_stats(byb_data_new)
    df_h2h_stats, df_h2battype_stats = create_h2h_stats(byb_data_new)

    df_stadium_runs["match_date"] = pd.to_datetime(df_stadium_runs["match_date"])

    df_batsman_entrypoint["match_date"] = pd.to_datetime(
        df_batsman_entrypoint["match_date"]
    )

    df_batsman_ip["match_date"] = pd.to_datetime(df_batsman_ip["match_date"])

    df_bowler_stats["match_date"] = pd.to_datetime(df_bowler_stats["match_date"])

    df_h2h_stats["match_date"] = pd.to_datetime(df_h2h_stats["match_date"])

    df_h2battype_stats["match_date"] = pd.to_datetime(df_h2battype_stats["match_date"])
    return (
        df_stadium_runs,
        df_batsman_entrypoint,
        df_batsman_ip,
        df_bowler_stats,
        df_h2h_stats,
        df_h2battype_stats,
    )
