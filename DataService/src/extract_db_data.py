from cassandra.cluster import Cluster
import pandas as pd
from tabulate import tabulate
import numpy as np
from cassandra.auth import PlainTextAuthProvider

# HOST = '127.0.0.1'
HOST = "10.168.134.170"
PORT = 9042
DB_NAME = "cricketsimulatordb"
auth_provider = PlainTextAuthProvider(username="dev_admin", password="tekcircudtev$sal")


def createCassandraSession(host_name, port_name, db):
    cluster = Cluster([host_name], port_name, auth_provider=auth_provider)
    connect = cluster.connect(db, wait_for_all_pools=True)
    return connect


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
    competition_name,
    session,
):
    # GET_BATCARD_DATA = """select match_id, innings, batting_team_id,
    # batsman_id, out_desc,
    # runs, balls, batting_position,
    # dot_balls, ones, twos, threes, fours,
    # sixes, strike_rate  from CricketSimulatorDB.MatchBattingCard
    # where competition_name={} ALLOW FILTERING; """.format(
    #     competition_name
    # )

    # bat_card_data = getPandasFactoryDF(session, GET_BATCARD_DATA)

    GET_MACTHES_DATA = session.prepare(
        """select match_id, match_name, venue, 
    match_date, winning_team,
    is_playoff, is_title, team2_target from 
    CricketSimulatorDB.Matches where competition_name=? ALLOW FILTERING; """
    )

    matches_data = getPandasFactoryDF(session, GET_MACTHES_DATA, [competition_name])

    if competition_name == "IPL":
        if 1076 in matches_data["match_id"].values:
            print("1076 - mathes data")

    GET_VENUE_DATA = (
        """select venue_id, stadium_name from CricketSimulatorDB.Venue"""
    )
    venue_data = getPandasFactoryDF(session, GET_VENUE_DATA)

    GET_PLAYERS_DATA = session.prepare(
        """select player_id, player_name, batting_type, 
    bowling_type, bowl_major_type, player_type, src_player_id from 
    CricketSimulatorDB.Players ALLOW FILTERING; """
    )

    players_data = getPandasFactoryDF(session, GET_PLAYERS_DATA)
    players_data["player_rank"] = players_data.groupby("player_id")["player_id"].rank(
        method="first", ascending=True
    )
    players_data = players_data[players_data["player_rank"] == 1]
    players_data = players_data.drop_duplicates("player_id")

    GET_TEAMS_DATA = session.prepare(
        """select team_id, team_name,team_short_name 
    from CricketSimulatorDB.Teams where competition_name=? ALLOW FILTERING;"""
    )
    teams_data = getPandasFactoryDF(session, GET_TEAMS_DATA, [competition_name])

    GET_BALL_SUMMARY_DATA = session.prepare(
        """select * from 
    CricketSimulatorDB.MatchBallSummary 
    where competition_name=? ALLOW FILTERING;"""
    )

    ball_summary_df = getPandasFactoryDF(
        session, GET_BALL_SUMMARY_DATA, [competition_name]
    )
    ball_summary_df = ball_summary_df.rename(columns={"against_bowler": "bowler_id"})

    if competition_name == "IPL":
        if 1076 in ball_summary_df["match_id"].values:
            print("1076 - ball_summary")

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


def main():
    print("Creating Session ...")
    session = createCassandraSession(HOST, PORT, DB_NAME)

    # print("Getting CPL Data ...")
    # data_cpl = create_data("CPL", session)
    # print("Getting BBL Data ...")
    # data_bbl = create_data("BBL", session)
    # print("Getting IPL Data ...")
    # data_ipl = create_data("IPL", session)

    print("Getting SA20 Data ...")
    data_sa20 = create_data("SA20", session)

    data_all = data_sa20
    data_all = data_all.rename(
        columns={
            "batsman_src_player_id": "batsman_source_id",
            "non_striker_src_player_id": "non_striker_source_id",
            "bowler_src_player_id": "bowler_source_id",
            "out_batsman_src_player_id": "out_batsman_source_id",
        }
    )

    data_all.to_csv("BallByBallData_ALL_new.csv", index=False)
    # data_all.to_csv(
    #     "BallByBallData_ALL.csv", index=False
    # )

    print("Getting Teams Data ...")
    GET_TEAMS = """select * from CricketSimulatorDB.Teams ALLOW FILTERING;"""
    Teams = getPandasFactoryDF(session, GET_TEAMS)
    Teams.to_csv("Teams.csv", index=False)

    print("Getting Players Data ...")
    GET_PLAYERS = """select * from CricketSimulatorDB.Players ALLOW FILTERING;"""
    Players = getPandasFactoryDF(session, GET_PLAYERS)
    Players.to_csv("Players.csv", index=False)

    print("Getting Venue Data ...")
    GET_VENUE = """select * from CricketSimulatorDB.Venue ALLOW FILTERING;"""
    Venue = getPandasFactoryDF(session, GET_VENUE)
    Venue.to_csv("Venue.csv", index=False)

    print("Getting Matches Data ...")
    GET_MATECHES = """select * from CricketSimulatorDB.Matches ALLOW FILTERING;"""
    Matches = getPandasFactoryDF(session, GET_MATECHES)
    Matches.to_csv("Matches.csv", index=False)

    print("Getting Ball Summary Data ...")
    GET_MBS = (
        """select * from CricketSimulatorDB.MatchBallSummary where competition_name='SA20' and season=2024 ALLOW FILTERING;"""
    )
    MatchBallSummary = getPandasFactoryDF(session, GET_MBS)
    MatchBallSummary.to_csv("MatchBallSummary.csv", index=False)


# ball_stats.sort_values(by=["id"]).to_csv("BallByBallData_CPL.csv", index=False)

if __name__ == "__main__":
    main()
