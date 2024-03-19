import sys

import numpy as np
import pandas as pd

sys.path.append("./../../")
sys.path.append("./")
from DataIngestion.query import (
    GET_MATCH_DATA_SQL,
    GET_MATCH_SUMMARY,
    GET_TEAM_SQL,
    GET_PLAYERS_SQL,
    GET_CS_TIMESTAMP,
)
from DataIngestion.utils.helper import readCSV, columnCheck
from DataService.fetch_sql_queries import GET_VENUE_DATA
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session
from log.log import get_logger
from DataIngestion.config import retained_list, in_auction

pd.options.mode.chained_assignment = None
logger = get_logger("Contribution_Score", "Contribution_Score")


def getMatchesData(session, get_ball_sql, get_match_sql):
    max_timestamp = getPandasFactoryDF(session, GET_CS_TIMESTAMP).iloc[0, 0]

    ball_data = getPandasFactoryDF(session, get_ball_sql)
    matches_data = getPandasFactoryDF(session, get_match_sql)

    if max_timestamp:
        ball_data = ball_data[ball_data["load_timestamp"] > max_timestamp]
        matches_data = matches_data[matches_data["load_timestamp"] > max_timestamp]
    else:
        ball_data = ball_data[ball_data["load_timestamp"] > "01-01-2000 00:00:00"]
        matches_data = matches_data[
            matches_data["load_timestamp"] > "01-01-2000 00:00:00"
        ]

    matches_data = matches_data[
        ["match_id", "match_date", "is_playoff", "match_name", "venue", "winning_team"]
    ]
    ball_data = pd.merge(ball_data, matches_data, how="left", on="match_id")
    if not ball_data.empty:
        teams_df = getPandasFactoryDF(session, GET_TEAM_SQL)[
            ["team_id", "team_name", "team_short_name"]
        ]

        ball_data = (
            pd.merge(
                ball_data,
                teams_df[["team_id", "team_name", "team_short_name"]],
                how="left",
                left_on="batsman_team_id",
                right_on="team_id",
            )
            .rename(
                columns={
                    "team_name": "batting_team",
                    "team_short_name": "bat_team_short_name",
                }
            )
            .drop(["team_id"], axis=1)
        )

        ball_data = (
            pd.merge(
                ball_data,
                teams_df[["team_id", "team_name", "team_short_name"]],
                how="left",
                left_on="bowler_team_id",
                right_on="team_id",
            )
            .rename(
                columns={
                    "team_name": "bowling_team",
                    "team_short_name": "bowl_team_short_name",
                }
            )
            .drop(["team_id"], axis=1)
        )

        players_df = getPandasFactoryDF(session, GET_PLAYERS_SQL)
        players_df["player_rank"] = players_df.groupby("player_id")["player_id"].rank(
            method="first", ascending=True
        )
        players_df = players_df[players_df["player_rank"] == 1]

        ball_data = (
            pd.merge(
                ball_data,
                players_df[["player_name", "player_id", "player_type", "batting_type"]],
                left_on="batsman_id",
                right_on="player_id",
                how="left",
            )
            .rename(columns={"player_name": "batsman"})
            .drop(["player_id"], axis=1)
        )

        ball_data = (
            pd.merge(
                ball_data,
                players_df[["player_name", "player_id", "bowling_type", "player_type"]],
                left_on="against_bowler",
                right_on="player_id",
                how="left",
            )
            .rename(
                columns={
                    "player_name": "bowler",
                    "player_type_x": "player_type",
                    "player_type_y": "Bowler_type",
                    "against_bowler": "bowler_id",
                }
            )
            .drop(["player_id"], axis=1)
        )

        venue_data = getPandasFactoryDF(session, GET_VENUE_DATA)
        ball_data = pd.merge(
            ball_data, venue_data, how="inner", left_on="venue", right_on="venue_id"
        ).drop(["venue"], axis=1)
        return ball_data
    else:
        return pd.DataFrame()


def change_sr_to_float(x):
    try:
        x = float(x)
    except:
        x = float(0)
    return x


eps = 1e-8


def get_batsman_contribution_score(row):
    RFAB = row["Overall Runs"] - (
        row["Overall Balls"] * (row["Overall Strike rate"] / 100)
    )
    BFAR = (row["Overall Runs"] / ((row["Overall Strike rate"] / 100) + eps)) - row[
        "Overall Balls"
    ]
    BPPE = abs(row["Overall Balls"] - row["Overall Avg Ball"]) / (
        row["Overall Avg Ball"] + eps
    )
    RSPE = abs(row["Overall Runs"] - row["Overall Avg Score"]) / (
        row["Overall Avg Score"] + eps
    )
    ES = (BPPE + RSPE) / (((BPPE / (BFAR + eps)) + (RSPE / (RFAB + eps))) + eps)
    score = row["Overall Runs"] + ES
    return score


def get_contribution_score_power_play(row):
    if row["Actual PowerPlay Over Balls"] < row["Powerplay Min ball"]:
        return "NA"
    RFAB = row["Actual PowerPlay Over Runs"] - (
        row["Actual PowerPlay Over Balls"] * (row["Powerplay Strike rate"] / 100)
    )
    BFAR = (
        row["Actual PowerPlay Over Runs"] / ((row["Powerplay Strike rate"] / 100) + eps)
    ) - row["Actual PowerPlay Over Balls"]
    BPPE = abs(row["Actual PowerPlay Over Balls"] - row["Powerplay Avg Ball"]) / (
        row["Powerplay Avg Ball"] + eps
    )
    RSPE = abs(row["Actual PowerPlay Over Runs"] - row["Powerplay Avg Score"]) / (
        row["Powerplay Avg Score"] + eps
    )
    ES = (BPPE + RSPE) / (((BPPE / (BFAR + eps)) + (RSPE / (RFAB + eps))) + eps)
    score = row["Actual PowerPlay Over Runs"] + ES
    return score


def get_contribution_score_7_10_overs(row):
    if row["Actual 7_10 Over Balls"] < row["7_10 Overs Min ball"]:
        return "NA"
    RFAB = row["Actual 7_10 Over Runs"] - (
        row["Actual 7_10 Over Balls"] * (row["7_10 Overs Strike rate"] / 100)
    )
    BFAR = (
        row["Actual 7_10 Over Runs"] / ((row["7_10 Overs Strike rate"] / 100) + eps)
    ) - row["Actual 7_10 Over Balls"]
    BPPE = abs(row["Actual 7_10 Over Balls"] - row["7_10 Overs Avg Ball"]) / (
        row["7_10 Overs Avg Ball"] + eps
    )
    RSPE = abs(row["Actual 7_10 Over Runs"] - row["7_10 Overs Avg Score"]) / (
        row["7_10 Overs Avg Score"] + eps
    )
    ES = (BPPE + RSPE) / (((BPPE / (BFAR + eps)) + (RSPE / (RFAB + eps))) + eps)
    score = row["Actual 7_10 Over Runs"] + ES
    return score


def get_contribution_score_11_15_overs(row):
    if row["Actual 11_15 Over Balls"] < row["11_15 Overs Min ball"]:
        return "NA"
    RFAB = row["Actual 11_15 Over Runs"] - (
        row["Actual 11_15 Over Balls"] * (row["11_15 Overs Strike rate"] / 100)
    )
    BFAR = (
        row["Actual 11_15 Over Runs"] / ((row["11_15 Overs Strike rate"] / 100) + eps)
    ) - row["Actual 11_15 Over Balls"]
    BPPE = abs(row["Actual 11_15 Over Balls"] - row["11_15 Overs Avg Ball"]) / (
        row["11_15 Overs Avg Ball"] + eps
    )
    RSPE = abs(row["Actual 11_15 Over Runs"] - row["11_15 Overs Avg Score"]) / (
        row["11_15 Overs Avg Score"] + eps
    )
    ES = (BPPE + RSPE) / (((BPPE / (BFAR + eps)) + (RSPE / (RFAB + eps))) + eps)
    score = row["Actual 11_15 Over Runs"] + ES
    return score


def get_contribution_score_death_overs(row):
    if row["Actual Death Over Balls"] < row["DeathOvers Min ball"]:
        return "NA"
    RFAB = row["Actual Death Over Runs"] - (
        row["Actual Death Over Balls"] * (row["DeathOvers Strike rate"] / 100)
    )
    BFAR = (
        row["Actual Death Over Runs"] / ((row["DeathOvers Strike rate"] / 100) + eps)
    ) - row["Actual Death Over Balls"]
    BPPE = abs(row["Actual Death Over Balls"] - row["DeathOvers Avg Ball"]) / (
        row["DeathOvers Avg Ball"] + eps
    )
    RSPE = abs(row["Actual Death Over Runs"] - row["DeathOvers Avg Score"]) / (
        row["DeathOvers Avg Score"] + eps
    )
    ES = (BPPE + RSPE) / (((BPPE / (BFAR + eps)) + (RSPE / (RFAB + eps))) + eps)
    score = row["Actual Death Over Runs"] + ES
    return score


def batting_contribution_score(constraints_file, matches_data):
    data = matches_data.drop("runs", axis=1).rename(columns={"ball_runs": "runs"})
    data["non_striker_id"] = data["non_striker_id"].astype("Int64")
    data["out_batsman_id"] = data["out_batsman_id"].astype("Int64")
    data_new = data.copy()
    data = data.rename(
        columns={
            "match_name": "Game Id",
            "innings": "Innings ID",
            "runs": "Overall Run Outcome in ball",
            "ball_number": "Ball Number",
            "is_wide": "Extra Type is WIDE",
            "is_no_ball": "Extra Type is NOBALL",
            "player_type": "Player_Type",
            "batting_position": "Position",
            "is_four": "Boundary Outcome is Four",
            "is_six": "Boundary Outcome is Six",
        }
    )

    data["Match Phase"] = np.where(
        (data["over_number"] >= 1) & (data["over_number"] <= 6),
        "BATTING_PHASE_POWER_PLAY",
        np.where(
            (data["over_number"] >= 7) & (data["over_number"] <= 10),
            "BATTING_PHASE_7_10",
            np.where(
                (data["over_number"] >= 11) & (data["over_number"] <= 15),
                "BATTING_PHASE_11_15",
                "BATTING_PHASE_DEATH_OVERS",
            ),
        ),
    )

    data["Team"] = data["batting_team"]
    data["is_won"] = np.where(data["batsman_team_id"] == data["winning_team"], 1, 0)
    data["batsman"] = data["batsman"].str.upper()
    data = data[(data["Extra Type is WIDE"] == 0)][
        [
            "Innings ID",
            "Game Id",
            "batsman",
            "Match Phase",
            "Overall Run Outcome in ball",
            "Boundary Outcome is Four",
            "Boundary Outcome is Six",
            "is_playoff",
            "match_date",
            "stadium_name",
            "Player_Type",
            "Position",
            "season",
            "Team",
            "Ball Number",
            "competition_name",
            "batsman_team_id",
            "batsman_id",
            "venue_id",
            "is_won",
            "batting_type",
            "is_wicket",
        ]
    ]

    data["Position"] = np.where(
        (data["Position"] == 1) | (data["Position"] == 2), "Openers", data["Position"]
    )

    data_runs = data.pivot_table(
        index=[
            "Innings ID",
            "Game Id",
            "batsman",
            "Position",
            "is_playoff",
            "stadium_name",
            "Player_Type",
            "season",
            "Team",
            "match_date",
            "batting_type",
        ],
        columns="Match Phase",
        values=[
            "Overall Run Outcome in ball",
            "Boundary Outcome is Four",
            "Boundary Outcome is Six",
        ],
        aggfunc="sum",
    )
    data_runs = data_runs.sort_index(axis=1, level=1)
    data_runs.columns = [f"{x}_{y}" for x, y in data_runs.columns]

    data_runs = data_runs.reset_index()
    data_runs = columnCheck(
        data_runs,
        [
            "Overall Run Outcome in ball_BATTING_PHASE_POWER_PLAY",
            "Boundary Outcome is Four_BATTING_PHASE_POWER_PLAY",
            "Boundary Outcome is Six_BATTING_PHASE_POWER_PLAY",
            "Overall Run Outcome in ball_BATTING_PHASE_7_10",
            "Boundary Outcome is Four_BATTING_PHASE_7_10",
            "Boundary Outcome is Six_BATTING_PHASE_7_10",
            "Overall Run Outcome in ball_BATTING_PHASE_11_15",
            "Boundary Outcome is Four_BATTING_PHASE_11_15",
            "Boundary Outcome is Six_BATTING_PHASE_11_15",
            "Overall Run Outcome in ball_BATTING_PHASE_DEATH_OVERS",
            "Boundary Outcome is Four_BATTING_PHASE_DEATH_OVERS",
            "Boundary Outcome is Six_BATTING_PHASE_DEATH_OVERS",
        ],
    )

    data_runs = data_runs.rename(
        columns={
            "batsman_": "batsman",
            "Innings ID_": "Innings ID",
            "Overall Run Outcome in ball_BATTING_PHASE_POWER_PLAY": "Actual PowerPlay Over Runs",
            "Boundary Outcome is Four_BATTING_PHASE_POWER_PLAY": "Number of PowerPlay Over Fours",
            "Boundary Outcome is Six_BATTING_PHASE_POWER_PLAY": "Number of PowerPlay Over Sixes",
            "Overall Run Outcome in ball_BATTING_PHASE_7_10": "Actual 7_10 Over Runs",
            "Boundary Outcome is Four_BATTING_PHASE_7_10": "Number of 7_10 Over Fours",
            "Boundary Outcome is Six_BATTING_PHASE_7_10": "Number of 7_10 Over Sixes",
            "Overall Run Outcome in ball_BATTING_PHASE_11_15": "Actual 11_15 Over Runs",
            "Boundary Outcome is Four_BATTING_PHASE_11_15": "Number of 11_15 Over Fours",
            "Boundary Outcome is Six_BATTING_PHASE_11_15": "Number of 11_15 Over Sixes",
            "Overall Run Outcome in ball_BATTING_PHASE_DEATH_OVERS": "Actual Death Over Runs",
            "Boundary Outcome is Four_BATTING_PHASE_DEATH_OVERS": "Number of Death Over Fours",
            "Boundary Outcome is Six_BATTING_PHASE_DEATH_OVERS": "Number of Death Over Sixes",
        }
    )
    data_runs = data_runs.fillna(0)
    data_runs["Overall Runs"] = (
        data_runs["Actual Death Over Runs"]
        + data_runs["Actual 7_10 Over Runs"]
        + data_runs["Actual 11_15 Over Runs"]
        + data_runs["Actual PowerPlay Over Runs"]
    )

    data_runs["Overall Fours"] = (
        data_runs["Number of Death Over Fours"]
        + data_runs["Number of 7_10 Over Fours"]
        + data_runs["Number of 11_15 Over Fours"]
        + data_runs["Number of PowerPlay Over Fours"]
    )
    data_runs["Overall Sixes"] = (
        data_runs["Number of Death Over Sixes"]
        + data_runs["Number of 7_10 Over Sixes"]
        + data_runs["Number of 11_15 Over Sixes"]
        + data_runs["Number of PowerPlay Over Sixes"]
    )
    # data_dimissed=data.groupby(['Game Id','batsman']).agg(is_out=('is_wicket','sum'))
    # data_runs =pd.merge(data_runs,data_dimissed,on=["batsman","Game Id"],how='left')
    data_balls = data.pivot_table(
        index=[
            "Innings ID",
            "Game Id",
            "batsman",
            "Position",
            "competition_name",
            "batsman_team_id",
            "batsman_id",
            "venue_id",
            "batting_type",
            "is_won",
        ],
        columns="Match Phase",
        values=["Ball Number"],
        aggfunc="count",
    )
    data_balls = data_balls.sort_index(axis=1, level=1)
    data_balls.columns = [f"{x}_{y}" for x, y in data_balls.columns]
    data_balls = data_balls.reset_index()
    data_balls = columnCheck(
        data_balls,
        [
            "Ball Number_BATTING_PHASE_DEATH_OVERS",
            "Ball Number_BATTING_PHASE_7_10",
            "Ball Number_BATTING_PHASE_11_15",
            "Ball Number_BATTING_PHASE_POWER_PLAY",
        ],
    )
    data_balls = data_balls.rename(
        columns={
            "batsman_": "batsman",
            "Innings ID_": "Innings ID",
            "Ball Number_BATTING_PHASE_DEATH_OVERS": "Actual Death Over Balls",
            "Ball Number_BATTING_PHASE_7_10": "Actual 7_10 Over Balls",
            "Ball Number_BATTING_PHASE_11_15": "Actual 11_15 Over Balls",
            "Ball Number_BATTING_PHASE_POWER_PLAY": "Actual PowerPlay Over Balls",
        }
    )
    data_balls = data_balls.fillna(0)
    data_balls["Overall Balls"] = (
        data_balls["Actual Death Over Balls"]
        + data_balls["Actual 7_10 Over Balls"]
        + data_balls["Actual 11_15 Over Balls"]
        + data_balls["Actual PowerPlay Over Balls"]
    )
    data_final = pd.merge(
        data_balls,
        data_runs,
        on=["Innings ID", "Game Id", "batsman", "Position", "batting_type"],
    )
    data_final["Actual PowerPlay Strike Rate"] = np.where(
        data_final["Actual PowerPlay Over Balls"] == 0,
        "NA",
        (
            data_final["Actual PowerPlay Over Runs"]
            / data_final["Actual PowerPlay Over Balls"]
        )
        * 100,
    )
    data_final["Actual 7_10 Overs Strike Rate"] = np.where(
        data_final["Actual 7_10 Over Balls"] == 0,
        "NA",
        (data_final["Actual 7_10 Over Runs"] / data_final["Actual 7_10 Over Balls"])
        * 100,
    )
    data_final["Actual 11_15 Overs Strike Rate"] = np.where(
        data_final["Actual 11_15 Over Balls"] == 0,
        "NA",
        (data_final["Actual 11_15 Over Runs"] / data_final["Actual 11_15 Over Balls"])
        * 100,
    )

    data_final["Actual DeathOvers Strike Rate"] = np.where(
        data_final["Actual Death Over Balls"] == 0,
        "NA",
        (data_final["Actual Death Over Runs"] / data_final["Actual Death Over Balls"])
        * 100,
    )
    data_final["Actual Overall Strike Rate"] = np.where(
        data_final["Overall Balls"] == 0,
        "NA",
        (data_final["Overall Runs"] / data_final["Overall Balls"]) * 100,
    )

    constraints = readCSV(constraints_file)
    data_con = pd.merge(data_final, constraints, on=["Position", "Innings ID"])
    fours_con = (
        data_con.groupby(["Position", "batsman"])
        .agg(
            {
                "Number of 7_10 Over Fours": "mean",
                "Number of 11_15 Over Fours": "mean",
                "Number of PowerPlay Over Fours": "mean",
                "Number of Death Over Fours": "mean",
                "Number of 7_10 Over Sixes": "mean",
                "Number of 11_15 Over Sixes": "mean",
                "Number of PowerPlay Over Sixes": "mean",
                "Number of Death Over Sixes": "mean",
                "Overall Fours": "mean",
                "Overall Sixes": "mean",
            }
        )
        .reset_index()
    )
    fours_con = fours_con.rename(
        columns={
            "Number of 7_10 Over Fours": "Avg 7_10 Over Fours",
            "Number of 11_15 Over Fours": "Avg 11_15 Over Fours",
            "Number of PowerPlay Over Fours": "Avg PowerPlay Over Fours",
            "Number of Death Over Fours": "Avg Death Over Fours",
            "Number of 7_10 Over Sixes": "Avg 7_10 Over Sixes",
            "Number of 11_15 Over Sixes": "Avg 11_15 Over Sixes",
            "Number of PowerPlay Over Sixes": "Avg PowerPlay Over Sixes",
            "Number of Death Over Sixes": "Avg Death Over Sixes",
            "Overall Fours": "Avg Overall Fours",
            "Overall Sixes": "Avg Overall Sixes",
        }
    )
    fours_con["Avg 7_10 Over Fours"] = fours_con["Avg 7_10 Over Fours"].apply(
        lambda x: round(x)
    )
    fours_con["Avg 11_15 Over Fours"] = fours_con["Avg 11_15 Over Fours"].apply(
        lambda x: round(x)
    )
    fours_con["Avg PowerPlay Over Fours"] = fours_con["Avg PowerPlay Over Fours"].apply(
        lambda x: round(x)
    )
    fours_con["Avg Death Over Fours"] = fours_con["Avg Death Over Fours"].apply(
        lambda x: round(x)
    )
    fours_con["Avg 7_10 Over Sixes"] = fours_con["Avg 7_10 Over Sixes"].apply(
        lambda x: round(x)
    )
    fours_con["Avg 11_15 Over Sixes"] = fours_con["Avg 11_15 Over Sixes"].apply(
        lambda x: round(x)
    )
    fours_con["Avg PowerPlay Over Sixes"] = fours_con["Avg PowerPlay Over Sixes"].apply(
        lambda x: round(x)
    )
    fours_con["Avg Death Over Sixes"] = fours_con["Avg Death Over Sixes"].apply(
        lambda x: round(x)
    )
    fours_con["Avg Overall Fours"] = fours_con["Avg Overall Fours"].apply(
        lambda x: round(x)
    )
    fours_con["Avg Overall Sixes"] = fours_con["Avg Overall Sixes"].apply(
        lambda x: round(x)
    )
    contr_final = pd.merge(data_con, fours_con, how="left", on=["Position", "batsman"])
    data_ball = (
        data_new.groupby(["match_id", "innings"])
        .apply(pd.DataFrame.sort_values, ["over_number", "ball_number"])
        .reset_index(drop=True)
    )
    data_ball = data_ball.rename(
        columns={
            "match_name": "MatchName",
            "batsman": "Striker",
            "innings": "InningsNo",
            "over_number": "OverNo",
            "runs": "Overall Run Outcome in ball",
            "ball_number": "BallNo",
        }
    )
    data_ball["total_score_till_this_ball"] = data_ball.groupby(
        ["MatchName", "InningsNo"]
    )["Overall Run Outcome in ball"].cumsum()
    data_ball["total_wickets_till_this_ball"] = data_ball.groupby(
        ["MatchName", "InningsNo"]
    )["is_wicket"].cumsum()
    data_ball_grp = (
        data_ball.groupby(["MatchName", "InningsNo"])
        .agg({"is_wicket": "sum"})
        .reset_index()
        .rename(columns={"is_wicket": "Total Wickets in Innings"})
    )
    data_ball = pd.merge(
        data_ball, data_ball_grp, how="left", on=["MatchName", "InningsNo"]
    )
    data_dis = data_ball[
        (data_ball["is_wicket"] == 1) & (data_ball["out_batsman_id"].notna())
    ]
    data_dis["Dismissed_on"] = (data_dis["OverNo"] - 1) * 6 + data_dis["BallNo"]
    data_dis = data_dis[["MatchName", "InningsNo", "Dismissed_on", "out_batsman_id"]]
    data_arr = calc_arrived_on(data_ball)
    data_final_dis = pd.merge(
        data_arr,
        data_dis,
        left_on=["MatchName", "InningsNo", "batsman_id"],
        right_on=["MatchName", "InningsNo", "out_batsman_id"],
        how="left",
    )
    final_arr_dis = data_final_dis.drop_duplicates(
        subset=["MatchName", "InningsNo", "Striker"]
    )
    final_arr_dis = final_arr_dis.rename(
        columns={
            "MatchName": "Game Id",
            "InningsNo": "Innings ID",
            "Striker": "batsman",
        }
    )
    final_arr_dis["Dismissed_on"] = np.where(
        final_arr_dis["Dismissed_on"].isna(), 9999, final_arr_dis["Dismissed_on"]
    )
    final_arr_dis = final_arr_dis.astype({"Dismissed_on": "int64"})
    sam = data_ball[
        [
            "MatchName",
            "InningsNo",
            "Striker",
            "bowler",
            "OverNo",
            "BallNo",
            "Overall Run Outcome in ball",
            "is_wicket",
        ]
    ]
    sam = sam.rename(
        columns={
            "MatchName": "Game Id",
            "InningsNo": "Innings ID",
            "Striker": "batsman",
            "bowler": "Bowler",
            "OverNo": "Over Number",
            "BallNo": "Ball Number",
            "is_wicket": "Wicket Outcome in ball",
        }
    )
    sam["totalballnumber"] = (sam["Over Number"] - 1) * 6 + sam["Ball Number"]
    data_ball = data_ball.rename(
        columns={
            "MatchName": "Game Id",
            "InningsNo": "Innings ID",
            "Striker": "batsman",
            "bowler": "Bowler",
            "OverNo": "Over Number",
            "BallNo": "Ball Number",
        }
    )
    data_wicket = pd.DataFrame(
        columns=["Game Id", "Innings ID", "batsman", "WicketCount"]
    )
    for i in range(0, final_arr_dis.shape[0]):
        gameid = final_arr_dis.iloc[i]["Game Id"]
        inningsid = final_arr_dis.iloc[i]["Innings ID"]
        striker = final_arr_dis.iloc[i]["batsman"]
        arrived_on = final_arr_dis.iloc[i]["Arrived_on"]
        dismissed_on = final_arr_dis.iloc[i]["Dismissed_on"]
        data = data_ball[
            (sam["Game Id"] == gameid)
            & (sam["Innings ID"] == inningsid)
            & (sam["totalballnumber"] >= arrived_on)
            & (sam["totalballnumber"] < dismissed_on)
        ]
        data = (
            data[data["is_wicket"] == 1]
            .groupby(["Game Id", "Innings ID"])
            .size()
            .reset_index(name="WicketCount")
        )
        data["batsman"] = striker
        data["WicketCount"] = data["WicketCount"].fillna(0)
        data_wicket = pd.concat([data_wicket, data], ignore_index=True)
    data_runs = pd.DataFrame(
        columns=["Game Id", "Innings ID", "batsman", "RunsByNonStrikers"]
    )
    for i in range(0, final_arr_dis.shape[0]):
        gameid = final_arr_dis.iloc[i]["Game Id"]
        inningsid = final_arr_dis.iloc[i]["Innings ID"]
        striker = final_arr_dis.iloc[i]["batsman"]
        arrived_on = final_arr_dis.iloc[i]["Arrived_on"]
        dismissed_on = final_arr_dis.iloc[i]["Dismissed_on"]
        data = sam[
            (sam["Game Id"] == gameid)
            & (sam["Innings ID"] == inningsid)
            & (sam["totalballnumber"] >= arrived_on)
            & (sam["totalballnumber"] < dismissed_on)
        ]
        data = data[data["batsman"] != striker]
        data = (
            data.groupby(["Game Id", "Innings ID"])
            .agg(RunsByNonStrikers=("Overall Run Outcome in ball", "sum"))
            .reset_index()
        )
        data["batsman"] = striker

        data_runs = pd.concat([data_runs, data], ignore_index=True)
    final_wicket_fallen = pd.merge(
        final_arr_dis, data_wicket, how="left", on=["Game Id", "Innings ID", "batsman"]
    )
    final_runs_wicket = pd.merge(
        final_wicket_fallen,
        data_runs,
        how="left",
        on=["Game Id", "Innings ID", "batsman"],
    )
    final_runs_wicket["batsman"] = final_runs_wicket["batsman"].str.upper()
    con_final = pd.merge(
        contr_final,
        final_runs_wicket,
        on=["Game Id", "Innings ID", "batsman", "batsman_id"],
        how="left",
    )
    first = data_new
    agg = (
        first.sort_values(
            ["season", "match_date", "innings", "over_number", "ball_number"]
        )
        .groupby(["season", "batting_team", "match_name", "match_date"])["over_number"]
        .count()
        .reset_index()
        .drop("over_number", axis=1)
    )
    agg["cumcount"] = (
        agg.sort_values("match_date")
        .groupby(["season", "batting_team"])["batting_team"]
        .cumcount()
        + 1
    )
    agg["Team"] = agg["batting_team"]
    agg["FirstHalf_SecondHalf"] = np.where(
        agg["cumcount"] > 7, "Second Half", "First Half"
    )
    agg.drop(["batting_team", "cumcount"], axis=1, inplace=True)
    agg.rename(
        columns={
            "match_name": "Game Id",
        },
        inplace=True,
    )
    con_final = pd.merge(
        con_final, agg, on=["match_date", "season", "Team", "Game Id"], how="left"
    )
    con_final.rename(
        columns={
            "is_playoff": "Play_Off",
            "stadium_name": "Stadium",
            "match_date": "Match Date",
        },
        inplace=True,
    )
    con_final["Home_Away"] = np.where(
        (con_final["Stadium"] == "WANKHEDE STADIUM") & (con_final["Team"] == "MI"),
        "Home",
        np.where(
            (con_final["Stadium"] == "ARUN JAITLEY STADIUM")
            & (con_final["Team"] == "DC"),
            "Home",
            np.where(
                (con_final["Stadium"] == "M CHINNASWAMY STADIUM")
                & (con_final["Team"] == "RCB"),
                "Home",
                np.where(
                    (con_final["Stadium"] == "EDEN GARDENS")
                    & (con_final["Team"] == "KKR"),
                    "Home",
                    np.where(
                        (con_final["Stadium"] == "MA CHIDAMBARAM STADIUM")
                        & (con_final["Team"] == "CSK"),
                        "Home",
                        np.where(
                            (
                                con_final["Stadium"]
                                == "RAJIV GANDHI INTERNATIONAL STADIUM, UPPAL"
                            )
                            & (con_final["Team"] == "SRH"),
                            "Home",
                            np.where(
                                (
                                    con_final["Stadium"]
                                    == "PUNJAB CRICKET ASSOCIATION STADIUM"
                                )
                                & (con_final["Team"] == "PBKS"),
                                "Home",
                                "Away",
                            ),
                        ),
                    ),
                ),
            ),
        ),
    )
    con_final["Max Outlier"] = np.where(
        con_final["Position"] == "3",
        90,
        np.where(con_final["Position"] == "Openers", 80, 36),
    )

    con_final["Actual Overall Strike Rate"] = con_final[
        "Actual Overall Strike Rate"
    ].apply(change_sr_to_float)
    con_final["Actual Overall Strike Rate"] = con_final[
        "Actual Overall Strike Rate"
    ].astype("float")

    con_final["Actual PowerPlay Strike Rate"] = con_final[
        "Actual PowerPlay Strike Rate"
    ].apply(change_sr_to_float)
    con_final["Actual PowerPlay Strike Rate"] = con_final[
        "Actual PowerPlay Strike Rate"
    ].astype("float")

    con_final["Actual 7_10 Overs Strike Rate"] = con_final[
        "Actual 7_10 Overs Strike Rate"
    ].apply(change_sr_to_float)
    con_final["Actual 7_10 Overs Strike Rate"] = con_final[
        "Actual 7_10 Overs Strike Rate"
    ].astype("float")

    con_final["Actual 11_15 Overs Strike Rate"] = con_final[
        "Actual 11_15 Overs Strike Rate"
    ].apply(change_sr_to_float)
    con_final["Actual 11_15 Overs Strike Rate"] = con_final[
        "Actual 11_15 Overs Strike Rate"
    ].astype("float")

    con_final["Actual DeathOvers Strike Rate"] = con_final[
        "Actual DeathOvers Strike Rate"
    ].apply(change_sr_to_float)
    con_final["Actual DeathOvers Strike Rate"] = con_final[
        "Actual DeathOvers Strike Rate"
    ].astype("float")

    con_final["Powerplay_contribution_score"] = con_final.apply(
        get_contribution_score_power_play, axis=1
    )
    con_final["7_10_Overs_contribution_score"] = con_final.apply(
        get_contribution_score_7_10_overs, axis=1
    )
    con_final["11_15_Overs_contribution_score"] = con_final.apply(
        get_contribution_score_11_15_overs, axis=1
    )
    con_final["Deathovers_contribution_score"] = con_final.apply(
        get_contribution_score_death_overs, axis=1
    )
    con_final["Final Contribution Score"] = con_final.apply(
        get_batsman_contribution_score, axis=1
    )
    con_final["Overall Contribution Score"] = con_final["Final Contribution Score"]

    con_final["Actual 7_10 Overs Strike Rate"] = con_final[
        "Actual 7_10 Overs Strike Rate"
    ].fillna(0)
    con_final["Actual 11_15 Overs Strike Rate"] = con_final[
        "Actual 11_15 Overs Strike Rate"
    ].fillna(0)
    con_final["Actual DeathOvers Strike Rate"] = con_final[
        "Actual DeathOvers Strike Rate"
    ].fillna(0)
    con_final["Actual 7_10 Overs Strike Rate"] = np.where(
        con_final["Actual 7_10 Overs Strike Rate"] == "NA",
        0,
        con_final["Actual 7_10 Overs Strike Rate"],
    )
    con_final["Actual 11_15 Overs Strike Rate"] = np.where(
        con_final["Actual 11_15 Overs Strike Rate"] == "NA",
        0,
        con_final["Actual 11_15 Overs Strike Rate"],
    )
    con_final["Actual DeathOvers Strike Rate"] = np.where(
        con_final["Actual DeathOvers Strike Rate"] == "NA",
        0,
        con_final["Actual DeathOvers Strike Rate"],
    )
    con_final["Actual 7_10 Overs Strike Rate"] = con_final[
        "Actual 7_10 Overs Strike Rate"
    ].astype("float")
    con_final["Actual 11_15 Overs Strike Rate"] = con_final[
        "Actual 11_15 Overs Strike Rate"
    ].astype("float")
    con_final["Actual DeathOvers Strike Rate"] = con_final[
        "Actual DeathOvers Strike Rate"
    ].astype("float")

    con_final["Strike Rate"] = (
        con_final["Overall Runs"] / con_final["Overall Balls"]
    ) * 100

    def get_position_wise_std(df, col):
        gp = df.groupby("Position")
        out_dict = dict()
        for group in gp.groups:
            grp = gp.get_group(group)
            # print(group)
            dict_percent = grp[col].quantile([0.025, 0.975]).to_dict()
            grp_without_outliers = grp[
                (grp[col] >= dict_percent[0.025]) & (grp[col] <= dict_percent[0.975])
            ]
            out_dict[group] = grp_without_outliers[col].std()
        return out_dict

    con_final[
        [
            "Powerplay_contribution_score",
            "7_10_Overs_contribution_score",
            "11_15_Overs_contribution_score",
            "Deathovers_contribution_score",
        ]
    ] = con_final[
        [
            "Powerplay_contribution_score",
            "7_10_Overs_contribution_score",
            "11_15_Overs_contribution_score",
            "Deathovers_contribution_score",
        ]
    ].fillna(
        "NA"
    )
    con_final["is_out"] = np.where(con_final["Dismissed_on"] == 9999, 0, 1)
    data_final = pd.DataFrame()
    con_final[
        [
            "Powerplay_contribution_score",
            "7_10_Overs_contribution_score",
            "11_15_Overs_contribution_score",
            "Deathovers_contribution_score",
        ]
    ] = con_final[
        [
            "Powerplay_contribution_score",
            "7_10_Overs_contribution_score",
            "11_15_Overs_contribution_score",
            "Deathovers_contribution_score",
        ]
    ].fillna(
        "NA"
    )
    for i in con_final.season.unique():
        data_nor = con_final[con_final["season"] == i]

        data_null = data_nor[
            (data_nor["Powerplay_contribution_score"] == "NA")
            | (data_nor["Powerplay_contribution_score"] == "nan")
        ]
        data_not_null = data_nor[
            (data_nor["Powerplay_contribution_score"] != "NA")
            & (data_nor["Powerplay_contribution_score"] != "nan")
        ]

        std_dict = get_position_wise_std(data_not_null, "Powerplay_contribution_score")
        data_not_null["Powerplay_Std"] = data_not_null["Position"].map(std_dict)

        # data_not_null['min']=data_not_null['Powerplay_contribution_score'].astype('float').min()
        # data_not_null['max']=data_not_null['Powerplay_contribution_score'].astype('float').max()

        data_not_null["Powerplay_Benchmark"] = data_not_null["Powerplay Avg Score"]

        data_nor = pd.concat([data_null, data_not_null])

        data_null = data_nor[
            (data_nor["7_10_Overs_contribution_score"] == "NA")
            | (data_nor["7_10_Overs_contribution_score"] == "nan")
        ]
        data_not_null = data_nor[
            (data_nor["7_10_Overs_contribution_score"] != "NA")
            & (data_nor["7_10_Overs_contribution_score"] != "nan")
        ]

        data_not_null["7_10_Overs_contribution_score"] = data_not_null[
            "7_10_Overs_contribution_score"
        ].astype("float")

        std_dict = get_position_wise_std(data_not_null, "7_10_Overs_contribution_score")
        data_not_null["7_10_Overs_Std"] = data_not_null["Position"].map(std_dict)
        # data_not_null['min']=data_not_null['7_10_Overs_contribution_score'].astype('float').min()
        # data_not_null['max']=data_not_null['7_10_Overs_contribution_score'].astype('float').max()
        data_not_null["7_10_Benchmark"] = data_not_null["7_10 Overs Avg Score"]
        data_nor = pd.concat([data_null, data_not_null])

        data_null = data_nor[
            (data_nor["11_15_Overs_contribution_score"] == "NA")
            | (data_nor["11_15_Overs_contribution_score"] == "nan")
        ]
        data_not_null = data_nor[
            (data_nor["11_15_Overs_contribution_score"] != "NA")
            & (data_nor["11_15_Overs_contribution_score"] != "nan")
        ]
        data_not_null["11_15_Overs_contribution_score"] = data_not_null[
            "11_15_Overs_contribution_score"
        ].astype("float")

        std_dict = get_position_wise_std(
            data_not_null, "11_15_Overs_contribution_score"
        )
        data_not_null["11_15_Overs_Std"] = data_not_null["Position"].map(std_dict)

        # data_not_null['min']=data_not_null['11_15_Overs_contribution_score'].astype('float').min()
        # data_not_null['max']=data_not_null['11_15_Overs_contribution_score'].astype('float').max()

        data_not_null["11_15_Benchmark"] = data_not_null["11_15 Overs Avg Score"]
        data_nor = pd.concat([data_null, data_not_null])

        data_null = data_nor[
            (data_nor["Deathovers_contribution_score"] == "NA")
            | (data_nor["Deathovers_contribution_score"] == "nan")
        ]
        data_not_null = data_nor[
            (data_nor["Deathovers_contribution_score"] != "NA")
            & (data_nor["Deathovers_contribution_score"] != "nan")
        ]
        data_not_null["Deathovers_contribution_score"] = data_not_null[
            "Deathovers_contribution_score"
        ].astype("float")

        std_dict = get_position_wise_std(data_not_null, "Deathovers_contribution_score")
        data_not_null["Deathovers_Std"] = data_not_null["Position"].map(std_dict)

        # data_not_null['min']=data_not_null['Deathovers_contribution_score'].astype('float').min()
        # data_not_null['max']=data_not_null['Deathovers_contribution_score'].astype('float').max()
        data_not_null["Deathovers_Benchmark"] = data_not_null["DeathOvers Avg Score"]
        data_nor = pd.concat([data_null, data_not_null])

        # data_nor['min']=data_nor['Final Contribution Score'].min()
        # data_nor['max']=data_nor['Final Contribution Score'].max()
        data_nor["Normalized Contribution Score"] = data_nor["Final Contribution Score"]
        data_nor["Benchmark"] = data_nor["Overall Avg Score"]

        std_dict = get_position_wise_std(data_nor, "Final Contribution Score")
        data_nor["final_Std"] = data_nor["Position"].map(std_dict)

        data_final = pd.concat([data_final, data_nor], ignore_index=True)

    data_final[
        [
            "Powerplay_contribution_score",
            "7_10_Overs_contribution_score",
            "11_15_Overs_contribution_score",
            "Deathovers_contribution_score",
        ]
    ] = data_final[
        [
            "Powerplay_contribution_score",
            "7_10_Overs_contribution_score",
            "11_15_Overs_contribution_score",
            "Deathovers_contribution_score",
        ]
    ].fillna(
        "NA"
    )
    data_consitency = pd.DataFrame()
    for i in data_final.season.unique():
        data_cons = data_final[data_final["season"] == i]

        data_cons["Bat Expectations"] = np.where(
            data_cons["Normalized Contribution Score"] < data_cons["Benchmark"],
            "NOT MET",
            np.where(
                data_cons["Normalized Contribution Score"]
                < data_cons["Benchmark"] + data_cons["final_Std"],
                "MET",
                "EXCEEDED",
            ),
        )

        data_cons["Met"] = (
            (data_cons["Bat Expectations"] == "EXCEEDED")
            | (data_cons["Bat Expectations"] == "MET")
        ).astype(int)
        data_cons["Not_Met"] = (data_cons["Bat Expectations"] == "NOT MET").astype(int)

        data_null = data_cons[
            (data_cons["Powerplay_contribution_score"] == "NA")
            | (data_cons["Powerplay_contribution_score"] == "nan")
        ]
        data_not_null = data_cons[
            (data_cons["Powerplay_contribution_score"] != "NA")
            & (data_cons["Powerplay_contribution_score"] != "nan")
        ]
        data_not_null["Bat Expectations_Powerplay"] = np.where(
            data_not_null["Powerplay_contribution_score"]
            < data_not_null["Powerplay_Benchmark"],
            "NOT MET",
            np.where(
                data_not_null["Powerplay_contribution_score"]
                < data_not_null["Powerplay_Benchmark"] + data_not_null["Powerplay_Std"],
                "MET",
                "EXCEEDED",
            ),
        )
        data_not_null["Met_Powerplay"] = (
            (data_not_null["Bat Expectations_Powerplay"] == "EXCEEDED")
            | (data_not_null["Bat Expectations_Powerplay"] == "MET")
        ).astype(int)
        data_not_null["Not_Met_Powerplay"] = (
            data_not_null["Bat Expectations_Powerplay"] == "NOT MET"
        ).astype(int)
        data_cons = pd.concat([data_null, data_not_null])

        data_null = data_cons[
            (data_cons["7_10_Overs_contribution_score"] == "NA")
            | (data_cons["7_10_Overs_contribution_score"] == "nan")
        ]
        data_not_null = data_cons[
            (data_cons["7_10_Overs_contribution_score"] != "NA")
            & (data_cons["7_10_Overs_contribution_score"] != "nan")
        ]

        data_not_null["Bat Expectations_7_10"] = np.where(
            data_not_null["7_10_Overs_contribution_score"]
            < data_not_null["7_10_Benchmark"],
            "NOT MET",
            np.where(
                data_not_null["7_10_Overs_contribution_score"]
                < data_not_null["7_10_Benchmark"] + data_not_null["7_10_Overs_Std"],
                "MET",
                "EXCEEDED",
            ),
        )
        data_not_null["Met_7_10"] = (
            (data_not_null["Bat Expectations_7_10"] == "EXCEEDED")
            | (data_not_null["Bat Expectations_7_10"] == "MET")
        ).astype(int)
        data_not_null["Not_Met_7_10"] = (
            data_not_null["Bat Expectations_7_10"] == "NOT MET"
        ).astype(int)
        data_cons = pd.concat([data_null, data_not_null])

        data_null = data_cons[
            (data_cons["11_15_Overs_contribution_score"] == "NA")
            | (data_cons["11_15_Overs_contribution_score"] == "nan")
        ]
        data_not_null = data_cons[
            (data_cons["11_15_Overs_contribution_score"] != "NA")
            & (data_cons["11_15_Overs_contribution_score"] != "nan")
        ]

        data_not_null["Bat Expectations_11_15"] = np.where(
            data_not_null["11_15_Overs_contribution_score"]
            < data_not_null["11_15_Benchmark"],
            "NOT MET",
            np.where(
                data_not_null["11_15_Overs_contribution_score"]
                < data_not_null["11_15_Benchmark"] + data_not_null["11_15_Overs_Std"],
                "MET",
                "EXCEEDED",
            ),
        )
        data_not_null["Met_11_15"] = (
            (data_not_null["Bat Expectations_11_15"] == "EXCEEDED")
            | (data_not_null["Bat Expectations_11_15"] == "MET")
        ).astype(int)
        data_not_null["Not_Met_11_15"] = (
            data_not_null["Bat Expectations_11_15"] == "NOT MET"
        ).astype(int)
        data_cons = pd.concat([data_null, data_not_null])

        data_null = data_cons[
            (data_cons["Deathovers_contribution_score"] == "NA")
            | (data_cons["Deathovers_contribution_score"] == "nan")
        ]
        data_not_null = data_cons[
            (data_cons["Deathovers_contribution_score"] != "NA")
            & (data_cons["Deathovers_contribution_score"] != "nan")
        ]

        data_not_null["Bat Expectations_Deathovers"] = np.where(
            data_not_null["Deathovers_contribution_score"]
            < data_not_null["Deathovers_Benchmark"],
            "NOT MET",
            np.where(
                data_not_null["Deathovers_contribution_score"]
                < data_not_null["Deathovers_Benchmark"]
                + data_not_null["Deathovers_Std"],
                "MET",
                "EXCEEDED",
            ),
        )
        data_not_null["Met_Deathovers"] = (
            (data_not_null["Bat Expectations_Deathovers"] == "EXCEEDED")
            | (data_not_null["Bat Expectations_Deathovers"] == "MET")
        ).astype(int)
        data_not_null["Not_Met_Deathovers"] = (
            data_not_null["Bat Expectations_Deathovers"] == "NOT MET"
        ).astype(int)
        data_cons = pd.concat([data_null, data_not_null])

        df_cons = (
            data_cons.groupby(["batsman"])
            .agg(
                {
                    "Met": "sum",
                    "Not_Met": "sum",
                    "Met_Powerplay": "sum",
                    "Not_Met_Powerplay": "sum",
                    "Met_7_10": "sum",
                    "Not_Met_7_10": "sum",
                    "Met_11_15": "sum",
                    "Not_Met_11_15": "sum",
                    "Met_Deathovers": "sum",
                    "Not_Met_Deathovers": "sum",
                }
            )
            .reset_index()
        )

        df_cons["Total"] = df_cons["Met"] + df_cons["Not_Met"]
        df_cons["Consistency_score"] = round(df_cons["Met"] / df_cons["Total"], 2) * 100
        df_cons["Total_Powerplay"] = (
            df_cons["Met_Powerplay"] + df_cons["Not_Met_Powerplay"]
        )
        df_cons["Consistency_score_Powerplay"] = (
            round(df_cons["Met_Powerplay"] / df_cons["Total_Powerplay"], 2) * 100
        )

        df_cons["Total_7_10"] = df_cons["Met_7_10"] + df_cons["Not_Met_7_10"]
        df_cons["Consistency_score_7_10"] = (
            round(df_cons["Met_7_10"] / df_cons["Total_7_10"], 2) * 100
        )

        df_cons["Total_11_15"] = df_cons["Met_11_15"] + df_cons["Not_Met_11_15"]
        df_cons["Consistency_score_11_15"] = (
            round(df_cons["Met_11_15"] / df_cons["Total_11_15"], 2) * 100
        )

        df_cons["Total_Deathovers"] = (
            df_cons["Met_Deathovers"] + df_cons["Not_Met_Deathovers"]
        )
        df_cons["Consistency_score_Deathovers"] = (
            round(df_cons["Met_Deathovers"] / df_cons["Total_Deathovers"], 2) * 100
        )

        data_cons = pd.merge(
            data_cons,
            df_cons[
                [
                    "batsman",
                    "Consistency_score",
                    "Consistency_score_Powerplay",
                    "Consistency_score_7_10",
                    "Consistency_score_11_15",
                    "Consistency_score_Deathovers",
                ]
            ],
            how="left",
            on=["batsman"],
        )
        data_consitency = pd.concat([data_consitency, data_cons], ignore_index=True)

    return data_consitency


def player_arraived(row):
    players_new = set([row["batsman_id"], row["non_striker_id"]])
    players_old = set([row["striker_shifted"], row["non_striker_shifted"]])
    new_player = players_new - players_old
    try:
        out = list(new_player)[0]
    except:
        out = np.nan
    return out


def calc_arrived_on(data_ball):
    # data_ball_arr = data_ball.groupby(['MatchName', 'InningsNo', 'Striker', 'batsman_id']).first().reset_index()
    # data_ball_arr['Arrived_on'] = ((data_ball_arr['OverNo'] - 1) * 6) + data_ball_arr['BallNo']
    # data_ball_arr = data_ball_arr[['MatchName', 'InningsNo', 'Striker', 'batsman_id', 'Total Wickets in Innings',
    #                                'Arrived_on']]
    # data_arr = data_ball_arr[data_ball_arr['Arrived_on'] != None]
    data_ball["ball_number_total"] = ((data_ball["OverNo"] - 1) * 6) + data_ball[
        "BallNo"
    ]
    data_ball["New_IDx"] = range(len(data_ball))
    data_ball["UID"] = (
        data_ball["MatchName"].astype(str)
        + "_"
        + data_ball["InningsNo"].astype(str)
        + "_"
        + data_ball["batsman_id"].astype(str)
    )

    data_ball_new = data_ball.sort_values(
        ["MatchName", "InningsNo", "OverNo", "BallNo"]
    )
    data_ball_wicket = data_ball_new.groupby(["MatchName", "InningsNo"])

    data_ball_wicket = data_ball_new.assign(
        is_wicket_shifted=data_ball_wicket["is_wicket"].shift(1),
        striker_shifted=data_ball_wicket["batsman_id"].shift(1),
        non_striker_shifted=data_ball_wicket["non_striker_id"].shift(1),
    )

    data_ball_wicket = data_ball_wicket[data_ball_wicket["is_wicket_shifted"] == 1]
    data_ball_wicket["player_new_id"] = data_ball_wicket.apply(
        lambda x: player_arraived(x), axis=1
    )
    data_ball_wicket["player_new_id"] = data_ball_wicket["player_new_id"].astype(
        "Int64"
    )
    data_ball_wicket["UID_new_player"] = (
        data_ball_wicket["MatchName"].astype(str)
        + "_"
        + data_ball_wicket["InningsNo"].astype(str)
        + "_"
        + data_ball_wicket["player_new_id"].astype(str)
    )
    dict_UID_arrived_on = (
        data_ball_wicket[
            [
                "UID_new_player",
                "ball_number_total",
                "MatchName",
                "InningsNo",
                "player_new_id",
                "New_IDx",
            ]
        ]
        .set_index("UID_new_player")
        .to_dict()
    )

    data_ball_new["Arrived_on"] = np.where(
        data_ball_new["batting_position"].isin([1, 2]),
        1,
        np.where(
            data_ball_new["UID"].isin(dict_UID_arrived_on["ball_number_total"].keys()),
            data_ball_new["UID"].map(dict_UID_arrived_on["ball_number_total"]),
            data_ball_new["ball_number_total"],
        ),
    )

    data_ball_wicket = data_ball_new[
        [
            "MatchName",
            "InningsNo",
            "Striker",
            "batsman_id",
            "Total Wickets in Innings",
            "Arrived_on",
        ]
    ]
    data_ball_wicket = data_ball_wicket[~data_ball_wicket["Arrived_on"].isnull()]
    data_ball_wicket = data_ball_wicket.drop_duplicates()

    data_ball.drop(columns=["ball_number_total", "New_IDx", "UID"], inplace=True)

    return data_ball_wicket


def fractional_overs_cal(series):
    pass


def Bowler_Contribution_Score(matches_data):
    data = matches_data
    data.rename(
        columns={
            "innings": "Innings ID",
            "match_name": "Game Id",
            "bowler": "Bowler",
            "runs": "Overall Run Outcome in ball",
            "match_phase": "Match Phase",
            "over_number": "Over Number",
            "is_bowler_wicket": "Wicket Outcome in ball",
            "stadium_name": "Stadium",
            "is_playoff": "Play_Off",
            "bowling_team": "Team",
            "Bowler_type": "Player_Type",
            "bowling_type": "Bowling_type",
        },
        inplace=True,
    )

    data["Match Phase"] = np.where(
        (data["Over Number"] >= 1) & (data["Over Number"] <= 6),
        "POWERPLAY",
        np.where(
            (data["Over Number"] >= 7) & (data["Over Number"] <= 10),
            "7_10_Overs",
            np.where(
                (data["Over Number"] >= 11) & (data["Over Number"] <= 15),
                "11_15_Overs",
                "DEATHOVERS",
            ),
        ),
    )
    data["is_won"] = np.where(data["bowler_team_id"] == data["winning_team"], 1, 0)
    data["is_bye"] = data["is_bye"].fillna(0)
    data["is_leg_bye"] = data["is_leg_bye"].fillna(0)
    data_runs = data[(data["is_leg_bye"] == 0) & (data["is_bye"] == 0)].pivot_table(
        index=[
            "Innings ID",
            "Game Id",
            "Bowler",
            "season",
            "match_date",
            "Play_Off",
            "Bowling_type",
            "Player_Type",
            "Team",
            "bowler_id",
            "bowler_team_id",
            "is_won",
            "competition_name",
            "venue_id",
        ],
        columns="Match Phase",
        values=["Overall Run Outcome in ball"],
        aggfunc="sum",
    )

    data_runs = data_runs.sort_index(axis=1, level=1)

    data_runs.columns = [f"{x}_{y}" for x, y in data_runs.columns]

    data_runs = data_runs.reset_index()

    data_runs = columnCheck(
        data_runs,
        [
            "Overall Run Outcome in ball_POWERPLAY",
            "Overall Run Outcome in ball_7_10_Overs",
            "Overall Run Outcome in ball_11_15_Overs",
            "Overall Run Outcome in ball_DEATHOVERS",
        ],
    )

    data_runs = data_runs.rename(
        columns={
            "Overall Run Outcome in ball_POWERPLAY": "Actual PowerPlay Over Runs",
            "Overall Run Outcome in ball_7_10_Overs": "Actual 7_10 Over Runs",
            "Overall Run Outcome in ball_11_15_Overs": "Actual 11_15 Over Runs",
            "Overall Run Outcome in ball_DEATHOVERS": "Actual Death Over Runs",
        }
    )

    data_runs = data_runs.fillna(0)
    data_runs["Overall Runs"] = (
        data_runs["Actual Death Over Runs"]
        + data_runs["Actual 7_10 Over Runs"]
        + data_runs["Actual 11_15 Over Runs"]
        + data_runs["Actual PowerPlay Over Runs"]
    )

    data_balls = data[(data["is_wide"] != 1) & (data["is_no_ball"] != 1)].pivot_table(
        index=["Innings ID", "Game Id", "Bowler"],
        columns="Match Phase",
        values=["ball_number"],
        aggfunc="count",
    )
    data_balls = data_balls.sort_index(axis=1, level=1)
    data_balls.columns = [f"{x}_{y}" for x, y in data_balls.columns]
    data_balls = data_balls.reset_index()
    data_balls = columnCheck(
        data_balls,
        [
            "ball_number_DEATHOVERS",
            "ball_number_7_10_Overs",
            "ball_number_11_15_Overs",
            "ball_number_POWERPLAY",
        ],
    )
    data_balls["Actual Death Overs"] = data_balls["ball_number_DEATHOVERS"].apply(
        lambda x: x // 6 + (x % 6) / 10 if pd.notnull(x) else np.nan
    )
    data_balls["Actual 7_10 Overs"] = data_balls["ball_number_7_10_Overs"].apply(
        lambda x: x // 6 + (x % 6) / 10 if pd.notnull(x) else np.nan
    )
    data_balls["Actual 11_15 Overs"] = data_balls["ball_number_11_15_Overs"].apply(
        lambda x: x // 6 + (x % 6) / 10 if pd.notnull(x) else np.nan
    )
    data_balls["Actual PowerPlay Overs"] = data_balls["ball_number_POWERPLAY"].apply(
        lambda x: x // 6 + (x % 6) / 10 if pd.notnull(x) else np.nan
    )
    data_balls = data_balls.fillna(0)
    data_balls["Total Balls Bowled"] = (
        data_balls["ball_number_DEATHOVERS"]
        + data_balls["ball_number_7_10_Overs"]
        + data_balls["ball_number_11_15_Overs"]
        + data_balls["ball_number_POWERPLAY"]
    )
    data_balls["Total Overs Bowled"] = (
        data_balls["Total Balls Bowled"] // 6
        + (data_balls["Total Balls Bowled"] % 6) / 10
    )
    data_wickets = data.pivot_table(
        index=["Innings ID", "Game Id", "Bowler"],
        columns="Match Phase",
        values=["Wicket Outcome in ball"],
        aggfunc="sum",
    )
    data_wickets = data_wickets.sort_index(axis=1, level=1)
    data_wickets.columns = [f"{x}_{y}" for x, y in data_wickets.columns]
    data_wickets = data_wickets.reset_index()
    data_wickets = columnCheck(
        data_wickets,
        [
            "Wicket Outcome in ball_DEATHOVERS",
            "Wicket Outcome in ball_7_10_Overs",
            "Wicket Outcome in ball_11_15_Overs",
            "Wicket Outcome in ball_POWERPLAY",
        ],
    )

    data_wickets = data_wickets.rename(
        columns={
            "Wicket Outcome in ball_DEATHOVERS": "Actual Death Over Wickets",
            "Wicket Outcome in ball_7_10_Overs": "Actual 7_10 Over Wickets",
            "Wicket Outcome in ball_11_15_Overs": "Actual 11_15 Over Wickets",
            "Wicket Outcome in ball_POWERPLAY": "Actual PowerPlay Over Wickets",
        }
    )
    data_wickets = data_wickets.fillna(0)
    data_wickets["Total Wickets"] = (
        data_wickets["Actual Death Over Wickets"]
        + data_wickets["Actual 7_10 Over Wickets"]
        + data_wickets["Actual 11_15 Over Wickets"]
        + data_wickets["Actual PowerPlay Over Wickets"]
    )
    data_final = pd.merge(data_balls, data_runs, on=["Innings ID", "Game Id", "Bowler"])
    data_runs_wickets = pd.merge(
        data_final, data_wickets, on=["Innings ID", "Game Id", "Bowler"]
    )
    data_total_wickets = (
        data.groupby(["Innings ID", "Game Id", "Bowler", "Over Number"])
        .agg(Total_wickets_in_over=("Wicket Outcome in ball", "sum"))
        .reset_index()
    )
    data_two_wickets = (
        data_total_wickets[data_total_wickets["Total_wickets_in_over"] >= 2]
        .groupby(["Innings ID", "Game Id", "Bowler"])
        .size()
        .reset_index(name="Two Wickets in over")
    )
    g = (
        data["Wicket Outcome in ball"]
        .ne(data["Wicket Outcome in ball"].shift())
        .cumsum()
    )
    counts = data.groupby(["Innings ID", "Game Id", "Bowler", g])[
        "Wicket Outcome in ball"
    ].transform("size")
    data["Consec_wicket_count"] = np.where(
        data["Wicket Outcome in ball"].eq(1), counts, 0
    )
    data_hatrick = (
        data[data["Consec_wicket_count"] >= 3]
        .groupby(["Innings ID", "Game Id", "Bowler"])["Bowler"]
        .nunique()
        .reset_index(name="Hattrick_Flag")
    )
    data_withtwo_wicket_flag = pd.merge(
        data_runs_wickets,
        data_two_wickets,
        on=["Innings ID", "Game Id", "Bowler"],
        how="left",
    )
    data_final_flags = pd.merge(
        data_withtwo_wicket_flag,
        data_hatrick,
        on=["Innings ID", "Game Id", "Bowler"],
        how="left",
    )
    data_final_flags = data_final_flags.fillna(0)
    data_final_flags["Overall Economy"] = data_final_flags["Overall Runs"] / (
        data_final_flags["Total Balls Bowled"] / 6
    )
    data_final_flags["Powerplay Economy"] = data_final_flags[
        "Actual PowerPlay Over Runs"
    ] / (data_final_flags["ball_number_POWERPLAY"] / 6)
    data_final_flags["7_10_Overs Economy"] = data_final_flags[
        "Actual 7_10 Over Runs"
    ] / (data_final_flags["ball_number_7_10_Overs"] / 6)
    data_final_flags["11_15_Overs Economy"] = data_final_flags[
        "Actual 11_15 Over Runs"
    ] / (data_final_flags["ball_number_11_15_Overs"] / 6)
    data_final_flags["Deathovers Economy"] = data_final_flags[
        "Actual Death Over Runs"
    ] / (data_final_flags["ball_number_DEATHOVERS"] / 6)
    data_with_stadium = pd.merge(
        data_final_flags,
        data[["Stadium", "Innings ID", "Game Id"]].drop_duplicates(),
        how="left",
        on=["Innings ID", "Game Id"],
    )
    stadium_eco = (
        data.groupby(["Innings ID", "Game Id", "Stadium"])
        .agg({"Overall Run Outcome in ball": "sum", "Over Number": "nunique"})
        .reset_index()
    )
    stadium_eco["Stadium_Economy"] = (
        stadium_eco["Overall Run Outcome in ball"] / stadium_eco["Over Number"]
    )
    stadium_eco = (
        stadium_eco.groupby("Stadium").agg({"Stadium_Economy": "mean"}).reset_index()
    )
    data_final_stadium = pd.merge(
        data_with_stadium, stadium_eco, how="left", on=["Stadium"]
    )

    # logging.info("Partnership Data Calculation ....")
    data_partnership = data.sort_values(
        ["Game Id", "Innings ID", "Over Number", "ball_number"]
    )
    data_partnership["partnership_runs"] = data_partnership.groupby(
        [
            "Game Id",
            "Innings ID",
            data_partnership["is_wicket"].shift(1, fill_value=0).cumsum(),
        ]
    )["ball_runs"].cumsum()
    data_partnership["partnership_runs"] = (
        data_partnership["partnership_runs"]
        * data_partnership["Wicket Outcome in ball"]
    )
    data_partnership["partnership_players"] = (
        data_partnership["batsman_id"].astype(str)
        + "-"
        + data_partnership["non_striker_id"].astype(str)
    )

    partnership_df = (
        data_partnership[
            [
                "Game Id",
                "Innings ID",
                "Bowler",
                "partnership_players",
                "partnership_runs",
            ]
        ]
        .groupby(["Game Id", "Innings ID", "Bowler"])
        .apply(
            lambda x: (
                x[x["partnership_runs"] > 0][
                    ["partnership_runs", "partnership_players"]
                ]
                .set_index("partnership_players")
                .to_dict()["partnership_runs"]
            )
        )
        .reset_index()
    )

    partnership_df.rename(columns={0: "partnerships_broken"}, inplace=True)

    def is_partnership_greaterthan_n(x, value=30):
        for i in x.values():
            if i >= value:
                return 1
        return 0

    partnership_df["Partnership broken"] = partnership_df["partnerships_broken"].apply(
        lambda x: is_partnership_greaterthan_n(x, 30)
    )

    data_final_part = pd.merge(
        data_final_stadium,
        partnership_df,
        how="left",
        on=["Game Id", "Innings ID", "Bowler"],
    )
    data_final_part["Partnership broken"] = data_final_part[
        "Partnership broken"
    ].fillna(0)
    data_final_part["Expected Powerplay Economy"] = 7
    data_final_part["Expected 7_10_Overs Economy"] = 6
    data_final_part["Expected 11_15_Overs Economy"] = 6
    data_final_part["Expected Deathovers Economy"] = 8
    data_final_part["Expected Overall Economy"] = 8
    data_final_part["Powerplay Contribution Score"] = np.where(
        data_final_part["Actual PowerPlay Overs"] <= 0,
        "NA",
        data_final_part["Expected Powerplay Economy"]
        - data_final_part["Powerplay Economy"]
        + 10 * data_final_part["Actual PowerPlay Over Wickets"],
    )
    data_final_part["7_10_Overs Contribution Score"] = np.where(
        data_final_part["Actual 7_10 Overs"] <= 0,
        "NA",
        data_final_part["Expected 7_10_Overs Economy"]
        - data_final_part["7_10_Overs Economy"]
        + 10 * data_final_part["Actual 7_10 Over Wickets"],
    )
    data_final_part["11_15_Overs Contribution Score"] = np.where(
        data_final_part["Actual 11_15 Overs"] <= 0,
        "NA",
        data_final_part["Expected 11_15_Overs Economy"]
        - data_final_part["11_15_Overs Economy"]
        + 10 * data_final_part["Actual 11_15 Over Wickets"],
    )
    data_final_part["DeathOvers Contribution Score"] = np.where(
        data_final_part["Actual Death Overs"] <= 0,
        "NA",
        data_final_part["Expected Deathovers Economy"]
        - data_final_part["Deathovers Economy"]
        + 10 * data_final_part["Actual Death Over Wickets"],
    )
    data_final_part["Overall Contribution Score"] = (
        data_final_part["Expected Overall Economy"]
        - data_final_part["Overall Economy"].fillna(0)
        + 10 * data_final_part["Total Wickets"].fillna(0)
    )
    data_final_part["Contribution Score"] = np.where(
        data_final_part["Hattrick_Flag"] >= 1,
        data_final_part["Overall Contribution Score"].fillna(0)
        + data_final_part["Overall Contribution Score"].abs(),
        np.where(
            data_final_part["Two Wickets in over"].fillna(0) >= 1,
            data_final_part["Overall Contribution Score"].fillna(0)
            + data_final_part["Overall Contribution Score"].abs(),
            np.where(
                data_final_part["Partnership broken"].fillna(0) >= 1,
                data_final_part["Overall Contribution Score"].fillna(0)
                + data_final_part["Overall Contribution Score"].abs(),
                data_final_part["Overall Contribution Score"].fillna(0),
            ),
        ),
    )
    data_final_part[
        [
            "Powerplay Contribution Score",
            "7_10_Overs Contribution Score",
            "11_15_Overs Contribution Score",
            "Deathovers_contribution_score",
        ]
    ] = data_final_part[
        [
            "Powerplay Contribution Score",
            "7_10_Overs Contribution Score",
            "11_15_Overs Contribution Score",
            "DeathOvers Contribution Score",
        ]
    ].fillna(
        "NA"
    )
    data_final = pd.DataFrame()
    for i in data_final_part.season.unique():
        data_nor = data_final_part[data_final_part["season"] == i]
        data_null = data_nor[
            (data_nor["Powerplay Contribution Score"] == "NA")
            | (data_nor["Powerplay Contribution Score"] == "nan")
        ]
        data_not_null = data_nor[
            (data_nor["Powerplay Contribution Score"] != "NA")
            & (data_nor["Powerplay Contribution Score"] != "nan")
        ]
        data_not_null["min"] = (
            data_not_null["Powerplay Contribution Score"].astype("float").min()
        )
        data_not_null["max"] = (
            data_not_null["Powerplay Contribution Score"].astype("float").max()
        )
        data_not_null["Powerplay Contribution Score"] = data_not_null[
            "Powerplay Contribution Score"
        ].astype("float")
        data_not_null["Powerplay Contribution Score"] = (
            (data_not_null["Powerplay Contribution Score"] - data_not_null["min"])
            / (data_not_null["max"] - data_not_null["min"])
        ) * 100
        data_not_null["Powerplay_Benchmark"] = (
            (0 - data_not_null["min"]) / (data_not_null["max"] - data_not_null["min"])
        ) * 100
        data_nor = pd.concat([data_null, data_not_null])
        data_null = data_nor[
            (data_nor["7_10_Overs Contribution Score"] == "NA")
            | (data_nor["7_10_Overs Contribution Score"] == "nan")
        ]
        data_not_null = data_nor[
            (data_nor["7_10_Overs Contribution Score"] != "NA")
            & (data_nor["7_10_Overs Contribution Score"] != "nan")
        ]
        data_not_null["7_10_Overs Contribution Score"] = data_not_null[
            "7_10_Overs Contribution Score"
        ].astype("float")
        data_not_null["min"] = (
            data_not_null["7_10_Overs Contribution Score"].astype("float").min()
        )
        data_not_null["max"] = (
            data_not_null["7_10_Overs Contribution Score"].astype("float").max()
        )
        data_not_null["7_10_Overs Contribution Score"] = (
            (data_not_null["7_10_Overs Contribution Score"] - data_not_null["min"])
            / (data_not_null["max"] - data_not_null["min"])
        ) * 100
        data_not_null["7_10_Overs_Benchmark"] = (
            (0 - data_not_null["min"]) / (data_not_null["max"] - data_not_null["min"])
        ) * 100
        data_nor = pd.concat([data_null, data_not_null])
        data_null = data_nor[
            (data_nor["11_15_Overs Contribution Score"] == "NA")
            | (data_nor["11_15_Overs Contribution Score"] == "nan")
        ]
        data_not_null = data_nor[
            (data_nor["11_15_Overs Contribution Score"] != "NA")
            & (data_nor["11_15_Overs Contribution Score"] != "nan")
        ]
        data_not_null["11_15_Overs Contribution Score"] = data_not_null[
            "11_15_Overs Contribution Score"
        ].astype("float")
        data_not_null["min"] = (
            data_not_null["11_15_Overs Contribution Score"].astype("float").min()
        )
        data_not_null["max"] = (
            data_not_null["11_15_Overs Contribution Score"].astype("float").max()
        )
        data_not_null["11_15_Overs Contribution Score"] = (
            (data_not_null["11_15_Overs Contribution Score"] - data_not_null["min"])
            / (data_not_null["max"] - data_not_null["min"])
        ) * 100
        data_not_null["11_15_Overs_Benchmark"] = (
            (0 - data_not_null["min"]) / (data_not_null["max"] - data_not_null["min"])
        ) * 100
        data_nor = pd.concat([data_null, data_not_null])
        data_null = data_nor[
            (data_nor["DeathOvers Contribution Score"] == "NA")
            | (data_nor["DeathOvers Contribution Score"] == "nan")
        ]
        data_not_null = data_nor[
            (data_nor["DeathOvers Contribution Score"] != "NA")
            & (data_nor["DeathOvers Contribution Score"] != "nan")
        ]
        data_not_null["DeathOvers Contribution Score"] = data_not_null[
            "DeathOvers Contribution Score"
        ].astype("float")
        data_not_null["min"] = (
            data_not_null["DeathOvers Contribution Score"].astype("float").min()
        )
        data_not_null["max"] = (
            data_not_null["DeathOvers Contribution Score"].astype("float").max()
        )
        data_not_null["DeathOvers Contribution Score"] = (
            (data_not_null["DeathOvers Contribution Score"] - data_not_null["min"])
            / (data_not_null["max"] - data_not_null["min"])
        ) * 100
        data_not_null["DeathOvers_Benchmark"] = (
            (0 - data_not_null["min"]) / (data_nor["max"] - data_nor["min"])
        ) * 100
        data_nor = pd.concat([data_null, data_not_null])
        data_nor["min"] = data_nor["Contribution Score"].min()
        data_nor["max"] = data_nor["Contribution Score"].max()
        data_nor["Normalized Contribution Score"] = (
            (data_nor["Contribution Score"] - data_nor["min"])
            / (data_nor["max"] - data_nor["min"])
        ) * 100
        data_nor["Benchmark"] = (
            (0 - data_nor["min"]) / (data_nor["max"] - data_nor["min"])
        ) * 100
        data_final = pd.concat([data_final, data_nor], ignore_index=True)
    data_consistency = pd.DataFrame()
    data_final[
        [
            "Powerplay Contribution Score",
            "7_10_Overs Contribution Score",
            "11_15_Overs Contribution Score",
            "Deathovers_contribution_score",
        ]
    ] = data_final[
        [
            "Powerplay Contribution Score",
            "7_10_Overs Contribution Score",
            "11_15_Overs Contribution Score",
            "DeathOvers Contribution Score",
        ]
    ].fillna(
        "NA"
    )
    for i in data_final.season.unique():
        data_cons = data_final[data_final["season"] == i]
        data_cons["Bowl Expectations"] = np.where(
            data_cons["Normalized Contribution Score"] < data_cons["Benchmark"],
            "NOT MET",
            np.where(
                data_cons["Normalized Contribution Score"]
                < data_cons["Benchmark"] + 15,
                "MET",
                "EXCEEDED",
            ),
        )
        data_cons["Met"] = (
            (data_cons["Bowl Expectations"] == "EXCEEDED")
            | (data_cons["Bowl Expectations"] == "MET")
        ).astype(int)
        data_cons["Not_Met"] = (data_cons["Bowl Expectations"] == "NOT MET").astype(int)
        data_null = data_cons[
            (data_cons["Powerplay Contribution Score"] == "NA")
            | (data_cons["Powerplay Contribution Score"] == "nan")
        ]
        data_not_null = data_cons[
            (data_cons["Powerplay Contribution Score"] != "NA")
            & (data_cons["Powerplay Contribution Score"] != "nan")
        ]
        data_not_null["Bowl Expectations_Powerplay"] = np.where(
            data_not_null["Powerplay Contribution Score"]
            < data_not_null["Powerplay_Benchmark"],
            "NOT MET",
            np.where(
                data_not_null["Powerplay Contribution Score"]
                < data_not_null["Powerplay_Benchmark"] + 15,
                "MET",
                "EXCEEDED",
            ),
        )
        data_not_null["Met_Powerplay"] = (
            (data_not_null["Bowl Expectations_Powerplay"] == "EXCEEDED")
            | (data_not_null["Bowl Expectations_Powerplay"] == "MET")
        ).astype(int)
        data_not_null["Not_Met_Powerplay"] = (
            data_not_null["Bowl Expectations_Powerplay"] == "NOT MET"
        ).astype(int)
        data_cons = pd.concat([data_null, data_not_null])
        data_null = data_cons[
            (data_cons["7_10_Overs Contribution Score"] == "NA")
            | (data_cons["7_10_Overs Contribution Score"] == "nan")
        ]
        data_not_null = data_cons[
            (data_cons["7_10_Overs Contribution Score"] != "NA")
            & (data_cons["7_10_Overs Contribution Score"] != "nan")
        ]
        data_not_null["Bowl Expectations_7_10_Overs"] = np.where(
            data_not_null["7_10_Overs Contribution Score"]
            < data_not_null["7_10_Overs_Benchmark"],
            "NOT MET",
            np.where(
                data_not_null["7_10_Overs Contribution Score"]
                < data_not_null["7_10_Overs_Benchmark"] + 15,
                "MET",
                "EXCEEDED",
            ),
        )
        data_not_null["Met_7_10_Overs"] = (
            (data_not_null["Bowl Expectations_7_10_Overs"] == "EXCEEDED")
            | (data_not_null["Bowl Expectations_7_10_Overs"] == "MET")
        ).astype(int)
        data_not_null["Not_Met_7_10_Overs"] = (
            data_not_null["Bowl Expectations_7_10_Overs"] == "NOT MET"
        ).astype(int)
        data_cons = pd.concat([data_null, data_not_null])
        data_null = data_cons[
            (data_cons["11_15_Overs Contribution Score"] == "NA")
            | (data_cons["11_15_Overs Contribution Score"] == "nan")
        ]
        data_not_null = data_cons[
            (data_cons["11_15_Overs Contribution Score"] != "NA")
            & (data_cons["11_15_Overs Contribution Score"] != "nan")
        ]
        data_not_null["Bowl Expectations_11_15_Overs"] = np.where(
            data_not_null["11_15_Overs Contribution Score"]
            < data_not_null["11_15_Overs_Benchmark"],
            "NOT MET",
            np.where(
                data_not_null["11_15_Overs Contribution Score"]
                < data_not_null["11_15_Overs_Benchmark"] + 15,
                "MET",
                "EXCEEDED",
            ),
        )
        data_not_null["Met_11_15_Overs"] = (
            (data_not_null["Bowl Expectations_11_15_Overs"] == "EXCEEDED")
            | (data_not_null["Bowl Expectations_11_15_Overs"] == "MET")
        ).astype(int)
        data_not_null["Not_Met_11_15_Overs"] = (
            data_not_null["Bowl Expectations_11_15_Overs"] == "NOT MET"
        ).astype(int)
        data_cons = pd.concat([data_null, data_not_null])
        data_null = data_cons[
            (data_cons["DeathOvers Contribution Score"] == "NA")
            | (data_cons["DeathOvers Contribution Score"] == "nan")
        ]
        data_not_null = data_cons[
            (data_cons["DeathOvers Contribution Score"] != "NA")
            & (data_cons["DeathOvers Contribution Score"] != "nan")
        ]
        data_not_null["Bowl Expectations_DeathOvers"] = np.where(
            data_not_null["DeathOvers Contribution Score"]
            < data_not_null["DeathOvers_Benchmark"],
            "NOT MET",
            np.where(
                data_not_null["DeathOvers Contribution Score"]
                < data_not_null["DeathOvers_Benchmark"] + 15,
                "MET",
                "EXCEEDED",
            ),
        )
        data_not_null["Met_DeathOvers"] = (
            (data_not_null["Bowl Expectations_DeathOvers"] == "EXCEEDED")
            | (data_not_null["Bowl Expectations_DeathOvers"] == "MET")
        ).astype(int)
        data_not_null["Not_Met_DeathOvers"] = (
            data_not_null["Bowl Expectations_DeathOvers"] == "NOT MET"
        ).astype(int)
        data_cons = pd.concat([data_null, data_not_null])
        df_cons = (
            data_cons.groupby(["Bowler"])
            .agg(
                {
                    "Met": "sum",
                    "Not_Met": "sum",
                    "Met_Powerplay": "sum",
                    "Not_Met_Powerplay": "sum",
                    "Met_7_10_Overs": "sum",
                    "Not_Met_7_10_Overs": "sum",
                    "Met_11_15_Overs": "sum",
                    "Not_Met_11_15_Overs": "sum",
                    "Met_DeathOvers": "sum",
                    "Not_Met_DeathOvers": "sum",
                }
            )
            .reset_index()
        )
        df_cons["Total"] = df_cons["Met"] + df_cons["Not_Met"]
        df_cons["Consistency_score"] = round(df_cons["Met"] / df_cons["Total"], 2) * 100
        df_cons["Total_Powerplay"] = (
            df_cons["Met_Powerplay"] + df_cons["Not_Met_Powerplay"]
        )
        df_cons["Consistency_score_Powerplay"] = (
            round(df_cons["Met_Powerplay"] / df_cons["Total_Powerplay"], 2) * 100
        )
        df_cons["Total_7_10_Overs"] = (
            df_cons["Met_7_10_Overs"] + df_cons["Not_Met_7_10_Overs"]
        )
        df_cons["Consistency_score_7_10_Overs"] = (
            round(df_cons["Met_7_10_Overs"] / df_cons["Total_7_10_Overs"], 2) * 100
        )
        df_cons["Total_11_15_Overs"] = (
            df_cons["Met_11_15_Overs"] + df_cons["Not_Met_11_15_Overs"]
        )
        df_cons["Consistency_score_11_15_Overs"] = (
            round(df_cons["Met_11_15_Overs"] / df_cons["Total_11_15_Overs"], 2) * 100
        )
        df_cons["Total_DeathOvers"] = (
            df_cons["Met_DeathOvers"] + df_cons["Not_Met_DeathOvers"]
        )
        df_cons["Consistency_score_DeathOvers"] = (
            round(df_cons["Met_DeathOvers"] / df_cons["Total_DeathOvers"], 2) * 100
        )
        data_cons = pd.merge(
            data_cons,
            df_cons[
                [
                    "Bowler",
                    "Consistency_score",
                    "Consistency_score_Powerplay",
                    "Consistency_score_7_10_Overs",
                    "Consistency_score_11_15_Overs",
                    "Consistency_score_DeathOvers",
                ]
            ],
            how="left",
            on=["Bowler"],
        )
        data_consistency = pd.concat([data_consistency, data_cons], ignore_index=True)
    data_consistency["Bowler"] = data_consistency["Bowler"].str.upper()
    return data_consistency


def fn(bat_pct, ball_pct):
    if bat_pct >= ball_pct:
        return "Batting Allrounder"
    else:
        return "Bowling Allrounder"


def get_contribution_score(constraints_file, load_timestamp):
    final_cs_df = pd.DataFrame()
    # matches_list = getPandasFactoryDF(session, "select match_id from matches")
    # matches_data = pd.DataFrame()
    # for index, row in matches_list.iterrows():
    #     match_data_sql = GET_MATCH_DATA_SQL + f" WHERE match_id = {row['match_id']} ALLOW FILTERING;"
    #     if matches_data.empty:
    #         matches_data = getMatchesData(
    #             session,
    #             match_data_sql,
    #             GET_MATCH_SUMMARY
    #         )
    #     else:
    #         matches_data = matches_data.append(
    #             getMatchesData(
    #                 session,
    #                 match_data_sql,
    #                 GET_MATCH_SUMMARY
    #             )
    #         )

    matches_data = getMatchesData(session, GET_MATCH_DATA_SQL, GET_MATCH_SUMMARY)
    if not matches_data.empty:
        tournaments = list(matches_data["competition_name"].unique())
        # tournaments = ['BBL']
        for competition in tournaments:
            logger.info(
                f"Contribution Score Data Generation Started for --> {competition}"
            )
            match_ball_data = matches_data[
                matches_data["competition_name"] == competition
            ]
            if not matches_data.empty:
                bat_con = batting_contribution_score(constraints_file, match_ball_data)
                bowl_con = Bowler_Contribution_Score(match_ball_data)
                bat = bat_con[
                    [
                        "Match Date",
                        "season",
                        "competition_name",
                        "venue_id",
                        "batting_type",
                        "batsman_team_id",
                        "is_won",
                        "is_out",
                        "batsman_id",
                        "Game Id",
                        "batsman",
                        "Player_Type",
                        "Team",
                        "FirstHalf_SecondHalf",
                        "Home_Away",
                        "Play_Off",
                        "Stadium",
                        "Position",
                        "Overall Balls",
                        "Overall Runs",
                        "Overall Fours",
                        "Overall Sixes",
                        "Actual Overall Strike Rate",
                        "Arrived_on",
                        "Dismissed_on",
                        "Powerplay_contribution_score",
                        "7_10_Overs_contribution_score",
                        "11_15_Overs_contribution_score",
                        "Deathovers_contribution_score",
                        "Normalized Contribution Score",
                        "Bat Expectations",
                        "Consistency_score",
                        "Innings ID",
                        "Actual PowerPlay Over Runs",
                        "Actual 7_10 Over Runs",
                        "Actual 11_15 Over Runs",
                        "Actual Death Over Runs",
                        "WicketCount",
                        "RunsByNonStrikers",
                        "Consistency_score_Powerplay",
                        "Consistency_score_7_10",
                        "Consistency_score_11_15",
                        "Consistency_score_Deathovers",
                        "Actual PowerPlay Over Balls",
                        "Actual 7_10 Over Balls",
                        "Actual 11_15 Over Balls",
                        "Actual Death Over Balls",
                    ]
                ]
                bat.rename(
                    columns={
                        "Match Date": "Match_date",
                        "batsman": "Player",
                        "Overall Balls": "Balls Faced",
                        "Overall Runs": "Runs scored",
                        "Actual Overall Strike Rate": "Batting Strike Rate",
                        "Powerplay_contribution_score": "Bat_powerplay_contribution_score",
                        "7_10_Overs_contribution_score": "Bat_7_10_Overs_contribution_score",
                        "11_15_Overs_contribution_score": "Bat_11_15_Overs_contribution_score",
                        "Deathovers_contribution_score": "Bat_deathovers_contribution_score",
                        "Normalized Contribution Score": "Batting Contribution Score",
                        "Consistency_score": "Batting Consistency Score",
                        "Innings ID": "Bat_innings",
                        "batsman_team_id": "team_id",
                        "is_won": "batsman_winning_flag",
                        "batsman_id": "player_id",
                        "WicketCount": "fow_during_stay",
                        "RunsByNonStrikers": "non_striker_runs",
                        "Consistency_score_Powerplay": "bat_consistency_score_powerplay",
                        "Consistency_score_7_10": "bat_consistency_score_7_10",
                        "Consistency_score_11_15": "bat_consistency_score_11_15",
                        "Consistency_score_Deathovers": "bat_consistency_score_deathovers",
                    },
                    inplace=True,
                )
                bowl = bowl_con[
                    [
                        "match_date",
                        "season",
                        "Game Id",
                        "Bowler",
                        "competition_name",
                        "venue_id",
                        "bowler_id",
                        "bowler_team_id",
                        "is_won",
                        "Player_Type",
                        "Team",
                        "Play_Off",
                        "Stadium",
                        "Total Overs Bowled",
                        "Total Balls Bowled",
                        "Overall Runs",
                        "Total Wickets",
                        "Overall Economy",
                        "Powerplay Contribution Score",
                        "7_10_Overs Contribution Score",
                        "11_15_Overs Contribution Score",
                        "DeathOvers Contribution Score",
                        "Normalized Contribution Score",
                        "Bowl Expectations",
                        "Consistency_score",
                        "Innings ID",
                        "Bowling_type",
                        "Hattrick_Flag",
                        "Consistency_score_Powerplay",
                        "Consistency_score_7_10_Overs",
                        "Consistency_score_11_15_Overs",
                        "Consistency_score_DeathOvers",
                    ]
                ]
                bowl.rename(
                    columns={
                        "match_date": "Match_date",
                        "Bowler": "Player",
                        "Overall Runs": "Runs Conceded",
                        "Overall Wickets": "Wickets taken",
                        "Powerplay Contribution Score": "Bowl_powerplay_contribution_score",
                        "7_10_Overs Contribution Score": "Bowl_7_10_Overs_contribution_score",
                        "11_15_Overs Contribution Score": "Bowl_11_15_Overs_contribution_score",
                        "DeathOvers Contribution Score": "Bowl_deathovers_contribution_score",
                        "Normalized Contribution Score": "Bowling Contribution Score",
                        "Consistency_score": "Bowling Consistency Score",
                        "Innings ID": "Bowl_innings",
                        "bowler_team_id": "team_id",
                        "is_won": "bowler_winning_flag",
                        "bowler_id": "player_id",
                        "Hattrick_Flag": "is_hatrick",
                        "Consistency_score_Powerplay": "bowl_consistency_score_powerplay",
                        "Consistency_score_7_10_Overs": "bowl_consistency_score_7_10",
                        "Consistency_score_11_15_Overs": "bowl_consistency_score_11_15",
                        "Consistency_score_DeathOvers": "bowl_consistency_score_deathovers",
                    },
                    inplace=True,
                )
                bat_bowl = pd.merge(
                    bat,
                    bowl,
                    on=[
                        "Match_date",
                        "Player",
                        "Game Id",
                        "Play_Off",
                        "Team",
                        "Stadium",
                        "season",
                        "Player_Type",
                        "team_id",
                        "player_id",
                        "competition_name",
                        "venue_id",
                    ],
                    how="outer",
                )
                bat_bowl["is_won"] = np.where(
                    bat_bowl["batsman_winning_flag"].isnull(),
                    bat_bowl["bowler_winning_flag"],
                    bat_bowl["batsman_winning_flag"],
                )
                bat_bowl[["is_won", "is_out"]] = (
                    bat_bowl[["is_won", "is_out"]].fillna(0).astype(int)
                )
                bat_bowl = bat_bowl.drop(
                    ["batsman_winning_flag", "bowler_winning_flag"], axis=1
                )
                for i in [
                    "Bat_powerplay_contribution_score",
                    "Bat_7_10_Overs_contribution_score",
                    "Bat_11_15_Overs_contribution_score",
                    "Bat_deathovers_contribution_score",
                    "Batting Consistency Score",
                    "Batting Contribution Score",
                    "Bowl_powerplay_contribution_score",
                    "Bowl_7_10_Overs_contribution_score",
                    "Bowl_11_15_Overs_contribution_score",
                    "Bowl_deathovers_contribution_score",
                    "Bowling Consistency Score",
                    "Bowling Contribution Score",
                    "bat_consistency_score_powerplay",
                    "bat_consistency_score_7_10",
                    "bat_consistency_score_11_15",
                    "bat_consistency_score_deathovers",
                    "bowl_consistency_score_powerplay",
                    "bowl_consistency_score_7_10",
                    "bowl_consistency_score_11_15",
                    "bowl_consistency_score_deathovers",
                ]:
                    bat_bowl[i] = bat_bowl[i].fillna(0)
                    bat_bowl[i] = np.where(bat_bowl[i] == "NA", 0, bat_bowl[i])
                    bat_bowl[i] = np.where(bat_bowl[i] == "nan", 0, bat_bowl[i])
                    bat_bowl[i] = bat_bowl[i].astype("float")
                bat_bowl["Overall_powerplay_contribution_score"] = bat_bowl[
                    "Bat_powerplay_contribution_score"
                ].fillna(0) + bat_bowl["Bowl_powerplay_contribution_score"].fillna(0)
                bat_bowl["Overall_7_10_Overs_contribution_score"] = bat_bowl[
                    "Bat_7_10_Overs_contribution_score"
                ].fillna(0) + bat_bowl["Bowl_7_10_Overs_contribution_score"].fillna(0)
                bat_bowl["Overall_11_15_Overs_contribution_score"] = bat_bowl[
                    "Bat_11_15_Overs_contribution_score"
                ].fillna(0) + bat_bowl["Bowl_11_15_Overs_contribution_score"].fillna(0)
                bat_bowl["Overall_deathovers_contribution_score"] = bat_bowl[
                    "Bat_deathovers_contribution_score"
                ].fillna(0) + bat_bowl["Bowl_deathovers_contribution_score"].fillna(0)
                bat_bowl["Overall_consistency_score"] = bat_bowl[
                    "Batting Consistency Score"
                ].fillna(0) + bat_bowl["Bowling Consistency Score"].fillna(0)
                bat_bowl["Overall_contribution_score"] = bat_bowl[
                    "Batting Contribution Score"
                ].fillna(0) + bat_bowl["Bowling Contribution Score"].fillna(0)
                data_bat_bowl = pd.DataFrame()
                for i in bat_bowl.season.unique():
                    data_season = bat_bowl[bat_bowl["season"] == i]
                    sys = (
                        data_season.groupby(["Player", "Position"])
                        .agg({"Game Id": "count"})
                        .reset_index()
                    )
                    sys["Batted_more_than_5_innings"] = np.where(
                        sys["Game Id"] >= 5, 1, 0
                    )
                    sys["Batted_more_than_10_innings"] = np.where(
                        sys["Game Id"] >= 10, 1, 0
                    )
                    sys_bowler = (
                        data_season.groupby("Player")
                        .agg({"Total Overs Bowled": "count"})
                        .reset_index()
                    )
                    sys_bowler["Bowled_more_than_5_innings"] = np.where(
                        sys_bowler["Total Overs Bowled"] >= 5, 1, 0
                    )
                    sys_bowler["Bowled_more_than_10_innings"] = np.where(
                        sys_bowler["Total Overs Bowled"] >= 10, 1, 0
                    )
                    data_season = pd.merge(
                        data_season,
                        sys[
                            [
                                "Player",
                                "Position",
                                "Batted_more_than_5_innings",
                                "Batted_more_than_10_innings",
                            ]
                        ],
                        on=["Player", "Position"],
                        how="left",
                    )
                    data_season = pd.merge(
                        data_season,
                        sys_bowler[
                            [
                                "Player",
                                "Bowled_more_than_5_innings",
                                "Bowled_more_than_10_innings",
                            ]
                        ],
                        on=["Player"],
                        how="left",
                    )
                    data_bat_bowl = pd.concat(
                        [data_bat_bowl, data_season], ignore_index=True
                    )
                data_bat_bowl["Bowled_3_or_4_overs"] = np.where(
                    bat_bowl["Total Overs Bowled"] >= 3, 1, 0
                )
                data_bat_bowl["Bowled_4_overs"] = np.where(
                    bat_bowl["Total Overs Bowled"] == 4, 1, 0
                )

                data_bat_bowl["retained"] = np.where(
                    data_bat_bowl["Player"].isin(retained_list), 1, 0
                )

                df_temp = data_bat_bowl.copy()
                df_temp["Total Overs Bowled"] = df_temp["Total Overs Bowled"].fillna(
                    value=0
                )
                df_temp["Total Balls Bowled"] = df_temp["Total Balls Bowled"].fillna(
                    value=0
                )
                df_temp["bowled_less_than_2_overs"] = (
                    df_temp["Total Overs Bowled"] < 2
                ).astype(int)
                df_temp["bowled_more_than_2_overs"] = (
                    df_temp["Total Overs Bowled"] >= 2
                ).astype(int)
                df_temp["Position"] = df_temp["Position"].replace("Openers", 1)
                df_temp["Position"] = df_temp["Position"].fillna(99)
                df_temp["Position"] = df_temp["Position"].astype(int)
                df_temp["Actual_bat_inns"] = df_temp.apply(
                    lambda x: (x["Bat_innings"] == 1) & (x["Position"] <= 8), axis=1
                )
                df_temp["Actual_bat_inns"] = df_temp["Actual_bat_inns"].astype(int)
                df_t = (
                    df_temp.groupby(["Player"])
                    .agg(
                        {
                            "Game Id": pd.Series.nunique,
                            "Actual_bat_inns": np.sum,
                            "Bowl_innings": np.sum,
                            "bowled_less_than_2_overs": np.sum,
                            "bowled_more_than_2_overs": np.sum,
                        }
                    )
                    .reset_index()
                )
                df_t["bowled_less_than_2_overs"] = (
                    df_t["Bowl_innings"] - df_t["bowled_more_than_2_overs"]
                )
                df_t["Bat_pct"] = round(
                    100 * df_t["Actual_bat_inns"] / df_t["Game Id"], 2
                )
                df_t["Ball_pct"] = round(
                    100 * df_t["bowled_more_than_2_overs"] / df_t["Game Id"], 2
                )
                # Pure batsman(80)
                pure_bat = df_t[(df_t["Ball_pct"].between(0, 10))].copy()
                # Pure Bowler(96)
                pure_bowl = df_t[
                    (df_t["Ball_pct"].between(80, 100))
                    & (df_t["Bat_pct"].between(0, 10))
                ].copy()
                pure_bowl["Speciality"] = "Bowler"
                pure_bat["Speciality"] = "Batsman"

                all_rounder = df_t[
                    (~df_t["Player"].isin(pure_bat["Player"].values))
                    & (~df_t["Player"].isin(pure_bowl["Player"].values))
                ].copy()

                if len(all_rounder) > 0:
                    all_rounder["Speciality"] = all_rounder.apply(
                        lambda x: fn(x["Bat_pct"], x["Ball_pct"]), axis=1
                    )
                else:
                    all_rounder["Speciality"] = "NA"
                players = pd.concat(
                    [
                        pure_bat[["Player", "Speciality"]],
                        pure_bowl[["Player", "Speciality"]],
                        all_rounder[["Player", "Speciality"]],
                    ],
                    ignore_index=True,
                )
                data_bat_bowl_new = pd.merge(
                    data_bat_bowl, players, how="left", on="Player"
                )
                data_bat_bowl_new.columns = [
                    x.lower() for x in data_bat_bowl_new.columns
                ]
                data_bat_bowl_new.columns = data_bat_bowl_new.columns.str.replace(
                    " ", "_"
                )
                data_bat_bowl_new[
                    [
                        "bat_powerplay_contribution_score",
                        "bat_7_10_overs_contribution_score",
                        "bat_11_15_overs_contribution_score",
                        "bat_deathovers_contribution_score",
                        "batting_contribution_score",
                        "batting_consistency_score",
                        "overall_fours",
                        "dismissed_on",
                        "arrived_on",
                        "bat_consistency_score_powerplay",
                        "bat_consistency_score_7_10",
                        "bat_consistency_score_11_15",
                        "bat_consistency_score_deathovers",
                        "bowl_consistency_score_powerplay",
                        "bowl_consistency_score_7_10",
                        "bowl_consistency_score_11_15",
                        "bowl_consistency_score_deathovers",
                    ]
                ] = (
                    data_bat_bowl_new[
                        [
                            "bat_powerplay_contribution_score",
                            "bat_7_10_overs_contribution_score",
                            "bat_11_15_overs_contribution_score",
                            "bat_deathovers_contribution_score",
                            "batting_contribution_score",
                            "batting_consistency_score",
                            "overall_fours",
                            "dismissed_on",
                            "arrived_on",
                            "bat_consistency_score_powerplay",
                            "bat_consistency_score_7_10",
                            "bat_consistency_score_11_15",
                            "bat_consistency_score_deathovers",
                            "bowl_consistency_score_powerplay",
                            "bowl_consistency_score_7_10",
                            "bowl_consistency_score_11_15",
                            "bowl_consistency_score_deathovers",
                        ]
                    ]
                    .apply(lambda x: round(x.fillna(9999)))
                    .astype(int)
                )

                data_bat_bowl_new["bat_innings"] = np.where(
                    data_bat_bowl_new["bat_innings"] == 2,
                    1,
                    data_bat_bowl_new["bat_innings"],
                )
                data_bat_bowl_new["bowl_innings"] = np.where(
                    data_bat_bowl_new["bowl_innings"] == 2,
                    1,
                    data_bat_bowl_new["bowl_innings"],
                )

                data_bat_bowl_new[
                    [
                        "runs_scored",
                        "runs_conceded",
                        "total_wickets",
                        "bowl_powerplay_contribution_score",
                        "bowl_7_10_overs_contribution_score",
                        "bowl_11_15_overs_contribution_score",
                        "bowl_deathovers_contribution_score",
                        "bowling_contribution_score",
                        "bowling_consistency_score",
                        "bowl_innings",
                        "bat_innings",
                        "bowled_3_or_4_overs",
                        "bowled_4_overs",
                        "batted_more_than_5_innings",
                        "batted_more_than_10_innings",
                        "bowled_more_than_5_innings",
                        "bowled_more_than_10_innings",
                        "actual_powerplay_over_runs",
                        "actual_7_10_over_runs",
                        "actual_11_15_over_runs",
                        "actual_death_over_runs",
                        "actual_powerplay_over_balls",
                        "actual_7_10_over_balls",
                        "actual_11_15_over_balls",
                        "actual_death_over_balls",
                    ]
                ] = (
                    data_bat_bowl_new[
                        [
                            "runs_scored",
                            "runs_conceded",
                            "total_wickets",
                            "bowl_powerplay_contribution_score",
                            "bowl_7_10_overs_contribution_score",
                            "bowl_11_15_overs_contribution_score",
                            "bowl_deathovers_contribution_score",
                            "bowling_contribution_score",
                            "bowling_consistency_score",
                            "bowl_innings",
                            "bat_innings",
                            "bowled_3_or_4_overs",
                            "bowled_4_overs",
                            "batted_more_than_5_innings",
                            "batted_more_than_10_innings",
                            "bowled_more_than_5_innings",
                            "bowled_more_than_10_innings",
                            "actual_powerplay_over_runs",
                            "actual_7_10_over_runs",
                            "actual_11_15_over_runs",
                            "actual_death_over_runs",
                            "actual_powerplay_over_balls",
                            "actual_7_10_over_balls",
                            "actual_11_15_over_balls",
                            "actual_death_over_balls",
                        ]
                    ]
                    .apply(lambda x: round(x.fillna(9999)))
                    .astype(int)
                )

                data_bat_bowl_new[
                    [
                        "player_id",
                        "overall_powerplay_contribution_score",
                        "overall_7_10_overs_contribution_score",
                        "overall_11_15_overs_contribution_score",
                        "overall_deathovers_contribution_score",
                        "overall_consistency_score",
                        "overall_contribution_score",
                        "overall_sixes",
                        "balls_faced",
                        "fow_during_stay",
                        "non_striker_runs",
                    ]
                ] = (
                    data_bat_bowl_new[
                        [
                            "player_id",
                            "overall_powerplay_contribution_score",
                            "overall_7_10_overs_contribution_score",
                            "overall_11_15_overs_contribution_score",
                            "overall_deathovers_contribution_score",
                            "overall_consistency_score",
                            "overall_contribution_score",
                            "overall_sixes",
                            "balls_faced",
                            "fow_during_stay",
                            "non_striker_runs",
                        ]
                    ]
                    .apply(lambda x: round(x.fillna(9999)))
                    .astype(int)
                )

                data_bat_bowl_new[
                    ["batting_strike_rate", "overall_economy"]
                ] = data_bat_bowl_new[["batting_strike_rate", "overall_economy"]].apply(
                    lambda x: round(x.fillna(0.0), 2)
                )
                data_bat_bowl_new["play_off"] = (
                    data_bat_bowl_new["play_off"].fillna(0).astype(int)
                )
                data_bat_bowl_new["is_hatrick"] = (
                    data_bat_bowl_new["is_hatrick"].fillna(0).astype(int)
                )
                data_bat_bowl_new["total_overs_bowled"] = data_bat_bowl_new[
                    "total_overs_bowled"
                ].fillna(0)
                data_bat_bowl_new["total_balls_bowled"] = (
                    data_bat_bowl_new["total_balls_bowled"].fillna(0).astype(int)
                )
                data_bat_bowl_new[
                    [
                        "bowl_expectations",
                        "bowling_type",
                        "bat_expectations",
                        "home_away",
                        "firsthalf_secondhalf",
                        "position",
                        "batting_type",
                    ]
                ] = data_bat_bowl_new[
                    [
                        "bowl_expectations",
                        "bowling_type",
                        "bat_expectations",
                        "home_away",
                        "firsthalf_secondhalf",
                        "position",
                        "batting_type",
                    ]
                ].fillna(
                    "NA"
                )

                final_cs_df = final_cs_df.append(data_bat_bowl_new, ignore_index=True)

        if not final_cs_df.empty:
            wicket_keepers_list = [
                "Ab De Villiers",
                "Alex Carey",
                "Anuj Rawat",
                "Dinesh Karthik",
                "Heinrich Klaasen",
                "Ishan Kishan",
                "Jagadeesan N",
                "Jonny Bairstow",
                "Jos Buttler",
                "Josh Philippe",
                "K S Bharat",
                "Kl Rahul",
                "Ms Dhoni",
                "Nicholas Pooran",
                "Parthiv Patel",
                "Quinton De Kock",
                "Rishabh Pant",
                "Robin Uthappa",
                "Sam Billings",
                "Sanju Samson",
                "Shreevats Goswami",
                "Simran Singh",
                "Tim Seifert",
                "Tom Banton",
                "Wriddhiman Saha",
            ]

            wicket_keepers_list = [x.upper() for x in wicket_keepers_list]

            final_cs_df["speciality"] = np.where(
                final_cs_df["player"].isin(wicket_keepers_list),
                "WK Batsman",
                final_cs_df["speciality"],
            )

            final_cs_df["in_auction"] = np.where(
                final_cs_df["player"].isin(in_auction), "AVAILABLE", "NOT AVAILABLE"
            )
            final_cs_df["load_timestamp"] = load_timestamp
            logger.info("Contribution Score Data Generation Completed!")
            return final_cs_df.to_dict(orient="records")

    else:
        logger.info("No New Contribution Score Data Available!")


# load_timestamp = '2001-01-01 00:00:00'
# from DataIngestion.config import CONTRIBUTION_CONSTRAINTS_DATA_PATH
# from common.utils.helper import getPrettyDF
# contribution_score = get_contribution_score(CONTRIBUTION_CONSTRAINTS_DATA_PATH, load_timestamp)
# print(getPrettyDF(contribution_score[contribution_score['player']=='GLENN MAXWELL']))
