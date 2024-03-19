import numpy as np
import pandas as pd

from DataIngestion.config import PI_CONFIG_DATA_PATH
from DataIngestion.pressure_index.utils import get_entry_point_group, read_json, get_percentiles_using_bootstrapping


def create_baseline_df(data):
    data["bowler_id"] = data["bowler_source_id"]
    data["non_striker_id"] = data["non_striker_source_id"]
    data["out_batsman_id"] = data["out_batsman_source_id"]
    data["batsman_id"] = data["batsman_source_id"]
    data["out_batsman_id"] = data["out_batsman_id"].fillna(-1)
    data["raw_ball_no"] = data.groupby(["match_name", "innings"]).cumcount() + 1
    data["is_batsman_ball"] = np.where(data["is_wide"] == 0, 1, 0)

    data["bowler_runs"] = np.where(
        ((data["is_leg_bye"] == 0) & (data["is_bye"] == 0)),
        data["runs"],
        0,
    )
    data["is_bowler_ball"] = np.where(
        ((data["is_wide"] == 1) | (data["is_no_ball"] == 1)), 0, 1
    )
    data["match_date"] = pd.to_datetime(data["match_date"])

    data["match_phase"] = np.where(
        (data["over_number"] >= 1) & (data["over_number"] <= 6),
        "P1(1-6 Overs)",
        np.where(
            (data["over_number"] >= 7) & (data["over_number"] <= 15),
            "P2(7-15 Overs)",
            "P3(16-20 Overs)",
        ),
    )
    data["total_wickets"] = data.groupby(["match_date", "match_name", "innings"])[
        "is_wicket"
].cumsum()
    data["total_runs_scored"] = data.groupby(["match_date", "match_name", "innings"])[
        "runs"
    ].cumsum()
    data["total_runs_scored_by_bat"] = data.groupby(
        ["match_date", "match_name", "innings"]
    )["ball_runs"].cumsum()

    data["total_balls_by_bat"] = data.groupby(["match_date", "match_name", "innings"])[
        "is_batsman_ball"
    ].cumsum()

    data["total_runs_given_by_bowler"] = data.groupby(
        ["match_date", "match_name", "innings"]
    )["bowler_runs"].cumsum()

    data["ball_no_exact"] = (data["over_number"] - 1) * 6 + data["ball_number"]
    data["ball_no_bowler"] = data["ball_no_exact"] - (1 - data["is_bowler_ball"])
    data["CRR"] = data["total_runs_scored"] / (data["ball_no_bowler"] / 6)
    return data


def get_batsman_pos_arr_dismiss_df(grp):
    required_columns = [
        "over_number",
        "ball_number",
        "raw_ball_no",
        "batsman_id",
        "non_striker_id",
        "is_wicket",
        "out_batsman_id",
        "batsman",
        "non_striker",
        "striker_batting_type",
        "non_striker_batting_type",
    ]

    assert all(col in grp.columns for col in required_columns)
    grp = grp[required_columns]
    df = pd.concat(
        [
            grp[
                [
                    "over_number",
                    "ball_number",
                    "raw_ball_no",
                    "batsman_id",
                    "batsman",
                    "striker_batting_type",
                ]
            ],
            grp[
                [
                    "over_number",
                    "ball_number",
                    "raw_ball_no",
                    "non_striker_id",
                    "non_striker",
                    "non_striker_batting_type",
                ]
            ].rename(
                columns={
                    "non_striker_id": "batsman_id",
                    "non_striker": "batsman",
                    "non_striker_batting_type": "striker_batting_type",
                }
            ),
        ]
    ).sort_values(["raw_ball_no"])

    dismissed_players = set(grp[grp["is_wicket"] == 1]["out_batsman_id"].values)

    df["Position"] = pd.factorize(df["batsman_id"])[0] + 1
    df_arr_dis = df.sort_values(["Position", "batsman_id", "raw_ball_no"])
    df_arrived = df_arr_dis.groupby(["Position", "batsman_id"]).head(1)
    df_dissmiss = df_arr_dis.groupby(["Position", "batsman_id"]).tail(1)
    df_dissmiss = df_dissmiss[df_dissmiss["batsman_id"].isin(dismissed_players)]

    df_arrived["arrived_on"] = ((df_arrived["over_number"] - 1) * 6) + df_arrived[
        "ball_number"
    ]
    df_dissmiss["dismissed_on"] = ((df_dissmiss["over_number"] - 1) * 6) + df_dissmiss[
        "ball_number"
    ]

    df = df.sort_values(["Position", "batsman_id"]).drop_duplicates(
        ["Position", "batsman_id"]
    )
    df = (
        df[["batsman_id", "batsman", "Position", "striker_batting_type"]]
        .reset_index()
        .drop("index", axis=1)
    )

    df = df.merge(
        df_arrived[["batsman_id", "arrived_on", "raw_ball_no"]].rename(
            columns={"raw_ball_no": "arrived_raw_ball_no"}
        ),
        how="left",
        on="batsman_id",
    )
    df = df.merge(
        df_dissmiss[["batsman_id", "dismissed_on", "raw_ball_no"]].rename(
            columns={"raw_ball_no": "dismissed_raw_ball_no"}
        ),
        how="left",
        on="batsman_id",
    )
    df = df.fillna(-1)
    df["entry_point"] = df["arrived_on"].apply(get_entry_point_group)
    return df


def create_percentile_min_max_vals(
    pi_byb_data, feature_json_path=PI_CONFIG_DATA_PATH+"feature_names.json"
):
    features = read_json(feature_json_path)["features"]
    percentile_vals = {}
    for col in features:
        percentile_vals[col] = {}

        temp = get_percentiles_using_bootstrapping(pi_byb_data[col])
        # if col == "wickets_by_10":
        #     min_percentile = 0
        #     max_percentile = 100
        # else:
        #     min_percentile = 10
        #     max_percentile = 90
        percentile_vals[col]["min"] = np.round(temp[0], 2)
        percentile_vals[col]["max"] = np.round(temp[1], 2)
    return percentile_vals
