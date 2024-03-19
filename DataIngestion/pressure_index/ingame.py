import numpy as np
import pandas as pd

from DataIngestion.config import PI_RESOURCE_UTILIZATION_FILE
from DataIngestion.pressure_index.pregame import get_avg_stadium_runs


def get_req_ingame_batsman_df(
    batsman_arr_diss_series, byb_data, req_batsman_id, batsman_name
):
    required_columns = [
        "match_name",
        "match_date",
        "innings",
        "raw_ball_no",
        "ball_runs",
        "batsman_id",
        "batsman",
        "non_striker_id",
        "non_striker",
        "bowler_id",
        "bowler",
        "is_wide",
        "is_batsman_ball",
        "match_phase",
    ]

    assert all(col in byb_data.columns for col in required_columns)

    if batsman_arr_diss_series["dismissed_raw_ball_no"] == -1:
        df_byb_one_batsman = byb_data[
            (byb_data["raw_ball_no"] >= batsman_arr_diss_series["arrived_raw_ball_no"])
            & (byb_data["innings"] == batsman_arr_diss_series["innings"])
        ]

        req_byb_data_batsman = byb_data[
            (byb_data["innings"] == batsman_arr_diss_series["innings"])
        ]

    else:
        df_byb_one_batsman = byb_data[
            (byb_data["raw_ball_no"] >= batsman_arr_diss_series["arrived_raw_ball_no"])
            & (
                byb_data["raw_ball_no"]
                <= batsman_arr_diss_series["dismissed_raw_ball_no"]
            )
            & (byb_data["innings"] == batsman_arr_diss_series["innings"])
        ]
        req_byb_data_batsman = byb_data[
            (byb_data["innings"] == batsman_arr_diss_series["innings"])
            & (
                byb_data["raw_ball_no"]
                <= batsman_arr_diss_series["dismissed_raw_ball_no"]
            )
        ]

    df_byb_one_batsman["check_issue_with_is_wicket"] = (
                                                               df_byb_one_batsman["batsman_id"] == req_batsman_id
                                                       ) | (df_byb_one_batsman["non_striker_id"] == req_batsman_id)
    df_byb_one_batsman = df_byb_one_batsman[
        df_byb_one_batsman["check_issue_with_is_wicket"] == 1
        ]

    data_batsman = df_byb_one_batsman[required_columns]
    ## striker or not
    data_batsman["is_striker"] = np.where(
        data_batsman["batsman_id"] == req_batsman_id, 1, 0
    )

    # Exponential SR of batsman
    data_batsman["ewm_sr"] = (
        data_batsman[
            (data_batsman["is_striker"] == 1) & (data_batsman["is_batsman_ball"] == 1)
        ]
        .groupby("batsman_id")["ball_runs"]
        .transform(lambda x: x.ewm(alpha=0.35).mean() * 100)
    )

    # No of balls played till now by batsman
    data_batsman["cum_balls_played"] = (
        data_batsman[
            (data_batsman["is_striker"] == 1) & (data_batsman["is_batsman_ball"] == 1)
        ]
        .groupby("batsman_id")["is_batsman_ball"]
        .transform(lambda x: x.cumsum())
    )

    # impute balls runs based on no of balls played (0 from 1 SR > 0 from 10)
    data_batsman["ball_runs_w_impute"] = data_batsman["ball_runs"] + (
        0.1 / (data_batsman["cum_balls_played"])
    )
    # if cum balls played is zero, ball runs has to be nan.
    data_batsman["ball_runs_w_impute"] = np.where(
        data_batsman["cum_balls_played"] == 0,
        np.nan,
        data_batsman["ball_runs_w_impute"],
    )

    # cum batsman rull till now
    data_batsman["cum_batsman_runs"] = (
        data_batsman[
            (data_batsman["is_striker"] == 1) & (data_batsman["is_batsman_ball"] == 1)
        ]
        .groupby("batsman_id")["ball_runs"]
        .transform(lambda x: x.cumsum())
    )

    # cum batsman runs with imputation (0 from 1 SR > 0 from 10)
    data_batsman["cum_batsman_runs_w_impute"] = (
        data_batsman[
            (data_batsman["is_striker"] == 1) & (data_batsman["is_batsman_ball"] == 1)
        ]
        .groupby("batsman_id")["ball_runs_w_impute"]
        .transform(lambda x: x.cumsum())
    )

    ##Exp weighted SR of batsman using imputed ball runs.
    data_batsman["ewm_sr_w_impute"] = (
        data_batsman[
            (data_batsman["is_striker"] == 1) & (data_batsman["is_batsman_ball"] == 1)
        ]
        .groupby("batsman_id")["ball_runs_w_impute"]
        .transform(lambda x: x.ewm(alpha=0.35).mean() * 100)
    )
    data_batsman = data_batsman.fillna(method="ffill")

    # data_batsman = data_batsman.fillna(0)
    data_batsman["cum_SR"] = (
        data_batsman["cum_batsman_runs"] / data_batsman["cum_balls_played"]
    ) * 100
    data_batsman["cum_SR_w_impute"] = (
        data_batsman["cum_batsman_runs_w_impute"] / data_batsman["cum_balls_played"]
    ) * 100

    data_batsman = data_batsman.rename(
        columns={"batsman_id": "striker_id", "batsman": "striker"}
    )
    # batsman_name = data_batsman[data_batsman["striker_id"] == req_batsman_id].iloc[0][
    #     "striker"
    # ]
    data_batsman["req_batsman"] = batsman_name
    data_batsman["req_batsman_id"] = req_batsman_id

    req_byb_data_batsman = get_non_striker_sr(req_byb_data_batsman, req_batsman_id)

    data_batsman = data_batsman.merge(
        req_byb_data_batsman, on=["raw_ball_no"], how="left", validate="1:1"
    )

    return data_batsman


def get_non_striker_sr(req_byb_data_batsman, req_batsman_id):
    ## Getting NonStriker SR
    req_byb_data_batsman["req_batsman_id"] = req_batsman_id

    req_byb_data_batsman["is_striker"] = np.where(
        req_byb_data_batsman["batsman_id"] == req_batsman_id, 1, 0
    )
    req_byb_data_batsman["ewm_nonstriker_sr"] = (
        req_byb_data_batsman[
            (req_byb_data_batsman["is_striker"] != 1)
            & (req_byb_data_batsman["is_batsman_ball"] == 1)
        ]
        .groupby("req_batsman_id")["ball_runs"]
        .transform(lambda x: x.ewm(alpha=0.25).mean() * 100)
    )

    def get_non_striker_balls(row, req_byb_data_batsman):
        return req_byb_data_batsman[
            (req_byb_data_batsman["raw_ball_no"] <= row["raw_ball_no"])
            & (req_byb_data_batsman["batsman_id"] == row["non_striker_id"])
        ]["is_batsman_ball"].sum()

    def get_non_striker_runs(row, req_byb_data_batsman):
        return req_byb_data_batsman[
            (req_byb_data_batsman["raw_ball_no"] <= row["raw_ball_no"])
            & (req_byb_data_batsman["batsman_id"] == row["non_striker_id"])
        ]["ball_runs"].sum()

    def get_non_striker_emw_sr(row, req_byb_data_batsman):
        temp_df = (
            req_byb_data_batsman[
                (req_byb_data_batsman["raw_ball_no"] <= row["raw_ball_no"])
                & (req_byb_data_batsman["batsman_id"] == row["non_striker_id"])
            ]["ball_runs"]
            .ewm(alpha=0.25)
            .mean()
            * 100
        )
        if len(temp_df) >= 1:
            return temp_df.values[-1]
        return 0

    req_byb_data_batsman[
        "non_sr_cum_balls_played_with_wicket_reset"
    ] = req_byb_data_batsman.apply(
        lambda row: get_non_striker_balls(row, req_byb_data_batsman), axis=1
    )
    req_byb_data_batsman[
        "non_sr_cum_runs_scored_with_wicket_reset"
    ] = req_byb_data_batsman.apply(
        lambda row: get_non_striker_runs(row, req_byb_data_batsman), axis=1
    )
    req_byb_data_batsman["non_sr_cum_striker_rate_with_wicket_reset"] = (
        req_byb_data_batsman["non_sr_cum_runs_scored_with_wicket_reset"]
        / req_byb_data_batsman["non_sr_cum_balls_played_with_wicket_reset"]
    ) * 100
    req_byb_data_batsman[
        "non_sr_cum_striker_rate_with_wicket_reset_emw"
    ] = req_byb_data_batsman.apply(
        lambda row: get_non_striker_emw_sr(row, req_byb_data_batsman), axis=1
    )

    req_byb_data_batsman["non_sr_cum_balls_played"] = (
        req_byb_data_batsman[
            (req_byb_data_batsman["is_striker"] != 1)
            & (req_byb_data_batsman["is_batsman_ball"] == 1)
        ]
        .groupby("req_batsman_id")["is_batsman_ball"]
        .transform(lambda x: x.cumsum())
    )

    # cum batsman rull till now
    req_byb_data_batsman["nonstriker_cum_batsman_runs"] = (
        req_byb_data_batsman[
            (req_byb_data_batsman["is_striker"] != 1)
            & (req_byb_data_batsman["is_batsman_ball"] == 1)
        ]
        .groupby("batsman_id")["ball_runs"]
        .transform(lambda x: x.cumsum())
    )

    req_byb_data_batsman["cum_non_striker_sr"] = np.where(
        req_byb_data_batsman["non_sr_cum_balls_played"] == 0,
        np.nan,
        (
            req_byb_data_batsman["nonstriker_cum_batsman_runs"]
            / req_byb_data_batsman["non_sr_cum_balls_played"]
        )
        * 100,
    )

    # impute balls runs based on no of balls played (0 from 1 SR > 0 from 10)
    req_byb_data_batsman["ball_runs_w_impute"] = req_byb_data_batsman["ball_runs"] + (
        0.1 / (req_byb_data_batsman["non_sr_cum_balls_played"])
    )
    # if cum balls played is zero, ball runs has to be nan.
    req_byb_data_batsman["ball_runs_w_impute"] = np.where(
        req_byb_data_batsman["non_sr_cum_balls_played"] == 0,
        np.nan,
        req_byb_data_batsman["ball_runs_w_impute"],
    )

    ##Exp weighted SR of batsman using imputed ball runs.
    req_byb_data_batsman["ewm_nonstriker_sr_w_impute"] = (
        req_byb_data_batsman[
            (req_byb_data_batsman["is_striker"] != 1)
            & (req_byb_data_batsman["is_batsman_ball"] == 1)
        ]
        .groupby("req_batsman_id")["ball_runs_w_impute"]
        .transform(lambda x: x.ewm(alpha=0.25).mean() * 100)
    )
    req_byb_data_batsman = req_byb_data_batsman.fillna(method="ffill")

    req_byb_data_batsman = req_byb_data_batsman[
        [
            "raw_ball_no",
            "ewm_nonstriker_sr",
            "ewm_nonstriker_sr_w_impute",
            "non_sr_cum_balls_played",
            "cum_non_striker_sr",
            "non_sr_cum_balls_played_with_wicket_reset",
            "non_sr_cum_runs_scored_with_wicket_reset",
            "non_sr_cum_striker_rate_with_wicket_reset",
            "non_sr_cum_striker_rate_with_wicket_reset_emw",
        ]
    ]
    return req_byb_data_batsman


def get_other_bower_eco(req_byb_data_bowler, req_bowler_id):
    req_byb_data_bowler["req_bowler_id"] = req_bowler_id
    req_byb_data_bowler["is_bowler"] = np.where(
        req_byb_data_bowler["bowler_id"] == req_bowler_id, 1, 0
    )

    req_byb_data_bowler["other_bowlers_cum_balls"] = (
        req_byb_data_bowler[
            (req_byb_data_bowler["is_bowler"] != 1)
            & (req_byb_data_bowler["is_bowler_ball"] == 1)
        ]
        .groupby("req_bowler_id")["is_bowler_ball"]
        .transform(lambda x: x.cumsum())
    )

    req_byb_data_bowler["other_bowlers_cum_runs"] = (
        req_byb_data_bowler[(req_byb_data_bowler["is_bowler"] != 1)]
        .groupby("req_bowler_id")["bowler_runs"]
        .transform(lambda x: x.cumsum())
    )

    req_byb_data_bowler["other_bowlers_cum_eco"] = np.where(
        req_byb_data_bowler["other_bowlers_cum_balls"] == 0,
        np.nan,
        req_byb_data_bowler["other_bowlers_cum_runs"]
        / (req_byb_data_bowler["other_bowlers_cum_balls"] / 6),
    )

    req_byb_data_bowler["bowler_runs_w_impute"] = req_byb_data_bowler["bowler_runs"] + (
        0.1 / (req_byb_data_bowler["other_bowlers_cum_balls"] + 1)
    )
    # get all wide and no balls.
    is_bowler_ball_zeros = req_byb_data_bowler["is_bowler_ball"] == 0

    # add wide and no balls runs to next ball.
    req_byb_data_bowler["bowler_runs_wo_extra"] = req_byb_data_bowler["bowler_runs"] + (
        (req_byb_data_bowler["bowler_runs"] * is_bowler_ball_zeros).shift(1)
    ).fillna(0)
    req_byb_data_bowler["bowler_runs_wo_extra"] = np.where(
        req_byb_data_bowler["is_bowler_ball"] == 0,
        np.nan,
        req_byb_data_bowler["bowler_runs_wo_extra"],
    )

    req_byb_data_bowler["bowler_runs_wo_extra_w_impute"] = req_byb_data_bowler[
        "bowler_runs_w_impute"
    ] + (
        (req_byb_data_bowler["bowler_runs_w_impute"] * is_bowler_ball_zeros).shift(1)
    ).fillna(
        0
    )

    req_byb_data_bowler["bowler_runs_wo_extra_w_impute"] = np.where(
        req_byb_data_bowler["is_bowler_ball"] == 0,
        np.nan,
        req_byb_data_bowler["bowler_runs_wo_extra_w_impute"],
    )

    req_byb_data_bowler["other_bowler_ewm_eco"] = (
        req_byb_data_bowler[
            (req_byb_data_bowler["is_bowler_ball"] == 1)
            & (req_byb_data_bowler["is_bowler"] != 1)
        ]
        .groupby("req_bowler_id")["bowler_runs_wo_extra"]
        .transform(lambda x: (6 * x).ewm(alpha=0.25, ignore_na=True).mean())
    )

    req_byb_data_bowler["other_bowler_ewm_eco_w_impute"] = (
        req_byb_data_bowler[
            (req_byb_data_bowler["is_bowler_ball"] == 1)
            & (req_byb_data_bowler["is_bowler"] != 1)
        ]
        .groupby("req_bowler_id")["bowler_runs_wo_extra_w_impute"]
        .transform(lambda x: (6 * x).ewm(alpha=0.25, ignore_na=True).mean())
    )

    req_byb_data_bowler = req_byb_data_bowler.fillna(method="ffill")

    req_byb_data_bowler = req_byb_data_bowler[
        [
            "raw_ball_no",
            "other_bowler_ewm_eco",
            "other_bowler_ewm_eco_w_impute",
            "other_bowlers_cum_balls",
            "other_bowlers_cum_eco",
        ]
    ]
    return req_byb_data_bowler


def get_req_ingame_bowler_df(df_bowler_data, byb_data):
    required_columns = [
        "match_date",
        "season",
        "match_name",
        "innings",
        "competition_name",
        "raw_ball_no",
        "bowler_id",
        "bowler",
        "bowler_runs",
        "runs",
        "is_bowler_ball",
        "is_bowler_wicket",
        "is_wide",
        "is_no_ball",
    ]
    assert all(col in df_bowler_data.columns for col in required_columns)
    df_bowler_data = df_bowler_data[required_columns]

    # creating cum balls, runs wickets for bowler.
    df_bowler_data[
        [
            "cum_balls_bowled",
            "cum_bowler_runs",
            "cum_bowler_wickets",
        ]
    ] = df_bowler_data[["is_bowler_ball", "bowler_runs", "is_bowler_wicket"]].cumsum()

    # bowler runs impute based on
    df_bowler_data["bowler_runs_w_impute"] = df_bowler_data["bowler_runs"] + (
        0.1 / (df_bowler_data["cum_balls_bowled"] + 1)
    )

    # cum bowler runs with imputation
    df_bowler_data["cum_bowler_runs_w_impute"] = df_bowler_data[
        "bowler_runs_w_impute"
    ].cumsum()

    # get all wide and no balls.
    is_bowler_ball_zeros = df_bowler_data["is_bowler_ball"] == 0

    # add wide and no balls runs to next ball.
    df_bowler_data["bowler_runs_wo_extra"] = df_bowler_data["bowler_runs"] + (
        (df_bowler_data["bowler_runs"] * is_bowler_ball_zeros).shift(1)
    ).fillna(0)
    df_bowler_data["bowler_runs_wo_extra"] = np.where(
        df_bowler_data["is_bowler_ball"] == 0,
        np.nan,
        df_bowler_data["bowler_runs_wo_extra"],
    )
    df_bowler_data["bowler_runs_wo_extra_w_impute"] = df_bowler_data[
        "bowler_runs_w_impute"
    ] + (
        (df_bowler_data["bowler_runs_w_impute"] * is_bowler_ball_zeros).shift(1)
    ).fillna(
        0
    )

    df_bowler_data["bowler_runs_wo_extra_w_impute"] = np.where(
        df_bowler_data["is_bowler_ball"] == 0,
        np.nan,
        df_bowler_data["bowler_runs_wo_extra_w_impute"],
    )

    df_bowler_data["ewm_eco"] = (
        df_bowler_data[df_bowler_data["is_bowler_ball"] == 1]
        .groupby("bowler_id")["bowler_runs_wo_extra"]
        .transform(lambda x: (6 * x).ewm(alpha=0.35, ignore_na=True).mean())
    )

    df_bowler_data["ewm_eco_w_impute"] = (
        df_bowler_data[df_bowler_data["is_bowler_ball"] == 1]
        .groupby("bowler_id")["bowler_runs_wo_extra_w_impute"]
        .transform(lambda x: (6 * x).ewm(alpha=0.35, ignore_na=True).mean())
    )

    df_bowler_data = df_bowler_data.fillna(method="ffill")

    df_bowler_data["cum_eco"] = np.where(
        df_bowler_data["cum_balls_bowled"] == 0,
        np.nan,
        df_bowler_data["cum_bowler_runs"] / (df_bowler_data["cum_balls_bowled"] / 6),
    )
    df_bowler_data["cum_eco_w_impute"] = np.where(
        df_bowler_data["cum_balls_bowled"] == 0,
        np.nan,
        df_bowler_data["cum_bowler_runs_w_impute"]
        / (df_bowler_data["cum_balls_bowled"] / 6),
    )

    df_bowler_data["cum_sr"] = np.where(
        df_bowler_data["cum_balls_bowled"] == 0,
        np.nan,
        df_bowler_data["cum_balls_bowled"] / df_bowler_data["cum_bowler_wickets"],
    )
    df_bowler_data["cum_sr_w_impute"] = np.where(
        df_bowler_data["cum_balls_bowled"] == 0,
        np.nan,
        (df_bowler_data["cum_balls_bowled"] + 0.5)
        / (df_bowler_data["cum_bowler_wickets"] + 0.5),
    )

    req_bowler_id = df_bowler_data.iloc[0].bowler_id
    innings = df_bowler_data.iloc[0].innings
    req_byb_data_bowler = get_other_bower_eco(
        byb_data[byb_data["innings"] == innings], req_bowler_id
    )
    df_bowler_data = df_bowler_data.merge(
        req_byb_data_bowler, on=["raw_ball_no"], how="left", validate="1:1"
    )
    return df_bowler_data


def get_req_ingame_h2h_df(df_h2h):
    required_columns = [
        "match_date",
        "season",
        "match_name",
        "innings",
        "competition_name",
        "raw_ball_no",
        "bowler_id",
        "bowler",
        "bowler_runs",
        "runs",
        "is_bowler_ball",
        "is_bowler_wicket",
        "is_wide",
        "is_no_ball",
        "batsman_id",
        "batsman",
        "is_batsman_ball",
        "ball_runs",
    ]
    assert all(col in df_h2h.columns for col in required_columns)

    df_h2h = df_h2h[required_columns]

    df_h2h["cum_h2h_balls"] = (
        df_h2h[(df_h2h["is_batsman_ball"] == 1)]
        .groupby(["batsman_id", "bowler_id"])["is_batsman_ball"]
        .transform(lambda x: x.cumsum())
    )

    df_h2h["cum_h2h_runs"] = (
        df_h2h[(df_h2h["is_batsman_ball"] == 1)]
        .groupby(["batsman_id", "bowler_id"])["ball_runs"]
        .transform(lambda x: x.cumsum())
    )

    df_h2h["cum_h2h_wkts"] = (
        df_h2h[(df_h2h["is_batsman_ball"] == 1)]
        .groupby(["batsman_id", "bowler_id"])["is_bowler_wicket"]
        .transform(lambda x: x.cumsum())
    )

    df_h2h["h2h_runs_w_impute"] = df_h2h["ball_runs"] + (
        0.1 / (df_h2h["cum_h2h_balls"])
    )
    df_h2h["h2h_runs_w_impute"] = np.where(
        df_h2h["cum_h2h_balls"] == 0, np.nan, df_h2h["h2h_runs_w_impute"]
    )

    df_h2h["cum_h2h_runs_w_impute"] = (
        df_h2h[(df_h2h["is_batsman_ball"] == 1)]
        .groupby(["batsman_id", "bowler_id"])["h2h_runs_w_impute"]
        .transform(lambda x: x.cumsum())
    )

    df_h2h["cum_h2h_ewm_eco"] = (
        df_h2h[(df_h2h["is_batsman_ball"] == 1)]
        .groupby(["batsman_id", "bowler_id"])["ball_runs"]
        .transform(
            lambda x: (6 * x)
            .ewm(
                alpha=0.35,
            )
            .mean()
        )
    )

    df_h2h["cum_h2h_ewm_eco_w_impute"] = (
        df_h2h[(df_h2h["is_batsman_ball"] == 1)]
        .groupby(["batsman_id", "bowler_id"])["h2h_runs_w_impute"]
        .transform(lambda x: (6 * x).ewm(alpha=0.35).mean())
    )

    df_h2h = df_h2h.fillna(method="ffill")

    df_h2h["cum_h2h_eco"] = np.where(
        df_h2h["cum_h2h_balls"] == 0,
        np.nan,
        df_h2h["cum_h2h_runs"] / (df_h2h["cum_h2h_balls"] / 6),
    )
    df_h2h["cum_h2h_eco_w_impute"] = np.where(
        df_h2h["cum_h2h_balls"] == 0,
        np.nan,
        df_h2h["cum_h2h_runs_w_impute"] / (df_h2h["cum_h2h_balls"] / 6),
    )

    df_h2h["cum_h2h_bsr"] = np.where(
        df_h2h["cum_h2h_balls"] == 0,
        np.nan,
        df_h2h["cum_h2h_balls"] / (df_h2h["cum_h2h_wkts"]),
    )
    df_h2h["cum_h2h_bsr_w_impute"] = np.where(
        df_h2h["cum_h2h_balls"] == 0,
        np.nan,
        (df_h2h["cum_h2h_balls"] + 0.5) / (df_h2h["cum_h2h_wkts"] + 0.5),
    )

    return df_h2h


def get_target_runs_grp(grp):
    if grp.iloc[0]["innings"] == 2:
        grp["new_target"] = grp["target_runs"]
        return grp
    target_runs_final = []
    prev_target_runs = grp["target_runs_inn1"].iloc[0]
    for idx, row in grp.iterrows():
        change_flag = False
        runs_check = row["total_runs_scored"] + 1.5 * row["IRRR"]
        if runs_check >= prev_target_runs:
            change_flag = True
        if row["is_wicket"] == 1:
            change_flag = True
        if (row["over_number"] in [6, 10, 15]) and (row["ball_number"] == 6):
            change_flag = True
        if change_flag:
            mt = max(row["mt1"], row["mt2"])
            new_target = int(
                row["total_runs_scored"] + (100 - row["resource_utilized"]) * mt
            )
            target_runs_final.append(new_target)
            prev_target_runs = new_target
        else:
            target_runs_final.append(prev_target_runs)
    grp["new_target"] = target_runs_final
    return grp


def create_target_runs(
    data_raw, df_stadium_runs, ru_data_path=PI_RESOURCE_UTILIZATION_FILE
):
    df_resource_utilization = pd.read_csv(ru_data_path)
    df_resource_utilization = df_resource_utilization.melt(
        id_vars="over_number", var_name="total_wickets", value_name="resource_utilized"
    )
    df_resource_utilization["over_number"] = 21 - df_resource_utilization["over_number"]
    df_resource_utilization[
        ["total_wickets", "resource_utilized"]
    ] = df_resource_utilization[["total_wickets", "resource_utilized"]].astype(float)
    df_resource_utilization["resource_utilized"] = (
        100 - df_resource_utilization["resource_utilized"]
    )

    data_raw["target_runs_inn1"] = data_raw.apply(
        lambda row: get_avg_stadium_runs(
            row["match_date"], row["stadium_name"], df_stadium_runs, 80
        ),
        axis=1,
    )
    data_raw = data_raw.merge(
        df_resource_utilization,
        on=["over_number", "total_wickets"],
        how="left",
        validate="m:1",
    )
    data_raw["resource_utilized"] = data_raw["resource_utilized"].fillna(100)
    data_raw["mt1"] = data_raw["target_runs_inn1"] / 120
    data_raw["IRRR"] = data_raw["target_runs_inn1"] / (120 / 6)
    # data_raw["mt2"] = data_raw["total_runs_scored"] / data_raw["ball_no_bowler"]
    data_raw["mt2"] = np.where(
        data_raw["ball_no_bowler"] != 0,
        data_raw["total_runs_scored"] / data_raw["ball_no_bowler"],
        0,
    )
    data_raw = data_raw.groupby(["match_name", "innings"]).apply(get_target_runs_grp)
    data_raw["target_runs"] = np.where(
        data_raw["innings"] == 1, data_raw["new_target"], data_raw["target_runs"]
    )
    data_raw["target_runs"] = data_raw.groupby(["match_date", "match_name", "innings"], as_index=False)[
        "target_runs"
    ].fillna(method="ffill")
    data_raw["target_runs"] = data_raw["target_runs"].astype(int)
    data_raw["RRR"] = (data_raw["target_runs"] - data_raw["total_runs_scored"]) / (
        (120 - data_raw["ball_no_bowler"]) / 6
    )
    data_raw["RRR"] = np.where(
        (120 - data_raw["ball_no_bowler"] <= 0),
        (data_raw["target_runs"] - data_raw["total_runs_scored"]) / (1 / 6),
        data_raw["RRR"],
    )
    return data_raw
