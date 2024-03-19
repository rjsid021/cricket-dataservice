from DataIngestion.pressure_index.utils import get_entry_point_group, get_innings_progression
from DataIngestion.pressure_index.byb_utils import get_batsman_pos_arr_dismiss_df
import numpy as np


def get_stadium_data(data_raw, save_path=None):
    data_raw = data_raw.sort_values(
        by=["match_date", "match_name", "innings", "raw_ball_no"]
    )

    data_raw["is_won"] = data_raw["batsman_team_id"] == data_raw["winning_team"]
    data_raw["is_won"] = data_raw["is_won"].astype(int)
    stadium_runs = (
        data_raw.groupby(["match_date", "venue_id", "stadium_name", "innings"])
        .agg({"runs": "sum", "is_won": "max"})
        .reset_index()
    )
    stadium_runs = stadium_runs.sort_values(["stadium_name", "match_date"])
    # if save_path is not None:
    #     path_val = os.path.join(save_path, "stadium_stats.csv")
    #     stadium_runs.to_csv(path_val, index=False)
    #     print("saved in", path_val)
    return stadium_runs


def create_batsman_stats(data_raw, save_path=None):
    required_columns = [
        "competition_name",
        "ball_runs",
        "ball_number",
        "innings",
        "season",
        "match_name",
        "batsman_id",
        "out_batsman_id",
        "non_striker_id",
        "is_batsman_ball",
        "raw_ball_no",
        "batsman",
        "non_striker",
        "over_number",
        "match_date",
        "is_wicket",
        "is_four",
        "is_six",
        "striker_batting_type",
        "non_striker_batting_type",
    ]
    assert all(col in data_raw.columns for col in required_columns)
    data_raw = data_raw[required_columns]
    data_raw["is_boundary"] = data_raw["is_four"] | data_raw["is_six"]
    df_batsman_arr_dismissed = (
        data_raw.groupby(
            ["match_date", "season", "competition_name", "match_name", "innings"]
        )
        .apply(get_batsman_pos_arr_dismiss_df)
        .reset_index()
        .drop(["level_5"], axis=1)
    )

    filtered_df = data_raw[(data_raw["is_batsman_ball"] == 1)].reset_index(drop=True)

    data_raw["batsman_ball_number"] = (
        filtered_df
        .groupby(["match_name", "batsman_id"])["is_batsman_ball"]
        .transform(lambda x: x.cumsum())
    )

    data_raw["IP"] = data_raw["batsman_ball_number"].apply(get_innings_progression)

    df_batsman_ip = (
        data_raw.groupby(
            ["match_date", "match_name", "batsman_id", "batsman", "innings", "IP"]
        )
        .agg({"ball_runs": "sum", "is_batsman_ball": "sum", "is_boundary": "sum"})
        .reset_index()
        .rename(
            columns={
                "ball_runs": "runs",
                "is_batsman_ball": "balls",
                "is_boundary": "boundaries",
            }
        )
    )

    df_batsman_entrypoint = (
        data_raw.groupby(
            ["match_date", "match_name", "batsman_id", "batsman", "innings"]
        )
        .agg({"ball_runs": "sum", "is_batsman_ball": "sum", "is_boundary": "sum"})
        .reset_index()
        .rename(
            columns={
                "ball_runs": "runs",
                "is_batsman_ball": "balls",
                "is_boundary": "boundaries",
            }
        )
    )
    df_batsman_entrypoint["SR"] = (
        (df_batsman_entrypoint["runs"] + 0.1) / (df_batsman_entrypoint["balls"] + 0.1)
    ) * 100

    # Code to Handle Duplicates coming in QA env at time of ingestion.
    df_batsman_entrypoint = df_batsman_entrypoint.drop_duplicates(subset=["match_date", "match_name", "batsman_id"])
    df_batsman_arr_dismissed = df_batsman_arr_dismissed.drop_duplicates(
        subset=["match_date", "match_name", "batsman_id"])

    df_batsman_entrypoint = df_batsman_entrypoint.merge(
        df_batsman_arr_dismissed[
            ["match_date", "match_name", "batsman_id", "arrived_on", "dismissed_on"]
        ],
        on=["match_date", "match_name", "batsman_id"],
        how="left",
        validate="1:1",
    )
    df_batsman_entrypoint["entry_point"] = df_batsman_entrypoint["arrived_on"].apply(
        get_entry_point_group
    )
    df_batsman_entrypoint = df_batsman_entrypoint.sort_values(
        ["batsman_id", "match_date"], ascending=False
    )
    df_batsman_ip = df_batsman_ip.sort_values(
        ["batsman_id", "match_date"], ascending=False
    )

    # if save_path is not None:
    #     path_val_ip = os.path.join(save_path, "batsman_innings_progression.csv")
    #     path_val_ep = os.path.join(save_path, "batsman_entrypoints.csv")
    #     df_batsman_ip.to_csv(path_val_ip, index=False)
    #     print("saved in", path_val_ip)
    #     df_batsman_entrypoint.to_csv(path_val_ep, index=False)
    #     print("saved in", path_val_ep)
    return df_batsman_entrypoint, df_batsman_ip


def create_bowler_stats(data_raw, save_path=None):
    required_columns = [
        "match_date",
        "match_name",
        "competition_name",
        "innings",
        "season",
        "bowler_id",
        "bowler",
        "match_phase",
        "bowler_runs",
        "is_bowler_ball",
        "is_bowler_wicket",
        "is_four",
        "is_six",
        "is_dot_ball",
    ]
    assert all(col in data_raw.columns for col in required_columns)
    data_raw = data_raw[required_columns]

    data_raw["is_boundary"] = data_raw["is_four"] | data_raw["is_six"]

    df_bowler_stats = (
        data_raw.groupby(
            [
                "match_date",
                "match_name",
                "bowler_id",
                "bowler",
                "innings",
                "match_phase",
            ]
        )
        .agg(
            {
                "bowler_runs": "sum",
                "is_bowler_ball": "sum",
                "is_bowler_wicket": "sum",
                "is_boundary": "sum",
                "is_dot_ball": "sum",
            }
        )
        .reset_index()
        .rename(
            columns={
                "bowler_runs": "runs",
                "is_bowler_ball": "balls",
                "is_boundary": "boundaries",
                "is_dot_ball": "dotballs",
                "is_bowler_wicket": "wkts",
            }
        )
    )
    df_bowler_stats["eco"] = (df_bowler_stats["runs"] + 0.1) / (
        (df_bowler_stats["balls"] + 0.1) / 6
    )
    df_bowler_stats["sr"] = (df_bowler_stats["balls"] + 0.5) / (
        df_bowler_stats["wkts"] + 0.5
    )
    df_bowler_stats = df_bowler_stats.sort_values(
        ["bowler_id", "match_date", "match_phase"], ascending=False
    )
    # if save_path is not None:
    #     path_val = os.path.join(save_path, "bowler_phase_stats.csv")
    #     df_bowler_stats.to_csv(path_val, index=False)
    #     print("saved in", path_val)

    return df_bowler_stats


def create_h2h_stats(data_raw, save_path=None):
    required_columns = [
        "match_date",
        "match_name",
        "competition_name",
        "innings",
        "season",
        "bowler_id",
        "bowler",
        "batsman_id",
        "batsman",
        "match_phase",
        "bowler_runs",
        "ball_runs",
        "is_bowler_ball",
        "is_bowler_wicket",
        "is_batsman_ball",
        "is_four",
        "is_six",
        "is_dot_ball",
        "striker_batting_type",
    ]
    assert all(col in data_raw.columns for col in required_columns)
    data_raw = data_raw[required_columns]

    data_raw["is_boundary"] = data_raw["is_four"] | data_raw["is_six"]

    df_h2h_stats = (
        data_raw.groupby(
            [
                "match_date",
                "match_name",
                "batsman_id",
                "batsman",
                "bowler_id",
                "bowler",
                "innings",
                "match_phase",
            ]
        )
        .agg(
            {
                "ball_runs": "sum",
                "is_batsman_ball": "sum",
                "is_bowler_wicket": "sum",
                "is_boundary": "sum",
                "is_dot_ball": "sum",
            }
        )
        .reset_index()
        .rename(
            columns={
                "ball_runs": "runs",
                "is_batsman_ball": "balls",
                "is_boundary": "boundaries",
                "is_dot_ball": "dotballs",
                "is_bowler_wicket": "wkts",
            }
        )
    )
    df_h2h_stats["eco"] = (df_h2h_stats["runs"] + 0.1) / (
        (df_h2h_stats["balls"] + 0.1) / 6
    )
    df_h2h_stats["bowl_sr"] = (df_h2h_stats["balls"] + 0.5) / (
        df_h2h_stats["wkts"] + 0.5
    )
    df_h2h_stats["bat_sr"] = (
        (df_h2h_stats["runs"] + 0.1) / (df_h2h_stats["balls"] + 0.1)
    ) * 100
    df_h2h_stats = df_h2h_stats.sort_values(
        ["batsman_id", "bowler_id", "match_date", "match_phase"], ascending=False
    )

    df_h2battype_stats = (
        data_raw.groupby(
            [
                "match_date",
                "match_name",
                "bowler_id",
                "bowler",
                "striker_batting_type",
                "innings",
                "match_phase",
            ]
        )
        .agg(
            {
                "ball_runs": "sum",
                "is_batsman_ball": "sum",
                "is_bowler_wicket": "sum",
                "is_boundary": "sum",
                "is_dot_ball": "sum",
            }
        )
        .reset_index()
        .rename(
            columns={
                "ball_runs": "runs",
                "is_batsman_ball": "balls",
                "is_boundary": "boundaries",
                "is_dot_ball": "dotballs",
                "is_bowler_wicket": "wkts",
            }
        )
    )

    df_h2battype_stats["eco"] = (df_h2battype_stats["runs"] + 0.1) / (
        (df_h2battype_stats["balls"] + 0.1) / 6
    )
    df_h2battype_stats["bowl_sr"] = (df_h2battype_stats["balls"] + 0.5) / (
        df_h2battype_stats["wkts"] + 0.5
    )
    df_h2battype_stats["bat_sr"] = (
        (df_h2battype_stats["runs"] + 0.1) / (df_h2battype_stats["balls"] + 0.1)
    ) * 100

    df_h2battype_stats = df_h2battype_stats.sort_values(
        ["bowler_id", "striker_batting_type", "match_date", "match_phase"],
        ascending=False,
    )

    # if save_path is not None:
    #     path_val = os.path.join(save_path, "h2h_phase_stats.csv")
    #     df_h2h_stats.to_csv(path_val, index=False)
    #     print("saved in", path_val)
    #
    #     path_val = os.path.join(save_path, "h2battype_phase_stats.csv")
    #     df_h2battype_stats.to_csv(path_val, index=False)
    #     print("saved in", path_val)

    return df_h2h_stats, df_h2battype_stats


# def get_avg_stadium_runs(date, stadium_name, df_stadium_runs, percentile_val=50):
#     df_stadium_runs_v1 = df_stadium_runs[
#         (df_stadium_runs["stadium_name"] == stadium_name)
#         & (df_stadium_runs["match_date"] < date)
#         & (df_stadium_runs["innings"] == 1)
#     ]
#     df_stadium_runs_v1 = df_stadium_runs_v1.sort_values("match_date", ascending=False)
#     if len(df_stadium_runs_v1) != 0:
#         dist_runs = df_stadium_runs_v1["runs"].values[:50]
#         return dist_runs.mean() + dist_runs.std()
#     # else:
#     #     df_stadium_runs_v1 = df_stadium_runs[
#     #         (df_stadium_runs["stadium_name"] == stadium_name)
#     #         & (df_stadium_runs["match_date"] < date)
#     #         & (df_stadium_runs["innings"] == 1)
#     #         & (df_stadium_runs["is_won"] == 1)
#     #     ]
#     #     df_stadium_runs_v1 = df_stadium_runs_v1.sort_values(
#     #         "match_date", ascending=False
#     #     )
#     #     if len(df_stadium_runs_v1) != 0:
#     #         dist_runs = df_stadium_runs_v1["runs"].values[:50]
#     #         return dist_runs.mean() + dist_runs.std()
#     return 150


def get_avg_stadium_runs(date, stadium_name, df_stadium_runs, percentile_val=80):
    df_stadium_runs_v1 = df_stadium_runs[
        (df_stadium_runs["stadium_name"] == stadium_name)
        & (df_stadium_runs["match_date"] < date)
        & (df_stadium_runs["innings"] == 1)
    ]
    df_stadium_runs_v1 = df_stadium_runs_v1.sort_values("match_date", ascending=False)
    if len(df_stadium_runs_v1) != 0:
        dist_runs = df_stadium_runs_v1["runs"].values[:50]
        bootstrapped_means = []
        bootstrapped_stds = []
        np.random.seed(20)
        for _ in range(100):
            bootstrap_sample = np.random.choice(
                dist_runs, size=len(dist_runs), replace=True
            )
            bootstrapped_means.append(np.mean(bootstrap_sample))
            bootstrapped_stds.append(np.std(bootstrap_sample))
        stadium_mean = np.percentile(bootstrapped_means, 80)
        stadium_std = np.percentile(bootstrapped_stds, 80)
        return int(stadium_mean + stadium_std)
    return 150
