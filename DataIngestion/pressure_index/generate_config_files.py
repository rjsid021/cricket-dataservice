import pandas as pd
from argparse import ArgumentParser
import warnings
import numpy as np

from DataIngestion.config import PI_FEATURES_NAME_FILE
from DataIngestion.pressure_index import BybData
from DataIngestion.pressure_index.extract_db_data import generate_and_load_pregame
from DataIngestion.pressure_index.pi import get_byb_pi_features, add_scaled_pi_features
from DataIngestion.pressure_index.byb_utils import create_baseline_df, create_percentile_min_max_vals
from DataIngestion.pressure_index.utils import save_json, read_json
from DataIngestion.pressure_index.scaling import get_best_n_components_svd, get_weights_using_svd_decom


warnings.simplefilter("ignore")


def main():

    parser = ArgumentParser()
    parser.add_argument(
        "--feature_config_file",
        default=PI_FEATURES_NAME_FILE,
        type=str,
    )

    args = parser.parse_args()
    # byb_data = byb_data[byb_data["competition_name"].isin(["IPL"])]

    # byb_data = byb_data[byb_data['match_name']=="CSKVSLKSG03042023"]

    # seasons = ["2023"]
    # byb_data["season"] = byb_data["season"].astype(str)
    # byb_data = byb_data[(byb_data["season"].isin(seasons))]

    byb_data = create_baseline_df(BybData.combine_data())

    (
        df_stadium_runs,
        df_batsman_entrypoint,
        df_batsman_ip,
        df_bowler_stats,
        df_h2h_stats,
        df_h2battype_stats,
    ) = generate_and_load_pregame(BybData.combine_data())

    pi_byb_data = get_byb_pi_features(
        byb_data,
        df_stadium_runs,
        df_batsman_entrypoint,
        df_bowler_stats,
        df_h2h_stats,
        df_h2battype_stats,
    )

    pi_2nd_inn = pi_byb_data[
        (pi_byb_data["innings"] == 2)
        & (pi_byb_data["match_date"] >= pd.to_datetime("Jan 01 2021"))
    ]

    percentile_vals = create_percentile_min_max_vals(
        pi_2nd_inn, args.feature_config_file
    )

    save_json("Data/min_max_scale_vals.json", percentile_vals)

    pi_byb_data = add_scaled_pi_features(
        "Data/min_max_scale_vals.json", pi_byb_data, args.feature_config_file
    )

    # pi_byb_data.to_csv("Data/config_pi_byb_data.csv", index=False)
    # print("saved data in Data/config_pi_byb_data.csv")

    feature_names = read_json(args.feature_config_file)["features"]
    features_df = pi_byb_data[
        (pi_byb_data["innings"] == 2)
        & (pi_byb_data["match_date"] >= pd.to_datetime("Jan 01 2021"))
    ][[i + "_scaled" for i in feature_names]].copy()

    for col in features_df.columns:
        features_df[col] = np.where(features_df[col] > 1, 1, features_df[col])
    # print(features_df.describe())

    n_components = get_best_n_components_svd(features_df.values, 6)
    weights = get_weights_using_svd_decom(n_components, features_df.values)

    feature_weight_scales = {}
    col_names_batsman_pi = features_df.columns
    for i, weight in enumerate(weights):
        feature_weight_scales[col_names_batsman_pi[i]] = weight

    save_json("Data/feature_weights.json", feature_weight_scales)
    print("Data/feature_weights.json")

    feature2_names = [
        "RRR_BY_CRR_wickets_resource_utilized",
        "BowlerEco_SR",
        "OtherBowlersEco_SR",
        "NonstrikerSR"
    ]
    features2_df = pi_byb_data[[i + "_scaled" for i in feature2_names]].copy()

    for col in features2_df.columns:
        features2_df[col] = np.where(features2_df[col] > 1, 1, features2_df[col])

    n_components = get_best_n_components_svd(features2_df.values, 5)
    weights_match_pi = get_weights_using_svd_decom(n_components, features2_df.values)

    feature2_weight_scales = {}
    col_names_batsman_pi = features2_df.columns
    for i, weight in enumerate(weights_match_pi):
        feature2_weight_scales[col_names_batsman_pi[i]] = weight

    save_json("Data/feature_weights_batsman_match_pi.json", feature2_weight_scales)
    print(" Saved in Data/feature_weights_batsman_match_pi.json")


    feature2_names = [
            "RRR_BY_CRR_wickets_resource_utilized",
            "BatSR",
            "NonstrikerSR",
            "OtherBowlersEco_SR"
        ]
    features2_df = pi_byb_data[[i + "_scaled" for i in feature2_names]].copy()

    for col in features2_df.columns:
        features2_df[col] = np.where(features2_df[col] > 1, 1, features2_df[col])

    n_components = get_best_n_components_svd(features2_df.values, 5)
    weights_match_pi = get_weights_using_svd_decom(n_components, features2_df.values)

    feature2_weight_scales = {}
    col_names_batsman_pi = features2_df.columns
    for i, weight in enumerate(weights_match_pi):
        feature2_weight_scales[col_names_batsman_pi[i]] = weight

    save_json("Data/feature_weights_bowler_match_pi.json", feature2_weight_scales)
    print(" Saved in Data/feature_weights_bowler_match_pi.json")


if __name__ == "__main__":
    main()
