import numpy as np
import pandas as pd

from DataIngestion import load_timestamp
from DataIngestion.config import PI_RESOURCE_UTILIZATION_FILE, PI_CONFIG_DATA_PATH, PRESSURE_INDEX_TABLE_NAME, \
    PRESSURE_INDEX_KEY_COL, PI_MAX_ID
from DataIngestion.pressure_index.byb_utils import create_baseline_df, get_batsman_pos_arr_dismiss_df
from DataIngestion.pressure_index.extract_db_data import generate_and_load_pregame
from DataIngestion.pressure_index.ingame import (
    get_req_ingame_batsman_df,
    get_req_ingame_bowler_df,
    get_req_ingame_h2h_df,
    create_target_runs,
)
from DataIngestion.pressure_index.utils import (
    get_mean_using_bs,
    get_w_progress,
    get_extra_val,
    read_json,
    balls_to_settle_factor, winsorized_mean, get_pi_category, get_bowler_pi_category,
)
from DataIngestion.utils.helper import generateSeq
from common.dao.fetch_db_data import getMaxId
from common.dao_client import session
from common.db_config import DB_NAME
from log.log import get_logger

logger = get_logger("Ingestion", "Ingestion")


def create_pi_raw_features_for_batter(
        byb_data,
        batsman_arr_diss_series,
        batter_ingame_df,
        bowler_ingame_df,
        h2h_ingame_df,
        batsman_entrypoint_df,
        bowler_match_stats_df,
        h2h_match_stats_df,
        h2battype_match_stats_df,
):
    """Creates the raw featires needed for PI for one batsman in a match

    Parameters
    ----------
    byb_data : pd.DataFrame
        a ball by ball data after create_baseline_df function on byb.
    batsman_arr_diss_series : pd.Series
        a pd series contains arrival dismissal balls with basic batsman data.
    batter_ingame_df : pd.DataFrame
        a pd DataFrame contains only one batsman ingame info.
    bowler_ingame_df : pd.DataFrame
        a pd DataFrame contains one match/one innings bowlers ingame info.
    h2h_ingame_df : pd.DataFrame
        a pd DataFrame contains one match/one innings H2H ingame info.
    batsman_entrypoint_df : pd.DataFrame
        all entrypoints data of required batsmen for each match (pregame).
    bowler_match_stats_df : pd.DataFrame
        required bowlers phase level stats for all pregame matches.
    h2h_match_stats_df : pd.DataFrame
        required batsman-bowler(h2h) phase level stats for all pregame matches.
    h2battype_match_stats_df : pd.DataFrame
        required bowler-bat type(h2battype) phase level stats for all pregame matches.
    """
    final_bat = []
    # loop a batsmans ingame df contains batsman raw features for byb.
    for idx, row_bat in batter_ingame_df.iterrows():
        temp_dict = {}
        # Copy match-related information from the batting row
        temp_dict["match_date"] = row_bat["match_date"]
        temp_dict["match_name"] = row_bat["match_name"]
        temp_dict["innings"] = row_bat["innings"]
        temp_dict["raw_ball_no"] = row_bat["raw_ball_no"]

        # Copy batsman and bowler information
        temp_dict["req_batsman_id"] = row_bat["req_batsman_id"]
        temp_dict["req_batsman"] = row_bat["req_batsman"]
        temp_dict["striker_id"] = row_bat["striker_id"]
        temp_dict["striker"] = row_bat["striker"]
        temp_dict["non_striker_id"] = row_bat["non_striker_id"]
        temp_dict["non_striker"] = row_bat["non_striker"]
        temp_dict["bowler_id"] = row_bat["bowler_id"]
        temp_dict["bowler"] = row_bat["bowler"]

        # Copy entry point and match phase information
        temp_dict["entry_point"] = batsman_arr_diss_series["entry_point"]
        temp_dict["match_phase"] = row_bat["match_phase"]

        # get previous batsman ball info
        row_bat_prev = batter_ingame_df[
            batter_ingame_df["raw_ball_no"] == row_bat["raw_ball_no"] - 1
            ]
        row_bat_prev = row_bat_prev.iloc[0] if len(row_bat_prev) != 0 else None

        row_bat_current = batter_ingame_df[
            batter_ingame_df["raw_ball_no"] == row_bat["raw_ball_no"]
            ]
        row_bat_current = row_bat_current.iloc[0] if len(row_bat_current) != 0 else None

        # get previous ball by ball data
        row_byb_prev = byb_data[
            (byb_data["match_name"] == temp_dict["match_name"])
            & (byb_data["innings"] == temp_dict["innings"])
            & (byb_data["raw_ball_no"] == row_bat["raw_ball_no"] - 1)
            ]
        row_byb_prev = row_byb_prev.iloc[0] if len(row_byb_prev) != 0 else None

        row_byb_current = byb_data[
            (byb_data["match_name"] == temp_dict["match_name"])
            & (byb_data["innings"] == temp_dict["innings"])
            & (byb_data["raw_ball_no"] == row_bat["raw_ball_no"])
            ]
        row_byb_current = row_byb_current.iloc[0] if len(row_byb_current) != 0 else None

        # get previus bowler ball data
        row_bowl_prev = bowler_ingame_df[
            (bowler_ingame_df["bowler_id"] == row_bat["bowler_id"])
            & (bowler_ingame_df["raw_ball_no"] < row_bat["raw_ball_no"])
            ].sort_values(["raw_ball_no"], ascending=False)
        row_bowl_prev = row_bowl_prev.iloc[0] if len(row_bowl_prev) != 0 else None

        row_bowl_current = bowler_ingame_df[
            (bowler_ingame_df["bowler_id"] == row_bat["bowler_id"])
            & (bowler_ingame_df["raw_ball_no"] <= row_bat["raw_ball_no"])
            ].sort_values(["raw_ball_no"], ascending=False)
        row_bowl_current = (
            row_bowl_current.iloc[0] if len(row_bowl_current) != 0 else None
        )

        # get previous h2h stat data
        row_h2h_prev = h2h_ingame_df[
            (h2h_ingame_df["bowler_id"] == row_bat["bowler_id"])
            & (h2h_ingame_df["batsman_id"] == row_bat["req_batsman_id"])
            & (h2h_ingame_df["raw_ball_no"] < row_bat["raw_ball_no"])
            ].sort_values(["raw_ball_no"], ascending=False)
        row_h2h_prev = row_h2h_prev.iloc[0] if len(row_h2h_prev) != 0 else None

        if row_byb_current is not None:
            temp_dict["ingame_CRR_after"] = row_byb_current["CRR"]
            temp_dict["ingame_RRR_after"] = row_byb_current["RRR"]
            temp_dict["ingame_RSR_after"] = (100 * row_byb_current["RRR"]) / 6
            temp_dict["ingame_totalwkts_after"] = row_byb_current["total_wickets"]
            temp_dict["ingame_resource_utilized_after"] = row_byb_current[
                "resource_utilized"
            ]
            temp_dict["byb_id"] = row_byb_current["id"]

        if row_bat_current is not None:
            temp_dict["ingame_BatsamnSR_after"] = row_bat_current["cum_SR"]
            temp_dict["ingame_Ewm_BatsmanSR_after"] = row_bat_current["ewm_sr"]
            temp_dict["ingame_BatsamnSR_w_impute_after"] = row_bat_current[
                "cum_SR_w_impute"
            ]
            temp_dict["ingame_Ewm_BatsmanSR_w_impute_after"] = row_bat_current[
                "ewm_sr_w_impute"
            ]
            temp_dict["ingame_total_BatsmanRuns_after"] = row_bat_current[
                "cum_batsman_runs"
            ]
            temp_dict["ingame_total_BatsmanBalls_after"] = row_bat_current[
                "cum_balls_played"
            ]
            temp_dict["ingame_NonStrikerSR_after"] = row_bat_current[
                "ewm_nonstriker_sr"
            ]
            temp_dict["ingame_NonStrikerSR_w_impute_after"] = row_bat_current[
                "ewm_nonstriker_sr_w_impute"
            ]
            temp_dict["ingame_nonstriker_balls_tillnow_after"] = row_bat_current[
                "non_sr_cum_balls_played"
            ]
            temp_dict["ingame_cum_non_striker_sr_after"] = row_bat_current[
                "non_sr_cum_balls_played"
            ]
            temp_dict[
                "ingame_nonstriker_balls_with_wicket_reset_after"
            ] = row_bat_current["non_sr_cum_balls_played_with_wicket_reset"]
            temp_dict[
                "ingame_nonstriker_runs_with_wicket_reset_after"
            ] = row_bat_current["non_sr_cum_runs_scored_with_wicket_reset"]
            temp_dict[
                "ingame_nonstriker_sr_with_wicket_reset_after"
            ] = row_bat_current["non_sr_cum_striker_rate_with_wicket_reset"]
            temp_dict[
                "ingame_emw_nonstriker_sr_with_wicket_reset_after"
            ] = row_bat_current["non_sr_cum_striker_rate_with_wicket_reset_emw"]

        else:
            temp_dict["ingame_NonStrikerSR_after"] = np.nan
            temp_dict["ingame_NonStrikerSR_w_impute_after"] = np.nan
            temp_dict["ingame_nonstriker_balls_tillnow_after"] = np.nan
            temp_dict["ingame_cum_non_striker_sr_after"] = np.nan

        if row_bowl_current is not None:
            temp_dict["ingame_BowlerEco_after"] = row_bowl_current["cum_eco"]
            temp_dict["ingame_Ewm_BowlerEco_after"] = row_bowl_current["ewm_eco"]

            temp_dict["ingame_BowlerEco_w_impute_after"] = row_bowl_current[
                "cum_eco_w_impute"
            ]
            temp_dict["ingame_Ewm_BowlerEco_w_impute_after"] = row_bowl_current[
                "ewm_eco_w_impute"
            ]

            temp_dict["ingame_BowlerSR_after"] = row_bowl_current["cum_sr"]
            temp_dict["ingame_BowlerSR_w_impute_after"] = row_bowl_current[
                "cum_sr_w_impute"
            ]

            temp_dict["ingame_total_BowlerRuns_after"] = row_bowl_current[
                "cum_bowler_runs"
            ]
            temp_dict["ingame_total_BowlerWkts_after"] = row_bowl_current[
                "cum_bowler_wickets"
            ]
            temp_dict["ingame_total_BowlerBalls_after"] = row_bowl_current[
                "cum_balls_bowled"
            ]
            temp_dict["prev_bowler_raw_ball_no_after"] = row_bowl_current["raw_ball_no"]

            temp_dict["ingame_Other_BowlerEco_after"] = row_bowl_current[
                "other_bowler_ewm_eco"
            ]
            temp_dict["ingame_Other_BowlerEco_w_impute_after"] = row_bowl_current[
                "other_bowler_ewm_eco_w_impute"
            ]
            temp_dict["ingame_otherbowler_balls_tillnow_after"] = row_bowl_current[
                "other_bowlers_cum_balls"
            ]
            temp_dict["ingame_otherbowler_cum_eco_after"] = row_bowl_current[
                "other_bowlers_cum_eco"
            ]
        else:
            temp_dict["ingame_Other_BowlerEco_after"] = np.nan
            temp_dict["ingame_Other_BowlerEco_w_impute_after"] = np.nan
            temp_dict["ingame_otherbowler_balls_tillnow_after"] = np.nan
            temp_dict["ingame_otherbowler_cum_eco_after"] = np.nan

        if row_byb_prev is not None:
            # copy ingame match features after previous ball (till present ball)
            temp_dict["prev_ball_runs"] = row_byb_prev["runs"]
            temp_dict["ingame_CRR"] = row_byb_prev["CRR"]
            temp_dict["ingame_RRR"] = row_byb_prev["RRR"]
            temp_dict["ingame_RSR"] = (100 * row_byb_prev["RRR"]) / 6
            temp_dict["ingame_totalwkts"] = row_byb_prev["total_wickets"]
            temp_dict["ingame_resource_utilized"] = row_byb_prev["resource_utilized"]
            temp_dict["ingame_totalruns"] = row_byb_prev["total_runs_scored"]
            temp_dict["previous_ball_number"] = row_byb_prev["ball_no_bowler"]
            temp_dict["previous_raw_ball_number"] = row_byb_prev["raw_ball_no"]
            temp_dict["is_striker"] = row_bat["is_striker"]
            # temp_dict["byb_id"] = row_byb_prev["id"]

            # ------------------------------------
            # Batsman Ingame data
            # ------------------------------------
            if row_bat_prev is not None:
                # copy ingame batsman features after previous ball (till present ball)
                temp_dict["ingame_BatsamnSR"] = row_bat_prev["cum_SR"]
                temp_dict["ingame_Ewm_BatsmanSR"] = row_bat_prev["ewm_sr"]

                temp_dict["ingame_BatsamnSR_w_impute"] = row_bat_prev["cum_SR_w_impute"]
                temp_dict["ingame_Ewm_BatsmanSR_w_impute"] = row_bat_prev[
                    "ewm_sr_w_impute"
                ]

                temp_dict["ingame_total_BatsmanRuns"] = row_bat_prev["cum_batsman_runs"]
                temp_dict["prev_ball_BatsmanRuns"] = row_bat_prev["ball_runs"]
                temp_dict["ingame_total_BatsmanBalls"] = row_bat_prev[
                    "cum_balls_played"
                ]

                temp_dict["ingame_NonStrikerSR"] = row_bat_prev["ewm_nonstriker_sr"]
                temp_dict["ingame_NonStrikerSR_w_impute"] = row_bat_prev[
                    "ewm_nonstriker_sr_w_impute"
                ]
                temp_dict["ingame_nonstriker_balls_tillnow"] = row_bat_prev[
                    "non_sr_cum_balls_played"
                ]
                temp_dict["ingame_cum_non_striker_sr"] = row_bat_prev[
                    "non_sr_cum_balls_played"
                ]
                temp_dict["ingame_nonstriker_balls_with_wicket_reset"] = row_bat_prev[
                    "non_sr_cum_balls_played_with_wicket_reset"
                ]
                temp_dict[
                    "ingame_nonstriker_balls_with_wicket_reset"
                ] = row_bat_prev["non_sr_cum_balls_played_with_wicket_reset"]
                temp_dict[
                    "ingame_nonstriker_runs_with_wicket_reset"
                ] = row_bat_prev["non_sr_cum_runs_scored_with_wicket_reset"]
                temp_dict[
                    "ingame_nonstriker_sr_with_wicket_reset"
                ] = row_bat_prev["non_sr_cum_striker_rate_with_wicket_reset"]
                temp_dict[
                    "ingame_emw_nonstriker_sr_with_wicket_reset"
                ] = row_bat_prev["non_sr_cum_striker_rate_with_wicket_reset_emw"]
            else:
                temp_dict["ingame_NonStrikerSR"] = np.nan
                temp_dict["ingame_NonStrikerSR_w_impute"] = np.nan
                temp_dict["ingame_nonstriker_balls_tillnow"] = np.nan
                temp_dict["ingame_cum_non_striker_sr"] = np.nan

                # total_rsb = (
                #     0
                #     if np.isnan(row_byb_prev["total_runs_scored_by_bat"])
                #     else row_byb_prev["total_runs_scored_by_bat"]
                # )
                # ingame_total_br = (
                #     0
                #     if np.isnan(temp_dict["ingame_total_BatsmanRuns"])
                #     else temp_dict["ingame_total_BatsmanRuns"]
                # )
                # prev_ball_no = (
                #     0
                #     if np.isnan(row_byb_prev["total_balls_by_bat"])
                #     else row_byb_prev["total_balls_by_bat"]
                # )
                # ingame_total_bb = (
                #     0
                #     if np.isnan(temp_dict["ingame_total_BatsmanBalls"])
                #     else temp_dict["ingame_total_BatsmanBalls"]
                # )

                # temp_dict["ingame_NonStrikerSR"] = (
                #     (total_rsb - ingame_total_br) / (prev_ball_no - ingame_total_bb)
                # ) * 100
                # temp_dict["ingame_NonStrikerSR_w_impute"] = (
                #     (total_rsb - ingame_total_br + 0.1)
                #     / (prev_ball_no - ingame_total_bb)
                # ) * 100

                # if np.isinf(temp_dict["ingame_NonStrikerSR"]):
                #     temp_dict["ingame_NonStrikerSR"] = np.nan
                # if np.isinf(temp_dict["ingame_NonStrikerSR_w_impute"]):
                #     temp_dict["ingame_NonStrikerSR_w_impute"] = np.nan

            # if len(req_byb_data_batsman) != 0:
            #     temp_dict["ingame_NonStrikerSR"] = (
            #         req_byb_data_batsman["ball_runs"].ewm(alpha=0.3).mean().values[-1]
            #         * 100
            #     )

            #     req_byb_data_batsman["cum_balls_played"] = req_byb_data_batsman[
            #         "is_batsman_ball"
            #     ].cumsum()
            #     req_byb_data_batsman["ball_runs_w_impute"] = req_byb_data_batsman[
            #         "ball_runs"
            #     ] + (0.1 / (req_byb_data_batsman["cum_balls_played"]))
            #     req_byb_data_batsman["ball_runs_w_impute"] = np.where(
            #         req_byb_data_batsman["cum_balls_played"] == 0,
            #         np.nan,
            #         req_byb_data_batsman["ball_runs_w_impute"],
            #     )
            #     temp_dict["ingame_NonStrikerSR_w_impute"] = (
            #         req_byb_data_batsman["ball_runs_w_impute"]
            #         .ewm(alpha=0.3)
            #         .mean()
            #         .values[-1]
            #         * 100
            #     )

            #     temp_dict["#nonstriker_balls_tillnow"] = len(req_byb_data_batsman)

            # else:
            #     temp_dict["ingame_NonStrikerSR"] = np.nan
            #     temp_dict["ingame_NonStrikerSR_w_impute"] = np.nan
            #     temp_dict["#nonstriker_balls_tillnow"] = np.nan

            # ------------------------------------
            # Bowler Ingame data
            # ------------------------------------
            if row_bowl_prev is not None:
                # copy ingame bowler features after previous ball (till present ball)
                temp_dict["ingame_BowlerEco"] = row_bowl_prev["cum_eco"]
                temp_dict["ingame_Ewm_BowlerEco"] = row_bowl_prev["ewm_eco"]

                temp_dict["ingame_BowlerEco_w_impute"] = row_bowl_prev[
                    "cum_eco_w_impute"
                ]
                temp_dict["ingame_Ewm_BowlerEco_w_impute"] = row_bowl_prev[
                    "ewm_eco_w_impute"
                ]

                temp_dict["ingame_BowlerSR"] = row_bowl_prev["cum_sr"]
                temp_dict["ingame_BowlerSR_w_impute"] = row_bowl_prev["cum_sr_w_impute"]

                temp_dict["ingame_total_BowlerRuns"] = row_bowl_prev["cum_bowler_runs"]
                temp_dict["ingame_total_BowlerWkts"] = row_bowl_prev[
                    "cum_bowler_wickets"
                ]
                temp_dict["ingame_total_BowlerBalls"] = row_bowl_prev[
                    "cum_balls_bowled"
                ]
                temp_dict["prev_bowler_raw_ball_no"] = row_bowl_prev["raw_ball_no"]

                temp_dict["ingame_Other_BowlerEco"] = row_bowl_prev[
                    "other_bowler_ewm_eco"
                ]
                temp_dict["ingame_Other_BowlerEco_w_impute"] = row_bowl_prev[
                    "other_bowler_ewm_eco_w_impute"
                ]
                temp_dict["ingame_otherbowler_balls_tillnow"] = row_bowl_prev[
                    "other_bowlers_cum_balls"
                ]
                temp_dict["ingame_otherbowler_cum_eco"] = row_bowl_prev[
                    "other_bowlers_cum_eco"
                ]
            else:
                temp_dict["ingame_Other_BowlerEco"] = np.nan
                temp_dict["ingame_Other_BowlerEco_w_impute"] = np.nan
                temp_dict["ingame_otherbowler_balls_tillnow"] = np.nan
                temp_dict["ingame_otherbowler_cum_eco"] = np.nan

                # total_rgbb = (
                #     0
                #     if np.isnan(row_byb_prev["total_runs_given_by_bowler"])
                #     else row_byb_prev["total_runs_given_by_bowler"]
                # )
                # ingame_total_bowlr = (
                #     0
                #     if np.isnan(temp_dict["ingame_total_BowlerRuns"])
                #     else temp_dict["ingame_total_BowlerRuns"]
                # )
                # prev_ball_no = (
                #     0
                #     if np.isnan(row_byb_prev["ball_no_bowler"])
                #     else row_byb_prev["ball_no_bowler"]
                # )
                # ingame_total_bowlb = (
                #     0
                #     if np.isnan(temp_dict["ingame_total_BowlerBalls"])
                #     else temp_dict["ingame_total_BowlerBalls"]
                # )

                # temp_dict["ingame_Other_BowlerEco"] = (
                #     total_rgbb - ingame_total_bowlr
                # ) / ((prev_ball_no - ingame_total_bowlb) / 6)
                # temp_dict["ingame_Other_BowlerEco_w_impute"] = (
                #     total_rgbb - ingame_total_bowlr + 0.1
                # ) / ((prev_ball_no - ingame_total_bowlb) / 6)

                # if np.isinf(temp_dict["ingame_Other_BowlerEco"]):
                #     temp_dict["ingame_Other_BowlerEco"] = np.nan
                # if np.isinf(temp_dict["ingame_Other_BowlerEco_w_impute"]):
                #     temp_dict["ingame_Other_BowlerEco_w_impute"] = np.nan
            # if len(req_byb_data_bowler) != 0:
            #     req_byb_data_bowler[
            #         [
            #             "cum_balls_bowled",
            #             "cum_bowler_runs",
            #         ]
            #     ] = req_byb_data_bowler[["is_bowler_ball", "bowler_runs"]].cumsum()

            #     req_byb_data_bowler["bowler_runs_w_impute"] = req_byb_data_bowler[
            #         "bowler_runs"
            #     ] + (0.1 / (req_byb_data_bowler["cum_balls_bowled"] + 1))
            #     is_bowler_ball_zeros = req_byb_data_bowler["is_bowler_ball"] == 0
            #     req_byb_data_bowler["bowler_runs_wo_extra"] = req_byb_data_bowler[
            #         "bowler_runs"
            #     ] + (
            #         (req_byb_data_bowler["bowler_runs"] *
            # is_bowler_ball_zeros).shift(1)
            #     ).fillna(
            #         0
            #     )

            #     req_byb_data_bowler["bowler_runs_wo_extra"] = np.where(
            #         req_byb_data_bowler["is_bowler_ball"] == 0,
            #         np.nan,
            #         req_byb_data_bowler["bowler_runs_wo_extra"],
            #     )

            #     req_byb_data_bowler[
            #         "bowler_runs_wo_extra_w_impute"
            #     ] = req_byb_data_bowler["bowler_runs_w_impute"] + (
            #         (
            #             req_byb_data_bowler["bowler_runs_w_impute"]
            #             * is_bowler_ball_zeros
            #         ).shift(1)
            #     ).fillna(
            #         0
            #     )

            #     req_byb_data_bowler["bowler_runs_wo_extra_w_impute"] = np.where(
            #         req_byb_data_bowler["is_bowler_ball"] == 0,
            #         np.nan,
            #         req_byb_data_bowler["bowler_runs_wo_extra_w_impute"],
            #     )

            #     temp_dict["ingame_Other_BowlerEco"] = (
            #         (req_byb_data_bowler["bowler_runs_wo_extra"] * 6)
            #         .ewm(alpha=0.3, ignore_na=True)
            #         .mean()
            #     )

            #     temp_dict["ingame_Other_BowlerEco_w_impute"] = (
            #         (req_byb_data_bowler["bowler_runs_wo_extra_w_impute"] * 6)
            #         .ewm(alpha=0.3, ignore_na=True)
            #         .mean()
            #     )

            #     temp_dict["#otherbowler_balls_tillnow"] = len(req_byb_data_batsman)
            # else:
            #     temp_dict["ingame_Other_BowlerEco"] = np.nan
            #     temp_dict["ingame_Other_BowlerEco_w_impute"] = np.nan
            #     temp_dict["#otherbowler_balls_tillnow"] = np.nan

            # ------------------------------------
            # H2H Ingame data
            # ------------------------------------
            if row_h2h_prev is not None:
                # copy h2h match features after previous ball (till present ball)
                temp_dict["ingame_h2hEco"] = row_h2h_prev["cum_h2h_eco"]

                temp_dict["ingame_h2hEco_w_impute"] = row_h2h_prev[
                    "cum_h2h_eco_w_impute"
                ]

                temp_dict["ingame_h2hBowlerSR"] = row_h2h_prev["cum_h2h_bsr"]
                temp_dict["ingame_h2hBowlerSR_w_impute"] = row_h2h_prev[
                    "cum_h2h_bsr_w_impute"
                ]

                temp_dict["ingame_total_h2hRuns"] = row_h2h_prev["cum_h2h_runs"]
                temp_dict["ingame_total_h2hBalls"] = row_h2h_prev["cum_h2h_balls"]

        elif row_bat["raw_ball_no"] == 1:
            # if ball is very first ball in innings
            row_byb_temp = byb_data[
                (byb_data["match_name"] == temp_dict["match_name"])
                & (byb_data["innings"] == temp_dict["innings"])
                & (byb_data["raw_ball_no"] == 1)
                ]
            row_byb_temp = row_byb_temp.iloc[0] if len(row_byb_temp) != 0 else None
            if row_byb_temp is not None:
                temp_dict["ingame_CRR"] = 0
                temp_dict["ingame_RRR"] = row_byb_temp["target_runs"] / (120 / 6)
                temp_dict["ingame_RSR"] = (100 * temp_dict["ingame_RRR"]) / 6
                temp_dict["ingame_totalwkts"] = 0
                temp_dict["ingame_totalruns"] = 0
                temp_dict["is_striker"] = row_bat["is_striker"]
                temp_dict["ingame_resource_utilized"] = 0
                # temp_dict["byb_id"] = row_byb_temp["id"]

        # ------------------------------------
        # Batsman Pregame data
        # ------------------------------------
        df_bat_pregame = batsman_entrypoint_df[
            (batsman_entrypoint_df["batsman_id"] == row_bat["req_batsman_id"])
            & (batsman_entrypoint_df["match_date"] < row_bat["match_date"])
            & (batsman_entrypoint_df["SR"].notna())
            ].sort_values(["match_date"], ascending=False)

        df_bat_pregame_need = df_bat_pregame[
            df_bat_pregame["entry_point"] == batsman_arr_diss_series["entry_point"]
            ]
        flag_entry_data_available = 1
        if len(df_bat_pregame_need) < 2:
            df_bat_pregame_need = df_bat_pregame
            flag_entry_data_available = 0

        df_bat_pregame_need["id"] = (
                pd.factorize(df_bat_pregame_need["match_name"])[0] + 1
        )
        df_bat_pregame_need = df_bat_pregame_need[df_bat_pregame_need["id"] <= 20]
        if len(df_bat_pregame_need) != 0:
            temp_dict["pregame_batsman_entrySR"] = get_mean_using_bs(
                df_bat_pregame_need["SR"].values
            )
            temp_dict["is_pregame_entrySR_available"] = flag_entry_data_available
        else:
            temp_dict["pregame_batsman_entrySR"] = np.nan
            temp_dict["is_pregame_entrySR_available"] = flag_entry_data_available
        # ------------------------------------

        # ------------------------------------
        # Bowler Pregame data
        # ------------------------------------
        df_bowler_pregame = bowler_match_stats_df[
            (bowler_match_stats_df["bowler_id"] == row_bat["bowler_id"])
            & (bowler_match_stats_df["match_date"] < row_bat["match_date"])
            ].sort_values(["match_date"], ascending=False)

        df_bowler_pregame_need = df_bowler_pregame[
            df_bowler_pregame["match_phase"] == row_bat["match_phase"]
            ]
        flag_bowler_phase_data_available = 1
        if len(df_bowler_pregame_need) < 2:
            df_bowler_pregame_need = df_bowler_pregame
            flag_bowler_phase_data_available = 0

        df_bowler_pregame_need["id"] = (
                pd.factorize(df_bowler_pregame_need["match_name"])[0] + 1
        )
        df_bowler_pregame_need = df_bowler_pregame_need[
            df_bowler_pregame_need["id"] <= 20
            ]

        if len(df_bowler_pregame_need) != 0:
            temp_dict["pregame_bowler_eco"] = get_mean_using_bs(
                df_bowler_pregame_need["eco"].values
            )

            temp_dict["pregame_bowler_sr"] = get_mean_using_bs(
                df_bowler_pregame_need["sr"].values
            )

            temp_dict[
                "is_pregame_bowler_phaseEco_available"
            ] = flag_entry_data_available
        else:
            temp_dict["pregame_bowler_eco"] = 9999
            temp_dict["pregame_bowler_sr"] = 9999
            temp_dict[
                "is_pregame_bowler_phaseEco_available"
            ] = flag_entry_data_available
        # ------------------------------------

        # ------------------------------------
        # H2H Pregame data
        # ------------------------------------
        df_h2h_pregame_type1 = h2h_match_stats_df[
            (h2h_match_stats_df["bowler_id"] == row_bat["bowler_id"])
            & (h2h_match_stats_df["batsman_id"] == row_bat["req_batsman_id"])
            & (h2h_match_stats_df["match_date"] < row_bat["match_date"])
            & (h2h_match_stats_df["match_phase"] == row_bat["match_phase"])
            ]
        if (len(df_h2h_pregame_type1) != 0) & (
                df_h2h_pregame_type1["balls"].sum() >= 6
        ):
            df_h2h_pregame_type1["id"] = (
                    pd.factorize(df_h2h_pregame_type1["match_name"])[0] + 1
            )
            df_h2h_pregame_type1 = df_h2h_pregame_type1[
                df_h2h_pregame_type1["id"] <= 20
                ]
            temp_dict["pregame_h2h_eco"] = get_mean_using_bs(
                df_h2h_pregame_type1["eco"].values
            )
            temp_dict["pregame_h2h_bowler_sr"] = get_mean_using_bs(
                df_h2h_pregame_type1["bowl_sr"].values
            )
            # temp_dict["is_pregame_h2h_available"] = flag_h2h_pregame_available
            temp_dict["pregame_h2h_type"] = 1
        else:
            df_h2h_pregame_type2 = h2battype_match_stats_df[
                (h2battype_match_stats_df["bowler_id"] == row_bat["bowler_id"])
                & (
                        h2battype_match_stats_df["striker_batting_type"]
                        == batsman_arr_diss_series["striker_batting_type"]
                )
                & (h2battype_match_stats_df["match_date"] < row_bat["match_date"])
                & (h2battype_match_stats_df["match_phase"] == row_bat["match_phase"])
                ]
            if (len(df_h2h_pregame_type2) != 0) & (
                    df_h2h_pregame_type2["balls"].sum() >= 6
            ):
                df_h2h_pregame_type2["id"] = (
                        pd.factorize(df_h2h_pregame_type2["match_name"])[0] + 1
                )
                df_h2h_pregame_type2 = df_h2h_pregame_type2[
                    df_h2h_pregame_type2["id"] <= 20
                    ]
                temp_dict["pregame_h2h_eco"] = get_mean_using_bs(
                    df_h2h_pregame_type2["eco"].values
                )
                temp_dict["pregame_h2h_bowler_sr"] = get_mean_using_bs(
                    df_h2h_pregame_type2["bowl_sr"].values
                )

                # temp_dict["is_pregame_h2h_available"] = flag_h2h_pregame_available
                temp_dict["pregame_h2h_type"] = 2
            else:
                temp_dict["pregame_h2h_eco"] = temp_dict["pregame_bowler_eco"]
                temp_dict["pregame_h2h_bowler_sr"] = temp_dict["pregame_bowler_sr"]
                temp_dict["is_pregame_h2h_available"] = flag_bowler_phase_data_available
                temp_dict["pregame_h2h_type"] = 3
        # ------------------------------------

        final_bat.append(temp_dict)
    final_df = pd.DataFrame(final_bat)
    final_df["index"] = (
            final_df["match_name"]
            + "_"
            + final_df["req_batsman"]
            + "_"
            + final_df["raw_ball_no"].astype(str)
    )
    final_df = final_df.set_index("index")
    return final_df


def generate_pi_raw_features_for_row(
        batsman_arr_diss_series,
        byb_data,
        df_bowler_data_ingame,
        df_h2h_ingame,
        df_batsman_entrypoint,
        df_bowler_stats,
        df_h2h_stats,
        df_h2battype_stats,
):
    """_summary_

    Parameters
    ----------
    batsman_arr_diss_series : pd.Series
        a pd series contains arrival dismissal balls with basic batsman data
    byb_data : pd.DataFrame
        a ball by ball data after create_baseline_df function on byb.
    df_bowler_data_ingame : pd.DataFrame
        a pd DataFrame contains bowlers ingame info.
    df_h2h_ingame : pd.DataFrame
        a pd DataFrame contains H2H ingame info.
    df_batsman_entrypoint : pd.DataFrame
        all entrypoints data of required batsmen for each match (pregame).
    df_bowler_stats : pd.DataFrame
        required bowlers phase level stats for all pregame matches.
    df_h2h_stats : pd.DataFrame
        required batsman-bowler(h2h) phase level stats for all pregame matches.
    df_h2battype_stats : pd.DataFrame
        required bowler-bat type(h2battype) phase level stats for all pregame matches.

    Returns
    -------
    pd.DataFrame
        a df contains raw features of batsman
    """
    # if batsman_arr_diss_series["dismissed_raw_ball_no"] == -1:
    #     df_byb_one_batsman = byb_data[
    #         (byb_data["raw_ball_no"] >=
    # batsman_arr_diss_series["arrived_raw_ball_no"])
    #         & (byb_data["innings"] == batsman_arr_diss_series["innings"])
    #     ]
    # else:
    #     df_byb_one_batsman = byb_data[
    #         (byb_data["raw_ball_no"] >=
    # batsman_arr_diss_series["arrived_raw_ball_no"])
    #         & (
    #             byb_data["raw_ball_no"]
    #             <= batsman_arr_diss_series["dismissed_raw_ball_no"]
    #         )
    #         & (byb_data["innings"] == batsman_arr_diss_series["innings"])
    #     ]

    batter_ingame_df = get_req_ingame_batsman_df(
        batsman_arr_diss_series,
        byb_data,
        batsman_arr_diss_series["batsman_id"],
        batsman_arr_diss_series["batsman"],
    )

    bowler_ingame_df = df_bowler_data_ingame[
        (df_bowler_data_ingame["innings"] == batsman_arr_diss_series["innings"])
    ]
    h2h_ingame_df = df_h2h_ingame[
        (df_h2h_ingame["innings"] == batsman_arr_diss_series["innings"])
    ]

    df_final = create_pi_raw_features_for_batter(
        byb_data,
        batsman_arr_diss_series,
        batter_ingame_df,
        bowler_ingame_df,
        h2h_ingame_df,
        df_batsman_entrypoint,
        df_bowler_stats,
        df_h2h_stats,
        df_h2battype_stats,
    )
    return df_final


def create_match_pi_raw_values(
        df_batsman_arr_dismissed_ingame,
        byb_data,
        df_bowler_data_ingame,
        df_h2h_ingame,
        df_batsman_entrypoint,
        df_bowler_stats,
        df_h2h_stats,
        df_h2battype_stats,
):
    player_dfs = df_batsman_arr_dismissed_ingame.apply(
        lambda row: generate_pi_raw_features_for_row(
            row,
            byb_data,
            df_bowler_data_ingame,
            df_h2h_ingame,
            df_batsman_entrypoint,
            df_bowler_stats,
            df_h2h_stats,
            df_h2battype_stats,
        ),
        axis=1,
    )
    match_df = pd.concat(player_dfs.values.tolist())
    return match_df


def create_match_pi_features_from_byb(kwargs):
    byb_data = kwargs["match_byb_data"]
    logger.info(f"Processing data for Match id : {byb_data['match_id'].iloc[0]}")
    df_stadium_runs = kwargs["df_stadium_runs"]
    df_batsman_entrypoint = kwargs["df_batsman_entrypoint"]
    df_bowler_stats = kwargs["df_bowler_stats"]
    df_h2h_stats = kwargs["df_h2h_stats"]
    df_h2battype_stats = kwargs["df_h2battype_stats"]
    ru_data_path = kwargs.get("ru_data_path", PI_RESOURCE_UTILIZATION_FILE)
    byb_data = create_baseline_df(byb_data)
    byb_data = create_target_runs(byb_data, df_stadium_runs, ru_data_path=ru_data_path)
    df_bowler_data_ingame = (
        byb_data.groupby(
            [
                "match_date",
                "season",
                "competition_name",
                "match_name",
                "innings",
                "bowler_id",
            ]
        )
        .apply(get_req_ingame_bowler_df, byb_data=byb_data)
        .reset_index(drop=True)
    )
    df_h2h_ingame = (
        byb_data.groupby(
            [
                "match_date",
                "season",
                "competition_name",
                "match_name",
                "innings",
                "batsman_id",
                "bowler_id",
            ]
        )
        .apply(get_req_ingame_h2h_df)
        .reset_index()
        .drop(["index"], axis=1)
    )
    df_batsman_arr_dismissed_ingame = (
        byb_data.groupby(
            ["match_date", "season", "competition_name", "match_name", "innings"]
        )
        .apply(get_batsman_pos_arr_dismiss_df)
        .reset_index()
        .drop(["level_5"], axis=1)
    )
    match_df = create_match_pi_raw_values(
        df_batsman_arr_dismissed_ingame,
        byb_data,
        df_bowler_data_ingame,
        df_h2h_ingame,
        df_batsman_entrypoint,
        df_bowler_stats,
        df_h2h_stats,
        df_h2battype_stats,
    )
    match_df = create_pi_features_from_raw_features(match_df)
    return match_df


def create_pi_features_from_raw_features(data_raw):
    data_raw["RRR_BY_CRR"] = (data_raw["ingame_RRR"] + 1) / (data_raw["ingame_CRR"] + 1)
    data_raw["RRR_BY_CRR_after"] = (data_raw["ingame_RRR_after"] + 1) / (
            data_raw["ingame_CRR_after"] + 1
    )

    data_raw["wickets_by_10"] = data_raw["ingame_totalwkts"] / 10
    data_raw["wickets_by_10_after"] = data_raw["ingame_totalwkts_after"] / 10

    data_raw["wickets_resource_utilized"] = np.exp(
        data_raw["ingame_resource_utilized"] / 100
    )
    data_raw["wickets_resource_utilized_after"] = np.exp(
        data_raw["ingame_resource_utilized_after"] / 100
    )

    data_raw["RRR_BY_CRR_wickets_resource_utilized"] = (
            data_raw["RRR_BY_CRR"] * data_raw["wickets_resource_utilized"]
    )
    data_raw["RRR_BY_CRR_wickets_resource_utilized_after"] = (
            data_raw["RRR_BY_CRR_after"] * data_raw["wickets_resource_utilized_after"]
    )
    # w_progress for the BatSR
    data_raw["w_progress_BatSR"] = data_raw.apply(
        lambda row: get_w_progress(
            row["ingame_total_BatsmanBalls"],
            row["ingame_Ewm_BatsmanSR_w_impute"],
            row["pregame_batsman_entrySR"],
        ),
        axis=1,
    )
    data_raw["w_progress_BatSR_after"] = data_raw.apply(
        lambda row: get_w_progress(
            row["ingame_total_BatsmanBalls_after"],
            row["ingame_Ewm_BatsmanSR_w_impute_after"],
            row["pregame_batsman_entrySR"],
        ),
        axis=1,
    )
    # data_raw['w_progress_BatSR'] = data_raw['w_progress_BatSR'].fillna(0)
    # BatSR
    data_raw["BatSR_raw"] = (
                                    (1 - data_raw["w_progress_BatSR"]) * data_raw["pregame_batsman_entrySR"]
                            ) + (data_raw["w_progress_BatSR"] * data_raw["ingame_Ewm_BatsmanSR_w_impute"])

    data_raw["BatSR_raw_after"] = (
                                          (1 - data_raw["w_progress_BatSR_after"]) * data_raw["pregame_batsman_entrySR"]
                                  ) + (
                                          data_raw["w_progress_BatSR_after"]
                                          * data_raw["ingame_Ewm_BatsmanSR_w_impute_after"]
                                  )

    # if there is only pregame SR and ingame balls are nan, use pregame SR only.
    data_raw["BatSR_raw"] = np.where(
        (
                (data_raw["ingame_Ewm_BatsmanSR_w_impute"].isna())
                & (data_raw["pregame_batsman_entrySR"].notna())
        ),
        data_raw["pregame_batsman_entrySR"],
        data_raw["BatSR_raw"],
    )
    data_raw["BatSR_raw_after"] = np.where(
        (
                (data_raw["ingame_Ewm_BatsmanSR_w_impute_after"].isna())
                & (data_raw["pregame_batsman_entrySR"].notna())
        ),
        data_raw["pregame_batsman_entrySR"],
        data_raw["BatSR_raw_after"],
    )

    # if there is only ingame SR and no pregameSR
    data_raw["BatSR_raw"] = np.where(
        (
                (data_raw["ingame_Ewm_BatsmanSR_w_impute"].notna())
                & (data_raw["pregame_batsman_entrySR"].isna())
        ),
        data_raw["ingame_Ewm_BatsmanSR_w_impute"],
        data_raw["BatSR_raw"],
    )
    data_raw["BatSR_raw_after"] = np.where(
        (
                (data_raw["ingame_Ewm_BatsmanSR_w_impute_after"].notna())
                & (data_raw["pregame_batsman_entrySR"].isna())
        ),
        data_raw["ingame_Ewm_BatsmanSR_w_impute_after"],
        data_raw["BatSR_raw_after"],
    )

    data_raw["is_BatSR_filled_random"] = np.where(data_raw["BatSR_raw"].isna(), 1, 0)
    data_raw["is_BatSR_filled_random_after"] = np.where(
        data_raw["BatSR_raw_after"].isna(), 1, 0
    )

    # if there is no pregame as well as ingame SR
    data_raw["BatSR_raw"] = np.where(
        data_raw["is_BatSR_filled_random"] == 1, 130, data_raw["BatSR_raw"]
    )
    data_raw["BatSR_raw_after"] = np.where(
        data_raw["is_BatSR_filled_random_after"] == 1, 130, data_raw["BatSR_raw_after"]
    )

    data_raw["BatSR"] = data_raw["ingame_RSR"] / data_raw["BatSR_raw"]
    data_raw["BatSR_after"] = data_raw["ingame_RSR_after"] / data_raw["BatSR_raw_after"]

    # NonstrikerSR
    data_raw["NonstrikerSR_raw"] = data_raw["ingame_NonStrikerSR_w_impute"]
    data_raw["NonstrikerSR_raw_after"] = data_raw["ingame_NonStrikerSR_w_impute_after"]

    data_raw["is_NonstrikerSR_filled_random"] = np.where(
        data_raw["NonstrikerSR_raw"].isna(), 1, 0
    )
    data_raw["is_NonstrikerSR_filled_random_after"] = np.where(
        data_raw["NonstrikerSR_raw_after"].isna(), 1, 0
    )

    data_raw["NonstrikerSR_raw"] = np.where(
        data_raw["NonstrikerSR_raw"].isna(), 130, data_raw["NonstrikerSR_raw"]
    )
    data_raw["NonstrikerSR_raw_after"] = np.where(
        data_raw["NonstrikerSR_raw_after"].isna(),
        130,
        data_raw["NonstrikerSR_raw_after"],
    )

    data_raw["NonstrikerSR"] = data_raw["ingame_RSR"] / data_raw["NonstrikerSR_raw"]
    data_raw["NonstrikerSR_after"] = (
            data_raw["ingame_RSR_after"] / data_raw["NonstrikerSR_raw_after"]
    )

    # bowlerEco
    data_raw["w_progress_BowlerEco"] = data_raw.apply(
        lambda row: get_w_progress(
            row["ingame_total_BowlerBalls"],
            row["ingame_Ewm_BowlerEco_w_impute"],
            row["pregame_bowler_eco"],
        ),
        axis=1,
    )
    data_raw["w_progress_BowlerEco_after"] = data_raw.apply(
        lambda row: get_w_progress(
            row["ingame_total_BowlerBalls_after"],
            row["ingame_Ewm_BowlerEco_w_impute_after"],
            row["pregame_bowler_eco"],
        ),
        axis=1,
    )

    data_raw["BowlerEco_raw"] = (
                                        (1 - data_raw["w_progress_BowlerEco"]) * data_raw["pregame_bowler_eco"]
                                ) + (data_raw["w_progress_BowlerEco"] * data_raw["ingame_Ewm_BowlerEco_w_impute"])
    data_raw["BowlerEco_raw_after"] = (
                                              (1 - data_raw["w_progress_BowlerEco_after"]) * data_raw[
                                          "pregame_bowler_eco"]
                                      ) + (
                                              data_raw["w_progress_BowlerEco_after"]
                                              * data_raw["ingame_Ewm_BowlerEco_w_impute_after"]
                                      )

    # if there is only pregame Eco and ingame balls are nan, use pregame Eco only.
    data_raw["BowlerEco_raw"] = np.where(
        (
                (data_raw["ingame_Ewm_BowlerEco_w_impute"].isna())
                & (data_raw["pregame_bowler_eco"].notna())
        ),
        data_raw["pregame_bowler_eco"],
        data_raw["BowlerEco_raw"],
    )
    data_raw["BowlerEco_raw_after"] = np.where(
        (
                (data_raw["ingame_Ewm_BowlerEco_w_impute_after"].isna())
                & (data_raw["pregame_bowler_eco"].notna())
        ),
        data_raw["pregame_bowler_eco"],
        data_raw["BowlerEco_raw_after"],
    )

    # if there is only ingame Eco and no pregame Eco
    data_raw["BowlerEco_raw"] = np.where(
        (
                (data_raw["ingame_Ewm_BowlerEco_w_impute"].notna())
                & (data_raw["pregame_bowler_eco"].isna())
        ),
        data_raw["ingame_Ewm_BowlerEco_w_impute"],
        data_raw["BowlerEco_raw"],
    )
    data_raw["BowlerEco_raw_after"] = np.where(
        (
                (data_raw["ingame_Ewm_BowlerEco_w_impute_after"].notna())
                & (data_raw["pregame_bowler_eco"].isna())
        ),
        data_raw["ingame_Ewm_BowlerEco_w_impute_after"],
        data_raw["BowlerEco_raw_after"],
    )

    data_raw["is_BowlerEco_filled_random"] = np.where(
        data_raw["BowlerEco_raw"].isna(), 1, 0
    )
    data_raw["is_BowlerEco_filled_random_after"] = np.where(
        data_raw["BowlerEco_raw_after"].isna(), 1, 0
    )

    # if there is no pregame as well as ingame SR
    data_raw["BowlerEco_raw"] = np.where(
        data_raw["is_BowlerEco_filled_random"] == 1, 9, data_raw["BowlerEco_raw"]
    )
    data_raw["BowlerEco_raw_after"] = np.where(
        data_raw["is_BowlerEco_filled_random_after"] == 1,
        9,
        data_raw["BowlerEco_raw_after"],
    )

    data_raw["BowlerEco"] = data_raw["ingame_RRR"] / data_raw["BowlerEco_raw"]
    data_raw["BowlerEco_after"] = (
            data_raw["ingame_RRR_after"] / data_raw["BowlerEco_raw_after"]
    )

    # OtherBowlersEco
    data_raw["OtherBowlersEco_raw"] = data_raw["ingame_Other_BowlerEco_w_impute"]
    data_raw["OtherBowlersEco_raw_after"] = data_raw[
        "ingame_Other_BowlerEco_w_impute_after"
    ]

    data_raw["is_OtherBowlersEco_filled_random"] = np.where(
        data_raw["OtherBowlersEco_raw"].isna(), 1, 0
    )
    data_raw["is_OtherBowlersEco_filled_random_after"] = np.where(
        data_raw["OtherBowlersEco_raw_after"].isna(), 1, 0
    )

    data_raw["OtherBowlersEco_raw"] = np.where(
        data_raw["OtherBowlersEco_raw"].isna(), 9, data_raw["OtherBowlersEco_raw"]
    )
    data_raw["OtherBowlersEco_raw_after"] = np.where(
        data_raw["OtherBowlersEco_raw_after"].isna(),
        9,
        data_raw["OtherBowlersEco_raw_after"],
    )

    data_raw["OtherBowlersEco"] = (
            data_raw["ingame_RRR"] / data_raw["OtherBowlersEco_raw"]
    )
    data_raw["OtherBowlersEco_after"] = (
            data_raw["ingame_RRR_after"] / data_raw["OtherBowlersEco_raw_after"]
    )

    # h2h Economy
    # w_progress for H2H eco
    data_raw["w_progress_H2HEco"] = data_raw.apply(
        lambda row: get_w_progress(
            row["ingame_total_h2hBalls"],
            row["ingame_h2hEco_w_impute"],
            row["pregame_h2h_eco"],
        ),
        axis=1,
    )
    data_raw["H2HEco_raw"] = (
                                     (1 - data_raw["w_progress_H2HEco"]) * data_raw["pregame_h2h_eco"]
                             ) + (data_raw["w_progress_H2HEco"] * data_raw["ingame_h2hEco_w_impute"])
    # if there is only pregame Eco and ingame balls are nan, use pregame Eco only.
    data_raw["H2HEco_raw"] = np.where(
        (
                (data_raw["ingame_h2hEco_w_impute"].isna())
                & (data_raw["pregame_h2h_eco"].notna())
        ),
        data_raw["pregame_h2h_eco"],
        data_raw["H2HEco_raw"],
    )
    # if there is only ingame Eco and no pregame Eco
    data_raw["H2HEco_raw"] = np.where(
        (
                (data_raw["ingame_h2hEco_w_impute"].notna())
                & (data_raw["pregame_h2h_eco"].isna())
        ),
        data_raw["ingame_h2hEco_w_impute"],
        data_raw["H2HEco_raw"],
    )
    # if there is no pregame as well as ingame SR
    data_raw["is_H2HEco_filled_random"] = np.where(data_raw["H2HEco_raw"].isna(), 1, 0)
    data_raw["H2HEco_raw"] = np.where(
        data_raw["is_H2HEco_filled_random"] == 1, 9, data_raw["H2HEco_raw"]
    )
    data_raw["H2HEco"] = data_raw["ingame_RRR"] / data_raw["H2HEco_raw"]

    data_raw["BowlerSR"] = (
            0.3 * data_raw["pregame_h2h_bowler_sr"]
            + 0.2 * data_raw["pregame_bowler_sr"]
            + 0.5 * data_raw["ingame_BowlerSR_w_impute"]
    )
    data_raw["BowlerSR"] = np.where(
        data_raw["BowlerSR"].isna(),
        0.5 * data_raw["pregame_h2h_bowler_sr"] + 0.5 * data_raw["pregame_bowler_sr"],
        data_raw["BowlerSR"],
    )
    data_raw["BowlerSR"] = 1 / data_raw["BowlerSR"]

    data_raw["ingame_bowler_wicket_factor"] = np.exp(
        data_raw["ingame_total_BowlerWkts"].fillna(0) / 10
    )
    data_raw["ingame_bowler_wicket_factor_after"] = np.exp(
        data_raw["ingame_total_BowlerWkts_after"].fillna(0) / 10
    )

    data_raw["BowlerEco_SR"] = (
            data_raw["BowlerEco"] * data_raw["ingame_bowler_wicket_factor"]
    )
    data_raw["BowlerEco_SR_after"] = (
            data_raw["BowlerEco_after"] * data_raw["ingame_bowler_wicket_factor_after"]
    )

    data_raw["ingame_other_bowlerWkts"] = (
            data_raw["ingame_totalwkts"] - data_raw["ingame_total_BowlerWkts"]
    ).fillna(0)
    data_raw["ingame_other_bowlerWkts_after"] = (
            data_raw["ingame_totalwkts_after"] - data_raw["ingame_total_BowlerWkts_after"]
    ).fillna(0)

    data_raw["ingame_other_bowlerWkts"] = np.where(
        data_raw["ingame_other_bowlerWkts"] < 0, 0, data_raw["ingame_other_bowlerWkts"]
    )
    data_raw["ingame_other_bowlerWkts_after"] = np.where(
        data_raw["ingame_other_bowlerWkts_after"] < 0,
        0,
        data_raw["ingame_other_bowlerWkts_after"],
    )

    data_raw["ingame_other_bowlerWkts_factor"] = np.exp(
        data_raw["ingame_other_bowlerWkts"] / 10
    )
    data_raw["ingame_other_bowlerWkts_factor_after"] = np.exp(
        data_raw["ingame_other_bowlerWkts_after"] / 10
    )

    data_raw["OtherBowlersEco_SR"] = (
            data_raw["OtherBowlersEco"] * data_raw["ingame_other_bowlerWkts_factor"]
    )
    data_raw["OtherBowlersEco_SR_after"] = (
            data_raw["OtherBowlersEco_after"]
            * data_raw["ingame_other_bowlerWkts_factor_after"]
    )
    return data_raw


def get_byb_pi_features(
        byb_data,
        df_stadium_runs,
        df_batsman_entrypoint,
        df_bowler_stats,
        df_h2h_stats,
        df_h2battype_stats,
        ru_data_path="Data/resource_utilization.csv",
        save_match_data=False,
):
    grouped_byb = byb_data.groupby(["match_date", "match_name"])
    pi_byb_data = pd.DataFrame()

    params = []
    for group_name, match_byb_data in grouped_byb:
        params.append({
            "match_byb_data": match_byb_data,
            "df_stadium_runs": df_stadium_runs,
            "df_batsman_entrypoint": df_batsman_entrypoint,
            "df_bowler_stats": df_bowler_stats,
            "df_h2h_stats": df_h2h_stats,
            "df_h2battype_stats": df_h2battype_stats
        })
    logger.info("Using Multi processing to speed up")
    import multiprocessing

    # Number of processes to be used (you can adjust this based on your CPU cores)
    num_processes = multiprocessing.cpu_count()

    # Create a pool of processes
    with multiprocessing.Pool(processes=num_processes) as pool:
        # Map the square function to the list of numbers using multiple processes
        results = pool.map(create_match_pi_features_from_byb, params)

    for result in results:
        pi_byb_data = pd.concat([pi_byb_data, result])

    return pi_byb_data


def add_scaled_pi_features(
        min_max_scale_file_path, pi_byb_data, feature_json_path=PI_CONFIG_DATA_PATH + "feature_names.json"
):
    feature_json = read_json(feature_json_path)
    features = feature_json["features"]
    features_after = feature_json["features_after"]

    percentile_vals_threshold = read_json(min_max_scale_file_path)

    for key_feature in features:
        thres = percentile_vals_threshold[key_feature]
        pi_byb_data[key_feature + "_scaled"] = (
                                                       pi_byb_data[key_feature].values - thres["min"]
                                               ) / (thres["max"] - thres["min"])
        pi_byb_data[key_feature + "_scaled"] = np.where(
            pi_byb_data[key_feature] < thres["min"],
            0,
            pi_byb_data[key_feature + "_scaled"],
        )
        pi_byb_data[key_feature + "_scaled"] = np.where(
            pi_byb_data[key_feature] > thres["max"],
            1 + get_extra_val(pi_byb_data[key_feature], thres["max"]),
            pi_byb_data[key_feature + "_scaled"],
        )

        if key_feature in features_after:
            pi_byb_data[key_feature + "_after_scaled"] = (
                                                                 pi_byb_data[key_feature + "_after"].values - thres[
                                                             "min"]
                                                         ) / (thres["max"] - thres["min"])
            pi_byb_data[key_feature + "_after_scaled"] = np.where(
                pi_byb_data[key_feature + "_after"] < thres["min"],
                0,
                pi_byb_data[key_feature + "_after" + "_scaled"],
            )
            pi_byb_data[key_feature + "_after" + "_scaled"] = np.where(
                pi_byb_data[key_feature + "_after"] > thres["max"],
                1 + get_extra_val(pi_byb_data[key_feature + "_after"], thres["max"]),
                pi_byb_data[key_feature + "_after" + "_scaled"],
            )

    return pi_byb_data


def create_final_pi_from_scaled_features(
        pi_byb_data,
        features_weights,
        features,
        features_weights_match_pi_batsman=None,
        features_weights_match_pi_bowler=None,
):
    pi_byb_data["BatSR_mulfactor"] = pi_byb_data["ingame_total_BatsmanBalls"].apply(
        lambda x: (2 / 3) * balls_to_settle_factor(x, no_balls_to_settle=10)
    )
    pi_byb_data["BatSR_mulfactor_after"] = pi_byb_data[
        "ingame_total_BatsmanBalls_after"
    ].apply(lambda x: (2 / 3) * balls_to_settle_factor(x, no_balls_to_settle=10))

    pi_byb_data["NonstrikerSR_mulfactor"] = pi_byb_data[
        "ingame_total_BatsmanBalls"
    ].apply(lambda x: (1 / 3) * balls_to_settle_factor(x, no_balls_to_settle=10))
    pi_byb_data["NonstrikerSR_mulfactor_after"] = pi_byb_data[
        "ingame_total_BatsmanBalls_after"
    ].apply(lambda x: (1 / 3) * balls_to_settle_factor(x, no_balls_to_settle=10))

    # pi_byb_data["NonstrikerSR_mulfactor"] = pi_byb_data[
    #     "ingame_nonstriker_balls_with_wicket_reset"
    # ].apply(lambda x: (1 / 3) * balls_to_settle_factor(x, no_balls_to_settle=6))
    # pi_byb_data["NonstrikerSR_mulfactor_after"] = pi_byb_data[
    #     "ingame_nonstriker_balls_with_wicket_reset_after"
    # ].apply(lambda x: (1 / 3) * balls_to_settle_factor(x, no_balls_to_settle=6))

    pi_byb_data["BowlerEco_SR_mulfactor"] = pi_byb_data[
        "ingame_total_BowlerBalls"
    ].apply(lambda x: (2 / 3) * balls_to_settle_factor(x, no_balls_to_settle=6))
    pi_byb_data["BowlerEco_SR_mulfactor_after"] = pi_byb_data[
        "ingame_total_BowlerBalls_after"
    ].apply(lambda x: (2 / 3) * balls_to_settle_factor(x, no_balls_to_settle=6))

    pi_byb_data["OtherBowlersEco_SR_mulfactor"] = pi_byb_data[
        "ingame_total_BowlerBalls"
    ].apply(lambda x: (1 / 3) * balls_to_settle_factor(x, no_balls_to_settle=6))
    pi_byb_data["OtherBowlersEco_SR_mulfactor_after"] = pi_byb_data[
        "ingame_total_BowlerBalls_after"
    ].apply(lambda x: (1 / 3) * balls_to_settle_factor(x, no_balls_to_settle=6))

    pi_byb_data["H2HEco_mulfactor"] = pi_byb_data["ingame_total_h2hBalls"].apply(
        lambda x: balls_to_settle_factor(x, no_balls_to_settle=6)
    )
    pi_byb_data["RRR_BY_CRR_wickets_resource_utilized_mulfactor"] = 1
    pi_byb_data["RRR_BY_CRR_wickets_resource_utilized_mulfactor_after"] = 1
    # pi_byb_data["wickets_resource_utilized_mulfactor"] = 1
    # pi_byb_data["BowlerSR_mulfactor"] = 1

    for key_feature in features:
        pi_byb_data[key_feature + "_final"] = (
                pi_byb_data[key_feature + "_scaled"]
                * features_weights[key_feature + "_scaled"]
                * pi_byb_data[key_feature + "_mulfactor"]
                * 10
        )

    final_col_names = [i + "_final" for i in features]
    pi_byb_data["batsmanpi_w_h2h"] = pi_byb_data[final_col_names].sum(axis=1)

    if features_weights_match_pi_batsman is not None:
        feature2_names = [
            "RRR_BY_CRR_wickets_resource_utilized",
            "BowlerEco_SR",
            "OtherBowlersEco_SR",
            "NonstrikerSR",
        ]
        for key_feature in feature2_names:
            pi_byb_data[key_feature + "_final_match_pi_batsman"] = (
                    pi_byb_data[key_feature + "_scaled"]
                    * features_weights_match_pi_batsman[key_feature + "_scaled"]
                    * pi_byb_data[key_feature + "_mulfactor"]
                    * 10
            )
        final_col_names = [i + "_final_match_pi_batsman" for i in feature2_names]
        pi_byb_data["BatsmanMatchPI"] = pi_byb_data[final_col_names].sum(axis=1)

        for key_feature in feature2_names:
            pi_byb_data[key_feature + "_final_match_pi_batsman_after"] = (
                    pi_byb_data[key_feature + "_after" + "_scaled"]
                    * features_weights_match_pi_batsman[key_feature + "_scaled"]
                    * pi_byb_data[key_feature + "_mulfactor_after"]
                    * 10
            )
        final_col_names = [i + "_final_match_pi_batsman_after" for i in feature2_names]
        pi_byb_data["BatsmanMatchPI_after"] = pi_byb_data[final_col_names].sum(axis=1)

    if features_weights_match_pi_bowler is not None:
        feature2_names = [
            "RRR_BY_CRR_wickets_resource_utilized",
            "BatSR",
            "NonstrikerSR",
            "OtherBowlersEco_SR",
        ]
        for key_feature in feature2_names:
            pi_byb_data[key_feature + "_final_match_pi_bowler"] = (
                    pi_byb_data[key_feature + "_scaled"]
                    * features_weights_match_pi_bowler[key_feature + "_scaled"]
                    * pi_byb_data[key_feature + "_mulfactor"]
                    * 10
            )
        final_col_names = [i + "_final_match_pi_bowler" for i in feature2_names]
        pi_byb_data["BowlerMatchPI"] = pi_byb_data[final_col_names].sum(axis=1)

        for key_feature in feature2_names:
            pi_byb_data[key_feature + "_final_match_pi_bowler_after"] = (
                    pi_byb_data[key_feature + "_after" + "_scaled"]
                    * features_weights_match_pi_bowler[key_feature + "_scaled"]
                    * pi_byb_data[key_feature + "_mulfactor_after"]
                    * 10
            )
        final_col_names = [i + "_final_match_pi_bowler_after" for i in feature2_names]
        pi_byb_data["BowlerMatchPI_after"] = pi_byb_data[final_col_names].sum(axis=1)

    pi_byb_data["MatchPI"] = pi_byb_data["RRR_BY_CRR_wickets_resource_utilized"]
    pi_byb_data["MatchPI_after"] = pi_byb_data[
        "RRR_BY_CRR_wickets_resource_utilized_after"
    ]
    return pi_byb_data


def get_categories(pi_byb_data):
    avg_pi_df = (
        pi_byb_data[pi_byb_data["is_striker"] == 1]
        .groupby(["match_name", "innings"])
        .agg(
            {
                "BatsmanMatchPI": winsorized_mean,
                "BowlerMatchPI": winsorized_mean,
                "batsmanpi_w_h2h": winsorized_mean,
                "MatchPI": winsorized_mean,
            }
        )
        .reset_index()
        .rename(
            columns={
                "BatsmanMatchPI": "AvgBatsmanMatchPI",
                "BowlerMatchPI": "AvgBowlerMatchPI",
                "batsmanpi_w_h2h": "AvgBatsmanPI_w/h2h",
                "MatchPI": "AvgMatchPI",
            }
        )
    )
    pi_byb_data = pi_byb_data.merge(
        avg_pi_df, on=["match_name", "innings"], how="left", validate="m:1"
    )
    pi_byb_data["BatsmanMatchPI_ratio"] = (
            pi_byb_data["BatsmanMatchPI"] / pi_byb_data["AvgBatsmanMatchPI"]
    )
    pi_byb_data["BowlerMatchPI_ratio"] = (
            pi_byb_data["BowlerMatchPI"] / pi_byb_data["AvgBowlerMatchPI"]
    )
    pi_byb_data["BatsmanPI_w_h2h_ratio"] = (
            pi_byb_data["batsmanpi_w_h2h"] / pi_byb_data["AvgBatsmanPI_w/h2h"]
    )
    pi_byb_data["MatchPI_ratio"] = pi_byb_data["MatchPI"] / pi_byb_data["AvgMatchPI"]

    percentile_vals = [0.4, 0.8, 1.2, 1.8]

    pi_byb_data["MatchPI_Cat"] = pi_byb_data["MatchPI_ratio"].apply(
        lambda x: get_pi_category(x, percentile_vals)
    )
    pi_byb_data["BatsmanMatchPI_Cat"] = pi_byb_data["BatsmanMatchPI_ratio"].apply(
        lambda x: get_pi_category(x, percentile_vals)
    )
    pi_byb_data["BowlerMatchPI_Cat"] = pi_byb_data["BowlerMatchPI_ratio"].apply(
        lambda x: get_bowler_pi_category(x, percentile_vals)
    )
    pi_byb_data["BatsmanPI_wh2h_Cat"] = pi_byb_data["BatsmanPI_w_h2h_ratio"].apply(
        lambda x: get_pi_category(x, percentile_vals)
    )
    return pi_byb_data


def get_byb_pi(
        byb_data,
        min_max_scale_file_path=PI_CONFIG_DATA_PATH + "min_max_scale_vals.json",
        feature_json_path=PI_CONFIG_DATA_PATH + "feature_names.json",
        feature_weights_json_path=PI_CONFIG_DATA_PATH + "feature_weights.json",
        batsman_match_feature_weights_json_path=PI_CONFIG_DATA_PATH + "feature_weights_batsman_match_pi.json",
        bowler_match_feature_weights_json_path=PI_CONFIG_DATA_PATH + "feature_weights_bowler_match_pi.json",
):
    logger.info("Creating Baseline dataframe")
    byb_data = create_baseline_df(byb_data)
    (
        df_stadium_runs,
        df_batsman_entrypoint,
        df_batsman_ip,
        df_bowler_stats,
        df_h2h_stats,
        df_h2battype_stats,
    ) = generate_and_load_pregame(byb_data, load_timestamp)

    logger.info("Creating Pressure Index Features")
    pi_byb_data = get_byb_pi_features(
        byb_data,
        df_stadium_runs,
        df_batsman_entrypoint,
        df_bowler_stats,
        df_h2h_stats,
        df_h2battype_stats,
    )

    logger.info("Scaling Pressure Index Data")
    pi_byb_data = add_scaled_pi_features(
        min_max_scale_file_path, pi_byb_data, feature_json_path
    )
    features = read_json(feature_json_path)["features"]
    features_weights = read_json(feature_weights_json_path)
    match_feature_weights_batsman = read_json(batsman_match_feature_weights_json_path)
    match_feature_weights_bowler = read_json(bowler_match_feature_weights_json_path)

    logger.info("Calculating Pressure Index Data")
    pi_byb_data = create_final_pi_from_scaled_features(
        pi_byb_data,
        features_weights,
        features,
        match_feature_weights_batsman,
        match_feature_weights_bowler,
    )
    print("Calculating Pressure Categories ....")
    pi_byb_data = get_categories(pi_byb_data)

    columns_to_drop = ['w_progress_BatSR', 'w_progress_BatSR_after', 'w_progress_H2HEco', 'H2HEco_raw',
                       'is_H2HEco_filled_random', 'H2HEco',
                       'BowlerSR', 'ingame_bowler_wicket_factor', 'ingame_bowler_wicket_factor_after', 'BowlerEco_SR',
                       'BowlerEco_SR_after',
                       'ingame_other_bowlerWkts', 'ingame_other_bowlerWkts_after', 'ingame_other_bowlerWkts_factor',
                       'ingame_other_bowlerWkts_factor_after',
                       'OtherBowlersEco_SR', 'OtherBowlersEco_SR_after', 'AvgBatsmanMatchPI', 'AvgBowlerMatchPI',
                       'AvgBatsmanPI_w/h2h', 'AvgMatchPI']
    pi_byb_data = pi_byb_data.drop(columns=columns_to_drop)

    pi_byb_data['match_date'] = pd.to_datetime(pi_byb_data['match_date'], format='%Y-%m-%d').apply(
        lambda x: x.strftime('%d %b %Y'))
    pi_byb_data = pi_byb_data.reset_index(drop=True)
    pi_byb_data["load_timestamp"] = load_timestamp

    # Replace NaN with 999 and Infinity with 9999 as default values in all numeric columns
    numeric_columns = pi_byb_data.select_dtypes(include=np.number).columns
    replacement_values = {np.nan: 999, np.inf: 9999, -np.inf: -9999}
    pi_byb_data[numeric_columns] = pi_byb_data[numeric_columns].replace(replacement_values)

    # max_key_val = getMaxId(session, PRESSURE_INDEX_TABLE_NAME, PRESSURE_INDEX_KEY_COL, DB_NAME)
    max_id_df = pd.read_csv(PI_MAX_ID)
    max_key_val = int(max_id_df['max_id'].get(0, 1))
    final_pi_byb_data = generateSeq(pi_byb_data.sort_values(['match_date', 'match_name', 'innings', 'raw_ball_no']),
                                    PRESSURE_INDEX_KEY_COL, max_key_val).to_dict(orient='records')
    final_max = max_key_val + len(final_pi_byb_data)
    max_id_df['max_id'] = final_max
    # Write the updated DataFrame back to the same CSV file, overwriting its contents
    max_id_df.to_csv(PI_MAX_ID, index=False)


    logger.info("Pressure Index Data Generation Completed!")
    return final_pi_byb_data

# def parallel_process_data(df, num_workers=4):
#     total_matches = df['match_id'].nunique()
#     chunk_size = total_matches // num_workers
#
#     # Split the DataFrame into chunks based on matches
#     chunks = [group[1] for group in df.groupby('match_name')]
#     # chunks = [df.iloc[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
#
#     with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
#         results = list(executor.map(get_byb_pi, chunks))
#
#     # Combine the results
#     final_result = pd.concat(results)
#     return final_result


# def process_ball_data_multithreaded(ball_level_data, num_threads):
#     result_dfs = []
#
#     # Function to process a chunk
#     def process_chunk(chunk):
#         return get_byb_pi(chunk)
#
#     # Group ball-level data by match_id
#     grouped_data = ball_level_data.groupby('match_name')
#
#     # Split matches into chunks for parallel processing
#     chunks = list(grouped_data)  # Each chunk is a tuple (match_id, ball_data)
#
#     # Use ThreadPoolExecutor to concurrently process chunks
#     with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
#         # Submit tasks for each chunk
#         futures = [executor.submit(process_chunk, chunk[1]) for chunk in chunks]
#
#         # Retrieve results as they complete
#         for future in concurrent.futures.as_completed(futures):
#             result_dfs.append(future.result())
#
#     # Concatenate the results into a single DataFrame
#     final_result_df = pd.concat(result_dfs, ignore_index=True)
#
#     return final_result_df
#
#
# if __name__ == "__main__":
#     # result = parallel_process_data(data_all_leagues)
#
#     # num_threads = 4  # Adjust the number of threads based on your needs
#     #
#     # # Identify unique matches
#     # unique_matches = data_all_leagues['match_name'].unique()
#     #
#     # # Split matches into chunks
#     # matches_per_chunk = 5  # Adjust this based on your requirements
#     # match_chunks = [unique_matches[i:i + matches_per_chunk] for i in range(0, len(unique_matches), matches_per_chunk)]
#     #
#     #
#     # # Function to create chunks of data based on match IDs
#     # def create_chunk(match_ids):
#     #     return data_all_leagues[data_all_leagues['match_name'].isin(match_ids)]
#     #
#     #
#     # # Function to process a chunk of data using threads
#     # def process_chunk(chunk):
#     #     with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
#     #         futures = [executor.submit(get_byb_pi, match) for _, match in chunk.groupby('match_name')]
#     #         results = [future.result() for future in futures]
#     #     return pd.concat(results)
#     #
#     #
#     # # Use ThreadPoolExecutor to process chunks in parallel
#     # with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
#     #     chunks_of_data = list(executor.map(create_chunk, match_chunks))
#     #     results = list(executor.map(process_chunk, chunks_of_data))
#     #
#     # # Concatenate the results
#     # final_result = pd.concat(results)
#
#     # Number of threads
#     num_threads = 4  # Adjust based on your requirements
#
#     # Use ThreadPoolExecutor to concurrently process matches
#     # with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
#     #     # Submit tasks for each match
#     #     futures = [executor.submit(get_byb_pi, match) for match in data_all_leagues]
#     #
#     #     # Wait for all tasks to complete and get results
#     #     results = [future.result() for future in concurrent.futures.as_completed(futures)]
#     #
#     #     # # Wait for all tasks to complete
#     #     # concurrent.futures.wait(futures)
#     #
#     # result_df = pd.DataFrame(results)
#
#     result_dataframe = process_ball_data_multithreaded(data_all_leagues, num_threads)
#
#     # Write the DataFrame to a CSV file
#     result_dataframe.to_csv('output.csv', index=False)
