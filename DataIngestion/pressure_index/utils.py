import numpy as np
import json
from scipy.stats.mstats import winsorize


def get_entry_point_group(x):
    x = np.ceil(x / 6)
    if x <= 4:
        return "1-4"
    elif x <= 8:
        return "5-8"
    elif x <= 12:
        return "9-12"
    elif x <= 16:
        return "13-16"
    else:
        return "17-20"


def get_innings_progression(x):
    if x <= 5:
        return "IP1 (1-5)"
    elif x <= 10:
        return "IP2 (5-10)"
    elif x <= 20:
        return "IP3 (10-20)"
    else:
        return "IP4 (20+)"


def get_mean_using_bs(arr, n=100, CI=97.5):
    np.random.seed(20)
    bootstrapped_means = []
    np.random.seed(24)
    for _ in range(100):
        bootstrap_sample = np.random.choice(arr, size=len(arr), replace=True)
        bootstrapped_means.append(np.mean(bootstrap_sample))
    out_mean = np.percentile(bootstrapped_means, CI)
    return out_mean


def get_percentiles_using_bootstrapping(data, num_bootstraps=3000):
    ten_percentiles_values = []
    ninty_percentiles_values = []
    np.random.seed(24)
    for _ in range(num_bootstraps):
        bootstrap_sample = np.random.choice(data, size=len(data), replace=True)
        ten_percentiles_values.append(np.percentile(bootstrap_sample, 10))
        ninty_percentiles_values.append(np.percentile(bootstrap_sample, 90))
    return np.median(ten_percentiles_values), np.median(ninty_percentiles_values)


def get_w_progress(
    balls_played, ingame_data, pregame_data, initial_threshold=0, max_threshold=10
):
    progress_ratio = min(
        max(
            (balls_played - initial_threshold) / (max_threshold - initial_threshold), 0
        ),
        1,
    )
    if balls_played > max_threshold:
        w_progress = progress_ratio
    else:
        pre_in_difference = abs((ingame_data - pregame_data) / (pregame_data + 1e-12))
        progress_ratio_eco_diff = 1 / (1 + np.exp(-pre_in_difference))
        w_progress = (2 * progress_ratio + progress_ratio_eco_diff) / 3
    return w_progress


def get_extra_val(x, max_val):
    extra_val = (x - max_val) / max_val
    extra_val = (1 / (1 + np.exp(-1.5 * extra_val))) - 0.5
    return extra_val


def read_json(path):
    with open(path, "r") as f:
        json_data = json.load(f)
    return json_data


def save_json(path, json_data):
    with open(path, "w") as f:
        json.dump(json_data, f, indent=4)
    print("Saved json in ", path)


def balls_to_settle_factor(ball_no, no_balls_to_settle=10, steepness=6):
    if np.isnan(ball_no):
        ball_no = 0
    if np.isinf(ball_no):
        ball_no = 0
    if ball_no > no_balls_to_settle:
        return 1
    ball_no = int(ball_no)
    values = np.linspace(0, 1, no_balls_to_settle + 1)
    factor = 1 / (1 + np.exp(-steepness * (values[ball_no] - 0.5)))
    return factor


def get_pi_category(value, percentile_values):
    if value < percentile_values[0]:
        return "Very Low"
    elif value < percentile_values[1]:
        return "Low"
    elif value < percentile_values[2]:
        return "Medium"
    elif value < percentile_values[3]:
        return "High"
    else:
        return "Very High"


def get_bowler_pi_category(value, percentile_values):
    if value < percentile_values[0]:
        return "Very High"
    elif value < percentile_values[1]:
        return "High"
    elif value < percentile_values[2]:
        return "Medium"
    elif value < percentile_values[3]:
        return "Low"
    else:
        return "Very Low"


def winsorized_mean(series):
    series = winsorize(series, limits=(0.1, 0.1))
    return np.mean(series)


def winsorized_std(series):
    series = winsorize(series, limits=(0.1, 0.1))
    return np.std(series)


def scale_pi_ratio(x):
    return 1 / (1 + np.exp(-3 * (x - 1)))


def get_change_val(
    BatsmanMatchPI_after_ratio, BatsmanMatchPI_ratio, raw_ball_no, with_minmax=True
):
    factor_to_mul = balls_to_settle_factor(raw_ball_no, 12)
    change_percentage = (BatsmanMatchPI_after_ratio - BatsmanMatchPI_ratio) / (
        BatsmanMatchPI_ratio + 1e-12
    )
    change_percentage = factor_to_mul * np.tanh(2 * change_percentage)
    if with_minmax:
        change_percentage = (change_percentage + 1) / 2
    return change_percentage


def get_std_using_bs(arr, n=100, CI=97.5):
    np.random.seed(20)
    bootstrapped_means = []
    np.random.seed(24)
    for _ in range(100):
        bootstrap_sample = np.random.choice(arr, size=len(arr), replace=True)
        bootstrapped_means.append(np.std(bootstrap_sample))
    out_mean = np.percentile(bootstrapped_means, CI)
    return out_mean

def get_overs_group(over_number):
    if over_number <= 3:
        return "1-3"
    elif over_number <= 6:
        return "4-6"
    elif over_number <= 10:
        return "7-10"
    elif over_number <= 14:
        return "11-14"
    elif over_number <= 17:
        return "15-17"
    else:
        return "18-20"
