from datetime import datetime
import pandas as pd
from DataService.utils.helper import getMean, getStandardDeviation
import numpy as np
import json
import re

def getDeliveryData(match_ball_data, gps_delivery_data):
    if (len(match_ball_data)>0) & (len(gps_delivery_data)>0):
        gps_delivery_data['date_name'] = gps_delivery_data['date_name'].astype(str)

        match_ball_data['match_date'] = match_ball_data['match_date'].apply(
            lambda x: datetime.strptime(x, '%d %b %Y').strftime('%Y-%m-%d'))

        match_ball_data["ball_no"] = match_ball_data.groupby(['match_id', 'src_bowler_id'])["id"].rank(method="first",
                                                                                                   ascending=True).astype(int)

        match_ball_data["over_no"] = match_ball_data.groupby(['match_id', 'src_bowler_id'])["over_number"].rank(method="dense",
                                                                                                            ascending=True).astype(int)

        final_delivery_data = pd.merge(gps_delivery_data, match_ball_data[
            ['match_id', 'match_name', 'src_bowler_id','bowler_name', 'over_no', 'ball_no', 'bowl_length', 'match_date', 'bowler_image_url']],
                                       how='inner', left_on=['date_name', 'player_id', 'ball_no'],
                                       right_on=['match_date', 'src_bowler_id', 'ball_no']).rename(columns={'bowler_image_url': 'player_image_url'}) \
            [['match_id', 'match_name', 'match_date', 'player_id', 'player_name', 'delivery_runup_distance',
              'peak_player_load', 'raw_peak_roll', 'raw_peak_yaw', 'bowl_length', 'over_no', 'season', 'player_image_url']]

        return final_delivery_data
    else:
        return pd.DataFrame()


def getGroupedGPSData(df, agg_type, req_cols_list, group_list):
    if agg_type == 'mean':
        final_df = getMean(df[req_cols_list], group_list).reset_index()
    elif agg_type == 'std':
        final_df = getStandardDeviation(
            df[req_cols_list], group_list).reset_index()
    else:
        final_df = pd.DataFrame()

    return final_df.fillna(0.0)


def generateResponse(df, group_cols, calc_cols):
    return df.groupby(group_cols)[calc_cols].agg(list).reset_index().to_json(orient='records')


def getGPSBowlingData(fitness_data, from_date, to_date, month_date):

    fitness_data['record_date_form'] = fitness_data['record_date'].apply(
        lambda x: datetime.strptime(datetime.strptime(str(x), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                                    '%Y-%m-%d').date())

    fitness_data['period_name'] = fitness_data['period_name'].apply(lambda x: x.lower())
    fitness_data['activity_name'] = fitness_data['activity_name'].apply(lambda x: x.lower())
    training_conditions = [(fitness_data["period_name"].str.contains("warm")),
                           (fitness_data["activity_name"].str.contains(
                               "train|practice|session|net|prep|pre|throwing|skills|running|quarter")) | (
                           fitness_data["period_name"].str.contains(
                               "train|practice|session|net|prep|pre|throwing|skills|running|quarter")),
                           (fitness_data["period_name"].str.contains("innings|scenario|duties|match|innnings")),
                           (~fitness_data["period_name"].str.contains(
                               "train|practice|session|net|prep|pre|throwing|skills|running|quarter")) &
                           (~fitness_data["period_name"].str.contains("innings|scenario|duties|match"))
                           ]

    training_values = ['Warm Up', 'Practice', 'Match', 'Practice']
    fitness_data['gen_period_name'] = np.select(training_conditions, training_values)
    fitness_data = fitness_data[fitness_data['gen_period_name'].isin(['Match', 'Practice'])]
    fitness_data_7d_match = fitness_data[
        (fitness_data['record_date_form'] >= from_date) & (fitness_data['record_date_form'] <= to_date)
        & (fitness_data['gen_period_name'] == 'Match')].groupby(['player_name', 'player_id']).agg(
        {'ball_no': 'count'}).reset_index().rename(columns={"ball_no": "match"})

    fitness_data_7d_practice = fitness_data[
        (fitness_data['record_date_form'] >= from_date) & (fitness_data['record_date_form'] <= to_date)
        & (fitness_data['gen_period_name'] == 'Practice')].groupby(['player_name', 'player_id']).agg(
        {'ball_no': 'count'}).reset_index().rename(columns={"ball_no": "training"})

    chronic = fitness_data[
        (fitness_data['record_date_form'] >= month_date) & (fitness_data['record_date_form'] <= to_date)].groupby(['player_name', 'player_id']).agg(
        {'ball_no': 'count'}).reset_index().rename(columns={"ball_no": "chronic"})
    chronic['chronic'] = chronic['chronic'].apply(lambda x: int(round(x/4)))
    bowling_sessions = fitness_data[
        (fitness_data['record_date_form'] >= from_date) & (fitness_data['record_date_form'] <= to_date)].groupby(
        ['player_id','player_name', 'record_date'])['period_name'].nunique().reset_index().rename(columns={"period_name": "bowling_sessions"})
    bowling_sessions = bowling_sessions.groupby(['player_name', 'player_id']).agg({'bowling_sessions': 'sum'}).reset_index()
    train_match = pd.merge(fitness_data_7d_practice, fitness_data_7d_match, on='player_name', how='outer')
    train_match['player_id'] = train_match['player_id_x'].fillna(train_match['player_id_y']).fillna(-1).astype(int)
    train_mat_chr = pd.merge(train_match.drop(['player_id_x', 'player_id_y'], axis=1), chronic, on='player_name', how='outer')
    train_mat_chr['player_id'] = train_mat_chr['player_id_x'].fillna(train_mat_chr['player_id_y']).fillna(-1).astype(int)
    final_data = pd.merge(train_mat_chr.drop(['player_id_x', 'player_id_y'], axis=1), bowling_sessions, on="player_name", how="outer")
    final_data['player_id'] = final_data['player_id_x'].fillna(final_data['player_id_y']).fillna(-1).astype(int)
    final_data['match'] = final_data['match'].fillna(0).astype(int)
    final_data['training'] = final_data['training'].fillna(0).astype(int)
    final_data['total_deliveries'] = final_data['training'] + final_data['match']
    final_data['bowling_sessions'] = final_data['bowling_sessions'].fillna(0).astype(int)
    final_data['loading_status'] = (abs(final_data['total_deliveries'].fillna(0)- final_data['chronic'].fillna(0))/ final_data['total_deliveries'].fillna(0)) * 100
    final_data = final_data.replace(np.inf, 0)
    final_data = final_data[['player_name','player_id', 'training', 'match', 'total_deliveries', 'chronic', 'loading_status',
                             'bowling_sessions']].fillna(0).round(2)
    final_data = final_data.replace([np.inf, -np.inf], np.NaN).fillna(0)
    return final_data, json.loads(re.sub(r'\binfinity\b', '\"\"', final_data.to_json(orient='records')))


def getFormBowlingData(fitness_form_df, from_date, to_date, month_date):

    fitness_form_df['record_date_form'] = fitness_form_df['record_date'].apply(
        lambda x: datetime.strptime(datetime.strptime(str(x), '%A, %b %d, %Y').strftime('%Y-%m-%d'),
                                    '%Y-%m-%d').date())
    fitness_form_df = fitness_form_df.mask(fitness_form_df == -1, 0)
    fitness_form_acute = fitness_form_df[
        (fitness_form_df['record_date_form'] >= from_date) & (fitness_form_df['record_date_form'] <= to_date)].groupby(
        ['player_name', 'player_id']).agg({'bowling_train_balls': 'sum',
                              'bowling_match_balls': 'sum',
                              'record_date_form': 'nunique'}).reset_index() \
        .rename(columns={'bowling_train_balls': 'training', 'bowling_match_balls': 'match',
                         'record_date_form': 'bowling_sessions'})

    fitness_form_chronic = fitness_form_df[
        (fitness_form_df['record_date_form'] >= month_date) & (fitness_form_df['record_date_form'] <= to_date)].groupby(
        ['player_name', 'player_id']). \
        agg({'bowling_train_balls': 'sum'}).reset_index().rename(columns={'bowling_train_balls': 'chronic'})
    fitness_form_chronic['chronic'] = fitness_form_chronic['chronic'].apply(lambda x: int(round(x/4)))
    fitness_form_tr_chr = pd.merge(fitness_form_acute, fitness_form_chronic, on='player_name', how='outer')
    fitness_form_tr_chr['player_id'] = fitness_form_tr_chr['player_id_x'].fillna(fitness_form_tr_chr['player_id_y']).fillna(-1).astype(int)
    fitness_form_tr_chr['match'] = fitness_form_tr_chr['match'].fillna(0).astype(int)
    fitness_form_tr_chr['training'] = fitness_form_tr_chr['training'].fillna(0).astype(int)
    fitness_form_tr_chr['total_deliveries'] = fitness_form_tr_chr['training'] + fitness_form_tr_chr['match']
    fitness_form_tr_chr['loading_status'] = (abs(fitness_form_tr_chr['total_deliveries'].fillna(0) -
                                              fitness_form_tr_chr['chronic'].fillna(0)) / fitness_form_tr_chr['total_deliveries'].fillna(0)) * 100

    fitness_form_tr_chr['bowling_sessions'] = fitness_form_tr_chr['bowling_sessions'].fillna(0).astype(int)
    fitness_form_tr_chr = fitness_form_tr_chr[
        ['player_name', 'player_id','training', 'match', 'total_deliveries', 'chronic', 'loading_status',
         'bowling_sessions']].fillna(0).round(2)
    fitness_form_tr_chr = fitness_form_tr_chr.replace([np.inf, -np.inf], np.NaN).fillna(0)
    return fitness_form_tr_chr, json.loads(re.sub(r'\binfinity\b', '\"\"', fitness_form_tr_chr.to_json(orient='records')))
