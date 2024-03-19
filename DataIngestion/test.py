from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session
from common.utils.helper import getPrettyDF


def test_venue():
    GET_VENUE_DATA = '''select venue_id, src_venue_id, stadium_name from CricketSimulatorDB.Venue;'''
    venues_df = getPandasFactoryDF(session, GET_VENUE_DATA)

    venues_df["stadium_name"] = venues_df["stadium_name"].apply(lambda x: x.strip().replace(" ", "").lower())

    assert venues_df.duplicated(subset=['stadium_name']).sum() == 0

    # duplicate id check
    assert venues_df.duplicated(subset=['src_venue_id']).sum() == 0


def test_teams():
    GET_TEAMS_DATA = '''select team_id, src_team_id, team_name, team_short_name, competition_name, playoffs, 
     titles from CricketSimulatorDB.Teams;'''
    teams_df = getPandasFactoryDF(session, GET_TEAMS_DATA)
    teams_df["team_name"] = teams_df["team_name"].apply(lambda x: x.strip().replace(" ", "").lower())

    assert teams_df.duplicated(subset=['team_name']).sum() == 0

    # duplicate id check
    assert teams_df.duplicated(subset=['src_team_id']).sum() == 0

    # team_short_name nulls check
    assert teams_df["team_short_name"].isnull().values.any() == False


def test_players():
    GET_PLAYERS_DATA = '''select player_id, src_player_id, player_name, team_id, competition_name, season
    from CricketSimulatorDB.Players; '''
    players_df = getPandasFactoryDF(session, GET_PLAYERS_DATA)
    players_df["player_name"] = players_df["player_name"].apply(lambda x: x.strip().replace(" ", "").lower())
    print(players_df[players_df["player_name"] == "alexcarey"])
    # 526, 133, 583, 490
    # duplicate names check
    assert players_df.duplicated(subset=['player_name', 'competition_name', 'season']).sum() == 0

    # duplicate id check
    assert players_df.duplicated(subset=['src_player_id', 'competition_name', 'season']).sum() == 0

    # team_id nulls check
    assert players_df["team_id"].isnull().values.any() == False


def test_matches():
    GET_MATCHES_DATA = '''select match_id, match_name,src_match_id, competition_name, venue, team1, team2, match_date, season,
    winning_team, match_result, team1_players, team2_players, toss_team from CricketSimulatorDB.Matches;'''
    matches_df = getPandasFactoryDF(session, GET_MATCHES_DATA)
    matches_df_2022 = matches_df[matches_df['season']==2022]
    print(getPrettyDF(matches_df_2022))

    matches_df["match_result"] = matches_df["match_result"].apply(lambda x: x.strip().replace(" ", "").lower())
    # check for duplicates
    assert matches_df.duplicated(subset=['venue', 'team1', 'team2', 'match_date']).sum() == 0

    # venue nulls check
    assert matches_df["venue"].isnull().values.any() == False

    # toss_team nulls check
    assert matches_df["toss_team"].isnull().values.any() == False

    # team1 nulls check
    assert matches_df["team1"].isnull().values.any() == False

    # team2 nulls check
    assert matches_df["team2"].isnull().values.any() == False

    # team1_players nulls check
    assert matches_df["team1_players"].isnull().values.any() == False

    # team1_players nulls check
    assert matches_df["team2_players"].isnull().values.any() == False

    matches_winning_df = matches_df[matches_df["winning_team"] == -1]
    matches_winning_df = matches_winning_df[(matches_winning_df["match_result"] != "noresult") &
                                            (matches_winning_df["match_result"] != "matchabandoned")
                                            & (matches_winning_df["match_result"] != "abandonedduetorain") & (
                                                    matches_winning_df["match_result"] != "tie")]

    assert matches_winning_df.count()[0] == 0

    # print(getPrettyDF(matches_winning_df))


def test_matchBallSummary():
    GET_BALL_SUMMARY_DATA = '''select id, is_two, is_bye,is_wicket,innings,is_leg_bye,is_one,is_wide,
    bowl_line, extras,runs,ball_number,wicket_type,is_no_ball,is_bowler_wicket,bowl_length,is_four,shot_type,bowl_type,
    is_six,is_maiden, is_three,is_dot_ball,is_extra,batting_position,match_id,batsman_id, y_pitch, x_pitch,fielder_angle
    , against_bowler,batsman_team_id, bowler_team_id,over_number, batting_phase,over_text,fielder_length_ratio
    from CricketSimulatorDB.MatchBallSummary;'''

    ball_summary_df = getPandasFactoryDF(session, GET_BALL_SUMMARY_DATA)
    # ball_summary_df[['y_pitch', 'x_pitch', 'fielder_angle', 'fielder_length_ratio']] = \
    #     ball_summary_df[['y_pitch', 'x_pitch', 'fielder_angle', 'fielder_length_ratio']] \
    #     .apply(pd.to_numeric, errors='coerce')
    #
    # dupesdf = psql.sqldf('''select  match_id, batsman_id, against_bowler, over_number, ball_number, extras,
    # count(match_id) as cnt from ball_summary_df group by match_id, batsman_id, against_bowler, over_number,
    # ball_number, extras having count(match_id)>1''')

    # dupesdf = psql.sqldf('''select  * from ball_summary_df where match_id=2 and  batsman_id=723 and
    #   against_bowler=229 and over_number=2 and  ball_number=2 and extras=0''')
    # # 288 |          192 |              168 |             2 |             1 |        0 |     4 |
    #
    # print(getPrettyDF(dupesdf))

    # check for duplicates
    # assert ball_summary_df.duplicated(subset=['match_id', 'batsman_id', 'against_bowler',
    #                                           'over_number', 'ball_number', 'extras']).sum() == 0

    # check for non-join values in match_id
    assert ball_summary_df[ball_summary_df["match_id"] == -1].count()[0] == 0

    # check for non-join values in batsman_id
    assert ball_summary_df[ball_summary_df["batsman_id"] == -1].count()[0] == 0

    # check for non-join values in against_bowler
    assert ball_summary_df[ball_summary_df["against_bowler"] == -1].count()[0] == 0

    # check for non-join values in batsman_team_id
    assert ball_summary_df[ball_summary_df["batsman_team_id"] == -1].count()[0] == 0

    # check for non-join values in bowler_team_id
    assert ball_summary_df[ball_summary_df["bowler_team_id"] == -1].count()[0] == 0

    # check for non-join values in bowler_team_id
    assert ball_summary_df["batting_position"].isnull().values.any() == False


def test_batsmanBowlerMatchStats():
    GET_BATSMAN_BOWLER_DATA = '''select id, batsman_id, match_id, innings, batting_position, batting_phase,
    against_bowler, dismissal_type from CricketSimulatorDB.BatsmanBowlerMatchStatistics;'''

    batsman_bowler_df = getPandasFactoryDF(session, GET_BATSMAN_BOWLER_DATA)

    # check for duplicates
    assert batsman_bowler_df.duplicated(subset=['match_id', 'batsman_id', 'against_bowler', 'batting_phase',
                                                'innings', 'dismissal_type', 'batting_position']).sum() == 0


def test_matchBattingCard():
    GET_BAT_CARD_DATA = '''select id, match_id, innings, batting_team_id, batsman_id, batting_position,
    bowler_id, out_desc from CricketSimulatorDB.MatchBattingCard;'''

    bat_card_df = getPandasFactoryDF(session, GET_BAT_CARD_DATA)

    bat_card_df["out_desc"] = bat_card_df["out_desc"].apply(lambda x: x.strip().replace(" ", "").lower())

    # check for duplicates
    assert bat_card_df.duplicated(subset=['match_id', 'batsman_id', 'bowler_id', 'innings', ]).sum() == 0

    # check for non-join values in match_id
    assert bat_card_df[bat_card_df["match_id"] == -1].count()[0] == 0

    # check for non-join values in batsman_id
    assert bat_card_df[bat_card_df["batsman_id"] == -1].count()[0] == 0

    not_outDF = bat_card_df[(bat_card_df['out_desc']!="notout") & (bat_card_df['out_desc']!="na")]

    # check for non-join values in bowler_id after removing the players who remained not out and hasn't played
    assert not_outDF[not_outDF["bowler_id"] == -1].count()[0] == 0

    # check for non-join values in batting_team_id
    assert bat_card_df[bat_card_df["batting_team_id"] == -1].count()[0] == 0


def test_matchBowlingCard():
    GET_BOWL_CARD_DATA = '''select id,match_id, innings, team_id, bowler_id from CricketSimulatorDB.MatchBowlingCard;'''

    bowl_card_df = getPandasFactoryDF(session, GET_BOWL_CARD_DATA)

    # check for duplicates
    assert bowl_card_df.duplicated(subset=['match_id', 'team_id', 'bowler_id', 'innings', ]).sum() == 0

    # check for non-join values in batsman_id
    assert bowl_card_df[bowl_card_df["match_id"] == -1].count()[0] == 0

    # check for non-join values in bowler_id
    assert bowl_card_df[bowl_card_df["bowler_id"] == -1].count()[0] == 0

    # check for non-join values in batting_team_id
    assert bowl_card_df[bowl_card_df["team_id"] == -1].count()[0] == 0


def test_matchExtras():
    GET_EXTRAS_DATA = '''select id,match_id, innings, team_id, total_extras,no_balls, byes, wides, leg_byes
      from CricketSimulatorDB.MatchExtras;'''

    extras_df = getPandasFactoryDF(session, GET_EXTRAS_DATA)

    # check for duplicates
    assert extras_df.duplicated(subset=['match_id', 'team_id']).sum() == 0

    # check for non-join values in match_id
    assert extras_df[extras_df["match_id"] == -1].count()[0] == 0

    # check for non-join values in team_id
    assert extras_df[extras_df["team_id"] == -1].count()[0] == 0


def test_matchPartnerships():
    GET_PARTNERSHIP_DATA = '''select id,match_id, innings, team_id, striker,partnership_total, striker_runs, 
    striker_balls, extras, non_striker_runs,non_striker_balls, non_striker  from CricketSimulatorDB.MatchPartnership;'''

    partnership_df = getPandasFactoryDF(session, GET_PARTNERSHIP_DATA)

    # check for duplicates
    assert partnership_df.duplicated(subset=['match_id', 'innings', 'striker', 'non_striker', ]).sum() == 0

    # check for non-join values in match_id
    assert partnership_df[partnership_df["match_id"] == -1].count()[0] == 0

    # check for non-join values in team_id
    assert partnership_df[partnership_df["team_id"] == -1].count()[0] == 0

    # check for non-join values in striker
    assert partnership_df[partnership_df["striker"] == -1].count()[0] == 0

    # check for non-join values in non_striker
    assert partnership_df[partnership_df["non_striker"] == -1].count()[0] == 0


def test_batsmanGlobalStats():
    GET_BATSMAN_GLOBAL_DATA = '''select id, batsman_id, competition_name, season, total_matches_played, 
total_innings_batted, total_runs, batting_average, strike_rate, fifties, not_outs, duck_outs, num_sixes, num_fours, 
    highest_score  from CricketSimulatorDB.BatsmanGlobalStats;'''

    batsman_global_df = getPandasFactoryDF(session, GET_BATSMAN_GLOBAL_DATA)

    # check for duplicates
    assert batsman_global_df.duplicated(subset=['batsman_id', 'competition_name', 'season']).sum() == 0


def test_bowlerGlobalStats():
    GET_BOWLER_GLOBAL_DATA = '''select id, bowler_id, competition_name, season, total_matches_played, 
total_innings_played, total_balls_bowled, total_overs_bowled, total_runs_conceded, bowling_average, bowling_economy, 
 bowling_strike_rate, num_four_wkt_hauls, num_five_wkt_hauls, num_ten_wkt_hauls, num_extras_conceded, total_wickets,
 best_figure  from CricketSimulatorDB.BowlerGlobalStats;'''

    bowler_global_df = getPandasFactoryDF(session, GET_BOWLER_GLOBAL_DATA)

    # check for duplicates
    assert bowler_global_df.duplicated(subset=['bowler_id', 'competition_name', 'season']).sum() == 0

