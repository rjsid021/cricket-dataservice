from DataService.utils.helper import addColumnsToSQL, executeQuery, connection_duckdb

con = connection_duckdb()


def getBatsmanAggContributionDF(filters, match_phase, innings, params, sort_key, asc, auction, retained_players_list, in_auction_list):
    if filters:
        batsman_contribution_sql = '''select player_id, player,
        cast(coalesce(round(sum(batting_consistency_score)/sum(bat_innings)),0) as int) as
             batting_consistency_score, cast(coalesce(round(sum(batting_contribution_score)/sum(bat_innings)),0) as int)
             as batting_contribution_score, cast(sum(coalesce(runs_scored,0)) as int) as runs_scored,
             cast(sum(coalesce(balls_played,0)) as int) as balls_played, cast(sum(bat_innings) as int) as bat_innings,
             round(coalesce(sum(coalesce(runs_scored,0))*100.00/sum(coalesce(balls_played,0)),0), 2) as avg_strike_rate,
             coalesce(round((sum(runs_scored)*1.00)/sum(dismissal),2),0.0) as batting_average,
             cast(sum(total_thirty_plus_scores) as int) as total_thirty_plus_scores
                          from contribution_agg_data ''' + filters + ''' group by player_id, player'''

    else:
        params = []
        batsman_contribution_sql = '''select player_id, player,
        cast(coalesce(round(sum(batting_consistency_score)/sum(bat_innings)),0) as int) as
             batting_consistency_score, cast(coalesce(round(sum(batting_contribution_score)/sum(bat_innings)),0) as int)
             as batting_contribution_score, cast(sum(coalesce(runs_scored,0)) as int) as runs_scored,
             cast(sum(coalesce(balls_played,0)) as int) as balls_played, cast(sum(bat_innings) as int) as bat_innings,
             round(coalesce(sum(coalesce(runs_scored,0))*100.00/sum(coalesce(balls_played,0)),0), 2) as avg_strike_rate,
             coalesce(round((sum(runs_scored)*1.00)/sum(dismissal),2),0.0) as batting_average,
             cast(sum(total_thirty_plus_scores) as int) as total_thirty_plus_scores
                          from contribution_agg_data group by player_id, player'''

    if innings:
        batsman_contribution_sql = batsman_contribution_sql + f" having sum(bat_innings)>=?"
        params.append(innings)
    else:
        batsman_contribution_sql = batsman_contribution_sql

    if match_phase:
        if match_phase == "POWERPLAY":
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "batting_contribution_score",
                                                       "bat_powerplay_contribution_score")
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "runs_scored",
                                                       "actual_powerplay_over_runs")
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "batting_consistency_score",
                                                       "bat_consistency_score_powerplay")
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "balls_played",
                                                       "actual_powerplay_over_balls")

        elif match_phase == "7-10 OVERS":
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "batting_contribution_score",
                                                       "bat_7_10_overs_contribution_score")
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "runs_scored",
                                                       "actual_7_10_over_runs")
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "batting_consistency_score",
                                                       "bat_consistency_score_7_10")
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "balls_played",
                                                       "actual_7_10_over_balls")

        elif match_phase == "11-15 OVERS":
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "batting_contribution_score",
                                                       "bat_11_15_overs_contribution_score")
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "runs_scored",
                                                       "actual_11_15_over_runs")
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "batting_consistency_score",
                                                       "bat_consistency_score_11_15")
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "balls_played",
                                                       "actual_11_15_over_balls")
        elif match_phase == "DEATH OVERS":
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "batting_contribution_score",
                                                       "bat_deathovers_contribution_score")
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "runs_scored",
                                                       "actual_death_over_runs")
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "batting_consistency_score",
                                                       "bat_consistency_score_deathovers")
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "balls_played",
                                                       "actual_death_over_balls")
    else:
        batsman_contribution_sql = batsman_contribution_sql

    contribution_df = executeQuery(con, batsman_contribution_sql, params).rename(
        columns={"bat_powerplay_contribution_score": "batting_contribution_score",
                 "bat_7_10_overs_contribution_score": "batting_contribution_score",
                 "bat_11_15_overs_contribution_score": "batting_contribution_score",
                 "bat_deathovers_contribution_score": "batting_contribution_score",
                 "actual_powerplay_over_runs": "runs_scored",
                 "actual_7_10_over_runs": "runs_scored",
                 "actual_11_15_over_runs": "runs_scored",
                 "actual_death_over_runs": "runs_scored",
                 "bat_consistency_score_powerplay": "batting_consistency_score",
                 "bat_consistency_score_7_10": "batting_consistency_score",
                 "bat_consistency_score_11_15": "batting_consistency_score",
                 "bat_consistency_score_deathovers": "batting_consistency_score",
                 "actual_powerplay_over_balls":"balls_played",
                 "actual_7_10_over_balls":"balls_played",
                 "actual_11_15_over_balls":"balls_played",
                 "actual_death_over_balls":"balls_played"
                 })
    if auction:
        if auction == "Retained":
            contribution_df = contribution_df[contribution_df['player'].isin(retained_players_list)]
        elif auction == "In-Auction":
            contribution_df = contribution_df[contribution_df['player'].isin(in_auction_list)]
        elif auction == "Others":
            retained_players_list.extend(in_auction_list)
            contribution_df = contribution_df[~contribution_df['player'].isin(retained_players_list)]
        else:
            contribution_df = contribution_df

    return contribution_df.sort_values(sort_key, ascending=asc)


def getBowlerAggContributionDF(filters, match_phase, innings, params, sort_key, asc, auction, retained_players_list, in_auction_list):
    if filters:
        bowler_contribution_sql = '''select player_id, player, 
        cast(coalesce(round(sum(bowling_consistency_score)/sum(bowl_innings)),0) as int) as bowling_consistency_score,
        cast(coalesce(round(sum(bowling_contribution_score)/sum(bowl_innings)),0) as int) as bowling_contribution_score, 
         round(sum(overall_economy)/sum(bowl_innings),2) as overall_economy, cast(sum(wickets_taken) as int) as wickets_taken,
         round(sum(coalesce(total_balls_bowled,0))/sum(wickets_taken),2) as bowling_strike_rate,
         coalesce(round((sum(runs_conceded)*1.00)/sum(wickets_taken),2),0.0) as bowling_average, 
         cast(sum(total_three_plus_wickets) as int) as total_three_plus_wickets 
          from contribution_agg_data ''' + filters + ''' group by player_id, player'''
    else:
        params = []
        bowler_contribution_sql = '''select player_id, player, 
        cast(coalesce(round(sum(bowling_consistency_score)/sum(bowl_innings)),0) as int) as bowling_consistency_score,
        cast(coalesce(round(sum(bowling_contribution_score)/sum(bowl_innings)),0) as int) as bowling_contribution_score, 
         round(sum(overall_economy)/sum(bowl_innings),2) as overall_economy, cast(sum(wickets_taken) as int) as wickets_taken,
         round(sum(coalesce(total_balls_bowled,0))/sum(wickets_taken),2) as bowling_strike_rate,
         coalesce(round((sum(runs_conceded)*1.00)/sum(wickets_taken),2),0.0) as bowling_average, 
         cast(sum(total_three_plus_wickets) as int) as total_three_plus_wickets 
          from contribution_agg_data group by player_id, player'''

    if innings:
        bowler_contribution_sql = bowler_contribution_sql + f" having sum(bowl_innings)>=?"
        params.append(innings)
    else:
        bowler_contribution_sql = bowler_contribution_sql

    if match_phase:
        if match_phase == "POWERPLAY":
            bowler_contribution_sql = addColumnsToSQL(bowler_contribution_sql, "bowling_contribution_score",
                                                      "bowl_powerplay_contribution_score")
            bowler_contribution_sql = addColumnsToSQL(bowler_contribution_sql, "bowling_consistency_score",
                                                      "bowl_consistency_score_powerplay")
        elif match_phase == "7-10 OVERS":
            bowler_contribution_sql = addColumnsToSQL(bowler_contribution_sql, "bowling_contribution_score",
                                                      "bowl_7_10_overs_contribution_score")
            bowler_contribution_sql = addColumnsToSQL(bowler_contribution_sql, "bowling_consistency_score",
                                                      "bowl_consistency_score_7_10")
        elif match_phase == "11-15 OVERS":
            bowler_contribution_sql = addColumnsToSQL(bowler_contribution_sql, "bowling_contribution_score",
                                                      "bowl_11_15_overs_contribution_score")
            bowler_contribution_sql = addColumnsToSQL(bowler_contribution_sql, "bowling_consistency_score",
                                                      "bowl_consistency_score_11_15")

        elif match_phase == "DEATH OVERS":
            bowler_contribution_sql = addColumnsToSQL(bowler_contribution_sql, "bowling_contribution_score",
                                                      "bowl_deathovers_contribution_score")
            bowler_contribution_sql = addColumnsToSQL(bowler_contribution_sql, "bowling_consistency_score",
                                                      "bowl_consistency_score_deathovers")
    else:
        bowler_contribution_sql = bowler_contribution_sql

    contribution_df = executeQuery(con, bowler_contribution_sql, params).rename(
        columns={"bowl_powerplay_contribution_score": "bowling_contribution_score",
                 "bowl_7_10_overs_contribution_score": "bowling_contribution_score",
                 "bowl_11_15_overs_contribution_score": "bowling_contribution_score",
                 "bowl_deathovers_contribution_score": "bowling_contribution_score",
                 "bowl_consistency_score_powerplay": "bowling_consistency_score",
                 "bowl_consistency_score_7_10": "bowling_consistency_score",
                 "bowl_consistency_score_11_15": "bowling_consistency_score",
                 "bowl_consistency_score_deathovers": "bowling_consistency_score"
                 })

    if auction:
        if auction == "Retained":
            contribution_df = contribution_df[contribution_df['player'].isin(retained_players_list)]
        elif auction == "In-Auction":
            contribution_df = contribution_df[contribution_df['player'].isin(in_auction_list)]
        elif auction == "Others":
            retained_players_list.extend(in_auction_list)
            contribution_df = contribution_df[~contribution_df['player'].isin(retained_players_list)]
        else:
            contribution_df = contribution_df

    return contribution_df.sort_values(sort_key, ascending=asc)


def getAllRounderAggContributionDF(filters, match_phase, innings, params, sort_key, asc):
    if filters:
        allrounder_contribution_sql = '''select player_id, player, 
    cast(coalesce(round(sum(overall_consistency_score)/(sum(bat_innings)+sum(bowl_innings))),0) as int) as 
    overall_consistency_score,
    cast(coalesce(round(sum(batting_consistency_score)/sum(bat_innings)),0) as int) as
    batting_consistency_score,  
    cast(coalesce(round(sum(bowling_consistency_score)/sum(bowl_innings)),0) as int) as bowling_consistency_score,
    cast(coalesce(round(sum(overall_contribution_score)/(sum(bat_innings)+sum(bowl_innings))),0) as int) as overall_contribution_score,
    cast(coalesce(round(sum(batting_contribution_score)/sum(bat_innings)),0) as int) as batting_contribution_score,
    cast(coalesce(round(sum(bowling_contribution_score)/sum(bowl_innings)),0) as int) as bowling_contribution_score 
     from contribution_agg_data ''' + filters + ''' group by player_id, player'''

    else:
        params = []
        allrounder_contribution_sql = '''select player_id, player, 
    cast(coalesce(round(sum(overall_consistency_score)/(sum(bat_innings)+sum(bowl_innings))),0) as int) as 
    overall_consistency_score,
    cast(coalesce(round(sum(batting_consistency_score)/sum(bat_innings)),0) as int) as
    batting_consistency_score, 
    cast(coalesce(round(sum(bowling_consistency_score)/sum(bowl_innings)),0) as int) as bowling_consistency_score,
    cast(coalesce(round(sum(overall_contribution_score)/(sum(bat_innings)+sum(bowl_innings))),0) as int) as overall_contribution_score,
    cast(coalesce(round(sum(batting_contribution_score)/sum(bat_innings)),0) as int) as batting_contribution_score,
    cast(coalesce(round(sum(bowling_contribution_score)/sum(bowl_innings)),0) as int) as bowling_contribution_score 
     from contribution_agg_data group by player_id, player'''

    if innings:
        if "BATTED" in innings.split(" "):
            allrounder_contribution_sql = allrounder_contribution_sql + f" having sum(bat_innings)>=?"
            params.append(innings)
        elif "BOWLED" in innings.split(" "):
            allrounder_contribution_sql = allrounder_contribution_sql + f" having sum(bowl_innings)>=?"
            params.append(innings)
    else:
        allrounder_contribution_sql = allrounder_contribution_sql

    if match_phase:
        if match_phase == "POWERPLAY":
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "overall_contribution_score",
                                                          "overall_powerplay_contribution_score")
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "bowling_contribution_score",
                                                          "bowl_powerplay_contribution_score")
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "batting_contribution_score",
                                                          "bat_powerplay_contribution_score")
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "runs_scored",
                                                          "actual_powerplay_over_runs")

        elif match_phase == "7-10 OVERS":
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "overall_contribution_score",
                                                          "overall_7_10_overs_contribution_score")
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "batting_contribution_score",
                                                          "bat_7_10_overs_contribution_score")
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "bowling_contribution_score",
                                                          "bowl_7_10_overs_contribution_score")
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "runs_scored",
                                                          "actual_7_10_over_runs")

        elif match_phase == "11-15 OVERS":
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "overall_contribution_score",
                                                          "overall_11_15_overs_contribution_score")
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "batting_contribution_score",
                                                          "bat_11_15_overs_contribution_score")
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "bowling_contribution_score",
                                                          "bowl_11_15_overs_contribution_score")
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "runs_scored",
                                                          "actual_11_15_over_runs")

        elif match_phase == "DEATH OVERS":
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "overall_contribution_score",
                                                          "overall_deathovers_contribution_score")
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "batting_contribution_score",
                                                          "bat_deathovers_contribution_score")
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "bowling_contribution_score",
                                                          "bowl_deathovers_contribution_score")
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "runs_scored",
                                                          "actual_death_over_runs")

    else:
        allrounder_contribution_sql = allrounder_contribution_sql

    contribution_df = executeQuery(con, allrounder_contribution_sql, params).rename(
        columns={"bowl_powerplay_contribution_score": "bowling_contribution_score",
                 "bowl_7_10_overs_contribution_score": "bowling_contribution_score",
                 "bowl_11_15_overs_contribution_score": "bowling_contribution_score",
                 "bowl_deathovers_contribution_score": "bowling_contribution_score",
                 "bat_powerplay_contribution_score": "batting_contribution_score",
                 "bat_7_10_overs_contribution_score": "batting_contribution_score",
                 "bat_11_15_overs_contribution_score": "batting_contribution_score",
                 "bat_deathovers_contribution_score": "batting_contribution_score",
                 "overall_powerplay_contribution_score": "overall_contribution_score",
                 "overall_7_10_overs_contribution_score": "overall_contribution_score",
                 "overall_11_15_overs_contribution_score": "overall_contribution_score",
                 "overall_deathovers_contribution_score": "overall_contribution_score",
                 "actual_powerplay_over_runs": "runs_scored",
                 "actual_7_10_over_runs": "runs_scored",
                 "actual_11_15_over_runs": "runs_scored",
                 "actual_death_over_runs": "runs_scored"
                 })

    return contribution_df.sort_values(sort_key, ascending=asc)


def getBatsmanContributionDF(filters, match_phase, params, auction, retained_players_list, in_auction_list):
    if filters:
        batsman_contribution_sql = '''select match_date, game_id, player_id, player, batting_consistency_score, 
        batting_contribution_score, cast(runs_scored as int) as runs_scored, cast(balls_faced as int) as balls_played,
        cast(bat_innings  as int) as bat_innings,
        round(coalesce(coalesce(runs_scored,0)*100.00/coalesce(balls_faced,0),0), 2) as batting_strike_rate, 
         cast(coalesce(overall_fours,0)+coalesce(overall_sixes,0) as int) as boundaries,
        cast(coalesce(fow_during_stay,0) as int) as fow_during_stay, 
        cast(coalesce(runs_scored,0)+coalesce(non_striker_runs,0) as int) as partnership_runs,
        cast(arrived_on as int) as arrived_on, dismissed_on, cast(coalesce(non_striker_runs,0) as int) as non_striker_runs,
        case when bat_expectations='MET' then 1 when 
        bat_expectations='NOT MET' then 0 when bat_expectations='EXCEEDED' then 2 else 0 end as bat_expectations
         from contribution_data ''' + filters + ''' and bat_innings is not null;'''

    else:
        params = []
        batsman_contribution_sql = '''select match_date, game_id, player_id, player, batting_consistency_score, 
                batting_contribution_score, cast(runs_scored as int) as runs_scored, cast(balls_faced as int) as balls_played,
                cast(bat_innings  as int) as bat_innings,
                round(coalesce(coalesce(runs_scored,0)*100.00/coalesce(balls_faced,0),0), 2) as batting_strike_rate,
                cast(coalesce(overall_fours,0)+coalesce(overall_sixes,0) as int) as boundaries,
                cast(coalesce(fow_during_stay,0) as int) as fow_during_stay, 
                cast(coalesce(runs_scored,0)+coalesce(non_striker_runs,0) as int) as partnership_runs,
                cast(arrived_on as int) as arrived_on, dismissed_on, cast(coalesce(non_striker_runs,0) as int) as non_striker_runs,
                case when bat_expectations='MET' then 1 when 
                bat_expectations='NOT MET' then 0 when bat_expectations='EXCEEDED' then 2 else 0 end as bat_expectations
                 from contribution_data where bat_innings is not null;'''

    if match_phase:
        if match_phase == "POWERPLAY":
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "batting_contribution_score",
                                                       "bat_powerplay_contribution_score")
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "runs_scored",
                                                       "actual_powerplay_over_runs")
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "batting_consistency_score",
                                                       "bat_consistency_score_powerplay")
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "balls_faced",
                                                       "actual_powerplay_over_balls")

        elif match_phase == "7-10 OVERS":
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "batting_contribution_score",
                                                       "bat_7_10_overs_contribution_score")
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "runs_scored",
                                                       "actual_7_10_over_runs")
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "batting_consistency_score",
                                                       "bat_consistency_score_7_10")
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "balls_faced",
                                                       "actual_7_10_over_balls")

        elif match_phase == "11-15 OVERS":
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "batting_contribution_score",
                                                       "bat_11_15_overs_contribution_score")
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "runs_scored",
                                                       "actual_11_15_over_runs")
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "batting_consistency_score",
                                                       "bat_consistency_score_11_15")
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "balls_faced",
                                                       "actual_11_15_over_balls")
        elif match_phase == "DEATH OVERS":
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "batting_contribution_score",
                                                       "bat_deathovers_contribution_score")
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "runs_scored",
                                                       "actual_death_over_runs")
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "batting_consistency_score",
                                                       "bat_consistency_score_deathovers")
            batsman_contribution_sql = addColumnsToSQL(batsman_contribution_sql, "balls_faced",
                                                       "actual_death_over_balls")
    else:
        batsman_contribution_sql = batsman_contribution_sql

    contribution_df = executeQuery(con, batsman_contribution_sql, params).rename(
        columns={"bat_powerplay_contribution_score": "batting_contribution_score",
                 "bat_7_10_overs_contribution_score": "batting_contribution_score",
                 "bat_11_15_overs_contribution_score": "batting_contribution_score",
                 "bat_deathovers_contribution_score": "batting_contribution_score",
                 "actual_powerplay_over_runs": "runs_scored",
                 "actual_7_10_over_runs": "runs_scored",
                 "actual_11_15_over_runs": "runs_scored",
                 "actual_death_over_runs": "runs_scored",
                 "bat_consistency_score_powerplay": "batting_consistency_score",
                 "bat_consistency_score_7_10": "batting_consistency_score",
                 "bat_consistency_score_11_15": "batting_consistency_score",
                 "bat_consistency_score_deathovers": "batting_consistency_score",
                #  "actual_powerplay_over_balls":"balls_faced",
                #  "actual_7_10_over_balls":"balls_faced",
                #  "actual_11_15_over_balls":"balls_faced",
                #  "actual_death_over_balls":"balls_faced"
                 })

    if auction:
        if auction == "Retained":
            contribution_df = contribution_df[contribution_df['player'].isin(retained_players_list)]
        elif auction == "In-Auction":
            contribution_df = contribution_df[contribution_df['player'].isin(in_auction_list)]
        elif auction == "Others":
            retained_players_list.extend(in_auction_list)
            contribution_df = contribution_df[~contribution_df['player'].isin(retained_players_list)]
        else:
            contribution_df = contribution_df

    return contribution_df


def getBowlerContributionDF(filters, match_phase, params, auction, retained_players_list, in_auction_list):
    if filters:
        bowler_contribution_sql = '''select match_date, game_id, player_id, player, bowling_consistency_score,
        bowling_contribution_score, overall_economy, wickets_taken, runs_conceded, is_hatrick,
        round(coalesce((total_balls_bowled/wickets_taken), 0),2) as bowling_strike_rate,
        case when bowl_expectations='MET' then 1 when 
        bowl_expectations='NOT MET' then 0 when bowl_expectations='EXCEEDED' then 2 else 0 end as bowl_expectations
         from contribution_data ''' + filters + ''' and bowl_innings is not null;'''
    else:
        params = []
        bowler_contribution_sql = '''select match_date, game_id, player_id, player, bowling_consistency_score,
            bowling_contribution_score, overall_economy, wickets_taken, runs_conceded, is_hatrick,
            round(coalesce(total_balls_bowled,0)/wickets_taken,2) as bowling_strike_rate,
            case when bowl_expectations='MET' then 1 when 
            bowl_expectations='NOT MET' then 0 when bowl_expectations='EXCEEDED' then 2 else 0 end as bowl_expectations
             from contribution_data ''' + filters + ''' and bowl_innings is not null;'''

    if match_phase:
        if match_phase == "POWERPLAY":
            bowler_contribution_sql = addColumnsToSQL(bowler_contribution_sql, "bowling_contribution_score",
                                                      "bowl_powerplay_contribution_score")
            bowler_contribution_sql = addColumnsToSQL(bowler_contribution_sql, "bowling_consistency_score",
                                                      "bowl_consistency_score_powerplay")
        elif match_phase == "7-10 OVERS":
            bowler_contribution_sql = addColumnsToSQL(bowler_contribution_sql, "bowling_contribution_score",
                                                      "bowl_7_10_overs_contribution_score")
            bowler_contribution_sql = addColumnsToSQL(bowler_contribution_sql, "bowling_consistency_score",
                                                      "bowl_consistency_score_7_10")
        elif match_phase == "11-15 OVERS":
            bowler_contribution_sql = addColumnsToSQL(bowler_contribution_sql, "bowling_contribution_score",
                                                      "bowl_11_15_overs_contribution_score")
            bowler_contribution_sql = addColumnsToSQL(bowler_contribution_sql, "bowling_consistency_score",
                                                      "bowl_consistency_score_11_15")

        elif match_phase == "DEATH OVERS":
            bowler_contribution_sql = addColumnsToSQL(bowler_contribution_sql, "bowling_contribution_score",
                                                      "bowl_deathovers_contribution_score")
            bowler_contribution_sql = addColumnsToSQL(bowler_contribution_sql, "bowling_consistency_score",
                                                      "bowl_consistency_score_deathovers")
    else:
        bowler_contribution_sql = bowler_contribution_sql

    contribution_df = executeQuery(con, bowler_contribution_sql, params).rename(
        columns={"bowl_powerplay_contribution_score": "bowling_contribution_score",
                 "bowl_7_10_overs_contribution_score": "bowling_contribution_score",
                 "bowl_11_15_overs_contribution_score": "bowling_contribution_score",
                 "bowl_deathovers_contribution_score": "bowling_contribution_score",
                 "bowl_consistency_score_powerplay": "bowling_consistency_score",
                 "bowl_consistency_score_7_10": "bowling_consistency_score",
                 "bowl_consistency_score_11_15": "bowling_consistency_score",
                 "bowl_consistency_score_deathovers": "bowling_consistency_score"
                 })

    if auction:
        if auction == "Retained":
            contribution_df = contribution_df[contribution_df['player'].isin(retained_players_list)]
        elif auction == "In-Auction":
            contribution_df = contribution_df[contribution_df['player'].isin(in_auction_list)]
        elif auction == "Others":
            retained_players_list.extend(in_auction_list)
            contribution_df = contribution_df[~contribution_df['player'].isin(retained_players_list)]
        else:
            contribution_df = contribution_df

    return contribution_df


def getAllRounderContributionDF(filters, match_phase, params):
    if filters:
        allrounder_contribution_sql = '''select match_date, game_id, player_id, player,
    overall_consistency_score, batting_consistency_score,bowling_consistency_score, overall_contribution_score,
     batting_contribution_score, bowling_contribution_score, case when bat_expectations='MET' then 1 when 
        bat_expectations='NOT MET' then 0 when bat_expectations='EXCEEDED' then 2 else 0 end as bat_expectations, 
        case when bowl_expectations='MET' then 1 when bowl_expectations='NOT MET' then 0 when 
        bowl_expectations='EXCEEDED' then 2 else 0 end as bowl_expectations from contribution_data ''' + filters

    else:
        params = []
        allrounder_contribution_sql = '''select match_date, game_id, player_id, player,
    overall_consistency_score, batting_consistency_score,bowling_consistency_score, overall_contribution_score,
     batting_contribution_score, bowling_contribution_score, case when bat_expectations='MET' then 1 when 
        bat_expectations='NOT MET' then 0 when bat_expectations='EXCEEDED' then 2 else 0 end as bat_expectations, 
        case when bowl_expectations='MET' then 1 when bowl_expectations='NOT MET' then 0 when 
        bowl_expectations='EXCEEDED' then 2 else 0 end as bowl_expectations from contribution_data'''

    if match_phase:
        if match_phase == "POWERPLAY":
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "overall_contribution_score",
                                                          "overall_powerplay_contribution_score")
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "bowling_contribution_score",
                                                          "bowl_powerplay_contribution_score")
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "batting_contribution_score",
                                                          "bat_powerplay_contribution_score")
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "runs_scored",
                                                          "actual_powerplay_over_runs")

        elif match_phase == "7-10 OVERS":
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "overall_contribution_score",
                                                          "overall_7_10_overs_contribution_score")
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "batting_contribution_score",
                                                          "bat_7_10_overs_contribution_score")
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "bowling_contribution_score",
                                                          "bowl_7_10_overs_contribution_score")
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "runs_scored",
                                                          "actual_7_10_over_runs")

        elif match_phase == "11-15 OVERS":
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "overall_contribution_score",
                                                          "overall_11_15_overs_contribution_score")
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "batting_contribution_score",
                                                          "bat_11_15_overs_contribution_score")
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "bowling_contribution_score",
                                                          "bowl_11_15_overs_contribution_score")
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "runs_scored",
                                                          "actual_11_15_over_runs")

        elif match_phase == "DEATH OVERS":
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "overall_contribution_score",
                                                          "overall_deathovers_contribution_score")
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "batting_contribution_score",
                                                          "bat_deathovers_contribution_score")
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "bowling_contribution_score",
                                                          "bowl_deathovers_contribution_score")
            allrounder_contribution_sql = addColumnsToSQL(allrounder_contribution_sql, "runs_scored",
                                                          "actual_death_over_runs")

    else:
        allrounder_contribution_sql = allrounder_contribution_sql

    contribution_df = executeQuery(con, allrounder_contribution_sql, params).rename(
        columns={"bowl_powerplay_contribution_score": "bowling_contribution_score",
                 "bowl_7_10_overs_contribution_score": "bowling_contribution_score",
                 "bowl_11_15_overs_contribution_score": "bowling_contribution_score",
                 "bowl_deathovers_contribution_score": "bowling_contribution_score",
                 "bat_powerplay_contribution_score": "batting_contribution_score",
                 "bat_7_10_overs_contribution_score": "batting_contribution_score",
                 "bat_11_15_overs_contribution_score": "batting_contribution_score",
                 "bat_deathovers_contribution_score": "batting_contribution_score",
                 "overall_powerplay_contribution_score": "overall_contribution_score",
                 "overall_7_10_overs_contribution_score": "overall_contribution_score",
                 "overall_11_15_overs_contribution_score": "overall_contribution_score",
                 "overall_deathovers_contribution_score": "overall_contribution_score",
                 "actual_powerplay_over_runs": "runs_scored",
                 "actual_7_10_over_runs": "runs_scored",
                 "actual_11_15_over_runs": "runs_scored",
                 "actual_death_over_runs": "runs_scored"
                 })

    return contribution_df
