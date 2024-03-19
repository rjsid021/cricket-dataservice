from pathlib import Path
import flask
import json
import os
import pytest
import sys

sys.path.append(os.path.abspath(Path(__file__).parent.parent))

from DataService.src import app

FR = flask.Response


@pytest.fixture
def client():
    with app.app.test_client() as client:
        yield client


def test_home(client):
    rv: FR = client.get("/")
    assert rv.is_json
    assert rv.get_json() is not None
    assert rv.status_code is 200


def test_getBatsmanStats_player(client):
    rv: FR = client.get("/getBatsmanStats_player/Aaron Finch")
    assert rv.is_json
    assert [
    {
        "Innings_Played": 87,
        "Num_dismissals": 97,
        "Striker": "Aaron Finch",
        "StrikerBattingType": "RIGHT HAND BATSMAN",
        "ballsfaced": 2060,
        "batting_average": 24.75,
        "not_outs": 6,
        "num_dots": 811,
        "num_doubles": 142,
        "num_fours": 204,
        "num_singles": 687,
        "num_sixes": 75,
        "num_triples": 8,
        "strike_rate": 127.71,
        "totalruns": 2005
    }
] == rv.get_json()


def test_getBatsmanStats_year(client):
    rv: FR = client.get("/getBatsmanStats_year/2020")
    assert rv.is_json
    assert rv.get_json() == [
    {
        "Innings_Played": 6080,
        "Num_dismissals": 668,
        "ballsfaced": 14510,
        "batting_average": 27.71,
        "not_outs": 759,
        "num_dots": 5084,
        "num_double": 1006,
        "num_fours": 1610,
        "num_singles": 5603,
        "num_sixes": 734,
        "num_triples": 47,
        "strike_rate": 127.55,
        "totalruns": 18508,
        "year": 2020
    }
]
    rv: FR = client.get("/getBatsmanStats_year/2020")
    assert rv.is_json
    assert rv.status_code == 200


def test_getBatsmanStats_phase(client):
    rv: FR = client.get("/getBatsmanStats_phase/1")
    assert rv.is_json
    assert rv.get_json() == [
    {
        "Innings_Played": 18118,
        "Num_dismissals": 2583,
        "ballsfaced": 67962,
        "batting_average": 5.21,
        "match_phase": 1,
        "not_outs": 759,
        "num_dots": 31847,
        "num_double": 2982,
        "num_fours": 10265,
        "num_singles": 18352,
        "num_sixes": 2235,
        "num_triples": 343,
        "strike_rate": 19.79,
        "totalruns": 13448
    }
]


def test_getPlayersForTeam(client):
    rv: FR = client.get("/getPlayersForTeam?TeamID=21")
    # Merged API.
    # assert rv.is_json
    json.loads(rv.get_data()) == [
    {
        "PlayerID": 581,
        "PlayerName": "Ryan Mclaren",
        "TeamID": 21,
        "Team": "Mumbai Indians",
        "BattingType": "RIGHT HAND BATSMAN",
        "BowlingType": "RIGHT ARM KNUCKLEBALL",
        "skill_name": "ALLROUNDER",
        "AdditionalSkill": None,
        "Is_Captain": 0
    },
    {
        "PlayerID": 604,
        "PlayerName": "Ambati Rayadu",
        "TeamID": 21,
        "Team": "Mumbai Indians",
        "BattingType": "RIGHT HAND BATSMAN",
        "BowlingType": "RIGHT ARM OFF SPIN",
        "skill_name": "WICKETKEEPER",
        "AdditionalSkill": "WICKETKEEPER",
        "Is_Captain": 0
    },
    {
        "PlayerID": 683,
        "PlayerName": "Saurabh Tiwary",
        "TeamID": 21,
        "Team": "Mumbai Indians",
        "BattingType": "LEFT HAND BATSMAN",
        "BowlingType": "RIGHT ARM OFF SPIN",
        "skill_name": "BATSMAN",
        "AdditionalSkill": None,
        "Is_Captain": 0
    },
    {
        "PlayerID": 157,
        "PlayerName": "Lasith Malinga",
        "TeamID": 21,
        "Team": "Mumbai Indians",
        "BattingType": "RIGHT HAND BATSMAN",
        "BowlingType": "RIGHT ARM KNUCKLEBALL",
        "skill_name": "BATSMAN",
        "AdditionalSkill": None,
        "Is_Captain": 0
    },
    {
        "PlayerID": 535,
        "PlayerName": "Abhishek Nayar",
        "TeamID": 21,
        "Team": "Mumbai Indians",
        "BattingType": "LEFT HAND BATSMAN",
        "BowlingType": "RIGHT ARM KNUCKLEBALL",
        "skill_name": "ALLROUNDER",
        "AdditionalSkill": None,
        "Is_Captain": 0
    },
    {
        "PlayerID": 359,
        "PlayerName": "Kieron Pollard",
        "TeamID": 21,
        "Team": "Mumbai Indians",
        "BattingType": "RIGHT HAND BATSMAN",
        "BowlingType": "RIGHT ARM KNUCKLEBALL",
        "skill_name": "ALLROUNDER",
        "AdditionalSkill": None,
        "Is_Captain": 0
    },
    {
        "PlayerID": 184,
        "PlayerName": "Chandan Madan",
        "TeamID": 21,
        "Team": "Mumbai Indians",
        "BattingType": "RIGHT HAND BATSMAN",
        "BowlingType": "LEFT ARM KNUCKLEBALL",
        "skill_name": "WICKETKEEPER",
        "AdditionalSkill": "WICKETKEEPER",
        "Is_Captain": 0
    },
    {
        "PlayerID": 699,
        "PlayerName": "Sanath Jayasuriya",
        "TeamID": 21,
        "Team": "Mumbai Indians",
        "BattingType": "LEFT HAND BATSMAN",
        "BowlingType": "LEFT ARM ORTHODOX",
        "skill_name": "ALLROUNDER",
        "AdditionalSkill": None,
        "Is_Captain": 0
    },
    {
        "PlayerID": 43,
        "PlayerName": "Stuart Binny",
        "TeamID": 21,
        "Team": "Mumbai Indians",
        "BattingType": "RIGHT HAND BATSMAN",
        "BowlingType": "RIGHT ARM KNUCKLEBALL",
        "skill_name": "ALLROUNDER",
        "AdditionalSkill": None,
        "Is_Captain": 0
    },
    {
        "PlayerID": 103,
        "PlayerName": "Ali Murtaza",
        "TeamID": 21,
        "Team": "Mumbai Indians",
        "BattingType": "LEFT HAND BATSMAN",
        "BowlingType": "LEFT ARM ORTHODOX",
        "skill_name": "ALLROUNDER",
        "AdditionalSkill": None,
        "Is_Captain": 0
    },
    {
        "PlayerID": 472,
        "PlayerName": "Dilhara Fernando",
        "TeamID": 21,
        "Team": "Mumbai Indians",
        "BattingType": "RIGHT HAND BATSMAN",
        "BowlingType": "RIGHT ARM KNUCKLEBALL",
        "skill_name": "BOWLER",
        "AdditionalSkill": None,
        "Is_Captain": 0
    },
    {
        "PlayerID": 302,
        "PlayerName": "Dwayne Bravo",
        "TeamID": 21,
        "Team": "Mumbai Indians",
        "BattingType": "RIGHT HAND BATSMAN",
        "BowlingType": "RIGHT ARM KNUCKLEBALL",
        "skill_name": "ALLROUNDER",
        "AdditionalSkill": None,
        "Is_Captain": 0
    },
    {
        "PlayerID": 483,
        "PlayerName": "Shikhar Dhawan",
        "TeamID": 21,
        "Team": "Mumbai Indians",
        "BattingType": "LEFT HAND BATSMAN",
        "BowlingType": "RIGHT ARM OFF SPIN",
        "skill_name": "BATSMAN",
        "AdditionalSkill": None,
        "Is_Captain": 0
    },
    {
        "PlayerID": 210,
        "PlayerName": "Abu Nechim",
        "TeamID": 21,
        "Team": "Mumbai Indians",
        "BattingType": "RIGHT HAND BATSMAN",
        "BowlingType": "RIGHT ARM KNUCKLEBALL",
        "skill_name": "BOWLER",
        "AdditionalSkill": None,
        "Is_Captain": 0
    },
    {
        "PlayerID": 284,
        "PlayerName": "R Satish",
        "TeamID": 21,
        "Team": "Mumbai Indians",
        "BattingType": "RIGHT HAND BATSMAN",
        "BowlingType": "RIGHT ARM KNUCKLEBALL",
        "skill_name": "ALLROUNDER",
        "AdditionalSkill": None,
        "Is_Captain": 0
    },
    {
        "PlayerID": 466,
        "PlayerName": "Sachin Tendulkar",
        "TeamID": 21,
        "Team": "Mumbai Indians",
        "BattingType": "RIGHT HAND BATSMAN",
        "BowlingType": "RIGHT ARM LEG SPIN",
        "skill_name": "BATSMAN",
        "AdditionalSkill": None,
        "Is_Captain": 1
    },
    {
        "PlayerID": 721,
        "PlayerName": "Harbhajan Singh",
        "TeamID": 21,
        "Team": "Mumbai Indians",
        "BattingType": "RIGHT HAND BATSMAN",
        "BowlingType": "RIGHT ARM OFF SPIN",
        "skill_name": "BOWLER",
        "AdditionalSkill": None,
        "Is_Captain": 0
    },
    {
        "PlayerID": 351,
        "PlayerName": "Zaheer Khan",
        "TeamID": 21,
        "Team": "Mumbai Indians",
        "BattingType": "RIGHT HAND BATSMAN",
        "BowlingType": "LEFT ARM KNUCKLEBALL",
        "skill_name": "BOWLER",
        "AdditionalSkill": None,
        "Is_Captain": 0
    },
    {
        "PlayerID": 425,
        "PlayerName": "Aditya Tare",
        "TeamID": 21,
        "Team": "Mumbai Indians",
        "BattingType": "RIGHT HAND BATSMAN",
        "BowlingType": "LEFT ARM KNUCKLEBALL",
        "skill_name": "BATSMAN",
        "AdditionalSkill": None,
        "Is_Captain": 0
    },
    {
        "PlayerID": 189,
        "PlayerName": "Jp Duminy",
        "TeamID": 21,
        "Team": "Mumbai Indians",
        "BattingType": "LEFT HAND BATSMAN",
        "BowlingType": "RIGHT ARM OFF SPIN",
        "skill_name": "ALLROUNDER",
        "AdditionalSkill": None,
        "Is_Captain": 0
    },
    {
        "PlayerID": 263,
        "PlayerName": "Dhawal Kulkarni",
        "TeamID": 21,
        "Team": "Mumbai Indians",
        "BattingType": "RIGHT HAND BATSMAN",
        "BowlingType": "RIGHT ARM KNUCKLEBALL",
        "skill_name": "BOWLER",
        "AdditionalSkill": None,
        "Is_Captain": 0
    }
]


def test_getTeamNames(client):
    rv: FR = client.get("/getTeams")
    # Merged APIs
    # assert rv.is_json
    assert json.loads(rv.get_data()) == [
    {
        "TeamID": 23,
        "Team": "Pune Warriors India"
    },
    {
        "TeamID": 16,
        "Team": "Deccan Chargers"
    },
    {
        "TeamID": 19,
        "Team": "Kochi Tuskers Kerala"
    },
    {
        "TeamID": 18,
        "Team": "Gujarat Lions"
    },
    {
        "TeamID": 15,
        "Team": "Chennai Super Kings"
    },
    {
        "TeamID": 22,
        "Team": "Pune Super Giants"
    },
    {
        "TeamID": 27,
        "Team": "Sunrisers Hyderabad"
    },
    {
        "TeamID": 20,
        "Team": "Kolkata Knight Riders"
    },
    {
        "TeamID": 26,
        "Team": "Royal Challengers Bangalore"
    },
    {
        "TeamID": 21,
        "Team": "Mumbai Indians"
    },
    {
        "TeamID": 17,
        "Team": "Delhi Capitals"
    },
    {
        "TeamID": 24,
        "Team": "Punjab Kings"
    },
    {
        "TeamID": 25,
        "Team": "Rajasthan Royals"
    }
]
    """
    [
        {"Team": "Sunrisers Hyderabad", "TeamID": 5},
        {"Team": "Rajasthan Royals", "TeamID": 1},
        {"Team": "Royal Challengers Bangalore", "TeamID": 8},
        {"Team": "Mumbai Indians", "TeamID": 2},
        {"Team": "Kolkata Knight Riders", "TeamID": 4},
        {"Team": "Chennai Super Kings", "TeamID": 7},
        {"Team": "Delhi Capitals", "TeamID": 6},
        {"Team": "Kings XI Punjab", "TeamID": 3},
    ]
    """


def test_getGround(client):
    rv: FR = client.get("/getGround")
    # Merge APIs
    # assert rv.is_json
    assert json.loads(rv.get_data()) == [
    {
        "VenueID": 1,
        "venue": "MA CHIDAMBARAM STADIUM"
    },
    {
        "VenueID": 2,
        "venue": "WANKHEDE STADIUM"
    },
    {
        "VenueID": 3,
        "venue": "M CHINNASWAMY STADIUM"
    },
    {
        "VenueID": 4,
        "venue": "EDEN GARDENS"
    },
    {
        "VenueID": 5,
        "venue": "ARUN JAITLEY STADIUM"
    },
    {
        "VenueID": 6,
        "venue": "HIMACHAL PRADESH CRICKET ASSOCIATION STADIUM"
    },
    {
        "VenueID": 7,
        "venue": "DR.DY.PATEL SPORTS ACADEMY"
    },
    {
        "VenueID": 8,
        "venue": "HOLKAR CRICKET STADIUM"
    },
    {
        "VenueID": 9,
        "venue": "SAWAI MANSINGH STADIUM"
    },
    {
        "VenueID": 10,
        "venue": "PUNJAB CRICKET ASSOCIATION STADIUM"
    },
    {
        "VenueID": 11,
        "venue": "RAJIV GANDHI INTERNATIONAL STADIUM, UPPAL"
    },
    {
        "VenueID": 12,
        "venue": "NEHRU STADIUM KOCHI"
    },
    {
        "VenueID": 13,
        "venue": "BRIAN LARA STADIUM"
    },
    {
        "VenueID": 14,
        "venue": "QUEENS PARK OVAL"
    },
    {
        "VenueID": 15,
        "venue": "ACA-VDCA CRICKET STADIUM"
    },
    {
        "VenueID": 16,
        "venue": "PROVIDENCE STADIUM"
    },
    {
        "VenueID": 17,
        "venue": "KENSINGTON OVAL"
    },
    {
        "VenueID": 18,
        "venue": "DARREN SAMMY NATIONAL CRICKET STADIUM"
    },
    {
        "VenueID": 19,
        "venue": "SABINA PARK"
    },
    {
        "VenueID": 20,
        "venue": "WARNER PARK"
    },
    {
        "VenueID": 21,
        "venue": "CHHATTISGARH INTERNATIONAL CRICKET STADIUM"
    },
    {
        "VenueID": 22,
        "venue": "GREEN PARK KANPUR"
    },
    {
        "VenueID": 23,
        "venue": "SAURASHTRA CRICKET ASSOCIATION STADIUM"
    },
    {
        "VenueID": 24,
        "venue": "MAHARASHTRA CRICKET ASSOCIATION STADIUM"
    },
    {
        "VenueID": 25,
        "venue": "BARABATI STADIUM"
    },
    {
        "VenueID": 26,
        "venue": "JSCA INTERNATIONAL STADIUM"
    },
    {
        "VenueID": 27,
        "venue": "BRABOURNE STADIUM"
    },
    {
        "VenueID": 28,
        "venue": "NARENDRA MODI STADIUM"
    },
    {
        "VenueID": 29,
        "venue": "DUBAI INTERNATIONAL CRICKET STADIUM"
    },
    {
        "VenueID": 30,
        "venue": "SHEIKH ZAYED STADIUM"
    },
    {
        "VenueID": 31,
        "venue": "SHARJAH CRICKET STADIUM"
    },
    {
        "VenueID": 32,
        "venue": "VIDARBHA CRICKET ASSOCIATION STADIUM"
    },
    {
        "VenueID": 33,
        "venue": "SYDNEY CRICKET GROUND"
    },
    {
        "VenueID": 34,
        "venue": "MELBOURNE CRICKET GROUND"
    },
    {
        "VenueID": 35,
        "venue": "ADELAIDE OVAL"
    },
    {
        "VenueID": 36,
        "venue": "BELLERIVE OVAL"
    },
    {
        "VenueID": 37,
        "venue": "DOCKLANDS STADIUM"
    },
    {
        "VenueID": 38,
        "venue": "SYDNEY SHOWGROUND STADIUM"
    },
    {
        "VenueID": 39,
        "venue": "PERTH STADIUM"
    },
    {
        "VenueID": 40,
        "venue": "BRISBANE CRICKET GROUND"
    },
    {
        "VenueID": 41,
        "venue": "AURORA STADIUM"
    },
    {
        "VenueID": 42,
        "venue": "MANUKA OVAL"
    },
    {
        "VenueID": 43,
        "venue": "SIMONDS STADIUM, SOUTH GEELONG, VICTORIA"
    },
    {
        "VenueID": 44,
        "venue": "C.EX COFFS INTERNATIONAL STADIUM"
    },
    {
        "VenueID": 45,
        "venue": "CARRARA OVAL"
    },
    {
        "VenueID": 46,
        "venue": "TED SUMMERTON  RESERVE"
    },
    {
        "VenueID": 47,
        "venue": "TRAEGER PARK"
    }
]


def test_getBatsmanStats_5(client):
    rv: FR = client.get("/getBatsmanStats_Phase_Year/Aaron Finch/1/2020/")
    assert rv.is_json
    assert rv.get_json() == [
    {
        "Innings_Played": 42,
        "Num_dismissals": 5,
        "Striker": "Aaron Finch",
        "ballsfaced": 182,
        "batting_average": 37.8,
        "match_phase": 1,
        "not_outs": 759,
        "num_dots": 87,
        "num_double": 10,
        "num_fours": 22,
        "num_singles": 46,
        "num_sixes": 6,
        "num_triple": 1,
        "strike_rate": 103.85,
        "totalruns": 189,
        "year": 2020
    }
]

    rv: FR = client.get("/getBatsmanStats_Phase_Year/Aaron Finch/2/2020/")
    assert rv.is_json
    assert rv.get_json() == [
    {
        "Innings_Played": 24,
        "Num_dismissals": 7,
        "Striker": "Aaron Finch",
        "ballsfaced": 72,
        "batting_average": 11.29,
        "match_phase": 2,
        "not_outs": 759,
        "num_dots": 28,
        "num_double": 6,
        "num_fours": 7,
        "num_singles": 27,
        "num_sixes": 2,
        "num_triple": 0,
        "strike_rate": 109.72,
        "totalruns": 79,
        "year": 2020
    }
]


def test_getBatsmanStats_4(client):
    rv: FR = client.get("/getBatsmanStats_Year/Aaron Finch/2020/")
    assert rv.is_json
    assert rv.get_json() == [
    {
        "Innings_Played": 66,
        "Num_dismissals": 12,
        "Striker": "Aaron Finch",
        "ballsfaced": 254,
        "batting_average": 22.33,
        "not_outs": 759,
        "num_dots": 115,
        "num_double": 16,
        "num_fours": 29,
        "num_singles": 73,
        "num_sixes": 8,
        "num_triple": 1,
        "strike_rate": 105.51,
        "totalruns": 268,
        "year": 2020
    }
]


def test_getBatsmanStats_1(client):
    rv: FR = client.get("/getBatsmanStats/Aaron Finch/2/1/")
    assert rv.is_json
    assert rv.get_json() == [
    {
        "InningsNo": 2,
        "Innings_Played": 130,
        "Num_dismissals": 24,
        "Striker": "Aaron Finch",
        "StrikerBattingType": "RIGHT HAND BATSMAN",
        "ballsfaced": 33852,
        "batting_average": 23.71,
        "match_phase": 1,
        "not_outs": 759,
        "num_dots": 241,
        "num_double": 25,
        "num_fours": 74,
        "num_singles": 120,
        "num_sixes": 17,
        "num_triple": 3,
        "strike_rate": 1.68,
        "totalruns": 569
    }
]

    rv: FR = client.get("/getBatsmanStats/Aaron Finch/1/1/")
    assert rv.is_json
    assert rv.get_json() == [
    {
        "InningsNo": 1,
        "Innings_Played": 145,
        "Num_dismissals": 22,
        "Striker": "Aaron Finch",
        "StrikerBattingType": "RIGHT HAND BATSMAN",
        "ballsfaced": 34110,
        "batting_average": 26.09,
        "match_phase": 1,
        "not_outs": 759,
        "num_dots": 256,
        "num_double": 27,
        "num_fours": 78,
        "num_singles": 142,
        "num_sixes": 12,
        "num_triple": 2,
        "strike_rate": 1.68,
        "totalruns": 574
    }
]


def test_getBatsmanStats(client):
    rv: FR = client.get("/getBatsmanStats/Aaron Finch/1/1/LEG SPIN")
    assert rv.is_json
    assert rv.get_json() == [
    {
        "BowlType": "LEG SPIN",
        "InningsNo": 1,
        "Innings_Played": 145,
        "Num_dismissals": 22,
        "Striker": "Aaron Finch",
        "StrikerBattingType": "RIGHT HAND BATSMAN",
        "ballsfaced": 823,
        "batting_average": 1.0,
        "match_phase": 1,
        "not_outs": 759,
        "num_dots": 2,
        "num_double": 0,
        "num_fours": 5,
        "num_singles": 2,
        "num_sixes": 0,
        "num_triple": 0,
        "strike_rate": 2.67,
        "totalruns": 22
    }
]


def test_getBatsmanStats_3(client):
    rv: FR = client.get("/getBatsmanStats_BowlType_Year/Aaron Finch/LEG SPIN/2020/")
    assert rv.is_json
    assert rv.get_json() == [
    {
        "BowlType": "LEG SPIN",
        "Innings_Played": 66,
        "Num_dismissals": 12,
        "Striker": "Aaron Finch",
        "ballsfaced": 26,
        "batting_average": 4.08,
        "not_outs": 759,
        "num_dots": 10,
        "num_double": 1,
        "num_fours": 7,
        "num_singles": 6,
        "num_sixes": 2,
        "num_triple": 0,
        "strike_rate": 188.46,
        "totalruns": 49,
        "year": 2020
    }
]


def test_getBatsmanStats_2(client):
    rv: FR = client.get("/getBatsmanStats/Aaron Finch/LEG SPIN")
    assert rv.is_json
    assert rv.get_json() == [
    {
        "BowlType": "LEG SPIN",
        "Innings_Played": 549,
        "Num_dismissals": 97,
        "Striker": "Aaron Finch",
        "StrikerBattingType": "RIGHT HAND BATSMAN",
        "ballsfaced": 136,
        "batting_average": 2.06,
        "not_outs": 759,
        "num_dots": 52,
        "num_double": 6,
        "num_fours": 19,
        "num_singles": 49,
        "num_sixes": 10,
        "num_triple": 0,
        "strike_rate": 147.06,
        "totalruns": 200
    }
]


def test_getBowlerStats_player(client):
    rv: FR = client.get("/getBowlerStats_player/Krunal Pandya")
    assert rv.is_json
    assert rv.get_json() == [
    {
        "BowlType": "LEFT ARM ORTHODOX",
        "Bowler": "Krunal Pandya",
        "Innings_Played": 162,
        "bowling_average": 269.31,
        "bowling_economy": 51.75,
        "bowling_strike_rate": 216.53,
        "num_dots_conceded": 479,
        "num_double_conceded": 103,
        "num_extras_conceded": 106,
        "num_fours_conceded": 122,
        "num_singles_conceded": 730,
        "num_sixes_conceded": 56,
        "num_triple_conceded": 2,
        "total_balls_bowled": 2888,
        "total_runs_conceded": 3546,
        "total_wickets_taken": 102
    }
]


def test_getBowlerStats_year(client):
    rv: FR = client.get("/getBowlerStats_year/2020")
    assert rv.is_json
    assert rv.get_json() == [
    {
        "Innings_Played": 0,
        "bowling_average": 30.51,
        "bowling_economy": 8.17,
        "bowling_strike_rate": 22.41,
        "num_dots_conceded": 5084,
        "num_double_conceded": 1006,
        "num_extras_conceded": 698,
        "num_fours_conceded": 1610,
        "num_singles_conceded": 5603,
        "num_sixes_conceded": 734,
        "num_triple_conceded": 47,
        "total_balls_bowled": 14007,
        "total_runs_conceded": 19070,
        "total_wickets_taken": 625,
        "year": 2020
    }
]


def test_getBowlerStats_phase(client):
    rv: FR = client.get("/getBowlerStats_phase/2")
    assert rv.is_json
    assert rv.get_json() == [
    {
        "Innings_Played": 0,
        "bowling_average": 31.48,
        "bowling_economy": 7.43,
        "bowling_strike_rate": 25.43,
        "match_phase": 2,
        "num_dots_conceded": 32708,
        "num_double_conceded": 6787,
        "num_extras_conceded": 4198,
        "num_fours_conceded": 8676,
        "num_singles_conceded": 44072,
        "num_sixes_conceded": 4141,
        "num_triple_conceded": 272,
        "total_balls_bowled": 98944,
        "total_runs_conceded": 122494,
        "total_wickets_taken": 3891
    }
]


def test_getBowlerStats_4(client):
    rv: FR = client.get("/getBowlerStats/Krunal Pandya/1/2020/")
    assert rv.is_json
    assert rv.get_json() == [
    {
        "Bowler": "Krunal Pandya",
        "Innings_Played": 0,
        "bowling_average": 34.5,
        "bowling_economy": 5.91,
        "bowling_strike_rate": 35.0,
        "match_phase": 1,
        "num_dots_conceded": 35,
        "num_double_conceded": 3,
        "num_extras_conceded": 5,
        "num_fours_conceded": 6,
        "num_singles_conceded": 20,
        "num_sixes_conceded": 2,
        "num_triple_conceded": 0,
        "total_balls_bowled": 70,
        "total_runs_conceded": 69,
        "total_wickets_taken": 2,
        "year": 2020
    }
]


def test_getBowlerStats_2(client):
    rv: FR = client.get("/getBowlerStats/Krunal Pandya/1/")
    assert rv.is_json
    assert rv.get_json() == [
    {
        "BowlType": "LEFT ARM ORTHODOX",
        "Bowler": "Krunal Pandya",
        "Innings_Played": 0,
        "bowling_average": 31.33,
        "bowling_economy": 6.94,
        "bowling_strike_rate": 27.08,
        "match_phase": 1,
        "num_dots_conceded": 145,
        "num_double_conceded": 10,
        "num_extras_conceded": 10,
        "num_fours_conceded": 37,
        "num_singles_conceded": 113,
        "num_sixes_conceded": 13,
        "num_triple_conceded": 1,
        "total_balls_bowled": 325,
        "total_runs_conceded": 376,
        "total_wickets_taken": 12
    }
]


def test_getBowlerStats_1(client):
    rv: FR = client.get("/getBowlerStats_1/Krunal Pandya/1/1/")
    assert rv.is_json
    assert rv.get_json() == [
    {
        "BowlType": "LEFT ARM ORTHODOX",
        "Bowler": "Krunal Pandya",
        "Innings_Played": 0,
        "bowling_average": 22.0,
        "bowling_economy": 6.09,
        "bowling_strike_rate": 21.67,
        "innings": 1,
        "match_phase": 1,
        "num_dots_conceded": 60,
        "num_double_conceded": 4,
        "num_extras_conceded": 6,
        "num_fours_conceded": 11,
        "num_singles_conceded": 48,
        "num_sixes_conceded": 4,
        "num_triple_conceded": 0,
        "total_balls_bowled": 130,
        "total_runs_conceded": 132,
        "total_wickets_taken": 6
    }
]


def test_getBowlerStats(client):
    rv: FR = client.get("/getBowlerStats/Krunal Pandya/1/1/Right Hand Batsman")
    assert rv.is_json
    assert rv.get_json() == [
    {
        "BowlType": "LEFT ARM ORTHODOX",
        "Bowler": "Krunal Pandya",
        "InningsNo": 1,
        "Innings_Played": 0,
        "StrikerBattingType": "Right Hand Batsman",
        "bowling_average": 16.72,
        "bowling_economy": 5.83,
        "bowling_strike_rate": 17.21,
        "match_phase": 1,
        "num_dots_conceded": 486,
        "num_double_conceded": 28,
        "num_extras_conceded": 50,
        "num_fours_conceded": 91,
        "num_singles_conceded": 328,
        "num_sixes_conceded": 22,
        "num_triple_conceded": 0,
        "total_balls_bowled": 981,
        "total_runs_conceded": 953,
        "total_wickets_taken": 57
    }
]

