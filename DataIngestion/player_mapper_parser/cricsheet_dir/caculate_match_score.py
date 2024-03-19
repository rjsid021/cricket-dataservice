import json


def read_json(path):
    f = open(path)
    return json.load(f)


def calculate():
    json = read_json(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService/data/cricsheet/ODI/ODI 2023/1377013.json"
    )
    innings = json['innings']
    innings_1 = innings[0]
    innings_2 = innings[1]

    innings_1_overs = innings_1['overs']
    innings_2_overs = innings_2['overs']

    innings_1_runs = 0
    innings_1_extras = 0
    wicket_counter_1 = 0
    wicket_counter_2 = 0
    inning1_wicket = []
    inning2_wicket = []
    inning1_no_balls_counter = 0
    inning2_no_balls_counter = 0
    inning1_legbyes_counter = 0
    inning2_legbyes_counter = 0
    inning2___byes_counter = 0
    inning1___byes_counter = 0
    player_dict_runs = {}
    player_dict_ball_counter = {}
    from collections import defaultdict
    baller_dots_inning_1 = defaultdict(int)
    baller_dots_inning_2 = defaultdict(int)
    batter_dict = defaultdict(int)
    balls = 0
    for over in innings_1_overs:
        deliveries = over['deliveries']
        for delivery in deliveries:
            balls += 1
            if delivery.get('runs'):
                innings_1_runs += delivery['runs']['total']
                innings_1_extras += delivery['runs']['extras']
                batter_dict[delivery['batter']] = batter_dict[delivery['batter']] + delivery['runs']['batter']
            if delivery.get("wickets"):
                inning1_wicket.append(delivery.get("batter"))
                wicket_counter_1 += 1
            if delivery.get("runs").get("total") == 0:
                baller_dots_inning_1[delivery.get("bowler")] = baller_dots_inning_1[delivery.get("bowler")] + 1
            if delivery.get('extras'):
                if delivery.get('extras').get('noballs'):
                    inning1_no_balls_counter += 1
                if delivery.get('extras').get('legbyes'):
                    inning1_legbyes_counter += 1
                if delivery.get('extras').get('byes'):
                    inning1___byes_counter += 1
            if delivery.get('runs'):
                if player_dict_runs.get((delivery.get('batter'), delivery.get('non_striker'))):
                    player_dict_runs[(delivery.get('batter'), delivery.get('non_striker'))] += delivery.get('runs')[
                        "total"]
                    player_dict_ball_counter[(delivery.get('batter'), delivery.get('non_striker'))] += 1
                else:
                    player_dict_runs[(delivery.get('batter'), delivery.get('non_striker'))] = delivery.get('runs')[
                        "total"]
                    player_dict_ball_counter[(delivery.get('batter'), delivery.get('non_striker'))] = 1

    innings_2_runs = 0
    innings_2_extras = 0

    for over in innings_2_overs:
        deliveries = over['deliveries']
        for delivery in deliveries:
            balls += 1
            if delivery.get('runs'):
                innings_2_runs += delivery['runs']['total']
                innings_2_extras += delivery['runs']['extras']
                batter_dict[delivery['batter']] = batter_dict[delivery['batter']] + delivery['runs']['batter']
            if delivery.get("wickets"):
                inning2_wicket.append(delivery.get("batter"))
                wicket_counter_2 += 1
            if delivery.get("runs").get("total") == 0:
                baller_dots_inning_2[delivery.get("bowler")] = baller_dots_inning_2[delivery.get("bowler")] + 1
            if delivery.get('extras'):
                if delivery.get('extras').get('noballs'):
                    inning2_no_balls_counter += 1
                if delivery.get('extras').get('legbyes'):
                    inning2_legbyes_counter += 1
                if delivery.get('extras').get('byes'):
                    inning2___byes_counter += 1
            if delivery.get('runs'):
                if player_dict_runs.get((delivery.get('batter'), delivery.get('non_striker'))):
                    player_dict_runs[(delivery.get('batter'), delivery.get('non_striker'))] += delivery.get('runs')[
                        "total"]
                    player_dict_ball_counter[(delivery.get('batter'), delivery.get('non_striker'))] += 1
                else:
                    player_dict_runs[(delivery.get('batter'), delivery.get('non_striker'))] = delivery.get('runs')[
                        "total"]
                    player_dict_ball_counter[(delivery.get('batter'), delivery.get('non_striker'))] = 1
    print('innings_1_runs', innings_1_runs)
    print('innings_1_extras', innings_1_extras)
    print('inning1_no_balls_counter', inning1_no_balls_counter)
    print('inning2_no_balls_counter', inning2_no_balls_counter)
    print('inning1_leg_byes_counter', inning1_legbyes_counter)
    print('inning2_leg_byes_counter', inning2_legbyes_counter)
    print('inning1___byes_counter', inning1___byes_counter)
    print('inning2___byes_counter', inning2___byes_counter)
    print('innings_1_overs', len(innings_1_overs))
    print("innings_1 wicket: ", wicket_counter_1)
    print('innings_2_runs', innings_2_runs)
    print('innings_2_extras', innings_2_extras)
    print('innings_2_overs', len(innings_2_overs))
    print("won by - ", abs(innings_1_runs - innings_2_runs))
    print("innings_2 wicket: ", wicket_counter_2)
    print("inning 1 wicket", inning1_wicket)
    print("")
    print("")
    print("")
    print("inning 2 wicket", inning2_wicket)
    print("")
    print("")
    print("")
    print("baller_dots_inning_1", dict(baller_dots_inning_1))
    print("")
    print("")
    print("baller_dots_inning_2", dict(baller_dots_inning_2))
    print("")
    print("")
    print("")
    print(player_dict_runs)
    print("")
    print("")
    print("")
    print(player_dict_ball_counter)
    print("")
    print("")
    print("")
    print("balls -> ", balls)


if __name__ == '__main__':
    calculate()

scrum_update = "I ❤ cricsheet and nvplay"
