import os

from common.utils.helper import get_project_root, getEnvVariables

# e.g. of FILE_SHARE_PATH - /azurefs/Dev from Azure cloud for deployed app on cloud
FILE_SHARE_PATH = getEnvVariables("FILE_SHARE_PATH")
# Check if it is local env, if yes point to local file dirs
if getEnvVariables("FILE_SHARE_PATH") == "local":
    FILE_SHARE_PATH = get_project_root()

ROOT_DATA_PATH = os.path.join(FILE_SHARE_PATH, "data/Data_Feeds/")
PLAYER_INCREMENTAL_FILE_PATH = os.path.join(FILE_SHARE_PATH, "data/player_increment_sftp.xlsx")
DUCK_DB_PATH = os.path.join(FILE_SHARE_PATH, "data/duckdb/file.db")
DUCK_DB_MAIN_INGESTION_PATH = os.path.join(FILE_SHARE_PATH, "data/duckdb/ingestion.db")
DUCK_DB_CONFIG_PATH = os.path.join(FILE_SHARE_PATH, "data/duckdb/duck_tables_cache.json")
INGESTION_ENABLED = getEnvVariables("INGESTION_ENABLED") in ["True" or "true"]
SFTP_INGESTION_ENABLED = getEnvVariables("SFTP_INGESTION_ENABLED") in ["True" or "true"]
SFTP_CONFIG_PATH = os.path.join(FILE_SHARE_PATH, "data/sftp_config/config.yaml")
SFTP_INGESTION_CACHE = os.path.join(FILE_SHARE_PATH, "data/sftp_config/sftp_ingestion_cache.json")
INGESTION_CONFIG = os.path.join(FILE_SHARE_PATH, "data/ingestion_config/nvplay.json")
NVPLAY_INGESTION_CONFIG = os.path.join(FILE_SHARE_PATH, "data/ingestion_config/ingestion_mail_recipients.json")
IPL_2022_PATH = os.path.join(ROOT_DATA_PATH, 'IPL/IPL 2022/')
SQUAD_ROOT_PATH = os.path.join(FILE_SHARE_PATH, "data/Squad_Feeds/")
PITCH_TYPE_DATA_PATH = os.path.join(FILE_SHARE_PATH, "data/IPL-NATURE_OF_WICKETS.xlsx")
TEAM_COLOR_PATH = os.path.join(FILE_SHARE_PATH, "data/teams_color.csv")
TEAM_MAPPING_PATH = os.path.join(FILE_SHARE_PATH, "data/team_mapping.csv")
DAILY_ACTIVITY_EXCEL_DATA_PATH = os.path.join(FILE_SHARE_PATH, "data/Daily_Activity.xlsx")
CRICSHEET_DATA_PATH = os.path.join(FILE_SHARE_PATH, "data/cricsheet/")
NVPLAY_PATH = os.path.join(FILE_SHARE_PATH, "data/nv_play/")
NVPLAY_PATH_SQUAD = os.path.join(FILE_SHARE_PATH, "data/nv_play_squad/")
PITCH_TYPE_2019_TO_2021_DATA_PATH = os.path.join(FILE_SHARE_PATH, "data/2019_2020_2021_matches-wicket-type.csv")
CONTRIBUTION_CONSTRAINTS_DATA_PATH = os.path.join(FILE_SHARE_PATH, "data/Contribution_Constraints.csv")
MISSING_SQUAD_2022 = os.path.join(FILE_SHARE_PATH, 'data/Squad_missing.xlsx')
MISSING_PLAYERS_2022 = os.path.join(FILE_SHARE_PATH, 'data/missing_players-2022.xlsx')
FITNESS_DATA = os.path.join(FILE_SHARE_PATH, 'data/fitness_data.csv')
BOWLING_PLANNING_TEMPLATE = os.path.join(FILE_SHARE_PATH, 'data/bowling-planning-template.xlsx')
GPS_BOWLING_2021_TRENT_BOULT = os.path.join(FILE_SHARE_PATH, 'data/MI_GPS_2021_Trent_Boult.xlsx')
CONTRIBUTION_SCORE_TABLE_NAME = 'contributionScore'
PI_MAX_ID = os.path.join(FILE_SHARE_PATH, 'data/pi_max_id.csv')
PLAYER_MAPPING_BLOB_PATH = "player_mapping.csv"
PLAYER_MAPPING_LOCAL_PATH = "player_mapping.csv"

IMAGE_STORE_URL = getEnvVariables("IMAGE_STORE_URL")
PLAYER_LIST_2023_FILE = os.path.join(FILE_SHARE_PATH, "data/player_list_2023.csv")

FIELDING_ANALYSIS_TEMP = os.path.join(FILE_SHARE_PATH, "data/fielding-analysis-template.xlsx")
FIELD_ANALYSIS_FILE = os.path.join(FILE_SHARE_PATH, "data/fielding-analysis-file.xlsx")
PI_CONFIG_DATA_PATH = os.path.join(FILE_SHARE_PATH, "data/pressure_index_config/")
PI_RESOURCE_UTILIZATION_FILE = os.path.join(FILE_SHARE_PATH, "data/pressure_index_config/resource_utilization.csv")
PI_FEATURES_NAME_FILE = os.path.join(FILE_SHARE_PATH, "data/pressure_index_config/feature_names.json")

# FILE METADATA
FILES_KEY_COL = "file_id"
FILES_TABLE = "fileLogs"

# Players configs
SQUAD_KEY_LIST = ['squadA', 'squadB']
PLAYERS_TABLE_NAME = 'players'
PLAYERS_KEY_COL = 'player_id'
PLAYERS_REQD_COLS = {"TeamID", "PlayerName", "PlayerID", "BattingType", "BowlingProficiency", "PlayerSkill",
                     "IsCaptain",
                     "season", "competition_name", "TeamName"}

# Teams configs
TEAMS_TABLE_NAME = 'Teams'
TEAMS_KEY_COL = 'team_id'
TEAMS_KEYS_FROM_SQUAD = {'TeamID', 'TeamName', 'TeamCode'}

# Venue Configs
VENUE_TABLE_NAME = 'Venue'
VENUE_KEY_COL = 'venue_id'

# Matches Configs
MATCHES_KEY_COL = 'match_id'
MATCHES_TABLE_NAME = 'Matches'
MATCHES_REQD_COLS = ["MatchID", "MatchDate", "MatchTime", "FirstBattingTeamID", "MatchDateOrder", "MatchName", \
                     "FirstBattingTeamName", "SecondBattingTeamID", "SecondBattingTeamName", \
                     "GroundName", "Comments", "TossTeam", "TossDetails", "FirstBattingSummary", \
                     "SecondBattingSummary", "competition_name", "seasons"]

# Innings configs

INNINGS_REQD_COLS = 'BallID', 'MatchID', 'InningsNo', 'StrikerID', 'BowlerID', 'OutBatsManID', \
    'OverNo', 'BallNo', 'Runs', 'IsOne', 'IsTwo', 'IsThree', 'IsDotball', 'Extras', 'IsExtra', \
    'IsWide', 'IsNoBall', 'IsBye', 'IsLegBye', 'IsFour', 'IsSix', 'IsWicket', 'WicketType', \
    'IsBowlerWicket', 'Xpitch', 'Ypitch', 'IsMaiden', 'Line', 'BowlType', 'Length', \
    'ShotType', 'CommentOver', 'BallRuns', 'competition_name', 'season', 'NonStrikerID', 'IsBeaten', \
    'IsUncomfortable', 'BowlingDirection', 'VideoFile'

WAGONWHEEL_REQD_COLS = 'BallID', 'FielderAngle', 'FielderLengthRatio', 'FielderLengthRation'
BATTING_REQD_COLS = 'PlayerID', 'PlayingOrder', 'TeamID'
BOWLING_REQD_COLS = 'PlayerID', 'TeamID'
INNINGS_KEY_COL = 'id'
INNINGS_TABLE_NAME = 'MatchBallSummary'

# Extras required columns

EXTRAS_REQD_COLS = 'MatchID', 'InningsNo', 'TeamID', 'TotalExtras', 'Byes', 'LegByes', 'NoBalls', 'Wides'
EXTRAS_KEY_COL = 'id'
EXTRAS_TABLE_NAME = "MatchExtras"
# Match Stats Configs

MATCH_STATS_TABLE_NAME = 'BatsmanBowlerMatchStatistics'
MATCH_STATS_KEY_COL = 'id'

# Batting Card configs
BAT_CARD_TABLE_NAME = 'MatchBattingCard'
BAT_CARD_REQD_COLS = 'MatchID', 'InningsNo', 'TeamID', 'PlayerName', 'PlayingOrder', 'OutDesc', 'Runs', \
    'season', 'Balls', 'DotBalls', 'Ones', 'Twos', 'Threes', 'Fours', 'Sixes', 'StrikeRate', 'PlayerID', 'competition_name',

BAT_CARD_KEY_COL = 'id'

# Bowling Card configs
BOWL_CARD_TABLE_NAME = 'MatchBowlingCard'
BOWL_CARD_REQD_COLS = 'MatchID', 'InningsNo', 'TeamID', 'PlayerName', 'Overs', 'Maidens', 'Runs', 'Wickets', 'Wides', \
    'NoBalls', 'Economy', 'BowlingOrder', 'DotBalls', 'Ones', 'Twos', 'Threes', 'Fours', 'Sixes', 'StrikeRate', \
    'TotalLegalBallsBowled', 'PlayerID', 'competition_name', 'season'
BOWL_CARD_KEY_COL = 'id'

# Partnership configs
PARTNERSHIP_TABLE_NAME = 'MatchPartnership'
PARTNERSHIP_REQD_COLS = 'MatchID', 'BattingTeamID', 'InningsNo', 'Striker', 'PartnershipTotal', 'StrikerRuns', \
    'StrikerBalls', 'Extras', 'NonStrikerRuns', 'NonStrikerBalls', 'NonStriker', 'StrikerID', 'NonStrikerID'
PARTNERSHIP_KEY_COL = 'id'

# Batsman global stats configs
BATSMAN_GLOBAL_STATS_TABLE = 'BatsmanGlobalStats'
BATSMAN_GLOBAL_STATS_COL = 'id'

# Bowler global stats config
BOWLER_GLOBAL_STATS_TABLE = 'BowlerGlobalStats'
BOWLER_GLOBAL_STATS_COL = 'id'

# Pressure Index config
PRESSURE_INDEX_TABLE_NAME = 'PressureIndex'
PRESSURE_INDEX_KEY_COL = 'id'

retained_list = ['SIMRAN SINGH', 'ABHINAV MANOHAR', 'AMAN KHAN', 'ANRICH NORTJE', 'AXAR PATEL', 'CHETAN SAKARIYA',
                 'DAVID WARNER', 'KAMALESH NAGARKOTI', 'KULDEEP YADAV', 'LALIT YADAV', 'LUNGI NGIDI', 'MITCHELL MARSH',
                 'MUSTAFIZUR RAHMAN', 'PRAVEEN DUBEY', 'PRITHVI SHAW', 'RIPAL PATEL', 'RISHABH PANT', 'ROVMAN POWELL',
                 'SARFARAZ KHAN', 'KHALEEL AHMED', 'ABDUL SAMAD', 'ABHISHEK SHARMA', 'AIDEN MARKRAM',
                 'BHUVNESHWAR KUMAR',
                 'FAZALHAQ FAROOQI', 'GLENN PHILLIPS', 'KARTIK TYAGI', 'MARCO JANSEN', 'RAHUL TRIPATHI', 'T NATARAJAN',
                 'UMRAN MALIK', 'WASHINGTON SUNDAR', 'DEWALD BREVIS', 'HRITHIK SHOKEEN', 'ISHAN KISHAN',
                 'JASON BEHRENDROFF',
                 'JASPRIT BUMRAH', 'JOFRA ARCHER', 'KUMAR KARTIKEYA SINGH', 'THAKUR TILAK VARMA', 'RAMANDEEP SINGH',
                 'ROHIT SHARMA', 'SURYAKUMAR YADAV', 'TIM DAVID', 'TRISTAN STUBBS', 'AVESH KHAN', 'AYUSH BADONI',
                 'DEEPAK HOODA', 'KRISHNAPPA GOWTHAM', 'KARAN SHARMA', 'KL RAHUL', 'KRUNAL PANDYA', 'MANAN VOHRA',
                 'MARCUS STOINIS', 'MARK WOOD', 'MOHSIN KHAN', 'QUINTON DE KOCK', 'RAVI BISHNOI', 'ANDRE RUSSELL',
                 'ANUKUL ROY', 'HARSHIT RANA', 'LOCKIE FERGUSON', 'NITISH RANA', 'RINKU SINGH', 'SHARDUL THAKUR',
                 'SHREYAS IYER', 'SUNIL NARINE', 'TIM SOUTHEE', 'UMESH YADAV', 'VARUN CHAKRAVATHI V', 'VENKATESH IYER',
                 'ALZARRI JOSEPH', 'SAI SUDHARSAN B', 'DARSHAN NALKANDE', 'DAVID MILLER', 'HARDIK PANDYA',
                 'JAYANT YADAV',
                 'MATTHEW WADE', 'MOHAMMED SHAMI', 'PRADEEP SANGWAN', 'SAI KISHORE R', 'RAHUL TEWATIA', 'RASHID KHAN',
                 'SHUBMAN GILL', 'VIJAY SHANKAR', 'WRIDDHIMAN SAHA', 'YASH DAYAL', 'SHIKHAR DHAWAN', 'SHAHRUKH KHAN M',
                 'RAHUL CHAHAR', 'ARSHDEEP SINGH', 'HARPREET BRAR', 'RAJANGAD BAWA', 'RISHI DHAWAN', 'JITESH SHARMA',
                 'LIAM LIVINGSTONE', 'KAGISO RABADA', 'JONNY BAIRSTOW', 'NATHAN ELLIS', 'BHANUKA RAJAPAKSA',
                 'AKASH DEEP',
                 'ANUJ RAWAT', 'DAVID WILLEY', 'DINESH KARTHIK', 'FAF DU PLESSIS', 'GLENN MAXWELL', 'HARSHAL PATEL',
                 'JOSH HAZLEWOOD', 'KARN SHARMA', 'MAHIPAL LOMROR', 'MOHAMMED SIRAJ', 'RAJAT PATIDAR', 'SHAHBAZ AHMED',
                 'SIDDARTH KAUL', 'SUYASH PRABHUDESSAI', 'VIRAT KOHLI', 'WANINDU HASARANGA', 'DEVDUTT PADIKKAL',
                 'JOS BUTTLER', 'KC CARIAPPA', 'KULDEEP SEN', 'KULDIP YADAV', 'NAVDEEP SAINI', 'OBED MCCOY',
                 'PRASIDH KRISHNA', 'R ASHWIN', 'RIYAN PARAG', 'SANJU SAMSON', 'SHIMRON HETMYER', 'TRENT BOULT',
                 'YASHASVI JAISWAL', 'YUZVENDRA CHAHAL', 'AMBATI RAYADU', 'DEEPAK CHAHAR', 'DEVON CONWAY',
                 'DWAINE PRETORIUS',
                 'MAHEESH THEEKSHANA', 'MATHEESHA PATHIRANA', 'MITCHELL SANTNER', 'MOEEN ALI', 'MS DHONI',
                 'MUKESH CHOUDHARY',
                 'PRASHANT SOLANKI', 'RAVINDRA JADEJA', 'RUTURAJ GAIKWAD', 'SHIVAM DUBE', 'SIMARJEET SINGH',
                 'TUSHAR DESHPANDE']

in_auction = ['INDRAJITH B', 'JAMES NEESHAM', 'K S BHARAT', 'ASIF KM', 'MUJEEB UR RAHMAN', 'JAGADEESAN N',
              'NATHAN COULTER NILE',
              'SUDHESAN MIDHUN', 'ANKIT RAJPOOT', 'DUSHMANTHA CHAMEERA', 'GURKEERAT MANN SINGH', 'HARPREET SINGH',
              'JOSH PHILIPPE',
              'LUKMAN MERIWALA', 'PRASANT CHOPRA', 'PRAYAS RAY BARMAN', 'PRITHVI RAJ', 'RASIKH SALAM', 'MAYANK AGARWAL',
              'AJINKYA RAHANE',
              'RILEE ROSSOUW', 'KANE WILLIAMSON', 'SAM CURRAN', 'SHAKIB AL HASAN', 'JASON HOLDER', 'ODEAN SMITH',
              'BEN STOKES', 'TOM BANTON',
              'HEINRICH KLAASEN', 'NICHOLAS POORAN', 'CHRIS JORDAN', 'ADAM MILNE', 'JHYE RICHARDSON', 'ISHANT SHARMA',
              'JAYDEV UNADKAT',
              'MAYANK MARKANDE', 'ADIL RASHID', 'TABRAIZ SHAMSI', 'ADAM ZAMPA', 'ANMOLPREET SINGH', 'PRIYAM GARG',
              'SHASHANK SINGH',
              'SHIVAM MAVI', 'MURUGAN ASHWIN', 'SHREYAS GOPAL', 'TRAVIS HEAD', 'DAWID MALAN', 'MANISH PANDEY',
              'SHERFANE RUTHERFORD',
              'MANDEEP SINGH', 'RASSIE VAN DER DUSSEN', 'DARYL MITCHELL', 'MOHAMMAD NABI', 'WAYNE PARNELL',
              'DANIEL SAMS', 'ROMARIO SHEPHERD',
              'KYLE JAMIESON', 'RILEY MEREDITH', 'SANDEEP SHARMA', 'PIYUSH CHAWLA', 'AMIT MISHRA', 'SHAHBAZ NADEEM',
              'ISH SODHI',
              'SACHIN BABY', 'VIRAT SINGH', 'PRERAK MANKAD', 'JAGADEESHA SUCHITH', 'RICKY BHUI', 'SHELDON JACKSON',
              'VISHNU VINOD',
              'ANIKET CHOUDHARY', 'ISHAN POREL', 'AKASH SINGH', 'BASIL THAMPI', 'TEJAS BAROKA', 'SHIVAM SHARMA',
              'CHRIS LYNN',
              'KARUN NAIR', 'JASON ROY', 'SEAN ABBOTT', 'GEORGE GARTON', 'SANDEEP WARRIER', 'MOHIT SHARMA',
              'BILLY STANLAKE', 'ANDREW TYE',
              'SWAPNIL SINGH', 'DAVID WIESE', 'SANJAY YADAV', 'ABHIJEET TOMAR', 'TOM CURRAN', 'MOISES HENRIQUES',
              'SCOTT KUGGELEIJN',
              'DARCY SHORT', 'VARUN AARON', 'MATT HENRY', 'DHAWAL KULKARNI', 'TYMAL MILLS', 'BARINDER SRAN',
              'AKSHDEEP NATH',
              'FABIAN ALLEN', 'CARLOS BRATHWAITE', 'PAWAN NEGI', 'KEEMO PAUL', 'KULWANT KHEJROLIYA']

# GPS data config
GPS_TABLE_NAME = 'fitnessGPSData'
BASE_URL = getEnvVariables("BASE_URL")
TOKEN = getEnvVariables("TOKEN")
EMIRATES_BASE_URL = getEnvVariables("EMIRATES_BASE_URL")
EMIRATES_TOKEN = getEnvVariables("EMIRATES_TOKEN")
WPL_TOKEN = getEnvVariables("WPL_TOKEN")
STATS_API_NAME = "stats"
GPS_AGG_DATA_GROUP_LIST = ["athlete", "date", "team_name", "activity_name", "period_name", "athlete_id"]
GPS_AGG_DATA_JOIN_LIST = ["athlete_id", "date_name", "team_name", "activity_name", "period_name"]

GPS_AGG_DECIMAL_COL_LIST = ['max_velocity_kmh', 'total_accel_load', 'total_distance_m', 'total_player_load',
                            'vel_b1_tot_dist_m', 'vel_b2_tot_dist_m', 'vel_b3_tot_dist_m', 'vel_b5_avg_distance_m',
                            'vel_b4_tot_dist_m', 'vel_b3_max_eff_duration', 'vel_b5_tot_dist_m',
                            'vel_b4_avg_distance_m',
                            'vel_b3_max_eff_dist_m', 'vel_b4_max_eff_dist_m', 'vel_b5_max_eff_dist_m']

GPS_AGG_INT_COL_LIST = ['accel_b3_eff_gen2', 'decel_b3_eff_gen2', 'total_dives', 'total_jumps',
                        'vel_b3_total_eff_count', 'vel_b4_total_eff_count', 'vel_b5_total_eff_count',
                        'accel_b1_eff_gen2', 'accel_b13_total_eff_gen2', 'accel_b2_eff_gen2', 'decel_b1_eff_gen2',
                        'decel_b13_total_eff_gen2', 'decel_b2_eff_gen2']

GPS_SRC_KEY_MAPPING = {"date_name": "date_name",
                       "period_name": "period_name",
                       "activity_name": "activity_name",
                       "team_name": "team_name",
                       "athlete_name": "athlete_name",
                       "athlete_id": "athlete_id",
                       "max_vel": "max_velocity_kmh",
                       "total_acceleration_load": "total_accel_load",
                       "total_distance": "total_distance_m",
                       "total_player_load": "total_player_load",
                       "velocity_band1_total_distance": "vel_b1_tot_dist_m",
                       "velocity_band2_total_distance": "vel_b2_tot_dist_m",
                       "velocity_band3_total_distance": "vel_b3_tot_dist_m",
                       "velocity_band4_total_distance": "vel_b4_tot_dist_m",
                       "velocity_band5_total_distance": "vel_b5_tot_dist_m",
                       "gen2_acceleration_band8_total_effort_count": "accel_b3_eff_gen2",
                       "gen2_acceleration_band1_total_effort_count": "decel_b3_eff_gen2",
                       "total_dives": "total_dives", "total_jumps": "total_jumps",
                       "velocity_band3_max_effort_distance": "vel_b3_max_eff_dist_m",
                       "velocity_band3_max_effort_duration": "vel_b3_max_eff_duration",
                       "velocity_band3_total_effort_count": "vel_b3_total_eff_count",
                       "velocity_band4_average_distance": "vel_b4_avg_distance_m",
                       "velocity_band4_max_effort_distance": "vel_b4_max_eff_dist_m",
                       "velocity_band4_total_effort_count": "vel_b4_total_eff_count",
                       "velocity_band5_average_distance": "vel_b5_avg_distance_m",
                       "velocity_band5_max_effort_distance": "vel_b5_max_eff_dist_m",
                       "velocity_band5_total_effort_count": "vel_b5_total_eff_count",
                       "gen2_acceleration_band6_total_effort_count": "accel_b1_eff_gen2",
                       "gen2_acceleration_band6plus_total_effort_count": "accel_b13_total_eff_gen2",
                       "gen2_acceleration_band7_total_effort_count": "accel_b2_eff_gen2",
                       "gen2_acceleration_band3_total_effort_count": "decel_b1_eff_gen2",
                       "gen2_acceleration_band3plus_total_effort_count": "decel_b13_total_eff_gen2",
                       "gen2_acceleration_band2_total_effort_count": "decel_b2_eff_gen2"}

GPS_COLUMN_MAPPING = {'Name': 'player_name',
                      'Maximum Velocity (km/h)': 'max_velocity_kmh',
                      'Total Acceleration Load': 'total_accel_load',
                      'Total Distance (m)': 'total_distance_m',
                      'Total Player Load': 'total_player_load',
                      'Velocity Band 1 Total Distance (m)': 'vel_b1_tot_dist_m',
                      'Velocity Band 2 Total Distance (m)': 'vel_b2_tot_dist_m',
                      'Velocity Band 3 Total Distance (m)': 'vel_b3_tot_dist_m',
                      'Velocity Band 4 Total Distance (m)': 'vel_b4_tot_dist_m',
                      'Velocity Band 5 Total Distance (m)': 'vel_b5_tot_dist_m',
                      'Acceleration B3 Efforts (Gen 2)': 'accel_b3_eff_gen2',
                      'Deceleration B3 Efforts (Gen 2)': 'decel_b3_eff_gen2',
                      'Total Dives': 'total_dives',
                      'Total Jumps': 'total_jumps',
                      'Velocity Band 3 Max Effort Distance (m)': 'vel_b3_max_eff_dist_m',
                      'Velocity Band 3 Max Effort Duration': 'vel_b3_max_eff_duration',
                      'Velocity Band 3 Total Effort Count': 'vel_b3_total_eff_count',
                      'Velocity Band 4 Average Distance (m)': 'vel_b4_avg_distance_m',
                      'Velocity Band 4 Max Effort Distance (m)': 'vel_b4_max_eff_dist_m',
                      'Velocity Band 4 Total Effort Count': 'vel_b4_total_eff_count',
                      'Velocity Band 5 Average Distance (m)': 'vel_b5_avg_distance_m',
                      'Velocity Band 5 Max Effort Distance (m)': 'vel_b5_max_eff_dist_m',
                      'Velocity Band 5 Total Effort Count': 'vel_b5_total_eff_count',
                      'Acceleration B1 Efforts (Gen 2)': 'accel_b1_eff_gen2',
                      'Acceleration B1-3 Total Efforts (Gen 2)': 'accel_b13_total_eff_gen2',
                      'Acceleration B2 Efforts (Gen 2)': 'accel_b2_eff_gen2',
                      'Deceleration B1 Efforts (Gen 2)': 'decel_b1_eff_gen2',
                      'Deceleration B1-3 Total Efforts (Gen 2)': 'decel_b13_total_eff_gen2',
                      'Deceleration B2 Efforts (Gen 2)': 'decel_b2_eff_gen2'}

# GPS DELIVERY DATA CONFIG
GPS_DELIVERY_GROUP_LIST = ["athlete", "date", "team_name", "activity_name", "period_name", "delivery", "athlete_id"]
GPS_DELIVERY_JOIN_LIST = ["athlete_id", "date_name", "team_name", "activity_name", "period_name", "delivery_name",
                          "delivery_time"]
GPS_DELIVERY_TABLE_NAME = 'fitnessGPSBallData'

GPS_BALL_DECIMAL_COL_LIST = ['delivery_runup_distance', 'max_runup_velocity', 'peak_player_load', 'raw_peak_roll',
                             'raw_peak_yaw', 'delivery_speed', 'raw_peak_resultant', 'relative_peak_player_load',
                             'relative_peak_resultant', 'relative_peak_roll', 'relative_peak_yaw',
                             'total_runup_distance',
                             'vel_at_pod', 'max_raw_resultant', 'avg_runup_velocity', 'avg_runup_distance',
                             'max_raw_player_load',
                             'max_raw_roll', 'max_raw_yaw']

GPS_BALL_INT_COL_LIST = ['avg_delivery_count', 'total_delivery_count', 'delivery_time']
GPS_DELIVERY_SRC_KEY_MAPPING = {"date_name": "date_name",
                                "delivery_name": "delivery_name",
                                "period_name": "period_name",
                                "activity_name": "activity_name",
                                "athlete_name": "athlete_name",
                                "athlete_id": "athlete_id",
                                "team_name": "team_name",
                                "delivery_runup_distance": "delivery_runup_distance",
                                "delivery_runup_max_speed": "max_runup_velocity",
                                "peak_player_load": "peak_player_load",
                                "events_roll": "raw_peak_roll",
                                "events_pl": "raw_peak_yaw",
                                "delivery_speed": "delivery_speed",
                                "events_resultant": "raw_peak_resultant",
                                "events_rel_pl": "relative_peak_player_load",
                                "events_rel_resultant": "relative_peak_resultant",
                                "events_rel_roll": "relative_peak_roll",
                                "events_rel_yaw": "relative_peak_yaw",
                                "imadeliveries_band0_average_count": "avg_delivery_count",
                                "imadeliveries_band0_total_count": "total_delivery_count",
                                "imadeliveries_band0_total_power": "total_runup_distance",
                                "delivery_runup_speed": "vel_at_pod",
                                "events_max_raw_resultant": "max_raw_resultant",
                                "delivery_runup_mean_speed": "avg_runup_velocity",
                                "imadeliveries_band0_average_power": "avg_runup_distance",
                                "delivery_time": "delivery_time",
                                "events_max_raw_pl": "max_raw_player_load",
                                "events_max_raw_roll": "max_raw_roll",
                                "events_max_raw_yaw": "max_raw_yaw"}

DAILY_ACTIVITY_TABLE_NAME = "fitnessForm"
DAILY_ACTIVITY_COLS_MAPPING = {"ID": "id",
                               "Start time": "start_time",
                               "Completion time": "completion_time",
                               "Column1": "record_date",
                               "Name2": "player_name",
                               "Who is filling out this form?": "form_filler",
                               "Did you train today?": "trained_today",
                               "Did you play today?": "played_today",
                               "Why didnt you train or play?": "reason_noplay_or_train",
                               "Batting Train (Minutes)": "batting_train_mins",
                               "Batting Train (RPE)": "batting_train_rpe",
                               "Bowling Train (Minutes)": "bowling_train_mins",
                               "Bowling Train Balls": "bowling_train_balls",
                               "Bowling Train (RPE)": "bowling_train_rpe",
                               "Fielding Train (Minutes)": "fielding_train_mins",
                               "Fielding Train (RPE)": "fielding_train_rpe",
                               "Strength (Minutes)": "strength_mins",
                               "Strength (RPE)": "strength_rpe",
                               "Running (Minutes)": "running_mins",
                               "Running (RPE)": "running_rpe",
                               "Cross Training (Minutes)": "cross_training_mins",
                               "Cross Training (RPE)": "cross_training_rpe",
                               "Rehab (Minutes)": "rehab_mins",
                               "Rehab (RPE)": "rehab_rpe",
                               "Batting Match (Minutes)": "batting_match_mins",
                               "Bowling Match Balls": "bowling_match_balls",
                               "Bowling Match (RPE)": "bowling_match_rpe",
                               "Batting Match (RPE)": "batting_match_rpe",
                               "Bowling Match (Minutes)": "bowling_match_mins",
                               "Fielding Match (Minutes)": "fielding_match_mins",
                               "Fielding Match (RPE)": "fielding_match_rpe",
                               "How do you rate your level of fatigue?2": "fatigue_level_rating",
                               "How do you rate last nights sleep?2": "sleep_rating",
                               "How do you rate your muscle soreness?2": "muscle_soreness_rating",
                               "How would you rate your stress levels?2": "stress_levels_rating",
                               "How is your wellness?2": "wellness_rating"
                               }

PLAYER_LOAD_TABLE_NAME = "playerLoad"

NOTIFICATION_SCHEDULER_TABLE_NAME = 'notificationScheduler'
NOTIFICATION_SCHEDULER_COL = "id"

PEAK_LOAD_TABLE_NAME = "matchPeakLoad"

SCHEDULER_LOG_TABLE_NAME = 'schedulerlog'
SCHEDULER_LOG_COL = "id"

USER_QUERY_TABLE_NAME = "userQueries"

BOWL_PLANNING_TABLE_NAME = "bowlPlanning"
BOWL_PLANNING_KEY_COL = "id"

CALENDAR_EVENT_TABLE_NAME = 'calendarEvents'
CALENDAR_EVENT_KEY_COL = "id"

RECIPIENT_GROUP_TABLE_NAME = 'recipientGroup'
RECIPIENT_GROUP_COL = "id"

FIELDING_ANALYSIS_TABLE_NAME = "fieldingAnalysis"
FIELDING_ANALYSIS_KEY_COL = "id"

PLAYER_MAPPER_TABLE_NAME = 'playerMapping'
PLAYER_MAPPER_KEY_COL = "id"

INGESTION_LOG_TABLE_NAME = 'ingestionLog'
INGESTION_LOG_KEY_COL = "id"

QUERY_FEEDBACK_TABLE_NAME = "searchQueryFeedback"

# Replace spelling mistake in teams_df
TEAM_SPELL_MAPPING = {
    'DURBAN SUPER GAINTS': 'DURBAN SUPER GIANTS',
    'GULF GAINTS': 'GULF GIANTS',
    'DESERT VIPES': 'DESERT VIPERS'
}

# SMTP SERVER CONFIG
BUILD_ENV = getEnvVariables("BUILD_ENV")
SMTP_EMAIL = getEnvVariables("SMTP_EMAIL")
SMTP_PASSWORD = getEnvVariables("SMTP_PASSWORD")
SMTP_SERVER_IP = getEnvVariables("SMTP_SERVER_IP")
SMTP_PORT = getEnvVariables("SMTP_PORT")
