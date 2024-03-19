from common.utils.helper import getEnvVariables

APP_BASE_URL = getEnvVariables('APP_BASE_URL')
BASE_URL = f"{APP_BASE_URL}/#/gps"
WELLNESS_DASHBOARD_PATTERN = f"{BASE_URL}/playerWellness/dailyWellness"
BOWLING_GPS_ENDPOINT = f"{BASE_URL}/wellnessDashboard/combinedBowlingGPS?src=wtsap"
BOWLING_REPORT_ENDPOINT = f"{BASE_URL}/wellnessDashboard/bowlingReport?src=wtsap"
COACH_REPORT_ENDPOINT = f"{BASE_URL}/wellnessDashboard/coachView?src=wtsap"
GROUP_GPS_REPORT_ENDPOINT = f"{BASE_URL}/wellnessDashboard?src=wtsap"
GROUP_WEEKLY_ENDPOINT = f"{BASE_URL}/wellnessDashboard/groupWeekly?src=wtsap"
PLAYER_READINESS_ENDPOINT = f"{BASE_URL}/wellnessDashboard/playerReadiness?src=wtsap"

PLAYER_WELLNESS_DASHBOARD_PATTERN = f"{BASE_URL}/playerWellness/dailyWellness?src=wtsap"
PLAYER_BOWLING_GPS_ENDPOINT = f"{BASE_URL}/playerWellness/bowlingGPS?src=wtsap"
PLAYER_WEEKLY_GPS_REPORT_ENDPOINT = f"{BASE_URL}/playerWellness/weeklyGPS?src=wtsap"
PLAYER_GROUP_GPS_ENDPOINT = f"{BASE_URL}/playerWellness/groupGPS?src=wtsap"
PLAYER_INDIVIDUAL_GROUP_ENDPOINT = f"{BASE_URL}/playerWellness/groupIndividual?src=wtsap"
