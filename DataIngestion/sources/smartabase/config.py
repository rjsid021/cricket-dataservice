from common.utils.helper import getEnvVariables

SMARTABASE_USER = getEnvVariables("SMARTABASE_USER")
SMARTABASE_PASSWORD = getEnvVariables("SMARTABASE_PASSWORD")
SMARTABASE_APP = getEnvVariables("SMARTABASE_APP")
SMARTABASE_URL = getEnvVariables("SMARTABASE_URL")

LOGIN_API = f"{SMARTABASE_URL}/{SMARTABASE_APP}/api/v2/user/loginUser"
SMARTABASE_USERS_URL = f"{SMARTABASE_URL}/{SMARTABASE_APP}/api/v1/groupmembers?informat=json&format=json"
SMARTABASE_DATA_SYNC_URL = f"{SMARTABASE_URL}/{SMARTABASE_APP}/api/v1/synchronise?informat=json&format=json"
SMARTABASE_LOGOUT_URL = f"{SMARTABASE_URL}/{SMARTABASE_APP}/api/v2/user/logout"

READINESS_TO_PERFORM_FORM_NAME = "Readiness to Perform"
READINESS_TO_PERFORM_TABLE = "readiness_to_perform"
READINESS_TO_PERFORM_TEXT_COLS = ["playing_status", "physical_status", "injury_status", "action_status_comments"]
READINESS_TO_PERFORM_COLUMN_MAPPING = {
    'start_date': 'start_date',
    'start_time': 'start_time',
    'finish_date': 'finish_date',
    'finish_time': 'finish_time',
    'user_id': 'smartabase_id',
    'event_id': 'event_id',
    'last_sync_time': 'last_sync_time',
    'Playing Status Comments': 'playing_status',
    'Physical Status Comments': 'physical_status',
    'Injury Status Comments': 'injury_status',
    'First Name': 'first_name',
    'Last Name': 'last_name',
    'Actions Status Comments': 'action_status_comments'
}

MI_AVAILABILITY_FORM_NAME = "MI Availability"
MI_AVAILABILITY_TABLE = "mi_availability"
MI_AVAILABILITY_TEXT_COLS = ["linked_medical_status", "comments"]
MI_AVAILABILITY_COLUMN_MAPPING = {
    'start_date': 'start_date',
    'start_time': 'start_time',
    'finish_date': 'finish_date',
    'finish_time': 'finish_time',
    'user_id': 'smartabase_id',
    'event_id': 'event_id',
    'last_sync_time': 'last_sync_time',
    'Overall Status': 'overall_status',
    'First Name': 'first_name',
    'Last Name': 'last_name',
    'Linked Medical Status': 'linked_medical_status',
    'Current Comments': 'comments'
}