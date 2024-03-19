import urllib.parse
from datetime import date
from datetime import datetime, timedelta

from scheduler.constants import WELLNESS_NOTIFICATION_MODULE, MODULE_MAPPING, BOWLING_GPS, BOWLING_REPORT, COACH_REPORT, \
    GROUP_GPS_REPORT, GROUP_WEEKLY, PLAYER_READINESS, PLAYER_MODULE_MAPPING, PLAYER_WELLNESS_DASHBOARD_REPORT, \
    PLAYER_INDIVIDUAL_GROUP_REPORT, PLAYER_GROUP_GPS_REPORT, PLAYER_WEEKLY_GPS_REPORT_REPORT, PLAYER_BOWLING_GPS_REPORT, \
    UTM_CAMPAIGN_NAME
from third_party_service.notification.constants import WELLNESS_DASHBOARD_PATTERN, BOWLING_GPS_ENDPOINT, \
    BOWLING_REPORT_ENDPOINT, COACH_REPORT_ENDPOINT, GROUP_GPS_REPORT_ENDPOINT, GROUP_WEEKLY_ENDPOINT, \
    PLAYER_READINESS_ENDPOINT, PLAYER_BOWLING_GPS_ENDPOINT, PLAYER_WEEKLY_GPS_REPORT_ENDPOINT, \
    PLAYER_GROUP_GPS_ENDPOINT, PLAYER_INDIVIDUAL_GROUP_ENDPOINT, PLAYER_WELLNESS_DASHBOARD_PATTERN
from third_party_service.notification.whatsapp import config
from third_party_service.notification.whatsapp.config import WELLNESS_CAMPAIGN_DETAILS, WELLNESS_CAMPAIGN_TEMPLATE, \
    WELLNESS_CAMPAIGN_BUSINESS, WELLNESS_CAMPAIGN_CODE, GPS_CAMPAIGN_CODE, GPS_CAMPAIGN_BUSINESS, GPS_CAMPAIGN_DETAILS, \
    GPS_CAMPAIGN_TEMPLATE


class PayloadTemplate:
    def __init__(self):
        self.campaign_auth_id = config.CAMPAIGN_AUTH_ID

    def get_template(self, player_name, report_name, player_number, date_filter, kwargs):
        if report_name == WELLNESS_NOTIFICATION_MODULE:
            return self.get_fitness_template(player_name, player_number, date_filter)
        elif report_name in [
            BOWLING_GPS,
            BOWLING_REPORT,
            COACH_REPORT,
            GROUP_GPS_REPORT,
            GROUP_WEEKLY,
            PLAYER_READINESS,
            PLAYER_BOWLING_GPS_REPORT,
            PLAYER_WEEKLY_GPS_REPORT_REPORT,
            PLAYER_GROUP_GPS_REPORT,
            PLAYER_INDIVIDUAL_GROUP_REPORT,
            PLAYER_WELLNESS_DASHBOARD_REPORT
        ]:
            return self.get_gps_template(player_name, report_name, player_number, date_filter, kwargs)

    def get_gps_template(self, player_name, report_name, player_number, date_filter, kwargs):
        URL_PATTERN = ""
        if report_name in [
            PLAYER_BOWLING_GPS_REPORT,
            PLAYER_WEEKLY_GPS_REPORT_REPORT,
            PLAYER_GROUP_GPS_REPORT,
            PLAYER_INDIVIDUAL_GROUP_REPORT,
            PLAYER_WELLNESS_DASHBOARD_REPORT
        ]:
            if date_filter.get('record_date'):
                if type(date_filter.get('record_date')) is list:
                    record_date = ""
                    for iter in date_filter.get('record_date'):
                        record_date += f"&record_date={urllib.parse.quote(iter)}"
                else:
                    record_date = f"&record_date={urllib.parse.quote(date_filter.get('record_date'))}"
                record_date = record_date + f"&userName={urllib.parse.quote(str(kwargs.get('user_name')))}"
            if date_filter.get('start_date'):
                start_date_epoch = date_filter.get('start_date')
                end_date_epoch = date_filter.get('end_date')
                period_range = f"&start_date={start_date_epoch}&end_date={end_date_epoch}&userName={urllib.parse.quote(str(kwargs.get('user_name')))}"
                bowling_report_period_range = f"&start_date={start_date_epoch}&end_date={end_date_epoch}&userName={urllib.parse.quote(str(kwargs.get('user_name')))}"
            if not date_filter:
                today = datetime.now()
                today_date = today.strftime('%A,%b %d, %Y')
                week_ago_day_epoch = int((today - timedelta(weeks=1)).timestamp() * 1000)
                today_day_epoch = int(today.timestamp() * 1000)
                fifteen_days_ago_day_epoch = int((today - timedelta(days=15)).timestamp() * 1000)
                fifteen_days_later_epoch = int((today + timedelta(days=15)).timestamp() * 1000)

                # Create query param string.
                period_range = f"&start_date={week_ago_day_epoch}&end_date={today_day_epoch}"
                bowling_report_period_range = f"&start_date={fifteen_days_ago_day_epoch}&end_date={fifteen_days_later_epoch}"
                record_date = f"&record_date={urllib.parse.quote(today_date)}"

            # Form report url
            if report_name == PLAYER_BOWLING_GPS_REPORT:
                # Only start date in query parameters
                if kwargs.get('active_tab') == 0:
                    URL_PATTERN = PLAYER_BOWLING_GPS_ENDPOINT + "&actTab=0" + record_date + f"&utm_medium=whatsapp&utm_campaign={UTM_CAMPAIGN_NAME.get(PLAYER_BOWLING_GPS_REPORT)}"
                else:
                    URL_PATTERN = PLAYER_BOWLING_GPS_ENDPOINT + f"&actTab={kwargs.get('active_tab')}" + record_date + f"&utm_medium=whatsapp&utm_campaign={UTM_CAMPAIGN_NAME.get(PLAYER_BOWLING_GPS_REPORT)}"
            elif report_name == PLAYER_WEEKLY_GPS_REPORT_REPORT:
                # Start date and End date in query parameters
                URL_PATTERN = PLAYER_WEEKLY_GPS_REPORT_ENDPOINT + period_range + f"&utm_medium=whatsapp&utm_campaign={UTM_CAMPAIGN_NAME.get(PLAYER_WEEKLY_GPS_REPORT_REPORT)}"
            elif report_name == PLAYER_GROUP_GPS_REPORT:
                # Start date and End date in query parameters
                URL_PATTERN = PLAYER_GROUP_GPS_ENDPOINT + record_date + f"&utm_medium=whatsapp&utm_campaign={UTM_CAMPAIGN_NAME.get(PLAYER_GROUP_GPS_REPORT)}"
            elif report_name == PLAYER_INDIVIDUAL_GROUP_REPORT:
                # Only start date in query parameters
                URL_PATTERN = PLAYER_INDIVIDUAL_GROUP_ENDPOINT + period_range + f"&utm_medium=whatsapp&utm_campaign={UTM_CAMPAIGN_NAME.get(PLAYER_INDIVIDUAL_GROUP_REPORT)}"
            elif report_name == PLAYER_WELLNESS_DASHBOARD_REPORT:
                # Start date and End date in query parameters
                URL_PATTERN = PLAYER_WELLNESS_DASHBOARD_PATTERN + record_date + f"&utm_medium=whatsapp&utm_campaign={UTM_CAMPAIGN_NAME.get(PLAYER_WELLNESS_DASHBOARD_REPORT)}"

            return {
                "authId": self.campaign_auth_id,
                "campaignCode": GPS_CAMPAIGN_CODE,
                "campaignDetails": GPS_CAMPAIGN_DETAILS,
                "campaignResponseType": "Non-actionable",
                "language": "en",
                "body": {
                    "text": GPS_CAMPAIGN_TEMPLATE,
                    "responseType": "template",
                    "templateComponents": [
                        {
                            "type": "text",
                            "sub_type": "",
                            "order": 0,
                            "value": player_name
                        },
                        {
                            "type": "text",
                            "sub_type": "",
                            "order": 1,
                            "value": PLAYER_MODULE_MAPPING.get(report_name)
                        },
                        {
                            "type": "text",
                            "sub_type": "",
                            "order": 2,
                            "value": str(date.today())
                        },
                        {
                            "type": "text",
                            "sub_type": "",
                            "order": 3,
                            "value": PLAYER_MODULE_MAPPING.get(report_name)
                        },
                        {
                            "type": "text",
                            "sub_type": "",
                            "order": 4,
                            "value": URL_PATTERN
                        }
                    ]
                },
                "business": GPS_CAMPAIGN_BUSINESS,
                "user": player_number
            }
        else:
            if date_filter.get('record_date'):
                if type(date_filter.get('record_date')) is list:
                    record_date = ""
                    for iter in date_filter.get('record_date'):
                        record_date += f"&record_date={urllib.parse.quote(iter)}"
                else:
                    record_date = f"&record_date={urllib.parse.quote(date_filter.get('record_date'))}"
            if date_filter.get('start_date'):
                start_date_epoch = date_filter.get('start_date')
                end_date_epoch = date_filter.get('end_date')
                period_range = f"&start_date={start_date_epoch}&end_date={end_date_epoch}"
                bowling_report_period_range = f"&start_date={start_date_epoch}&end_date={end_date_epoch}"
            if not date_filter:
                today = datetime.now()
                today_date = today.strftime('%A,%b %d, %Y')
                week_ago_day_epoch = int((today - timedelta(weeks=1)).timestamp() * 1000)
                today_day_epoch = int(today.timestamp() * 1000)
                fifteen_days_ago_day_epoch = int((today - timedelta(days=15)).timestamp() * 1000)
                fifteen_days_later_epoch = int((today + timedelta(days=15)).timestamp() * 1000)

                # Create query param string.
                period_range = f"&start_date={week_ago_day_epoch}&end_date={today_day_epoch}"
                bowling_report_period_range = f"&start_date={fifteen_days_ago_day_epoch}&end_date={fifteen_days_later_epoch}"
                record_date = f"&record_date={urllib.parse.quote(today_date)}"

            # Form report url
            if report_name == BOWLING_GPS:
                # Only start date in query parameters
                URL_PATTERN = BOWLING_GPS_ENDPOINT + record_date + f"&utm_medium=whatsapp&utm_campaign={UTM_CAMPAIGN_NAME.get(BOWLING_GPS)}"
            elif report_name == BOWLING_REPORT:
                # Start date and End date in query parameters
                URL_PATTERN = BOWLING_REPORT_ENDPOINT + bowling_report_period_range + f"&actTab={kwargs.get('active_tab')}" + f"&utm_medium=whatsapp&utm_campaign={UTM_CAMPAIGN_NAME.get(BOWLING_REPORT)}"
            elif report_name == COACH_REPORT:
                # Start date and End date in query parameters
                URL_PATTERN = COACH_REPORT_ENDPOINT + period_range + f"&utm_medium=whatsapp&utm_campaign={UTM_CAMPAIGN_NAME.get(COACH_REPORT)}"
            elif report_name == GROUP_GPS_REPORT:
                # Only start date in query parameters
                URL_PATTERN = GROUP_GPS_REPORT_ENDPOINT + record_date + f"&utm_medium=whatsapp&utm_campaign={UTM_CAMPAIGN_NAME.get(GROUP_GPS_REPORT)}"
            elif report_name == GROUP_WEEKLY:
                # Start date and End date in query parameters
                URL_PATTERN = GROUP_WEEKLY_ENDPOINT + period_range + f"&utm_medium=whatsapp&utm_campaign={UTM_CAMPAIGN_NAME.get(GROUP_WEEKLY)}"
            elif report_name == PLAYER_READINESS:
                utm_campaign = UTM_CAMPAIGN_NAME.get(f"PLAYER_READINESS_{kwargs.get('active_tab')}")
                # Start date and End date in query parameters
                URL_PATTERN = PLAYER_READINESS_ENDPOINT + period_range + f"&actTab={kwargs.get('active_tab')}" + f"&utm_medium=whatsapp&utm_campaign={utm_campaign}"

            return {
                "authId": self.campaign_auth_id,
                "campaignCode": GPS_CAMPAIGN_CODE,
                "campaignDetails": GPS_CAMPAIGN_DETAILS,
                "campaignResponseType": "Non-actionable",
                "language": "en",
                "body": {
                    "text": GPS_CAMPAIGN_TEMPLATE,
                    "responseType": "template",
                    "templateComponents": [
                        {
                            "type": "text",
                            "sub_type": "",
                            "order": 0,
                            "value": player_name
                        },
                        {
                            "type": "text",
                            "sub_type": "",
                            "order": 1,
                            "value": MODULE_MAPPING.get(report_name)
                        },
                        {
                            "type": "text",
                            "sub_type": "",
                            "order": 2,
                            "value": str(date.today())
                        },
                        {
                            "type": "text",
                            "sub_type": "",
                            "order": 3,
                            "value": MODULE_MAPPING.get(report_name)
                        },
                        {
                            "type": "text",
                            "sub_type": "",
                            "order": 4,
                            "value": URL_PATTERN
                        }
                    ]
                },
                "business": GPS_CAMPAIGN_BUSINESS,
                "user": player_number
            }

    def get_fitness_template(self, player_name, player_number, date_filter):
        if date_filter.get('record_date'):
            record_date = f"&record_date={urllib.parse.quote(date_filter.get('record_date'))}"
        if not date_filter:
            today = datetime.now()
            today_date = today.strftime('%A,%b %d, %Y')
            record_date = f"&record_date={urllib.parse.quote(today_date)}"
        return {
            "authId": self.campaign_auth_id,
            "campaignCode": WELLNESS_CAMPAIGN_CODE,
            "campaignDetails": WELLNESS_CAMPAIGN_DETAILS,
            "campaignResponseType": "Non-actionable",
            "language": "en",
            "body": {
                "text": WELLNESS_CAMPAIGN_TEMPLATE,
                "responseType": "template",
                "templateComponents": [
                    {
                        "type": "text",
                        "sub_type": "",
                        "order": 0,
                        "value": player_name
                    },
                    {
                        "type": "text",
                        "sub_type": "",
                        "order": 4,
                        "value": f"{WELLNESS_DASHBOARD_PATTERN}?playerName={urllib.parse.quote(player_name)}" + record_date + f"&utm_medium=whatsapp&utm_campaign={UTM_CAMPAIGN_NAME.get(WELLNESS_NOTIFICATION_MODULE)}"
                    }
                ]
            },
            "business": WELLNESS_CAMPAIGN_BUSINESS,
            "user": player_number
        }
