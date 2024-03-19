import pandas as pd

from DataIngestion.config import TEAM_COLOR_PATH, TEAMS_TABLE_NAME
from DataIngestion.utils.helper import readCSV
from DataService.fetch_sql_queries import GET_TEAMS_DATA
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao.insert_data import insertToDB
from common.dao_client import session
from common.db_config import DB_NAME


def ingest_team_color():
    color_df = readCSV(TEAM_COLOR_PATH)
    existing_teams_data = getPandasFactoryDF(session, GET_TEAMS_DATA)
    updated_teams = pd.merge(
        existing_teams_data,
        color_df,
        left_on='team_short_name',
        right_on='Team Short Name',
        how='left'
    ).drop(['Team Name', 'Team Short Name'], axis=1)

    updated_teams['Color Code'] = updated_teams['Color Code'].fillna('')
    updated_teams = updated_teams.rename(
        columns={
            'Color Code': 'color_code'
        }
    )
    updated_teams = updated_teams.to_dict(orient='records')
    insertToDB(session, updated_teams, DB_NAME, TEAMS_TABLE_NAME)


if __name__ == "__main__":
    ingest_team_color()
