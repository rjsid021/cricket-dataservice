import sys
sys.path.append("./../../")
sys.path.append("./")
from DataIngestion.config import IMAGE_STORE_URL
from DataService.utils.helper import dropFilter, getUpdateSetValues
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session
from common.db_config import DB_NAME

GET_TEAMS_DATA = f'''select team_id, team_name, competition_name from {DB_NAME}.teams;'''
teams_data = getPandasFactoryDF(session, GET_TEAMS_DATA)
teams_data['team_image_url'] = (IMAGE_STORE_URL + 'teams/' + teams_data['competition_name'] + '/' + teams_data['team_name'].apply(lambda x: x.replace(' ', '-')
                               .lower()).astype(str) + ".png")

for i in teams_data.to_dict(orient='records'):
    team_id = i['team_id']
    fitness_form_update_dict = dropFilter(['team_name', 'competition_name', 'team_id'], i)
    fitness_form_set_values = getUpdateSetValues(fitness_form_update_dict)
    fitness_form_update_sql = f"update {DB_NAME}.teams set {', '.join(fitness_form_set_values)} where team_id={team_id}"
    session.execute(fitness_form_update_sql)