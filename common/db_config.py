# db creds
from common.utils.helper import getEnvVariables

HOST = getEnvVariables("DB_HOST")
PORT = int(getEnvVariables("DB_PORT"))
DB_NAME = getEnvVariables("DB_NAME")
DB_USER = getEnvVariables("DB_USER")
DB_PASSWORD = getEnvVariables("DB_PASSWORD")