import os
import re
from pathlib import Path

import pandas as pd
from environs import Env
from tabulate import tabulate


# get project root
def get_project_root() -> Path:
    return Path(__file__).parent.parent.parent


# prints dataframe in a tabular format
def getPrettyDF(df):
    return tabulate(df, headers='keys', tablefmt='psql')


# pandas row factory function to generate df out of db table
def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


# get integer from the input string
def getIntFromString(x: str):
    return int(re.search(r'\d+', x).group())


# function to get environment variables
def getEnvVariables(key):
    env = Env()
    env.read_env()
    return os.environ.get(key)


def getTeamsMapping():
    team_dict = {
        "MI": "Mumbai Indians",
        "MICT": "MI Capetown",
        "MIE": "MI Emirates",
        "MINY": "MI New York",
        "MIW": "Mumbai Indian Womens"
    }
    return team_dict


# method to convert an string to titlecase
def titlecase(s):
    return re.sub(
        r"[A-Za-zé]+('[A-Za-z]+)?",
        lambda word: word.group(0).capitalize(),
        s)
