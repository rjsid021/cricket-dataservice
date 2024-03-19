import json
import sys

from cassandra.concurrent import execute_concurrent

from common.dao_client import session

sys.path.append("./../../")
sys.path.append("./")
from DataIngestion.utils.helper import readExcel
from DataIngestion.config import DAILY_ACTIVITY_EXCEL_DATA_PATH, DAILY_ACTIVITY_COLS_MAPPING


def patch_column():
    daily_activity = readExcel(DAILY_ACTIVITY_EXCEL_DATA_PATH, "Form1")[
        [key for key, value in DAILY_ACTIVITY_COLS_MAPPING.items()]].rename(columns=DAILY_ACTIVITY_COLS_MAPPING)
    daily_activity = daily_activity.fillna(-1)
    daily_activity["bowling_train_balls"] = daily_activity["bowling_train_balls"].astype(int)

    statements_and_params = []
    for index, row in daily_activity.iterrows():
        print(row['id'], row['bowling_train_balls'])
        sql_stmt = "UPDATE fitnessform SET bowling_train_balls={} WHERE id={}".format(row["bowling_train_balls"],
                                                                                      row["id"])
        statements_and_params.append((sql_stmt, ()))
    execute_concurrent(session, statements_and_params, concurrency=50)


if __name__ == "__main__":
    patch_column()
