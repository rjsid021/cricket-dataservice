import json
import os

from DataIngestion.config import CRICSHEET_DATA_PATH


def year_wise_file_sorting():
    competitions = os.listdir(CRICSHEET_DATA_PATH)

    counter = 0
    for competition in competitions:
        if competition == '.DS_Store':
            continue
        competition_path = os.path.join(CRICSHEET_DATA_PATH, competition)
        file_dirs = os.listdir(competition_path)
        for file in file_dirs:
            file_or_dir = f"{os.path.join(competition_path, file)}"
            with open(file_or_dir, 'r') as f:
                if file_or_dir.split('/')[-1].split(".")[-1] != "json":
                    continue
                json_file = json.load(f)
                source_of_truth = json_file["info"]['season']
                if type(source_of_truth) == int:
                    season = source_of_truth
                else:
                    season = int(source_of_truth[:4])
                folder_path = "/".join(file_or_dir.split('/')[1: -1]) + "/" + "".join(
                    file_or_dir.split('/')[-2]) + " " + str(season)
                if not os.path.exists(os.path.join(folder_path)):
                    os.makedirs(os.path.join(folder_path))
                    print("Directory created:", os.path.join(folder_path))
                os.rename(
                    file_or_dir,
                    folder_path + "/" + file
                )


if __name__ == '__main__':
    year_wise_file_sorting()
