import pandas as pd


def parse_html(file_name):
    from bs4 import BeautifulSoup
    if file_name.find("html") == -1:
        return
    # Read the HTML file
    with open(file_name, 'r', encoding='utf-8') as file:
        html_content = file.read()

    # Parse the HTML content
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find all <a> tags
    a_tags = soup.find_all('a')
    href_set = set()
    # Extract and print the href attributes of the <a> tags
    for a_tag in a_tags:
        href = a_tag.get('href')
        if href:
            if "cricketers" in href.split("/"):
                href_set.add(href)
    return href_set


if __name__ == "__main__":
    import os

    # Specify the path to the folder you want to list
    folder_path = '/data/sM'

    # Check if the path exists and is a directory
    if os.path.exists(folder_path) and os.path.isdir(folder_path):
        # List all items (files and directories) in the specified folder
        items = os.listdir(folder_path)
        sM_df = pd.DataFrame(columns=['item', 'season', 'match', 'cricketer', 'cricinfo_id'])
        # Iterate through the items and print them
        for item in items:
            if item == '.DS_Store':
                continue
            x = os.path.join(folder_path, item)
            seasons = os.listdir(x)

            for season in seasons:
                if season == '.DS_Store':
                    continue

                matches_list = os.listdir(os.path.join(x, season))
                for match in matches_list:
                    if match == '.DS_Store':
                        continue
                    cricketer_list = parse_html(os.path.join(os.path.join(x, season), match))
                    for cricketer in cricketer_list:
                        if cricketer == '.DS_Store':
                            continue
                        sM_df.loc[len(sM_df.index)] = [item, season, match, cricketer, cricketer.split("-")[-1]]
                    sM_df.to_csv("sM_df.csv")

    else:
        print(f"The specified path '{folder_path}' does not exist or is not a directory.")
