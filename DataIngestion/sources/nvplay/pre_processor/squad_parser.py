import os


def xyz(file_name, asdfasdf):
    try:
        from bs4 import BeautifulSoup

        # Load the HTML file
        with open(file_name, 'r', encoding='utf-8') as html_file:
            html_content = html_file.read()
        # Create a BeautifulSoup object to parse the HTML
        soup_tasty = BeautifulSoup(html_content, 'html.parser')

        venue = "".join(
            str(file_name).split(",")[3:]
        ).split("-")[0].strip() + " : " + file_name.split(",")[0].split("/")[-1]
        # Create a BeautifulSoup object to parse the HTML
        soup = BeautifulSoup(html_content, 'html.parser')

        # Find all <a> elements
        divs_with_class_container = soup.find_all('div', class_='ds-rounded-lg ds-mt-2')
        # Create a BeautifulSoup object to parse the HTML
        # Loop through the found <div> elements
        div_with_class = soup.find_all('table',
                                       class_='ds-w-full ds-table ds-table-md ds-table-auto ci-scorecard-table')
        master_iterator = 0
        team1 = []
        team2 = []
        # match_date_raw = soup.find_all('div', class_='ds-flex ds-justify-between ds-items-center')[2]
        # match_date = 'May 24'
        match_date_raw = soup.find_all('div', class_='ds-text-compact-xxs ds-p-2 ds-px-4 lg:ds-py-3')[0]
        match_date = str(match_date_raw).split("<div")[8].split(",")[3].strip()
        import re

        def is_date_format(input_string):
            # Define the regular expression pattern for 'Month Day' format
            pattern = r'^(January|February|March|April|May|June|July|August|September|October|November|December)\s\d{1,2}$'

            # Check if the input string matches the pattern
            return bool(re.match(pattern, input_string))

        if not is_date_format(match_date):
            print(f"'{match_date}' is not in the 'Month Day' format.")
            match_date = str(match_date_raw).split("<div")[8].split(",")[4].strip()

        for div in div_with_class:
            a_tags = div.find_all('a')
            iterator = -1
            # Loop through the found <a> tags and print their attributes or text

            for a_tag in a_tags:
                iterator += 1
                href = a_tag.get('href')
                text = a_tag.get_text().replace('\xa0', '')
                href = href.split("/")[-1].split("-")[-1]
                text = text.replace("\n", "").replace(",", "").replace("†", "").replace("(c)", "")
                if href == 'photo':
                    continue
                if master_iterator == 1:
                    if iterator < 11:
                        team2.append({
                            "cricinfo_id": href,
                            "player_name": text.strip()
                        })
                    else:
                        break
                if master_iterator == 0:
                    team1.append({
                        "cricinfo_id": href,
                        "player_name": text.strip()
                    })
                    if iterator == 10:
                        master_iterator += 1
                        break
        team_11 = soup.find_all('span', class_="ds-text-title-xs ds-font-bold ds-text-typo")[0].text.split("(")[
            0].strip()
        team_22 = soup.find_all('span', class_="ds-text-title-xs ds-font-bold ds-text-typo")[1].text.split("(")[
            0].strip()
        # Parse the input date string to a datetime object
        from datetime import datetime
        year = venue.strip().split(" ")[2]
        if year not in ["2023", '2022', '2021', '2020', '2019', '2018', '2017', '2016', '2015']:
            year = venue.strip().split(" ")[6]
        parsed_date = datetime.strptime(f"{match_date} {year}", "%B %d %Y")

        # Format the parsed date to the desired format "DD/MM/YYYY"
        formatted_date_string = parsed_date.strftime("%d/%m/%Y")
        return ({
            "venue": venue.strip(),
            "match_date": formatted_date_string,
            "match_name_": f'{team_11} v {team_22}',
            "match_name": f'{team_22} v {team_11}',
            team_11: team1,
            team_22: team2
        })
    except Exception as e:
        print("---------------->", file_name, "<-----------------")
        return None


def fun():
    year = 2023
    # Specify the directory you want to start iterating from
    # Use os.walk() to traverse the directory and its subdirectories
    root_directory = '/Users/achintya.chaudhary/Documents/projects/CricketDataService/t20_last'
    output = []
    iterator = 1
    for root, dirs, files in os.walk(root_directory):
        for file in files:
            # Join the root directory with the file name to get the full path
            file_path = os.path.join(root, file)
            if file_path.split("/")[-1] == ".DS_Store":
                continue
            print(iterator)
            output.append(xyz(file_path, year))
            iterator += 1
            print("--------------------------------")

    print(output)


if __name__ == "__main__":
    fun()
