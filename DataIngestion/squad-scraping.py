import requests
from bs4 import BeautifulSoup
import os


team_list = ['chennai-super-kings', 'delhi-capitals', 'gujarat-titans', 'kolkata-knight-riders', 'lucknow-super-giants',
             'punjab-kings', 'rajasthan-royals', 'royal-challengers-bangalore', 'sunrisers-hyderabad', 'mumbai-indians']

for team in team_list:
    # mumbai-indians'
    URL = "https://www.iplt20.com/teams/" + team + "/squad" # Replace this with the website's URL

    cur_dir = os.path.dirname(os.path.abspath("__file__"))
    images_dir = os.path.join(cur_dir, 'images')
    team_images_dir = os.path.join(images_dir, team)
    if not os.path.isdir(team_images_dir):
        os.makedirs(team_images_dir)

    print(f"cur_dir ---> {team_images_dir}")

    getURL = requests.get(URL, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(getURL.text, 'html.parser')

    results = soup.find_all("div", class_="ih-pcard-wrap")
    # print(results.prettify())

    for job_element in results:
        players = job_element.find_all("div", class_="ih-p-img")
        # print(f"image ---> {players}")
        for data in players:
            img = data.find('img').get('src')
            webs = requests.get(img)
            print(f"img --> {img}")
            player = data.find('h2').text
            print(f"h2 --> {player}")
            print(f"file path ----> {'images/' + team + '/' + player.replace('.', ' ').replace(' ', '-').lower() + '.png'}")
            open('images/' + team + '/' + player.replace('.', ' ').replace(' ', '-').lower() + '.png', 'wb').write(webs.content)