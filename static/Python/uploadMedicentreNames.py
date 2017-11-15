from bs4 import BeautifulSoup
from urllib.request import urlopen
import pandas as pd
import sqlite3
import requests, json, re, time
import datetime

#This function hits the medicentre website and scrapes the data to get the names of the medicentres
def getMedicentreWaitTimes(url= "https://www.medicentres.com/clinic-locations/"):
    payload = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
               "Accept-Encoding": "gzip, deflate, br",
               "Accept-Language": "en-GB,en-US;q=0.8,en;q=0.6",
               "Cache-Control": "max-age=0",
               "Connection": "keep-alive",
               "Content-Length": "103",
               "Content-Type": "application/x-www-form-urlencoded",
               "Cookie": "_ga=GA1.2.1572985675.1506014656; _gid=GA1.2.297042775.1507133010",
               "Host": "www.medicentres.com",
               "Origin": "https://www.medicentres.com",
               "Referer": "https://www.medicentres.com/clinic-locations/",
               "Upgrade-Insecure-Requests": "1",
               "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36"
               }
    data = {"navigator": "yes",
            "city": "Edmonton, Alberta",
            "clinic": "",
            "waittimes": "all",
            "distancewithin": "all",
            "waittimer-submit": "Submit"}
    r = requests.post(url=url, headers=payload, data=json.dumps(data))

    soup = BeautifulSoup(r.content, "lxml")

    df = pd.DataFrame(columns=['Name'])

    for elem in soup.select('div.col-sm-12.medicentre'):
        if "Edmonton," in elem.find(class_='address').get_text() \
                or 'Sherwood Park' in elem.find(class_='address').get_text() \
                or 'St. Albert' in elem.find(class_='address').get_text():
            name = elem.find('a').get_text()
            df = df.append(pd.DataFrame([[name]], columns=['Name']), ignore_index=True)

    df.index.name = 'ID'
    print(df)

    con = sqlite3.connect("/Users/usmanr/PycharmProjects/waitTimesDB/waitTimes.db")

    c = con.cursor()

    c.execute('''DELETE FROM medicentreNames;''')

    df.to_sql("medicentreNames", con=con, if_exists='append', index=True)


if __name__ == "__main__":
    getMedicentreWaitTimes()