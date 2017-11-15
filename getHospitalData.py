from bs4 import BeautifulSoup
from urllib.request import urlopen
import pandas as pd
import sqlite3
import requests, json, re, time
import datetime

#import geocoder
import pprint


con = sqlite3.connect("/Users/usmanr/PycharmProjects/waitTimesDB/waitTimes.db")

c = con.cursor()

#This function hits the AHS website and scrapes the data their and uploads it to the SQL database
def getHospitalWaitTimes():
    # fix hospital name
    def fixValue(key):
        s = [i.strip() for i in key.splitlines() if len(i.strip()) > 0]
        return s

    url = "http://www12.albertahealthservices.ca/repacPublic/SnapShotController?direct=displayEdmonton"
    page = urlopen(url).read()
    soup = BeautifulSoup(page, "lxml")
    #pp = pprint.PrettyPrinter(indent=4)
    #pp.pprint(soup)
    now_time = soup.find_all("div", class_="publicRepacDate")[0].findAll(text=True)[1].replace("\n", "").replace("\r",
                                                                                                                 "").replace(
        " ", "")

    df_ = pd.read_sql("""SELECT ID,Name FROM hospitalNames""", con=con)

    hospitalID = dict(zip(df_.Name, df_.ID))

    hospital_wait_times = {}
    for hit in soup.find_all("tr"):
        if len(hit.find_all("td", class_="publicRepacSiteCell")) > 0:
            time = ''
            # print(hit.find("td", class_="publicRepacSiteCell").text)
            for img in hit.find_all("img", attrs={"alt": True}):
                #print(img.get("alt"))
                time += img.get("alt")
            name = fixValue(hit.find("td", class_="publicRepacSiteCell").text)[0]
            hospital_wait_times[hospitalID[name]] = time[:2] + ":" + time[2:]

    #add the time when the hospital waitTimes were lat updated to the dictionary
    hospital_wait_times[hospitalID['lastUpdate']] = now_time

    df = pd.DataFrame(list(hospital_wait_times.items()), columns=['ID', 'waitTime'])

    c.execute('''DELETE FROM hospitalWaitTimes;''')

    df.to_sql("hospitalWaitTimes", con=con, if_exists='append', index=False)

#This function hits the medicentre website and scrapes the data and updates the database
def getMedicentreWaitTimes(url= "https://www.medicentres.com/clinic-locations/"):

    df_ = pd.read_sql("""SELECT * FROM medicentreNames""", con=con)
    medicentreID = dict(zip(df_.Name, df_.ID))

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

    df = pd.DataFrame(columns=['ID', 'waitTime', 'lastUpdated'])

    for elem in soup.select('div.col-sm-12.medicentre'):
        if "Edmonton," in elem.find(class_='address').get_text() \
                or 'Sherwood Park' in elem.find(class_='address').get_text() \
                or 'St. Albert' in elem.find(class_='address').get_text():
            name = elem.find('a').get_text()
            s1 = re.findall(r'\d+', elem.find(class_='waittime').get_text())

            #This means that the wait time is below an hour
            if len(s1) == 3:
                if 'mins' not in elem.find(class_='waittime').get_text():
                    waitTimes = str("%02d" % (int(s1[0]),)) + ":00"
                else:
                    waitTimes = "00:" + str("%02d" % (int(s1[0]),))
                lastUpdated = datetime.datetime.now().strftime("%Y-%m-%d") + " " + re.findall(r'\d{1,2}:\d{1,2} (?:am|pm)', elem.find(class_='waittime').get_text())[0]
                lastUpdated = int(datetime.datetime.strptime(lastUpdated, '%Y-%m-%d %I:%M %p').strftime("%s"))
            elif len(s1) == 4:
                waitTimes = str("%02d" % (int(s1[0]),)) + ":" + str("%02d" % (int(s1[1]),))
                lastUpdated = datetime.datetime.now().strftime("%Y-%m-%d") + " " + re.findall(r'\d{1,2}:\d{1,2} (?:am|pm)', elem.find(class_='waittime').get_text())[0]
                lastUpdated = int(datetime.datetime.strptime(lastUpdated, '%Y-%m-%d %I:%M %p').strftime("%s"))
            else:
                waitTimes = ""
                lastUpdated = 'Clinic Closed'

            df = df.append(pd.DataFrame([[medicentreID[name], waitTimes, lastUpdated]], columns=['ID', 'waitTime', 'lastUpdated']))

    c.execute('''DELETE FROM medicentreWaitTimes;''')
    #print(df)

    df.to_sql("medicentreWaitTimes", con=con, if_exists='append', index=False)


if __name__=='__main__':
    getMedicentreWaitTimes()
    i = 0
    while True:
        try:
            getHospitalWaitTimes()
            getMedicentreWaitTimes()
        except:
            pass
        print(i)
        i+=1
        time.sleep(100)

