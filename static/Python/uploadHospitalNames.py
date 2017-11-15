from bs4 import BeautifulSoup
from urllib.request import urlopen
import pandas as pd
import sqlite3
import requests, json, re, time
import datetime

#update the hospital names in the database
def getHospitalNames():
    # fix hospital name
    def fixValue(key):
        s = [i.strip() for i in key.splitlines() if len(i.strip()) > 0]
        return s

    url = "http://www12.albertahealthservices.ca/repacPublic/SnapShotController?direct=displayEdmonton"
    page = urlopen(url).read()
    soup = BeautifulSoup(page, "lxml")
    #pp = pprint.PrettyPrinter(indent=4)
    #pp.pprint(soup)

    df = pd.DataFrame([['lastUpdate', ""]],columns=['Name', 'extraInformation'])
    for hit in soup.find_all("tr"):
        if len(hit.find_all("td", class_="publicRepacSiteCell")) > 0:

            time = ''
            name = fixValue(hit.find("td", class_="publicRepacSiteCell").text)
            while len(name) < 2:
                name.append("")
            df = df.append(pd.DataFrame([[name[0], name[1]]], columns=['Name', 'extraInformation']), ignore_index=True)

    df.index.name = 'ID'
    print(df)

    con = sqlite3.connect("/Users/usmanr/PycharmProjects/waitTimesDB/waitTimes.db")

    c = con.cursor()

    c.execute('''DELETE FROM hospitalNames;''')

    df.to_sql("hospitalNames", con=con, if_exists='append',  index=True)

    return df



if __name__ == "__main__":
    getHospitalNames()