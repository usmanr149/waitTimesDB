from bs4 import BeautifulSoup
from urllib.request import urlopen
import pandas as pd
import sqlite3

#This code was run to get the open, close, break times for all the walk-in clinics
import json, re
from pprint import pprint
import pandas as pd

with open('../data/clinicLocationTimes.json') as data_file:
    data = json.load(data_file)


df = pd.DataFrame(columns=['Name'])

for k,v in data.items():
    if 'Medicentre' not in k:
        df = df.append(pd.DataFrame([[k]], columns=['Name']), ignore_index=True)

df.index.name = 'ID'
print(df)

con = sqlite3.connect("/Users/usmanr/PycharmProjects/waitTimesDB/waitTimes.db")

c = con.cursor()

c.execute('''DELETE FROM otherWalkInClinicsNames;''')

df.to_sql("otherWalkInClinicsNames", con=con, if_exists='append',  index=True)
