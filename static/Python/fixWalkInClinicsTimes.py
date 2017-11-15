#This code was run to get the open, close, break times for all the walk-in clinics
import json, re
from pprint import pprint
import pandas as pd

df =pd.read_csv("../data/otherWalkInClinicsTimes.csv")

dow = {"Monday": 0, "Tuesday": 1, "Wednesday": 2, "Thursday": 3, "Friday": 4, "Saturday": 5, "Sunday": 6}

df['DayofWeek'] = df['DayofWeek'].map(dow)

#df2 = pd.read_csv('../data/times.csv')
#df2['breakOpen'] = ''
#df2['breakClose'] = ''

#df = df.append(df2)

#df.to_csv('times.csv', index=False)

print(df)