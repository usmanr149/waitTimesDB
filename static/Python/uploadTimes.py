#upload the addresses to the database

import pandas as pd
import sqlite3

con = sqlite3.connect("/Users/usmanr/PycharmProjects/waitTimesDB/waitTimes.db")

c = con.cursor()

def uploadMedicentreTimes():
    df = pd.read_csv('../data/medicentreTimes.csv')

    df_ = pd.read_sql("""SELECT * FROM medicentreNames""", con=con)

    id = dict(zip(df_.Name, df_.ID))

    df['ID'] = df['Name'].map(id)
    print(df)
    df = df.astype(object).where(pd.notnull(df), None)

    c.execute('''DELETE FROM medicentreTimes;''')

    dow = {"Monday": 0, "Tuesday": 1, "Wednesday": 2, "Thursday": 3, "Friday": 4, "Saturday": 5, "Sunday": 6,
           "Holiday": 7}

    df['DayofWeek'] = df['DayofWeek'].map(dow)
    print(df)

    df[['ID', 'DayofWeek', 'Open', 'Close', 'breakOpen', 'breakClose']].to_sql('medicentreTimes', con=con, index=False, if_exists='append')

def fixTime(x):
    if isinstance(x, str):
        return float(x.split(":")[0]) + float(x.split(":")[1])/60
    else:
        return None

def uploadOtherWalkInClinicsTimes():
    df = pd.read_csv('../data/otherWalkInClinicsTimes.csv')

    df['Open'] = df['Open'].map(lambda x: fixTime(x))
    df['Close'] = df['Close'].map(lambda x: fixTime(x))
    df['breakOpen'] = df['breakOpen'].map(lambda x: fixTime(x))
    df['breakClose'] = df['breakClose'].map(lambda x: fixTime(x))

    df = df.astype(object).where(pd.notnull(df), None)

    df_ = pd.read_sql("""SELECT * FROM otherWalkInClinicsNames""", con=con)

    id = dict(zip(df_.Name, df_.ID))

    df['ID'] = df['Name'].map(id)

    dow = {"Monday": 0, "Tuesday": 1, "Wednesday": 2, "Thursday": 3, "Friday": 4, "Saturday": 5, "Sunday": 6, "Holiday": 7}

    df['DayofWeek'] = df['DayofWeek'].map(dow)

    print(df)

    c.execute('''DELETE FROM otherWalkInClinicsTimes;''')

    df[['ID', 'DayofWeek', 'Open', 'Close', 'breakOpen', 'breakClose']].to_sql('otherWalkInClinicsTimes', con=con, index=False, if_exists='append')

uploadMedicentreTimes()
uploadOtherWalkInClinicsTimes()