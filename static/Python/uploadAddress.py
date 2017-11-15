#upload the addresses to the database

import pandas as pd
import sqlite3

con = sqlite3.connect("/Users/usmanr/PycharmProjects/waitTimesDB/waitTimes.db")

c = con.cursor()

def uploadMedicentreAddresses():
    df = pd.read_csv('../data/medicentreAddresses.csv')

    df_ = pd.read_sql("""SELECT * FROM medicentreNames""", con=con)

    id = dict(zip(df_.Name, df_.ID))

    df['ID'] = df['Name'].map(id)

    c.execute('''DELETE FROM medicentreAddresses;''')

    df[['ID', 'lat', 'lon']].to_sql('medicentreAddresses', con=con, index=False, if_exists='append')

def uploadHospitalAddresses():
    df = pd.read_csv('../data/hospitalAddress.csv')

    df_ = pd.read_sql("""SELECT * FROM hospitalNames""", con=con)

    id = dict(zip(df_.Name, df_.ID))

    df['ID'] = df['Name'].map(id)

    c.execute('''DELETE FROM hospitalAddresses;''')

    df[['ID', 'lat', 'lon']].to_sql('hospitalAddresses', con=con, if_exists='append', index=False)

def uploadOtherWalkInClinicAddresses():
    df = pd.read_csv('../data/otherWalkInClinicsAddresses.csv')

    df_ = pd.read_sql("""SELECT * FROM otherWalkInClinicsNames""", con=con)

    id = dict(zip(df_.Name, df_.ID))

    df['ID'] = df['Name'].map(id)

    c.execute('''DELETE FROM otherWalkInClinicsAddresses;''')

    df[['ID', 'lat', 'lon']].to_sql('otherWalkInClinicsAddresses', con=con, if_exists='append', index=False)


uploadMedicentreAddresses()
uploadHospitalAddresses()
uploadOtherWalkInClinicAddresses()