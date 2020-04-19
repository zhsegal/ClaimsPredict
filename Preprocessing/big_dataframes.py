import pandas as pd
import sqlite3

pd.set_option('display.max_columns',320)
pd.set_option('display.width',320)

data=pd.read_csv('Data/DE1_0_2010_Beneficiary_Summary_File_Sample_1.csv', nrows=100)
data=data[['DESYNPUF_ID','BENE_BIRTH_DT','SP_ALZHDMTA','BENE_DEATH_DT']]
#data2=pd.read_csv('Data/DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.csv', nrows=100)


conn = sqlite3.connect('DB/TestDB.db')  # You can create a new database by changing the name within the quotes
c = conn.cursor() # The database will be saved in the location where your 'py' file is saved

c.execute('''DROP TABLE CLIENTS''')

c.execute('''CREATE TABLE CLIENTS
              ([DESYNPUF_ID] text,[BENE_BIRTH_DT] text, [SP_ALZHDMTA] integer, [BENE_DEATH_DT] date)''')

# c.execute('''CREATE TABLE COUNTRY
#              ([generated_id] INTEGER PRIMARY KEY,[Country_ID] integer, [Country_Name] text)''')
#
# # Create table - DAILY_STATUS
# c.execute('''CREATE TABLE DAILY_STATUS
#              ([Client_Name] text, [Country_Name] text, [Date] date)''')

conn.commit()

data.to_sql('CLIENTS', conn, if_exists='append', index = False) # Insert the values from the csv file into the table 'CLIENTS'
c.execute('''
SELECT DISTINCT *
FROM CLIENTS

          ''')

print(c.fetchall())

print ("dog")