import json
from urllib.request import urlopen
import ssl
import sqlite3

conn=sqlite3.connect('cdcdata.sqlite')
cur=conn.cursor()

cur.execute('''CREATE TABLE IF NOT EXISTS CState (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
state_id INTEGER, State TEXT, Count INTEGER)''')
cur.execute('''CREATE TABLE IF NOT EXISTS CDate(id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
date_id INTEGER, state_id INTEGER,  Date INTEGER, Count INTEGER)''')
cur.execute('''CREATE TABLE State_COIVD_Numbers (State TEXT, state_id INTEGER, date_id INTEGER, count INTEGER)''')
# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

data = open('rows.json', encoding='utf8').read()
info = json.loads(data)

for data in info['meta']['view']['columns']:
     data1 = data['position']
     if data1 != 1:
         continue

     date_data = data['cachedContents']['top']
     cur.execute("SELECT Date FROM CDate WHERE Count= ?",(memoryview(date_data.encode()), ))
     #print(date_data)

for data in info['meta']['view']['columns']:
     data1 = data['position']
     if data1 != 2:
         continue
     state_data = data['cachedContents']['top']
     cur.execute("SELECT State FROM CState WHERE Count= ?",(memoryview(state_data.encode()), ))
    # print(state_data)

cur.execute('''INSERT INTO CState(State, Count) VALUES(?,?)'''(memoryview(state_data.encode())))
cur.execute('''INSERT INTO CDate(Date, Count) VALUES(?,?)'''(memoryview(date_data.encode())))
cur.execute('''select CState.State, CDate.Date FROM CState join CDate on State_COVID_Numbers.state_id=state.id''' )
cur.execute('''select CDate.Date, CState.State FROM CDate join CState on State_COVID_Numbers.date_id=date.id''' )
Print('You Did It!')
