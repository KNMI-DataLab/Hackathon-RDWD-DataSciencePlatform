'''
This is ONLY for testing the mySQL-db connection.

The real processing class is implemented in:
wps/wrangler/mySQLTooling.py

It uses DB credentials as defined in file: dbConnect/configDB.json
!!! THIS file is NOT part of the repository !!!

'''

import pymysql
import json
import pprint
import numpy as np


def initDBConn():
    with open("configDB.json") as data_file:
        configData = json.load(data_file)

    user = configData['user']
    pwd = configData['pw']
    host = configData['host']
    port = configData['port']
    db = configData['db']

    db = pymysql.connect(host=host,  # your host, usually localhost
                         user=user,  # your username
                         passwd=pwd,  # your password
                         db=db)  # name of the data base

    # you must create a Cursor object. It will let
    #  you execute all the queries you need
    return(db)


def exampleQueryStations():
    db = initDBConn()
    cur = db.cursor()
    # Use all the SQL you like
    cur.execute("SELECT * FROM stations")
    data=[]
    for row in cur.fetchall():
        data.append(row)
    db.close()
    return data

stationsList = exampleQueryStations()
stationsList
len(stationsList)


def queryElements():
    db = initDBConn()
    cur = db.cursor()    
    queryStr = "SELECT * FROM elements;"
    print queryStr
    cur.execute(queryStr)
    dataDict = {}
    for row in cur.fetchall():
        key = row[1]
        dataDict[key] = row
    db.close()
    return dataDict

elementsDict = queryElements()
pprint.pprint(elementsDict, width=200)

'''
*** TABLE ('elements',) ****
{'N': (3, 'N', 'Cloud cover', 'Cloud cover', 1.0, 'oktas'),
 'RD': (2, 'RD', 'Precipitation', 'Precipitation manual rain gauge 8-8', 0.1, 'mm'),
 'RH': (1, 'RH', 'Precipitation', 'Precipitation AWS', 0.1, 'mm'),
 'RR': (7, 'RR', 'Precipitation', 'Precipitation derived from radar', 0.1, 'mm'),
 'T': (4, 'T', 'Temperature', 'Air temperature', 0.1, 'degrees C'),
 'TN6': (5, 'TN6', 'Minimum temperature', 'Minimum air temperature over last 6 hours', 0.1, 'degrees C'),
 'TX6': (6, 'TX6', 'Maximum temperature', 'Maximum air temperature over last 6 hours', 0.1, 'degrees C'),
 'T_DRYB_10': (8, 'T_DRYB_10', 'Temperature', 'Air temperature (10-minute)', 0.1, 'degrees C')}
'''

#"1hour_validated_rh" .. contains data already
#'1hour_validated_t' .. this should be filled

def queryValues(stationCode, elementName, elementId, limitTo):
    db = initDBConn()
    cur = db.cursor()    
    #queryStr1 = "SELECT * FROM %s a limit 100;" %(valueName)
    #queryStr1 = "SELECT * FROM %s a limit %d;" %(valueName, limitTo)

    queryStr1 = "SELECT c.name, c.code, c.type_id, c.latitude, c.longitude, a.data_id, a.date, a.value, a.qc, d.element \
    FROM %s a, series b, stations c, elements d, types e \
    WHERE c.type_id = 2 and c.type_id = e.type_id and a.data_id = b.data_id and b.code = c.code and \
    a.date > '2015-01-01 10:30:00' and a.date < '2015-01-01 12:30:00' and \
    b.type_id = c.type_id and d.element_id = b.element_id and \
    d.element = '%s' and e.type = 'H' limit %d;" %(elementName, elementId, limitTo)
    #d.element = 'rh' and e.type = 'H' limit %d;" %(valueName,stationCode, limitTo)
    #a.date > date('2015-01-01 10:30:00') and a.date < date('2015-01-01 12:30:00') and \

    
    queryStr = "SELECT c.name, c.code, c.type_id, c.latitude, c.longitude, a.data_id, a.date, a.value, a.qc, d.element \
    FROM %s a, series b, stations c, elements d, types e \
    WHERE c.type_id = 2 and c.type_id = e.type_id and a.data_id = b.data_id and b.code = c.code and \
    b.type_id = c.type_id and d.element_id = b.element_id and c.code = %d and \
    d.element = '%s' and e.type = 'H' limit %d;" %(elementName, stationCode, elementId, limitTo)
    #d.element = 'rh' and e.type = 'H' limit %d;" %(valueName,stationCode, limitTo)

    queryStr0 = "SELECT c.name, c.code, e.type, c.latitude, c.longitude, a.data_id, a.date, a.value, a.qc \
    FROM 1hour_validated_rh a, series b, stations c, elements d, types e \
    WHERE c.type_id = e.type_id and a.data_id = b.data_id and b.code = c.code and \
    b.type_id = c.type_id and d.element_id = b.element_id and c.code = 260 and \
    d.element = 'rh' and e.type = 'H' limit 100;"
   
    print queryStr1
    cur.execute(queryStr1)
    data=[]
    for row in cur.fetchall():
        data.append(list(row))

    db.close()
    dataNP = np.array(data)
    return dataNP 

## (1, 'RH', 'Precipitation', 'Precipitation AWS', 0.1, 'mm')
## elementsDict['RH'][4] ==  0.1 ... scaling
## elementsDict['RH'][5]  .. units 

precipitationDataDeBILT = queryValues(260, '1hour_validated_rh', 'RH', 100)
pprint.pprint(list(precipitationDataDeBILT), width=200)

tempDataDeBILT = queryValues(260, '10min_validated_t', 'T_DRYB_10', 100)
pprint.pprint(list(tempDataDeBILT), width=200)

tempDataDeBILT = queryValues(260, '10min_validated_t', 'T_DRYB_10', 100000000)
pprint.pprint(list(tempDataDeBILT), width=200)

tempDataDeBILT[:,6]
Out[167]: 
array([datetime.datetime(2006, 1, 1, 0, 10),
       datetime.datetime(2006, 1, 1, 0, 20),
       datetime.datetime(2006, 1, 1, 0, 30), ...,
       datetime.datetime(2017, 5, 31, 23, 40),
       datetime.datetime(2017, 5, 31, 23, 50),
       datetime.datetime(2017, 6, 1, 0, 0)], dtype=object)

'''
for t in tables:
    try:
        tableDataSampleDeBILT = queryValues(260, t ,100)
        print t,"****"
        print tableDataSampleDeBILT
    except:
        pass
'''

def queryStationsLonLat():
    db = initDBConn()
    cur = db.cursor()
    cur.execute("SELECT c.name, c.code, e.type, c.latitude, c.longitude \
    FROM 1hour_validated_rh a, series b, stations c, elements d, types e \
    WHERE c.code = 260 \
    limit 100;")
    data=[]
    for row in cur.fetchall():
        data.append(row)

    db.close()
    return data
    
stationsData = queryStationsLonLat()


def queryAutomaticStationsLonLat():
    db = initDBConn()
    cur = db.cursor()
    cur.execute("SELECT c.name, c.code, c.type_id, c.latitude, c.longitude \
    FROM stations c \
    WHERE c.type_id = 2\
    limit 1000;")
    data=[]
    for row in cur.fetchall():
        data.append(row)

    db.close()
    return data
    
autoStationsData = queryAutomaticStationsLonLat()
autoStationsData
len(autoStationsData)

def queryAllStationsLonLat():
    db = initDBConn()
    cur = db.cursor()
    cur.execute("SELECT c.name, c.code, c.type_id, c.latitude, c.longitude \
    FROM stations c \
    limit 1000;")
    data=[]
    for row in cur.fetchall():
        data.append(row)

    db.close()
    return data
    
stationsData = queryAllStationsLonLat()
stationsData
len(stationsData)  # 593

def queryStationsTypes():
    db = initDBConn()
    cur = db.cursor()
    cur.execute("SELECT * \
    FROM types e \
    limit 1000;")
    data=[]
    for row in cur.fetchall():
        data.append(row)
    db.close()
    return data
    
stationsTypes = queryStationsTypes()
stationsTypes

'''
[(1, 'N', 'Manual rain gauge (8-8)'),
 (2, 'H', 'Automatic Weather Station'),
 (3, 'V', 'Visibility station'),
 (4, 'G', 'Border station'),
 (5, 'Z', 'Sunshine duration station'),
 (6, 'Q', 'Radiation station'),
 (7, 'W', 'Wind station'),
 (8, 'D', 'Other North Sea station'),
 (9, 'A_a', 'Pressure and weather station'),
 (10, 'R_a', 'Precipition station'),
 (11, 'S_a', 'Solar radiation/sunshine duration station'),
 (12, 'T_a', 'Temperature station'),
 (13, 'W_a', 'Wind station'),
 (14, 'C_a', 'Cloud cover station'),
 (15, 'B', 'Ground measurement station'),
 (16, 'Rad', 'Radar derived station')]

'''

def queryTables():
    db = initDBConn()
    cur = db.cursor()
    cur.execute("SHOW TABLES;")
    data=[]
    for row in cur.fetchall():
        data.append(row)
    db.close()
    return data
    
tables = queryTables()
tables
'''
Out[50]: 
[('10min_validated_t',),
 ('1day_derived_n',),
 ('1day_derived_rd',),
 ('1day_derived_rh',),
 ('1day_derived_t',),
 ('1day_derived_tn6',),
 ('1day_derived_tx6',),
 ('1hour_validated_n',),
 ('1hour_validated_rh',),
 ('1hour_validated_t',),
 ('1hour_validated_tn6',),
 ('1hour_validated_tx6',),
 ('elements',),
 ('month_derived_n',),
 ('month_derived_rd',),
 ('month_derived_rh',),
 ('month_derived_t',),
 ('month_derived_tn6',),
 ('month_derived_tx6',),
 ('nearby_stations',),
 ('quality',),
 ('season_derived_n',),
 ('season_derived_rd',),
 ('season_derived_rh',),
 ('season_derived_t',),
 ('season_derived_tn6',),
 ('season_derived_tx6',),
 ('series',),
 ('series_derived',),
 ('stations',),
 ('types',),
 ('year_derived_n',),
 ('year_derived_rd',),
 ('year_derived_rh',),
 ('year_derived_t',),
 ('year_derived_tn6',),
 ('year_derived_tx6',)]
'''
