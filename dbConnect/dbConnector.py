import pymysql
import json


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

    # print all the first cell of all the rows
    for row in cur.fetchall():
        print row

    db.close()




def queryValues():
    db = initDBConn()

    cur = db.cursor()

    # Use all the SQL you like
    cur.execute("SELECT c.name, c.code, e.type, c.latitude, c.longitude, a.data_id, a.date, a.value, a.qc FROM 1hour_validated_rh a, series b, stations c, elements d, types e WHERE c.type_id = e.type_id and a.data_id = b.data_id and b.code = c.code and b.type_id = c.type_id and d.element_id = b.element_id and c.code = 260 and d.element = 'rh' and e.type = 'H' limit 100;")

    for row in cur.fetchall():
        print row

    db.close()




##likely rain gauges stations
#exampleQueryStations()
#AWS stations values
queryValues()