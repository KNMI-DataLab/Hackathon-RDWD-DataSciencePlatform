import pymysql
import json


def initDBConn():
    with open("configDB.json") as data_file:
        configData = json.load(data_file)

    user = configData['user']
    pwd = configData['pw']
    host = configData['host']
    port = configData['port']


    db = pymysql.connect(host=host,  # your host, usually localhost
                         user=user,  # your username
                         passwd=pwd,  # your password
                         db="mqm_db")  # name of the data base

    # you must create a Cursor object. It will let
    #  you execute all the queries you need
    cur = db.cursor()

    # Use all the SQL you like
    cur.execute("SELECT * FROM stations")

    # print all the first cell of all the rows
    for row in cur.fetchall():
        print row[0]

    db.close()



    return()


initDBConn()