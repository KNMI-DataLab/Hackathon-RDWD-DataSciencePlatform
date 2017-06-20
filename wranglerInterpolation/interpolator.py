import numpy as np
from scipy import interpolate
import pandas as pd
import pymysql
import json
import matplotlib.pyplot as plt



#data is a pandas data frame containing latitude, longitude and value (of the observation)
#B-Spline interpolation are used to calculate the required point (https://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.bisplrep.html#scipy.interpolate.bisplrep)
def interpolateAWS(data, newPoint):

    X = data["latitude"].values
    Y = data["longitude"].values
    value = data["value"].values
    m =len(X)
    #number in [(m - sqrt(2 * m) , m + sqrt(2 * m))]
    tck = interpolate.bisplrep(x=X, y=Y, z=value, s=m)
    Xnew = newPoint[0]
    Ynew = newPoint[1]
    znew = interpolate.bisplev(x=Xnew, y=Ynew, tck=tck)
    #print(znew)
    return(znew)






def initDBConn():
    with open("../dbConnect/configDB.json") as data_file:
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

    queryString = "SELECT * FROM stations WHERE type_id=2"

    df = pd.read_sql(queryString, db)


    # Use all the SQL you like
    #cur.execute(queryString)

    # print all the first cell of all the rows
    #for row in cur.fetchall():
      #  print row


    #psql.frame_query()

    #df = DataFrame(cur.fetchall())
    #df.columns = cur.keys()

    db.close()

    return(df)





#Example


import plotly



#Example:
df = exampleQueryStations()
df = df.rename(columns={"elevation": 'value'})

print(df)

newPoint = [51.4422, 3.59611]

newz=interpolateAWS(df, newPoint)

df = df.append({"code" : -99999, "name": "NEWPOINT", "type_id": -99999, "latitude": newPoint[0], "longitude": newPoint[1], "value": newz
                         }, ignore_index = True)

print(df)



scl = [ [0,"rgb(5, 10, 172)"],[0.35,"rgb(40, 60, 190)"],[0.5,"rgb(70, 100, 245)"],\
    [0.6,"rgb(90, 120, 245)"],[0.7,"rgb(106, 137, 247)"],[1,"rgb(220, 220, 220)"] ]



data = [ dict(
        type = 'scattergeo',
        lon = df['longitude'],
        lat = df['latitude'],
        #text = df['text'],
        mode = 'markers',
        marker = dict(
            size = 8,
            opacity = 0.8,
            reversescale = True,
            autocolorscale = False,
            symbol = 'square',
            line = dict(
                width=1,
                color='rgba(102, 102, 102)'
            ),
            colorscale = scl,
            cmin = 0,
            color = df['value'],
            cmax = df['value'].max(),
            colorbar=dict(
                title="Altitude"
            )
        ))]

fig = dict( data=data)
plotly.offline.plot( fig, validate=False)




