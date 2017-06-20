import numpy as np
from scipy import interpolate
import pandas as pd
import matplotlib.pyplot as plt

import sys
sys.path.insert(0, "/usr/people/pagani/development/innovationWeeks/Hackathon-RDWD-DataSciencePlatform/dbConnect")
from dbConnector import *





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



import plotly







#from mpl_toolkits.basemap import Basemap

#map = Basemap(projection='merc', lat_0=50, lon_0=5,
           #   resolution = 'h', area_thresh = 0.1, llcrnrlon = 3,
            #  llcrnrlat = 48, urcrnrlon = 8, urcrnrlat = 57)

#map.drawcoastlines()
#map.drawcountries()
#map.fillcontinents()
#map.drawmapboundary()


#Example:
df = exampleQueryStations()
df = df.rename(columns={"elevation": 'value'})

print(df)

newPoint = [51.4422, 3.59611]

newz=interpolateAWS(df, newPoint)

#lons = df["longitude"].tolist()
#lats = df["latitude"].tolist()
#vals = df["value"].tolist()
#x,y = map(lons, lats)
#map.scatter(x,y, c=vals)
#plt.scatter(df["longitude"],df["latitude"],c=df["value"])
#plt.scatter(newPoint[1], newPoint[0], c= newz)
#plt.colorbar()
#plt.show()

#df.append()

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
