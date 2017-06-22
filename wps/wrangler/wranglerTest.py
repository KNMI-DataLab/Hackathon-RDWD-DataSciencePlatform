import pandas as pd
import pyproj

import urllib2

import json
import pprint
import numpy as np
import datetime





def round_to_5min(t):
    delta = datetime.timedelta(minutes=t.minute%5,
                               seconds=t.second,
                               microseconds=t.microsecond)
    t -= delta
    if delta > datetime.timedelta(minutes=2):
        t += datetime.timedelta(minutes=5)
    return t





projectionString = "+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889 +k=0.9999079 +x_0=155000 +y_0=463000 +ellps=bessel +units=m +no_defs"

#projectionStringADAGUC = "+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889 +k=0.9999079 +x_0=155000 +y_0=463000 +ellps=bessel +towgs84=565.4171,50.3319,465.5524,-0.398957388243134,0.343987817378283,-1.87740163998045,4.0725 +units=m +no_defs"

csvWrangledDF = pd.DataFrame.from_csv("./output/meteoDataAddedFullValidate1000.csv", index_col=None)





wgs84 = pyproj.Proj("+init=EPSG:4326")
epsg28992 = pyproj.Proj(projectionString)

#epsg28992 = pyproj.Proj("+init=EPSG:28992")



    ##lists are filled with values of the data frame
[lon, lat] = pyproj.transform(epsg28992, wgs84, csvWrangledDF["X"].values, csvWrangledDF["Y"].values)




testDF = csvWrangledDF

testDF["testLON"] = lon
testDF["testLAT"] = lat

#times = testDF["utc-time"].values


testDF["dateTimes"]=pd.to_datetime(testDF["utc-datetime"], format="%Y-%m-%d %H:%M:%S")

testDF['5minTimes'] = testDF["dateTimes"].map(round_to_5min)


times = testDF["5minTimes"].values



lats = testDF["latitude"].values
lons = testDF["longitude"].values


lats = testDF["testLAT"].values
lons = testDF["testLON"].values



#http://geoservices.knmi.nl/cgi-bin/RADNL_OPER_R___25PCPRR_L3.cgi?&SERVICE=WMS&REQUEST=GetPointValue&VERSION=1.3.0&LAYERS=RADNL_OPER_R___25PCPRR_L3_COLOR&QUERY_LAYERS=RADNL_OPER_R___25PCPRR_L3_COLOR&CRS=EPSG%3A4326&WIDTH=1345&HEIGHT=984&I=776&J=513&FORMAT=image/gif&INFO_FORMAT=application/json&STYLES=&&time=2017-06-22T09%3A50%3A00Z&X=1.75&Y=53.10


#http://bhw485.knmi.nl:8080/cgi-bin/autoresource.cgi?source=%2Fpagani%2FradarFull2006.nc&&SERVICE=WMS&REQUEST=GetFeatureInfo&VERSION=1.3.0&LAYERS=image1_image_data&QUERY_LAYERS=image1_image_data&CRS=EPSG%3A3857&BBOX=153444.00891130202,6616039.123753257,1056801.9761203022,7203967.14729654&WIDTH=1515&HEIGHT=986&I=695&J=719&FORMAT=image/gif&INFO_FORMAT=text/html&STYLES=&&time=2006-12-31T23%3A50%3A00Z




#http://bhw485.knmi.nl:8080/cgi-bin/autoresource.cgi?source=%2Fpagani%2FradarFull2006.nc&&SERVICE=WMS&REQUEST=GetPointValue&VERSION=1.3.0&LAYERS=image1_image_data&QUERY_LAYERS=image1_image_data&CRS=EPSG%3A4326&BBOX=49.10069871572277,-0.09743112999999999,55.56858228427723,9.84054413&WIDTH=1515&HEIGHT=986&I=903&J=514&FORMAT=image/gif&INFO_FORMAT=text/html&STYLES=&&time=2006-12-31T23%3A50%3A00Z&X=5.83&Y=52.20

lonStr = lons.astype("str")
latStr = lats.astype("str")
#timeStr = times[0]
#timeStr = times.map(lambda x: x.replace(" ", "T"))

#replacer = lambda t: t.replace(" ", "T")
#vfunc = np.vectorize(replacer)
#timeStr = vfunc(times)

timeStr = testDF["5minTimes"].dt.strftime('%Y-%m-%dT%H:%M:%S').values

lengthCSV = testDF.shape[0]


radarValues = []


for i in range(0,lengthCSV):

    lon = lonStr[i]
    lat = latStr[i]
    time = timeStr[i]

    url = "http://bhw485.knmi.nl:8080/cgi-bin/autoresource.cgi?source=%2Fpagani%2FradarFull2006.nc&&SERVICE=WMS&REQUEST=GetPointValue&VERSION=1.3.0&LAYERS=image1_image_data&QUERY_LAYERS=image1_image_data&CRS=EPSG%3A4326&BBOX=49.10069871572277,-0.09743112999999999,55.56858228427723,9.84054413&WIDTH=1515&HEIGHT=986&I=903&J=514&FORMAT=image/gif&INFO_FORMAT=application/json&STYLES=&&time="+time+"&X="+lon+"&Y="+lat




    resp = urllib2.urlopen(url).read()

#print(resp)


    jsonObj = json.loads(resp)

    value = jsonObj[0]["data"].values()
    if (value[0] == "nodata"):
        value[0] = np.nan
    radarValues.append(value[0])
    print(value[0])


testDF["refValues"] = radarValues
#pprint.pprint(jsonObj)

testDF['refValues'] = testDF['refValues'].astype(float)

#testDF['same'] = np.where(testDF["refValues"]==testDF["precipitation_amount"],True, False )

roundingFactor = 6

testDF['same'] = (testDF['refValues'].dropna().round(roundingFactor) == testDF['precipitation_amount'].dropna().round(6))

#print(testDF[testDF["refValues"].isnull()] == testDF[testDF["precipitation_amount"].isnull()])


equal = (testDF["precipitation_amount"].isnull() == testDF["refValues"].isnull()).all()and(testDF["precipitation_amount"][testDF["precipitation_amount"].notnull()].round(roundingFactor) == testDF["refValues"][testDF["refValues"].notnull()].round(roundingFactor)).all()


if equal ==True:
    print("The data sets are equal to the "+str(roundingFactor) + "th decimal place")
else:
    print("The data sets are not equal")



#print("pippo")