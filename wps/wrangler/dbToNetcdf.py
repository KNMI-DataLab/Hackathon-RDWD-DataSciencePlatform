import mySQLTooling
import pandas as pd



import netCDF4 #import Dataset
ncFile = netCDF4.Dataset("test.nc", "w", format="NETCDF4")




MDB = mySQLTooling.mySqlDataObject()
MDB.SetDatabaseConfigurationFile("dbConnect/configDB.json")
stationsDB = MDB.QueryAutomaticStationsLonLat()

numStationsDB = len(stationsDB)
station = ncFile.createDimension("station", numStationsDB)
time = ncFile.createDimension("time", None)



times = ncFile.createVariable("time","f8",("time",))
latitudes = ncFile.createVariable("lat","f4",("station",))
longitudes = ncFile.createVariable("lon","f4",("station",))
stations = ncFile.createVariable("station", str, ("station",))
stationsID = ncFile.createVariable("id","u4",("station",))
temperatures = ncFile.createVariable("temperature", "f4", ("time","station",))


stationsID[:] = stationsDB[:,1]
lats = stationsDB[:,3]
lons = stationsDB[:,2]
latitudes[:] = lats
longitudes[:] = lons
stationArray =stationsDB[:,0]
stations[:] = stationsDB[:,0]



print("station", stations[:])
print("latitude", latitudes[:])
print("longitude", longitudes[:])
print("stationsID", stationsID[:])


stats= stationsDB[:,1]
dictStations = dict(enumerate(stats.flatten(),0))

reversedStations = dict(zip(dictStations.values(),dictStations.keys()))


timeInterval = pd.date_range("2006-01-01 00:00", "2016-01-01 00:00", freq="10min")

test = timeInterval.map(lambda x: netCDF4.date2num(x,"seconds since 1950-01-01 00:00:00"))

times[:] = test[:]


for stationNumber in stats:

    arrayVal = reversedStations[str(stationNumber)]

    print("gathering values of station"+ str(stationNumber))



    temperatureDateTimesDeBILT = MDB.QueryValues(int(stationNumber), '10min_validated_t', 'T_DRYB_10', getValues=True, getStationName=False, getLonLat=False, timeRange=("",""), limitTo=-1)

    if len(temperatureDateTimesDeBILT)==0:
        continue

    tempDF = pd.DataFrame(temperatureDateTimesDeBILT)
    tempDF.columns = ["dateTime","temperature"]
    tempDF["dateTime"] = pd.to_datetime(tempDF["dateTime"])




    timeDF = pd.DataFrame(pd.to_datetime(timeInterval))
    timeDF.columns = ["dateTime"]



    merged = timeDF.merge(tempDF, on="dateTime", how="left")


    arrayTempVals = merged["temperature"].values*0.1

    #temperatures[arrayVal:] =

    temperatures[:,arrayVal] = arrayTempVals[:]



    #print(merged)
    #print("pippo")


ncFile.close()

    #lat = rootgrp.createDimension("lat", 73)
    #lon = rootgrp.createDimension("lon", 144)