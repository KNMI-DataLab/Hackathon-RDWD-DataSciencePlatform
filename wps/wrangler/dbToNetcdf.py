import mySQLTooling
import pandas as pd
import netCDF4



ncFile = netCDF4.Dataset("test.nc", "w", format="NETCDF4")

MDB = mySQLTooling.mySqlDataObject()
MDB.SetDatabaseConfigurationFile("dbConnect/configDB.json")
stationsDB = MDB.QueryAutomaticStationsLonLat()


stationsToLookInto = stationsDB[:,1]
validStations = []
#check the stations that have actually values through the period of interest not very efficient, but for the time being let's live with it
for stationToCheck in stationsToLookInto:
    temperatureDateTimes = MDB.QueryValues(int(stationToCheck), '10min_validated_t', 'T_DRYB_10', getValues=True, getStationName=False, getLonLat=False, timeRange=("",""), limitTo=-1)
    if(len(temperatureDateTimes)!=0):
        validStations.append(stationToCheck)


#putting the 2-dimensional array in a pandas dataframe
stationsDF = pd.DataFrame(stationsDB)
stationsDF.columns = ["stationName", "stationID", "longitude", "latitude"]

#check the station that have values and comparing to the whole station list from DB
stationsAvailableDF = stationsDF[stationsDF['stationID'].isin(validStations)]
stationsAvailableDF.reset_index(drop=True, inplace=True)

#dimensions definition
numStationsDB = stationsAvailableDF.shape[0]
station = ncFile.createDimension("station", numStationsDB)
time = ncFile.createDimension("time", None)


#creation of variables
times = ncFile.createVariable("time","f8",("time",))
latitudes = ncFile.createVariable("lat","f4",("station",))
longitudes = ncFile.createVariable("lon","f4",("station",))
stations = ncFile.createVariable("station", str, ("station",))
stationsID = ncFile.createVariable("id","u4",("station",))
temperatures = ncFile.createVariable("temperature", "f4", ("time","station",),fill_value=-9999)

#setting attributes
ncFile.featureType = "timeSeries"

latitudes.units = "degrees north"
latitudes.standard_name = "latitude"
latitudes.long_name = "station latitude"

longitudes.units = "degrees east"
longitudes.standard_name = "longitude"
longitudes.long_name = "station longitude"

temperatures.units = "degrees Celsius"
temperatures.coordinates = "lat lon"
#temperatures._FillValue = "-9999.f"

times.units = "seconds since 1950-01-01 00:00:00"
times.long_name = "time of measurement"
times.standard_name = "time"

stations.long_name = "station name"
stations.cf_role = "timeseries_id"

stationsID.long_name = "station ID"

#filling variables of stationsID, lat, lon and station names
stationsID[:] = stationsAvailableDF["stationID"].values
lats = stationsAvailableDF["latitude"].values
lons = stationsAvailableDF["longitude"].values
latitudes[:] = lats
longitudes[:] = lons
stationArray =stationsAvailableDF["stationName"].values
stations[:] = stationsAvailableDF["stationName"].values



#print("station", stations[:])
#print("latitude", latitudes[:])
#print("longitude", longitudes[:])
#print("stationsID", stationsID[:])




#setting the whole time interval for the chosen period
timeInterval = pd.date_range("2006-01-01 00:00", "2016-01-01 00:00", freq="10min")
test = timeInterval.map(lambda x: netCDF4.date2num(x,"seconds since 1950-01-01 00:00:00"))
times[:] = test[:]


#gathering temperature data from the available stations in the period
for stationNumber in stationsAvailableDF["stationID"]:

    #value of  the  index of the available station to be used in filling the array for NetCDF
    arrayVal = stationsAvailableDF.loc[stationsAvailableDF["stationID"]==stationNumber].index.values[0]

    print("gathering values of station "+ str(stationNumber))



    temperatureDateTimes = MDB.QueryValues(int(stationNumber), '10min_validated_t', 'T_DRYB_10', getValues=True, getStationName=False, getLonLat=False, timeRange=("",""), limitTo=-1)

    #if len(temperatureDateTimesDeBILT)==0:
        #continue

    #making a dataFrame from the array of the query
    tempDF = pd.DataFrame(temperatureDateTimes)
    tempDF.columns = ["dateTime","temperature"]
    tempDF["dateTime"] = pd.to_datetime(tempDF["dateTime"])

    #making a dataFrame from the time interval
    timeDF = pd.DataFrame(pd.to_datetime(timeInterval))
    timeDF.columns = ["dateTime"]

    #both time columns are changed to datetime, needed to issue the merge


    #merge on time column
    merged = timeDF.merge(tempDF, on="dateTime", how="left")

    merged.fillna(-99990, inplace= True)

    #multiply the values to Celsius
    arrayTempVals = merged["temperature"].values*0.1


    #write the array to the NetCDF variable
    temperatures[:,arrayVal] = arrayTempVals[:]



ncFile.close()