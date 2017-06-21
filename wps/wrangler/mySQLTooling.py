import numpy as np
import pytz
from datetime import datetime, timedelta
import sys, os
import time
import pyproj
from pyproj import Geod
import inspect
import csvTooling as csvT
from csvTooling import printProgress 

import pymysql
import json
import pprint
import numpy as np
from dataObjectBase import dataObjectBase

class mySqlDataObject(dataObjectBase):
    def __init__(self):
        '''
        Constructor:
            dataTypeName: string
            storageType : "cache" or "archive"
        '''
        self.verbose = True
        self.verboseLevel = 0       
        self.projFuncDefstring =""
        
    def __del__(self):
        #self.CLASSPRINT('..Destructor()..')
        pass
        
    def SetDatabaseConfigurationFile(self, dbConfig):
        # It uses DB credentials as defined in file: dbConnect/configDB.json
        # For the time-being  "dbConnect/configDB.json"
        # !!! THIS file is NOT part of the repository !!!
        # Install the DB-credential file manually.
        self.dbConfig = dbConfig

    def InitDBConnection(self):
        with open(self.dbConfig) as data_file:
            configData = json.load(data_file)

        user = configData['user']
        pwd = configData['pw']
        host = configData['host']
        port = configData['port']
        dbname = configData['db']

        DB = pymysql.connect(host=host,  # your host, usually localhost
                             user=user,  # your username
                             passwd=pwd,  # your password
                             db=dbname)  # name of the data base

        # you must create a Cursor object. It will let
        #  you execute all the queries you need
        return(DB)


    def QueryAutomaticStationsLonLat(self):
        db = self.InitDBConnection()
        cur = db.cursor()
        #cur.execute("SELECT * FROM stations")
        #cur.execute("SELECT c.name, c.code, c.type_id, c.longitude, c.latitude \
        queryStr = "SELECT c.name, c.code, c.longitude, c.latitude \
        FROM stations c \
        WHERE c.type_id = 2 \
        limit 1000;"
        cur.execute(queryStr)        
        data=[]
        for row in cur.fetchall():
            data.append(row)
        db.close()        
        npData = np.array(data)
        #print npData.shape, npData
        ## SORT ACCORDING THE STATION NAME:
        ## create sorted list of indices. ':' => vertical dimension
        indexSort = np.argsort(npData[:,0].astype(np.str))
        ## SORT ACCORDING THE STATION ID:
        #indexSort = np.argsort(npData[:,1].astype(np.int))        
        self.stationsList = npData[indexSort] ## create sorted 2-dimensional array        
        return self.stationsList
        

    def QueryAllStationsLonLat(self):
        db = self.InitDBConnection()
        cur = db.cursor()
        queryStr = "SELECT c.name, c.code, c.type_id, c.longitude, c.latitude \
        FROM stations c \
        limit 1000;"
        cur.execute(queryStr)       
        data=[]
        for row in cur.fetchall():
            data.append(row)

        db.close()
        return data
        
    def QueryStationsTypes(self):
        db = self.InitDBConnection()
        cur = db.cursor()
        queryStr = "SELECT * \
        FROM types e \
        limit 1000;"
        cur.execute(queryStr)       
        data=[]
        for row in cur.fetchall():
            data.append(row)
        db.close()
        self.stationsTypes = data
        return data
                

    def GetListOfExistingData(self):    
        # So-far the database is not completely filled with data.
        # The following list contatains filled data:
        self.supportedData = { 'Precipitation': ['RH', '1hour_validated_rh'],
                               'Temperature': ['T_DRYB_10', '10min_validated_t'] }

        return self.supportedData.keys()

    def QueryElements(self, useSupportedDataOnly = True):
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
        self.GetListOfExistingData()
        shortKeys = []
        for k in self.supportedData:
            shortKeys.append( self.supportedData[k][0] )
        db = self.InitDBConnection()
        cur = db.cursor()    
        queryStr = "SELECT * FROM elements;"
        cur.execute(queryStr)
        dataDict = {}
        for row in cur.fetchall():
            key = row[1]
            if useSupportedDataOnly:
                if key in shortKeys:
                    # add only known/complete datasets
                    dataDict[key] = row
            else:
                dataDict[key] = row

        db.close()
        self.elementsDict = dataDict
        return dataDict

    def QueryValues(self, stationCode, elementName, elementId, getValues=True, getStationName=False, getLonLat=False, timeRange=("",""), limitTo=-1):
        '''
        ## (1, 'RH', 'Precipitation', 'Precipitation AWS', 0.1, 'mm')
        ## elementsDict['RH'][4] ==  0.1 ... scaling
        ## elementsDict['RH'][5]  .. units 

        precipitationDataDeBILT = self.QueryValues(260, '1hour_validated_rh', 'RH', getValues=True, getStationName=False, getLonLat=False, timeRange=("",""), limitTo=-1)
        pprint.pprint(list(precipitationDataDeBILT), width=200)

        tempDataDeBILT = self.QueryValues(260, '10min_validated_t', 'T_DRYB_10', getValues=True, getStationName=False, getLonLat=False, timeRange=("",""), limitTo=-1)
        pprint.pprint(list(tempDataDeBILT), width=200)
       
        '''
        db = self.InitDBConnection()
        cur = db.cursor()    
        #queryStr1 = "SELECT * FROM %s a limit 100;" %(valueName)
        #queryStr1 = "SELECT * FROM %s a limit %d;" %(valueName, limitTo)
        if limitTo==-1:
            LIMIToption = ""
        else:
            LIMIToption = "limit %d" %(int(limitTo))
            
        if timeRange==("",""):
            timeRangeOption = ""
        else:
            #timeRangeOption=" a.date > '2015-01-01 10:30:00' and a.date < '2015-01-01 12:30:00' and "
            timeRangeOption=" a.date >= '%s' and a.date <= '%s' and " %(timeRange[0],timeRange[1])
            
        if getLonLat:
            getLonLatOption = "c.longitude, c.latitude, "
        else:
            getLonLatOption = ""
            
        if getStationName:
            getStationNameOption = "c.name, "
        else:
            getStationNameOption = ""

        if getValues:
            getValuesOption = ", a.value "
        else:
            getValuesOption = ""
        
        # NOTE: c.type_id = 2 + e.type = 'H' refer to automatic weather-stations 
        #queryStr1 = "SELECT %s c.code, c.type_id, %s a.data_id, a.date, a.value, a.qc, d.element \
        queryStr  = "SELECT %s %s a.date %s \
        FROM %s a, series b, stations c, elements d, types e \
        WHERE %s c.code = %d and c.type_id = 2 and c.type_id = e.type_id and a.data_id = b.data_id and b.code = c.code and \
        b.type_id = c.type_id and d.element_id = b.element_id and \
        d.element = '%s' and e.type = 'H' %s;" %(getStationNameOption, getLonLatOption, getValuesOption, elementName, timeRangeOption, stationCode, elementId,  LIMIToption)
        
        #queryStr1 = "SELECT c.name, c.code, c.type_id, c.latitude, c.longitude, a.data_id, a.date, a.value, a.qc, d.element \
        #FROM %s a, series b, stations c, elements d, types e \
        #WHERE c.type_id = 2 and c.type_id = e.type_id and a.data_id = b.data_id and b.code = c.code and \
        #a.date > '2015-01-01 10:30:00' and a.date < '2015-01-01 12:30:00' and \
        #b.type_id = c.type_id and d.element_id = b.element_id and \
        #d.element = '%s' and e.type = 'H' limit %d;" %(elementName, elementId, limitTo)
        #d.element = 'rh' and e.type = 'H' limit %d;" %(valueName,stationCode, limitTo)
        #a.date > date('2015-01-01 10:30:00') and a.date < date('2015-01-01 12:30:00') and \

        printProgress( "MySQL_query: "+ queryStr )
        cur.execute(queryStr)
        data=[]
        for row in cur.fetchall():
            data.append(list(row))

        db.close()
        dataNP = np.array(data)
        return dataNP 


    def OpenMetaData(self):
        self.CLASSPRINT('reading metadata from mySQL database: mqm_db'  )

        self.CLASSPRINT("*******************************************")
        self.CLASSPRINT("*****    Reading automatic stations  ******")
        self.CLASSPRINT("*******************************************")
        
        self.QueryAutomaticStationsLonLat()
        self.QueryStationsTypes()

        pprint.pprint(self.stationsList, width=200)
        
        self.lonLatStacked = self.stationsList[:,[2,3]]  # the order must be longitude, latitude !
        #pprint.pprint(self.lonLatStacked, width=200)
        self.arraySize = self.lonLatStacked.shape[0]
        
        self.CLASSPRINT("*******************************************")
        self.CLASSPRINT("*****    Reading available data (meta)  ***")
        self.CLASSPRINT("*******************************************")
        
        self.QueryElements(useSupportedDataOnly = True)
        for p in self.elementsDict:
            pprint.pprint("self.elementsDict[%s]:"%p + str(self.elementsDict[p]), width=200)

        self.CLASSPRINT("*******************************************")
        self.CLASSPRINT("*****    Reading time-dimension .... ******")
        self.CLASSPRINT("*******************************************")


        if 0:
            # NOTE: The following reads all the time-indices of the entire file. This may take some times (3-4 seconds).       
            # 260 .. De BILT; We suppose that the De Bilt station has the complete time-range.
            precipitationDateTimesDeBILT = self.QueryValues(260, '1hour_validated_rh', 'RH', getValues=False, getStationName=False, getLonLat=False, timeRange=("",""), limitTo=-1)
            #pprint.pprint(precipitationDateTimesDeBILT, width=200)
            self.dateTimeArray = precipitationDateTimesDeBILT[:,0]
            pprint.pprint(self.dateTimeArray, width=200)
        
        
        temperatureDateTimesDeBILT = self.QueryValues(260, '10min_validated_t', 'T_DRYB_10', getValues=False, getStationName=False, getLonLat=False, timeRange=("",""), limitTo=-1)
        self.dateTimeArray = temperatureDateTimesDeBILT[:,0]
        pprint.pprint(self.dateTimeArray, width=200)

        
        #self.timeUnits = self.metaData.variables['time'].units
        #self.dateTimeArray = ncdf.num2date(self.timeArray,self.timeUnits,calendar='standard')

        #self.timeDim = self.metaData.variables['time'].shape[0]
        
        # The date-time present in the NetCDF is (must be) in UTC
        self.minDateTime = np.min(self.dateTimeArray).replace(tzinfo=pytz.UTC)
        self.maxDateTime = np.max(self.dateTimeArray).replace(tzinfo=pytz.UTC)

        #fmt = '%Y-%m-%d %H:%M:%S %Z (%z)'
        fmt = '%Y-%m-%d %H:%M:%S %Z'
        self.minDateTime_str = self.minDateTime.strftime(fmt)
        self.maxDateTime_str = self.maxDateTime.strftime(fmt)
        #print (self.minDateTime_str,self.maxDateTime_str)
        self.deltaTime = int( (self.dateTimeArray[-1] - self.dateTimeArray[-2]).total_seconds() )  # datetime.timedelta(0, 300)
                
        self.CLASSPRINT("##########################################################")
        self.CLASSPRINT("minDateTime=%s, maxDateTime=%s, deltaTime=%d" %(self.minDateTime_str, self.maxDateTime_str, self.deltaTime)  )
        self.CLASSPRINT("##########################################################")

    def FindClosestLonLatPointIndex(self, lon, lat):
        #  This works with 2D array self.lonLatStacked: [ [lon,lat], ... ]
        printProgress("Computing distance %s: to lat-lon point-array [%d];" %(str((lon, lat)),self.arraySize))
        idx = 0
        distArray = np.zeros(self.lonLatStacked.shape[0]).astype(np.float)
        for tupleLL in self.lonLatStacked:
            # tupleLL == [lon,lat]
            #print "idx=",idx, "tupleLL=",tupleLL
            dist = self.Distance2pointsInLonLat(lon, lat, tupleLL[0], tupleLL[1])
            distArray[idx] = dist
            #printProgress("Compute distance %s: to [%d] %s; dist=%f" %(str((lon, lat)),idx, str(tupleLL),dist))
            idx +=1
        minDist = np.min(distArray)
        minDistIndex = np.where(distArray == minDist)[0][0]  # np.where(distArray == minDist) == gives= => [(array([26451]),)]        
        printProgress("Minimum distance %s: is %f; [%s] = %s" %(str((lon, lat)), minDist, str(minDistIndex), str(self.lonLatStacked[minDistIndex])))
        return minDistIndex

    def GetDataAtIndex(self, stationCode, timeIndex, dataIndex, variableName="precipitation"):
        '''
       
        ''' 
        if dataIndex>self.arraySize:
            self.CLASSPRINT("ERROR: Probing non-existing data!")
            return
            
        variableFound = (variableName in self.GetListOfExistingData())
        self.GetVariable(variableName)
        if variableFound:
            '''
                self.supportedData = { 'Precipitation': ['RH', '1hour_validated_rh'],
                                        'Temperature': ['T_DRYB_10', '10min_validated_t'] }
                self.elementsDict =  {
                 'RH': (1, 'RH', 'Precipitation', 'Precipitation AWS', 0.1, 'mm'),
                 'T_DRYB_10': (8, 'T_DRYB_10', 'Temperature', 'Air temperature (10-minute)', 0.1, 'degrees C')
                 }
                ## (1, 'RH', 'Precipitation', 'Precipitation AWS', 0.1, 'mm')
                ## self.elementsDict['RH'][4] ==  0.1 ... scaling
                ## self.elementsDict['RH'][5]  .. units 
            '''                       
            closestDateTime = self.dateTimeArray[closestDateTimeIndex]
            #print "closestDateTime=", closestDateTime
            elementName=self.supportedData[variableName][1]
            elementId=self.supportedData[variableName][0]
            dataValues = self.QueryValues( stationCode, elementName, elementId, 
                          getValues=True, getStationName=False, getLonLat=False, timeRange=(closestDateTime,closestDateTime), limitTo=-1)


            #precipitationDataDeBILT = self.QueryValues(260, '1hour_validated_rh', 'RH', getValues=True, getStationName=False, getLonLat=False, timeRange=("",""), limitTo=-1)
            #pprint.pprint(list(precipitationDataDeBILT), width=200)
            #
            #tempDataDeBILT = self.QueryValues(260, '10min_validated_t', 'T_DRYB_10', getValues=True, getStationName=False, getLonLat=False, timeRange=("",""), limitTo=-1)
            #pprint.pprint(list(tempDataDeBILT), width=200)
            
            # dataValues == [[datetime.datetime(2010, 7, 14, 16, 30) 193]]

            dataValue = dataValues[0][1] * self.elementsDict[elementId][4]
            #print self.elementsDict
            dataUnits = self.elementsDict[elementId][5]
            
            printProgress("givenDateTime=%s, closestDateTimeIndex=%d, query(lon, lat)=%s, minDistanceDataIndex=%d, dataValue=%f %s"
            %(str(givenDateTime), timeIndex, str((lon, lat)), dataIndex, float(dataValue), dataUnits) )
            return dataValue
        else:
            return None
        

        
if __name__ == "__main__":  # ONLY for testing
   
    thisApp = os.path.basename(sys.argv[0])
    csvT.loggerName = thisApp[:]
    homeDir = os.path.expanduser("~")
    csvT.defaultLogFile = "./wranglerProcess.log" 
    csvT.logFileName = csvT.defaultLogFile[:]
    loggerName = csvT.loggerName
    csvT.InitializeWranglerLogger(csvT.logFileName)
    

    # It uses DB credentials as defined in file: dbConnect/configDB.json
    # !!! THIS file is NOT part of the repository !!!
    # Install this DB-credential file manually.

    MDB = mySqlDataObject()
    MDB.SetDatabaseConfigurationFile("dbConnect/configDB.json")
    MDB.InitDBConnection()
    MDB.OpenMetaData()
    
    # MANUAL EXAMPLE of WRANGLING:
    # 1) Identify the closest time 
    fmt = '%Y-%m-%d %H:%M:%S %Z'
    givenDateTime = datetime.strptime('2010-07-14 16:33:00 UTC',fmt)
    closestDateTimeIndex = MDB.FindClosestDateTimeIndex(givenDateTime)
    closestDateTime = MDB.dateTimeArray[closestDateTimeIndex]
    print "closestDateTime=", closestDateTime
    
    # 2) Find the closest grid-point of the data
    (lon, lat) = (5.2, 52.0)
    minDistanceDataIndex = MDB.FindClosestLonLatPointIndex(lon, lat)
    print "MDB.stationsList[%d]="%(minDistanceDataIndex), MDB.stationsList[minDistanceDataIndex]
    #MDB.stationsList[minDistanceDataIndex]= ['DE BILT' '260' '5.1797' '52.0988']
    stationNumber = int(MDB.stationsList[minDistanceDataIndex][1])
    print "Closest stationNumber=%d" %(stationNumber)
    
    
    # 3) Get the data the closest-time and the closest grid-point
    dataValue = MDB.GetDataAtIndex(stationNumber, closestDateTimeIndex, minDistanceDataIndex, variableName="Temperature")

    
    sys.exit(0)
    
    '''
    When working properly it produces the following output:
    ------------------------------------------------------
    
    givenDateTime=2010-07-14 16:33:00, closestDateTime=2010-07-14 16:30:00, closestDateTimeIndex=238167
    closestDateTime= 2010-07-14 16:30:00
    Computing distance (5.2, 52.0): to lat-lon point-array [135];
    Minimum distance (5.2, 52.0): is 11081.164877; [14] = ['5.1797' '52.0988']
    MDB.stationsList[14]= ['DE BILT' '260' '5.1797' '52.0988']
    Closest stationNumber=260
    MySQL_query: SELECT   a.date , a.value          FROM 10min_validated_t a, series b, stations c, elements d, types e         WHERE  a.date >= '2010-07-14 16:30:00' and a.date <= '2010-07-14 16:30:00' and  c.code = 260 and c.type_id = 2 and c.type_id = e.type_id and a.data_id = b.data_id and b.code = c.code and         b.type_id = c.type_id and d.element_id = b.element_id and         d.element = 'T_DRYB_10' and e.type = 'H' ;
    givenDateTime=2010-07-14 16:33:00, closestDateTimeIndex=238167, query(lon, lat)=(5.2, 52.0), minDistanceDataIndex=14, dataValue=19.300000 degrees C
    '''
   
