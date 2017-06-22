#!/usr/bin/python
#***********************************************************************
#*       wrangleCSV_NCDF.py     Science Platform Project              **
#***********************************************************************
#*                      All rights reserved                           **
#*                    Copyright (c) 2017 KNMI                         **
#*             Royal Netherlands Meteorological Institute             **
#***********************************************************************
#* Demo-script : wrangleCSV_NCDF.py
#* Purpose     : On the basis of CSV (geo-locations and time) 
#*               extract from NetCDF data meteorological information
#* 
#* Usage      :  
#*
#* Project    : Science Platform Project
#*
#*
#* Developers:  MikoKNMI
#* Initial date:  20170612
#**********************************************************************
'''
[sciencePlatform/wrangler] >

TEST-JOB:
python wrangleCSV_NCDF.py --inputCSV ./data/ExportOngevalsData100lines.csv --metaCSV ./data/metaDataCsv.json --jobDesc ./data/jobDesc.json --outputCSV ./output/meteoDataAdded.csv --limitTo 10
meld data/ExportOngevalsData10lines.csv output/meteoDataAdded.csv

FULL-JOB:
python wrangleCSV_NCDF.py --inputCSV ./data/ExportOngevalsData.csv --metaCSV ./data/metaDataCsv.json --jobDesc ./data/jobDesc.json --outputCSV ./output/meteoDataAdded.csv --limitTo 10

PREVIEW-JOB:
python wrangleCSV_NCDF.py --inputCSV ./data/ExportOngevalsData.csv --metaCSV ./data/metaDataCsv.json --jobDesc ./data/jobDesc.json --outputCSV ./output/meteoDataAdded.csv 

'''
#from __future__ import print_function
import sys, os, os.path, string, datetime, re
import csvTooling as csvT
import logging
from csvTooling import printProgress 
import jsonTooling as jst
import ncdfTooling as ncdft
import os, sys, glob
from optparse import OptionParser
import pprint
import numpy as np


if "WPS_DATAPATH" in os.environ.keys():
    DATAPATH = os.environ['WPS_DATAPATH']
else:
    DATAPATH = "./data/"

thisApp = os.path.basename(sys.argv[0])
csvT.loggerName = thisApp[:]
homeDir = os.path.expanduser("~")

defaultDataDir = DATAPATH

# This is a temporary filename for the logfile;
# We will use ${outputCSV}.log instead.
csvT.defaultLogFile = "./wranglerProcess.log" 
csvT.logFileName = csvT.defaultLogFile[:]
#csvT.loglevel = logging.DEBUG


class dataWranglerProcessor():
    '''
    dwp.Initialize( { "inputCSV":options.inputCSV, "metaCSV": options.metaCSV, "jobDesc": options.jobDesc,
                      "outputCSV":options.outputCSV, "limitTo": options.limitTo 
                    } )
    dwp.ReadInputCSV()
    dwp.WrangleWithNetCdfData()
    
    '''
    

    def VerboseOn(self):
        self.verbose=True
    def VerboseOff(self):
        self.verbose=False

    def SetVerboseLevel(self, verboseLevel):
        self.verboseLevel = verboseLevel

    def GetClassName(self):
        return self.__class__.__name__


    def CLASSPRINT(self,*args):
        '''
        Printing debug information ...
        '''
        if self.verbose:
            try:
                pid = "%5d" %(os.getpid())
            except:
                pid = "%5d" %(0)
            try:
                printProgress( "[%s:%s] %s.%s() %s" %(pid,csvT.loggerName,self.__class__.__name__,inspect.stack()[1][3], ''.join(args)) )
            except:
                printProgress( "[%s:%s] %s.%s() %s" %(pid,csvT.loggerName,self.__class__.__name__,inspect.stack()[1][3], ''.join(map(str,args))  ) )

    def __init__(self):
        '''
        Constructor:
        '''
        self.verbose = True
        self.verboseLevel = 0
        self.initialized = False

        self.progressPercent    = 0
        self.progressTotalItems = 100
        self.progressItemCounter = 0
        self.progressBatchSize   = 1 # Number of items in processing batches that represent 1% of the job;
                                     # This triggers the status-update.
        self.statusCallback = None
        

    def __del__(self):
        pass

    def Initialize(self, argsDict):
        '''
        dwp.Initialize( { "inputCSV":options.inputCSV, "metaCSV": options.metaCSV, "jobDesc": options.jobDesc,
                          "logFile":options.outputCSV+".log",        
                          "limitTo": options.limitTo, "scanOnly": True
                          "verboseLevel" : 10  // 0,1,10 
                    } )
        OPTIONAL parameters:  limitTo, verboseLevel
        '''

        if not "logFile" in argsDict:
            printProgress("ERROR: logFile must be provided! ")
            raise ValueError('MISSING-input-file(s)')
        else:
            self.logFileName = argsDict["logFile"]
        
        csvT.logFileName = self.logFileName
        csvT.InitializeWranglerLogger(csvT.logFileName)
        
        if not "inputCSV" in argsDict:
            printProgress("ERROR: inputCSV must be provided! ")
            raise ValueError('MISSING-input-file(s)')
        else:
            self.inputCSV = argsDict["inputCSV"]

        if not "metaCSV" in argsDict:
            printProgress("ERROR: metaCSV must be provided! ")
            raise ValueError('MISSING-input-file(s)')
        else:
            self.metaCSV = argsDict["metaCSV"]

        if not "jobDesc" in argsDict and not ("scanOnly" in argsDict and argsDict["scanOnly"]):
            printProgress("ERROR: jobDesc must be provided! ")
            raise ValueError('MISSING-input-file(s)')
        elif "jobDesc" in argsDict and not ("scanOnly" in argsDict and argsDict["scanOnly"]):
            self.jobDesc = argsDict["jobDesc"]
            self.jobDescDict = jst.ReadJsonConfigurationFromFile(self.jobDesc)
            self.scanOnly = False
        elif ("scanOnly" in argsDict and argsDict["scanOnly"]):
            self.jobDesc = None
            self.jobDescDict = None
            self.scanOnly = True

        if not "limitTo" in argsDict:  # limitTo: OPTIONAL parameter
            self.limitTo = -1 # Does not apply limit; process the whole csv dataset
        else:
            self.limitTo = argsDict["limitTo"]

        if not os.path.exists(self.inputCSV):
            printProgress("ERROR: inputCSV does NOT exists! %s " %(self.inputCSV))
            raise ValueError('MISSING-input-file(s)')

        if "statusCallback" in argsDict:
            self.statusCallback = argsDict["statusCallback"]
            
        self.csvDataObj = csvT.csvDataObject()

        if  "verboseLevel" in argsDict:
            self.SetVerboseLevel( argsDict["verboseLevel"] )
            self.csvDataObj.SetVerboseLevel( argsDict["verboseLevel"] )
        
        self.csvDataObj.SetInputCSVFile(self.inputCSV)
        self.csvDataObj.SetInputMetaCSVFile(self.metaCSV)
        if not self.scanOnly: self.csvDataObj.SetJobDescriptionFile(self.jobDesc)
        if not os.path.exists("./output"):
            os.makedirs("./output")
        self.csvDataObj.ApplyLimit(self.limitTo)
        self.initialized = True
        
    def ReadInputCSV(self):
        if not self.initialized:
            printProgress("ERROR: Initialize first!")
            return
        self.csvDataObj.ReadInputCSV()
        
    def GetTimeRangeOfData(self):
        '''
        Return json: {  "minDateTime": "2006-01-03 00:10:00 UTC",
                        "maxDateTime": "2006-01-30 08:57:00 UTC" }
        Note: The actual range is compute in function: ReadInputCSV()
        '''
        if not self.initialized:
            printProgress("ERROR: Initialize first!")
            return
        return self.csvDataObj.GetTimeRangeOfData()

    def GetLatLonBBOXOfData(self):
        '''
        Return json: {  "west": 4.0161296310787549,
                        "east": 6.8340902374793098,
                        "north": 50.304600877017236,
                        "south": 48.4668502279617
                     }
        Note: The actual range is compute in function: ReadInputCSV()
        '''
        if not self.initialized:
            printProgress("ERROR: Initialize first!")
            return
        return self.csvDataObj.GetLatLonBBOXOfData()

    def GetProjectionString(self):
        return self.csvDataObj.GetProjectionString()

    def callStatusCallback(self, message, percentComplete=0):
        if self.statusCallback: self.statusCallback(message, percentComplete)

    def GetValueFromNetCdfDataSource_time_lon_lat(self, dataSource, arrayName, givenDateTime, lon, lat):
        ndo = dataSource["ndo"]
        # MANUAL EXAMPLE of WRANGLING:
        # 1) Identify the closest time 
        closestDateTimeIndex = ndo.FindClosestDateTimeIndex(givenDateTime)
        # TODO: What if the request is outside the timerange ??
        # ndo.minDateTime, ndo.maxDateTime, ndo.deltaTime
        
        # 2) Find the closest grid-point of the data
        minDistanceDataIndex = ndo.FindClosestLonLatPointIndex(lon, lat)
        # TODO: What if the request is outside the bouding-box of this netCDF datasource ??
        # ndo.llbox_west,ndo.llbox_east,ndo.llbox_north,ndo.llbox_south 
        
        # 3) Get the data the closest-time and the closest grid-point
        dataValue = ndo.GetDataAtIndex(closestDateTimeIndex, minDistanceDataIndex, variableName=arrayName)
        return dataValue
        
    def WrangleWithNetCdfDataArray(self, dataSource, arrayName, recordsProcessed):
        # self.csvDataObj.queryDataNPAdtsLL: 2-dimensional numpy array [  [id, utc-time, longitude, latitude ], .. ] 
        valueList = []
        for timeLonLat in self.csvDataObj.queryDataNPAdtsLL:
            print timeLonLat, timeLonLat
            idn     = timeLonLat[0]
            utcTime = timeLonLat[1]
            lon     = timeLonLat[2]
            lat     = timeLonLat[3]
            value = self.GetValueFromNetCdfDataSource_time_lon_lat(dataSource, arrayName, utcTime, lon, lat)
            valueList.append(value)
            recordsProcessed += 1
            if recordsProcessed % 10 == 0:
                if self.limitTo > 0: total = self.limitTo
                else: total = self.csvDataObj.GetTotalNumberOfCSVrows()
                self.callStatusCallback("Calculating. %d of %d records processed." % (recordsProcessed, total),
                                        10.0 / float(total) * 100 )
        valueArray = np.array(valueList)
        return valueArray
        
    def WrangleWithNetCdfData(self, argsDict):
        '''
           dwp.WrangleWithNetCdfData( { "outputCSV":options.outputCSV } )
        '''
        
        if not self.initialized:
            printProgress("ERROR: Initialize first!")
            return
        
        if not "outputCSV" in argsDict:
            printProgress("ERROR: outputCSV must be provided! ")
            raise ValueError('MISSING-input-file(s)')
        else:
            self.outputCSV = argsDict["outputCSV"]

        self.csvDataObj.SetOutputCSVFile(self.outputCSV)
        
        printProgress("*******************************************")
        printProgress("***** Wrangling Processing STARTED.  ******")
        printProgress("*******************************************")

        tmpFileName = "./tempFile.csv"
        self.csvDataObj.WriteFullQueryDataToTmpFile(tmpFileName)
        '''
        # Do everything at once:
        self.csvDataObj.ReadFullQueryDataFromTmpFile(tmpFileName, startAtRow = 0, readRows=-1)
        self.csvDataObj.WrangleMeteoParameter(parameterName = "temperature")
        self.csvDataObj.WrangleMeteoParameter(parameterName = "precipitation")
        self.csvDataObj.ProduceOutput(exportLonLat = True)
        '''
        '''      
        {"datatowrangle":
         [ 
           {
             "dataURL": "http://opendap.knmi.nl/knmi/thredds/dodsC/DATALAB/hackathon/radarFull2015.nc",
        #    r"http://opendap.knmi.nl/knmi/thredds/dodsC/DATALAB/hackathon/radarFull2015.nc"
        #     "/visdataATX/hackathon/radarFullWholeData.nc"     
             "fields": ["precipitation_amount"]
           }
        ]
        }
        '''
        for dataSource in self.jobDescDict["datatowrangle"]:
            url = dataSource["dataURL"]
            ndo = ncdft.ncdfDataObject()
            ndo.SetDataURL(url)
            ndo.OpenMetaData()
            dataSource["ndo"] = ndo
        
        self.totalNumberOfCSVrows = self.csvDataObj.GetTotalNumberOfCSVrows()
        self.nproc = 1
        #self.percentFraction = 0.01  # 0.01% of 150.000 rows => 10000 files of 15 rows in each file
        #self.percentFraction = 0.1  # 0.1% of 150.000 rows => 1000 files of 150 rows in each file
        self.percentFraction = 1  # 1% of 150.000 rows => 100 files of 1500 rows in each file
        self.percentParts = int( 100/self.percentFraction )
        self.processingBulkSize = self.totalNumberOfCSVrows / self.percentParts    # number of rows representing 1% (0.1%,0.01%) of total

        if self.limitTo>0 and self.limitTo < self.processingBulkSize:
            self.processingBulkSize = self.limitTo
            
        # split temporary request data into #nr bulks
        bulkNr = 0
        rowsProcessed = 0
        tempFileList = []
        parameterList = ["utc-time","longitude","latitude"]
        while rowsProcessed<self.totalNumberOfCSVrows:
            self.csvDataObj.ReadFullQueryDataFromTmpFile(tmpFileName, startAtRow = rowsProcessed, readRows=self.processingBulkSize)

            for dataSource in self.jobDescDict["datatowrangle"]:
                for parameterName in dataSource["fields"]:
                    self.csvDataObj.meteoDataStore[parameterName] = self.WrangleWithNetCdfDataArray(dataSource, parameterName, rowsProcessed)
                    parameterList.append(parameterName)

            #self.csvDataObj.WrangleMeteoParameter(parameterName = "temperature")
            #self.csvDataObj.WrangleMeteoParameter(parameterName = "precipitation")
            tmpBulkFileName = "./tempBulkOutputFile%d.csv"%(bulkNr)
            tempFileList.append(tmpBulkFileName)
            self.csvDataObj.ProduceBulkOutput(tmpBulkFileName, bulkNr, startAtRow = rowsProcessed, readRows=self.processingBulkSize, exportLonLat = True)
            rowsProcessed +=self.processingBulkSize
            bulkNr += 1
            #if self.limitTo > 0:
            #    self.callStatusCallback("Calculating. %d of %d records processed" % (rowsProcessed, self.limitTo),
            #                            (float(self.processingBulkSize) / float(self.limitTo)) * 100.0)
            #else:
            #    self.callStatusCallback("Calculating. %d of %d records processed" % (rowsProcessed, self.totalNumberOfCSVrows),
            #                            self.percentFraction)
            if self.limitTo>0 and rowsProcessed >= self.limitTo:
                break
        
        self.csvDataObj.WriteCSVHeader(fieldList = parameterList )
        self.csvDataObj.JoinBulkResults(tempFileList)
        
        printProgress("*******************************************")
        printProgress("***** Wrangling Processing FINISHED. ******")
        printProgress("*******************************************")
        
    #printProgress("##### Processing time: %s  #####" %(datetimestr))        
    #printProgress("##### FINISHED time: %s  #####" %(datetimestr))        

# GLOBAL instance:
dwp = dataWranglerProcessor( )


if __name__ == "__main__":
    
    # Specify command line syntax and read/interpret command line.
    parser = OptionParser(usage="%prog [options] ",
                          epilog="")

    parser.add_option("--inputCSV", dest="inputCSV", metavar="STRING", default="",
                      help="Input CSV data of the user of the platform.")

    parser.add_option("--metaCSV", dest="metaCSV", metavar="STRING", default="",
                      help="Metadata describing the input CSV data in JSON format.")
    
    parser.add_option("--jobDesc", dest="jobDesc", metavar="STRING", default="",
                      help="Job describtion in JSON format.")

    parser.add_option("--outputCSV", dest="outputCSV", metavar="STRING", default="",
                      help="Output CSV data.")

    parser.add_option("--limitTo", dest="limitTo", metavar='N', type=int, default=-1,
                      help="Used to quickly wrangler just a few lines of the user input.")

    parser.add_option("-o", "--logfile", dest="logfile", metavar="STRING", default=csvT.defaultLogFile,
                      help="The path and name of the file used by this program for its logging output.")
                      
    parser.add_option("-l", "--verboseLevel", dest="verboseLevel", metavar='N', type=int, default=0,
                      help="The level of logging used by this program.")
                      
    parser.add_option("--scanOnly", dest="scanOnly", action="store_true", default=False,
                      help="Perform SCAN-ONLY action on CSV data.")
                                            
    (options, args) = parser.parse_args()

        
    csvT.logFileName = options.logfile[:]
    
    try:
        dwp.Initialize( { "inputCSV":options.inputCSV, "metaCSV": options.metaCSV,
                          "jobDesc": options.jobDesc, "logFile":options.outputCSV+".log", 
                          #"logFile": csvT.logFileName,                          
                          "limitTo": options.limitTo, "verboseLevel": options.verboseLevel,
                          "scanOnly": options.scanOnly
                        } )    
        dwp.ReadInputCSV()
        if not options.scanOnly:
            dwp.WrangleWithNetCdfData( { "outputCSV":options.outputCSV } )

        #Possible exceptions raised:
        #raise ValueError('JSON-INVALID')
        #raise ValueError('MISSING-input-file(s)')    
    except ValueError as err:
        printProgress("Catched exception:"+str(err.args))
        sys.exit(1)

    sys.exit(0)
