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
import vtk
import csvTooling as csvT
import logging
from csvTooling import printProgress 

import os, sys, glob
from optparse import OptionParser
import pprint


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
        
        

    def __del__(self):
        pass

    def Initialize(self, argsDict):
        '''
        dwp.Initialize( { "inputCSV":options.inputCSV, "metaCSV": options.metaCSV, "jobDesc": options.jobDesc,
                      "outputCSV":options.outputCSV, "limitTo": options.limitTo 
                    } )
        limitTo: OPTIONAL parameter                    
        '''
        if not "outputCSV" in argsDict:
            printProgress("ERROR: outputCSV must be provided! ")
            raise ValueError('MISSING-input-file(s)')
        else:
            self.outputCSV = argsDict["outputCSV"]

        # We will use ${outputCSV}.log instead.
        csvT.logFileName = self.outputCSV + ".log"
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

        if not "jobDesc" in argsDict:
            printProgress("ERROR: jobDesc must be provided! ")
            raise ValueError('MISSING-input-file(s)')
        else:
            self.jobDesc = argsDict["jobDesc"]

        if not "limitTo" in argsDict:  # limitTo: OPTIONAL parameter
            self.limitTo = -1 # Does not apply limit; process the whole csv dataset
        else:
            self.limitTo = argsDict["limitTo"]

        if not os.path.exists(self.inputCSV):
            printProgress("ERROR: inputCSV does NOT exists! %s " %(self.inputCSV))
            raise ValueError('MISSING-input-file(s)')
            
        self.csvDataObj = csvT.csvDataObject()
        self.csvDataObj.SetInputCSVFile(self.inputCSV)
        self.csvDataObj.SetInputMetaCSVFile(self.metaCSV)
        self.csvDataObj.SetJobDescriptionFile(self.jobDesc)
        if not os.path.exists("./output"):
            os.makedirs("./output")
        self.csvDataObj.SetOutputCSVFile(self.outputCSV)
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
        
        
    def WrangleWithNetCdfData(self):
        if not self.initialized:
            printProgress("ERROR: Initialize first!")
            return
        
        printProgress("*******************************************")
        printProgress("***** Wrangling Processing STARTED.  ******")
        printProgress("*******************************************")
        
        self.csvDataObj.WrangleMeteoParameter(parameterName = "temperature")
        self.csvDataObj.WrangleMeteoParameter(parameterName = "precipitation")
        self.csvDataObj.ProduceOutput(exportLonLat = True)
        
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
                      
    parser.add_option("-l", "--loglevel", dest="loglevel", metavar="STRING", default="info",
                      help="The level of logging used by this program.")
                                            
    (options, args) = parser.parse_args()

    # Translate the logger settings from the command line and apply them.
    if options.loglevel == "debug":
        #print "logging.DEBUG"
        loglevel = logging.DEBUG
    elif options.loglevel == "info":
        loglevel = logging.INFO
    elif options.loglevel == "warn":
        loglevel = logging.WARN
    elif options.loglevel == "error":
        loglevel = logging.ERROR
    elif options.loglevel == "fatal":
        loglevel = logging.FATAL
        
    csvT.logFileName = options.logfile[:]
    

    try:
        dwp.Initialize( { "inputCSV":options.inputCSV, "metaCSV": options.metaCSV, "jobDesc": options.jobDesc,
                          "outputCSV":options.outputCSV, "limitTo": options.limitTo 
                        } )
        dwp.csvDataObj.SetVerboseLevel(10)
        dwp.ReadInputCSV()
        dwp.WrangleWithNetCdfData()

        #Possible exceptions raised:
        #raise ValueError('JSON-INVALID')
        #raise ValueError('MISSING-input-file(s)')    
    except ValueError as err:
        printProgress("Catched exception:"+str(err.args))
        sys.exit(1)

    sys.exit(0)
    




