#!/usr/bin/env python

#from __future__ import print_function
import sys, os, os.path, glob, string, re
import inspect
import math

import numpy as np
import pyproj
from pyproj import Geod
import csv
from datetime import datetime,timedelta
import pytz 
import pprint
import jsonTooling as jst
import os, sys

#from numpy import *
#from numpy import ma
#import scipy as Sci
#import scipy.linalg


logFileName =  "/tmp/wranglerProcess.log"
logFile = None

wranglerLogger = None
loggerName = "abcd"

def InitializeWranglerLogger(filename=logFileName, level = 0):
    '''
    '''
    try:
        if os.path.exists(filename):
            os.remove(filename)
    except:
        print >> sys.stderr, "WARNING: Could not remove file: %s" %(filename)
    global logFile
    logFile=open(logFileName, 'w+')         
    print >> sys.stderr, "WARNING: Could not remove file: %s" %(filename)

def printProgress(infoStr):
    print >> sys.stderr, infoStr
    if logFile:
        logFile.write(infoStr+"\n")


geoTransfWGS84 = Geod(ellps='WGS84')
geoTransf = geoTransfWGS84

class csvDataObject():

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
                printProgress( "[%s:%s] %s.%s() %s" %(pid,loggerName,self.__class__.__name__,inspect.stack()[1][3], ''.join(args)) )
            except:
                printProgress( "[%s:%s] %s.%s() %s" %(pid,loggerName,self.__class__.__name__,inspect.stack()[1][3], ''.join(map(str,args))  ) )

    def __init__(self):
        '''
        Constructor:
            dataTypeName: string
            storageType : "cache" or "archive"
        '''
        self.verbose = True
        self.verboseLevel = 0
        self.rescaleFactor = 1.0
        self.inputCSVfile = ""
        self.metaCSVfile = ""
        self.jobDescfile = ""
        self.outputCSVfile = ""
        self.limitTo = -1
        self.delimiter = ","
        self.meteoDataStore = {}
        
        # There are situation wheren the hour and/or minute is NOT specified.
        # This can be automaticaly resolved.
        # Set the below variables:
        self.autoResolve_hour = -1
        self.autoResolve_minute = -1
        
        if self.verboseLevel>100:
            printProgress("Path at terminal when executing this file")
            printProgress(os.getcwd() + "\n")
            printProgress("This file path, relative to os.getcwd()")
            printProgress(__file__ + "\n")
            printProgress("This file full path (following symlinks)")
            full_path = os.path.realpath(__file__)
            printProgress(full_path + "\n")
            printProgress("This file directory and name")
            path, file = os.path.split(full_path)
            printProgress(path + ' --> ' + file + "\n")
            printProgress("This file directory only")
            printProgress(os.path.dirname(full_path))        

    def __del__(self):
        #self.CLASSPRINT('..Destructor()..')
        pass

    def checkFile(self, fname, infoStr):
        if os.path.exists(fname):
            return fname
        else:
            self.CLASSPRINT(' ERROR! (%s) File not exist: %s' %(infoStr, fname))
            raise ValueError('MISSING-input-file(s)')
    
    def SetInputCSVFile(self,inputCSV):
        self.inputCSVfile = self.checkFile(inputCSV,"inputCSV")
        
    def SetInputMetaCSVFile(self,metaCSV):
        self.metaCSVfile = self.checkFile(metaCSV,"metaCSV") 
        
    def SetJobDescriptionFile(self,jobDesc):
        self.jobDescfile = self.checkFile(jobDesc,"jobDesc") 
        
    def SetOutputCSVFile(self,outputCSV):
        self.outputCSVfile = outputCSV
        
    def ApplyLimit(self,limitTo):
        self.limitTo = limitTo

    '''
array([['03JAN06', '1.00-01.59', '10', '111998', '516711'],
       ['07JAN06', '7.00-07.59', '50', '95069', '451430'],
       ['21JAN06', '11.00-11.5', '10', '119330.273', '486468.495'],
       ['13JAN06', '12.00-12.5', '45', '91939', '440711'],
       ['09JAN06', '18.00-18.5', '9', '190371.475', '324748.091'],
       ['03JAN06', '8.00-08.59', '15', '258048', '468749'],
       ['12JAN06', '17.00-17.5', '57', '174487', '318218'],
       ['24JAN06', '17.00-17.5', '0', '177750', '319033'],
       ['13JAN06', '22.00-22.5', '49', '158178.588', '380926.549'],
       ['30JAN06', '9.00-09.59', '57', '53498', '407783']], 
      dtype='|S10')
    
    Readings about DST & timezone issues:  
    http://pytz.sourceforge.net/
    
    import pytz 
    fmt = '%Y-%m-%d %H:%M:%S %Z (%z)'
   
    local_tz = pytz.timezone ("Europe/Amsterdam")
    #datetime_without_tz = datetime(2004, 10, 31, 2, 0, 0)
    datetime_without_tz = datetime.strptime("2014-10-31 2:0:0", "%Y-%m-%d %H:%M:%S")
    needToDecide = False
    try:
        datetime_with_tz   = local_tz.localize(datetime_without_tz, is_dst=None) # No daylight saving time
    except pytz.exceptions.AmbiguousTimeError:
        #printProgress('pytz.exceptions.AmbiguousTimeError: %s' % dt)
        needToDecide = True
    except pytz.exceptions.NonExistentTimeError:
        #printProgress('pytz.exceptions.NonExistentTimeError: %s' % dt)        
        needToDecide = True
    if needToDecide:
        datetime_with_tz   = local_tz.localize(datetime_without_tz, is_dst=False) 
    
    datetime_in_utc    = datetime_with_tz.astimezone(pytz.utc)

    print datetime_with_tz.strftime(fmt)
    print datetime_in_utc.strftime(fmt)
         
    loc_dt1 = local_tz.localize(datetime_without_tz, is_dst=True)
    loc_dt2 = local_tz.localize(datetime_without_tz, is_dst=False)
    loc_dt1.strftime(fmt)
    >>> '2004-10-31 02:00:00 CEST (+0200)'
    loc_dt2.strftime(fmt)
    >>> '2004-10-31 02:00:00 CET (+0100)'
    str(loc_dt2 - loc_dt1)
    >>> '1:00:00'

    '''

    def ConvertLocalDateTime2Utc(self,datetime_without_tz, zone):
        fmt = '%Y-%m-%d %H:%M:%S %Z (%z)'
       
        local_tz = pytz.timezone (zone)
        #local_tz = pytz.timezone ("Europe/Amsterdam")
        #datetime_without_tz = datetime(2004, 10, 31, 2, 0, 0)
        #datetime_without_tz = datetime.strptime("2014-10-31 2:0:0", "%Y-%m-%d %H:%M:%S")
        needToDecide = False
        try:
            datetime_with_tz   = local_tz.localize(datetime_without_tz, is_dst=None) # No daylight saving time
        except pytz.exceptions.AmbiguousTimeError:
            #printProgress('pytz.exceptions.AmbiguousTimeError: %s' % dt)
            needToDecide = True
        except pytz.exceptions.NonExistentTimeError:
            #printProgress('pytz.exceptions.NonExistentTimeError: %s' % dt)        
            needToDecide = True
        if needToDecide:
            datetime_with_tz   = local_tz.localize(datetime_without_tz, is_dst=False) 
        
        datetime_in_utc    = datetime_with_tz.astimezone(pytz.utc)

        datetime_with_tz_str = datetime_with_tz.strftime(fmt)
        datetime_in_utc_str  = datetime_in_utc.strftime(fmt)

        return (datetime_in_utc, datetime_in_utc_str, datetime_with_tz_str) 

        
    def DecodeDateTime(self, dateStr, hourStr, minuteStr):
        if self.metaCSVdict['dateFormat'] == "%d%b%y":
            givenDate = datetime.strptime(dateStr, "%d%b%y")
            #analysisTime = datetime.strptime(specs['tag']['starttime'], "%Y%m%d%H")
            
        if self.metaCSVdict['hourFormat'] == "hourInterval":
            try:
                if '-' in hourStr:
                    hour = float(hourStr.split('-')[0])
                else:
                    hour = float(hourStr[:3])
            except:
                if self.autoResolve_hour>0:
                    hour = self.autoResolve_hour
                else:
                    self.CLASSPRINT('WARNING: could not extract hour from: "%s"' %(hourStr) )
                    return None  # this mean INVALID request

            try:
                minute = int(minuteStr)
            except:
                if self.autoResolve_minute>0:
                    minute = self.autoResolve_minute
                else:
                    self.CLASSPRINT('WARNING: could not extract minute from: "%s"' %(minuteStr) )
                    return None  # this mean INVALID request

            
        localTime = givenDate + timedelta(hours=hour, minutes=minute)
        
        (utcTime, datetime_in_utc_str, datetime_with_tz_str)  = self.ConvertLocalDateTime2Utc(localTime, zone=self.metaCSVdict['timeZone'])
        
        if self.verbose and self.verboseLevel>=10:
            self.CLASSPRINT(dateStr, '=>', givenDate,';', hourStr, '=>', hour,';',datetime_with_tz_str,'=>', datetime_in_utc_str)
        
        fmt = '%Y-%m-%d %H:%M:%S %Z'
        utcTimeStr = utcTime.strftime(fmt)
        return utcTimeStr
    
    def ReadInputCSV(self):
        self.CLASSPRINT('reading metaCSVfile: %s' %(self.metaCSVfile) )
        self.metaCSVdict = jst.ReadJsonConfigurationFromFile(self.metaCSVfile)
        if self.verbose:
            self.CLASSPRINT(self.metaCSVdict )
        self.delimiter = self.metaCSVdict['csvSeparator']
        self.columnsList = []
        self.columnsList.append(self.metaCSVdict['columnDate'])
        self.columnsList.append(self.metaCSVdict['columnHour'])
        self.columnsList.append(self.metaCSVdict['columnMinute'])
        self.columnsList.append(self.metaCSVdict['columnX'])
        self.columnsList.append(self.metaCSVdict['columnY'])
        '''
        {
        'dateFormat': 'DDmmmYY', 'hourFormat': 'hourInterval',
        'columnDate': 2, 'columnHour': 1, 'columnMinute': 3, 
        'columnX': 9, 'columnY': 10, 
        'minuteFormat': 'plainMinute', 
        'csvSeparator': ',', 
        'projString': '+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889 +k=0.9999079 +x_0=155000 +y_0=463000 +ellps=bessel +units=m +no_defs', 
        'timeZone': 'CET', 
        'geoProjection': 'rijksDriehoek'
        }
        '''

        self.CLASSPRINT('reading inputCSVfile: %s' %(self.inputCSVfile) )
        self.numHeaderLines = 1
        # read and store header
        self.headerText = ""
        n = self.numHeaderLines
        ftxt = open(self.inputCSVfile,'rU')  # read text file in UNIVERSAL mode to catch also the Windows line-endings CRLF / CR
        while n>0:
            ln = ftxt.readline()
            if not ln:
                break
            self.headerText += ln.rstrip('\n')  
            n -= 1
        ftxt.close()
        if self.verbose:
            self.CLASSPRINT( "headerText=\n%s"  %(self.headerText))
        self.dataUnsortedStr = np.recfromtxt(self.inputCSVfile, skip_header=self.numHeaderLines, comments="#", dtype="|S300",  delimiter=self.delimiter)                      
        if self.limitTo>0:
            self.dataColumns = self.dataUnsortedStr[:self.limitTo, self.columnsList ]
            if self.verbose and self.verboseLevel>=10:
                #pprint.pprintProgress(self.dataColumns) 
                self.CLASSPRINT( "dataColumns:\n", self.dataColumns )
        else:
            self.dataColumns = self.dataUnsortedStr[:, self.columnsList ]
        #dataUnsorted = dataUnsortedStr.astype(dtype=[('date', str), ('time', str), ('id', str), ('lon', float), ('lat', float), ('flightlevel', float) , ('windSpeed', float), ('windDirection', float), ('temp', float), ('flightphase', str)  ])
        #20131028 040029 M83240b 51.3094 1.0316 350.00 120.078 236.942 222.326 0
        self.CLASSPRINT("*******************************************")
        self.CLASSPRINT("***** Decoding date-time format STARTED. **")
        self.CLASSPRINT("*******************************************")
        idCounter = 0
        queryDataArray = []

        #[wps/wrangler] >cat data/ExportOngevalsData.csv | grep Onbekend| wc -l
        #33656
        
        for aa in self.dataColumns:
            utcTimeStr = self.DecodeDateTime(dateStr=aa[0], hourStr=aa[1], minuteStr=aa[2])
            if utcTimeStr==None:  # None means INVALID request!
                dataRow = [ idCounter, "INVALID", float(aa[3]), float(aa[4]) ]  # store [id, utc-time, X-coord, Y-coord ]
                self.CLASSPRINT('WARNING: INVALID date-time specification at row-number: %d; "%s"' %(idCounter, str( aa ) ) )
                queryDataArray.append( dataRow )
            else:
                dataRow = [ idCounter, utcTimeStr, float(aa[3]), float(aa[4]) ]  # store [id, utc-time, X-coord, Y-coord ]
                queryDataArray.append( dataRow )
            idCounter += 1
        queryDataNPA = np.array(queryDataArray)
        #print queryDataArray
        #print "queryDataNPA=\n", queryDataNPA
        # SORT ACCORDING THE UCT-TIME:
        ind = np.lexsort((queryDataNPA[:,1],))
        queryDataNPADateTimeSorted = queryDataNPA[ind]
        #print "queryDataNPADateTimeSorted=\n", queryDataNPADateTimeSorted
        self.minDateTime = queryDataNPADateTimeSorted[0,1]
        # There might be missing/unspecified HOURS in the user-csv data.
        # These are marked as INVALID as lexically sorted appearing at the of the array.
        utcTimeStr_last = queryDataNPADateTimeSorted[-1,1]
        if utcTimeStr_last!="INVALID":
            self.maxDateTime = utcTimeStr_last
        else:
            lastPos = -2
            try:
                utcTimeStr_last = queryDataNPADateTimeSorted[lastPos,1]
                while utcTimeStr_last=="INVALID":
                    lastPos -= 1
                    utcTimeStr_last = queryDataNPADateTimeSorted[lastPos,1]
                self.maxDateTime = utcTimeStr_last
            except:
                self.CLASSPRINT('WARNING: Setting maxDateTime = minDateTime; queryDataNPADateTimeSorted[:,1].shape=', queryDataNPADateTimeSorted[:,1].shape)
                self.maxDateTime = self.minDateTime
                
        self.CLASSPRINT("*******************************************")
        self.CLASSPRINT("***** Decoding date-time format FINISHED.**")
        self.CLASSPRINT("*******************************************")
        
        self.CLASSPRINT("*******************************************")
        self.CLASSPRINT("***** Computing LON-LAT STARTED.     ******")
        self.CLASSPRINT("*******************************************")       
        self.projFuncDefstring = self.metaCSVdict['projString']
        self.projectionFunction = pyproj.Proj(self.projFuncDefstring)
        # [  [id, utc-time, X-coord, Y-coord ], .. ]
        xcoords = queryDataNPADateTimeSorted[:,2]
        ycoords = queryDataNPADateTimeSorted[:,2]
        
        (longitudes,latitudes) = self.UnProject2LongitudeLatitudes(xcoords, ycoords)
        lonLatStacked = np.vstack((longitudes,latitudes)).T
        #print lonLatStacked
        self.llbox_west = np.min(longitudes)
        self.llbox_east = np.max(longitudes)
        self.llbox_north = np.max(latitudes)
        self.llbox_south = np.min(latitudes)
        self.CLASSPRINT("*******************************************")
        self.CLASSPRINT("***** Computing LON-LAT FINISHED.    ******")
        self.CLASSPRINT("*******************************************")
        
        self.CLASSPRINT("##########################################################")
        self.CLASSPRINT("minDateTime=%s, maxDateTime=%s" %(self.minDateTime, self.maxDateTime)  )
        self.CLASSPRINT("LATLON-BBOX (west, east, north, south):", (self.llbox_west,self.llbox_east,self.llbox_north,self.llbox_south) )
        self.CLASSPRINT( "##########################################################")
        #print "queryDataNPADateTimeSorted.shape=", queryDataNPADateTimeSorted[:,0].shape
        #print "longitudes.shape=", longitudes.shape
        self.queryDataNPAdtsLL = np.vstack(( queryDataNPADateTimeSorted[:,0], queryDataNPADateTimeSorted[:,1], 
                                        queryDataNPADateTimeSorted[:,2], queryDataNPADateTimeSorted[:,3], 
                                        longitudes,latitudes)).T
        if self.verbose and self.verboseLevel>=10:
            self.CLASSPRINT("queryDataNPADateTimeSorted:")
            arrayDims = self.queryDataNPAdtsLL.shape
            for i in xrange(arrayDims[0]):
                self.CLASSPRINT(list(self.queryDataNPAdtsLL[i,:]))
        return

    def GetTimeRangeOfData(self):
        '''
        Return json: {  "minDateTime": "2006-01-03 00:10:00 UTC",
                        "maxDateTime": "2006-01-30 08:57:00 UTC" }
        Note: The actual range is compute in function: ReadInputCSV()
        '''
        dataRange =  {  "minDateTime": self.minDateTime,
                        "maxDateTime": self.maxDateTime }
        return dataRange
    
    def GetLatLonBBOXOfData(self):
        '''
        Return json: {  "west": 4.0161296310787549,
                        "east": 6.8340902374793098,
                        "north": 50.304600877017236,
                        "south": 48.4668502279617
                     }
        Note: The actual range is compute in function: ReadInputCSV()
        '''
        LATLONBBOX =  { "west": self.llbox_west,
                        "east": self.llbox_east,
                        "north": self.llbox_north,
                        "south": self.llbox_south
                     }
        return LATLONBBOX
        

    def UnProject2LongitudeLatitudes(self, xcoords, ycoords):
        LL  = self.projectionFunction(xcoords, ycoords,inverse=True)
        longitudes = LL[0]
        latitudes = LL[1]
        return (longitudes,latitudes) # tuple, vector

    def ProjectLongitudeLatitudes(self, longitudes,latitudes):
        XYcoords  = self.projectionFunction(longitudes,latitudes,inverse=False)
        xcoords = XYcoords[0]
        ycoords = XYcoords[1]  
        return (xcoords, ycoords) # tuple, vector

    def ProjectLonLatSinglePoint(self, lon, lat):
        # Usage:
        # (x,y) = ProjectLonLatSinglePoint(52.379293, 4.899645)
        XYcoords  = self.projectionFunction([lon,],[lat,],inverse=False)
        xcoords = XYcoords[0]
        ycoords = XYcoords[1]  
        return (xcoords[0],ycoords[0])
            
    def WrangleMeteoParameter(self, parameterName):
        dims = self.queryDataNPAdtsLL.shape
        #print "self.queryDataNPAdtsLL.shape=",self.queryDataNPAdtsLL.shape
        #self.meteoDataStore[parameterName] = np.array(np.ones(dims[0]))
        if "temp" in parameterName:
            self.meteoDataStore[parameterName] = np.random.uniform(low=273.15, high=280, size=(dims[0],))
        else:
            self.meteoDataStore[parameterName] = np.random.uniform(low=0.0, high=100.0, size=(dims[0],))
        #if self.verbose:
        #    self.CLASSPRINT("WrangleMeteoParameter(%s): meteoDataStore[%s]="%(parameterName,parameterName) ,self.meteoDataStore[parameterName])


    '''
    self.queryDataNPAdtsLL = 
     0      1                            2         3            4             5
     id,  datetime-utc,                 xcoord, ycoord,         lon,        , lat 
    ['0', '2006-01-03 00:10:00 UTC', '111998.0', '516711.0', '4.80027528245', '48.9994215447']
    ['5', '2006-01-03 07:15:00 UTC', '258048.0', '468749.0', '6.83409023748', '50.304600877']
    ['1', '2006-01-07 06:50:00 UTC', '95069.0', '451430.0', '4.57160470779', '48.8458532221']
    ['4', '2006-01-09 17:09:00 UTC', '190371.475', '324748.091', '5.87787666049', '49.7043104226']
    ['6', '2006-01-12 16:57:00 UTC', '174487.0', '318218.0', '5.6569228243', '49.5622693229']
    '''
    def PrintArray(self,arr, arrayName=""):
        dims = arr.shape
        print "%s%s =" %(arrayName, str(list(dims)))
        for i in xrange(dims[0]):
            print list(arr[i])
            
    def PrintArrayJoinedAsString(self,arr, arrayName="", delimiter=","):
        dims = arr.shape
        print "%s%s =" %(arrayName, str(list(dims)))
        for i in xrange(dims[0]):
            print delimiter.join( list(arr[i]) )

            
    def ProduceOutput(self, exportLonLat = True):
        self.CLASSPRINT(' writing outputCSVfile: %s' %(self.outputCSVfile) )
        
        #onlyTesting = True
        onlyTesting = False
        if onlyTesting:
            if self.limitTo>0:
                dataOut = self.dataUnsortedStr[:self.limitTo,1:]
            else:
                dataOut = self.dataUnsortedStr[:,1:]
        else:
            if self.limitTo>0:
                dataOut = self.dataUnsortedStr[:self.limitTo]
            else:
                dataOut = self.dataUnsortedStr
        
        # write the header
        n = self.numHeaderLines
        ftxt = open(self.outputCSVfile,"wt")
        headerTextOutput = self.headerText[:].rstrip('\n')
        
        if exportLonLat:
            headerTextOutput += ",longitude,latitude"
        meteoDataStoreKeys =  self.meteoDataStore.keys()
        for k in meteoDataStoreKeys:
            headerTextOutput += ",%s" %k
            
        ftxt.writelines(headerTextOutput+"\n")
        if self.verbose:
            printProgress(headerTextOutput)
            
        rowId = 0
        # NOTE: The array is sorted according DATE-TIME
        indicesqueryDataNPAdtsLL =  self.queryDataNPAdtsLL[:,0]
        #print "indicesqueryDataNPAdtsLL=", indicesqueryDataNPAdtsLL.astype(np.int)
        
        # SORT ACCORDING THE INDEX-reference (== the original row-order from CSV):
        indexSort = np.lexsort((self.queryDataNPAdtsLL[:,0],))
        queryDataNPAdtsLLIdxSorted = self.queryDataNPAdtsLL[indexSort]
        indicesqueryDataNPAdtsLLIdxSorted =  (queryDataNPAdtsLLIdxSorted[:,0]).astype(np.int)
        #print "indicesqueryDataNPAdtsLLIdxSorted=", indicesqueryDataNPAdtsLLIdxSorted
        self.PrintArray(queryDataNPAdtsLLIdxSorted, arrayName="queryDataNPAdtsLLIdxSorted")
        self.PrintArray(dataOut, arrayName="dataOut")
        #print "dataOut.shape=", dataOut.shape
        
        
        meteoDataStoreNpArray = np.array([])
        
        lonNpArrayIdxSorted = queryDataNPAdtsLLIdxSorted[:,[4]]
        latNpArrayIdxSorted = queryDataNPAdtsLLIdxSorted[:,[5]]

        #self.PrintArray(lonNpArrayIdxSorted, arrayName="lonNpArrayIdxSorted")
        #self.PrintArray(latNpArrayIdxSorted, arrayName="latNpArrayIdxSorted")
        
        # add longitude
        meteoDataStoreNpArray = lonNpArrayIdxSorted.reshape(lonNpArrayIdxSorted.shape[0],1)
        # add latitude
        meteoDataStoreNpArray = np.hstack((meteoDataStoreNpArray, latNpArrayIdxSorted.reshape(latNpArrayIdxSorted.shape[0],1)) )
       
        for k in meteoDataStoreKeys:
            dataParamArray = self.meteoDataStore[k]
            #print "meteoDataStore[k=%s]: dataParamArray.shape="%k, dataParamArray.shape
            if  meteoDataStoreNpArray.size==0:
                meteoDataStoreNpArray = dataParamArray[indexSort].reshape(dataParamArray.shape[0],1)
            else:
                meteoDataStoreNpArray = np.hstack((meteoDataStoreNpArray, dataParamArray[indexSort].reshape(dataParamArray.shape[0],1)) )

        self.PrintArray(meteoDataStoreNpArray, arrayName="meteoDataStoreNpArray")       
        #print "meteoDataStoreNpArray.shape=", meteoDataStoreNpArray.shape
        reassembledResultArray = np.hstack((dataOut,meteoDataStoreNpArray))
        #print "reassembledResultArray.shape=", reassembledResultArray.shape

        if onlyTesting:
            self.PrintArray(reassembledResultArray, arrayName="reassembledResultArray")
        self.PrintArrayJoinedAsString(reassembledResultArray, arrayName="reassembledResultArray")
        
        #reassembledResultArray = np.hstack( (dataOut,meteoDataStoreList) )
                
        #self.queryDataNPAdtsLL = np.vstack(( queryDataNPADateTimeSorted[:,0], queryDataNPADateTimeSorted[:,1], 
        #                                queryDataNPADateTimeSorted[:,2], queryDataNPADateTimeSorted[:,3], 
        #                                longitudes,latitudes)).T
        #print "reassembledResultArray=\n", reassembledResultArray
        
        #for rowId in indicesqueryDataNPAdtsLLIdxSorted:
        
        if onlyTesting:
            ## Slower version to CONTROL the above results
            printProgress("** Control procedure: should give same as above .. ***")
            for dc in self.dataColumns:
                rowIdStr = "%d"%rowId
                indexResults = np.where(indicesqueryDataNPAdtsLL == rowIdStr)
                #print "rowId=",rowId," === queryDataNPAdtsLL[indexResults][0][0]=", self.queryDataNPAdtsLL[indexResults][0][0]
                ROWorig = list(dataOut[rowId])
                dataAppendStr = self.delimiter.join(ROWorig)
                # For testing add lat-lon
                lonStr = str(self.queryDataNPAdtsLL[indexResults,4][0][0])
                latStr = str(self.queryDataNPAdtsLL[indexResults,5][0][0])
                dataAppendStr += self.delimiter + lonStr + self.delimiter + latStr
               
                for k in meteoDataStoreKeys:
                    dataParamArray = self.meteoDataStore[k]
                    dataValue = dataParamArray[indexResults]
                    dataValueStr = "%f" % dataValue
                    if len(dataAppendStr)>0:
                        dataAppendStr += self.delimiter
                    dataAppendStr += dataValueStr
                
                rowId += 1 
                ftxt.writelines(dataAppendStr+"\n")
                if self.verbose and self.verboseLevel>=10:
                    printProgress(dataAppendStr)
                    
        ftxt.close()

        

    def Distance2pointsInLonLat(self, lng1,lat1,lng2,lat2):
        #global geoTransfWGS84
        #geoTransfWGS84
        az12,az21,dist = geoTransf.inv(lng1,lat1,lng2,lat2)
        return dist
        #help(Geod.__new__) gives a list of possible ellipsoids.
        #Calculate the distance between two points, as well as the local heading
        # lat1,lng1 = (40.7143528, -74.0059731)  # New York, NY
        # lat2,lng2 = (49.261226, -123.1139268)   # Vancouver, Canada
        # az12,az21,dist = geoTransf.inv(lng1,lat1,lng2,lat2)

    def Distance2pointsInXY(self, ptA,ptB):
        #ptA=[(0,0)]
        #ptB=[(10,20)]
        vecAB = np.array(ptB) - np.array(ptA)
        vecLng = np.linalg.norm(vecAB)
        return vecLng


    
    