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
        """
        Called in ReadInputCSV in a loop for every record.
        This routine converts local time stamps in the CSV to UTC.
        TODO: If the time is already in UTC, this might not work.
        TODO: throw exception if we cannot convert because of unsupported date/time format.
        """
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
                    try:
                        minute = int(minuteStr)
                        self.CLASSPRINT('WARNING: could not extract (hour) from: "(%s)"' %(hourStr) )
                    except:
                        self.CLASSPRINT('WARNING: could not extract (hour & minute) from: "(%s,%s)"' %(hourStr,minuteStr) )
                    
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
        """
        Read and scan the input CSV. convert time stamps to UTC.
        When this function returns successfully, we have all information to
        create metadata JSON file.
        """
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
        
        self.CLASSPRINT("*******************************************")
        self.CLASSPRINT("***** Reading CSV-file STARTED.   *********")
        self.CLASSPRINT("*******************************************")

        ## dtype is important. The statement below reads in the entire CSV into
        ## a 2 dimensional numpy array where every element is a 300 character string.
        
        self.dataUnsortedStr = np.recfromtxt(self.inputCSVfile, skip_header=self.numHeaderLines, comments="#", dtype="|S300",  delimiter=self.delimiter)                      
        self.CLASSPRINT("***** Extracting data columns...  *********")
        if self.limitTo>0:
            ## self.dataColumns is a 2 dimensional array of strings with only the requested columns from columnsList.
            self.dataColumns = self.dataUnsortedStr[:self.limitTo, self.columnsList ]
            if self.verbose and self.verboseLevel>=10:
                #pprint.pprintProgress(self.dataColumns) 
                self.CLASSPRINT( "dataColumns:\n", self.dataColumns )
        else:
            self.dataColumns = self.dataUnsortedStr[:, self.columnsList ]
            if self.verbose and self.verboseLevel>=10:
                self.PrintArray(self.dataColumns, arrayName="self.dataColumns")

        self.CLASSPRINT("*******************************************")
        self.CLASSPRINT("***** Reading CSV-file FINISHED.  *********")
        self.CLASSPRINT("*******************************************")

        ## This would be a good location to call the WPS status callback function.
        self.CLASSPRINT("*******************************************")
        self.CLASSPRINT("***** Decoding date-time format STARTED. **")
        self.CLASSPRINT("*******************************************")

        '''
        [wps/wrangler] >cat data/ExportOngevalsData.csv | grep Onbekend| wc -l
        33656 .. there are diffrent places (columns) where "Onbekend" is...
        ..
        Ongeval exact gekoppeld aan BN,8.00-08.59,22JAN07,13,Letsel,0,0,Onbekend,Kruispunt,258737,471220,1,0,1,0
        Ongeval exact gekoppeld aan BN,22.00-22.59,18JAN07,0,Letsel,0,0,Onbekend,Kruispunt,75750,453134,1,0,0,0
        Ongeval exact gekoppeld aan BN,8.00-08.59,05FEB07,33,Letsel,0,0,Onbekend,Kruispunt,169835,482830,1,0,1,0
        Ongeval exact gekoppeld aan BN,17.00-17.59,03FEB07,31,Letsel,0,0,Onbekend,Kruispunt,145169,456460,1,0,0,0
        ..
        Ongeval exact gekoppeld aan BN,Onbekend,29JAN07,Onbekend,Letsel,0,0,Flank,Kruispunt,254097,569483,1,0,1,0
        Ongeval exact gekoppeld aan BN,Onbekend,05FEB07,Onbekend,Letsel,0,0,Frontaal,Wegvak,129124.252,426803.097,0,1,1,0        
        
        .. practically only 55 (=46+9) are related to HOUR and MINUTE specification ..
        [bhwatx2] [wrangler/data] >grep BN,Onbekend ExportOngevalsData.csv | wc -l
        46
        [bhwatx2] [wrangler/data] >grep niveau,Onbekend ExportOngevalsData.csv | wc -l
        9        
        '''

        idCounter = 0
        queryDataArray = []        
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
            
        ## Translate the python list to a 2 dimensional numpy array of [ [id, utc-time, X-coord, Y-coord], ... ]
        queryDataNPA = np.array(queryDataArray)

        # uncomment for debugging
        #print queryDataArray
        #print "queryDataNPA=\n", queryDataNPA

        # SORT ACCORDING THE UTC-TIME:
        self.sortedIndexUtcTime = np.lexsort((queryDataNPA[:,1],)) ## create sorted list of indices. ':' => vertical dimension
        queryDataNPADateTimeSorted = queryDataNPA[self.sortedIndexUtcTime] ## create sorted 2-dimensional array
        self.queryDataNPADateTimeSorted = queryDataNPADateTimeSorted
        #print "queryDataNPADateTimeSorted=\n", queryDataNPADateTimeSorted
        self.minDateTime = queryDataNPADateTimeSorted[0,1]
        
        # There might be missing/unspecified HOURS in the user-csv data.
        # These are marked as INVALID as lexically sorted appearing at the of the array.
        utcTimeStr_last = queryDataNPADateTimeSorted[-1,1]
        if utcTimeStr_last!="INVALID":
            self.maxDateTime = utcTimeStr_last
        else:
            ## Find the first non-invalid time from the last record upwards
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
        # 2-dimensional numpy array [  [id, utc-time, X-coord, Y-coord ], .. ] sorted by utc time
        xcoords = queryDataNPADateTimeSorted[:,2] ## still a 1-dimensional numpy array of strings
        ycoords = queryDataNPADateTimeSorted[:,3] ## still a 1-dimensional numpy array of strings
        
        (longitudes,latitudes) = self.UnProject2LongitudeLatitudes(xcoords, ycoords)
        lonLatStacked = np.vstack((longitudes,latitudes)).T
        #print lonLatStacked

        ## Determine the bounding box.
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

        # 2-dimensional numpy array [  [id, utc-time, X-coord, Y-coord, longitude, latitude ], .. ] sorted by utc time
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

    def GetProjectionString(self):
        if not self.metaCSVdict:
            return None
        return self.projFuncDefstring
        

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
        self.WrangleMeteoParameterDummy(parameterName)
        
    def WrangleMeteoParameterDummy(self, parameterName):
        dims = self.queryDataNPAdtsLL.shape
        
        #print "self.queryDataNPAdtsLL.shape=",self.queryDataNPAdtsLL.shape
        #self.meteoDataStore[parameterName] = np.array(np.ones(dims[0]))
        
        testinOnly = False
        if testinOnly:
            # This MUST in the produced merged result-data give columns of growing numbers 0,1,2,3,4, ....
            self.meteoDataStore[parameterName] = self.queryDataNPADateTimeSorted[:,0]
            # Obviously the row-of-code below will not give the right order as during wrangling-process
            # because at this stage is everything sorted according utc-time
            #self.meteoDataStore[parameterName] = np.arange(dims[0]) 
            '''
            If working correcly you should see at the end 0,0 .. 1,1, .. 199,199, .. 200,200, ...
            reassembledResultArray[201, 19] =
            Ongeval gekoppeld op gemeente niveau,1.00-01.59,03JAN06,10,Letsel,0,0,Vast voorwerp,Kruispunt,111998,516711,1,0,0,0,4.75230997787,52.6372473247,0,0
            Ongeval exact gekoppeld aan BN,7.00-07.59,07JAN06,50,Letsel,0,1,Frontaal,Kruispunt,95069,451430,1,0,0,0,4.51384787854,52.0489154656,1,1
            Ongeval exact gekoppeld aan BN,11.00-11.59,21JAN06,10,Letsel,0,0,Flank,Wegvak,119330.273,486468.495,1,0,0,0,4.86386813359,52.365955465,2,2
            ...
            Ongeval exact gekoppeld aan BN,21.00-21.59,24JAN06,15,Letsel,0,0,Frontaal,Kruispunt,191085,465993,2,0,0,0,5.91531887881,52.1818847736,197,197
            Ongeval exact gekoppeld aan BN,8.00-08.59,25JAN06,59,Letsel,0,0,Vast voorwerp,Wegvak,226182.003,495246.493,1,0,0,0,6.43466885838,52.4413903097,198,198
            Ongeval exact gekoppeld aan BN,16.00-16.59,25JAN06,31,Letsel,0,0,Frontaal,Wegvak,44511.752,361228.214,2,0,0,0,3.80550803333,51.2305216668,199,199
            Ongeval exact gekoppeld aan BN,7.00-07.59,13JAN06,32,Letsel,0,0,Flank,Wegvak,58741.713,370184.112,1,0,0,0,4.00678725282,51.3136078449,200,200
            ...
            '''
        else:
            if "temp" in parameterName:
                self.meteoDataStore[parameterName] = np.random.uniform(low=273.15, high=280, size=(dims[0],))
            else:
                self.meteoDataStore[parameterName] = np.random.uniform(low=0.0, high=100.0, size=(dims[0],))
        if self.verbose:
            self.CLASSPRINT("WrangleMeteoParameterDummy(%s): meteoDataStore[%s]="%(parameterName,parameterName),self.meteoDataStore[parameterName].shape)


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
        printProgress( "%s%s =" %(arrayName, str(list(dims))) )
        for i in xrange(dims[0]):
            printProgress( str( list(arr[i]) ) )
            
    def PrintArrayJoinedAsString(self,arr, arrayName="", delimiter=","):
        dims = arr.shape
        printProgress( "%s%s =" %(arrayName, str(list(dims))) )
        for i in xrange(dims[0]):
            printProgress( delimiter.join( list(arr[i]) ) )

            
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
        
        headerTextOutput = self.headerText[:].rstrip('\n')
        
        if exportLonLat:
            headerTextOutput += ",longitude,latitude"
        meteoDataStoreKeys =  self.meteoDataStore.keys()
        for k in meteoDataStoreKeys:
            headerTextOutput += ",%s" %k

        self.CLASSPRINT(' Working on reassembledResultArray..' )
            
        rowId = 0
        # NOTE: The array is sorted according DATE-TIME
        indicesqueryDataNPAdtsLL =  self.queryDataNPAdtsLL[:,0]
        #print "indicesqueryDataNPAdtsLL=", indicesqueryDataNPAdtsLL.astype(np.int)
        
        # SORT ACCORDING THE INDEX-reference (== the original row-order from CSV):
        # This is a VITAL part !!! Don't touch this unless you are sure what you are doing with numpy
        # Note: We cannot use np.lexsort ! As it will give the folowing order: '0','1','10','11','12',..,'19','2','20'
        #indexSort = np.lexsort((self.queryDataNPAdtsLL[:,0],))  # DON'T use this here!
        
        indexSort = np.argsort(self.queryDataNPAdtsLL[:,0].astype(np.int))
        #print indexSort
        #self.PrintArray(indexSort.reshape(len(indexSort),1), arrayName="indexSort")
        queryDataNPAdtsLLIdxSorted = self.queryDataNPAdtsLL[indexSort]
        indicesqueryDataNPAdtsLLIdxSorted =  (queryDataNPAdtsLLIdxSorted[:,0]).astype(np.int)
        #print "indicesqueryDataNPAdtsLLIdxSorted=", indicesqueryDataNPAdtsLLIdxSorted
        
        if self.verbose and self.verboseLevel>=10:
            self.PrintArray(queryDataNPAdtsLLIdxSorted, arrayName="queryDataNPAdtsLLIdxSorted")
            self.PrintArray(dataOut, arrayName="dataOut")
            #print "dataOut.shape=", dataOut.shape
        
        meteoDataStoreNpArray = np.array([])
        
        lonNpArrayIdxSorted = queryDataNPAdtsLLIdxSorted[:,[4]]
        latNpArrayIdxSorted = queryDataNPAdtsLLIdxSorted[:,[5]]

        #self.PrintArray(lonNpArrayIdxSorted, arrayName="lonNpArrayIdxSorted")
        #self.PrintArray(latNpArrayIdxSorted, arrayName="latNpArrayIdxSorted")

        # add longitude
        self.CLASSPRINT(' Adding result array: %s' %('longitude') )
        meteoDataStoreNpArray = lonNpArrayIdxSorted.reshape(lonNpArrayIdxSorted.shape[0],1)
        # add latitude
        self.CLASSPRINT(' Adding result array: %s' %('latitude') )
        meteoDataStoreNpArray = np.hstack((meteoDataStoreNpArray, latNpArrayIdxSorted.reshape(latNpArrayIdxSorted.shape[0],1)) )
       
        for k in meteoDataStoreKeys:
            dataParamArray = self.meteoDataStore[k]
            #print "meteoDataStore[k=%s]: dataParamArray.shape="%k, dataParamArray.shape
            self.CLASSPRINT(' Adding result array: %s' %(k) )            
            if  meteoDataStoreNpArray.size==0:
                meteoDataStoreNpArray = dataParamArray[indexSort].reshape(dataParamArray.shape[0],1)
            else:
                meteoDataStoreNpArray = np.hstack((meteoDataStoreNpArray, dataParamArray[indexSort].reshape(dataParamArray.shape[0],1)) )

        if self.verbose and self.verboseLevel>=10:
            self.PrintArray(meteoDataStoreNpArray, arrayName="meteoDataStoreNpArray")       
        #print "meteoDataStoreNpArray.shape=", meteoDataStoreNpArray.shape
        reassembledResultArray = np.hstack((dataOut,meteoDataStoreNpArray))
        #print "reassembledResultArray.shape=", reassembledResultArray.shape

        if onlyTesting:
            if self.verbose and self.verboseLevel>=10:          
                self.PrintArray(reassembledResultArray, arrayName="reassembledResultArray")
        
        if self.verbose and self.verboseLevel>=10:
            self.PrintArrayJoinedAsString(reassembledResultArray, arrayName="reassembledResultArray")
                        
        if onlyTesting:
            printProgress("** Slower control procedure: should give same as above .. ***")
            # write the header
            ftxt = open(self.outputCSVfile,"wt")           
            ftxt.writelines(headerTextOutput+"\n")
            if self.verbose:
                printProgress(headerTextOutput)
            
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
            # time: 188.356u 6.136s 3:14.50 99.9%	0+0k 0+38384io 0pf+0w
        else:
            self.CLASSPRINT(' Writing reassembledResultArray: to the output file..')            
            np.savetxt(self.outputCSVfile, reassembledResultArray, fmt='%s', delimiter=self.delimiter, comments='', header=headerTextOutput)
            # time: 11.780u 6.344s 0:18.13 99.9%	0+0k 0+50240io 0pf+0w



    
    
