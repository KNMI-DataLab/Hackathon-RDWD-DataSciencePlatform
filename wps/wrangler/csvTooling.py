#!/usr/bin/env python

#from __future__ import print_function
import sys, os, os.path, glob, string, re
import inspect
import math

import numpy as np
np.set_printoptions(linewidth=200)
import pyproj
from pyproj import Geod
import csv
from datetime import datetime,timedelta
import pytz 
import pprint
import jsonTooling as jst
import os, sys
from dataObjectBase import dataObjectBase

import pandas as pd
# apt-get install python-pandas

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


class csvDataObject(dataObjectBase):
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

        # Resolving situations like:
        #     Ongeval exact gekoppeld aan BN,Onbekend,29JAN07,Onbekend,Letsel,0,0,Flank,Kruispunt,254097,569483,1,0,1,0
        self.invalidDateTime = np.datetime64('4000-01-01') # reserved  for "INVALID"        
        
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
        ## When scanning, we need to scan the entire file, so
        ## don't honor the limit here, only when actually wrangling
        ## in the WrangleWithNetCdfData of dataWranglerProcessor.
        pass
        #self.limitTo = limitTo

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
                    
                    return None,None  # this mean INVALID request

            try:
                minute = int(minuteStr)
            except:
                if self.autoResolve_minute>0:
                    minute = self.autoResolve_minute
                else:
                    self.CLASSPRINT('WARNING: could not extract minute from: "%s"' %(minuteStr) )
                    return None,None  # this mean INVALID request

            
        localTime = givenDate + timedelta(hours=hour, minutes=minute)
        
        (utcTime, datetime_in_utc_str, datetime_with_tz_str)  = self.ConvertLocalDateTime2Utc(localTime, zone=self.metaCSVdict['timeZone'])
        
        if self.verbose and self.verboseLevel>=10:
            self.CLASSPRINT(dateStr, '=>', givenDate,';', hourStr, '=>', hour,';',datetime_with_tz_str,'=>', datetime_in_utc_str)
        
        fmt = '%Y-%m-%d %H:%M:%S %Z'
        utcTimeStr = utcTime.strftime(fmt)
        return (utcTimeStr, utcTime)
    
    def ReadCSVHeader(self):
        self.CLASSPRINT('reading header inputCSVfile: %s' %(self.inputCSVfile) )
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
        self.ReadCSVHeader()
        
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

        rowCounter = 0
        queryDataArray = []        
        for aa in self.dataColumns:
            (utcTimeStr, utcTime) = self.DecodeDateTime(dateStr=aa[0], hourStr=aa[1], minuteStr=aa[2])
            if utcTimeStr==None:  # None means INVALID request!
                #dataRow = [ rowCounter, "INVALID", float(aa[3]), float(aa[4]) ]  # store [id, utc-time, X-coord, Y-coord ]
                dataRow = [ rowCounter, self.invalidDateTime, "INVALID", float(aa[3]), float(aa[4]) ]  # store [id, utc-time, utc-time-str, X-coord, Y-coord ]
                self.CLASSPRINT('WARNING: INVALID date-time specification at row-number: %d; "%s"' %(rowCounter, str( aa ) ) )
                queryDataArray.append( dataRow )
            else:
                #dataRow = [ rowCounter, utcTimeStr, float(aa[3]), float(aa[4]) ]  # store [id, utc-time, X-coord, Y-coord ]
                dataRow = [ rowCounter, np.datetime64(utcTime), utcTimeStr, float(aa[3]), float(aa[4]) ]  # store [id, utc-time, utc-time-str, X-coord, Y-coord ]
                queryDataArray.append( dataRow )
            rowCounter += 1
            
        ## Translate the python list to a 2 dimensional numpy array of [ [id, utc-time, X-coord, Y-coord], ... ]
        queryDataNPA = np.array(queryDataArray)

        # uncomment for debugging
        #print queryDataArray
        #print "queryDataNPA=\n", queryDataNPA

        self.timeUnits = "" #self.metaData.variables['time'].units
        self.dateTimeArray = queryDataNPA[:,1]
        #print self.dateTimeArray
        
        # remove invalid dateTime records from the array
        indexDelete = np.where(self.dateTimeArray == self.invalidDateTime) # reserved  for "INVALID"
        self.dateTimeArrayClean = np.delete(self.dateTimeArray, indexDelete)
        #print self.dateTimeArrayClean
        
        # np.datetime64 => datetime; The date-time must be in UTC
        self.minDateTime = np.min(self.dateTimeArrayClean).astype(datetime).replace(tzinfo=pytz.UTC)
        self.maxDateTime = np.max(self.dateTimeArrayClean).astype(datetime).replace(tzinfo=pytz.UTC)

        fmt = '%Y-%m-%d %H:%M:%S %Z'
        self.minDateTime_str = self.minDateTime.strftime(fmt) 
        self.maxDateTime_str = self.maxDateTime.strftime(fmt)


        self.CLASSPRINT("##########################################################")
        self.CLASSPRINT("minDateTime_str=%s, maxDateTime_str=%s" %(self.minDateTime_str, self.maxDateTime_str)  )
        self.CLASSPRINT("minDateTime=%s, maxDateTime=%s" %(self.minDateTime, self.maxDateTime)  )
        self.CLASSPRINT("##########################################################")

        queryDataNPAdt = queryDataNPA ## create sorted 2-dimensional array
        self.queryDataNPAdt = queryDataNPAdt

        self.CLASSPRINT("*******************************************")
        self.CLASSPRINT("***** Decoding date-time format FINISHED.**")
        self.CLASSPRINT("*******************************************")
        
        self.CLASSPRINT("*******************************************")
        self.CLASSPRINT("***** Computing LON-LAT STARTED.     ******")
        self.CLASSPRINT("*******************************************")
        
        self.projFuncDefstring = self.metaCSVdict['projString']
        self.projectionFunction = pyproj.Proj(self.projFuncDefstring)
        # 2-dimensional numpy array [  [id, utc-time, X-coord, Y-coord ], .. ] sorted by utc time
        xcoords = queryDataNPAdt[:,3] ## still a 1-dimensional numpy array of strings
        ycoords = queryDataNPAdt[:,4] ## still a 1-dimensional numpy array of strings
        
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
        #print "queryDataNPAdt.shape=", queryDataNPAdt[:,0].shape
        #print "longitudes.shape=", longitudes.shape

        # self.queryDataNPAdtsLL 2-dimensional numpy array [  [id, utc-time, utc-time-str, X-coord, Y-coord, longitude, latitude ], .. ] 
        #self.queryDataNPAdtsLL = np.vstack(( queryDataNPAdt[:,0], queryDataNPAdt[:,1], queryDataNPAdt[:,2], 
        #                                queryDataNPAdt[:,3], queryDataNPAdt[:,4], 
        #                                longitudes,latitudes)).T
        # self.queryDataNPAdtsLL: 2-dimensional numpy array [  [id, utc-time, longitude, latitude ], .. ] 
        self.queryDataNPAdtsLL = np.vstack(( queryDataNPAdt[:,0], queryDataNPAdt[:,1], 
                                            longitudes,latitudes)).T
        if self.verbose and self.verboseLevel>=10:
            self.CLASSPRINT("queryDataNPAdt:")
            arrayDims = self.queryDataNPAdtsLL.shape
            #for i in xrange(arrayDims[0]):
            for i in xrange(10):
                #printProgress("%s" %( pprint.pformat(str(self.queryDataNPAdtsLL[i][:]), width=200) ) )
                printProgress("%s" %( pprint.pformat(self.queryDataNPAdtsLL[i][:], width=200) ) )
                
        return

    def WriteFullQueryDataToTmpFile(self,  tmpFileName):
        self.CLASSPRINT(' Writing queryDataNPAdtsLL: to the temporary file..%s' %(tmpFileName))
        #headerTextTmpFile =  "id, utc-time, utc-time-str, X-coord, Y-coord, longitude, latitude"  # 2-dimensional numpy array sorted by utc time
        headerTextTmpFile =  "id, utc-time, longitude, latitude"  # 2-dimensional numpy array sorted by utc time
        np.savetxt(tmpFileName, self.queryDataNPAdtsLL, fmt='%s', delimiter=',', comments='', header=headerTextTmpFile)
        if self.verbose and  self.verboseLevel>=10:
            printProgress( "self.queryDataNPAdtsLL[:10] =")
            for i in xrange(10):            
                #printProgress("%s" %( pprint.pformat(str(self.queryDataNPAdtsLL[i][:]), width=200) ) )
                printProgress("%s" %( pprint.pformat(self.queryDataNPAdtsLL[i][:], width=200) ) )
            
    def GetTotalNumberOfCSVrows(self):
        return self.queryDataNPAdtsLL.shape[0]
        
    def ReadFullQueryDataFromTmpFile(self,  tmpFileName, startAtRow = 0, readRows=-1):
        #headerTextTmpFile =  "id, utc-time, utc-time-str, X-coord, Y-coord, longitude, latitude"  # 2-dimensional numpy array sorted by utc time
        headerTextTmpFile =  "id, utc-time, longitude, latitude"  # 2-dimensional numpy array sorted by utc time            

        headers = ["id","utc-time", "longitude", "latitude"]
        dtypes = { "id": int, "utc-time": datetime, "longitude": float, "latitude": float}
        parse_dates = ['utc-time']
            
        if readRows>0:            
            self.CLASSPRINT(' Reading queryDataNPAdtsLL: from the temporary file: %s; startAtRow = %d, readRows=%d' %(tmpFileName, startAtRow, readRows))
            #dataTmpCSV = np.recfromtxt(tmpFileName, skip_header=1 + startAtRow, max_rows = readRows, comments="#", dtype="|S300",  delimiter=',')
            #dataTmpCSV = np.recfromtxt(tmpFileName, skip_header=1 + startAtRow, max_rows = readRows, comments="#", dtype='int,datetime64,float,float',  delimiter=',')
            dataTmpCSV = pd.read_csv(tmpFileName, sep=',', header=None, skiprows=1+startAtRow, nrows=readRows, names=headers, dtype=dtypes, parse_dates=parse_dates)
        else:
            self.CLASSPRINT(' Reading queryDataNPAdtsLL: from the output file: %s; startAtRow = %d, whole-file-read' %(tmpFileName, startAtRow))
            #dataTmpCSV = np.recfromtxt(tmpFileName, skip_header=1, comments="#", dtype="|S300",  delimiter=',')
            #dataTmpCSV = np.recfromtxt(tmpFileName, skip_header=1, comments="#", dtype='int,datetime64,float,float',  delimiter=',')
            #date_parser = pd.datetools.to_datetime            
            dataTmpCSV = pd.read_csv(tmpFileName, sep=',', header=None, skiprows=1+startAtRow, names=headers, dtype=dtypes, parse_dates=parse_dates)

        
        #self.queryDataNPAdtsLL = dataTmpCSV.values[:]
        dateTimeArrayTimestamp =  dataTmpCSV["utc-time"] # pandas dataframe
        dateTimeArray  =  dateTimeArrayTimestamp.dt.to_pydatetime()
        #print "dateTimeArray:\n",dateTimeArray
        #printProgress( "dateTimeArray.queryDataNPAdtsLL=\n"+str(dateTimeArray) )
        size = dateTimeArray.shape[0]
        self.queryDataNPAdtsLL = np.hstack((dataTmpCSV.values[:,0].reshape(size,1), dateTimeArray.reshape(size,1), dataTmpCSV.values[:,2].reshape(size,1), dataTmpCSV.values[:,3].reshape(size,1) ) )
        
        #printProgress( "self.queryDataNPAdtsLL=\n"+str(self.queryDataNPAdtsLL) )
        #printProgress( "self.queryDataNPAdtsLL.shape="+str(self.queryDataNPAdtsLL.shape) )
        
        if self.verbose and  self.verboseLevel>=10:
            printProgress( "len(dataTmpCSV)=%d"%len(dataTmpCSV) )
            printProgress( "self.queryDataNPAdtsLL[:10] =" )
            for i in xrange(10):
                #printProgress("%s" %( pprint.pformat(str(self.queryDataNPAdtsLL[i][:]), width=200) ) )
                printProgress("%s" %( pprint.pformat(self.queryDataNPAdtsLL[i][:], width=200) ) )
        '''
        self.queryDataNPAdtsLL[:10] = 
        [[0 numpy.datetime64('2006-01-03T00:10:00.000000') 4.752309977868518 52.63724732465345]
         [1 numpy.datetime64('2006-01-07T06:50:00.000000') 4.513847878535973 52.048915465574154]
         [2 numpy.datetime64('2006-01-21T10:10:00.000000') 4.863868133591141 52.36595546503313]
         [3 numpy.datetime64('2006-01-13T11:45:00.000000') 4.470192057036912 51.95222282937921]
         [4 numpy.datetime64('2006-01-09T17:09:00.000000') 5.890625864700398 50.91224069570073]
         [5 numpy.datetime64('2006-01-03T07:15:00.000000') 6.89515403058042 52.19821169074023]
         [6 numpy.datetime64('2006-01-12T16:57:00.000000') 5.66439893004735 50.85429803479775]
         [7 numpy.datetime64('2006-01-24T16:00:00.000000') 5.710791456635714 50.861504457973496]
         [8 numpy.datetime64('2006-01-13T21:49:00.000000') 5.433339456169923 51.418358706500634]
         [9 numpy.datetime64('2006-01-30T08:57:00.000000') 3.9207341453872204 51.65060526354358]]
        '''
        
            

    def GetTimeRangeOfData(self):
        '''
        Return json: {  "minDateTime": "2006-01-03 00:10:00 UTC",
                        "maxDateTime": "2006-01-30 08:57:00 UTC" }
        Note: The actual range is compute in function: ReadInputCSV()
        '''
        dataRange =  {  "minDateTime": self.minDateTime_str,
                        "maxDateTime": self.maxDateTime_str }
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

    def WrangleMeteoParameter(self, parameterName):
        self.WrangleMeteoParameterDummy(parameterName)
        
    def WrangleMeteoParameterDummy(self, parameterName):
        dims = self.queryDataNPAdtsLL.shape
        
        #print "self.queryDataNPAdtsLL.shape=",self.queryDataNPAdtsLL.shape
        #self.meteoDataStore[parameterName] = np.array(np.ones(dims[0]))
        
        testinOnly = False
        if testinOnly:
            # This MUST in the produced merged result-data give columns of growing numbers 0,1,2,3,4, ....
            self.meteoDataStore[parameterName] = self.queryDataNPAdtsLL[:,0]
            #self.meteoDataStore[parameterName] = self.queryDataNPAdt[:,0]
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

        # self.queryDataNPAdtsLL: 2-dimensional numpy array [  [id, utc-time, longitude, latitude ], .. ] 
            
        rowId = 0
        # NOTE: The array is sorted according DATE-TIME
        indicesqueryDataNPAdtsLL =  self.queryDataNPAdtsLL[:,0]            
        if self.verbose and self.verboseLevel>=10:
            self.PrintArray(self.queryDataNPAdtsLL, arrayName="self.queryDataNPAdtsLL")
            self.PrintArray(dataOut, arrayName="dataOut")
            #print "dataOut.shape=", dataOut.shape
        
        meteoDataStoreNpArray = np.array([])
        

        # add utc-date-time column
        utcTimeNpArrayIdxSorted = self.queryDataNPAdtsLL[:,[1]]
        self.CLASSPRINT(' Adding result array: %s' %('utc-date-time') )
        meteoDataStoreNpArray = utcTimeNpArrayIdxSorted.reshape(utcTimeNpArrayIdxSorted.shape[0],1)

        lonNpArrayIdxSorted = self.queryDataNPAdtsLL[:,[2]]
        latNpArrayIdxSorted = self.queryDataNPAdtsLL[:,[3]]

        #self.PrintArray(lonNpArrayIdxSorted, arrayName="lonNpArrayIdxSorted")
        #self.PrintArray(latNpArrayIdxSorted, arrayName="latNpArrayIdxSorted")

        # add longitude
        self.CLASSPRINT(' Adding result array: %s' %('longitude') )
        if  meteoDataStoreNpArray.size==0:
            meteoDataStoreNpArray = lonNpArrayIdxSorted.reshape(lonNpArrayIdxSorted.shape[0],1)
        else:
            meteoDataStoreNpArray = np.hstack((meteoDataStoreNpArray, lonNpArrayIdxSorted.reshape(lonNpArrayIdxSorted.shape[0],1)) )
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
                meteoDataStoreNpArray = np.hstack((meteoDataStoreNpArray, dataParamArray.reshape(dataParamArray.shape[0],1)) )

        if self.verbose and self.verboseLevel>=10:
            self.PrintArray(meteoDataStoreNpArray, arrayName="meteoDataStoreNpArray")       
        #print "meteoDataStoreNpArray.shape=", meteoDataStoreNpArray.shape
        reassembledResultArray = np.hstack((dataOut,meteoDataStoreNpArray))
        #print "reassembledResultArray.shape=", reassembledResultArray.shape

        if onlyTesting:
            if self.verbose and self.verboseLevel>=10:          
                self.PrintArray(reassembledResultArray, arrayName="reassembledResultArray")
        
        if self.verbose and self.verboseLevel>=10:
            self.PrintArrayJoinedAsString(reassembledResultArray.astype(np.str), arrayName="reassembledResultArray")
                        
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


    def WriteCSVHeader(self, fieldList):
        '''
        WriteCSVHeader(fieldList = ["utc-time","longitude","latitude", ...] )
        JoinBulkResults(tempFileList)
        '''
        self.ReadCSVHeader()
        headerTextOutput = self.headerText[:].rstrip('\n')
        for k in fieldList:
            headerTextOutput += ",%s" %k

        if os.path.exists(self.outputCSVfile):
            os.remove(self.outputCSVfile)

        ftxt = open(self.outputCSVfile,'wt')
        ftxt.writelines([headerTextOutput+"\n"])
        ftxt.close()

        if self.verbose:
            self.CLASSPRINT( "headerText=\n%s"  %(headerTextOutput))


    def JoinBulkResults(self, tempFileList, removeTempFiles=True):
        ftxt = open(self.outputCSVfile,'a')
        for tempfile in tempFileList:
            if self.verbose:
                self.CLASSPRINT( "Appending %s to %s"  %(tempfile, self.outputCSVfile))
            tempfileHandle = open(tempfile,'rU')
            for line in tempfileHandle:
                ftxt.write(line)
            tempfileHandle.close()
        ftxt.close()
        if removeTempFiles:
            for tempfile in tempFileList:
                    try:
                        os.remove(tempfile)
                    except:
                        pass
        self.CLASSPRINT(' Finished. Produced output: %s'%(self.outputCSVfile))


    def ProduceBulkOutput(self, tmpBulkFileName, bulkNr, startAtRow, readRows, exportLonLat = True):
        self.CLASSPRINT(' Producing bulkNr. %d, startAtRow=%d, readRows=%d into tempFile: %s' %(bulkNr, startAtRow, readRows,tmpBulkFileName) )
        
        self.CLASSPRINT(' Reading partially [%d:%d] inputCSVfile: %s' %(startAtRow, readRows, self.inputCSVfile) )
        self.dataUnsortedStr = np.recfromtxt(self.inputCSVfile, skip_header=self.numHeaderLines + startAtRow,
                                             max_rows = readRows, comments="#", dtype="|S300", delimiter=self.delimiter)
               
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

        meteoDataStoreKeys =  self.meteoDataStore.keys()
        
           
        headerTextOutput = "" # temporary bulk-output files have NO header!        

        self.CLASSPRINT(' Working on reassembledResultArray..' )

        # self.queryDataNPAdtsLL: 2-dimensional numpy array [  [id, utc-time, longitude, latitude ], .. ] 
            
        rowId = 0
        indicesqueryDataNPAdtsLL =  self.queryDataNPAdtsLL[:,0]            
        if self.verbose and self.verboseLevel>=10:
            self.PrintArray(self.queryDataNPAdtsLL, arrayName="self.queryDataNPAdtsLL")
            self.PrintArray(dataOut, arrayName="dataOut")
            #print "dataOut.shape=", dataOut.shape
        
        meteoDataStoreNpArray = np.array([])
        

        # add utc-date-time column
        
        utcTimeNpArrayIdxSorted = self.queryDataNPAdtsLL[:,[1]]
        self.CLASSPRINT(' Adding result array: %s' %('utc-date-time') )
        meteoDataStoreNpArray = utcTimeNpArrayIdxSorted.reshape(utcTimeNpArrayIdxSorted.shape[0],1)
        printProgress( "utcTimeNpArrayIdxSorted.shape="+str(utcTimeNpArrayIdxSorted.shape) )

        lonNpArrayIdxSorted = self.queryDataNPAdtsLL[:,[2]]
        latNpArrayIdxSorted = self.queryDataNPAdtsLL[:,[3]]

        #self.PrintArray(lonNpArrayIdxSorted, arrayName="lonNpArrayIdxSorted")
        #self.PrintArray(latNpArrayIdxSorted, arrayName="latNpArrayIdxSorted")

        # add longitude
        self.CLASSPRINT(' Adding result array: %s' %('longitude') )
        if  meteoDataStoreNpArray.size==0:
            meteoDataStoreNpArray = lonNpArrayIdxSorted.reshape(lonNpArrayIdxSorted.shape[0],1)
        else:
            meteoDataStoreNpArray = np.hstack((meteoDataStoreNpArray, lonNpArrayIdxSorted.reshape(lonNpArrayIdxSorted.shape[0],1)) )
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
                meteoDataStoreNpArray = np.hstack((meteoDataStoreNpArray, dataParamArray.reshape(dataParamArray.shape[0],1)) )

        if self.verbose and self.verboseLevel>=10:
            self.PrintArray(meteoDataStoreNpArray, arrayName="meteoDataStoreNpArray")       
            
        printProgress( "dataOut.shape="+str(dataOut.shape) )          
        printProgress( "meteoDataStoreNpArray.shape="+str(meteoDataStoreNpArray.shape) )
        
        reassembledResultArray = np.hstack((dataOut,meteoDataStoreNpArray))        
        printProgress( "reassembledResultArray.shape="+str(reassembledResultArray.shape) )

        if onlyTesting:
            if self.verbose and self.verboseLevel>=10:          
                self.PrintArray(reassembledResultArray, arrayName="reassembledResultArray")
        
        if self.verbose and self.verboseLevel>=10:
            self.PrintArrayJoinedAsString(reassembledResultArray.astype(np.str), arrayName="reassembledResultArray")
                        
        self.CLASSPRINT(' Writing reassembledResultArray: to the output file..')            
        np.savetxt(tmpBulkFileName, reassembledResultArray, fmt='%s', delimiter=self.delimiter, comments='', header="")


    
    
