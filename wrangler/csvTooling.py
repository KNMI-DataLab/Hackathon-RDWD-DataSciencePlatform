#!/usr/bin/env python

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

#from numpy import *
#from numpy import ma
#import scipy as Sci
#import scipy.linalg

import logging

def printProgress(infoStr, log=True):
    print infoStr
    if log:
        logger = logging.getLogger('main')
        logger.info(infoStr)


geoTransfWGS84 = Geod(ellps='WGS84')
geoTransf = geoTransfWGS84

class csvDataObject():

    def VerboseOn(self):
        self.verbose=True
    def VerboseOff(self):
        self.verbose=False

    def SetVerboseLevel(self, verboseLevel):
        self.verboseLevel = 0

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
                print "[%s:%s] %s.%s() %s" %(pid,pid,self.__class__.__name__,inspect.stack()[1][3], ''.join(args))
            except:
                print "[%s:%s] %s.%s() %s" %(pid,pid,self.__class__.__name__,inspect.stack()[1][3], ''.join(map(str,args))  )

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
        
        if self.verboseLevel>100:
            print("Path at terminal when executing this file")
            print(os.getcwd() + "\n")
            print("This file path, relative to os.getcwd()")
            print(__file__ + "\n")
            print("This file full path (following symlinks)")
            full_path = os.path.realpath(__file__)
            print(full_path + "\n")
            print("This file directory and name")
            path, file = os.path.split(full_path)
            print(path + ' --> ' + file + "\n")
            print("This file directory only")
            print(os.path.dirname(full_path))        

    def __del__(self):
        #self.CLASSPRINT('..Destructor()..')
        pass

    def checkFile(self, fname, infoStr):
        if os.path.exists(fname):
            return fname
        else:
            self.CLASSPRINT(' ERROR! (%s) File not exist: %s' %(infoStr, fname))
            sys.exit(1)
    
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
        #print('pytz.exceptions.AmbiguousTimeError: %s' % dt)
        needToDecide = True
    except pytz.exceptions.NonExistentTimeError:
        #print('pytz.exceptions.NonExistentTimeError: %s' % dt)        
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
            #print('pytz.exceptions.AmbiguousTimeError: %s' % dt)
            needToDecide = True
        except pytz.exceptions.NonExistentTimeError:
            #print('pytz.exceptions.NonExistentTimeError: %s' % dt)        
            needToDecide = True
        if needToDecide:
            datetime_with_tz   = local_tz.localize(datetime_without_tz, is_dst=False) 
        
        datetime_in_utc    = datetime_with_tz.astimezone(pytz.utc)

        if self.verbose:
            print datetime_with_tz.strftime(fmt),'=>', datetime_in_utc.strftime(fmt)
        return datetime_in_utc
        
    def DecodeDateTime(self, dateStr, hourStr, minuteStr):
        if self.metaCSVdict['dateFormat'] == "%d%b%y":
            givenDate = datetime.strptime(dateStr, "%d%b%y")
            #analysisTime = datetime.strptime(specs['tag']['starttime'], "%Y%m%d%H")
        if self.verbose:
            print dateStr, '=>', givenDate,';', 
            
        if self.metaCSVdict['hourFormat'] == "hourInterval":
            hour = float(hourStr.split('-')[0])
            if self.verbose:
                print hourStr, '=>', hour,';', 
            minute = int(minuteStr)
            
        localTime = givenDate + timedelta(hours=hour, minutes=minute)
        
        utcTime = self.ConvertLocalDateTime2Utc(localTime, zone=self.metaCSVdict['timeZone'])
        fmt = '%Y-%m-%d %H:%M:%S %Z'
        utcTimeStr = utcTime.strftime(fmt)
        return utcTimeStr
    
    def ReadInputCSV(self):
        self.CLASSPRINT(' reading metaCSVfile: %s' %(self.metaCSVfile) )
        self.metaCSVdict = jst.ReadJsonConfigurationFromFile(self.metaCSVfile)
        if self.verbose:
            print self.metaCSVdict
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

        self.CLASSPRINT(' reading inputCSVfile: %s' %(self.inputCSVfile) )
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
            print "headerText=\n*****\n%s"  %(self.headerText); print "*****"
        self.dataUnsortedStr = np.recfromtxt(self.inputCSVfile, skip_header=self.numHeaderLines, comments="#", dtype="|S300",  delimiter=self.delimiter)                      
        if self.limitTo>0:
            self.dataColumns = self.dataUnsortedStr[:self.limitTo, self.columnsList ]
            if self.verbose:
                #pprint.pprint(self.dataColumns) 
                print self.dataColumns
        else:
            self.dataColumns = self.dataUnsortedStr[:, self.columnsList ]
        #dataUnsorted = dataUnsortedStr.astype(dtype=[('date', str), ('time', str), ('id', str), ('lon', float), ('lat', float), ('flightlevel', float) , ('windSpeed', float), ('windDirection', float), ('temp', float), ('flightphase', str)  ])
        #20131028 040029 M83240b 51.3094 1.0316 350.00 120.078 236.942 222.326 0
        queryDataArray = []
        idCounter = 0
        if self.verbose:
            print "Decoding date-time format..."
        for aa in self.dataColumns:
            utcTimeStr = self.DecodeDateTime(dateStr=aa[0], hourStr=aa[1], minuteStr=aa[2])
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
        self.maxDateTime = queryDataNPADateTimeSorted[-1,1]
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
        print "##########################################################"
        print "minDateTime=%s, maxDateTime=%s" %(self.minDateTime, self.maxDateTime)       
        print "LATLON-BBOX (west, east, north, south):", (self.llbox_west,self.llbox_east,self.llbox_north,self.llbox_south)
        print "##########################################################"
        #print "queryDataNPADateTimeSorted.shape=", queryDataNPADateTimeSorted[:,0].shape
        #print "longitudes.shape=", longitudes.shape
        self.queryDataNPAdtsLL = np.vstack(( queryDataNPADateTimeSorted[:,0], queryDataNPADateTimeSorted[:,1], 
                                        queryDataNPADateTimeSorted[:,2], queryDataNPADateTimeSorted[:,3], 
                                        longitudes,latitudes)).T
        if self.verbose:
            print "queryDataNPADateTimeSorted:"
            for i in xrange(10):
                print list(self.queryDataNPAdtsLL[i,:])
        return

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
        if self.verbose:
            print "WrangleMeteoParameter(%s): meteoDataStore[%s]="%(parameterName,parameterName) ,self.meteoDataStore[parameterName]


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
    
    def ProduceOutput(self, exportLonLat = True):
        self.CLASSPRINT(' writing outputCSVfile: %s' %(self.outputCSVfile) )
        
        # read and store header
        if self.limitTo>0:
            dataOut = self.dataUnsortedStr[:self.limitTo]
        else:
            dataOut = self.dataUnsortedStr
        
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
            print headerTextOutput        
        rowId = 0
        indicesqueryDataNPAdtsLL =  self.queryDataNPAdtsLL[:,0]
        for dc in self.dataColumns:
            rowIdStr = "%d"%rowId
            indexResults = np.where(indicesqueryDataNPAdtsLL == rowIdStr)
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
            if self.verbose:
                print dataAppendStr
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


    
    
