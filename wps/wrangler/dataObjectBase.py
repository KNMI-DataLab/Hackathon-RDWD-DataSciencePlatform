import numpy as np
import pytz
from datetime import datetime, timedelta
import sys, os
import time
import pyproj
from pyproj import Geod
import inspect
import csvTooling as csvT

geoTransfWGS84 = Geod(ellps='WGS84')
geoTransf = geoTransfWGS84

class dataObjectBase():

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
                csvT.printProgress( "[%s:%s] %s.%s() %s" %(pid,csvT.loggerName,self.__class__.__name__,inspect.stack()[1][3], ''.join(args)) )
            except:
                csvT.printProgress( "[%s:%s] %s.%s() %s" %(pid,csvT.loggerName,self.__class__.__name__,inspect.stack()[1][3], ''.join(map(str,args))  ) )

    def __init__(self):
        '''
        Constructor:
            dataTypeName: string
            storageType : "cache" or "archive"
        '''
        self.verbose = True
        self.verboseLevel = 0       
        self.projFuncDefstring =""
        self.deltaTime = 1        
        
    def __del__(self):
        #self.CLASSPRINT('..Destructor()..')
        pass
        
        
    def FindClosestDateTime(self, givenDateTime, dateTimeList):
        '''
        # This (*1*) will ensure this:
        # givenDateTime=2010-07-14 16:33:00, closestDateTime=2010-07-14 16:30:00,        
        # Otherwise it will only find positive timedelta
        # givenDateTime=2010-07-14 16:33:00, closestDateTime=2010-07-14 16:40:00, # wrong result
        
        givenDateTime: 2010-07-14 16:33:00
        [[-180.0 datetime.datetime(2010, 7, 14, 16, 30)]
         [420.0 datetime.datetime(2010, 7, 14, 16, 40)]
         [1020.0 datetime.datetime(2010, 7, 14, 16, 50)]
         [1620.0 datetime.datetime(2010, 7, 14, 17, 0)]
         [2220.0 datetime.datetime(2010, 7, 14, 17, 10)]
         [2820.0 datetime.datetime(2010, 7, 14, 17, 20)]
         [3420.0 datetime.datetime(2010, 7, 14, 17, 30)]
         [4020.0 datetime.datetime(2010, 7, 14, 17, 40)]
         [4620.0 datetime.datetime(2010, 7, 14, 17, 50)]
         [5220.0 datetime.datetime(2010, 7, 14, 18, 0)]]
        givenDateTime=2010-07-14 16:33:00, closestDateTime=2010-07-14 16:30:00, closestDateTimeIndex=238167        
        '''
        b_d = givenDateTime #  == datetime object
        b_d0 = b_d - timedelta(seconds=0.5*self.deltaTime)  # (*1*)
        deltaSecondsList=[]  # only for debugging
        #print "givenDateTime:",givenDateTime ,"deltaTime:", self.deltaTime, "b_d0:", b_d0
        def func(x):
            d =  x           #  #  == datetime object
            delta =  d - b_d if d > b_d0 else timedelta.max # (*1*)
            #delta =  d - b_d if d > b_d else timedelta.max 
            #deltaSeconds = delta.total_seconds()
            #deltaSecondsList.append([deltaSeconds, x])
            return delta
            
        minDateTime =  min(dateTimeList, key = func)
        #deltaSecondsArray = np.array(deltaSecondsList)
        #indexSort = np.argsort(deltaSecondsArray[:,0])
        #deltaSorted = deltaSecondsArray[indexSort]
        #print deltaSorted[:10]
        return minDateTime


        

    def FindClosestDateTimeIndex(self, givenDateTime):
        closestDateTime = self.FindClosestDateTime(givenDateTime, self.dateTimeArray)
        closestDateTimeIndex = np.where(self.dateTimeArray == closestDateTime)[0][0]  # np.where(self.dateTimeArray == closestDateTime) == gives= => [(array([2511]),)]
        csvT.printProgress("givenDateTime=%s, closestDateTime=%s, closestDateTimeIndex=%d" %(str(givenDateTime),str(closestDateTime),closestDateTimeIndex ))
        return closestDateTimeIndex

    def GetTimeRangeOfData(self):
        ''' 
        Returns json: { "minDateTime": "2005-12-31 23:55:00 UTC",
                        "maxDateTime": "2015-12-31 23:50:00 UTC",
                        "deltaTime": 300 
                        }
        Note: The actual range is compute in the function: OpenMetaData()
        '''
        dataRange =  {  "minDateTime": self.minDateTime,
                        "maxDateTime": self.maxDateTime,
                        "deltaTime":   self.deltaTime }
        return dataRange
    
    def GetLatLonBBOXOfData(self):
        ''' 
        Returns json: { "west": 0.01920864511118657,
                        "east": 8.3233501877260778,
                        "north": 55.285268133270826,
                        "south": 49.385066927714206
                     }
        Note: The actual range is compute in the function: OpenMetaData()
        '''
        LATLONBBOX =  { "west": self.llbox_west,
                        "east": self.llbox_east,
                        "north": self.llbox_north,
                        "south": self.llbox_south
                     }
        return LATLONBBOX

    def GetProjectionString(self):
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

    def FindClosestLonLatPointIndex(self, lon, lat):
        # overload in derived classes
        pass
        
    def GetVariable(self, variableName):
        # overload in derived classes
        pass
        
    def GetDataAtIndex(self, timeIndex, dataIndex, variableName="precipitation_amount"):
        # overload in derived classes
        pass
        

        
