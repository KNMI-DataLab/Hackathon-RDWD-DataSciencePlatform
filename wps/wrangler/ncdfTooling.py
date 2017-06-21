import netCDF4 as ncdf
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
from dataObjectBase import dataObjectBase

class ncdfDataObject(dataObjectBase):
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
        
    def SetDataURL(self, dataURL):
        self.dataURL = dataURL

    def OpenMetaData(self):
        self.CLASSPRINT('reading netCDF file (headers): %s' %(self.dataURL) )
        
        self.metaData = ncdf.Dataset(self.dataURL)

        self.CLASSPRINT("*******************************************")
        self.CLASSPRINT("*****    Reading time-dimension .... ******")
        self.CLASSPRINT("*******************************************")

        self.timeDim = self.metaData.variables['time'].shape[0]
        # NOTE: The following reads all the time-indices of the entire file. This may take some times (3-4 seconds).
        self.timeArray = self.metaData.variables['time'][:]
        
        self.timeUnits = self.metaData.variables['time'].units
        self.dateTimeArray = ncdf.num2date(self.timeArray,self.timeUnits,calendar='standard')

        # The date-time present in the NetCDF is (must be) in UTC
        self.minDateTime = np.min(self.dateTimeArray).replace(tzinfo=pytz.UTC)
        self.maxDateTime = np.max(self.dateTimeArray).replace(tzinfo=pytz.UTC)

        #fmt = '%Y-%m-%d %H:%M:%S %Z (%z)'
        fmt = '%Y-%m-%d %H:%M:%S %Z'
        self.minDateTime_str = self.minDateTime.strftime(fmt)
        self.maxDateTime_str = self.maxDateTime.strftime(fmt)
        #print (self.minDateTime_str,self.maxDateTime_str)
        self.deltaTime = int( (self.dateTimeArray[1] - self.dateTimeArray[0]).total_seconds() )  # datetime.timedelta(0, 300)
                
        self.CLASSPRINT("##########################################################")
        self.CLASSPRINT("minDateTime=%s, maxDateTime=%s, deltaTime=%d" %(self.minDateTime_str, self.maxDateTime_str, self.deltaTime)  )
        self.CLASSPRINT("##########################################################")
        
        self.xDim = self.metaData.variables['x'].shape[0]
        self.yDim = self.metaData.variables['y'].shape[0]
        # 1D dimensional arrays; x/y-axes only
        self.xAxis = self.metaData.variables['x'][:]
        self.yAxis = self.metaData.variables['y'][:]
        
        # TODO:  Check if the data is grid-data or point-data!
        # The code below is grid-data specific.
        # We have to make a 2D grid from the x/y-axes.
        # meschgrid works like this:
        # gridded_lons, gridded_lats = np.meshgrid(lons_axis, lats_axis)
        self.xcoords, self.ycoords  = np.meshgrid(self.xAxis, self.yAxis)
        #print self.xcoords.shape, self.xcoords
        #print self.ycoords.shape, self.ycoords
        #print self.xcoords.shape, self.ycoords.shape
        
        # aquire projection (proj4) string
        self.projFuncDefstring = self.metaData.variables['projection'].proj4_params
        self.projectionFunction = pyproj.Proj(self.projFuncDefstring)

        # Both longitudes and latitudes are 2D arrays of the (RADAR) grid
        (self.longitudes,self.latitudes) = self.UnProject2LongitudeLatitudes(self.xcoords, self.ycoords)

        #print self.longitudes.shape, self.longitudes
        #print self.latitudes.shape, self.latitudes
        #print self.longitudes.shape, self.latitudes.shape
        
        self.gridSize = self.xDim * self.yDim
        # Both gives same result ...
        #np.hstack((self.longitudes.reshape(self.gridSize,1),self.latitudes.reshape(self.gridSize,1)))
        #np.vstack((self.longitudes,self.latitudes)).T
        
        # Flatten 2d arrays of lons & lats into 1d arrays and stack it into one 2d array [  [lon,lat] ... x self.gridSize .. ]
        self.lonLatStacked = np.hstack((self.longitudes.reshape(self.gridSize,1),self.latitudes.reshape(self.gridSize,1)))
        #print self.lonLatStacked

        ## Determine the bounding box.
        self.llbox_west = np.min(self.longitudes)
        self.llbox_east = np.max(self.longitudes)
        self.llbox_north = np.max(self.latitudes)
        self.llbox_south = np.min(self.latitudes)
        self.CLASSPRINT("*******************************************")
        self.CLASSPRINT("*****    Computing LON-LAT ....      ******")
        self.CLASSPRINT("*******************************************")

        self.CLASSPRINT("##########################################################")
        self.CLASSPRINT("Grid-dimensions: ", (self.xDim, self.yDim) )
        self.CLASSPRINT("LATLON-BBOX (west, east, north, south):", (self.llbox_west,self.llbox_east,self.llbox_north,self.llbox_south) )
        self.CLASSPRINT("##########################################################")
        
    def GetGridDimensions(self):
        ''' 
        Returns json: [ 256, 256 ]
        Note: Read in the  function: OpenMetaData()
        '''
        return [self.xDim, self.yDim]
    
       
    def FindClosestLonLatPointIndex(self, lon, lat):
        #  This works with 2D array self.lonLatStacked: [ [lon,lat], ... ]
        printProgress("Computing distance %s: to lat-lon-grid [%dx%d]; gridSize=%d" %(str((lon, lat)),self.xDim,self.yDim,self.gridSize))
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

    def GetVariable(self, variableName):
        keylist = ndo.metaData.variables.keys()
        variableFound = None
        for k in keylist:
            try:
                if ndo.metaData.variables[k].standard_name == variableName:
                    #  ndo.metaData.variables['image1_image_data'].standard_name == variableName ..
                    variableFound = ndo.metaData.variables[k]
            except:
                pass
        return variableFound
        
        
    def GetDataAtIndex(self, timeIndex, dataIndex, variableName="precipitation_amount"):
        '''
        NOTE: dataIndex is 1D index to the data.
        For 2D arrays the 2D index has to be computed from the 1D index...
        
        self.longitudes .. 2D array [256][256]
        self.latitudes .. 2D array  [256][256]
        
        self.lonLatStacked [256 * 256] of [ lon, lat ]
        
        print ndo.metaData.variables['image1_image_data']
        
        <type 'netCDF4._netCDF4.Variable'>
        uint16 image1_image_data(time, y, x)
            VERSION: 1.2
            grid_mapping: projection
            units: kg m-2
            _FillValue: 65535
            standard_name: precipitation_amount
            comment: Original units are in mm
            scale_factor: 0.01
            add_offset: 0.0
        unlimited dimensions: time
        current shape = (1051776, 256, 256)
        filling on
        
        ''' 
        idX = dataIndex % self.xDim
        idY = dataIndex / self.xDim
        keylist = ndo.metaData.variables.keys()
        variableFound = self.GetVariable(variableName)
        if variableFound:
            dataValue = variableFound[timeIndex][idY][idX]
            printProgress("givenDateTime=%s, closestDateTimeIndex=%d, query(lon, lat)=%s, minDistanceDataIndex=%d, dataValue=%f %s"
            %(str(givenDateTime), timeIndex, str((lon, lat)), dataIndex, float(dataValue), variableFound.units) )
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
    
    ndo = ncdfDataObject()
    #ndo.SetDataURL(r"http://opendap.knmi.nl/knmi/thredds/dodsC/DATALAB/hackathon/radarFull2015.nc")
    ndo.SetDataURL(r"http://opendap.knmi.nl/knmi/thredds/dodsC/DATALAB/hackathon/radarFullWholeData.nc")
    #ndo.SetDataURL("/visdataATX/hackathon/radarFullWholeData.nc")
    ndo.OpenMetaData()
    
    
    # MANUAL EXAMPLE of WRANGLING:
    # 1) Identify the closest time 
    fmt = '%Y-%m-%d %H:%M:%S %Z'
    givenDateTime = datetime.strptime('2010-07-14 16:33:00 UTC',fmt)
    closestDateTimeIndex = ndo.FindClosestDateTimeIndex(givenDateTime)
    
    # 2) Find the closest grid-point of the data
    (lon, lat) = (5.2, 52.0)
    minDistanceDataIndex = ndo.FindClosestLonLatPointIndex(lon, lat)
    
    # 3) Get the data the closest-time and the closest grid-point
    dataValue = ndo.GetDataAtIndex(closestDateTimeIndex, minDistanceDataIndex, variableName="precipitation_amount")
    
    sys.exit(0)
    
    variableFound = ndo.GetVariable(variableName="precipitation_amount")
    # same as above
    #variableFound = ndo.metaData.variables['image1_image_data']
    dataValue = variableFound[closestDateTimeIndex, minDistanceDataIndex/256, minDistanceDataIndex%256] 
    print "dataValue=",dataValue,variableFound.units
    # dataValue= 0.59 kg m-2





'''
OPENDAP URL to testdata:

http://opendap.knmi.nl/knmi/thredds/dodsC/DATALAB/hackathon/radarFullWholeData.nc

# Note: the URL below is NOT correct for OpenDAP acces;
# http://opendap.knmi.nl/knmi/thredds/fileServer/DATALAB/hackathon/radarFullWholeData.nc

field of interest: "precipitation_amount"



[~/develop/sciencePlatform] >ncdump -t "http://opendap.knmi.nl/knmi/thredds/dodsC/DATALAB/hackathon/radarFull2015.nc?time[20:1:30]"

 time = "2015-01-01 01:35:0.000000", "2015-01-01 01:39:60.000000", 
    "2015-01-01 01:45", "2015-01-01 01:50:0.000000", 
    "2015-01-01 01:54:60.000000", "2015-01-01 02", 
    "2015-01-01 02:05:0.000000", "2015-01-01 02:09:60.000000", 
    "2015-01-01 02:15", "2015-01-01 02:20:0.000000", 
    "2015-01-01 02:24:60.000000" ;


[~/develop/sciencePlatform] >ncdump -t "http://opendap.knmi.nl/knmi/thredds/dodsC/DATALAB/hackathon/radarFull2015.nc?image1_image_data[100][100][100],time[100],x[100],y[100]"

 time = "2015-01-01 08:15" ;

 x = 251.25 ;

 y = -3978.51501464844 ;

 image1_image_data =
  _ ;
}

[~/develop/sciencePlatform] >ncdump -t "http://opendap.knmi.nl/knmi/thredds/dodsC/DATALAB/hackathon/radarFull2015.nc?time[1:1:20]"

netcdf radarFull2015 {
dimensions:
    time = UNLIMITED ; // (105120 currently)
variables:
    double time(time) ;
        time:units = "seconds since 2000-01-01 00:00:00" ;
        time:standard_name = "time" ;
        time:calendar = "standard" ;
        time:long_name = "time" ;
        time:_ChunkSize = 524288 ;
...

data:

 time = "2015-01-01", "2015-01-01 00:05:0.000000", 
    "2015-01-01 00:09:60.000000", "2015-01-01 00:15", 
    "2015-01-01 00:20:0.000000", "2015-01-01 00:24:60.000000", 
    "2015-01-01 00:30", "2015-01-01 00:35:0.000000", 
    "2015-01-01 00:39:60.000000", "2015-01-01 00:45", 
    "2015-01-01 00:50:0.000000", "2015-01-01 00:54:60.000000", 
    "2015-01-01 01", "2015-01-01 01:05:0.000000", 
    "2015-01-01 01:09:60.000000", "2015-01-01 01:15", 
    "2015-01-01 01:20:0.000000", "2015-01-01 01:24:60.000000", 
    "2015-01-01 01:30", "2015-01-01 01:35:0.000000" ;
}

[~/develop/sciencePlatform] >ncdump -v x http://opendap.knmi.nl/knmi/thredds/dodsC/DATALAB/hackathon/radarFull2015.nc

dimensions:
	time = UNLIMITED ; // (105120 currently)
	maxStrlen64 = 64 ;
	x = 256 ;
	y = 256 ;
...
data:

 x = 1.25, 3.75, 6.25, 8.75, 11.25, 13.75, 16.25, 18.75, 21.25, 23.75, 26.25, 
    28.75, 31.25, 33.75, 36.25, 38.75, 41.25, 43.75, 46.25, 48.75, 51.25, 
    53.75, 56.25, 58.75, 61.25, 63.75, 66.25, 68.75, 71.25, 73.75, 76.25, 
    78.75, 81.25, 83.75, 86.25, 88.75, 91.25, 93.75, 96.25, 98.75, 101.25, 
...
    598.75, 601.25, 603.75, 606.25, 608.75, 611.25, 613.75, 616.25, 618.75, 
    621.25, 623.75, 626.25, 628.75, 631.25, 633.75, 636.25, 638.75 ;
}

[~/develop/sciencePlatform] >ncdump -v y http://opendap.knmi.nl/knmi/thredds/dodsC/DATALAB/hackathon/radarFull2015.nc

dimensions:
	time = UNLIMITED ; // (105120 currently)
	maxStrlen64 = 64 ;
	x = 256 ;
	y = 256 ;
...
data:

 y = -3728.51501464844, -3731.01501464844, -3733.51501464844, 
    -3736.01501464844, -3738.51501464844, -3741.01501464844, 
    -3743.51501464844, -3746.01501464844, -3748.51501464844, 
....
    -4351.01501464844, -4353.51501464844, -4356.01501464844, 
    -4358.51501464844, -4361.01501464844, -4363.51501464844, -4366.01501464844 ;
}
[bhwatx2] [~/develop/sciencePlatform] >ncdump -v y http://opendap.knmi.nl/knmi/thredds/dodsC/DATALAB/hackathon/radarFull2015.nc

ndo.metaData.variables['geographic']
Out[24]: 
<type 'netCDF4._netCDF4.Variable'>
|S1 geographic()
    geo_number_rows: 256
    geo_number_columns: 256
    geo_pixel_size_x: 2.5
    geo_pixel_size_y: -2.5
    geo_dim_pixel: KM,KM
    geo_column_offset: 0.0
    geo_row_offset: 1490.91
    geo_pixel_def: LU
    geo_product_corners: [  0.          49.76900101   0.          55.29600143   9.74300003
  54.81800079   8.33699989  49.3730011 ]
    long_name: geographic


[~/develop/sciencePlatform] >ncdump -h http://opendap.knmi.nl/knmi/thredds/dodsC/DATALAB/hackathon/radarFull2015.nc\?image1_image_data

netcdf radarFull2015 {
dimensions:
    time = UNLIMITED ; // (105120 currently)
    x = 256 ;
    y = 256 ;
variables:
    short image1_image_data(time, y, x) ;
        image1_image_data:VERSION = "1.2" ;
        image1_image_data:grid_mapping = "projection" ;
        image1_image_data:units = "kg m-2" ;
        image1_image_data:_FillValue = -1s ;
        image1_image_data:standard_name = "precipitation_amount" ;
        image1_image_data:comment = "Original units are in mm" ;
        image1_image_data:scale_factor = 0.01f ;
        image1_image_data:add_offset = 0.f ;
        image1_image_data:_Unsigned = "true" ;
        image1_image_data:_ChunkSize = 1, 256, 256 ;

// global attributes:
        :_NCProperties = "version=1|netcdflibversion=4.4.1.1|hdf5libversion=1.8.17" ;
        :Conventions = "CF-1.5" ;
        :history = "Mon Jun 12 13:58:52 2017: ncks --mk_rec_dmn time ./2015/01/RAD_NL21_RAC_MFBS_5min_201501010000.nc ./2015/01/RAD_NL21_RAC_MFBS_5min_201501010000.nc.nc\n",
            "Metadata adjusted by ADAGUC from KNMIHDF5 to NetCDF-CF" ;
        :NCO = "4.4.4" ;
        :DODS.strlen = 0 ;
        :DODS_EXTRA.Unlimited_Dimension = "time" ;
        :geographic.geo_number_rows = 256 ;
        :geographic.geo_number_columns = 256 ;
        :geographic.geo_pixel_size_x = 2.5f ;
        :geographic.geo_pixel_size_y = -2.5f ;
        :geographic.geo_dim_pixel = "KM,KM" ;
        :geographic.geo_column_offset = 0.f ;
        :geographic.geo_row_offset = 1490.906f ;
        :geographic.geo_pixel_def = "LU" ;
        :geographic.geo_product_corners = 0.f, 49.769f, 0.f, 55.296f, 9.743f, 54.818f, 8.337f, 49.373f ;
        :geographic.long_name = "geographic" ;
        :geographic.DODS.strlen = 0 ;
        :image1_calibration.calibration_flag = "Y" ;
        :image1_calibration.calibration_formulas = "GEO=0.01*PV+0.0" ;
        :image1_calibration.calibration_missing_data = 65535 ;
        :image1_calibration.calibration_out_of_image = 65535 ;
        :image1_calibration.long_name = "Scale and offset description" ;
        :image1_calibration.DODS.strlen = 0 ;
        :iso_dataset.min_x = "0.000000f" ;
        :iso_dataset.min_y = "49.769001f" ;
        :iso_dataset.max_x = "9.743000f" ;
        :iso_dataset.max_y = "54.818001f" ;
        :iso_dataset.long_name = "Iso 19115 metadata description" ;
        :iso_dataset.title = "RAD_NL21_RAC_MFBS_5min" ;
        :iso_dataset.abstract = "Composites of 5 min precipitation depths adjusted using data from rain gauge networks." ;
        :iso_dataset.status = "ongoing" ;
        :iso_dataset.type = "dataset" ;
        :iso_dataset.uid = "8562936d-f50f-4c37-9ed7-7803d8a9bd69" ;
        :iso_dataset.topic = "atmosphere" ;
        :iso_dataset.keyword = "Precipitation" ;
        :iso_dataset.temporal_extent = "1998-01-01 - ongoing" ;
        :iso_dataset.date = "2014-02-27" ;
        :iso_dataset.dateType = "publication date" ;
        :iso_dataset.statement = "Climatological radar rainfall dataset of 5 min precipitation depths, which have been adjusted employing rain gauge data from both KNMI rain gauge networks. Based on the composites of radar reflectivities from both KNMI weather radars (De Bilt at 52.103 NB, 5.179 OL and Den Helder at 52.955 NB, 4.79 OL; 1998 to January 2001 only De Bilt). The time in the file name is the time of the end of the observation. Data are available for the entire land surface of the Netherlands. Pixels have 2.4 km spatial resolution." ;
        :iso_dataset.code = "Not applicable" ;
        :iso_dataset.codeSpace = "EPSG" ;
        :iso_dataset.accessConstraints = "none" ;
        :iso_dataset.useLimitation = "none" ;
        :iso_dataset.organisationName_dataset = "Royal Netherlands Meteorological Institute (KNMI)" ;
        :iso_dataset.email_dataset = "overeem@knmi.nl, adaguc@knmi.nl" ;
        :iso_dataset.role_dataset = "pointOfContact" ;
        :iso_dataset.metadata_id = "70a9b201-f4e1-43d9-a020-1b23d03bafc4" ;
        :iso_dataset.organisationName_metadata = "Royal Netherlands Meteorological Institute (KNMI)" ;
        :iso_dataset.role_metadata = "pointOfContact" ;
        :iso_dataset.email_metadata = "adaguc@knmi.nl" ;
        :iso_dataset.url_metadata = "http://adaguc.knmi.nl" ;
        :iso_dataset.datestamp = "2014-02-27" ;
        :iso_dataset.language = "eng" ;
        :iso_dataset.metadataStandardName = "ISO 19115" ;
        :iso_dataset.metadataStandardNameVersion = "Nederlandse metadatastandaard op ISO 19115 voor geografie 1.2" ;
        :iso_dataset.DODS.strlen = 0 ;
        :overview.products_missing = "NA" ;
        :overview.hdftag_version_number = "3.5" ;
        :overview.number_satellite_groups = 0 ;
        :overview.number_station_groups = 0 ;
        :overview.number_radar_groups = 2 ;
        :overview.number_image_groups = 1 ;
        :overview.product_group_name = "RAD_NL21_RAC_MFBS_5min" ;
        :overview.product_datetime_start = "31-DEC-2014;23:55:00.000" ;
        :overview.product_datetime_end = "01-JAN-2015;00:00:00.000" ;
        :overview.long_name = "Date time description" ;
        :overview.DODS.strlen = 0 ;
        :product.validity_start = "20141231T235500" ;
        :product.validity_stop = "20150101T000000" ;
        :product.long_name = "ADAGUC Data Products Standard" ;
        :product.units = "1" ;
        :product.ref_doc = "ADAGUC Data Products Standard" ;
        :product.ref_doc_version = "1.1" ;
        :product.format_version = "1.1" ;
        :product.originator = "Royal Netherlands Meteorological Institute (KNMI)" ;
        :product.type = "R" ;
        :product.acronym = "RAD_NL21_RAC_MFBS_5min" ;
        :product.level = "L2" ;
        :product.style = "camelCase" ;
        :product.DODS.strlen = 0 ;
        :projection.grid_mapping_name = "polar_stereographic" ;
        :projection.latitude_of_projection_origin = 90.f ;
        :projection.straight_vertical_longitude_from_pole = 0.f ;
        :projection.standard_parallel = 60.f ;
        :projection.false_easting = 0.f ;
        :projection.false_northing = 0.f ;
        :projection.semi_major_axis = 6378388.f ;
        :projection.semi_minor_axis = 6356912.f ;
        :projection.proj4_params = "+proj=stere +lat_0=90 +lon_0=0.0 +lat_ts=60.0 +a=6378.388 +b=6356.912 +x_0=0 +y_0=0" ;
        :projection.proj4_origin = "+proj=stere +lat_0=90.000000 +lon_0=0.000000 +lat_ts=60.000000 +a=6378.388000 +b=6356.912000 +x_0=0.000000 +y_0=0.000000" ;
        :projection.DODS.strlen = 0 ;
        :radar1.radar_location = 5.179f, 52.103f ;
        :radar1.radar_name = "De_Bilt" ;
        :radar1.radar_num_contrib = 56 ;
        :radar1.long_name = "Location of radar" ;
        :radar1.DODS.strlen = 0 ;
        :radar2.long_name = "Location of radar" ;
        :radar2.radar_location = 4.79f, 52.955f ;
        :radar2.radar_name = "Den_Helder" ;
        :radar2.radar_num_contrib = -924803925 ;
        :radar2.DODS.strlen = 0 ;
}

cs2cs +proj=stere +lat_0=90 +lon_0=0.0 +lat_ts=60.0 +a=6378.388 +b=6356.912 +x_0=0 +y_0=0 +to +proj=latlong +datum=WGS84

'''
