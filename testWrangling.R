library(ncdf4)
library(data.table)
#fileH<-nc_open("data/INTER_OPER_R___FG1_____L3__20150322T000000_20150322T001000_0001.nc")


#loading NetCDF file of wind
fileFull<-nc_open("data/mergedWindDataSWOV.nc")

#reading in the variables
lat<-ncvar_get(fileFull,"lat")
lon<-ncvar_get(fileFull,"lon")
x<-ncvar_get(fileFull, "x")
y<-ncvar_get(fileFull, "y")
dataGrid<-ncvar_get(fileFull,"grid")
time<-ncvar_get(fileFull,"time")

#specify the time origin accoding to the NetCDF info
time<-as.POSIXct(time, origin = "1950-01-01", tz = "UTC")

#putting the time in a data table to use for coupling the data and keep track of index
timeDT<-data.table(time,1:length(time), time)
setnames(timeDT,c("timeToCouple","timeIndex","timeWindUTC"))
setkey(timeDT,"timeToCouple")


#dataAccidents<-read.csv("data/accidents/ExportOngevalsData.csv")
#dataAccidents$date<-as.Date(dataAccidents$datum, "%d%b%y")
#levels(dataAccidents$Uur)[levels(dataAccidents$Uur)=='Onbekend'] <- NA
#dataAccidents$hour<-sapply(dataAccidents$Uur, function(y) strsplit(as.character(y),"\\.")[[1]][1])

#loading the data from a Rdata file (provided by the partner in Rdata format this time)
load("data/accidents/weekdataFiets.Rdata")
dataAccidentsBike<-weekdataFiets

#reconstricting the time from some scattered columns
dataAccidentsBike$datetime<-paste0(dataAccidentsBike$datum, ' ', dataAccidentsBike$Uur,":",dataAccidentsBike$minuut,":00")
#set the time to a posix object that will take care of the time zone
dataAccidentsBike$datetime<-as.POSIXct(dataAccidentsBike$datetime, tz="Europe/Amsterdam", format = "%Y-%m-%d %H:%M:%S")

dataAccidentsBike<-data.table(dataAccidentsBike)
setkey(dataAccidentsBike,'datetime')

#merging the time of the accident to the nearest time of observation 
dataAccidentsBike<-timeDT[dataAccidentsBike, roll='nearest']

##wrangling spatially the accidents and the wind speed
xvar<-dataAccidentsBike$X
yvar<-dataAccidentsBike$Y

xmin<-min(x)
ymin<-min(y)

cellSize<-x[[2]]-x[[1]] #constant size square grid is assumed in this projection
offsetGrid<-cellSize/2 #x,y coordinate of the grid are mid points

xcell<-ceiling((xvar-xmin+offsetGrid)/cellSize)#getting to the NetCDF array cell from the coordinate
ycell<-ceiling((yvar-ymin+offsetGrid)/cellSize)#getting to the NetCDF array cell from the coordinate
wind<-dataGrid[cbind(xcell,ycell,dataAccidentsBike$timeIndex)]#extracting the data in space and time from the whole NetCDF array

dataAccidentsBike<-cbind(dataAccidentsBike,wind)



# dataFolderWind<-"/nobackup/users/pagani/gsie/output/pagani/wind10min/Fg_1_oper_v0001/2015/"
# 
# nc.files<-list.files(dataFolderWind,pattern="*.nc",full.names=T, recursive = T)
# lapply()
# st2<-stack(nc.files,varname=c("time"))

