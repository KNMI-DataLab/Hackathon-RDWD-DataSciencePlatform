#!/usr/bin/python
#***********************************************************************
#*       wrangleCSV-NCDF.py     Science Platform Project              **
#***********************************************************************
#*                      All rights reserved                           **
#*                    Copyright (c) 2017 KNMI                         **
#*             Royal Netherlands Meteorological Institute             **
#***********************************************************************
#* Demo-script : wrangleCSV-NCDF.py
#* Purpose     : On the basis of CSV (geo-locations and time) 
#*               extract from NetCDF data meteorological information
#* 
#* Usage      :  
#*
#* Project    : Science Platform Project
#*
#*
#* Developers:  Michal Koutek
#* Initial date:  20170612
#**********************************************************************
'''
[sciencePlatform/wrangler] >

python wrangleCSV-NCDF.py --inputCSV ./data/ExportOngevalsData100lines.csv --metaCSV ./data/metaDataCsv.json --jobDesc ./data/jobDesc.json --outputCSV ./output/meteoDataAdded.csv --limitTo 10
meld data/ExportOngevalsData10lines.csv output/meteoDataAdded.csv

python wrangleCSV-NCDF.py --inputCSV ./data/ExportOngevalsData.csv --metaCSV ./data/metaDataCsv.json --jobDesc ./data/jobDesc.json --outputCSV ./output/meteoDataAdded.csv 
python wrangleCSV-NCDF.py --inputCSV ./data/ExportOngevalsData.csv --metaCSV ./data/metaDataCsv.json --jobDesc ./data/jobDesc.json --outputCSV ./output/meteoDataAdded.csv --limitTo 10


'''

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


if __name__ == "__main__":
    home = os.path.expanduser("~")

    defaultDataDir = DATAPATH
    defaultLogFile = "./wranglerProcess.log"
    
    # Specify command line syntax and read/interpret command line.
    parser = OptionParser(usage="%prog [options] ",
                          epilog="")

    parser.add_option("--inputCSV", dest="inputCSV", metavar="STRING", default="",
                      help="Input CVS data of the user of the platform.")

    parser.add_option("--metaCSV", dest="metaCSV", metavar="STRING", default="",
                      help="Metadata describing the input CVS data in JSON format.")
    
    parser.add_option("--jobDesc", dest="jobDesc", metavar="STRING", default="",
                      help="Job describtion in JSON format.")

    parser.add_option("--outputCSV", dest="outputCSV", metavar="STRING", default="",
                      help="Output CVS data.")

    parser.add_option("--limitTo", dest="limitTo", metavar='N', type=int, default=-1,
                      help="Used to quickly wrangler just a few lines of the user input.")

    parser.add_option("-o", "--logfile", dest="logfile", metavar="STRING", default=defaultLogFile,
                      help="The path and name of the file used by this program for its logging output.")
                      
    parser.add_option("-l", "--loglevel", dest="loglevel", metavar="STRING", default="info",
                      help="The level of logging used by this program.")
                                            
    (options, args) = parser.parse_args()

    # Translate the logger settings from the command line and apply them.
    loglevel = logging.DEBUG
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
        
    if os.path.exists(options.logfile):
        try:
            os.remove(options.logfile)
        except:
            pass
    logging.basicConfig(filename=options.logfile, level=loglevel, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    logger = logging.getLogger('main')
    

    printProgress("*******************************************")
    printProgress("***** Wrangling Processing STARTED.  ******")
    printProgress("*******************************************")
    
    if not os.path.exists(options.inputCSV):
        printProgress("ERROR: inputCSV does NOT exists! %s " %(options.inputCSV))
        sys.exit(1)
        
    csvDataObj = csvT.cvsDataObject()
    csvDataObj.SetInputCSVFile(options.inputCSV)
    csvDataObj.SetInputMetaCSVFile(options.metaCSV)
    csvDataObj.SetJobDescriptionFile(options.jobDesc)
    
    if not os.path.exists("./output"):
        os.makedirs("./output")
    csvDataObj.SetOutputCSVFile(options.outputCSV)
    csvDataObj.ApplyLimit(options.limitTo)
    csvDataObj.ReadInputCSV()
    csvDataObj.WrangleMeteoParameter(parameterName = "temperature")
    csvDataObj.WrangleMeteoParameter(parameterName = "precipitation")
    csvDataObj.ProduceOutput()
    
    #printProgress("##### Processing time: %s  #####" %(datetimestr))        
    #printProgress("##### FINISHED time: %s  #####" %(datetimestr))        

    printProgress("*******************************************")
    printProgress("***** Wrangling Processing FINISHED. ******")
    printProgress("*******************************************")
    sys.exit(0)
    




