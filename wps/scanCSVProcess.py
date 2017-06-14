import wrangler.wrangleCSV_NCDF as wrangler
from pywps.Process import WPSProcess

import sys, logging, json, types, os, time

class ScanCSVProcess(WPSProcess):
    def __init__(self):
        # init process
        WPSProcess.__init__(self,
                            identifier = "scanCSVProcess", # must be same, as filename
                            title="Scan CSV process",
                            version = "0.1",
                            storeSupported = "true",
                            statusSupported = "true",
                            abstract=("The scan process accepts the relative path of a CSV file "
                                      "in the basket, as well as a relative path to the metadata "
                                      "description of the CSV file and a description of the process."
                                      "The result of the process is a meta data file that is"
                                      "of the same format as a CF NetCDF file."),
                            grassLocation =False)

        self.inputCSVPath = self.addLiteralInput(identifier = "inputCSVPath",
                                                 title = "The path/URL to the input CSV file which needs to be wrangled",
                                                 type="String")

        self.descCSVPath = self.addLiteralInput(identifier="descCSVPath",
                                                title="The path to the metadata describing the CSV file in JSON format",
                                                type="String")

        self.jobDescPath = self.addLiteralInput(identifier="jobDescPath",
                                                title="A path to the description of the parameters which should be added to the input CSV",
                                                type="String")

        self.outputURL = self.addLiteralOutput(identifier="outputURL",
                                               title="The url to the output CSV file",
                                               type="String")
    def execute(self):

        inputCSVPath = self.inputCSVPath.getValue()
        inputCSVPath_t = os.path.splitext(inputCSVPath)
        outputFileName = inputCSVPath_t[0]+"_metadata.json"
        descCSVPath = self.descCSVPath.getValue()
        jobDescPath = self.jobDescPath.getValue()

        pathToBasket = os.environ['POF_OUTPUT_PATH']
        urlToBasket  = os.environ['POF_OUTPUT_URL']

        dwp_dict = {"inputCSV":pathToBasket+"/../"+inputCSVPath,
                    "metaCSV":pathToBasket+"/../"+descCSVPath,
                    "jobDesc":pathToBasket+"/../"+jobDescPath,
                    "logFile":pathToBasket+inputCSVPath_t[0]+".log"}
        try:
            dwp = wrangler.dataWranglerProcessor()
            dwp.Initialize(dwp_dict)
            dwp.ReadInputCSV()

            csvMetaData = {} ## JSON data object
            csvMetaData.update(dwp.GetTimeRangeOfData())
            csvMetaData["observationType"] = "point"
            csvMetaData["projString"] = dwp.GetProjectionString()
            csvMetaData.update(dwp.GetLatLonBBOXOfData())

            metaCSVFile = open(pathToBasket+"/"+outputFileName, "w")
            json.dump(csvMetaData, metaCSVFile)
            metaCSVFile.close()
            
        except Exception, e:
            self.status.set(e, 500)
            raise Exception(e)
            return 1

        self.outputURL.setValue(urlToBasket+outputFileName)
        self.status.set("Ready", 100)
