import wrangler.wrangleCSV_NCDF as wrangler
from pywps.Process import WPSProcess

import sys, logging, json, types, os, time, tempfile, uuid

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

        self.outputURL = self.addLiteralOutput(identifier="outputURL",
                                               title="The url to the output CSV file",
                                               type="String")
        self.percentComplete = 0

    def statusCallback(self, message, percentComplete=0):
        self.percentComplete += percentComplete
        if self.percentComplete >= 100: self.percentComplete = 100
        self.status.set(message, self.percentComplete)
        time.sleep(0.5)
        
    def execute(self):

        inputCSVPath = self.inputCSVPath.getValue()
        inputCSVPath_t = os.path.splitext(inputCSVPath)
        outputFileName = inputCSVPath_t[0]+"_metadata.json"
        descCSVPath = self.descCSVPath.getValue()

        currentBasket = inputCSVPath_t[0]+"_"+time.strftime("%Y%m%dt%H%M%S"+"_")
        pathToBasket = os.environ['POF_OUTPUT_PATH']

        basket = tempfile.mkdtemp(prefix=currentBasket, dir=pathToBasket)
        urlToBasket  = os.environ['POF_OUTPUT_URL']+"/"+basket[len(pathToBasket):]

        dwp_dict = {"inputCSV":basket+"/../../"+inputCSVPath,
                    "metaCSV":basket+"/../../"+descCSVPath,
                    "statusCallback":self.statusCallback,
                    "scanOnly": True,
                    "logFile":basket+"/"+inputCSVPath_t[0]+".log"}
        try:
            dwp = wrangler.dataWranglerProcessor()
            dwp.Initialize(dwp_dict)
            dwp.ReadInputCSV()

            csvMetaData = {} ## JSON data object
            csvMetaData.update(dwp.GetTimeRangeOfData())
            csvMetaData["name"] = str(uuid.uuid4())
            csvMetaData["title"] = inputCSVPath_t[0]
            csvMetaData["datatype"] = "POINT"
            csvMetaData["projection"] = dwp.GetProjectionString()
            csvMetaData.update(dwp.GetLatLonBBOXOfData())
            csvMetaData["source"] = "OBSERVATION"

            metaCSVFile = open(basket+"/"+outputFileName, "w")
            json.dump(csvMetaData, metaCSVFile)
            metaCSVFile.close()
            
        except Exception, e:
            self.status.set(e, 500)
            raise Exception(e)
            return 1

        self.outputURL.setValue(urlToBasket+"/"+outputFileName)
        self.status.set("Ready", 100)
