from pywps.Process import WPSProcess
import types
import os
import time
import wrangler.wrangleCSV_NCDF as wrangler
import sys, logging

class WrangleProcess(WPSProcess):
    def __init__(self):
        # init process
        WPSProcess.__init__(self,
                            identifier = "wrangleProcess", # must be same, as filename
                            title="Wrangle process",
                            version = "0.1",
                            storeSupported = "true",
                            statusSupported = "true",
                            abstract=("The wrangle process accepts the relative path of a CSV file in the basket, as well as a relative path to the metadata description of the CSV file and a description of the process."
                                      "The result of the process is a wrangled CSV file."),
                            grassLocation =False)

        self.inputCSVPath = self.addLiteralInput(identifier = "inputCSVPath",
                                                 title = "The path/URL to the input CSV file which needs to be wrangled",
                                                 type="String")

        self.metaCSVPath = self.addLiteralInput(identifier="metaCSVPath",
                                                title="The path to the metadata describing the CSV file in JSON format",
                                                type="String")

        self.jobDescPath = self.addLiteralInput(identifier="jobDescPath",
                                                title="A path to the description of the parameters which should be added to the input CSV",
                                                type="String")

        self.limit = self.addLiteralInput(identifier="limit",
                                          title="An optional limit in the amount of lines which should be processed",
                                          type=types.IntType,
                                          default=-1)

        self.outputURL = self.addLiteralOutput(identifier="outputURL",
                                               title="The url to the output CSV file",
                                               type="String")
    def execute(self):

        inputCSVPath = self.inputCSVPath.getValue()
        inputCSVPath_t = os.path.splitext(inputCSVPath)
        outputFileName = inputCSVPath_t[0]+"_wrangled"+inputCSVPath_t[1]
        metaCSVPath = self.metaCSVPath.getValue()
        jobDescPath = self.jobDescPath.getValue()
        limit = self.limit.getValue()

        pathToBasket = os.environ['POF_OUTPUT_PATH']
        urlToBasket  = os.environ['POF_OUTPUT_URL']

        dwp_dict = {"inputCSV":pathToBasket+"/../"+inputCSVPath,
                    "metaCSV":pathToBasket+"/../"+metaCSVPath,
                    "jobDesc":pathToBasket+"/../"+jobDescPath,
                    "logFile":pathToBasket+inputCSVPath_t[0]+".log",
                    "limitTo":limit}
        try:
            dwp = wrangler.dataWranglerProcessor()
            dwp.Initialize(dwp_dict)
            dwp.ReadInputCSV()
            dwp.WrangleWithNetCdfData({"outputCSV":pathToBasket+outputFileName})
        except Exception, e:
            self.status.set(e, 500)
            raise Exception(e)
            return 1

        self.outputURL.setValue(urlToBasket+outputFileName)
        self.status.set("Ready", 100)
