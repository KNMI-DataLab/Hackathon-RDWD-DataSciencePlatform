# Installation notes for the Data Science Platform WPS process.

Set the ADAGUCSERVICES environment variable to the directory where the PyWPS
distribution has been unpacked and where the adaguc-services expects to find
PyWPS.

1. cd ${ADAGUCSERVICES}/src
1. Checkout the repository next to the the PyWPS distribution:
```git clone https://github.com/KNMI/Hackathon-RDWD-DataSciencePlatform.git dsp_wps```
1. Edit $HOME/adaguc-services-config.xml and set the pywpsprocessesdir to
the directory where you cloned the WPS process code (dsp_wps in this
example) and add the wps subdirectory (dsp_wps/wps).
1. For now, copy the example files under dsp_wps/wps/wrangler/data/ to the
the basket directory: ${ADAGUCSERVICES}/data/adaguc-services-space/<user>/data
1. Run the adaguc-service from Eclipse/IntelliJ/whatever.
1. Enter the following URL replacing yourhostname:
> https://yourhostname:8090/wps?service=wps&reqest=Execute&identifier=wrangleProcess&version=1.0.0&DataInputs=inputCSVPath=ExportOngevalsData100lines.csv;metaCSVPath=metaDataCsv.json;jobDescPath=jobDesc.json

