import sys
import os,re
import pprint


import json
import jsonSyntaxTester as jsTstr


JsonEncodingType = "utf-8"
#JsonEncodingType = "ISO-8859-1"

try:
    import commentjson
    MISSING_LIB_commentjson = False
except:
    MISSING_LIB_commentjson = True

import uuid

def InsertUUID2jsonDict(self, jsonDict):
    try:
        jsonDict["uuid"] = str(uuid.uuid1())
    except:
        pass

def enumerateLines(textLines):
    lineList = textLines.split('\n')
    lineListEN = list(enumerate(lineList,1))
    enumeratedLines = ""
    for lln in lineListEN:
        enumeratedLines +='\n'+ "%03d:" %(lln[0]) +lln[1]
    enumeratedLines +='\n'
    return enumeratedLines


def getJsonFromString(jsonStringOrDict, handlePrintFunc=None, _hookObject=None):
    if type(jsonStringOrDict) == type({}):
        # It is a (python) dictionary ...
        try:
            jsonString = json.dumps(jsonStringOrDict, encoding=JsonEncodingType)
            if _hookObject==None:
                jsonDict   = json.loads(jsonString)
            else:
                jsonDict   = json.loads(jsonString,object_hook=_hookObject)
            return jsonDict 
        except Exception as eStr:
            exceptionStr = "Cannot construct JSON  from: type=%s;\ngiven = %s;\nException: %s " % (str(type(jsonStringOrDict)), enumerateLines( str(jsonStringOrDict) ), str(eStr))
            (jsonOK, errMsg) = (False, exceptionStr)
            return None
    else:
        # It is a NOT dictionary ...
        if not ( (type("a") == type(jsonStringOrDict)) or (type(u'a') == type(jsonStringOrDict)) ):
            exceptionStr = "Cannot construct JSON from: type=%s;\ngiven=%s" %(str(type(jsonStringOrDict)), enumerateLines( str(jsonStringOrDict) ))
            jsonOK = False
            return None
        else:
            # Given object is a JSON STRING
            jsonString = jsonStringOrDict
            try:
                if _hookObject==None:
                    jsonDict = json.loads(jsonString, encoding=JsonEncodingType)
                else:
                    jsonDict = json.loads(jsonString, encoding=JsonEncodingType, object_hook=_hookObject)

                jsonOK = True
                return jsonDict 
                
            except Exception as eStr0:
                try:
                    # Java-styled Json strings give sometimes exceptions
                    jsonDict = eval(jsonString) # Therefore use here eval instead of json.loads()
                    jsonString = json.dumps(jsonDict, encoding=JsonEncodingType)
                    if _hookObject==None:
                        jsonDict = json.loads(jsonString, encoding=JsonEncodingType)
                    else:
                        jsonDict = json.loads(jsonString, encoding=JsonEncodingType, object_hook=_hookObject)
                    return jsonDict
                except Exception as eStr1:
                    exceptionStr = "Cannot construct JSON from: type=%s;\ngiven = %s;\nException0: %s;\nException1: %s " % (str(type(jsonStringOrDict)), enumerateLines( str(jsonStringOrDict) ), str(eStr0), str(eStr1))
                    jsonOK = False
                    return None


def _decode_list(data):
    rv = []
    for item in data:
        if isinstance(item, unicode):
            item = item.encode('utf-8')
        elif isinstance(item, list):
            item = _decode_list(item)
        elif isinstance(item, dict):
            item = _decode_dict(item)
        rv.append(item)
    return rv

def _decode_dict(data):
    rv = {}
    for key, value in data.iteritems():
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        elif isinstance(value, list):
            value = _decode_list(value)
        elif isinstance(value, dict):
            value = _decode_dict(value)
        rv[key] = value
    return rv

def printHandleJsonValidity(jsonOK, errMsg):
    #print "printHandleJsonValidity():", (jsonOK, errMsg)
    if jsonOK:
        #w3dxLog.PrintInfo("JSON is OK")
        pass
    else:
        w3dxLog.PrintError("JSON is WRONG!\n%s" %errMsg)

### Function: ReadJsonConfigurationFromFile:  ###
def ReadJsonConfigurationFromFile(jsonFileName):
    jsonFile = open(jsonFileName)
    jsonLines = jsonFile.readlines()
    # Note: object_hook takes care of removing unicode u".." where possible

    #print "MISSING_LIB_commentjson=",MISSING_LIB_commentjson
    if not MISSING_LIB_commentjson:
        jsonStr= ''.join(jsonLines)
        #print jsonStr
        #jsTstr.checkJsonValidity(jsonStringOrDict, handlePrintFunc=None, _hookObject=None)
        if not jsTstr.checkJsonValidity(jsonStr, handlePrintFunc=printHandleJsonValidity, _hookObject=_decode_dict):
            w3dxLog.PrintError("jsonStr is NOT VALID JSON!\n")
            #sys.exit(1)
            return None
        try:
            w3dxConfigDict = commentjson.loads(jsonStr, encoding=jsTstr.JsonEncodingType, object_hook=_decode_dict)
        except Exception as eStr:
            exceptionStr = "Cannot construct JSON  from: type=%s;\nException: %s;\ngiven = %s" % (str(type(jsonStr)), str(eStr), jsTstr.enumerateLines( str(jsonStr) ))
            (jsonOK, errMsg) = (False, exceptionStr)
            printHandleJsonValidity(jsonOK, errMsg)
            sys.exit(1)
            #return None
    else:
        # In case of missing library commentjson
        # Remove commented lines from json text
        jsonStr = ''
        for ln in jsonLines:
            ln0 = ln.strip()
            if (len(ln0)>0) and (ln0[0]!='#') and  (ln0!='\n'):
                jsonStr += ln
                        
        #  print jsonStr
        if not jsTstr.checkJsonValidity(jsonStr, handlePrintFunc=printHandleJsonValidity, _hookObject=_decode_dict):
            w3dxLog.PrintError("jsonStr is NOT VALID JSON!\n")
            #print "ERROR: jsonStr is NOT VALID JSON!\n"
            sys.exit(1)
            #return None
        try:
            w3dxConfigDict = json.loads(jsonStr, encoding=jsTstr.JsonEncodingType, object_hook=_decode_dict)
        except Exception as eStr:
            exceptionStr = "Cannot construct JSON  from: type=%s;\nException: %s;\ngiven = %s" % (str(type(jsonStr)), str(eStr), jsTstr.enumerateLines( str(jsonStr) ))
            (jsonOK, errMsg) = (False, exceptionStr)
            printHandleJsonValidity(jsonOK, errMsg)
            sys.exit(1)
            #return None

    return w3dxConfigDict

### Function: WriteJsonToFile(jsonFileOUT, jsonFileName)  ###
def WriteJsonToFile(jsonLines, jsonFileName):
    jsonFileOUT = open(jsonFileName, "wt")
    jsonFileOUT.write(jsonLines)
    jsonFileOUT.close()

