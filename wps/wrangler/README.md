Notes for development and testing of the "data-wrangler".

TEST-JOB:

``` sh
python wrangleCSV_NCDF.py --inputCSV ./data/ExportOngevalsData100lines.csv --metaCSV ./data/metaDataCsv.json --jobDesc ./data/jobDesc.json --outputCSV ./output/meteoDataAdded.csv --limitTo 10
meld data/ExportOngevalsData10lines.csv output/meteoDataAdded.csv
```

FULL-JOB:

``` sh
python wrangleCSV_NCDF.py --inputCSV ./data/ExportOngevalsData.csv --metaCSV ./data/metaDataCsv.json --jobDesc ./data/jobDesc.json --outputCSV ./output/meteoDataAddedFull.csv  --limitTo 10
python wrangleCSV_NCDF.py --inputCSV ./data/ExportOngevalsData.csv --metaCSV ./data/metaDataCsv.json --jobDesc ./data/jobDesc.json --outputCSV ./output/meteoDataAddedFull.csv

```

SCAN-JOB:

``` sh
python wrangleCSV_NCDF.py --scanOnly --inputCSV ./data/ExportOngevalsData.csv --metaCSV ./data/metaDataCsv.json --jobDesc ./data/jobDesc.json --outputCSV ./output/meteoDataAddedFull.csv 

[wps/wrangler] >time python wrangleCSV_NCDF.py --scanOnly --inputCSV ./data/ExportOngevalsData.csv --metaCSV ./data/metaDataCsv.json --jobDesc ./data/jobDesc.json --outputCSV ./output/meteoDataAddedFull.csv
WARNING: Could not remove file: ./output/meteoDataAddedFull.csv.log
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() reading metaCSVfile: ./data/metaDataCsv.json
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() {'columnMinute': 3, 'dateFormat': '%d%b%y', 'hourFormat': 'hourInterval', 'columnDate': 2, 'columnHour': 1, 'columnY': 10, 'minuteFormat': 'plainMinute', 'csvSeparator': ',', 'projString': '+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889 +k=0.9999079 +x_0=155000 +y_0=463000 +ellps=bessel +units=m +no_defs', 'timeZone': 'CET', 'columnX': 9, 'geoProjection': 'rijksDriehoek'}
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() reading inputCSVfile: ./data/ExportOngevalsData.csv
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() headerText=
Niveaukop,Uur,datum,minuut,ernong,N_Slacht_dood,N_Slacht_Zh,Aardong,loctypon,X,Y,N_Personenauto,N_Brom_snorfiets,N_Fiets,N_Voetganger
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() *******************************************
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() ***** Reading CSV-file STARTED.   *********
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() *******************************************
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() ***** Extracting data columns...  *********
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() *******************************************
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() ***** Reading CSV-file FINISHED.  *********
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() *******************************************
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() *******************************************
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() ***** Decoding date-time format STARTED. **
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() *******************************************
[21205:wrangleCSV_NCDF.py] csvDataObject.DecodeDateTime() WARNING: could not extract (hour & minute) from: "(Onbekend,Onbekend)"
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() WARNING: INVALID date-time specification at row-number: 23117; "['11DEC06' 'Onbekend' 'Onbekend' '263447.126' '575528.125']"
[21205:wrangleCSV_NCDF.py] csvDataObject.DecodeDateTime() WARNING: could not extract (hour & minute) from: "(Onbekend,Onbekend)"
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() WARNING: INVALID date-time specification at row-number: 26143; "['29JAN07' 'Onbekend' 'Onbekend' '254097' '569483']"
[21205:wrangleCSV_NCDF.py] csvDataObject.DecodeDateTime() WARNING: could not extract (hour & minute) from: "(Onbekend,Onbekend)"
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() WARNING: INVALID date-time specification at row-number: 26218; "['05FEB07' 'Onbekend' 'Onbekend' '129124.252' '426803.097']"
[21205:wrangleCSV_NCDF.py] csvDataObject.DecodeDateTime() WARNING: could not extract (hour & minute) from: "(Onbekend,Onbekend)"
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() WARNING: INVALID date-time specification at row-number: 31084; "['17MAR07' 'Onbekend' 'Onbekend' '255358.694' '570317.558']"
[21205:wrangleCSV_NCDF.py] csvDataObject.DecodeDateTime() WARNING: could not extract (hour & minute) from: "(Onbekend,Onbekend)"
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() WARNING: INVALID date-time specification at row-number: 32965; "['02APR07' 'Onbekend' 'Onbekend' '260257.384' '557465.849']"
....
[21205:wrangleCSV_NCDF.py] csvDataObject.DecodeDateTime() WARNING: could not extract (hour & minute) from: "(Onbekend,Onbekend)"
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() WARNING: INVALID date-time specification at row-number: 155686; "['23JAN15' 'Onbekend' 'Onbekend' '112727.557' '478625.286']"
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() *******************************************
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() ***** Decoding date-time format FINISHED.**
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() *******************************************
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() *******************************************
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() ***** Computing LON-LAT STARTED.     ******
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() *******************************************
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() *******************************************
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() ***** Computing LON-LAT FINISHED.    ******
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() *******************************************
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() ##########################################################
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() minDateTime=2005-12-31 23:14:00 UTC, maxDateTime=2015-12-31 22:38:00 UTC
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() LATLON-BBOX (west, east, north, south):(3.359861046500733, 7.220277659244057, 53.487478565722419, 50.755904620455084)
[21205:wrangleCSV_NCDF.py] csvDataObject.ReadInputCSV() ##########################################################
10.404u 4.268s 0:14.68 99.8%	0+0k 0+40io 0pf+0w

```

PREVIEW-JOB:

``` sh
python wrangleCSV_NCDF.py --inputCSV ./data/ExportOngevalsData.csv --metaCSV ./data/metaDataCsv.json --jobDesc ./data/jobDesc.json --outputCSV ./output/meteoDataAddedFull.csv 
```

######### TESTING example:  #############

``` sh
python wrangleCSV_NCDF.py --inputCSV ./data/ExportOngevalsData100lines.csv --metaCSV ./data/metaDataCsv.json --jobDesc ./data/jobDesc.json --outputCSV ./output/meteoDataAdded.csv --limitTo 10
```

> *******************************************
> ***** Wrangling Processing STARTED.  ******
> *******************************************
> [ 7818: 7818] cvsDataObject.ReadInputCSV()  reading metaCSVfile: ./data/metaDataCsv.json
> {'columnMinute': 3, 'dateFormat': '%d%b%y', 'hourFormat': 'hourInterval', 'columnDate': 2, 'columnHour': 1, 'columnY': 10, 'minuteFormat': 'plainMinute', 'csvSeparator': ',', 'projString': '+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889 +k=0.9999079 +x_0=155000 +y_0=463000 +ellps=bessel +units=m +no_defs', 'timeZone': 'CET', 'columnX': 9, 'geoProjection': 'rijksDriehoek'}
> [ 7818: 7818] cvsDataObject.ReadInputCSV()  reading inputCSVfile: ./data/ExportOngevalsData100lines.csv
> headerText=
> *****
> Niveaukop,Uur,datum,minuut,ernong,N_Slacht_dood,N_Slacht_Zh,Aardong,loctypon,X,Y,N_Personenauto,N_Brom_snorfiets,N_Fiets,N_Voetganger
> *****
> [['03JAN06' '1.00-01.59' '10' '111998' '516711']
>  ['07JAN06' '7.00-07.59' '50' '95069' '451430']
>  ['21JAN06' '11.00-11.59' '10' '119330.273' '486468.495']
>  ['13JAN06' '12.00-12.59' '45' '91939' '440711']
>  ['09JAN06' '18.00-18.59' '9' '190371.475' '324748.091']
>  ['03JAN06' '8.00-08.59' '15' '258048' '468749']
>  ['12JAN06' '17.00-17.59' '57' '174487' '318218']
>  ['24JAN06' '17.00-17.59' '0' '177750' '319033']
>  ['13JAN06' '22.00-22.59' '49' '158178.588' '380926.549']
>  ['30JAN06' '9.00-09.59' '57' '53498' '407783']]
> Decoding date-time format...
> 03JAN06 => 2006-01-03 00:00:00 ; 1.00-01.59 => 1.0 ; 2006-01-03 01:10:00 CET (+0100) => 2006-01-03 00:10:00 UTC (+0000)
> 07JAN06 => 2006-01-07 00:00:00 ; 7.00-07.59 => 7.0 ; 2006-01-07 07:50:00 CET (+0100) => 2006-01-07 06:50:00 UTC (+0000)
> 21JAN06 => 2006-01-21 00:00:00 ; 11.00-11.59 => 11.0 ; 2006-01-21 11:10:00 CET (+0100) => 2006-01-21 10:10:00 UTC (+0000)
> 13JAN06 => 2006-01-13 00:00:00 ; 12.00-12.59 => 12.0 ; 2006-01-13 12:45:00 CET (+0100) => 2006-01-13 11:45:00 UTC (+0000)
> 09JAN06 => 2006-01-09 00:00:00 ; 18.00-18.59 => 18.0 ; 2006-01-09 18:09:00 CET (+0100) => 2006-01-09 17:09:00 UTC (+0000)
> 03JAN06 => 2006-01-03 00:00:00 ; 8.00-08.59 => 8.0 ; 2006-01-03 08:15:00 CET (+0100) => 2006-01-03 07:15:00 UTC (+0000)
> 12JAN06 => 2006-01-12 00:00:00 ; 17.00-17.59 => 17.0 ; 2006-01-12 17:57:00 CET (+0100) => 2006-01-12 16:57:00 UTC (+0000)
> 24JAN06 => 2006-01-24 00:00:00 ; 17.00-17.59 => 17.0 ; 2006-01-24 17:00:00 CET (+0100) => 2006-01-24 16:00:00 UTC (+0000)
> 13JAN06 => 2006-01-13 00:00:00 ; 22.00-22.59 => 22.0 ; 2006-01-13 22:49:00 CET (+0100) => 2006-01-13 21:49:00 UTC (+0000)
> 30JAN06 => 2006-01-30 00:00:00 ; 9.00-09.59 => 9.0 ; 2006-01-30 09:57:00 CET (+0100) => 2006-01-30 08:57:00 UTC (+0000)
> ##########################################################
> minDateTime=2006-01-03 00:10:00 UTC, maxDateTime=2006-01-30 08:57:00 UTC
> LATLON-BBOX (west, east, north, south): (4.0161296310787549, 6.8340902374793098, 50.304600877017236, 48.4668502279617)
> ##########################################################
> queryDataNPAdt:
> ['0', '2006-01-03 00:10:00 UTC', '111998.0', '516711.0', '4.80027528245', '48.9994215447']
> ['5', '2006-01-03 07:15:00 UTC', '258048.0', '468749.0', '6.83409023748', '50.304600877']
> ['1', '2006-01-07 06:50:00 UTC', '95069.0', '451430.0', '4.57160470779', '48.8458532221']
> ['4', '2006-01-09 17:09:00 UTC', '190371.475', '324748.091', '5.87787666049', '49.7043104226']
> ['6', '2006-01-12 16:57:00 UTC', '174487.0', '318218.0', '5.6569228243', '49.5622693229']
> ['3', '2006-01-13 11:45:00 UTC', '91939.0', '440711.0', '4.5294829405', '48.8174104336']
> ['8', '2006-01-13 21:49:00 UTC', '158178.588', '380926.549', '5.43142933206', '49.4159962516']
> ['2', '2006-01-21 10:10:00 UTC', '119330.273', '486468.495', '4.89976378956', '49.0657933919']
> ['7', '2006-01-24 16:00:00 UTC', '177750.0', '319033.0', '5.70220454386', '49.5914822897']
> ['9', '2006-01-30 08:57:00 UTC', '53498.0', '407783.0', '4.01612963108', '48.466850228']
> WrangleMeteoParameter(temperature): meteoDataStore[temperature]= [ 273.22161811  274.65844541  277.40944022  278.88862053  278.66643919
>   275.22383444  274.42747159  275.58827778  277.94655194  273.75254489]
> WrangleMeteoParameter(precipitation): meteoDataStore[precipitation]= [  3.85489284  71.24108782  73.89865603  31.86614483  23.01585971
>   48.1115348    3.6190141   64.57047075   7.84414874  72.99356093]
> [ 7818: 7818] cvsDataObject.ProduceOutput()  writing outputCSVfile: ./output/meteoDataAdded.csv
> Niveaukop,Uur,datum,minuut,ernong,N_Slacht_dood,N_Slacht_Zh,Aardong,loctypon,X,Y,N_Personenauto,N_Brom_snorfiets,N_Fiets,N_Voetganger,longitude,latitude,precipitation,temperature
> Ongeval gekoppeld op gemeente niveau,1.00-01.59,03JAN06,10,Letsel,0,0,Vast voorwerp,Kruispunt,111998,516711,1,0,0,0,4.80027528245,48.9994215447,3.854893,273.221618
> Ongeval exact gekoppeld aan BN,7.00-07.59,07JAN06,50,Letsel,0,1,Frontaal,Kruispunt,95069,451430,1,0,0,0,4.57160470779,48.8458532221,73.898656,277.409440
> Ongeval exact gekoppeld aan BN,11.00-11.59,21JAN06,10,Letsel,0,0,Flank,Wegvak,119330.273,486468.495,1,0,0,0,4.89976378956,49.0657933919,64.570471,275.588278
> Ongeval exact gekoppeld aan BN,12.00-12.59,13JAN06,45,Letsel,0,0,Kop/staart,Kruispunt,91939,440711,2,0,0,0,4.5294829405,48.8174104336,48.111535,275.223834
> Ongeval exact gekoppeld aan BN,18.00-18.59,09JAN06,9,Letsel,0,0,Vast voorwerp,Wegvak,190371.475,324748.091,0,0,0,0,5.87787666049,49.7043104226,31.866145,278.888621
> Ongeval exact gekoppeld aan BN,8.00-08.59,03JAN06,15,Letsel,0,0,Flank,Kruispunt,258048,468749,1,0,1,0,6.83409023748,50.304600877,71.241088,274.658445
> Ongeval exact gekoppeld aan BN,17.00-17.59,12JAN06,57,Letsel,0,0,Kop/staart,Kruispunt,174487,318218,2,0,0,0,5.6569228243,49.5622693229,23.015860,278.666439
> Ongeval exact gekoppeld aan BN,17.00-17.59,24JAN06,0,Letsel,0,0,Flank,Kruispunt,177750,319033,1,1,0,0,5.70220454386,49.5914822897,7.844149,277.946552
> Ongeval gekoppeld op straat niveau,22.00-22.59,13JAN06,49,Letsel,0,0,Voetganger,Wegvak,158178.588,380926.549,1,0,0,1,5.43142933206,49.4159962516,3.619014,274.427472
> Ongeval gekoppeld op kruispuntniveau,9.00-09.59,30JAN06,57,Letsel,0,0,Flank,Kruispunt,53498,407783,1,1,0,0,4.01612963108,48.466850228,72.993561,273.752545
> *******************************************
> ***** Wrangling Processing FINISHED. ******
> *******************************************



