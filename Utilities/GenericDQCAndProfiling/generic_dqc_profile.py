#!/usr/bin/python

'''
  Purpose: Performs Data Quality and Data Profiling on a file based on JSON based Metadata.

  MODIFICATION LOG
  =========================================================================
  Date       Who              Ver.  Proj         Reason
  =========================================================================
  20190319 Debo Mukherjee      v1   Hackathon        Initial draft
'''

import json
import logging
from datetime import datetime
import os
import sys
import getopt
from pprint import pprint
from pyspark.sql import SparkSession
from dataQuality.checkDataQuality import DataValidation
from dataProfiling.checkDataProfiling import DataProfiling


##Setting Up Logging Parameters
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')


def getCommandLineArgs():

    logging.info("Starting -- Validation of Command Line Arguments")

    global srcFileName
    global profileFileName
    srcFileName = None
    profileFileName = None

    try:
        opts, args = getopt.getopt(sys.argv[1:],"s:p:")
    except Exception as e:
        logging.error("Invalid Arguments - {e}".format(e=e))
        sys.exit()

    for opt,arg in opts:
        if (opt == "-s"):
            srcFileName = arg
        elif (opt == "-p"):
            profileFileName = arg

    if (srcFileName is None or srcFileName ==''):
        logging.error("Source File name not provided with argument -s")
        sys.exit()
    elif (profileFileName is None or profileFileName ==''):
        logging.error("Profile File name not provided with argument -p")
        sys.exit()

    logging.info("Finished -- Validation of Command Line Arguments")
    print("*****************************************************************************************************")

    return True



def checkFileLocation():
    if(not(os.path.isfile(srcFileName))):
        logging.error("Source File '{srcFileName}' not found".format(srcFileName=srcFileName))
        sys.exit()
    elif(not(os.path.isfile(profileFileName))):
        sys.exit("ERROR: Profile File '{profileFileName}' not found".format(profileFileName=profileFileName))
    else:
        print("Found the Source File '{srcFileName}' and profile File '{profileFileName}'".format(srcFileName=srcFileName,profileFileName=profileFileName))

    return True




def parsePropertiesFile():
    global data_properties
    data_properties    = []
    global fileDelimiter
    try:
        with open(profileFileName) as property_file:
            data_item = json.load(property_file)
    except Exception as e:
        sys.exit("ERROR: Failed to read JSON property file - {e}".format(e=e))

    for profile_data in data_item['profile']:
        data_properties.append(profile_data)

    fileDelimiter = data_item['delimiter']
    return True



def validatingFieldProperties():
    defaultDateFormat = "yyyy-MM-dd"
    defaultTimeStampFormat = "yyyy-MM-dd HH:mm:ss"
    possibleDataTypes = ['text','date','timestamp','categorical','numeric']

    for property in data_properties:
        if(not set(['field','type']) <= set(property.keys())):
            sys.exit("ERROR: Failed as the the property is missing keys 'field' or 'type' - {property}".format(property =  property.keys()))

        elif(not property['type'] in possibleDataTypes):
            sys.exit("ERROR: Failed as the data type mentioned '{dataType}' does not belong to {possibleDataTypes}".format(dataType = property['type'], possibleDataTypes = possibleDataTypes))

        elif(property['type'] == 'date' and not set(['format']) <= set(property.keys())):
            sys.exit("ERROR: Failed as the field type is set to 'date' but the format is missing - {property}".format(property =  property.keys()))

        elif(property['type'] == 'timestamp' and not set(['format']) <= set(property.keys())):
            sys.exit("ERROR: Failed as the field type is set to 'timestamp' but the format is missing - {property}".format(property =  property.keys()))

    return True




def invokeSparkSession():
    global spark
    spark = SparkSession.builder.appName("{srcFileName} Data Quality Check and Profiling at {dateTime}".format(srcFileName = srcFileName,dateTime = datetime.now().strftime('%Y-%m-%d %H:%M:%S'))).getOrCreate()
    return True




def readFile():
    global readFileDf
    readFileDf = spark.read.option("delimiter",fileDelimiter).option("header",True).option("inferSchema",False).csv(srcFileName)



def dataQualityCheck():
    global dqDpResult
    dqDpResult = []
    print(data_properties)
    for dataElement in data_properties:
        if('dqcheck' in dataElement):
            for dqcType in dataElement['dqcheck']:
                dqc = DataValidation()
                print(dqcType)
                if(dqcType == 'date'):
                    dataElement['dqcDateFlag'] = dqc.validateDate(spark, readFileDf , dataElement['field'],dataElement['format'])

                elif(dqcType == 'numeric'):
                    dataElement['dqcNumericFlag'] = dqc.validateInt(spark, readFileDf , dataElement['field'])

                elif(dqcType == 'null'):
                    dataElement['dqcNullFlag'] = dqc.validateNull(spark, readFileDf , dataElement['field'])

                elif(dqcType == 'duplicate'):
                    dataElement['dqcDuplicateFlag'] = dqc.validateDuplicate(spark, readFileDf , dataElement['field'])

        dqDpResult.append(dataElement)
    print dqDpResult



def dataProfiling():
    dpr = DataProfiling()
    for dataElement in data_properties:
        if(dataElement['type'] == 'date'):
            dataElement['dpfDateMinValue'] = dpr.findMinDate(spark,readFileDf,dataElement['field'],dataElement['format'])
            dataElement['dpfDateMaxValue'] = dpr.findMaxDate(spark,readFileDf,dataElement['field'],dataElement['format'])
            dataElement['dpfDateNullCount'] = dpr.findNumberOfNulls(spark,readFileDf,dataElement['field'])

        elif(dataElement['type'] == 'numeric'):
            dataElement['dpfNumMinValue'] = dpr.findMinNum(spark,readFileDf,dataElement['field'])
            dataElement['dpfNumMaxValue'] = dpr.findMaxNum(spark,readFileDf,dataElement['field'])
            dataElement['dpfNumNullCount'] = dpr.findNumberOfNulls(spark,readFileDf,dataElement['field'])

        elif(dataElement['type'] == 'text'):
            dataElement['dpfTxtMinLen'] = dpr.findMinLength(spark,readFileDf,dataElement['field'])
            dataElement['dpfTxtMaxLen'] = dpr.findMaxLength(spark,readFileDf,dataElement['field'])
            dataElement['dpfTxtNullCount'] = dpr.findNumberOfNulls(spark,readFileDf,dataElement['field'])

        dqDpResult.append(dataElement)
    print dqDpResult



def writeOutputDQC():
    with open('data.json', 'w') as outfile:
         json.dump(dqDpResult, outfile)



'''
###########################################
The main script starts below
###########################################
'''
def main():

    print(2*"\n")
    logging.info("Starting Job to Profile File based on metadata")
    print("*****************************************************************************************************")


    #Step1 - Check and Get Command line arguments
    getCommandLineArgs()
    logging.info("The name of the Source file is    : " + srcFileName)
    logging.info("The name of the Profile file is    : " + profileFileName)



    #Step2 - Check source file and profile file location
    logging.info("Starting -- Validation of Source and Profile File Paths")
    checkFileLocation()
    logging.info("Finished -- Validation of Source and Profile File Paths")
    print("*****************************************************************************************************")


    #Step3 - Parsing the profile file
    logging.info("Starting -- Parsing of Profile File")
    parsePropertiesFile()
    logging.info("Finished -- Parsing of Profile File")
    print("*****************************************************************************************************")


    #Step4 - Validating the properties file
    logging.info("Starting -- Validation of Field Properties")
    validatingFieldProperties()
    logging.info("Finished -- Validation of Field Properties")
    print("*****************************************************************************************************")


    #Step5 - Invoke Spark Session
    logging.info("Starting -- Invoking Spark Session")
    invokeSparkSession()
    logging.info("Finished -- Invoking Spark Session")
    print("*****************************************************************************************************")


    #Step6 - Reading the Source File in a Spark DataFrame
    logging.info("Starting -- Reading Source File")
    readFile()
    logging.info("Finished -- Reading Source File")
    print("*****************************************************************************************************")


    #Step7 - Performing Data Quality Check
    logging.info("Starting -- Data Quality Check")
    dataQualityCheck()
    logging.info("Finished -- Data Quality Check")
    print("*****************************************************************************************************")


    #Step8 - Performing Data Profiling
    logging.info("Starting -- Data Profiling")
    dataProfiling()
    logging.info("Finished -- Data Profiling")
    print("*****************************************************************************************************")


    #Step9 - Performing Data Quality Check
    logging.info("Starting -- Write to JSON")
    writeOutputDQC()
    logging.info("Finished -- Write to JSON")
    print("*****************************************************************************************************")



if(__name__ == "__main__"):
    main()
