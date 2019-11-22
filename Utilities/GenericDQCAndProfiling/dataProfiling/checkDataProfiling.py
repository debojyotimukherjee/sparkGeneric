'''
  Purpose: Class and Methods for Data Profiling

  MODIFICATION LOG
  =========================================================================
  Date     Who                Ver.  Proj             Reason
  =========================================================================
  20190330 Debo Mukherjee      v1   Hackathon       Initial draft
'''

###Class defined for data validation

class DataProfiling:
    def __init__(self,*args):
        pass
    
    def findMaxMinNum(self,spark,df,columnName):
        df.createOrReplaceTempView("findMaxMinNum")
        maxMinValueNumDf = spark.sql("select max({columnName}),min({columnName}) from findMaxMinNum".format(columnName=columnName))
        maxValueNum = maxMinValueNumDf.collect()[0][0]
        minValueNum = maxMinValueNumDf.collect()[0][1]
        return [maxValueNum,minValueNum]

    def findMaxMinLen(self,spark,df,columnName):
        df.createOrReplaceTempView("findMaxMinLen")
        maxMinLenDf = spark.sql("select max(length({columnName})),min(length({columnName})) from findMaxMinLen".format(columnName=columnName))
        maxLen = maxMinLenDf.collect()[0][0]
        minLen = maxMinLenDf.collect()[0][1]
        return [maxLen,minLen]

    def findMaxMinDate(self,spark,df,columnName,format):
        df.createOrReplaceTempView("findMaxMinDate")
        maxMinValueDateDf = spark.sql("select max(unix_timestamp(trim({columnName}),'{format}')), min(unix_timestamp(trim({columnName}),'{format}')) from findMaxMinDate".format(columnName=columnName,format=format))
        maxValueDate = maxMinValueDateDf.collect()[0][0]
        minValueDate = maxMinValueDateDf.collect()[0][1]
        return [maxValueDate,minValueDate]

    def findMaxMinLength(self,spark,df,columnName):
        df.createOrReplaceTempView("findMaxMinLength")
        maxMinLengthDf = spark.sql("select max(length({columnName})), min(length({columnName})) from findMaxMinLength".format(columnName=columnName))
        maxLength = maxMinLengthDf.collect()[0][0]
        minLength = maxMinLengthDf.collect()[0][1]
        return [maxLength,minLength]

    def findNumberOfNulls(self,spark,df,columnName):
        df.createOrReplaceTempView("findNumberOfNulls")
        findNumberOfNullsDf = spark.sql("select count(*) from findNumberOfNulls where {columnName} is NULL".format(columnName=columnName))
        numberOfNulls = findNumberOfNullsDf.collect()[0][0]
        return numberOfNulls


if(__name__ == "__main__"):
    pass
