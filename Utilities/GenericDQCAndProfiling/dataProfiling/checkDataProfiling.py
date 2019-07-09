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



    def findMaxNum(self,spark,df,columnName):
        df.createOrReplaceTempView("findMaxNum")
        maxValueNumDf = spark.sql("select max({columnName}) from findMaxNum".format(columnName=columnName))
        maxValueNum = maxValueNumDf.collect()[0]
        return maxValueNum



    def findMinNum(self,spark,df,columnName):
        df.createOrReplaceTempView("findMinNum")
        minValueNumDf = spark.sql("select min({columnName}) from findMinNum".format(columnName=columnName))
        minValueNum = minValueNumDf.collect()[0]
        return minValueNum

    def findMaxDate(self,spark,df,columnName,format):
        df.createOrReplaceTempView("findMaxDate")
        maxValueDateDf = spark.sql("select max(unix_timestamp(trim({columnName}),'{format}')) from findMaxDate".format(columnName=columnName,format=format))
        maxValueDate = maxValueDateDf.collect()[0]
        return maxValueDate



    def findMinDate(self,spark,df,columnName,format):
        df.createOrReplaceTempView("findMinDate")
        minValueDateDf = spark.sql("select min(unix_timestamp(trim({columnName}),'{format}')) from findMinDate".format(columnName=columnName,format=format))
        minValueDate = minValueDateDf.collect()[0]
        return minValueDate



    def findMaxLength(self,spark,df,columnName):
        df.createOrReplaceTempView("findMaxLength")
        findMaxLengthDf = spark.sql("select min({columnName}) from findMaxLength".format(columnName=columnName))
        maxLength = findMaxLengthDf.collect()[0]
        return maxLength



    def findMinLength(self,spark,df,columnName):
        df.createOrReplaceTempView("findMinLength")
        findMinLengthDf = spark.sql("select min({columnName}) from findMinLength".format(columnName=columnName))
        minLength = findMinLengthDf.collect()[0]
        return minLength



    def findNumberOfNulls(self,spark,df,columnName):
        df.createOrReplaceTempView("findNumberOfNulls")
        findNumberOfNullsDf = spark.sql("select count(*) from findNumberOfNulls where {columnName} is NULL".format(columnName=columnName))
        numberOfNulls = findNumberOfNullsDf.collect()[0]
        return numberOfNulls



if(__name__ == "__main__"):
    pass
