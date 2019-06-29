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

	def findMax(self,spark,df,columnName,format):
        df.createOrReplaceTempView("findMax")
        maxValueDf = spark.sql("select max({columnName}) from findMax".format(columnName=columnName))
        maxValue = maxValueDf.collect()[0]
		return maxValue

	def findMin(self,spark,df,columnName,format):
        df.createOrReplaceTempView("findMin")
		minValueDf = spark.sql("select min({columnName}) from findMin".format(columnName=columnName))
		minValue = minValueDf.collect()[0]
		return minValue

	def findMaxLength(self,spark,df,columnName,format):
        df.createOrReplaceTempView("findMaxLength")
		findMaxLengthDf = spark.sql("select min({columnName}) from findMaxLength".format(columnName=columnName))
		maxLength = findMaxLengthDf.collect()[0]
		return maxLength

	def findMinLength(self,spark,df,columnName,format):
        df.createOrReplaceTempView("findMinLength")
		findMinLengthDf = spark.sql("select min({columnName}) from findMinLength".format(columnName=columnName))
		minLength = findMinLengthDf.collect()[0]
		return minLength

	def findNumberOfNulls(self,spark,df,columnName,format):
        df.createOrReplaceTempView("findNumberOfNulls")
		findNumberOfNullsDf = spark.sql("select count(*) from findNumberOfNulls where {columnName} is NULL".format(columnName=columnName))
		numberOfNulls = findNumberOfNullsDf.collect()[0]
		return numberOfNulls


if(__name__ == "__main__"):
    pass
