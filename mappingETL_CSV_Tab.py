from pyspark import SparkContext
from pyspark.sql import *

sc = SparkContext
spark = SparkSession(sc)


# Load data from HDFS and store results back to HDFS using Spark
# Problem formulation
# =============================
# Read the /user/cloudera/rawdata/flight_dataset/carrier/carriers.csv file from hdfs,
# convert all records in the file to a tab-delimited format and save back to hdfs at
# /user/cloudera/rawdata/flight_dataset/py_out/carrier_tabbed.
# Also remove all quotes in each field as well as the header

#Load the CSV File In Raw Format
# Note: we will need to parse the fields and create a row object to load a DF
data = sc.textFile("/Users/palwell/PycharmProjects/localCode/CCA175/2008.csv")

#Infer the Schema
data.take(1)

#[u'Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime,\
#CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,\
#WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay']

# remove header
filterData = data.filter(lambda l: not l.startswith("Year"))
# remove quotes
noQuotes = filterData.map(lambda l: l.replace("\"",""))
# split by comma and seperate by tab
splitData = noQuotes.map(lambda l: l.split(",")).map(lambda r: r[0]+ "\t" + r[1] + "\t" + r[2] + "\t" + r[3] + "\t" + r[4] + "\t" +r[5])
# sort the output by word
sortData = splitData.sortBy(lambda l: l)
# save as textFile
sortData.saveAsTextFile("/Users/palwell/PycharmProjects/localCode/CCA175/tab-delimited_2008.txt")
