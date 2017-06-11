from pyspark import SparkContext
from pyspark.sql import *

sc = SparkContext
spark = SparkSession(sc)

#Load the CSV File In Raw Format
# Note: we will need to parse the fields and create a row object to load a DF
data = sc.textFile("/Users/palwell/PycharmProjects/localCode/CCA175/2008.csv")

# Load the CSV with Spark load attribute, header is in file
# Note in this case csv_data is loaded as a dataframe
csv_data = spark.load.csv("/Users/palwell/PycharmProjects/localCode/CCA175/2008.csv", header=True)

#Infer the Schema
data.take(1)

#[u'Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime,\
#CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,\
#WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay']

#Parse the raw data into comma seperated fields and convert the file into a row array
dataSplit = data.map(lambda l: l.split(","))
row = dataSplit.map(lambda r: Row(
    Year=str(r[0])
    , Month=str(r[1])
    , DayofMonth=r[2]
    , DayOfWeek=r[3]
    , DepTime=str(r[4])
    , CRSDepTime=str(r[5])
    , ArrTime=str(r[6])
    , CRSArrTime=str(r[7])
    , UniqueCarrier=r[8]
    , FlightNum=str(r[9])
    , TailNum=str(r[10])
    , ActualElapsedTime=str(r[11])
    , CRSElapsedTime=str(r[12])
    , AirTime=str(r[13])
    , ArrDelay=r[14]
    , DepDelay=r[15]
    , Origin=r[16]
    , Dest=r[17],
    , Distance=r[18]
    , TaxiIn=r[19]
    , TaxiOut=r[20]
    , Cancelled=r[21]
    , CancellationCode=r[22]
    , Diverted=r[23]
    , CarrierDelay=r[24]
    , WeatherDelay=r[25]
    , NASDelay=r[26]
    , SecurityDelay=r[27]
    , LateAircraftDelay=r[28]))

#build the schema as an array calling StructType on StructField (x,y)
schema = types.StructType([
       types.StructField('Year',types.IntegerType())
       ,types.StructField('Month',types.IntegerType())
       ,types.StructField('DayofMonth',types.IntegerType())
       ,types.StructField('DayOfWeek',types.StringType())
       ,types.StructField('DepTime',types.StringType())
       ,types.StructField('CRSDepTime',types.IntegerType())
       ,types.StructField('ArrTime',types.IntegerType())
       ,types.StructField('CRSArrTime',types.IntegerType())
       ,types.StructField('UniqueCarrier',types.StringType())
       ,types.StructField('FlightNum',types.IntegerType())
       ,types.StructField('TailNum',types.IntegerType())
       ,types.StructField('ActualElapsedTime',types.StringType())
       ,types.StructField('CRSElapsedTime',types.StringType())
       ,types.StructField('AirTime',types.IntegerType())
       ,types.StructField('ArrDelay',types.StringType())
       ,types.StructField('DepDelay',types.StringType())
       ,types.StructField('Origin',types.StringType())
       ,types.StructField('Dest',types.StringType())
       ,types.StructField('Distance',types.StringType())
       ,types.StructField('TaxiIn',types.StringType())
       ,types.StructField('TaxiOut',types.StringType())
       ,types.StructField('Cancelled',types.StringType())
       ,types.StructField('CancellationCode',types.StringType())
       ,types.StructField('Diverted',types.StringType())
       ,types.StructField('CarrierDelay',types.StringType())
       ,types.StructField('WeatherDelay',types.StringType())
       ,types.StructField('NASDelay',types.StringType())
       ,types.StructField('SecurityDelay',types.StringType())
       ,types.StructField('LateAircraftDelay',types.StringType())
       ])

#Create a dataframe with infered schema
flight_infer_info = spark.createDataFrame(row)

# create data frame by passing in the mapped data AND its strict schema
flight_info = spark.createDataFrame(row,schema)

# Register temp tables
flight_infer_info.createOrReplaceTempView('Flights')
flight_info.createOrReplaceTempView('flights')

# Conduct sql and show results
spark.sql("SELECT * FROM flights LIMIT 5").show()

# stop the context
spark.stop()
