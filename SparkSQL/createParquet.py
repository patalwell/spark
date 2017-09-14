from pyspark import SparkContext
from pyspark.sql import SparkSession, types
from pyspark.sql.types import *
sc = SparkContext()
spark = SparkSession(sc)
from datetime import datetime

"""This is a pyspark application that creates parquet files from raw tabular data."""

#raw output
data = sc.textFile("s3://customer_address.dat",numParitions)

    # original SQL Database schema for this table
    # (
    #     ca_address_sk             integer               not null,
    #     ca_address_id             char(16)              not null,
    #     ca_street_number          char(10)                      ,
    #     ca_street_name            varchar(60)                   ,
    #     ca_street_type            char(15)                      ,
    #     ca_suite_number           char(10)                      ,
    #     ca_city                   varchar(60)                   ,
    #     ca_county                 varchar(30)                   ,
    #     ca_state                  char(2)                       ,
    #     ca_zip                    char(10)                      ,
    #     ca_country                varchar(20)                   ,
    #     ca_gmt_offset             decimal(5,2)                  ,
    #     ca_location_type          char(20)                      ,
    #     primary key (ca_address_sk)
    # );

    # Import the above sample data into RDD
    #lines = sc.parallelize(data)

# map the data, return data as a Row, and cast data types of some fields
split = data.map(lambda l: l.split("|"))
row = split.map(lambda r: Row(
       ca_address_sk=int(r[0]),
       ca_address_id=r[1],
       ca_street_number=r[2],
       ca_street_name=r[3],
       ca_street_type=r[4],
       ca_suite_number=r[5],
       ca_city=r[6],
       ca_county=r[7],
       ca_state=r[8],
       ca_zip=r[9],
       ca_country=r[10],
       ca_gmt_offset=None if r[11]=='' else Decimal(r[11]),
       ca_location_type=r[12])
    )

#build strict schema for the table closely based on the original sql schema
schema = types.StructType([
       types.StructField('ca_address_sk',types.IntegerType(),False)
       ,types.StructField('ca_address_id',types.StringType(),False)
       ,types.StructField('ca_street_number',types.StringType())
       ,types.StructField('ca_street_name',types.StringType())
       ,types.StructField('ca_street_type',types.StringType())
       ,types.StructField('ca_suite_number',types.StringType())
       ,types.StructField('ca_city',types.StringType())
       ,types.StructField('ca_county',types.StringType())
       ,types.StructField('ca_state',types.StringType())
       ,types.StructField('ca_zip',types.StringType())
       ,types.StructField('ca_country',types.StringType())
       ,types.StructField('ca_gmt_offset',types.DecimalType(5,2))
       ,types.StructField('ca_location_type',types.StringType())])

# create data frame by passing in the mapped data AND its strict schema
customer_address = spark.createDataFrame(row,schema)

#create parquet file
customer_address.write.parquet(s3Path + "customer_address.parquet")

# create temp table name of the new table
customer_address.createOrReplaceTempView("customer_address")

#write DF to parquet file
#customer_address.write.parquet("C:/SparkPython/customer.parquet")

#read in the parquet file
#parquetFileDF = spark.read.parquet("C:/SparkPython/customer.parquet")

#create a temp view to run SQL
#parquetFileDF.createOrReplaceTempView("customer")

# SQL can be run over parquet file as the temp viewD
results = spark.sql("SELECT * FROM customer LIMIT 5")

results.show()

spark.stop()
