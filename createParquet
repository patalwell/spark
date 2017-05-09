from pyspark import SparkContext
from pyspark.sql import SparkSession, types
from pyspark.sql.types import *
sc = SparkContext()
spark = SparkSession(sc)
from datetime import datetime

"""This is a pyspark application that creates parquet files from raw tabular data."""

#raw output
data = ['1|AAAAAAAABAAAAAAA|18|Jackson |Parkway|Suite 280|Fairfield|Maricopa County|AZ|86192|United States|-7|condo|', 
'2|AAAAAAAACAAAAAAA|362|Washington 6th|RD|Suite 80|Fairview|Taos County|NM|85709|United States|-7|condo|',
'3|AAAAAAAADAAAAAAA|585|Dogwood Washington|Circle|Suite Q|Pleasant Valley|York County|PA|12477|United States|-5|single family|',
'4|AAAAAAAAEAAAAAAA|111|Smith |Wy|Suite A|Oak Ridge|Kit Carson County|CO|88371|United States|-7|condo|', 
'5|AAAAAAAAFAAAAAAA|31|College |Blvd|Suite 180|Glendale|Barry County|MO|63951|United States|-6|single family|']

# Create a mapping function  for row
#lines = sc.textFile("s3a://aqa-query-testing/tpcds/1G/raw/dbgen_version.dat")
lines = sc.paralellize(data)
split = lines.map(lambda l: l.split("|"))
row = split.map(lambda r: (r[0],(r[1]),(r[2]),r[3]))

#build the schema as an array calling StructType on StructField (x,y)
schema = types.StructType([
       types.StructField('dv_version',types.StringType())
       ,types.StructField('dv_create_date',types.DateType())
       ,types.StructField('dv_create_time',types.DateType())
       ,types.StructField('dv_cmdline_args',types.StringType())])

# create table customer_address
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

# Infer the schema, and register the DataFrame as a table.
dbgen_version = spark.createDataFrame(row,schema)
dbgen_version.createOrReplaceTempView("dbgen_version")

#write DF to parquet file
#schemaPeople.write.parquet("C:/SparkPython/people.parquet")

#read in the parquet file
#parquetFileDF = spark.read.parquet("C:/SparkPython/people.parquet")

#create a temp view to run SQL
#parquetFileDF.createOrReplaceTempView("parquetFile")

# SQL can be run over parquet file as the temp viewD
results = spark.sql("SELECT * FROM dbgen_version")

results.show()

spark.stop()
