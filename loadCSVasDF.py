from pyspark import SparkContext
from pyspark.sql import *

sc = SparkContext
spark = SparkSession(sc)

# Load the CSV with Spark load attribute, header is in file
# Note in this case csv_data is loaded as a dataframe
csv_data = spark.read.csv("/Users/palwell/PycharmProjects/localCode/CCA175/2008.csv", header=True)

# create a tempview
csv_data.createOrReplaceTempView('2008')

# Query the view/table and show results
spark.sql("SELECT * FROM 2008 LIMIT 5").show()

spark.stop()
