#Background: Users might be curious to learn more about how our product inherently speeds up queries.
# Instead of creating an adjacent data store, our query accelerator performs the task of a skilled DBA \
# by effectively caching and storing an adjacent algebraic expression store. The expression store allows for a greater range of \
# queries than an adjacent data store or index/library could alone perform.
# What's more, this process is autonomous and limits the use of added hardware and intellectual capital.
# This application is a DF benchmarking tool for QA purposes


# module imports for our application
from pyspark import SparkContext
from pyspark.sql import SparkSession
from datetime import datetime, time
from pyspark.sql import functions as f

# initializing the SparkSession Class and enabling cross joins
spark = SparkSession(sc)
spark.conf.set("spark.sql.crossJoin.enabled", True)


# rdd = sc.textFile("/my_directory")
df_customer = spark.read.parquet("~/orders.parquet")
df_lineitem = spark.read.parquet("~/lineitem.parquet")
df_supplier = spark.read.parquet("~/supplier.parquet")
df_nation = spark.read.parquet("~/nation.parquet")
df_region = spark.read.parquet("~/region.parquet")

df_customer.registerTempTable("customer")
df_orders.registerTempTable("orders")
df_lineitem.registerTempTable("lineitem")
df_supplier.registerTempTable("supplier")
df_nation.registerTempTable("nation")
df_region.registerTempTable("region")

# init vars
runtimes = []

def runBenchmarkQuery(query,message):
  print("Starting: " + message);
  #start time
  queryStartTime = datetime.now()

  #run the query and show the result
  query.show()

  #end time
  queryStopTime = datetime.now()
  runTime = (queryStopTime-queryStartTime).seconds
  print("Runtime: %s seconds" % (runTime))
  runtimes.append( runTime )
  print ("Finishing: " + message);
  return

#Errors during coying of original SQL Query to a DF API
    #.where(df_orders.o_orderdate < f.add_months('1993-01-01', '12'))
    #py4j.Py4JException: Method add_months([class org.apache.spark.sql.Column, class java.lang.String]) does not exist
#It doesn't seem like the DF Spark API supports the method add_months for predicates, so I changed the predicate to read as '1994-01-01'

# def add_months(start, months):
#     """
#     Returns the date that is `months` months after `start`

#     >>> df = spark.createDataFrame([('2015-04-08',)], ['d'])
#     >>> df.select(add_months(df.d, 1).alias('d')).collect()
#     [Row(d=datetime.date(2015, 5, 8))]
#     """
#     sc = SparkContext._active_spark_context
#     return Column(sc._jvm.functions.add_months(_to_java_column(start), months))


#========================
# RUN 1
runBenchmarkQuery(
df_customer\
.join(df_orders)\
.join(df_lineitem)\
.join(df_supplier)\
.join(df_nation)\
.join(df_region)\
.where(df_customer.c_custkey == df_orders.o_custkey)\
.where(df_lineitem.l_orderkey == df_orders.o_orderkey)\
.where(df_lineitem.l_suppkey == df_supplier.s_suppkey)\
.where(df_customer.c_nationkey == df_supplier.s_nationkey)\
.where(df_supplier.s_nationkey == df_nation.n_nationkey)\
.where(df_nation.n_regionkey == df_region.r_regionkey)\
.where(df_region.r_name == 'AFRICA')\
.where(df_orders.o_orderdate >='1993-01-01')\
.where(df_orders.o_orderdate < '1994-01-01')\
.groupby(df_nation.n_name)\
.agg(f.sum((df_lineitem.l_extendedprice * (1 - df_lineitem.l_discount)).alias('revenue')))\
, "Run 1")

# ========================
# RUN 2
runBenchmarkQuery(
df_customer\
.join(df_orders)\
.join(df_lineitem)\
.join(df_supplier)\
.join(df_nation)\
.join(df_region)\
.where(df_customer.c_custkey == df_orders.o_custkey)\
.where(df_lineitem.l_orderkey == df_orders.o_orderkey)\
.where(df_lineitem.l_suppkey == df_supplier.s_suppkey)\
.where(df_customer.c_nationkey == df_supplier.s_nationkey)\
.where(df_supplier.s_nationkey == df_nation.n_nationkey)\
.where(df_nation.n_regionkey == df_region.r_regionkey)\
.where(df_region.r_name == 'AMERICA')\
.where(df_orders.o_orderdate >='1993-01-01')\
.where(df_orders.o_orderdate < '1994-01-01')\
.groupby(df_nation.n_name)\
.agg(f.sum((df_lineitem.l_extendedprice * (1 - df_lineitem.l_discount)).alias('revenue')))\
, "Run 2")


# ========================
# RUN 3
runBenchmarkQuery(
df_customer\
.join(df_orders)\
.join(df_lineitem)\
.join(df_supplier)\
.join(df_nation)\
.join(df_region)\
.where(df_customer.c_custkey == df_orders.o_custkey)\
.where(df_lineitem.l_orderkey == df_orders.o_orderkey)\
.where(df_lineitem.l_suppkey == df_supplier.s_suppkey)\
.where(df_customer.c_nationkey == df_supplier.s_nationkey)\
.where(df_supplier.s_nationkey == df_nation.n_nationkey)\
.where(df_nation.n_regionkey == df_region.r_regionkey)\
.where(df_region.r_name == 'ASIA')\
.where(df_orders.o_orderdate >='1993-01-01')\
.where(df_orders.o_orderdate < '1994-01-01')\
.groupby(df_nation.n_name)\
.agg(f.sum((df_lineitem.l_extendedprice * (1 - df_lineitem.l_discount)).alias('revenue')))\
, "Run 3")

# ========================
# RUN 4
runBenchmarkQuery(
df_customer\
.join(df_orders)\
.join(df_lineitem)\
.join(df_supplier)\
.join(df_nation)\
.join(df_region)\
.where(df_customer.c_custkey == df_orders.o_custkey)\
.where(df_lineitem.l_orderkey == df_orders.o_orderkey)\
.where(df_lineitem.l_suppkey == df_supplier.s_suppkey)\
.where(df_customer.c_nationkey == df_supplier.s_nationkey)\
.where(df_supplier.s_nationkey == df_nation.n_nationkey)\
.where(df_nation.n_regionkey == df_region.r_regionkey)\
.where(df_region.r_name == 'EUROPE')\
.where(df_orders.o_orderdate >='1993-01-01')\
.where(df_orders.o_orderdate < '1994-01-01')\
.groupby(df_nation.n_name)\
.agg(f.sum((df_lineitem.l_extendedprice * (1 - df_lineitem.l_discount)).alias('revenue')))\
,"Run 4")

# ========================
# RUN 5
runBenchmarkQuery(
df_customer\
.join(df_orders)\
.join(df_lineitem)\
.join(df_supplier)\
.join(df_nation)\
.join(df_region)\
.where(df_customer.c_custkey == df_orders.o_custkey)\
.where(df_lineitem.l_orderkey == df_orders.o_orderkey)\
.where(df_lineitem.l_suppkey == df_supplier.s_suppkey)\
.where(df_customer.c_nationkey == df_supplier.s_nationkey)\
.where(df_supplier.s_nationkey == df_nation.n_nationkey)\
.where(df_nation.n_regionkey == df_region.r_regionkey)\
.where(df_region.r_name == 'MIDDLE EAST')\
.where(df_orders.o_orderdate >='1993-01-01')\
.where(df_orders.o_orderdate < '1994-01-01')\
.groupby(df_nation.n_name)\
.agg(f.sum((df_lineitem.l_extendedprice * (1 - df_lineitem.l_discount)).alias('revenue')))\
,"Run 5")

# ========================
# RUN 6
runBenchmarkQuery(
df_customer\
.join(df_orders)\
.join(df_lineitem)\
.join(df_supplier)\
.join(df_nation)\
.join(df_region)\
.where(df_customer.c_custkey == df_orders.o_custkey)\
.where(df_lineitem.l_orderkey == df_orders.o_orderkey)\
.where(df_lineitem.l_suppkey == df_supplier.s_suppkey)\
.where(df_customer.c_nationkey == df_supplier.s_nationkey)\
.where(df_supplier.s_nationkey == df_nation.n_nationkey)\
.where(df_nation.n_regionkey == df_region.r_regionkey)\
.where(df_region.r_name == 'AFRICA')\
.where(df_orders.o_orderdate >='1993-01-01')\
.where(df_orders.o_orderdate < '1994-01-01')\
.groupby(df_nation.n_name)\
.agg(f.sum((df_lineitem.l_extendedprice * (1 - df_lineitem.l_discount)).alias('revenue')))\
,"Run 6")


# ========================
# RUN 7
runBenchmarkQuery(
df_customer\
.join(df_orders)\
.join(df_lineitem)\
.join(df_supplier)\
.join(df_nation)\
.join(df_region)\
.where(df_customer.c_custkey == df_orders.o_custkey)\
.where(df_lineitem.l_orderkey == df_orders.o_orderkey)\
.where(df_lineitem.l_suppkey == df_supplier.s_suppkey)\
.where(df_customer.c_nationkey == df_supplier.s_nationkey)\
.where(df_supplier.s_nationkey == df_nation.n_nationkey)\
.where(df_nation.n_regionkey == df_region.r_regionkey)\
.where(df_region.r_name == 'AMERICA')\
.where(df_orders.o_orderdate >='1993-01-01')\
.where(df_orders.o_orderdate < '1994-01-01')\
.groupby(df_nation.n_name)\
.agg(f.sum((df_lineitem.l_extendedprice * (1 - df_lineitem.l_discount)).alias('revenue')))\
,"Run 7")

# ========================
# RUN 8
runBenchmarkQuery(
df_customer\
.join(df_orders)\
.join(df_lineitem)\
.join(df_supplier)\
.join(df_nation)\
.join(df_region)\
.where(df_customer.c_custkey == df_orders.o_custkey)\
.where(df_lineitem.l_orderkey == df_orders.o_orderkey)\
.where(df_lineitem.l_suppkey == df_supplier.s_suppkey)\
.where(df_customer.c_nationkey == df_supplier.s_nationkey)\
.where(df_supplier.s_nationkey == df_nation.n_nationkey)\
.where(df_nation.n_regionkey == df_region.r_regionkey)\
.where(df_region.r_name == 'ASIA')\
.where(df_orders.o_orderdate >='1993-01-01')\
.where(df_orders.o_orderdate < '1994-01-01')\
.groupby(df_nation.n_name)\
.agg(f.sum((df_lineitem.l_extendedprice * (1 - df_lineitem.l_discount)).alias('revenue')))\
,"Run 8")

# ========================
# RUN 9
runBenchmarkQuery(
df_customer\
.join(df_orders)\
.join(df_lineitem)\
.join(df_supplier)\
.join(df_nation)\
.join(df_region)\
.where(df_customer.c_custkey == df_orders.o_custkey)\
.where(df_lineitem.l_orderkey == df_orders.o_orderkey)\
.where(df_lineitem.l_suppkey == df_supplier.s_suppkey)\
.where(df_customer.c_nationkey == df_supplier.s_nationkey)\
.where(df_supplier.s_nationkey == df_nation.n_nationkey)\
.where(df_nation.n_regionkey == df_region.r_regionkey)\
.where(df_region.r_name == 'EUROPE')\
.where(df_orders.o_orderdate >='1993-01-01')\
.where(df_orders.o_orderdate < '1994-01-01')\
.groupby(df_nation.n_name)\
.agg(f.sum((df_lineitem.l_extendedprice * (1 - df_lineitem.l_discount)).alias('revenue')))\
, "Run 9")


# ========================
# RUN 10
runBenchmarkQuery(
df_customer\
.join(df_orders)\
.join(df_lineitem)\
.join(df_supplier)\
.join(df_nation)\
.join(df_region)\
.where(df_customer.c_custkey == df_orders.o_custkey)\
.where(df_lineitem.l_orderkey == df_orders.o_orderkey)\
.where(df_lineitem.l_suppkey == df_supplier.s_suppkey)\
.where(df_customer.c_nationkey == df_supplier.s_nationkey)\
.where(df_supplier.s_nationkey == df_nation.n_nationkey)\
.where(df_nation.n_regionkey == df_region.r_regionkey)\
.where(df_region.r_name == 'MIDDLE EAST')\
.where(df_orders.o_orderdate >='1993-01-01')\
.where(df_orders.o_orderdate < '1994-01-01')\
.groupby(df_nation.n_name)\
.agg(f.sum((df_lineitem.l_extendedprice * (1 - df_lineitem.l_discount)).alias('revenue')))\
, "Run 10")
