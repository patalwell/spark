from pyspark import SparkContext
from pyspark.sql import SparkSession
from datetime import datetime, time

spark = SparkSession.builder.getOrCreate()

# Import data and create a dataframe from parquet files located on S3 or HDFS
data_path = "/data/tpch/100G/"

# create dictionary and list of table names
data_frames = {}
tables = ["customer", "orders", "lineitem", "supplier", "nation", "region", "part", "partsupp"]


# function that creates dataFrames and tempTables, appending to our dictionary and iterating table elements
def return_dataframe():
    for table in tables:
        # create df
        data_frames["df_" + table] = spark.read.parquet(data_path + table + ".parquet")
        # register tempTables
        data_frames["df_" + table].registerTempTable(table)


# calling our function to load tables
return_dataframe()

# init vars to collect benchmarking times
runtimes = []

def run_benchmark_query(query, message):
    print("Starting: " + message)
    # start time
    query_start_time = datetime.now()

    # run the query and show the result
    spark.sql(query).show(10)

    # end time
    query_stop_time = datetime.now()
    run_time = (query_stop_time-query_start_time).seconds
    print("Runtime: %s seconds" % run_time)
    runtimes.append(run_time)
    print("Finishing: " + message)
    return
    
 #++++++++++++++++++++++++++++++++Query Stream ++++++++++++++++++++++++++++++++

run_benchmark_query("""
    SELECT
    c_mktsegment,
    sum(c_acctbal)
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND n_name = 'BRAZIL'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    c_mktsegment
""", "Run 1")


run_benchmark_query("""
    SELECT
    c_mktsegment,
    sum(c_acctbal)
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND n_name = 'CANADA'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    c_mktsegment
""", "Run 2")

run_benchmark_query("""
    SELECT
    c_mktsegment,
    sum(c_acctbal)
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND n_name = 'UNITED STATES'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    c_mktsegment
""", "Run 3")

run_benchmark_query("""
    SELECT
    c_mktsegment,
    sum(c_acctbal)
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND n_name = 'INDONESIA'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    c_mktsegment
""", "Run 4")

run_benchmark_query("""
    SELECT
    c_mktsegment,
    sum(c_acctbal)
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND n_name = 'KENYA'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    c_mktsegment
""", "Run 5")

run_benchmark_query("""
    SELECT
    c_mktsegment,
    sum(c_acctbal)
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND n_name = 'MOROCCO'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    c_mktsegment
""", "Run 6")

run_benchmark_query("""
    SELECT
    c_mktsegment,
    sum(c_acctbal)
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND n_name = 'ROMANIA'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    c_mktsegment
""", "Run 7")

run_benchmark_query("""
    SELECT
    c_mktsegment,
    sum(c_acctbal)
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND n_name = 'PERU'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    c_mktsegment
""", "Run 8")

run_benchmark_query("""
    SELECT
    c_mktsegment,
    sum(c_acctbal)
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND n_name = 'CHINA'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    c_mktsegment
""", "Run 9")



run_benchmark_query("""
    SELECT
    c_mktsegment,
    sum(c_acctbal)
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND n_name = 'EGYPT'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    c_mktsegment
""", "Run 10")

run_benchmark_query("""
SELECT
    n_name,
    avg(o_totalprice)
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND o_orderpriority = '5-LOW'
GROUP BY
    n_name
""", "Run 11")

run_benchmark_query("""
SELECT
    n_name,
    avg(o_totalprice)
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND o_orderpriority = '4-NOT SPECIFIED'
    AND o_orderdate BETWEEN cast('1993-01-01' as date) AND cast('1996-01-01' as date)
GROUP BY
    n_name
""", "Run 22")

for run in runtimes:
  print run
