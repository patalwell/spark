from pyspark import SparkContext
from pyspark.sql import SparkSession, Row, types
from pyspark.sql.types import *
from pyspark.sql import functions as f
from decimal import *
from datetime import datetime

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext


#Generating customer parquet table
customer_data = sc.textFile("/data/tpch/customer.tbl")
customer_split = customer_data.map(lambda l: l.split("|"))
customer_row = customer_split.map( lambda r: Row(
    custkey=int(r[0]),
    name=r[1],
    address=r[2],
    nationkey=int(r[3]),
    phone=r[4],
    acctbal=Decimal(r[5]),
    mktsegment=r[6],
    comment=r[7]
))

customer_schema = types.StructType([
       types.StructField('custkey',types.IntegerType(),False)
       ,types.StructField('name',types.StringType())
       ,types.StructField('address',types.StringType())
       ,types.StructField('nationkey',types.StringType(),False)
       ,types.StructField('phone',types.StringType())
       ,types.StructField('acctbal',types.DecimalType())
       ,types.StructField('mktsegment',types.StringType())
       ,types.StructField('comment',types.StringType())])

customer_df = spark.createDataFrame(customer_row,customer_schema)

customer_df.write.parquet("/data/tpch/customer.parquet")

customer_df_parquet = spark.read.parquet("/data/tpch/customer.parquet")

customer_df_parquet.createOrReplaceTempView("customer")

spark.sql("SELECT * FROM customer LIMIT 10").show()

#generating region parquet table
region_data = sc.textFile("/data/tpch/region.tbl")
region_split = region_data.map(lambda l: l.split("|"))
region_row = region_split.map( lambda r: Row(
    regionkey=int(r[0]),
    name=r[1],
    comment=r[2]
))

region_schema = types.StructType([
       types.StructField('regionkey',types.IntegerType(),False)
       ,types.StructField('name',types.StringType())
       ,types.StructField('comment',types.StringType())])

region_df = spark.createDataFrame(region_row,region_schema)

region_df.write.parquet("/data/tpch/region.parquet")

region_df_parquet = spark.read.parquet("/data/tpch/region.parquet")

region_df_parquet.createOrReplaceTempView("region")

spark.sql("SELECT * FROM region LIMIT 10").show()

#generating nation parquet table
nation_data = sc.textFile("/data/tpch/nation.tbl")
nation_split = nation_data.map(lambda l: l.split("|"))
nation_row = nation_split.map( lambda r: Row(
    nationkey=int(r[0]),
    name=r[1],
    regionkey=int(r[2]),
    comment=r[3]
))

nation_schema = types.StructType([
       types.StructField('nationkey',types.IntegerType(),False)
       ,types.StructField('name',types.StringType())
       ,types.StructField('regionkey',types.IntegerType())
       ,types.StructField('comment',types.StringType())])

nation_df = spark.createDataFrame(nation_row,nation_schema)

nation_df.write.parquet("/data/tpch/nation.parquet")

nation_df_parquet = spark.read.parquet("/data/tpch/nation.parquet")

nation_df_parquet.createOrReplaceTempView("nation")

spark.sql("SELECT * FROM nation LIMIT 10").show()

# generating lineitem parquet table; partitionBy shipdate

lineitem_data = sc.textFile("/data/tpch/lineitem.tbl")
lineitem_split = lineitem_data.map(lambda l: l.split("|"))
lineitem_row = lineitem_split.map( lambda r: Row(
    orderkey=int(r[0]),
    partkey=int(r[1]),
    suppkey=int(r[2]),
    linenumber =int(r[3]),
    quantity =int(r[4]),
    extendedprice= Decimal(r[5]),
    discount= Decimal(r[6]),
    tax= Decimal(r[7]),
    returnflag= r[8],
    linestatus= r[9],
    shipdate= str(r[10]),
    commitdate= str(r[11]),
    receiptdate= str(r[12]),
    shipinstruct= r[13],
    shipmode= r[14],
    comment= r[15]
))

lineitem_schema = types.StructType([
       types.StructField('orderkey',types.IntegerType(),False)
       ,types.StructField('partkey',types.IntegerType(),False)
       ,types.StructField('suppkey',types.IntegerType(),False)
       ,types.StructField('linenumber',types.IntegerType())
       ,types.StructField('quantity',types.IntegerType())
       ,types.StructField('extendedprice',types.DecimalType())
       ,types.StructField('discount',types.DecimalType())
       ,types.StructField('tax',types.DecimalType())
       ,types.StructField('returnflag',types.StringType())
       ,types.StructField('linestatus',types.StringType())
       ,types.StructField('shipdate',types.StringType())
       ,types.StructField('commitdate',types.StringType())
       ,types.StructField('receiptdate',types.StringType())
       ,types.StructField('shipinstruct',types.StringType())
       ,types.StructField('shipmode',types.StringType())
       ,types.StructField('comment',types.StringType())])

lineitem_df = spark.createDataFrame(lineitem_row,lineitem_schema)

# this is a hack; as datetime methods wont work on Row() objects
lineitem_df = lineitem_df.withColumn('shipdate',f.to_date(lineitem_df.shipdate))
lineitem_df = lineitem_df.withColumn('commitdate',f.to_date(lineitem_df.commitdate))
lineitem_df = lineitem_df.withColumn('receiptdate',f.to_date(lineitem_df.receiptdate))

lineitem_df.write.partitionBy('shipdate').parquet("/data/tpch/lineitem.parquet")

lineitem_df_parquet = spark.read.parquet("/data/tpch/lineitem.parquet")

lineitem_df_parquet.createOrReplaceTempView("lineitem")

spark.sql("SELECT * FROM lineitem LIMIT 10").show()

# generating orders parquet table; paritionBy orderdate
orders_data = sc.textFile("/data/tpch/orders.tbl")
orders_split = orders_data.map(lambda l: l.split("|"))
orders_row = orders_split.map( lambda r: Row(
    orderkey=int(r[0]),
    custkey=int(r[1]),
    orderstatus=r[2],
    totalprice=Decimal(r[3]),
    orderdate=str(r[4]),
    orderpriority=r[5],
    clerk=r[6],
    shippriority=int(r[7]),
    comment=r[8]
))

orders_schema = types.StructType([
       types.StructField('orderkey',types.IntegerType(),False)
       ,types.StructField('custkey',types.IntegerType(),False)
       ,types.StructField('orderstatus',types.StringType())
       ,types.StructField('totalprice',types.DecimalType())
       ,types.StructField('orderdate',types.StringType())
       ,types.StructField('orderpriority',types.StringType())
       ,types.StructField('clerk',types.StringType())
       ,types.StructField('shippriority',types.IntegerType())
       ,types.StructField('comment',types.StringType())])

orders_df = spark.createDataFrame(orders_row,orders_schema)

# this is a hack; as datetime methods wont work on Row() objects
orders_df = orders_df.withColumn('orderdate',f.to_date(orders_df.orderdate))

orders_df.write.partitionBy('orderdate').parquet("/data/tpch/orders.parquet")

orders_df_parquet = spark.read.parquet("/data/tpch/orders.parquet")

orders_df_parquet.createOrReplaceTempView("orders")

spark.sql("SELECT * FROM orders LIMIT 10").show()

# generating partsupp parquet table
partsupp_data = sc.textFile("/data/tpch/partsupp.tbl")
partsupp_split = partsupp_data.map(lambda l: l.split("|"))
partsupp_row = partsupp_split.map( lambda r: Row(
    partkey=int(r[0]),
    suppkey=int(r[1]),
    availqty=int(r[2]),
    supplycost=Decimal(r[3]),
    comment=r[4]
))

partsupp_schema = types.StructType([
       types.StructField('partkey',types.IntegerType(),False)
       ,types.StructField('suppkey',types.IntegerType(),False)
       ,types.StructField('availqty',types.IntegerType())
       ,types.StructField('supplycost',types.DecimalType())
       ,types.StructField('comment',types.StringType())])

partsupp_df = spark.createDataFrame(partsupp_row,partsupp_schema)

partsupp_df.write.parquet("/data/tpch/partsupp.parquet")

partsupp_df_parquet = spark.read.parquet("/data/tpch/partsupp.parquet")

partsupp_df_parquet.createOrReplaceTempView("partsupp")

spark.sql("SELECT * FROM partsupp LIMIT 10").show()

# generating supplier parquet table
supplier_data = sc.textFile("/data/tpch/supplier.tbl")
supplier_split = supplier_data.map(lambda l: l.split("|"))
supplier_row = supplier_split.map( lambda r: Row(
    suppkey=int(r[0]),
    name=r[1],
    address=r[2],
    nationkey=int(r[3]),
    phone=r[4],
    acctbal=Decimal(r[5]),
    comment=r[6]
))

supplier_schema = types.StructType([
       types.StructField('suppkey',types.IntegerType(),False)
       ,types.StructField('name',types.StringType())
       ,types.StructField('address',types.StringType())
       ,types.StructField('nationkey',types.IntegerType())
       ,types.StructField('phone',types.StringType())
       ,types.StructField('acctbal',types.DecimalType())
       ,types.StructField('comment',types.StringType())])

supplier_df = spark.createDataFrame(supplier_row,supplier_schema)

supplier_df.write.parquet("/data/tpch/supplier.parquet")

supplier_df_parquet = spark.read.parquet("/data/tpch/supplier.parquet")

supplier_df_parquet.createOrReplaceTempView("supplier")

spark.sql("SELECT * FROM supplier LIMIT 10").show()

# generating part parquet table

part_data = sc.textFile("/data/tpch/part.tbl")
part_split = part_data.map(lambda l: l.split("|"))
part_row = part_split.map( lambda r: Row(
    partkey=int(r[0]),
    name=r[1],
    mfgr=r[2],
    brand=r[3],
    type=r[4],
    size=int(r[5]),
    container=r[6],
    retailprice=Decimal(r[7]),
    comment=r[8]
))

part_schema = types.StructType([
       types.StructField('partkey',types.IntegerType(),False)
       ,types.StructField('name',types.StringType())
       ,types.StructField('mfgr',types.StringType())
       ,types.StructField('brand',types.StringType())
       ,types.StructField('type',types.StringType())
       ,types.StructField('size',types.IntegerType())
       ,types.StructField('container',types.StringType())
       ,types.StructField('retailprice',types.DecimalType())
       ,types.StructField('comment',types.StringType())])

part_df = spark.createDataFrame(part_row,part_schema)

part_df.write.parquet("/data/tpch/part.parquet")

part_df_parquet = spark.read.parquet("/data/tpch/part.parquet")

part_df_parquet.createOrReplaceTempView("part")

spark.sql("SELECT * FROM part LIMIT 10").show()

spark.stop()
