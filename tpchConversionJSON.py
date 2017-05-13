from pyspark import SparkContext
from pyspark.sql import SparkSession, types, Row
from datetime import date, datetime
from decimal import *

sc = SparkContext()
spark = SparkSession(sc)

#Raw Data
RawPath="~raw_data_file_path~"
customer= sc.textFile(RawPath + "customer.tbl*")
orders=sc.textFile(RawPath + "orders.tbl*")
lineitem=sc.textFile(RawPath + "lineitem.tbl*")
supplier=sc.textFile(RawPath + "supplier.tbl*")
nation=sc.textFile(RawPath + "nation.tbl*")
region=sc.textFile(RawPath + "region.tbl*")
part=sc.textFile(RawPath + "part.tbl*")

JsonPath="~export_file_path~"

def string_to_date(date_string):
    """Return a `date` object, created from the string ``date_string``."""
    date_values = date_string.split('-')
    return date(year=int(date_values[0]), month=int(date_values[1]), day=int(date_values[2]))

def getCustomerTblSchema(data):
# root
#  |-- c_custkey: integer (nullable = true)
#  |-- c_name: string (nullable = true)
#  |-- c_address: string (nullable = true)
#  |-- c_nationkey: integer (nullable = true)
#  |-- c_phone: string (nullable = true)
#  |-- c_acctbal: decimal(12,2) (nullable = true)
#  |-- c_mktsegment: string (nullable = true)
#  |-- c_comment: string (nullable = true)
 
    # map the data, return data as a Row, and cast data types of some fields
    split = data.map(lambda l: l.split("|"))
    row = split.map(lambda r: Row(
        c_custkey=int(r[0]),
        c_name=r[1],
        c_address=r[2],
        c_nationkey=int(r[3]),
        c_phone=r[4],
        c_acctbal=Decimal(r[5]),
        c_mktsegment=r[6],
        c_comment=r[7])
        )

    #build strict schema for the table closely based on the original sql schema
    schema = types.StructType([
           types.StructField('c_custkey',types.IntegerType(),False)
           ,types.StructField('c_name',types.StringType())
           ,types.StructField('c_address',types.StringType())
           ,types.StructField('c_nationkey',types.IntegerType())
           ,types.StructField('c_phone',types.StringType())
           ,types.StructField('c_acctbal',types.DecimalType(12,2))
           ,types.StructField('c_mktsegment',types.StringType())
           ,types.StructField('c_comment',types.StringType())])
    return row, schema

def getOrdersTblSchema(data):
# root
#  |-- o_orderkey: integer (nullable = true)
#  |-- o_custkey: integer (nullable = true)
#  |-- o_orderstatus: string (nullable = true)
#  |-- o_totalprice: decimal(12,2) (nullable = true)
#  |-- o_orderdate: date (nullable = true)
#  |-- o_orderpriority: string (nullable = true)
#  |-- o_clerk: string (nullable = true)
#  |-- o_shippriority: integer (nullable = true)
#  |-- o_comment: string (nullable = true)
 
    # map the data, return data as a Row, and cast data types of some fields
    split = data.map(lambda l: l.split("|"))
    row = split.map(lambda r: Row(
        o_orderkey=int(r[0]),
        o_custkey=int(r[1]),
        o_orderstatus=r[2],
        o_totalprice=Decimal(r[3]),
        o_orderdate=r[4],
        o_orderpriority=r[5],
        o_clerk=r[6],
        o_shippriority=int(r[7]),
        o_comment=r[8]
            )
        )
                    
    #build strict schema for the table closely based on the original sql schema
    schema = types.StructType([
        types.StructField('o_orderkey',types.IntegerType(),False)
        ,types.StructField('o_custkey',types.IntegerType(),False)
        ,types.StructField('o_orderstatus',types.StringType())
        ,types.StructField('o_totalprice',types.DecimalType(12,2))
        ,types.StructField('o_orderdate',types.StringType())
        ,types.StructField('o_orderpriority',types.StringType())
        ,types.StructField('o_clerk',types.StringType())
        ,types.StructField('o_shippriority',types.IntegerType())
        ,types.StructField('o_comment',types.StringType())
        ])
    
    return row, schema
    
def getLineitemTblSchema(data):
# root
#  |-- l_orderkey: integer (nullable = true)
#  |-- l_partkey: integer (nullable = true)
#  |-- l_suppkey: integer (nullable = true)
#  |-- l_linenumber: integer (nullable = true)
#  |-- l_quantity: decimal(12,2) (nullable = true)
#  |-- l_extendedprice: decimal(12,2) (nullable = true)
#  |-- l_discount: decimal(12,2) (nullable = true)
#  |-- l_tax: decimal(12,2) (nullable = true)
#  |-- l_returnflag: string (nullable = true)
#  |-- l_linestatus: string (nullable = true)
#  |-- l_shipdate: date (nullable = true)
#  |-- l_commitdate: date (nullable = true)
#  |-- l_receiptdate: date (nullable = true)
#  |-- l_shipinstruct: string (nullable = true)
#  |-- l_shipmode: string (nullable = true)
#  |-- l_comment: string (nullable = true)
 
    # map the data, return data as a Row, and cast data types of some fields
    split = data.map(lambda l: l.split("|"))
    row = split.map(lambda r: Row(
        l_orderkey=int(r[0]),
        l_partkey=int(r[1]),
        l_suppkey=int(r[2]),
        l_linenumber=int(r[3]),
        l_quantity=Decimal(r[4]),
        l_extendedprice=Decimal(r[5]),
        l_discount=Decimal(r[6]),
        l_tax=Decimal(r[7]),
        l_returnflag=r[8],
        l_linestatus=r[9],
        l_shipdate=r[10],
        l_commitdate=r[11],
        l_receiptdate=r[12],
        l_shipinstruct=r[13],
        l_shipmode=r[14],
        l_comment=r[15]
            )
        )
                    
    #build strict schema for the table closely based on the original sql schema
    schema = types.StructType([
        types.StructField('l_orderkey',types.IntegerType(),False)
        ,types.StructField('l_partkey',types.IntegerType(),False)
        ,types.StructField('l_suppkey',types.IntegerType(),False)
        ,types.StructField('l_linenumber',types.IntegerType())
        ,types.StructField('l_quantity',types.DecimalType(12,2))
        ,types.StructField('l_extendedprice',types.DecimalType(12,2))
        ,types.StructField('l_discount',types.DecimalType(12,2))
        ,types.StructField('l_tax',types.DecimalType(12,2))
        ,types.StructField('l_returnflag',types.StringType())
        ,types.StructField('l_linestatus',types.StringType())
        ,types.StructField('l_shipdate',types.StringType())
        ,types.StructField('l_commitdate',types.StringType())
        ,types.StructField('l_receiptdate',types.StringType())
        ,types.StructField('l_shipinstruct',types.StringType())
        ,types.StructField('l_shipmode',types.StringType())
        ,types.StructField('l_comment',types.StringType())
    ])
    
    return row, schema

def getSupplierTblSchema(data):
# root
#  |-- s_suppkey: integer (nullable = true)
#  |-- s_name: string (nullable = true)
#  |-- s_address: string (nullable = true)
#  |-- s_nationkey: integer (nullable = true)
#  |-- s_phone: string (nullable = true)
#  |-- s_acctbal: decimal(12,2) (nullable = true)
#  |-- s_comment: string (nullable = true)
 
    # map the data, return data as a Row, and cast data types of some fields
    split = data.map(lambda l: l.split("|"))
    row = split.map(lambda r: Row(
        s_suppkey=int(r[0]),
        s_name=r[1],
        s_address=r[2],
        s_nationkey=int(r[3]),
        s_phone=r[4],
        s_acctbal=Decimal(r[5]),
        s_comment=r[6]
            )
        )
                    
                    
    #build strict schema for the table closely based on the original sql schema
    schema = types.StructType([
        types.StructField('s_suppkey',types.IntegerType(),False)
        ,types.StructField('s_name',types.StringType())
        ,types.StructField('s_address',types.StringType())
        ,types.StructField('s_nationkey',types.IntegerType())
        ,types.StructField('s_phone',types.StringType())
        ,types.StructField('s_acctbal',types.DecimalType(12,2))
        ,types.StructField('s_comment',types.StringType())
        ])
    
    return row, schema

def getNationTblSchema(data):
# root
#  |-- n_nationkey: integer (nullable = true)
#  |-- n_name: string (nullable = true)
#  |-- n_regionkey: integer (nullable = true)
#  |-- n_comment: string (nullable = true)
 
    # map the data, return data as a Row, and cast data types of some fields
    split = data.map(lambda l: l.split("|"))
    row = split.map(lambda r: Row(
        n_nationkey=int(r[0]),
        n_name=r[1],
        n_regionkey=int(r[2]),
        n_comment=r[3]
            )
        )
                    
    #build strict schema for the table closely based on the original sql schema
    schema = types.StructType([
        types.StructField('n_nationkey',types.IntegerType(),False)
        ,types.StructField('n_name',types.StringType())
        ,types.StructField('n_regionkey',types.IntegerType())
        ,types.StructField('n_comment',types.StringType())
        ])
    
    return row, schema

def getRegionTblSchema(data):
# root
#  |-- r_regionkey: integer (nullable = true)
#  |-- r_name: string (nullable = true)
#  |-- r_comment: string (nullable = true)
 
    # map the data, return data as a Row, and cast data types of some fields
    split = data.map(lambda l: l.split("|"))
    row = split.map(lambda r: Row(
        r_regionkey=int(r[0]),
        r_name=r[1],
        r_comment=r[2]
            )
        )
                    
    #build strict schema for the table closely based on the original sql schema
    schema = types.StructType([
        types.StructField('r_regionkey',types.IntegerType(),False)
        ,types.StructField('r_name',types.StringType())
        ,types.StructField('r_comment',types.StringType())
        ])
    
    return row, schema

def getPartTblSchema(data):
# root
#  |-- p_partkey: integer (nullable = true)
#  |-- p_name: string (nullable = true)
#  |-- p_mfgr: string (nullable = true)
#  |-- p_brand: string (nullable = true)
#  |-- p_type: string (nullable = true)
#  |-- p_size: integer (nullable = true)
#  |-- p_container: string (nullable = true)
#  |-- p_retailprice: decimal(12,2) (nullable = true)
#  |-- p_comment: string (nullable = true)

    # map the data, return data as a Row, and cast data types of some fields
    split = data.map(lambda l: l.split("|"))
    row = split.map(lambda r: Row(
        p_partkey=int(r[0]),
        p_name=r[1],
        p_mfgr=r[2],
        p_brand=r[3],
        p_type=r[4],
        p_size=int(r[5]),
        p_container=r[6],
        p_retailprice=Decimal(r[7]),
        p_comment=r[8]
            )
        )
                    
    #build strict schema for the table closely based on the original sql schema
    schema = types.StructType([
        types.StructField('p_partkey',types.IntegerType(),False)
        ,types.StructField('p_name',types.StringType())
        ,types.StructField('p_mfgr',types.StringType())
        ,types.StructField('p_brand',types.StringType())
        ,types.StructField('p_type',types.StringType())
        ,types.StructField('p_size',types.IntegerType())
        ,types.StructField('p_container',types.StringType())
        ,types.StructField('p_retailprice',types.DecimalType(12,2))
        ,types.StructField('p_comment',types.StringType())
        ])
    
    return row, schema

def createTable(tblName):
    """Create table from raw data and write to json file
    @param tblName string name of the table
    """
    row = None
    schema = None
    if tblName == "customer":
        row, schema = getCustomerTblSchema(customer)
    elif tblName == "orders":
        row, schema = getOrdersTblSchema(orders)
    elif tblName == "lineitem":
        row, schema = getLineitemTblSchema(lineitem)
    elif tblName == "supplier":
        row, schema = getSupplierTblSchema(supplier)
    elif tblName == "nation":
        row, schema = getNationTblSchema(nation)
    elif tblName == "region":
        row, schema = getRegionTblSchema(region)
    elif tblName == "part":
        row, schema = getPartTblSchema(part)
    else:
        return

    # create data frame by passing in the mapped data AND its strict schema
    df = spark.createDataFrame(row, schema)

    #create json file
    df.write.json(JsonPath + tblName + ".json")
    #write to local - for testing purpose
    #df.write.json("./" + tblName + ".json")

    # create temp table name of the new table
    df.createOrReplaceTempView(tblName)
    return

createTable("customer")
createTable("orders")
createTable("lineitem")
createTable("supplier")
createTable("nation")
createTable("region")
createTable("part")

spark.stop()
