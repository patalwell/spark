--This file creates the tables from tpc.org for TPCH related queries and database schema

--Part Table

CREATE TABLE IF NOT EXISTS part (
partkey BIGINT,
name VARCHAR(55),
mfgr CHAR(25),
brand CHAR(10),
type VARCHAR(25),
size INT,
container CHAR(10),
retailprice DECIMAL,
comment VARCHAR(23)
)
row format delimited
fields terminated by "|";

LOAD DATA LOCAL INPATH '/data/tpch/part.tbl' INTO TABLE default.part;

select * from part limit 10;

--Supplier Table

CREATE TABLE IF NOT EXISTS supplier (
suppkey BIGINT,
name CHAR(25),
address VARCHAR(40),
nationkey BIGINT,
phone CHAR(15),
acctbal DECIMAL,
comment VARCHAR(101)
)
row format delimited
fields terminated by "|";

LOAD DATA LOCAL INPATH '/data/tpch/supplier.tbl' INTO TABLE default.supplier;

select * from supplier limit 10;

--Partsupp Table

CREATE TABLE IF NOT EXISTS partsupp (
partkey BIGINT,
suppkey BIGINT,
availqty INTEGER,
supplycost DECIMAL,
comment VARCHAR(199)
)
row format delimited
fields terminated by "|";

LOAD DATA LOCAL INPATH '/data/tpch/partsupp.tbl' INTO TABLE default.partsupp;

select * from partsupp limit 10;


--Orders Table
CREATE TABLE IF NOT EXISTS orders (
orderkey BIGINT,
custkey BIGINT,
orderstatus CHAR (1),
totalprice DECIMAL,
orderdate DATE,
orderpriority CHAR(15),
clerk CHAR(15),
shippriority INT,
comment VARCHAR(79)
)
row format delimited
fields terminated by "|";

LOAD DATA LOCAL INPATH '/data/tpch/orders.tbl' INTO TABLE default.orders;

select * from orders limit 10;

--Line Item Table

CREATE TABLE IF NOT EXISTS lineitem (
orderkey BIGINT,
partkey BIGINT,
suppkey BIGINT,
linenumber INT,
quantity DECIMAL,
extendedprice DECIMAL,
discount DECIMAL,
tax DECIMAL,
returnflag CHAR(1),
linestatus CHAR(1),
shipdate DATE,
commitdate DATE,
receiptdate DATE,
shipinstruct CHAR(25),
shipmode CHAR(10),
comment VARCHAR(44)
)
row format delimited
fields terminated by "|";

LOAD DATA LOCAL INPATH '/data/tpch/lineitem.tbl' INTO TABLE default.lineitem;

select * from lineitem limit 10;

--Nation Table

CREATE TABLE IF NOT EXISTS nation (
nationkey BIGINT,
name CHAR(25),
regionkey BIGINT,
comment VARCHAR(152)
)
row format delimited
fields terminated by "|";

LOAD DATA LOCAL INPATH '/data/tpch/nation.tbl' INTO TABLE default.nation;

select * from nation limit 10;

--Region Table

CREATE TABLE IF NOT EXISTS region (
regionkey BIGINT,
name CHAR(25),
comment VARCHAR(152)
)
row format delimited
fields terminated by "|";

LOAD DATA LOCAL INPATH '/data/tpch/region.tbl' INTO TABLE default.region;

select * from region limit 10;

--Customer Table

CREATE TABLE IF NOT EXISTS customer (
custkey BIGINT,
name VARCHAR(25),
address VARCHAR(40),
nationkey BIGINT,
phone CHAR(15)
acctbal DECIMAL,
mktsegment CHAR(10),
comment VARCHAR(117)
)
row format delimited
fields terminated by "|";


LOAD DATA LOCAL INPATH '/data/tpch/customer.tbl' INTO TABLE default.customer;

select * from customer limit 10;
