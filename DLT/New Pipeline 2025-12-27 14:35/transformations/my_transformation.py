import dlt
from pyspark.sql.functions import *

#Creating streaming tables for orders
@dlt.table(
    table_properties={"quality": "bronze"},
    comment="orders Raw data from the bronze layer"
)
def orders_bronze_table():
    df=spark.readStream.table("data_dev.bronze.orders_raw")
    return df

#Creating a materialized view for customers
@dlt.table(
    table_properties={"quality": "bronze"},
    comment="Customers Raw data from the bronze layer"
)
def customers_bronze_table():
    df=spark.read.table("data_dev.bronze.customers_raw")
    return df

#Create a view for joining orders with customers
@dlt.view(
    comment="Joined view of orders and customers"
)
def joined_View():
    df_customers=spark.read.table("LIVE.customers_bronze_table")
    df_orders=spark.read.table("LIVE.orders_bronze_table")
    df_join=df_orders.join(df_customers, how = "left_outer", on=df_customers.c_custkey==df_orders.o_custkey)
    return df_join

#create materialized view to add new column
from pyspark.sql.functions import current_timestamp
@dlt.table(
    table_properties={"quality": "silver"},
    comment="Joined view of orders and customers",
    name = "joined_silver"
)
def joined_silver():
    df=spark.read.table("LIVE.joined_View").withColumn("__insert_date", current_timestamp())
    return df

#Aggregate based on segment and find count of orders.
@dlt.table(
    table_properties={"quality": "gold"},
    comment="agg table of orders and customers"
)
def orders_agg_gold():
    df=spark.read.table("LIVE.joined_silver")
    df_final=df.groupBy("c_mktsegment").agg(count("o_orderkey").alias("SUM_orders")).withColumn("__insert_date", current_timestamp())
    return df_final


