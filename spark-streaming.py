import os
import sys

os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_161/jre"
os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.6-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# utility function for calculationg total value of a single order
def get_total_order_value(items, order_type):
	cost = 0
	type_multiplier = 1 if order_type == 'ORDER' else -1
	for item in items:
		cost = cost + (item['unit_price']  * item['quantity'])

    	return cost * type_multiplier
    
# utility function for calculationg number of items presnt in a single order
def get_total_item_count(items):
    total_count = 0
    for item in items:
        total_count = total_count + item['quantity']
    return total_count
    
# utility function for is_order flag value of a single order
def get_order_flag(order_type):
    return 1 if order_type == 'ORDER' else 0
    
# utility function for is_return flag value of a single order
def get_return_flag(order_type):
    return 1 if order_type == 'RETURN' else 0
    
if __name__ == "__main__":
     # validate command line arguments
    if len(sys.argv) != 4:
        print( "Usage: spark-submit driver.py <hostname> <port> <topic>")
        exit(-1)
        
    host = sys.argv[1]
    port = sys.argv[2]
    topic = sys.argv[3] 
    
    #initialize Spark Session
    spark = SparkSession \
           .builder \
           .appName("retailOrderAnalyzer") \
           .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    
    #Read Input from Kafka
    orderRaw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", host + ":" + port) \
        .option("startingOffsets", "latest") \
        .option("subscribe", topic) \
        .option("failOnDataLoss", False) \
        .load()
        
    # Define scheme of a single order
    jsonSchema = StructType() \
        .add("invoice_no", LongType() ) \
        .add("timestamp", TimestampType() ) \
        .add("type", StringType() ) \
        .add("country", StringType() ) \
        .add("items", ArrayType(StructType([
        StructField("SKU", StringType()),
        StructField("title", StringType()),
        StructField("unit_price", DoubleType()),
        StructField("quantity", IntegerType()),
        ])))
        
        
    orderStream = orderRaw.select( from_json(col("value").cast("string"), jsonSchema).alias("data")).select("data.*") 
        
    
    # Define the UDFs with the utility functions
    add_total_order_value = udf(get_total_order_value, DoubleType())
    add_total_item_count = udf(get_total_item_count, IntegerType())   
    add_order_flag = udf(get_order_flag, IntegerType())
    add_return_flag = udf(get_return_flag, IntegerType())    
        
        
        
    # Calculate additional columns
    expandedOrderStream = orderStream \
        .withColumn("total_cost",add_total_order_value(orderStream.items, orderStream.type)) \
        .withColumn("total_items",add_total_item_count(orderStream.items)) \
        .withColumn("is_order",add_order_flag(orderStream.items)) \
        .withColumn("total_items",add_return_flag(orderStream.items)) 
        
        
        
    # Write the summarixe input values in console
    extendedOrderQuery = expandedOrderStream \
        .select("invoice_no", "country", "timestamp", "total_cost", "total_items", "is_order", "is_return") \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime = "1 minute") \
        .start()
        
        
    #Calculate time based KPIs
    aggStreamByTime = expandedOrderStream \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(window("timestamp", "1 minute", "1 minute")) \
        .agg(sum("total_cost"),
            avg("total_cost"),
            count("invoice_no").alias("OPM"),
            avg("is_return")) \
        .select("window",
                "OPM",
                format_number("sum(total_cost)", 2).alias("total_sale_volume"),
                format_number("avg(total_cost)", 2).alias("average_transaction_size"),
                format_number("avg(is_return)", 2).alias("rate_of_return")) 
           
    #Calculate time & country based KPIs
    aggStreamByTimeNCountry = expandedOrderStream \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(window("timestamp", "1 minute", "1 minute"),"country") \
        .agg(sum("total_cost"),
            count("invoice_no").alias("OPM"),
            avg("is_return")) \
        .select("window",
                "country",
                "OPM",
                format_number("sum(total_cost)", 2).alias("total_sale_volume"),
                format_number("avg(is_return)", 2).alias("rate_of_return")) 
            
    #Write time based KPIs values
    queryByTime = aggStreamByTime.writeStream \
            .format("json") \
            .outputMode("append") \
            .option("truncate","false") \
            .option("path","/tmp/op1") \
            .option("checkpointLocation","/tmp/cp1") \
            .trigger(processingTime="1 minute") \
            .start()
        
    #Write time & country based KPIs values
    queryByTimeNCountry = aggStreamByTimeNCountry.writeStream \
            .format("json") \
            .outputMode("append") \
            .option("truncate","false") \
            .option("path","/tmp/op1") \
            .option("checkpointLocation","/tmp/cp2") \
            .trigger(processingTime="1 minute") \
            .start()
            
    queryByTimeNCountry.awaitTermination()
    
           
        
        
        
        
        
        
        
        
    

    

