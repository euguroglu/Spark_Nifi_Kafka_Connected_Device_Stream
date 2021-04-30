from os.path import abspath
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import from_json, col, to_timestamp, window, expr, sum, approx_count_distinct, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

warehouse_location = abspath('spark-warehouse')

#Define foreach batch function to aggrate stream data several times and print console
def foreach_batch_func(df, epoch_id):
    df = df.sort(desc("source_number"))
    df \
        .write.format("console") \
        .save()
    pass

#Define foreach batch function to aggrate stream data and sink to hive
def foreach_batch_func2(df, epoch_id):
    df = df.sort(desc("source_number"))
    df \
        .write.mode("append") \
        .insertInto("commerce") \
    pass

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Tumbling Window Stream Users Platform") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .config("spark.sql.shuffle.partitions", 2) \
        .enableHiveSupport() \
        .getOrCreate()

#Describe schema (productid will be enough to find viewed category in the last 5 minute)
    schema = StructType([
    StructField("context", StructType([
        StructField("source", StringType())
    ])),
    StructField("timestamp", StringType())
])
#Read data from kafka topic
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "platform") \
        .option("startingOffsets", "earliest") \
        .load()
#Data in kafka topic have key-value format, from_json is used to deserialize json value from string
    value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value"))
#Checking schema if everything is correct
    value_df.printSchema()
#Explode dataframe to remove sub-structures
    explode_df = value_df.selectExpr("value.context.source", "value.timestamp")
#Checking schema if everything is correct
    explode_df.printSchema()
#Set timeParserPolicy=Legacy to parse timestamp in given format
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
#Convert string type to timestamp
    transformed_df = explode_df.select("source", "timestamp") \
        .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \

#Checcking schema if everything is correct
    transformed_df.printSchema()
#Create 5 min window
#Create watermark to autoclean history
#Groupby product_id and count considering distinct users
#Rename new column as count
    window_count_df = transformed_df \
        .withWatermark("timestamp", "5 minute") \
        .groupBy(col("source"),
            window(col("timestamp"),"5 minute")).count()

    output_df = window_count_df.select("window.start", "window.end", "source", "count") \
        .withColumn("source_number", col("count")) \
        .drop("count")
#Write spark stream to console or csv sink

    output_df.printSchema()

#Write spark stream to console or csv sink
    window_query = output_df.writeStream \
    .foreachBatch(lambda df, epoch_id: foreach_batch_func(df, epoch_id))\
    .option("checkpointLocation", "chk-point-dir") \
    .outputMode("update") \
    .trigger(processingTime="5 minutes") \
    .start()

    window_query2 = output_df.writeStream \
    .foreachBatch(lambda df, epoch_id: foreach_batch_func2(df, epoch_id))\
    .option("checkpointLocation", "chk-point-dir") \
    .outputMode("update") \
    .trigger(processingTime="5 minutes") \
    .start()

    window_query.awaitTermination()
