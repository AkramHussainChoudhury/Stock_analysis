from pyspark.sql import SparkSession, functions
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'



def main():
    spark: SparkSession = SparkSession.builder.appName("Streaming_Crypto_application") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "3") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    inputStreamDf = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "stock_topic") \
        .option("startingOffsets", "earliest") \
        .load()

    valueDf = inputStreamDf.selectExpr("CAST(value AS STRING)")

    cryptoDf = valueDf.selectExpr("split(value,',') as values") \
        .selectExpr("cast(values[0] as String) as Type",
                    "cast(values[1] as Double) as Close",
                    "cast(values[2] as Timestamp) as Updatetime"
                    )

    priceDf = cryptoDf \
        .withWatermark("Updatetime", "30 minute") \
        .groupBy("Type") \
        .agg(functions.max("Close").alias("maxPrice"), functions.min("Close").alias("minPrice"))

    writeDf = priceDf.writeStream \
        .format("console") \
        .outputMode("update") \
        .option("checkpointLocation", ".\checkpoint") \
        .trigger(processingTime='1 minute') \
        .start()

    writeDf.awaitTermination()






if __name__=="__main__":
    main()