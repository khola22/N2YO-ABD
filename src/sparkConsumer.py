from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, ArrayType

# Define schema for the JSON
"""
{'info': 
    {
        'satname': 'SPACE STATION', 
        'satid': 25544, 
        'transactionscount': 0
    }, 
    'positions': 
        [
            {
                'satlatitude': 51.75853274, 
                'satlongitude': 178.21328368, 
                'sataltitude': 419.54, 
                'azimuth': 320.62, 
                'elevation': -31.5, 
                'ra': 228.37335072, 
                'dec': 9.21832668, 
                'timestamp': 1771186481, 
                'eclipsed': False
            }, 
        ]
    }
"""

# Construction plan
# Position of the satellite
position_schema = StructType([
    StructField("satlatitude", DoubleType()),
    StructField("satlongitude", DoubleType()),
    StructField("sataltitude", DoubleType()),
    StructField("azimuth", DoubleType()),
    StructField("elevation", DoubleType()),
    StructField("ra", DoubleType()),
    StructField("dec", DoubleType()),
    StructField("timestamp", IntegerType()),
    StructField("eclipsed", BooleanType())
])

# Name of the satellite
info_schema = StructType([
    StructField("satname", StringType()),
    StructField("satid", IntegerType()),
    StructField("transactionscount", IntegerType())
])

schema = StructType([
    StructField("info", info_schema),
    StructField("positions", ArrayType(position_schema))
])

# SparksSession creation
spark = SparkSession.builder \
    .appName("SatelliteStream") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()
    # .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
    

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "satellite-positions") \
    .load()

# Parse JSON
json_df = df.select(from_json(col("value").cast("string"), schema).alias("data"))
positions_df = json_df.select(
    col("data.info.satname").alias("satname"),
    col("data.info.satid").alias("satid"),
    explode(col("data.positions")).alias("position")
).select(
    "satname", "satid",
    col("position.satlatitude").alias("satlatitude"),
    col("position.satlongitude").alias("satlongitude"),
    col("position.sataltitude").alias("sataltitude"),
    col("position.timestamp").alias("timestamp"),
    col("position.eclipsed").alias("eclipsed")
)

# Write to Cassandra (Speed Layer)
query1 = positions_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "satellite") \
    .option("table", "positions") \
    .option("checkpointLocation", "/opt/spark/home/checkpoint_cassandra") \
    .trigger(processingTime='200 seconds') \
    .outputMode("append") \
    .start()

# Write to Parquet (Batch Layer)
# The /home/parquet_data path in the consumer maps to the ./ volume already mounted on Cassandra. We should instead use /opt/spark/home/parquet_data to keep it within the spark_home volume that's consistently mounted across your Spark services:
# Why not inside the producer container? 
# The Dockerfile/producer service uses a lightweight python:3.11-slim image with no JVM — PySpark requires Java, so it must live on the Spark image. 
# Keeping them separate also respects the Lambda architecture the pipeline is building toward (producer = ingestion layer, Spark = batch + speed layers).
query2 = positions_df.writeStream \
    .format("parquet") \
    .option("path", "/opt/spark/home/parquet_data") \
    .option("checkpointLocation","/opt/spark/home/parquet_checkpoint") \
    .outputMode("append") \
    .start()

# This will now print WHY a stream stopped
for q in [query1, query2]:
    try:
        q.awaitTermination()
    except Exception as e:
        print(f"Stream failed: {e}")
        raise

spark.streams.awaitAnyTermination()