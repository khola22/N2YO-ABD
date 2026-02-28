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

info_schema = StructType([
    StructField("satname", StringType()),
    StructField("satid", IntegerType()),
    StructField("transactionscount", IntegerType())
])

schema = StructType([
    StructField("info", info_schema),
    StructField("positions", ArrayType(position_schema))
])

spark = SparkSession.builder \
    .appName("SatelliteStream") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
    .getOrCreate()

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
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
positions_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "satellite") \
    .option("table", "positions") \
    .outputMode("append") \
    .start()

# Write to Parquet (Batch Layer)
positions_df.writeStream \
    .format("parquet") \
    .option("path", "/home/parquet_data") \
    .option("checkpointLocation", "/home/parquet_checkpoint") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()