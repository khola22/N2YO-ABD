# Imports
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import explode, col, from_json, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, ArrayType
from pyspark.sql.functions import (
    explode, col, from_json, lag, sqrt, pow as spark_pow,
    sin, cos, asin, radians
)

# 1. CONSTRUCTION PLAN
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

# 2. SESSION SPARK CREATION
spark = SparkSession.builder \
    .appName("SatelliteStream") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()
    # .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \

# ─────────────────────────────────────────────────────────────────────────────
# TRAITEMENT 2 — VITESSE ORBITALE (formule de Haversine)
# Stockage  : Cassandra (Speed Layer) → table orbital_speed
# Objectif  : Estimer la vitesse instantanée de l'ISS en km/s entre deux
#             positions consécutives. Permet de détecter des anomalies orbitales
#             (manœuvres, freinage atmosphérique). Stocké en Cassandra pour
#             alertes temps réel. Parquet pour analyse de tendance long terme.
#
# Formule Haversine :
#   a = sin²(Δlat/2) + cos(lat1) * cos(lat2) * sin²(Δlon/2)
#   d = 2R * asin(√a)        R = rayon Terre + altitude (~6800 km)
# ─────────────────────────────────────────────────────────────────────────────


# 3. CALCULATION FONCTION (orbital speed)
def compute_speed_and_write(batch_df, batch_id):
    # grouping satellite positions and sorting them chronologically
    window_spec = Window.partitionBy("satid").orderBy("timestamp")
    
    result = batch_df \
        # Retrievement of the previous point (latitude, longitude, and timestamp)
        .withColumn("prev_lat",       lag("satlatitude",  1).over(window_spec)) \
        .withColumn("prev_lon",       lag("satlongitude", 1).over(window_spec)) \
        .withColumn("prev_timestamp", lag("timestamp",    1).over(window_spec)) \
        # Preparing the sphere variables
        .withColumn("R", col("sataltitude") + 6371.0) \
        .withColumn("dlat", radians(col("satlatitude")  - col("prev_lat"))) \
        .withColumn("dlon", radians(col("satlongitude") - col("prev_lon"))) \
        # Haversine formula
        .withColumn("a",
            spark_pow(sin(col("dlat") / 2), 2) +
            cos(radians(col("prev_lat"))) *
            cos(radians(col("satlatitude"))) *
            spark_pow(sin(col("dlon") / 2), 2)
        ) \
        .withColumn("distance_km", 2 * col("R") * asin(sqrt(col("a")))) \
        # Speed calculation
        .withColumn("delta_t", col("timestamp") - col("prev_timestamp")) \
        .withColumn("speed_km_s",
            when(col("delta_t") > 0, col("distance_km") / col("delta_t"))
            .otherwise(None)
        ) \
        .drop("prev_lat", "prev_lon", "prev_timestamp", "R",
              "dlat", "dlon", "a", "distance_km", "delta_t")
    
    # Écrire dans Cassandra
    result.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="positions", keyspace="satellite") \
        .save()
    
    # Écrire en Parquet
    result.write \
        .format("parquet") \
        .mode("append") \
        .save("/opt/spark/home/parquet_data")

# 4. LECTURE AND PARSING
# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "satellite-positions") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
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

# ─────────────────────────────────────────────────────────────────────────────
# TRAITEMENT 1 — POSITIONS BRUTES
# Stockage  : Cassandra (Speed Layer) + Parquet (Batch Layer)
# Objectif  : Conserver chaque position reçue pour historique et requêtes live.
#             Cassandra permet des lectures en <10ms pour le dashboard temps réel.
#             Parquet permet les analyses historiques en batch (trajectoires, stats).
# ─────────────────────────────────────────────────────────────────────────────

# 5. QUERIES STARTING   
queryCassandra = positions_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="positions", keyspace="satellite") \
    .option("checkpointLocation", "/opt/spark/home/checkpoint_cassandra") \
    .outputMode("append") \
    .start()
    # .trigger(processingTime='20 seconds') \
    # .option("keyspace", "satellite") \
    # .option("table", "positions") \


# NB : Le problème est fondamental — Window.partitionBy().orderBy() avec lag() ne peut jamais être 
# appliqué directement sur un stream, peu importe la table ou la structure.
# Spark streaming ne peut pas regarder en arrière dans l'historique des données — il ne voit qu'un batch à la fois.
# C'est pour ça que foreachBatch est la seule solution — 
# il convertit temporairement chaque mini-batch en DataFrame statique où lag() est autorisé
# C'ets pour cela que foreachBatch est la seule solution:

queryFinal = positions_df.writeStream \
    .foreachBatch(compute_speed_and_write) \
    .trigger(processingTime='10 seconds') \
    .option("checkpointLocation", "/opt/spark/home/checkpoint_final") \
    .outputMode("append") \
    .start()

try:
    queryFinal.awaitTermination()
except Exception as e:
    print(f"Erreur pendant le streaming : {e}")