# Imports
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import explode, col, from_json, when, from_unixtime, to_timestamp
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
# Do not set -Djava.security.manager=allow here when Spark runs on Java 11.
# On Java 11, the JVM treats "allow" as a SecurityManager class name and
# fails during startup with ClassNotFoundException: allow.
spark = SparkSession.builder \
    .appName("SatelliteStream") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()
    # .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    # .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
    
    # .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \

# ─────────────────────────────────────────────────────────────────────────────
# TRAITEMENT — POSITIONS BRUTES + VITESSE ORBITALE (formule de Haversine)
#
# Stockage Cassandra (Speed Layer) :
#   → Uniquement les positions avec vitesse calculée (speed_km_s non null)
#   → Lectures en <10ms pour le dashboard temps réel et alertes orbitales
#
# Stockage Parquet (Batch Layer) :
#   → Toutes les positions, y compris le premier point (sans vitesse)
#   → Analyses historiques en batch (trajectoires, stats, tendances long terme)
#
# Calcul de vitesse — Formule de Haversine :
#   a = sin²(Δlat/2) + cos(lat1) * cos(lat2) * sin²(Δlon/2)
#   d = 2R * asin(√a)        R = rayon Terre + altitude (~6800 km)
#   vitesse = d / Δt         en km/s
#
# Objectif : Estimer la vitesse instantanée de l'ISS entre deux positions
#            consécutives. Permet de détecter des anomalies orbitales
#            (manœuvres, freinage atmosphérique).
# ─────────────────────────────────────────────────────────────────────────────

# 3. CALCULATION FONCTION (orbital speed)              
def compute_speed_and_write(batch_df, batch_id):
    if batch_df.count() > 0:
        # 1. CALCUL DE LA VITESSE (logique Haversine)
        window_spec = Window.partitionBy("satid").orderBy("timestamp")
        
        enriched_df = batch_df \
            .withColumn("prev_lat",       lag("satlatitude",  1).over(window_spec)) \
            .withColumn("prev_lon",       lag("satlongitude", 1).over(window_spec)) \
            .withColumn("prev_timestamp", lag("timestamp",    1).over(window_spec)) \
            .withColumn("R", col("sataltitude") + 6371.0) \
            .withColumn("dlat", radians(col("satlatitude")  - col("prev_lat"))) \
            .withColumn("dlon", radians(col("satlongitude") - col("prev_lon"))) \
            .withColumn("a",
                spark_pow(sin(col("dlat") / 2), 2) +
                cos(radians(col("prev_lat"))) *
                cos(radians(col("satlatitude"))) *
                spark_pow(sin(col("dlon") / 2), 2)
            ) \
            .withColumn("distance_km", 2 * col("R") * asin(sqrt(col("a")))) \
            .withColumn("delta_t", col("timestamp") - col("prev_timestamp")) \
            .withColumn("speed_km_s",
                when(col("delta_t") > 0, col("distance_km") / col("delta_t"))
                .otherwise(None)
            ) \
            .drop("prev_lat", "prev_lon", "prev_timestamp", "R",
                  "dlat", "dlon", "a", "distance_km", "delta_t") \
            .withColumn("datetime", to_timestamp(from_unixtime(col("timestamp"))))

        # Cassandra : uniquement les points avec vitesse (on ne garde pas les valeurs nulles)
        enriched_df.filter(col("speed_km_s").isNotNull()) \
            .select(
                "satid",
                "timestamp",
                "satname",
                "datetime",
                "satlatitude",
                "satlongitude",
                "sataltitude",
                "eclipsed",
                "speed_km_s",
            ) \
            .write.format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="positions", keyspace="satellite") \
            .save()

        # Parquet : tout l'historique, y compris le premier point
        enriched_df.write \
            .mode("append") \
            .parquet("/opt/spark/home/parquet_data") \
        
        print(f"Batch {batch_id} traité : Cassandra mis à jour et Parquet archivé.")

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

# 5. QUERY STARTING   
queryFinal = positions_df.writeStream \
    .foreachBatch(compute_speed_and_write) \
    .trigger(processingTime='10 seconds') \
    .option("checkpointLocation", "/opt/spark/home/checkpoint_final") \
    .start()

try:
    queryFinal.awaitTermination()
except Exception as e:
    print(f"Erreur pendant le streaming : {e}")

# NB : Le problème est fondamental — Window.partitionBy().orderBy() avec lag() ne peut jamais être 
# appliqué directement sur un stream, peu importe la table ou la structure.
# Spark streaming ne peut pas regarder en arrière dans l'historique des données — il ne voit qu'un batch à la fois.
# C'est pour ça que foreachBatch est la seule solution — 
# il convertit temporairement chaque mini-batch en DataFrame statique où lag() est autorisé
