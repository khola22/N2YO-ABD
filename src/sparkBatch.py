# Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, count, from_unixtime, to_date

# 1. SPARK SESSION
spark = SparkSession.builder \
    .appName("SatelliteBatchAnalysis") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.sql.caseSensitive", "false") \
    .getOrCreate()

# 2. LECTURE
historical_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="positions", keyspace="satellite") \
    .load()

print("--- SCHEMA DETECTE ---")
historical_df.printSchema()

# Filtre pour éviter les erreurs sur les calculs
filtered_df = historical_df.filter(col("speed_km_s").isNotNull())

# 3. TREATMENT
print("Calcul des stats quotidiennes...")
batch_stats = filtered_df.withColumn("day", to_date(from_unixtime(col("timestamp")))) \
    .groupBy("satid", "satname", "day") \
    .agg(
        avg("speed_km_s").alias("avg_speed"),
        max("sataltitude").alias("max_altitude"),
        min("sataltitude").alias("min_altitude"),
        count("*").alias("total_records") # Spark sort un BigInt/Long par défaut
    )

# Optimisation : on fait le count une seule fois
result_count = batch_stats.count()
print(f"Nombre de lignes agrégées : {result_count}")

if result_count > 0:
    batch_stats.show()
    # 4. WRITING
    print("Sauvegarde dans Cassandra (daily_stats)...")
    batch_stats.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="daily_stats", keyspace="satellite") \
        .mode("append") \
        .save()
    print("✅ Job Batch terminé avec succès !")
else:
    print("❌ Erreur : batch_stats est vide. Vérifie le contenu de la table 'positions'.")

spark.stop()