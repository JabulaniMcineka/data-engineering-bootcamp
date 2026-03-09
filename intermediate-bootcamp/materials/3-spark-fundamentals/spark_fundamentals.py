from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col, broadcast

spark = SparkSession.builder \
    .appName("Spark Fundamentals Homework") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Load Data
match_details = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/match_details.csv")
matches = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/matches.csv")
medals_matches_players = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals_matches_players.csv")
medals = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals.csv")
maps = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/maps.csv")

# Create namespace
spark.sql("CREATE DATABASE IF NOT EXISTS nyc")

# Bucket Join Setup
match_details.write.mode("overwrite").bucketBy(16, "match_id").saveAsTable("nyc.match_details_bucketed")
matches.write.mode("overwrite").bucketBy(16, "match_id").saveAsTable("nyc.matches_bucketed")
medals_matches_players.write.mode("overwrite").bucketBy(16, "match_id").saveAsTable("nyc.medals_matches_players_bucketed")

# Read bucketed tables
match_details_b = spark.table("nyc.match_details_bucketed")
matches_b = spark.table("nyc.matches_bucketed")
medals_matches_players_b = spark.table("nyc.medals_matches_players_bucketed")

# Joins
joined = match_details_b \
    .join(matches_b, "match_id") \
    .join(medals_matches_players_b, "match_id") \
    .join(broadcast(medals), "medal_id") \
    .join(broadcast(maps), "mapid")

# Q1: Which player averages the most kills per game?
print("=== Player with Most Average Kills Per Game ===")
joined.groupBy(match_details_b["player_gamertag"]) \
    .agg(avg("player_total_kills").alias("avg_kills")) \
    .orderBy(col("avg_kills").desc()) \
    .show(5)

# Q2: Which playlist gets played the most?
print("=== Most Played Playlist ===")
joined.groupBy("playlist_id") \
    .agg(count("match_id").alias("total_matches")) \
    .orderBy(col("total_matches").desc()) \
    .show(5)

# Q3: Which map gets played the most?
print("=== Most Played Map ===")
joined.groupBy(maps["name"]) \
    .agg(count("match_id").alias("total_matches")) \
    .orderBy(col("total_matches").desc()) \
    .show(5)

# Q4: Which map do players get the most Killing Spree medals on?
print("=== Map with Most Killing Spree Medals ===")
joined.filter(col("classification") == "KillingSpree") \
    .groupBy(maps["name"]) \
    .agg(count("medal_id").alias("total_killing_sprees")) \
    .orderBy(col("total_killing_sprees").desc()) \
    .show(5)

# sortWithinPartitions
# Select specific columns to avoid duplicates
joined_clean = joined.select(
    match_details_b["match_id"],
    match_details_b["player_gamertag"],
    match_details_b["player_total_kills"],
    matches_b["playlist_id"],
    maps["name"],
    medals["classification"]
)
joined_clean.sortWithinPartitions("playlist_id").write.mode("overwrite").parquet("/home/iceberg/output/sorted_by_playlist")
joined_clean.sortWithinPartitions("name").write.mode("overwrite").parquet("/home/iceberg/output/sorted_by_map")
joined_clean.sortWithinPartitions("player_gamertag").write.mode("overwrite").parquet("/home/iceberg/output/sorted_by_player")

print("=== Done! ===")
spark.stop()
