"""Module 6 (applied 🏏) — a PySpark batch job on the Iceberg lakehouse.

Reads lakehouse.cricket.batting (Iceberg, via the Lakekeeper REST catalog), derives innings +
fifty-plus with the DataFrame API, and writes a new Iceberg table lakehouse.cricket_spark.
batting_summary — proving Spark and Trino share the same open tables. The Spark<->Iceberg wiring
(catalog, S3/MinIO creds) is supplied by the SparkApplication's sparkConf, so the code just uses
`spark.table("lakehouse.cricket.batting")`.

Run it as a SparkApplication on the Spark Operator (see README + cricket-batch-sparkapplication.yaml).
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("cricket-batch").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

bat = spark.table("lakehouse.cricket.batting")
print("READ lakehouse.cricket.batting rows:", bat.count())

ranges = ["_0_9", "_10_19", "_20_29", "_30_39", "_40_49", "_50_59",
          "_60_69", "_70_79", "_80_89", "_90_99", "_100_149", "_150x"]
innings = sum([F.col(c) for c in ranges])                      # innings = Σ run-range buckets
fifties = (F.col("_50_59") + F.col("_60_69") + F.col("_70_79")
           + F.col("_80_89") + F.col("_90_99") + F.col("_100_149") + F.col("_150x"))

summary = (bat
           .withColumn("innings", innings)
           .withColumn("fifty_plus", fifties)
           .select("player", "season", "total_runs", "innings", "fifty_plus")
           .orderBy(F.desc("total_runs")))
summary.show(5, truncate=False)

# Spark writing Iceberg (Day 63): createOrReplace is idempotent (a full-overwrite snapshot)
spark.sql("CREATE SCHEMA IF NOT EXISTS lakehouse.cricket_spark")
(summary.writeTo("lakehouse.cricket_spark.batting_summary")
        .using("iceberg").createOrReplace())
print("WROTE lakehouse.cricket_spark.batting_summary rows:",
      spark.table("lakehouse.cricket_spark.batting_summary").count())

spark.stop()
