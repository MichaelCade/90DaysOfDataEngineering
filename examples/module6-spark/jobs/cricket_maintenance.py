"""Module 6 (Day 67) — Iceberg table maintenance from Spark.

Spark calls the Iceberg maintenance *procedures* via SQL: rewrite_data_files (compaction) and
expire_snapshots (reclaim). Same operations Trino exposes as ALTER TABLE ... EXECUTE (Day 33),
here run as a Spark batch job — the natural home for heavy, scheduled maintenance at scale.
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("cricket-maint").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE SCHEMA IF NOT EXISTS lakehouse.cricket_spark")
spark.sql("DROP TABLE IF EXISTS lakehouse.cricket_spark.maint_demo")
spark.sql("CREATE TABLE lakehouse.cricket_spark.maint_demo (id int, v string) USING iceberg")

for i in range(5):                                  # 5 appends -> 5 small data files
    spark.sql(f"INSERT INTO lakehouse.cricket_spark.maint_demo VALUES ({i}, 'row{i}')")

files = lambda: spark.sql("SELECT count(*) FROM lakehouse.cricket_spark.maint_demo.files").collect()[0][0]
snaps = lambda: spark.sql("SELECT count(*) FROM lakehouse.cricket_spark.maint_demo.snapshots").collect()[0][0]
print("BEFORE compaction: files=", files(), "snapshots=", snaps())

# compaction: bin-pack the small files into fewer, larger ones
res = spark.sql("CALL lakehouse.system.rewrite_data_files(table => 'cricket_spark.maint_demo')").collect()
print("rewrite_data_files:", res[0].asDict())
print("AFTER compaction:  files=", files(), "snapshots=", snaps())

# expiry: drop old snapshots + reclaim their files (bounds time travel — Day 33)
spark.sql("""CALL lakehouse.system.expire_snapshots(
             table => 'cricket_spark.maint_demo',
             older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)""").collect()
print("AFTER expire:      files=", files(), "snapshots=", snaps(),
      "rows=", spark.table("lakehouse.cricket_spark.maint_demo").count())
spark.stop()
