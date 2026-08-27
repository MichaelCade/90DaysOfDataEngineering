"""Module 6 (Day 68) — Pandera validation INSIDE a Spark job.

pandera.pyspark validates a native Spark DataFrame against a declared schema (types + value
checks) as part of the pipeline code — quality enforced where the data is transformed, before it's
written. Complements Soda (at-rest scans, Day 52) and dbt tests (post-transform, Day 43).

NOTE: the apache/spark image has no pandera; here we pip-install it on the driver at startup (works
but slow — pulls pandas/numpy). For production, BAKE pandera into a custom Spark image instead.
"""
import subprocess
import sys

subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", "--target", "/tmp/pp",
                "pandera[pyspark]==0.20.4"], check=True)
sys.path.insert(0, "/tmp/pp")

from pyspark.sql import SparkSession, functions as F           # noqa: E402
import pandera.pyspark as pa                                    # noqa: E402
from pandera.pyspark import DataFrameSchema, Column             # noqa: E402

spark = SparkSession.builder.appName("cricket-pandera").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Types are part of the contract: Iceberg bigint = Spark LongType, so declaring `int` and NOT
# casting makes pandera (correctly) flag WRONG_DATATYPE. Cast to match the declared schema.
bat = (spark.table("lakehouse.cricket.batting")
       .select("player",
               F.col("season").cast("int").alias("season"),
               F.col("total_runs").cast("int").alias("total_runs"),
               F.col("high_score").cast("int").alias("high_score")))

schema = DataFrameSchema({
    "player":     Column(str, nullable=False),
    "season":     Column(int, pa.Check.equal_to(2026)),
    "total_runs": Column(int, pa.Check.greater_than_or_equal_to(0)),
    "high_score": Column(int, pa.Check.greater_than_or_equal_to(0)),
})

print("VALID DATA errors:", dict(schema.validate(bat).pandera.errors) or "NONE (passed)")

# inject a contract-violating row (negative runs) -> pandera flags the DATA check
bad = bat.union(spark.createDataFrame([("Cheater", 2026, -5, -1)], bat.columns))
errs = schema.validate(bad).pandera.errors
print("BAD DATA errors present:", bool(errs))
print("BAD DATA failed checks:",
      [e["check"] for lst in errs.get("DATA", {}).values() for e in lst])
spark.stop()
