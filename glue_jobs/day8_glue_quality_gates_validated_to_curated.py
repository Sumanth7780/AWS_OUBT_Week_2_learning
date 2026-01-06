import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "VALIDATED_DELTA_PATH",
    "CURATED_DELTA_PATH",
    "REJECTS_PATH",
    "MIN_PASS_RATE"
])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

VALIDATED = args["VALIDATED_DELTA_PATH"]
CURATED = args["CURATED_DELTA_PATH"]
REJECTS = args["REJECTS_PATH"]
MIN_PASS_RATE = float(args["MIN_PASS_RATE"])

df = spark.read.format("delta").load(VALIDATED)

# Rules
dq = df.withColumn("dq_pickup_not_null", F.col("pickup_ts").isNotNull()) \
       .withColumn("dq_dropoff_ge_pickup", F.col("dropoff_ts") >= F.col("pickup_ts"))

if "fare_amount" in df.columns:
    dq = dq.withColumn("dq_fare_positive", F.col("fare_amount").cast(DoubleType()) > F.lit(0.0))
    dq = dq.withColumn("dq_pass", F.col("dq_pickup_not_null") & F.col("dq_dropoff_ge_pickup") & F.col("dq_fare_positive"))
else:
    dq = dq.withColumn("dq_pass", F.col("dq_pickup_not_null") & F.col("dq_dropoff_ge_pickup"))

total = dq.count()
passed = dq.filter("dq_pass = true").count()
pass_rate = (passed / total) if total else 0.0

print(f"Total={total}, Passed={passed}, PassRate={pass_rate}")

# Rejects
dq.filter("dq_pass = false") \
  .withColumn("rejected_at", F.current_timestamp()) \
  .write.mode("overwrite").parquet(REJECTS)

if pass_rate < MIN_PASS_RATE:
    raise RuntimeError(f"DQ gate failed: {pass_rate} < {MIN_PASS_RATE}. Rejects at {REJECTS}")

# Write curated (passed only)
passed_df = dq.filter("dq_pass = true") \
    .withColumn("governance_zone", F.lit("curated")) \
    .withColumn("curated_at", F.current_timestamp())

(passed_df.write.format("delta")
 .mode("overwrite")
 .partitionBy("pickup_date")
 .save(CURATED))

job.commit()
print(f"✅ Day 8 done: curated={CURATED}, rejects={REJECTS}")
