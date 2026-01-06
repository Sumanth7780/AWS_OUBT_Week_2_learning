import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "CURATED_DELTA_PATH",
    "MASTER_DELTA_PATH",
    "AUDIT_DELTA_PATH"
])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

CURATED = args["CURATED_DELTA_PATH"]
MASTER = args["MASTER_DELTA_PATH"]
AUDIT = args["AUDIT_DELTA_PATH"]

df = spark.read.format("delta").load(CURATED)

# "Golden" master example: de-duplicate by VendorID + pickup_date (adjust keys as needed)
keys = []
if "VendorID" in df.columns:
    keys.append("VendorID")
keys.append("pickup_date")

master = (df.dropDuplicates(keys)
          .withColumn("lifecycle_state", F.lit("ACTIVE"))
          .withColumn("effective_from", F.current_date())
          .withColumn("effective_to", F.lit(None).cast("date"))
          .withColumn("master_created_at", F.current_timestamp())
          .withColumn("governance_zone", F.lit("master")))

master.write.format("delta").mode("overwrite").save(MASTER)

# Audit trail snapshot
audit = master.select(
    *([F.col("VendorID")] if "VendorID" in master.columns else []),
    F.current_timestamp().alias("changed_at"),
    F.lit("system").alias("changed_by"),
    F.lit("MASTER_LOAD").alias("change_type"),
    F.lit("ACTIVE").alias("new_lifecycle_state")
)

audit.write.format("delta").mode("append").save(AUDIT)

job.commit()
print(f"✅ Day 10 done: master={MASTER}, audit={AUDIT}")
