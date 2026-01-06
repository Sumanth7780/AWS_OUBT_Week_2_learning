import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "RAW_ZONES_CSV_PATH",
    "VALIDATED_REF_OUT_PATH"
])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RAW_ZONES = args["RAW_ZONES_CSV_PATH"]
OUT = args["VALIDATED_REF_OUT_PATH"]

zones = (spark.read.option("header", True).csv(RAW_ZONES)
         .withColumnRenamed("LocationID", "location_id")
         .withColumnRenamed("Zone", "zone_name")
         .withColumnRenamed("Borough", "borough"))

# Duplicate checks
dup_by_id = zones.groupBy("location_id").count().filter("count > 1")
dup_by_zone_borough = zones.groupBy("zone_name", "borough").count().filter("count > 1")

# Write outputs
dup_by_id.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUT}/dup_taxi_zones_by_zone_id/")
dup_by_zone_borough.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUT}/dup_taxi_zones_by_zone_borough/")

summary = spark.createDataFrame(
    [("dup_by_location_id", dup_by_id.count()),
     ("dup_by_zone_borough", dup_by_zone_borough.count())],
    ["check_name", "duplicate_groups"]
)

summary.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUT}/matching_summary/")

job.commit()
print(f"✅ Day 9 done: outputs at {OUT}")
