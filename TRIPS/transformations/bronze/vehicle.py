# importing libraries from databricks
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

# create a streaming table to auto loder incoming data
@dp.table(name="bronze.vehicles")
# check data quality
@dp.expect_all_or_drop({
    "vehicle_id_is_not_null": "vehicle_id is not null"
})
# define the function to read and save the data
def vehicles():
    df = (
        spark.readStream.table("trips_catalog.landing.vehicles")
    ).select(
        col("vehicle_id"),
        col("license_plate"),
        col("model"),
        col("make"),
        col("year"),
        col("vehicle_type")
    )
    df = df.withColumn("created_at", current_timestamp())
    df = df.dropDuplicates(["vehicle_id"])
    return df
