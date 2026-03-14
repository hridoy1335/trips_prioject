# importing libraries from databricks
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

# create a streaming table to auto loder incoming data
@dp.table(name="bronze.trips")
# check data quality
@dp.expect_all_or_drop({
    "trip_id_is_not_null": "trip_id is not null",
    "driver_id_is_not_null": "driver_id is not null",
    "customer_id_is_not_null": "customer_id is not null",
    "vehicle_id_is_not_null": "vehicle_id is not null",
    "trip_start_is_not_null": "trip_start_time is not null",
    "trip_end_is_not_null": "trip_end_time is not null",
    "distance_is_not_null": "distance_km is not null and distance_km >= 0",
    "fare_amount_is_not_null": "fare_amount is not null and fare_amount >= 0",
    "payment_method_is_not_null": "payment_method is not null"
})
# define the function to read and save the data
def trips():
    df = (
        spark.readStream.table("trips_catalog.landing.trips")
    ).select(
        col("trip_id"),
        col("driver_id"),
        col("customer_id"),
        col("vehicle_id"),
        col("trip_start_time"),
        col("trip_end_time"),
        col("start_location"),
        col("end_location"),
        col("distance_km"),
        col("fare_amount"),
        col("payment_method"),
        col("trip_status")
    )
    df = df.withColumn("created_at", current_timestamp())
    df = df.dropDuplicates(["trip_id"])
    return df