# importing libraries from databricks
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

# create a streaming table to auto loder incoming data
@dp.table(name="drivers")
# check data quality
@dp.expect_all_or_drop({
    "driver_id_is_not_null": "driver_id is not null",
    "first_name_is_not_null" : "first_name is not null",
    "phone_number_is_not_null" : "phone_number is not null",
    "vehicle_id_is_not_null" : "vehicle_id is not null"
})
# define the function to read and save the data
def customers():
    df = (
        spark.readStream.table("trips_catalog.landing.drivers")
    ).select(
        col("driver_id"),
        col("first_name"),
        col("last_name"),
        col("phone_number"),
        col("vehicle_id"),
        col("driver_rating"),
        col("city")
    )
    df = df.withColumn("created_at", current_timestamp())
    df = df.dropDuplicates(["driver_id"])
    return df
