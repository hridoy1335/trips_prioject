# importing libraries from databricks
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

# create a streaming table to auto loder incoming data
@dp.table(name="bronze.locations")
# check data quality
@dp.expect_all_or_drop({
    "location_id_is_not_null": "location_id is not null"
})
# define the function to read and save the data
def locations():
    df = (
        spark.readStream.table("trips_catalog.landing.locations")
    ).select(
        col("location_id"),
        col("city"),
        col("state"),
        col("country"),
        col("latitude"),
        col("longitude")
    )
    df = df.withColumn("created_at", current_timestamp())
    df = df.dropDuplicates(["location_id"])
    return df
