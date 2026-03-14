# importing libraries from databricks
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

# create a streaming table to auto loder incoming data
@dp.table(name="bronze.payments")
# check data quality
@dp.expect_all_or_drop({
    "payment_id_is_not_null": "payment_id is not null",
    "trip_id_is_not_null": "trip_id is not null",
    "customer_id_is_not_null": "customer_id is not null",
    "payment_method_is_not_null": "payment_method is not null",
    "amount_is_not_null": "amount is not null and amount >= 0",
    "transaction_time_is_not_null": "transaction_time is not null"
})
# define the function to read and save the data
def payments():
    df = (
        spark.readStream.table("trips_catalog.landing.payments")
    ).select(
        col("payment_id"),
        col("trip_id"),
        col("customer_id"),
        col("payment_method"),
        col("payment_status"),
        col("amount"),
        col("transaction_time")
    )
    df = df.withColumn("created_at", current_timestamp())
    df = df.dropDuplicates(["payment_id"])
    return df
