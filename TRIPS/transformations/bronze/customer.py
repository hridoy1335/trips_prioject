# importing libraries from databricks
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

# create a streaming table to auto loder incoming data
@dp.table(name="customers")
# check data quality
@dp.expect_all_or_drop({
    "customer_id_is_not_null": "customer_id is not null",
    "first_name_is_not_null" : "first_name is not null",
    "email_is_not_null" : "email is not null",
    "phone_number_is_not_null" : "phone_number is not null"
})
# define the function to read and save the data
def customers():
    df = (
        spark.readStream.table("trips_catalog.landing.customers")
    ).select(
        col("customer_id"),
        col("first_name"),
        col("last_name"),
        col("email"),
        col("phone_number"),
        col("city"),
        col("signup_date")
    )
    df = df.withColumn("created_at", current_timestamp())
    df = df.dropDuplicates(["customer_id"])
    return df
