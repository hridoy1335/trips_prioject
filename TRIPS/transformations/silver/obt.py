# importing dependencies
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp
# define constant for data quality check
@dp.expect_all_or_drop({
    # trip data constant data quality check
    "trip_id_not_null": "trip_id IS NOT NULL",
    "trip_start_time_valid": "trip_start_time IS NOT NULL",
    "trip_end_time_valid": "trip_end_time IS NOT NULL",
    "trip_start_time_before_end_time": "trip_start_time < trip_end_time",
    "start_location_not_null": "start_location IS NOT NULL",
    "end_location_not_null": "end_location IS NOT NULL",
    "trip_location_valid": "start_location != end_location",
    "distance_km_not_null": "distance_km IS NOT NULL AND distance_km >= 0",
    "fare_amount_not_null": "fare_amount IS NOT NULL AND fare_amount > 0",
    # customer data constant data quality check
    "customer_id_not_null": "customer_id IS NOT NULL",
    "customer_first_name_not_null": "customer_first_name IS NOT NULL",
    "email_valid": "customer_email LIKE '%@%'",
    "phone_number_valid": "customer_phone_number IS NOT NULL",
    "signup_date_valid": "customer_signup_date IS NOT NULL",
    # driver data constant data quality check
    "driver_id_not_null": "driver_id IS NOT NULL",
    "driver_first_name_not_null": "driver_first_name IS NOT NULL",
    "driver_phone_number_valid": "driver_phone_number IS NOT NULL",
    "driver_vehicle_id_valid": "driver_vehicle_id IS NOT NULL",
    "driver_city_not_null": "driver_city IS NOT NULL",
    # payment data constant data quality check
    "payment_id_not_null": "payment_id IS NOT NULL",
    "payment_payment_method_not_null": "payment_method_payment IS NOT NULL",
    "payment_payment_status_not_null": "payment_status IS NOT NULL",
    "payment_amount_not_null": "payment_amount IS NOT NULL AND payment_amount > 0",
    "payment_transaction_time_valid": "payment_transaction_time IS NOT NULL",
    # vehicle data constant data quality check
    "vehicle_id_not_null": "vehicle_id IS NOT NULL",
    "vehicle_license_plate_not_null": "vehicle_license_plate IS NOT NULL",
    "vehicle_model_not_null": "vehicle_model IS NOT NULL",
    "vehicle_make_not_null": "vehicle_make IS NOT NULL",
    "vehicle_year_not_null": "vehicle_year IS NOT NULL"
})
# making streaming table
@dp.view(name="multiple_table")
def multiple_table():
    trip = (
            spark.readStream.table("bronze.trips")
                 .withColumn("start_location", trim(col("start_location")))
                 .withColumn("end_location", trim(col("end_location")))
                 .withColumn("payment_method", trim(col("payment_method")))
                 .withColumn("trip_status", trim(col("trip_status")))
    )
    customer = (
            spark.read.table("bronze.customers")
                 .withColumn("first_name", trim(col("first_name")))
                 .withColumn("last_name", trim(col("last_name")))
                 .withColumn("email", lower(trim(regexp_replace(col("email"), "[^a-zA-Z0-9@._-]", ""))))
                 .withColumn("phone_number", regexp_replace(trim(col("phone_number")), "[^0-9]", ""))
                 .withColumn("city", trim(col("city")))
    )
    driver = (
            spark.read.table("bronze.drivers")
                 .withColumn("first_name", trim(col("first_name")))
                 .withColumn("last_name", trim(col("last_name")))
                 .withColumn("phone_number", regexp_replace(trim("phone_number"), "[^0-9]", ""))
                 .withColumn("city", trim(col("city")))
    )
    payment = (
            spark.read.table("bronze.payments")
                 .withColumn("payment_method", trim(col("payment_method")))
                 .withColumn("payment_status", trim(col("payment_status")))
    )
    vehicle = (
            spark.read.table("bronze.vehicles")
                 .withColumn("license_plate", trim(col("license_plate")))
                 .withColumn("model", trim(col("model")))
                 .withColumn("make", trim(col("make")))
                 .withColumn("vehicle_type", trim(col("vehicle_type")))
    )
    obt = (
        trip.alias("t")
            .join(customer.alias("c"), col("t.customer_id") == col("c.customer_id"), "left")
            .join(driver.alias("d"), col("t.driver_id") == col("d.driver_id"), "left")
            .join(payment.alias("p"), col("t.trip_id") == col("p.trip_id"), "left")
            .join(vehicle.alias("v"), col("t.vehicle_id") == col("v.vehicle_id"), "left")
            .select(
                col("t.*"),
                # trips table make complete
                col("c.first_name").alias("customer_first_name"),
                col("c.last_name").alias("customer_last_name"),
                col("c.email").alias("customer_email"),
                col("c.phone_number").alias("customer_phone_number"),
                col("c.city").alias("customer_city"),
                col("c.signup_date").alias("customer_signup_date"),
                col("c.created_at").alias("customer_created_at"),
                # customer table make complete
                col("d.first_name").alias("driver_first_name"),
                col("d.last_name").alias("driver_last_name"),
                col("d.phone_number").alias("driver_phone_number"),
                col("d.vehicle_id").alias("driver_vehicle_id"),
                col("d.driver_rating").alias("driver_rating"),
                col("d.city").alias("driver_city"),
                col("d.created_at").alias("driver_created_at"),
                # driver table make complete
                col("p.payment_id").alias("payment_id"),
                col("p.payment_method").alias("payment_method_payment"),
                col("p.payment_status").alias("payment_status"),
                col("p.amount").alias("payment_amount"),
                col("p.transaction_time").alias("payment_transaction_time"),
                col("p.created_at").alias("payment_created_at"),
                # payment table make complete
                col("v.license_plate").alias("vehicle_license_plate"),
                col("v.model").alias("vehicle_model"),
                col("v.make").alias("vehicle_make"),
                col("v.year").alias("vehicle_year"),
                col("v.vehicle_type").alias("vehicle_type"),
                col("v.created_at").alias("vehicle_created_at"),
                # vehicle table make complete
                current_timestamp().alias("obt_ingest")
            )
    )
    return obt
# making one big table the with empty streaming table
dp.create_streaming_table(
    name="silver.one_big_table",
    comment="One Big Table",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
# making one big table the scd type-1 to upsert data
dp.create_auto_cdc_flow(
    target="silver.one_big_table",
    source="multiple_table",
    keys=["trip_id"],
    sequence_by="obt_ingest", 
    stored_as_scd_type=1
)