from pyspark import pipelines as dp


@dp.view
def dim_passenger_view():
    df = spark.readStream.table('silver_obt')
    df = df.select('passenger_id', 'passenger_name', 'passenger_email', 'passenger_phone')
    df = df.dropDuplicates(subset = ['passenger_id'])
    return df


dp.create_streaming_table('dim_passenger')
dp.create_auto_cdc_flow(
  target = "dim_passenger",
  source = "dim_passenger_view",
  keys = ["passenger_id"],
sequence_by='passenger_id',
  stored_as_scd_type = "1", # optional

)

@dp.view
def dim_driver_view():
    df = spark.readStream.table('silver_obt')
    df = df.select('driver_id', 'driver_name', 'driver_rating', 'driver_phone', 'driver_license')
    df = df.dropDuplicates(subset = ['driver_id'])
    return df

dp.create_streaming_table('dim_driver')
dp.create_auto_cdc_flow(
  target = "dim_driver",
  source = "dim_driver_view",
  keys = ["driver_id"],
sequence_by='driver_id',
  stored_as_scd_type = "1", # optional

)

@dp.view
def dim_vehicle_view():
    df = spark.readStream.table('silver_obt')
    df = df.select('vehicle_id', 'vehicle_model', 'vehicle_color', 'vehicle_make_id', 'vehicle_type_id','license_plate','vehicle_make','vehicle_type')
    df = df.dropDuplicates(subset = ['vehicle_id'])
    return df

dp.create_streaming_table('dim_vehicle')
dp.create_auto_cdc_flow(
  target = "dim_vehicle",
  source = "dim_vehicle_view",
  keys = ["vehicle_id"],
sequence_by='vehicle_id',
  stored_as_scd_type = "1", # optional

)

@dp.view
def dim_payment_view():
    df = spark.readStream.table('silver_obt')
    df = df.select('payment_method_id','payment_method',"is_card","requires_auth")
    df = df.dropDuplicates(subset = ['payment_method_id'])
    return df

dp.create_streaming_table('dim_payment')
dp.create_auto_cdc_flow(
  target = "dim_payment",
  source = "dim_payment_view",
  keys = ["payment_method_id"],
sequence_by='payment_method_id',
  stored_as_scd_type = "1", # optional

)


@dp.view
def dim_booking_view():
    df = spark.readStream.table("silver_obt")
    df = df.select(
        "ride_id", "confirmation_number", "dropoff_location_id", "ride_status_id",
        "dropoff_city_id", "cancellation_reason_id", "dropoff_address",
        "dropoff_latitude", "dropoff_longitude", "booking_timestamp", "dropoff_timestamp",'pickup_latitude','pickup_longitude',"pickup_location_id"
    )
    df = df.dropDuplicates(subset=["ride_id"])
    return df


dp.create_streaming_table('dim_booking')
dp.create_auto_cdc_flow(
  target = "dim_booking",
  source = "dim_booking_view",
  keys = ["ride_id"],
sequence_by='ride_id',
  stored_as_scd_type = "1", # optional

)

@dp.view
def dim_cities_view():
    return (spark.readStream.table("silver_obt")
            .select("pickup_city_id", "pickup_city", "pickup_state",
                    "pickup_region", "pickup_city_updated_at"))

dp.create_streaming_table("dim_cities")

dp.create_auto_cdc_flow(
    target="dim_cities",
    source="dim_cities_view",
    keys=["pickup_city_id"],
    sequence_by="pickup_city_updated_at",
    stored_as_scd_type=2
)
















# FACT TABLE

@dp.view
def fact_view():

  df = spark.readStream.table("uber.bronze.silver_obt")
  df = df.select("ride_id", "passenger_id", "driver_id", "pickup_timestamp", "dropoff_timestamp", "pickup_location_id", "dropoff_location_id", "pickup_city_id", "dropoff_city_id", "payment_method_id", "vehicle_id", "vehicle_type_id","distance_miles", "duration_minutes", "base_fare", "distance_fare", "time_fare", "surge_multiplier", "total_fare", "tip_amount", "rating", "base_rate", "per_mile", "per_minute")
 
  return df

dp.create_streaming_table('fact')
dp.create_auto_cdc_flow(
  target = "fact",
  source = "fact_view",
  keys = ["ride_id","pickup_city_id","payment_method_id","passenger_id","vehicle_id"],
sequence_by='ride_id',
  stored_as_scd_type = "1", # optional

)



# @dp.table
# def dim_cities_view():
#     df = spark.readStream.table("silver_obt")
#     df = df.select(
#         'pickup_city_id','pickup_city','pickup_state','pickup_region','pickup_address',"pickup_city_updated_at"
#     )
#     df = df.dropDuplicates(subset=["pickup_city_id", "pickup_city_updated_at"])
#     return df


  





# dp.create_streaming_table('dim_cities')
# dp.create_auto_cdc_flow(
#   target = "dim_cities",
#   source = "dim_cities_view",
#   keys = ["pickup_city_id"],
# sequence_by='pickup_city_updated_at',
#   stored_as_scd_type = "2", # optional

# )


# from pyspark import pipelines as dp

# # ---------- passenger ----------
# @dp.view
# def dim_passenger_view():
#     return (spark.readStream.table("silver_obt")
#             .select("passenger_id", "passenger_name", "passenger_email",
#                     "passenger_phone", "booking_timestamp"))

# dp.create_streaming_table("dim_passenger")
# dp.create_auto_cdc_flow(
#     target="dim_passenger", source="dim_passenger_view",
#     keys=["passenger_id"], sequence_by="booking_timestamp", stored_as_scd_type=1
# )

# # ---------- driver ----------
# @dp.view
# def dim_driver_view():
#     return (spark.readStream.table("silver_obt")
#             .select("driver_id", "driver_name", "driver_rating", "driver_phone",
#                     "driver_license", "booking_timestamp"))

# dp.create_streaming_table("dim_driver")
# dp.create_auto_cdc_flow(
#     target="dim_driver", source="dim_driver_view",
#     keys=["driver_id"], sequence_by="booking_timestamp", stored_as_scd_type=1
# )

# # ---------- vehicle ----------
# @dp.view
# def dim_vehicle_view():
#     return (spark.readStream.table("silver_obt")
#             .select("vehicle_id", "vehicle_model", "vehicle_color", "vehicle_make_id",
#                     "vehicle_type_id", "license_plate", "vehicle_make", "vehicle_type",
#                     "booking_timestamp"))

# dp.create_streaming_table("dim_vehicle")
# dp.create_auto_cdc_flow(
#     target="dim_vehicle", source="dim_vehicle_view",
#     keys=["vehicle_id"], sequence_by="booking_timestamp", stored_as_scd_type=1
# )

# # ---------- payment ----------
# @dp.view
# def dim_payment_view():
#     return (spark.readStream.table("silver_obt")
#             .select("payment_method_id", "payment_method", "is_card",
#                     "requires_auth", "booking_timestamp"))

# dp.create_streaming_table("dim_payment")
# dp.create_auto_cdc_flow(
#     target="dim_payment", source="dim_payment_view",
#     keys=["payment_method_id"], sequence_by="booking_timestamp", stored_as_scd_type=1
# )

# # ---------- booking ----------
# @dp.view
# def dim_booking_view():
#     return (spark.readStream.table("silver_obt")
#             .select("ride_id", "confirmation_number", "pickup_location_id",
#                     "pickup_latitude", "pickup_longitude", "dropoff_location_id",
#                     "dropoff_address", "dropoff_latitude", "dropoff_longitude",
#                     "ride_status_id", "dropoff_city_id", "cancellation_reason_id",
#                     "booking_timestamp", "dropoff_timestamp"))

# dp.create_streaming_table("dim_booking")
# dp.create_auto_cdc_flow(
#     target="dim_booking", source="dim_booking_view",
#     keys=["ride_id"], sequence_by="booking_timestamp", stored_as_scd_type=1
# )

# # ---------- cities (straight from the lookup table) ----------
# @dp.view
# def dim_cities_view():
#     return (spark.readStream
#             .option("skipChangeCommits", "true")
#             .table("map_cities"))

# dp.create_streaming_table("dim_cities")
# dp.create_auto_cdc_flow(
#     target="dim_cities",
#     source="dim_cities_view",
#     keys=["city_id"],
#     sequence_by="updated_at",
#     stored_as_scd_type=2
# )





























