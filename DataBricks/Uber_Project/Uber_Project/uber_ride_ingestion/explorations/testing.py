# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Event Hubs configuration
EH_NAMESPACE   = "ubereventsom"
EH_NAME        = "ubertopic"

EH_CONN_STR = spark.conf.get("connection_string", "")
# Kafka Consumer configuration

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" : 10000,
  "kafka.session.timeout.ms" : 10000,
  "maxOffsetsPerTrigger"     : 10000,
  "failOnDataLoss"           : 'true',
  "startingOffsets"          : 'earliest'
}


df = spark.readStream.format('kafka')\
    .options(**KAFKA_OPTIONS)\
    .load()
   
    
query = (
    df.writeStream
      .format("memory")
      .queryName("rides_preview")
      .outputMode("append")
      .trigger(availableNow=True)
      .option("checkpointLocation", "/Volumes/uber/bronze/my_volume/_checkpoints/explore3/")
      .start()
)

query.awaitTermination()


# COMMAND ----------

display(spark.table("rides_preview"))

# COMMAND ----------

print("bulk_rides:", spark.table("uber.bronze.bulk_rides").count())
print("uber_rides:", spark.table("uber.bronze.uber_rides").count())
display(spark.table("uber.bronze.uber_rides").orderBy("ride_id").limit(20))

# COMMAND ----------

print("bulk_rides:", spark.table("uber.bronze.bulk_rides").count())

# COMMAND ----------

print("uber_rides:", spark.table("uber.bronze.uber_rides").count())


# COMMAND ----------

display(spark.table("uber.bronze.uber_rides").limit(20))

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table uber.bronze.uber_rides

# COMMAND ----------

from pyspark.sql.functions import col

ur = spark.table("uber.bronze.uber_rides")
bulk_ids = spark.table("uber.bronze.bulk_rides").select("ride_id")

# rows that did NOT come from the bulk load = the parsed Kafka rows
parsed_only = ur.join(bulk_ids, "ride_id", "left_anti")

print("parsed rows:", parsed_only.count())          # expect 9
display(parsed_only)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber.bronze.silver_obt

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber.bronze.map_cities

# COMMAND ----------

rides = spark.table("uber.bronze.uber_rides")
cities = spark.table("uber.bronze.map_cities")

# 1. Types of the two join keys
print(dict(rides.dtypes)["pickup_city_id"], dict(cities.dtypes)["city_id"])

# 2. Sample values from each side
rides.select("pickup_city_id").distinct().show(10)
cities.select("city_id", "city").show(10)

# 3. How many rides have a null or unmatched city id
print("null ids:", rides.filter("pickup_city_id is null").count())
print("unmatched:", rides.join(cities, rides.pickup_city_id == cities.city_id, "left_anti").count())

# COMMAND ----------

print(spark.table("uber.bronze.silver_obt").columns)

# COMMAND ----------

for t in ["map_payment_methods", "map_cancellation_reasons"]:
    print(t, spark.table(f"uber.bronze.{t}").columns)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS uber.bronze.dim_cities;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber.bronze.dim_cities

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pickup_city_id, pickup_city, pickup_city_updated_at, __START_AT, __END_AT
# MAGIC FROM uber.bronze.dim_cities
# MAGIC ORDER BY pickup_city_id, __START_AT;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT city_id, city, updated_at FROM uber.bronze.map_cities ORDER BY city_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pickup_city_id, pickup_city, __START_AT, __END_AT
# MAGIC FROM uber.bronze.dim_cities
# MAGIC ORDER BY pickup_city_id, __START_AT;

# COMMAND ----------

# MAGIC %sql
# MAGIC    SELECT city_id, city, updated_at FROM uber.bronze.map_cities ORDER BY city_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber.bronze.dim_cities

# COMMAND ----------

# MAGIC %sql
# MAGIC    SELECT * FROM uber.bronze.dim_cities WHERE __END_AT IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber.bronze.silver_obt

# COMMAND ----------

