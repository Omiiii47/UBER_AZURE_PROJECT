# Databricks notebook source
import pandas as pd

files = [
{"file":"map_cities"},
{"file":"map_cancellation_reasons"},
{"file":"map_payment_methods"},
{"file":"map_ride_statuses"},
{"file":"map_vehicle_makes"},
{"file":"map_vehicle_types"}
]

 

for file in files:

    url = f'https://omuberdatastorage.blob.core.windows.net/raw/ingestion/{file['file']}.json?sp=r&st=2026-11-03T16:38:54Z&se=2026-11-06T12:53:54Z&spr=https&sv=2026-02-06&sr=c&sig=0Bb%2FzZ%2FgjleVsJyr6AVYYhid3TagefogTA%2FS7weHU%2F0%3D'

    df = pd.read_json(url)
    df_spark = spark.createDataFrame(df)

    # Writing data into the bronze layer

    df_spark.write.format('delta')\
        .mode("overwrite")\
        .option('overwriteSchema','true')\
        .saveAsTable(f'uber.bronze.{file['file']}')

# COMMAND ----------

url = 'https://omuberdatastorage.blob.core.windows.net/raw/ingestion/bulk_rides.json?sp=r&st=2026-11-03T16:38:54Z&se=2026-11-06T12:53:54Z&spr=https&sv=2026-02-06&sr=c&sig=0Bb%2FzZ%2FgjleVsJyr6AVYYhid3TagefogTA%2FS7weHU%2F0%3D'
df = pd.read_json(url)
df_spark = spark.createDataFrame(df)
if not spark.catalog.tableExists('uber.bronze.bulk_rides'):
  df_spark.write.format('delta')\
    .mode("overwrite")\
    .saveAsTable('uber.bronze.bulk_rides')
    

# COMMAND ----------

import pandas as pd 
df = pd.read_json(f'https://omuberdatastorage.blob.core.windows.net/raw/ingestion/map_cities.json?sp=r&st=2026-11-03T16:38:54Z&se=2026-11-06T12:53:54Z&spr=https&sv=2026-02-06&sr=c&sig=0Bb%2FzZ%2FgjleVsJyr6AVYYhid3TagefogTA%2FS7weHU%2F0%3D'
)

df_spark = spark.createDataFrame(df)

display(df_spark)

# COMMAND ----------

# MAGIC     %sql
# MAGIC     SELECT * FROM uber.bronze.map_cities

# COMMAND ----------

df = pd.read_json(url)
df

# COMMAND ----------

