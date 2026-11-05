# Databricks notebook source
# MAGIC %md
# MAGIC ## Stream Rides Transformation

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.table('uber.bronze.rides_raw')
display(df)

# COMMAND ----------

df = spark.sql('select * from uber.bronze.bulk_rides')
display(df.schema)

# COMMAND ----------

rides_schema = StructType([StructField('ride_id', StringType(), True), StructField('confirmation_number', StringType(), True), StructField('passenger_id', StringType(), True), StructField('driver_id', StringType(), True), StructField('vehicle_id', StringType(), True), StructField('pickup_location_id', StringType(), True), StructField('dropoff_location_id', StringType(), True), StructField('vehicle_type_id', LongType(), True), StructField('vehicle_make_id', LongType(), True), StructField('payment_method_id', LongType(), True), StructField('ride_status_id', LongType(), True), StructField('pickup_city_id', LongType(), True), StructField('dropoff_city_id', LongType(), True), StructField('cancellation_reason_id', LongType(), True), StructField('passenger_name', StringType(), True), StructField('passenger_email', StringType(), True), StructField('passenger_phone', StringType(), True), StructField('driver_name', StringType(), True), StructField('driver_rating', DoubleType(), True), StructField('driver_phone', StringType(), True), StructField('driver_license', StringType(), True), StructField('vehicle_model', StringType(), True), StructField('vehicle_color', StringType(), True), StructField('license_plate', StringType(), True), StructField('pickup_address', StringType(), True), StructField('pickup_latitude', DoubleType(), True), StructField('pickup_longitude', DoubleType(), True), StructField('dropoff_address', StringType(), True), StructField('dropoff_latitude', DoubleType(), True), StructField('dropoff_longitude', DoubleType(), True), StructField('distance_miles', DoubleType(), True), StructField('duration_minutes', LongType(), True), StructField('booking_timestamp', TimestampType(), True), StructField('pickup_timestamp', TimestampType(), True), StructField('dropoff_timestamp', StringType(), True), StructField('base_fare', DoubleType(), True), StructField('distance_fare', DoubleType(), True), StructField('time_fare', DoubleType(), True), StructField('surge_multiplier', DoubleType(), True), StructField('subtotal', DoubleType(), True), StructField('tip_amount', DoubleType(), True), StructField('total_fare', DoubleType(), True), StructField('rating', DoubleType(), True)])

# COMMAND ----------

df_parsed = df.withColumn("parsed_rides", from_json(col("rides"), rides_schema))

# COMMAND ----------

from pyspark.sql.functions import from_json, col

# schema comes from bulk_rides
rides_schema = spark.table("uber.bronze.bulk_rides").schema

# the raw Kafka table is where the JSON string lives
df = spark.table("uber.bronze.rides_raw")

df_parsed = df.withColumn("parsed_rides", from_json(col("rides"), rides_schema))
display(df_parsed.select("parsed_rides.*"))


# COMMAND ----------

# MAGIC %md
# MAGIC # Jinja template for OBT

# COMMAND ----------

pip install Jinja2

# COMMAND ----------

rides_cols = spark.table("uber.bronze.uber_rides").columns
rides_select = ",".join(f"uber_rides.{c}" for c in rides_cols)

jinja_config = [
    {
        'table': 'uber.bronze.uber_rides uber_rides',
        'select': rides_select,          # <-- changed from 'uber_rides.*'
        'where': ""
    },

    {
        'table': 'uber.bronze.map_vehicle_makes map_vehicle_makes',
        'select': 'map_vehicle_makes.vehicle_make',
        'where': "",
        'on': 'uber_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id'
    },

    {
        'table': 'uber.bronze.map_vehicle_types map_vehicle_types',
        'select': 'map_vehicle_types.vehicle_type,map_vehicle_types.description,map_vehicle_types.base_rate,map_vehicle_types.per_mile,map_vehicle_types.per_minute',
        'where': "",
        'on': 'uber_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id'
    },
    {
    'table': 'uber.bronze.map_cities pickup_city',
    'select': 'pickup_city.city AS pickup_city, pickup_city.state AS pickup_state, pickup_city.region AS pickup_region, pickup_city.updated_at AS pickup_city_updated_at',
    'where': "",
    'on': 'uber_rides.pickup_city_id = pickup_city.city_id'
},
{
    'table': 'uber.bronze.map_cities dropoff_city',
    'select': 'dropoff_city.city AS dropoff_city, dropoff_city.state AS dropoff_state, dropoff_city.region AS dropoff_region, dropoff_city.updated_at AS dropoff_city_updated_at',
    'where': "",
    'on': 'uber_rides.dropoff_city_id = dropoff_city.city_id'
},
{
    'table': 'uber.bronze.map_payment_methods map_payment_methods',
    'select': 'map_payment_methods.payment_method, map_payment_methods.is_card, map_payment_methods.requires_auth',
    'where': "",
    'on': 'uber_rides.payment_method_id = map_payment_methods.payment_method_id'
},
{
    'table': 'uber.bronze.map_cancellation_reasons map_cancellation_reasons',
    'select': 'map_cancellation_reasons.cancellation_reason',
    'where': "",
    'on': 'uber_rides.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id'
}
]

# COMMAND ----------

from jinja2 import Template

# COMMAND ----------


jinja_str = """
        
Select 
    {% for config in jinja_config %}
        {{config.select}}
            {% if not loop.last %}
                ,
            {% endif %}
    {% endfor %}

From  
    {% for config in jinja_config %}
        {% if loop.first %}
            {{ config.table }}
        {% else %}
            left join {{config.table}} on {{config.on}}
        {%endif%}
    {% endfor %}


    {% for config in jinga_config %}

        {% if loop.first %}
            WHERE
        {% endif %}
        {{ config.where}}
            {% if not loop.last %}
                {% if config.where !=  "" %}
                AND
                {% endif %}
            {% endif %}
    {% endfor %}
"""

# COMMAND ----------

jinja_str = """
create or refresh streaming table silver_obt
as
SELECT
    {{ jinja_config | selectattr('select') | map(attribute='select') | join(',\n    ') }}

FROM
{% for config in jinja_config %}
{% if loop.first %}
    stream ({{ config.table.split(' ')[0] }})
    watermark booking_timestamp delay of interval 3 minutes {{ config.table.split(' ')[1] }}
{% else %}
    left join {{ config.table }} on {{ config.on }}
{% endif %}
{% endfor %}

{% set conditions = jinja_config | selectattr('where') | map(attribute='where') | list %}
{% if conditions %}
WHERE {{ conditions | join(' AND ') }}
{% endif %}
"""

# COMMAND ----------


template = Template(jinja_str)
rendered_template = template.render(jinja_config=jinja_config)
print(rendered_template)

# COMMAND ----------

display(spark.sql(rendered_template))

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC     uber_rides.*
# MAGIC from
# MAGIC     uber.bronze.uber_rides as uber_rides
# MAGIC left join
# MAGIC     uber.bronze.map_vehicle_types as map_vehicle_types
# MAGIC on
# MAGIC     uber_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id
# MAGIC left join
# MAGIC     uber.bronze.map_vehicle_makes as map_vehicle_makes
# MAGIC on
# MAGIC     uber_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_timestamp()

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC passenger_name,
# MAGIC passenger_email,
# MAGIC passenger_id,
# MAGIC passenger_phone
# MAGIC from 
# MAGIC uber.bronze.silver_obt

# COMMAND ----------

for c in jinja_config:
    print(c['table'], '->', repr(c.get('select')))