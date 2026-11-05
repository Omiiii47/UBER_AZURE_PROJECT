create or refresh streaming table silver_obt
as
SELECT
    uber_rides.ride_id,uber_rides.confirmation_number,uber_rides.passenger_id,uber_rides.driver_id,uber_rides.vehicle_id,uber_rides.pickup_location_id,uber_rides.dropoff_location_id,uber_rides.vehicle_type_id,uber_rides.vehicle_make_id,uber_rides.payment_method_id,uber_rides.ride_status_id,uber_rides.pickup_city_id,uber_rides.dropoff_city_id,uber_rides.cancellation_reason_id,uber_rides.passenger_name,uber_rides.passenger_email,uber_rides.passenger_phone,uber_rides.driver_name,uber_rides.driver_rating,uber_rides.driver_phone,uber_rides.driver_license,uber_rides.vehicle_model,uber_rides.vehicle_color,uber_rides.license_plate,uber_rides.pickup_address,uber_rides.pickup_latitude,uber_rides.pickup_longitude,uber_rides.dropoff_address,uber_rides.dropoff_latitude,uber_rides.dropoff_longitude,uber_rides.distance_miles,uber_rides.duration_minutes,uber_rides.booking_timestamp,uber_rides.pickup_timestamp,uber_rides.dropoff_timestamp,uber_rides.base_fare,uber_rides.distance_fare,uber_rides.time_fare,uber_rides.surge_multiplier,uber_rides.subtotal,uber_rides.tip_amount,uber_rides.total_fare,uber_rides.rating,
    map_vehicle_makes.vehicle_make,
    map_vehicle_types.vehicle_type,map_vehicle_types.description,map_vehicle_types.base_rate,map_vehicle_types.per_mile,map_vehicle_types.per_minute,
    pickup_city.city AS pickup_city, pickup_city.state AS pickup_state, pickup_city.region AS pickup_region, CAST(pickup_city.updated_at AS TIMESTAMP) AS pickup_city_updated_at,
    dropoff_city.city AS dropoff_city, dropoff_city.state AS dropoff_state, dropoff_city.region AS dropoff_region, CAST(dropoff_city.updated_at AS TIMESTAMP) AS dropoff_city_updated_at,
    map_payment_methods.payment_method, map_payment_methods.is_card, map_payment_methods.requires_auth,
    map_cancellation_reasons.cancellation_reason

FROM
    stream (uber.bronze.uber_rides)
    watermark booking_timestamp delay of interval 3 minutes uber_rides

    left join uber.bronze.map_vehicle_makes map_vehicle_makes on uber_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id

    left join uber.bronze.map_vehicle_types map_vehicle_types on uber_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id

    left join uber.bronze.map_cities pickup_city on uber_rides.pickup_city_id = pickup_city.city_id

    left join uber.bronze.map_cities dropoff_city on uber_rides.dropoff_city_id = dropoff_city.city_id

    left join uber.bronze.map_payment_methods map_payment_methods on uber_rides.payment_method_id = map_payment_methods.payment_method_id

    left join uber.bronze.map_cancellation_reasons map_cancellation_reasons on uber_rides.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id