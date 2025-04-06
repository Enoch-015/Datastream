-- Create tables for taxi trip data
CREATE TABLE IF NOT EXISTS taxi_trips (
    id SERIAL PRIMARY KEY,
    vendor_id INTEGER,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance DOUBLE PRECISION,
    rate_code_id INTEGER,
    store_and_fwd_flag VARCHAR(1),
    pu_location_id INTEGER,
    do_location_id INTEGER,
    payment_type INTEGER,
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    airport_fee DOUBLE PRECISION,
    trip_duration BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create table for hourly aggregates
CREATE TABLE IF NOT EXISTS hourly_aggregates (
    id SERIAL PRIMARY KEY,
    hour INTEGER,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    average_fare DOUBLE PRECISION,
    trip_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_taxi_trips_pickup_datetime ON taxi_trips(pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_hourly_aggregates_hour ON hourly_aggregates(hour);
CREATE INDEX IF NOT EXISTS idx_hourly_aggregates_window ON hourly_aggregates(window_start, window_end);v