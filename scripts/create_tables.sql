-- Database setup for NYC Taxi Data Processing
-- Run this script on your AlwaysData PostgreSQL instance

-- Drop existing tables if they exist
DROP TABLE IF EXISTS daily_trip_stats;
DROP TABLE IF EXISTS taxi_trips_processed;
DROP TABLE IF EXISTS taxi_trips_raw;

-- Raw taxi trips table
CREATE TABLE taxi_trips_raw (
    id SERIAL PRIMARY KEY,
    vendor_id INTEGER,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance DECIMAL(8,2),
    pickup_longitude DECIMAL(10,6),
    pickup_latitude DECIMAL(10,6),
    rate_code_id INTEGER,
    store_and_fwd_flag CHAR(1),
    dropoff_longitude DECIMAL(10,6),
    dropoff_latitude DECIMAL(10,6),
    payment_type INTEGER,
    fare_amount DECIMAL(8,2),
    extra DECIMAL(8,2),
    mta_tax DECIMAL(8,2),
    tip_amount DECIMAL(8,2),
    tolls_amount DECIMAL(8,2),
    total_amount DECIMAL(8,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Processed taxi trips table
CREATE TABLE taxi_trips_processed (
    id SERIAL PRIMARY KEY,
    trip_date DATE,
    hour_of_day INTEGER,
    day_of_week INTEGER,
    vendor_id INTEGER,
    passenger_count INTEGER,
    trip_distance DECIMAL(8,2),
    trip_duration_minutes INTEGER,
    fare_amount DECIMAL(8,2),
    tip_amount DECIMAL(8,2),
    total_amount DECIMAL(8,2),
    payment_type INTEGER,
    rate_code_id INTEGER,
    is_weekend BOOLEAN,
    distance_category VARCHAR(20),
    fare_per_mile DECIMAL(8,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily aggregations table
CREATE TABLE daily_trip_stats (
    id SERIAL PRIMARY KEY,
    trip_date DATE UNIQUE,
    total_trips INTEGER,
    total_revenue DECIMAL(12,2),
    avg_trip_distance DECIMAL(8,2),
    avg_fare_amount DECIMAL(8,2),
    avg_tip_amount DECIMAL(8,2),
    avg_trip_duration DECIMAL(8,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX idx_taxi_trips_raw_pickup_datetime ON taxi_trips_raw(pickup_datetime);
CREATE INDEX idx_taxi_trips_raw_vendor_id ON taxi_trips_raw(vendor_id);

CREATE INDEX idx_taxi_trips_processed_trip_date ON taxi_trips_processed(trip_date);
CREATE INDEX idx_taxi_trips_processed_vendor_id ON taxi_trips_processed(vendor_id);
CREATE INDEX idx_taxi_trips_processed_hour_of_day ON taxi_trips_processed(hour_of_day);

CREATE INDEX idx_daily_trip_stats_trip_date ON daily_trip_stats(trip_date);

-- Create views for common queries
CREATE OR REPLACE VIEW monthly_summary AS
SELECT 
    DATE_TRUNC('month', trip_date) as month,
    SUM(total_trips) as monthly_trips,
    SUM(total_revenue) as monthly_revenue,
    AVG(avg_trip_distance) as avg_distance,
    AVG(avg_fare_amount) as avg_fare
FROM daily_trip_stats
GROUP BY DATE_TRUNC('month', trip_date)
ORDER BY month;

CREATE OR REPLACE VIEW weekend_vs_weekday AS
SELECT 
    is_weekend,
    COUNT(*) as trip_count,
    AVG(fare_amount) as avg_fare,
    AVG(tip_amount) as avg_tip,
    AVG(trip_distance) as avg_distance
FROM taxi_trips_processed
GROUP BY is_weekend;
