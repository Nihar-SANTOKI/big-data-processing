-- Database setup for NYC Taxi Data Processing
-- Run this script on your AlwaysData PostgreSQL instance

-- Drop existing tables if they exist
DROP TABLE IF EXISTS daily_trip_stats;
DROP TABLE IF EXISTS taxi_trips_processed;
DROP TABLE IF EXISTS taxi_trips_raw;

-- Raw taxi trips table (removed datetime columns)
CREATE TABLE taxi_trips_raw (
    id SERIAL PRIMARY KEY,
    vendor_id INTEGER,
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
    total_amount DECIMAL(8,2)
);

-- Processed taxi trips table (removed datetime columns)
CREATE TABLE taxi_trips_processed (
    id SERIAL PRIMARY KEY,
    vendor_id INTEGER,
    passenger_count INTEGER,
    trip_distance DECIMAL(8,2),
    fare_amount DECIMAL(8,2),
    tip_amount DECIMAL(8,2),
    total_amount DECIMAL(8,2),
    payment_type INTEGER,
    rate_code_id INTEGER,
    distance_category VARCHAR(20),
    fare_per_mile DECIMAL(8,2)
);

-- Daily aggregations table (simplified)
CREATE TABLE daily_trip_stats (
    id SERIAL PRIMARY KEY,
    total_trips INTEGER,
    total_revenue DECIMAL(12,2),
    avg_trip_distance DECIMAL(8,2),
    avg_fare_amount DECIMAL(8,2),
    avg_tip_amount DECIMAL(8,2)
);

-- Create indexes for better performance
CREATE INDEX idx_taxi_trips_raw_vendor_id ON taxi_trips_raw(vendor_id);
CREATE INDEX idx_taxi_trips_processed_vendor_id ON taxi_trips_processed(vendor_id);

-- Create views for common queries (simplified)
CREATE OR REPLACE VIEW distance_summary AS
SELECT 
    distance_category,
    COUNT(*) as trip_count,
    AVG(fare_amount) as avg_fare,
    AVG(tip_amount) as avg_tip,
    AVG(trip_distance) as avg_distance
FROM taxi_trips_processed
GROUP BY distance_category;

CREATE OR REPLACE VIEW vendor_summary AS
SELECT 
    vendor_id,
    COUNT(*) as trip_count,
    AVG(fare_amount) as avg_fare,
    AVG(tip_amount) as avg_tip,
    AVG(trip_distance) as avg_distance
FROM taxi_trips_processed
GROUP BY vendor_id;