#!/bin/bash

# Setup HDFS directories for the project
echo "Setting up HDFS directories..."

# Wait for namenode to be ready
echo "Waiting for namenode to be ready..."
sleep 30

# Create base directories
docker exec namenode hdfs dfs -mkdir -p /user/data/taxi/raw
docker exec namenode hdfs dfs -mkdir -p /user/data/taxi/processed
docker exec namenode hdfs dfs -mkdir -p /user/data/taxi/analytics

# Set permissions
docker exec namenode hdfs dfs -chmod 777 /user/data/taxi/raw
docker exec namenode hdfs dfs -chmod 777 /user/data/taxi/processed  
docker exec namenode hdfs dfs -chmod 777 /user/data/taxi/analytics

echo "HDFS directories created successfully!"

# List directories to verify
echo "HDFS directory structure:"
docker exec namenode hdfs dfs -ls -R /user/data/