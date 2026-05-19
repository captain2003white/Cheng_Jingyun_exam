#!/bin/bash

# ==============================================================================
# IoT Sensor Platform - REST API Automated Testing Suite
# This script executes validation requests against all active v1 microservice routes.
# ==============================================================================

BASE_URL="http://localhost:5000/api/v1"

echo "======================================================================"
echo "LAUNCHING ENDPOINT VALIDATION FOR EXPOSED REST API"
echo "======================================================================"

# 1. Verify Platform Operational Health and Dependency Status
echo -e "\n[TEST 1] Querying System Health & Core Infrastructure Connectivity..."
curl -X GET -s "${BASE_URL}/health"
echo -e "\n----------------------------------------------------------------------"

# 2. Extract Broker Partition Configurations and Active Meta Attributes
echo -e "\n[TEST 2] Fetching Live Kafka Cluster Topography Metrics..."
curl -X GET -s "${BASE_URL}/kafka/stats"
echo -e "\n----------------------------------------------------------------------"

# 3. Pull Filtered Historical Granular Entries from Curated Zone
echo -e "\n[TEST 3] Retrieving Recent Temperature Logs (Limit=5) from Curated Storage..."
curl -X GET -s "${BASE_URL}/sensors/recent?sensor_type=temperature&limit=5"
echo -e "\n----------------------------------------------------------------------"

# 4. Fetch Rolling Aggregation Benchmarks from Consumption Zone
echo -e "\n[TEST 4] Pulling Stream-Processed 5-Minute Windowed Consumption Summaries..."
curl -X GET -s "${BASE_URL}/sensors/summary"
echo -e "\n======================================================================"
echo "API VALIDATION TESTING COMPLETE."