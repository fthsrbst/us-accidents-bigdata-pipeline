#!/bin/bash
# ============================================
# HDFS Data Upload Script
# Uploads US Accidents CSV to HDFS
# ============================================

set -e

echo "============================================"
echo "HDFS DATA UPLOAD SCRIPT"
echo "============================================"

# Configuration
LOCAL_DATA_PATH="/data/US_Accidents_March23.csv"
HDFS_DATA_DIR="/data"
HDFS_HIVE_DIR="/user/hive/warehouse/accidents_db/raw"

# Wait for HDFS to be ready
echo "Waiting for HDFS to be ready..."
sleep 10

# Check if namenode is accessible
until hdfs dfs -ls / > /dev/null 2>&1; do
    echo "Waiting for HDFS NameNode..."
    sleep 5
done

echo "HDFS is ready!"

# Create directories
echo "Creating HDFS directories..."
hdfs dfs -mkdir -p $HDFS_DATA_DIR
hdfs dfs -mkdir -p $HDFS_HIVE_DIR
hdfs dfs -mkdir -p /user/hive/warehouse/accidents_db/cleaned
hdfs dfs -mkdir -p /spark-logs

# Check if data file exists
if [ -f "$LOCAL_DATA_PATH" ]; then
    echo "Uploading data to HDFS..."
    
    # Upload to data directory
    hdfs dfs -put -f $LOCAL_DATA_PATH $HDFS_DATA_DIR/
    
    # Copy to Hive raw table location
    hdfs dfs -put -f $LOCAL_DATA_PATH $HDFS_HIVE_DIR/
    
    # Verify upload
    echo "Verifying upload..."
    hdfs dfs -ls $HDFS_DATA_DIR/
    
    # Get file size
    FILE_SIZE=$(hdfs dfs -du -h $HDFS_DATA_DIR/US_Accidents_March23.csv | awk '{print $1}')
    echo "Uploaded file size: $FILE_SIZE"
    
    echo "Data upload complete!"
else
    echo "ERROR: Data file not found at $LOCAL_DATA_PATH"
    echo "Please ensure the CSV file is mounted to the container"
    exit 1
fi

# Set permissions
echo "Setting HDFS permissions..."
hdfs dfs -chmod -R 777 /data
hdfs dfs -chmod -R 777 /user/hive
hdfs dfs -chmod -R 777 /spark-logs

echo "============================================"
echo "HDFS UPLOAD COMPLETE"
echo "============================================"
echo ""
echo "Data locations:"
echo "  - Raw data: $HDFS_DATA_DIR/US_Accidents_March23.csv"
echo "  - Hive table: $HDFS_HIVE_DIR/US_Accidents_March23.csv"
echo ""
