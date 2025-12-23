-- ============================================
-- Hive DDL for US Accidents Data Pipeline
-- ============================================

-- Create database
CREATE DATABASE IF NOT EXISTS accidents_db
COMMENT 'US Accidents Big Data Pipeline Database'
LOCATION '/user/hive/warehouse/accidents_db';

USE accidents_db;

-- ============================================
-- External table for raw CSV data
-- ============================================
CREATE EXTERNAL TABLE IF NOT EXISTS us_accidents_raw (
    ID STRING,
    Severity INT,
    Start_Time STRING,
    End_Time STRING,
    Start_Lat DOUBLE,
    Start_Lng DOUBLE,
    End_Lat DOUBLE,
    End_Lng DOUBLE,
    Distance_mi DOUBLE,
    Description STRING,
    Street STRING,
    City STRING,
    County STRING,
    State STRING,
    Zipcode STRING,
    Country STRING,
    Timezone STRING,
    Airport_Code STRING,
    Weather_Timestamp STRING,
    Temperature_F DOUBLE,
    Wind_Chill_F DOUBLE,
    Humidity_Percent DOUBLE,
    Pressure_in DOUBLE,
    Visibility_mi DOUBLE,
    Wind_Direction STRING,
    Wind_Speed_mph DOUBLE,
    Precipitation_in DOUBLE,
    Weather_Condition STRING,
    Amenity BOOLEAN,
    Bump BOOLEAN,
    Crossing BOOLEAN,
    Give_Way BOOLEAN,
    Junction BOOLEAN,
    No_Exit BOOLEAN,
    Railway BOOLEAN,
    Roundabout BOOLEAN,
    Station BOOLEAN,
    Stop BOOLEAN,
    Traffic_Calming BOOLEAN,
    Traffic_Signal BOOLEAN,
    Turning_Loop BOOLEAN,
    Sunrise_Sunset STRING,
    Civil_Twilight STRING,
    Nautical_Twilight STRING,
    Astronomical_Twilight STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/accidents_db/raw'
TBLPROPERTIES ('skip.header.line.count'='1');

-- ============================================
-- ORC table for cleaned/processed data
-- ============================================
CREATE TABLE IF NOT EXISTS us_accidents_cleaned (
    ID STRING,
    Severity INT,
    Start_Lat DOUBLE,
    Start_Lng DOUBLE,
    Distance_mi DOUBLE,
    City STRING,
    County STRING,
    State STRING,
    Temperature_F DOUBLE,
    Humidity_Percent DOUBLE,
    Pressure_in DOUBLE,
    Visibility_mi DOUBLE,
    Wind_Speed_mph DOUBLE,
    Precipitation_in DOUBLE,
    Weather_Condition STRING,
    Amenity INT,
    Crossing INT,
    Junction INT,
    Railway INT,
    Station INT,
    Stop INT,
    Traffic_Signal INT,
    Sunrise_Sunset STRING,
    Hour_of_Day INT,
    Day_of_Week INT,
    Month INT
)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ============================================
-- Table for clustered data
-- ============================================
CREATE TABLE IF NOT EXISTS us_accidents_clustered (
    ID STRING,
    Severity INT,
    Start_Lat DOUBLE,
    Start_Lng DOUBLE,
    State STRING,
    City STRING,
    Weather_Condition STRING,
    Cluster_ID INT,
    Distance_to_Center DOUBLE
)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ============================================
-- Results tables
-- ============================================
CREATE TABLE IF NOT EXISTS model_results (
    model_name STRING,
    metric_name STRING,
    metric_value DOUBLE,
    run_timestamp STRING,
    parameters STRING
)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS cluster_centers (
    cluster_id INT,
    center_lat DOUBLE,
    center_lng DOUBLE,
    point_count INT,
    avg_severity DOUBLE
)
STORED AS ORC;

-- Show tables
SHOW TABLES;
