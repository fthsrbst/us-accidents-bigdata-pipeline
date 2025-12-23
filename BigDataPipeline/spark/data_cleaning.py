"""
Data Cleaning Job - PySpark (Simplified)
US Accidents Big Data Pipeline
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, hour, dayofweek, month,
    lit, count, isnan
)
from pyspark.sql.types import IntegerType, DoubleType
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DataCleaning")

def create_spark_session():
    return SparkSession.builder \
        .appName("US_Accidents_DataCleaning") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

def load_data(spark, input_path):
    logger.info(f"Loading data from: {input_path}")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(input_path)
    logger.info(f"Loaded {df.count()} records with {len(df.columns)} columns")
    return df

def select_features(df):
    logger.info("Selecting features...")
    selected_columns = [
        'ID', 'Severity', 'Start_Lat', 'Start_Lng',
        'Distance(mi)', 'City', 'County', 'State',
        'Temperature(F)', 'Humidity(%)', 'Pressure(in)',
        'Visibility(mi)', 'Wind_Speed(mph)', 'Precipitation(in)',
        'Weather_Condition', 'Amenity', 'Crossing', 'Junction',
        'Railway', 'Station', 'Stop', 'Traffic_Signal',
        'Sunrise_Sunset', 'Start_Time'
    ]
    existing_columns = [c for c in selected_columns if c in df.columns]
    return df.select(existing_columns)

def clean_data(df):
    logger.info("Cleaning data...")
    
    # Rename columns
    column_mapping = {
        'Distance(mi)': 'Distance_mi', 'Temperature(F)': 'Temperature_F',
        'Humidity(%)': 'Humidity_Percent', 'Pressure(in)': 'Pressure_in',
        'Visibility(mi)': 'Visibility_mi', 'Wind_Speed(mph)': 'Wind_Speed_mph',
        'Precipitation(in)': 'Precipitation_in'
    }
    for old_name, new_name in column_mapping.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    
    # Handle missing numeric values
    numeric_cols = ['Temperature_F', 'Humidity_Percent', 'Pressure_in',
                    'Visibility_mi', 'Wind_Speed_mph', 'Precipitation_in', 'Distance_mi']
    for col_name in numeric_cols:
        if col_name in df.columns:
            median_val = df.approxQuantile(col_name, [0.5], 0.01)
            if median_val:
                df = df.withColumn(col_name, 
                    when(col(col_name).isNull() | isnan(col(col_name)), lit(median_val[0]))
                    .otherwise(col(col_name)))
    
    # Handle categorical
    for col_name in ['Weather_Condition', 'Sunrise_Sunset', 'City', 'State']:
        if col_name in df.columns:
            df = df.withColumn(col_name,
                when(col(col_name).isNull() | (col(col_name) == ""), lit("Unknown"))
                .otherwise(col(col_name)))
    
    # Boolean to int
    for col_name in ['Amenity', 'Crossing', 'Junction', 'Railway', 'Station', 'Stop', 'Traffic_Signal']:
        if col_name in df.columns:
            df = df.withColumn(col_name,
                when(col(col_name) == True, lit(1))
                .when(col(col_name) == False, lit(0))
                .otherwise(lit(0)).cast(IntegerType()))
    
    # Temporal features
    if 'Start_Time' in df.columns:
        df = df.withColumn('Hour_of_Day', hour(col('Start_Time')))
        df = df.withColumn('Day_of_Week', dayofweek(col('Start_Time')))
        df = df.withColumn('Month', month(col('Start_Time')))
        df = df.drop('Start_Time')
    
    # Filter valid data
    df = df.filter(col('Start_Lat').isNotNull() & col('Start_Lng').isNotNull())
    df = df.filter((col('Severity') >= 1) & (col('Severity') <= 4))
    
    logger.info(f"Cleaned data has {df.count()} records")
    return df

def main():
    logger.info("=" * 60)
    logger.info("US ACCIDENTS DATA CLEANING PIPELINE")
    logger.info("=" * 60)
    
    input_path = "/home/jovyan/data/US_Accidents_March23.csv"
    output_path = "/home/jovyan/output/cleaned_data"
    
    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        
        # Load and sample data
        df_raw = load_data(spark, input_path)
        
        # Sample for faster processing (10% of data)
        df_sampled = df_raw.sample(False, 0.1, seed=42)
        logger.info(f"Sampled to {df_sampled.count()} records")
        
        # Select and clean
        df_selected = select_features(df_sampled)
        df_cleaned = clean_data(df_selected)
        df_cleaned.cache()
        
        # Show sample
        logger.info("Sample of cleaned data:")
        df_cleaned.show(5, truncate=False)
        df_cleaned.printSchema()
        
        # Save
        os.makedirs(output_path, exist_ok=True)
        df_cleaned.write.mode("overwrite").parquet(output_path)
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("CLEANING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Cleaned records: {df_cleaned.count()}")
        logger.info(f"Features: {len(df_cleaned.columns)}")
        
        # Severity distribution
        logger.info("\nSeverity Distribution:")
        df_cleaned.groupBy("Severity").count().orderBy("Severity").show()
        
        spark.stop()
        logger.info("Data cleaning completed successfully!")
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
