#!/usr/bin/env python3
"""
Spark Streaming - Kafka'dan veri okuyup HDFS'e yazar
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, BooleanType, TimestampType
)

# Konfigürasyon
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'us_accidents')
HDFS_OUTPUT_PATH = os.getenv('HDFS_DATA_PATH', 'hdfs://namenode:9000/user/bigdata/accidents')
CHECKPOINT_PATH = os.getenv('CHECKPOINT_PATH', 'hdfs://namenode:9000/user/bigdata/checkpoints')

# US Accidents veri seti şeması
ACCIDENTS_SCHEMA = StructType([
    StructField("ID", StringType(), True),
    StructField("Source", StringType(), True),
    StructField("Severity", IntegerType(), True),
    StructField("Start_Time", StringType(), True),
    StructField("End_Time", StringType(), True),
    StructField("Start_Lat", DoubleType(), True),
    StructField("Start_Lng", DoubleType(), True),
    StructField("End_Lat", DoubleType(), True),
    StructField("End_Lng", DoubleType(), True),
    StructField("Distance(mi)", DoubleType(), True),
    StructField("Description", StringType(), True),
    StructField("Street", StringType(), True),
    StructField("City", StringType(), True),
    StructField("County", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Zipcode", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Timezone", StringType(), True),
    StructField("Airport_Code", StringType(), True),
    StructField("Weather_Timestamp", StringType(), True),
    StructField("Temperature(F)", DoubleType(), True),
    StructField("Wind_Chill(F)", DoubleType(), True),
    StructField("Humidity(%)", DoubleType(), True),
    StructField("Pressure(in)", DoubleType(), True),
    StructField("Visibility(mi)", DoubleType(), True),
    StructField("Wind_Direction", StringType(), True),
    StructField("Wind_Speed(mph)", DoubleType(), True),
    StructField("Precipitation(in)", DoubleType(), True),
    StructField("Weather_Condition", StringType(), True),
    StructField("Amenity", BooleanType(), True),
    StructField("Bump", BooleanType(), True),
    StructField("Crossing", BooleanType(), True),
    StructField("Give_Way", BooleanType(), True),
    StructField("Junction", BooleanType(), True),
    StructField("No_Exit", BooleanType(), True),
    StructField("Railway", BooleanType(), True),
    StructField("Roundabout", BooleanType(), True),
    StructField("Station", BooleanType(), True),
    StructField("Stop", BooleanType(), True),
    StructField("Traffic_Calming", BooleanType(), True),
    StructField("Traffic_Signal", BooleanType(), True),
    StructField("Turning_Loop", BooleanType(), True),
    StructField("Sunrise_Sunset", StringType(), True),
    StructField("Civil_Twilight", StringType(), True),
    StructField("Nautical_Twilight", StringType(), True),
    StructField("Astronomical_Twilight", StringType(), True)
])

def create_spark_session():
    """Spark Session oluştur"""
    spark = SparkSession.builder \
        .appName("US_Accidents_Streaming") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def start_streaming():
    """Kafka'dan streaming başlat ve HDFS'e yaz"""
    print("=" * 60)
    print("SPARK STREAMING - KAFKA TO HDFS")
    print("=" * 60)
    print(f"Kafka Server: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"HDFS Output: {HDFS_OUTPUT_PATH}")
    print("-" * 60)
    
    # Spark session oluştur
    spark = create_spark_session()
    print("✓ Spark Session oluşturuldu")
    
    # Kafka'dan stream oku
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    print("✓ Kafka stream bağlantısı kuruldu")
    
    # JSON'ı parse et
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), ACCIDENTS_SCHEMA).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    # İşlem timestamp'i ekle
    final_df = parsed_df.withColumn("processing_time", current_timestamp())
    
    # HDFS'e Parquet formatında yaz
    query = final_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", HDFS_OUTPUT_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    print("✓ Streaming başlatıldı - HDFS'e yazılıyor...")
    print("\nDurum bilgisi için: http://localhost:4040")
    print("Durdurmak için: Ctrl+C")
    print("-" * 60)
    
    # Streaming'i bekle
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n\nStreaming durduruluyor...")
        query.stop()
        spark.stop()
        print("✓ Streaming durduruldu")

def batch_load_to_hdfs():
    """
    Alternatif: Batch modunda CSV'yi HDFS'e yükle
    (Streaming yerine doğrudan yükleme için)
    """
    print("=" * 60)
    print("BATCH LOAD - CSV TO HDFS")
    print("=" * 60)
    
    spark = create_spark_session()
    
    # CSV dosyasını bul
    data_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")
    csv_files = [f for f in os.listdir(data_path) if f.endswith('.csv')]
    
    if not csv_files:
        print("HATA: CSV dosyası bulunamadı!")
        return
    
    csv_path = os.path.join(data_path, csv_files[0])
    print(f"CSV Dosyası: {csv_path}")
    
    # CSV'yi oku
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(csv_path)
    
    record_count = df.count()
    print(f"Toplam kayıt: {record_count:,}")
    
    # HDFS'e Parquet olarak yaz
    df.write \
        .mode("overwrite") \
        .parquet(HDFS_OUTPUT_PATH)
    
    print(f"✓ Veriler HDFS'e yazıldı: {HDFS_OUTPUT_PATH}")
    
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--batch":
        batch_load_to_hdfs()
    else:
        start_streaming()
