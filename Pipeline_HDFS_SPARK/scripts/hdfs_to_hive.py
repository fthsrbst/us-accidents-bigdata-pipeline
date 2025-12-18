#!/usr/bin/env python3
"""
HDFS'den Hive'a Veri Yükleme Script'i
Parquet dosyalarını Hive tablosu olarak tanımlar
"""

import os
from pyspark.sql import SparkSession

# Konfigürasyon
HDFS_DATA_PATH = os.getenv('HDFS_DATA_PATH', 'hdfs://namenode:9000/user/bigdata/accidents')
HIVE_DATABASE = 'bigdata_db'
HIVE_TABLE = 'us_accidents'

def create_spark_session():
    """Hive destekli Spark Session oluştur"""
    spark = SparkSession.builder \
        .appName("HDFS_to_Hive_Loader") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def create_hive_database(spark):
    """Hive veritabanı oluştur"""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DATABASE}")
    spark.sql(f"USE {HIVE_DATABASE}")
    print(f"✓ Veritabanı oluşturuldu/seçildi: {HIVE_DATABASE}")

def create_hive_table(spark):
    """HDFS'deki Parquet dosyalarından Hive tablosu oluştur"""
    
    # Önce mevcut tabloyu kontrol et
    tables = spark.sql(f"SHOW TABLES IN {HIVE_DATABASE}").collect()
    table_exists = any(row['tableName'] == HIVE_TABLE for row in tables)
    
    if table_exists:
        print(f"Tablo zaten mevcut: {HIVE_TABLE}")
        response = input("Tabloyu yeniden oluşturmak ister misiniz? (e/h): ")
        if response.lower() != 'e':
            return
        spark.sql(f"DROP TABLE IF EXISTS {HIVE_DATABASE}.{HIVE_TABLE}")
    
    # External table oluştur (HDFS'deki veriye işaret eden)
    create_table_sql = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {HIVE_DATABASE}.{HIVE_TABLE} (
        ID STRING,
        Source STRING,
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
        Humidity_pct DOUBLE,
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
        Astronomical_Twilight STRING,
        kafka_timestamp TIMESTAMP,
        processing_time TIMESTAMP
    )
    STORED AS PARQUET
    LOCATION '{HDFS_DATA_PATH}'
    """
    
    spark.sql(create_table_sql)
    print(f"✓ Hive tablosu oluşturuldu: {HIVE_DATABASE}.{HIVE_TABLE}")

def verify_table(spark):
    """Tabloyu doğrula ve örnek veri göster"""
    print("\n" + "-" * 60)
    print("TABLO DOĞRULAMA")
    print("-" * 60)
    
    # Kayıt sayısı
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {HIVE_DATABASE}.{HIVE_TABLE}").collect()[0]['cnt']
    print(f"Toplam kayıt sayısı: {count:,}")
    
    # Şema
    print("\nTablo şeması:")
    spark.sql(f"DESCRIBE {HIVE_DATABASE}.{HIVE_TABLE}").show(truncate=False)
    
    # Örnek veri
    print("\nÖrnek veriler (ilk 5 kayıt):")
    spark.sql(f"""
        SELECT ID, Severity, City, State, Weather_Condition 
        FROM {HIVE_DATABASE}.{HIVE_TABLE} 
        LIMIT 5
    """).show(truncate=False)
    
    # Severity dağılımı
    print("\nSeverity (Ciddiyet) dağılımı:")
    spark.sql(f"""
        SELECT Severity, COUNT(*) as count 
        FROM {HIVE_DATABASE}.{HIVE_TABLE} 
        GROUP BY Severity 
        ORDER BY Severity
    """).show()
    
    # State bazında istatistikler
    print("\nEn çok kaza olan 10 eyalet:")
    spark.sql(f"""
        SELECT State, COUNT(*) as accident_count 
        FROM {HIVE_DATABASE}.{HIVE_TABLE} 
        GROUP BY State 
        ORDER BY accident_count DESC 
        LIMIT 10
    """).show()

def run_sample_queries(spark):
    """Örnek Hive sorguları çalıştır"""
    print("\n" + "=" * 60)
    print("ÖRNEK HİVE SORGULARI")
    print("=" * 60)
    
    # Sorgu 1: Ortalama sıcaklık ve nem by severity
    print("\n1. Severity bazında ortalama hava durumu:")
    spark.sql(f"""
        SELECT 
            Severity,
            ROUND(AVG(Temperature_F), 2) as avg_temp,
            ROUND(AVG(Humidity_pct), 2) as avg_humidity,
            COUNT(*) as total
        FROM {HIVE_DATABASE}.{HIVE_TABLE}
        WHERE Temperature_F IS NOT NULL
        GROUP BY Severity
        ORDER BY Severity
    """).show()
    
    # Sorgu 2: Hava durumu koşullarına göre kaza sayısı
    print("\n2. En yaygın 10 hava durumu koşulu:")
    spark.sql(f"""
        SELECT 
            Weather_Condition,
            COUNT(*) as accident_count
        FROM {HIVE_DATABASE}.{HIVE_TABLE}
        WHERE Weather_Condition IS NOT NULL
        GROUP BY Weather_Condition
        ORDER BY accident_count DESC
        LIMIT 10
    """).show(truncate=False)
    
    # Sorgu 3: Gündüz/Gece dağılımı
    print("\n3. Gündüz/Gece kaza dağılımı:")
    spark.sql(f"""
        SELECT 
            Sunrise_Sunset as time_of_day,
            COUNT(*) as accident_count,
            ROUND(AVG(Severity), 2) as avg_severity
        FROM {HIVE_DATABASE}.{HIVE_TABLE}
        WHERE Sunrise_Sunset IS NOT NULL
        GROUP BY Sunrise_Sunset
    """).show()

def main():
    print("=" * 60)
    print("HDFS -> HIVE VERI YUKLEME")
    print("=" * 60)
    print(f"HDFS Kaynak: {HDFS_DATA_PATH}")
    print(f"Hive Veritabanı: {HIVE_DATABASE}")
    print(f"Hive Tablosu: {HIVE_TABLE}")
    print("-" * 60)
    
    # Spark session oluştur
    spark = create_spark_session()
    print("✓ Spark Session oluşturuldu (Hive desteği ile)")
    
    try:
        # Veritabanı oluştur
        create_hive_database(spark)
        
        # Tablo oluştur
        create_hive_table(spark)
        
        # Tabloyu doğrula
        verify_table(spark)
        
        # Örnek sorgular
        run_sample_queries(spark)
        
        print("\n" + "=" * 60)
        print("HIVE TABLOLARI BAŞARIYLA OLUŞTURULDU!")
        print("=" * 60)
        print("\nSonraki adım: Veri temizleme")
        print("  python scripts/data_cleaning.py")
        
    except Exception as e:
        print(f"\nHATA: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
