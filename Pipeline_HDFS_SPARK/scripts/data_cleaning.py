#!/usr/bin/env python3
"""
Veri Temizleme Script'i
PySpark ile US Accidents verisini temizler ve işler
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, count, isnan, isnull, mean, stddev,
    hour, dayofweek, month, year, to_timestamp,
    lit, coalesce, trim, upper, lower,
    regexp_replace, split, size
)
from pyspark.sql.types import IntegerType, DoubleType

# Konfigürasyon
HDFS_INPUT_PATH = os.getenv('HDFS_DATA_PATH', 'hdfs://namenode:9000/user/bigdata/accidents')
HDFS_CLEANED_PATH = 'hdfs://namenode:9000/user/bigdata/accidents_cleaned'

def create_spark_session():
    """Spark Session oluştur"""
    spark = SparkSession.builder \
        .appName("US_Accidents_Data_Cleaning") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def load_data(spark):
    """HDFS'den veri yükle"""
    print("Veri yükleniyor...")
    df = spark.read.parquet(HDFS_INPUT_PATH)
    print(f"✓ Toplam kayıt: {df.count():,}")
    print(f"✓ Sütun sayısı: {len(df.columns)}")
    return df

def analyze_missing_values(df):
    """Eksik değer analizi"""
    print("\n" + "=" * 60)
    print("EKSİK DEĞER ANALİZİ")
    print("=" * 60)
    
    total_rows = df.count()
    missing_stats = []
    
    for column in df.columns:
        # Null ve NaN değerleri say
        null_count = df.filter(
            col(column).isNull() | 
            (col(column) == "") |
            (col(column) == "NaN")
        ).count()
        
        if null_count > 0:
            missing_pct = (null_count / total_rows) * 100
            missing_stats.append({
                'column': column,
                'missing': null_count,
                'percentage': missing_pct
            })
    
    # Sırala ve göster
    missing_stats.sort(key=lambda x: x['percentage'], reverse=True)
    
    print(f"\n{'Sütun':<30} {'Eksik':<15} {'Yüzde':<10}")
    print("-" * 55)
    for stat in missing_stats[:20]:  # İlk 20
        print(f"{stat['column']:<30} {stat['missing']:<15,} {stat['percentage']:.2f}%")
    
    return missing_stats

def clean_data(df):
    """Veri temizleme işlemleri"""
    print("\n" + "=" * 60)
    print("VERİ TEMİZLEME")
    print("=" * 60)
    
    initial_count = df.count()
    
    # 1. Kritik sütunlardaki null değerleri olan satırları kaldır
    print("\n1. Kritik null değerler kaldırılıyor...")
    critical_columns = ['Severity', 'Start_Lat', 'Start_Lng', 'City', 'State']
    
    for col_name in critical_columns:
        if col_name in df.columns:
            df = df.filter(col(col_name).isNotNull())
    
    after_critical = df.count()
    print(f"   Kaldırılan kayıt: {initial_count - after_critical:,}")
    
    # 2. Sayısal sütunlarda eksik değerleri ortalama ile doldur
    print("\n2. Sayısal eksik değerler ortalama ile dolduruluyor...")
    numeric_columns = [
        'Temperature_F', 'Wind_Chill_F', 'Humidity_pct', 
        'Pressure_in', 'Visibility_mi', 'Wind_Speed_mph', 'Precipitation_in'
    ]
    
    # Sütun isimlerini düzelt (parantezli olanlar için)
    column_mapping = {
        'Temperature(F)': 'Temperature_F',
        'Wind_Chill(F)': 'Wind_Chill_F',
        'Humidity(%)': 'Humidity_pct',
        'Pressure(in)': 'Pressure_in',
        'Visibility(mi)': 'Visibility_mi',
        'Wind_Speed(mph)': 'Wind_Speed_mph',
        'Precipitation(in)': 'Precipitation_in',
        'Distance(mi)': 'Distance_mi'
    }
    
    # Sütun isimlerini değiştir
    for old_name, new_name in column_mapping.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    
    # Ortalama ile doldur
    for col_name in numeric_columns:
        if col_name in df.columns:
            avg_val = df.agg(mean(col(col_name))).collect()[0][0]
            if avg_val is not None:
                df = df.withColumn(
                    col_name,
                    when(col(col_name).isNull(), lit(avg_val))
                    .otherwise(col(col_name))
                )
    
    # 3. Boolean sütunlardaki null değerleri False yap
    print("\n3. Boolean null değerler False yapılıyor...")
    boolean_columns = [
        'Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction',
        'No_Exit', 'Railway', 'Roundabout', 'Station', 'Stop',
        'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop'
    ]
    
    for col_name in boolean_columns:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                when(col(col_name).isNull(), lit(False))
                .otherwise(col(col_name))
            )
    
    # 4. Kategorik sütunlardaki null değerleri 'Unknown' yap
    print("\n4. Kategorik null değerler 'Unknown' yapılıyor...")
    categorical_columns = [
        'Weather_Condition', 'Wind_Direction', 'Sunrise_Sunset',
        'Civil_Twilight', 'Nautical_Twilight', 'Astronomical_Twilight'
    ]
    
    for col_name in categorical_columns:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                when(col(col_name).isNull() | (col(col_name) == ""), lit("Unknown"))
                .otherwise(col(col_name))
            )
    
    # 5. String değerleri temizle (trim, standardize)
    print("\n5. String değerler standardize ediliyor...")
    string_columns = ['City', 'State', 'County', 'Street']
    
    for col_name in string_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, trim(col(col_name)))
    
    final_count = df.count()
    print(f"\n✓ Temizleme tamamlandı")
    print(f"  Başlangıç: {initial_count:,}")
    print(f"  Son: {final_count:,}")
    print(f"  Kaldırılan: {initial_count - final_count:,} ({(initial_count - final_count) / initial_count * 100:.2f}%)")
    
    return df

def feature_engineering(df):
    """Yeni özellikler oluştur"""
    print("\n" + "=" * 60)
    print("ÖZELLİK MÜHENDİSLİĞİ (FEATURE ENGINEERING)")
    print("=" * 60)
    
    # Start_Time'dan zaman özellikleri çıkar
    print("\n1. Zaman özellikleri oluşturuluyor...")
    
    if 'Start_Time' in df.columns:
        df = df.withColumn('Start_Time_ts', to_timestamp(col('Start_Time')))
        df = df.withColumn('Hour', hour(col('Start_Time_ts')))
        df = df.withColumn('DayOfWeek', dayofweek(col('Start_Time_ts')))
        df = df.withColumn('Month', month(col('Start_Time_ts')))
        df = df.withColumn('Year', year(col('Start_Time_ts')))
        
        # Saat dilimi kategorileri
        df = df.withColumn(
            'TimeOfDay',
            when((col('Hour') >= 6) & (col('Hour') < 12), 'Morning')
            .when((col('Hour') >= 12) & (col('Hour') < 17), 'Afternoon')
            .when((col('Hour') >= 17) & (col('Hour') < 21), 'Evening')
            .otherwise('Night')
        )
        
        # Hafta sonu/Hafta içi
        df = df.withColumn(
            'IsWeekend',
            when(col('DayOfWeek').isin([1, 7]), lit(1)).otherwise(lit(0))
        )
    
    # 2. Hava durumu kategorileri
    print("2. Hava durumu kategorileri oluşturuluyor...")
    if 'Weather_Condition' in df.columns:
        df = df.withColumn(
            'Weather_Category',
            when(col('Weather_Condition').contains('Clear'), 'Clear')
            .when(col('Weather_Condition').contains('Cloud'), 'Cloudy')
            .when(col('Weather_Condition').contains('Rain'), 'Rainy')
            .when(col('Weather_Condition').contains('Snow'), 'Snowy')
            .when(col('Weather_Condition').contains('Fog'), 'Foggy')
            .when(col('Weather_Condition').contains('Wind'), 'Windy')
            .otherwise('Other')
        )
    
    # 3. Trafik özellikleri toplamı
    print("3. Trafik özellikleri toplamı hesaplanıyor...")
    traffic_cols = [
        'Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction',
        'No_Exit', 'Railway', 'Roundabout', 'Station', 'Stop',
        'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop'
    ]
    
    existing_traffic_cols = [c for c in traffic_cols if c in df.columns]
    if existing_traffic_cols:
        df = df.withColumn(
            'Traffic_Feature_Count',
            sum([col(c).cast(IntegerType()) for c in existing_traffic_cols])
        )
    
    # 4. Sıcaklık kategorileri
    print("4. Sıcaklık kategorileri oluşturuluyor...")
    if 'Temperature_F' in df.columns:
        df = df.withColumn(
            'Temp_Category',
            when(col('Temperature_F') < 32, 'Freezing')
            .when((col('Temperature_F') >= 32) & (col('Temperature_F') < 50), 'Cold')
            .when((col('Temperature_F') >= 50) & (col('Temperature_F') < 70), 'Mild')
            .when((col('Temperature_F') >= 70) & (col('Temperature_F') < 90), 'Warm')
            .otherwise('Hot')
        )
    
    print("✓ Feature engineering tamamlandı")
    print(f"  Yeni sütun sayısı: {len(df.columns)}")
    
    return df

def detect_outliers(df):
    """Aykırı değer tespiti"""
    print("\n" + "=" * 60)
    print("AYKIRI DEĞER TESPİTİ")
    print("=" * 60)
    
    numeric_columns = ['Temperature_F', 'Visibility_mi', 'Wind_Speed_mph', 'Humidity_pct']
    
    for col_name in numeric_columns:
        if col_name in df.columns:
            stats = df.select(
                mean(col(col_name)).alias('mean'),
                stddev(col(col_name)).alias('std')
            ).collect()[0]
            
            if stats['mean'] is not None and stats['std'] is not None:
                mean_val = stats['mean']
                std_val = stats['std']
                
                # 3 standart sapma dışındaki değerler
                lower_bound = mean_val - 3 * std_val
                upper_bound = mean_val + 3 * std_val
                
                outlier_count = df.filter(
                    (col(col_name) < lower_bound) | (col(col_name) > upper_bound)
                ).count()
                
                print(f"\n{col_name}:")
                print(f"  Ortalama: {mean_val:.2f}, Std: {std_val:.2f}")
                print(f"  Aralık: [{lower_bound:.2f}, {upper_bound:.2f}]")
                print(f"  Aykırı değer sayısı: {outlier_count:,}")

def save_cleaned_data(df):
    """Temizlenmiş veriyi kaydet"""
    print("\n" + "=" * 60)
    print("VERİ KAYDETME")
    print("=" * 60)
    
    # HDFS'e Parquet olarak kaydet
    df.write \
        .mode("overwrite") \
        .parquet(HDFS_CLEANED_PATH)
    
    print(f"✓ Temizlenmiş veri kaydedildi: {HDFS_CLEANED_PATH}")
    
    # Şema bilgisini yazdır
    print("\nKaydedilen veri şeması:")
    df.printSchema()

def main():
    print("=" * 60)
    print("US ACCIDENTS VERİ TEMİZLEME PİPELINE")
    print("=" * 60)
    print(f"Girdi: {HDFS_INPUT_PATH}")
    print(f"Çıktı: {HDFS_CLEANED_PATH}")
    print("-" * 60)
    
    # Spark session oluştur
    spark = create_spark_session()
    print("✓ Spark Session oluşturuldu")
    
    try:
        # Veri yükle
        df = load_data(spark)
        
        # Eksik değer analizi
        analyze_missing_values(df)
        
        # Veri temizleme
        df_cleaned = clean_data(df)
        
        # Aykırı değer tespiti
        detect_outliers(df_cleaned)
        
        # Feature engineering
        df_final = feature_engineering(df_cleaned)
        
        # Kaydet
        save_cleaned_data(df_final)
        
        print("\n" + "=" * 60)
        print("VERİ TEMİZLEME TAMAMLANDI!")
        print("=" * 60)
        print("\nSonraki adım: kNN Sınıflandırma")
        print("  python scripts/knn_classification.py")
        
    except Exception as e:
        print(f"\nHATA: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
