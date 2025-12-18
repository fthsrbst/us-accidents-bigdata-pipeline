#!/usr/bin/env python3
"""
MongoDB Export Script'i
Pipeline sonuçlarını MongoDB'ye yazar
"""

import os
import json
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

from pyspark.sql import SparkSession

# Konfigürasyon
MONGODB_HOST = os.getenv('MONGODB_HOST', 'localhost')
MONGODB_PORT = int(os.getenv('MONGODB_PORT', 27017))
MONGODB_USER = os.getenv('MONGODB_USER', 'admin')
MONGODB_PASSWORD = os.getenv('MONGODB_PASSWORD', 'admin123')
MONGODB_DATABASE = os.getenv('MONGODB_DATABASE', 'bigdata_pipeline')

HDFS_CLEANED_PATH = 'hdfs://namenode:9000/user/bigdata/accidents_cleaned'
RESULTS_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "results")

def connect_mongodb():
    """MongoDB'ye bağlan"""
    print(f"MongoDB'ye bağlanılıyor: {MONGODB_HOST}:{MONGODB_PORT}")
    
    try:
        # Bağlantı string'i
        connection_string = f"mongodb://{MONGODB_USER}:{MONGODB_PASSWORD}@{MONGODB_HOST}:{MONGODB_PORT}/"
        
        client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        
        # Bağlantıyı test et
        client.admin.command('ping')
        
        print("✓ MongoDB bağlantısı başarılı")
        return client
    
    except ConnectionFailure as e:
        print(f"HATA: MongoDB bağlantısı başarısız!")
        print(f"Detay: {str(e)}")
        print("\nMongoDB servisinin çalıştığından emin olun:")
        print("  docker-compose up -d mongodb")
        raise

def create_spark_session():
    """Spark Session oluştur"""
    spark = SparkSession.builder \
        .appName("MongoDB_Export") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def export_model_results(db):
    """Model sonuçlarını MongoDB'ye yaz"""
    print("\n" + "=" * 60)
    print("MODEL SONUÇLARI EXPORT EDİLİYOR")
    print("=" * 60)
    
    # kNN sonuçları
    knn_results_path = os.path.join(RESULTS_PATH, 'knn_results.json')
    if os.path.exists(knn_results_path):
        with open(knn_results_path, 'r') as f:
            knn_results = json.load(f)
        
        knn_results['_id'] = f"knn_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        knn_results['created_at'] = datetime.now()
        
        db.model_results.insert_one(knn_results)
        print(f"✓ kNN sonuçları kaydedildi: {knn_results['_id']}")
    else:
        print("! kNN sonuç dosyası bulunamadı")
    
    # K-Means sonuçları
    kmeans_results_path = os.path.join(RESULTS_PATH, 'kmeans_results.json')
    if os.path.exists(kmeans_results_path):
        with open(kmeans_results_path, 'r') as f:
            kmeans_results = json.load(f)
        
        kmeans_results['_id'] = f"kmeans_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        kmeans_results['created_at'] = datetime.now()
        
        db.model_results.insert_one(kmeans_results)
        print(f"✓ K-Means sonuçları kaydedildi: {kmeans_results['_id']}")
    else:
        print("! K-Means sonuç dosyası bulunamadı")

def export_sample_predictions(db, spark, sample_size=10000):
    """Örnek tahminleri MongoDB'ye yaz"""
    print("\n" + "=" * 60)
    print("ÖRNEK TAHMİNLER EXPORT EDİLİYOR")
    print("=" * 60)
    
    try:
        # Temizlenmiş veriyi oku
        df = spark.read.parquet(HDFS_CLEANED_PATH)
        
        # Örneklem al
        total = df.count()
        if total > sample_size:
            fraction = sample_size / total
            df_sample = df.sample(fraction=fraction, seed=42)
        else:
            df_sample = df
        
        # Pandas'a çevir
        pdf = df_sample.toPandas()
        
        # Sütunları seç
        columns_to_export = [
            'ID', 'Severity', 'Start_Lat', 'Start_Lng', 'City', 'State',
            'Temperature_F', 'Weather_Condition', 'Hour', 'DayOfWeek',
            'TimeOfDay', 'Weather_Category'
        ]
        
        existing_columns = [c for c in columns_to_export if c in pdf.columns]
        pdf_export = pdf[existing_columns].copy()
        
        # Timestamp ekle
        pdf_export['exported_at'] = datetime.now()
        
        # MongoDB'ye yaz
        records = pdf_export.to_dict('records')
        
        if records:
            db.accidents_sample.drop()  # Mevcut veriyi sil
            db.accidents_sample.insert_many(records)
            print(f"✓ {len(records):,} örnek kayıt MongoDB'ye yazıldı")
        
    except Exception as e:
        print(f"HATA: Örnek tahminler export edilemedi: {str(e)}")

def export_aggregated_stats(db, spark):
    """Agregat istatistikleri MongoDB'ye yaz"""
    print("\n" + "=" * 60)
    print("İSTATİSTİKLER EXPORT EDİLİYOR")
    print("=" * 60)
    
    try:
        df = spark.read.parquet(HDFS_CLEANED_PATH)
        
        stats = {
            '_id': f"stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'created_at': datetime.now(),
            'total_records': df.count()
        }
        
        # Severity dağılımı
        if 'Severity' in df.columns:
            severity_dist = df.groupBy('Severity').count().collect()
            stats['severity_distribution'] = {
                str(row['Severity']): row['count'] for row in severity_dist
            }
        
        # State dağılımı (ilk 10)
        if 'State' in df.columns:
            state_dist = df.groupBy('State').count().orderBy('count', ascending=False).limit(10).collect()
            stats['top_states'] = {
                row['State']: row['count'] for row in state_dist
            }
        
        # Hava durumu dağılımı (ilk 10)
        if 'Weather_Category' in df.columns:
            weather_dist = df.groupBy('Weather_Category').count().collect()
            stats['weather_distribution'] = {
                row['Weather_Category']: row['count'] for row in weather_dist
            }
        
        # Zaman dağılımı
        if 'TimeOfDay' in df.columns:
            time_dist = df.groupBy('TimeOfDay').count().collect()
            stats['time_of_day_distribution'] = {
                row['TimeOfDay']: row['count'] for row in time_dist
            }
        
        # Sayısal istatistikler
        numeric_cols = ['Temperature_F', 'Humidity_pct', 'Visibility_mi', 'Wind_Speed_mph']
        for col_name in numeric_cols:
            if col_name in df.columns:
                col_stats = df.select(col_name).describe().collect()
                stats[f'{col_name}_stats'] = {
                    row['summary']: row[col_name] for row in col_stats
                }
        
        # MongoDB'ye yaz
        db.statistics.insert_one(stats)
        print(f"✓ İstatistikler kaydedildi: {stats['_id']}")
        
    except Exception as e:
        print(f"HATA: İstatistikler export edilemedi: {str(e)}")

def export_pipeline_metadata(db):
    """Pipeline metadata'sını MongoDB'ye yaz"""
    print("\n" + "=" * 60)
    print("PİPELINE METADATA EXPORT EDİLİYOR")
    print("=" * 60)
    
    metadata = {
        '_id': f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        'created_at': datetime.now(),
        'pipeline_name': 'US Accidents Big Data Pipeline',
        'version': '1.0',
        'architecture': 'HDFS + Hive + Kafka + Spark + MongoDB',
        'components': {
            'data_ingestion': 'Kafka Producer',
            'stream_processing': 'Spark Streaming',
            'storage': 'HDFS (Parquet)',
            'metadata_store': 'Hive',
            'analytics': ['PySpark', 'Scikit-learn'],
            'models': ['kNN Classification', 'K-Means Clustering'],
            'output': 'MongoDB'
        },
        'dataset': {
            'name': 'US Accidents',
            'source': 'Kaggle',
            'features': [
                'Severity', 'Location (Lat/Lng)', 'Weather',
                'Time', 'Traffic Features'
            ]
        },
        'metrics_tracked': [
            'Accuracy', 'Precision', 'Recall', 'F1-Score', 'AUC-ROC',
            'Silhouette Score', 'Calinski-Harabasz', 'Davies-Bouldin'
        ]
    }
    
    db.pipeline_metadata.insert_one(metadata)
    print(f"✓ Pipeline metadata kaydedildi: {metadata['_id']}")

def create_indexes(db):
    """MongoDB indexleri oluştur"""
    print("\n" + "=" * 60)
    print("INDEXLER OLUŞTURULUYOR")
    print("=" * 60)
    
    # accidents_sample collection
    db.accidents_sample.create_index([('Severity', 1)])
    db.accidents_sample.create_index([('State', 1)])
    db.accidents_sample.create_index([('City', 1)])
    db.accidents_sample.create_index([('exported_at', -1)])
    print("✓ accidents_sample indexleri oluşturuldu")
    
    # model_results collection
    db.model_results.create_index([('model', 1)])
    db.model_results.create_index([('created_at', -1)])
    print("✓ model_results indexleri oluşturuldu")
    
    # statistics collection
    db.statistics.create_index([('created_at', -1)])
    print("✓ statistics indexleri oluşturuldu")

def print_summary(db):
    """MongoDB özeti yazdır"""
    print("\n" + "=" * 60)
    print("MONGODB ÖZET")
    print("=" * 60)
    
    collections = db.list_collection_names()
    
    print(f"\nVeritabanı: {MONGODB_DATABASE}")
    print(f"Collection sayısı: {len(collections)}")
    print("\nCollection detayları:")
    
    for coll_name in sorted(collections):
        count = db[coll_name].count_documents({})
        print(f"  - {coll_name}: {count:,} döküman")

def main():
    print("=" * 60)
    print("MONGODB EXPORT - BIG DATA PIPELINE")
    print("=" * 60)
    print(f"Host: {MONGODB_HOST}:{MONGODB_PORT}")
    print(f"Database: {MONGODB_DATABASE}")
    print("-" * 60)
    
    # MongoDB bağlantısı
    try:
        client = connect_mongodb()
        db = client[MONGODB_DATABASE]
    except Exception as e:
        print(f"MongoDB bağlantı hatası: {e}")
        return
    
    # Spark session
    spark = create_spark_session()
    print("✓ Spark Session oluşturuldu")
    
    try:
        # Model sonuçlarını export et
        export_model_results(db)
        
        # Örnek tahminleri export et
        export_sample_predictions(db, spark)
        
        # İstatistikleri export et
        export_aggregated_stats(db, spark)
        
        # Pipeline metadata
        export_pipeline_metadata(db)
        
        # Indexler oluştur
        create_indexes(db)
        
        # Özet
        print_summary(db)
        
        print("\n" + "=" * 60)
        print("MONGODB EXPORT TAMAMLANDI!")
        print("=" * 60)
        print("\nMongoDB Web UI: http://localhost:8082")
        print("  Kullanıcı: admin")
        print("  Şifre: admin123")
        
    except Exception as e:
        print(f"\nHATA: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        client.close()

if __name__ == "__main__":
    main()
