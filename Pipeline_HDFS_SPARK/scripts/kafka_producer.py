#!/usr/bin/env python3
"""
Kafka Producer - US Accidents Veri Seti
CSV dosyasını okuyup Kafka topic'ine stream eder
"""

import os
import sys
import json
import time
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Konfigürasyon
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'us_accidents')
DATA_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")
BATCH_SIZE = 1000  # Her batch'te gönderilecek kayıt sayısı
DELAY_BETWEEN_BATCHES = 0.1  # Batch'ler arası bekleme süresi (saniye)

def find_csv_file():
    """CSV dosyasını bul"""
    for file in os.listdir(DATA_PATH):
        if file.endswith('.csv'):
            return os.path.join(DATA_PATH, file)
    return None

def create_producer(max_retries=5):
    """Kafka Producer oluştur"""
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                batch_size=16384,
                linger_ms=10,
                buffer_memory=33554432
            )
            print(f"✓ Kafka Producer bağlantısı başarılı: {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka bağlantısı bekleniyor... (Deneme {attempt + 1}/{max_retries})")
            time.sleep(5)
    
    raise Exception("Kafka broker'a bağlanılamadı!")

def stream_data_to_kafka(producer, csv_path):
    """CSV verisini Kafka'ya stream et"""
    print(f"\nCSV dosyası okunuyor: {csv_path}")
    
    # CSV'yi chunk'lar halinde oku
    total_sent = 0
    chunk_num = 0
    
    for chunk in pd.read_csv(csv_path, chunksize=BATCH_SIZE):
        chunk_num += 1
        records = chunk.to_dict('records')
        
        for i, record in enumerate(records):
            # Her kayıt için unique key oluştur
            key = record.get('ID', f"record_{total_sent + i}")
            
            # Kafka'ya gönder
            producer.send(
                topic=KAFKA_TOPIC,
                key=str(key),
                value=record
            )
        
        total_sent += len(records)
        
        # İlerleme göster
        if chunk_num % 10 == 0:
            print(f"  Gönderilen kayıt sayısı: {total_sent:,}")
        
        # Batch'ler arası kısa bekleme
        time.sleep(DELAY_BETWEEN_BATCHES)
    
    # Tüm mesajların gönderildiğinden emin ol
    producer.flush()
    
    return total_sent

def main():
    print("=" * 60)
    print("KAFKA PRODUCER - US ACCIDENTS DATA STREAMING")
    print("=" * 60)
    print(f"Kafka Server: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print("-" * 60)
    
    # CSV dosyasını bul
    csv_path = find_csv_file()
    if not csv_path:
        print(f"HATA: {DATA_PATH} dizininde CSV dosyası bulunamadı!")
        print("Önce veri setini indirin: python scripts/download_data.py")
        sys.exit(1)
    
    # Dosya boyutunu kontrol et
    file_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
    print(f"CSV Dosyası: {os.path.basename(csv_path)} ({file_size_mb:.2f} MB)")
    
    # Producer oluştur
    try:
        producer = create_producer()
    except Exception as e:
        print(f"HATA: {str(e)}")
        print("\nKafka servisinin çalıştığından emin olun:")
        print("  docker-compose up -d kafka")
        sys.exit(1)
    
    # Veriyi stream et
    start_time = time.time()
    try:
        total_records = stream_data_to_kafka(producer, csv_path)
        elapsed_time = time.time() - start_time
        
        print("\n" + "=" * 60)
        print("STREAMING TAMAMLANDI!")
        print(f"Toplam kayıt: {total_records:,}")
        print(f"Geçen süre: {elapsed_time:.2f} saniye")
        print(f"Hız: {total_records / elapsed_time:.2f} kayıt/saniye")
        print("=" * 60)
        
    except KeyboardInterrupt:
        print("\n\nStreaming kullanıcı tarafından durduruldu.")
    finally:
        producer.close()
    
    print("\nSonraki adım: Spark Streaming'i çalıştırın")
    print("  python scripts/spark_streaming.py")

if __name__ == "__main__":
    main()
