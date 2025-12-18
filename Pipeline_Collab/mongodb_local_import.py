"""
🗄️ MongoDB Lokal Import Script
================================
Bu script, Google Colab'dan indirilen JSON dosyalarını
lokal MongoDB veritabanına yükler.

Kullanım:
1. MongoDB'yi başlatın (mongod)
2. Bu scripti çalıştırın: python mongodb_local_import.py

Gereksinimler:
- pip install pymongo
"""

import json
import os
from datetime import datetime
from pymongo import MongoClient

# ============================================
# YAPILANDIRMA
# ============================================

# MongoDB bağlantı ayarları (lokal)
MONGODB_HOST = "localhost"
MONGODB_PORT = 27017
DATABASE_NAME = "bigdata_project"

# JSON dosyalarının bulunduğu klasör
# ZIP'i çıkarttığınız klasörü belirtin
JSON_FOLDER = "mongodb_export"  # veya tam yol: "C:/Users/fatih/Downloads/mongodb_export"

# ============================================
# FONKSİYONLAR
# ============================================

def connect_mongodb():
    """MongoDB'ye bağlan"""
    try:
        client = MongoClient(MONGODB_HOST, MONGODB_PORT)
        # Bağlantıyı test et
        client.admin.command('ping')
        print(f"✅ MongoDB bağlantısı başarılı! ({MONGODB_HOST}:{MONGODB_PORT})")
        return client
    except Exception as e:
        print(f"❌ MongoDB bağlantı hatası: {e}")
        print("\n💡 MongoDB'nin çalıştığından emin olun:")
        print("   - Windows: 'mongod' komutunu çalıştırın")
        print("   - Veya MongoDB Compass'ı açın")
        return None

def load_json_file(filepath):
    """JSON dosyasını yükle"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except Exception as e:
        print(f"❌ Dosya okuma hatası ({filepath}): {e}")
        return None

def import_to_mongodb(db, collection_name, data, is_single_doc=False):
    """Veriyi MongoDB koleksiyonuna aktar"""
    try:
        collection = db[collection_name]
        
        # Var olan koleksiyonu temizle
        collection.drop()
        
        if is_single_doc:
            # Tek döküman (sonuçlar için)
            collection.insert_one(data)
            count = 1
        else:
            # Çoklu döküman (veri setleri için)
            if isinstance(data, list):
                collection.insert_many(data)
                count = len(data)
            else:
                collection.insert_one(data)
                count = 1
        
        print(f"   ✅ '{collection_name}': {count:,} döküman eklendi")
        return True
    except Exception as e:
        print(f"   ❌ '{collection_name}' hatası: {e}")
        return False

def main():
    """Ana fonksiyon"""
    print("="*60)
    print("     🗄️ MongoDB Lokal Import Script")
    print("="*60)
    print(f"\n📅 Tarih: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📁 JSON Klasörü: {JSON_FOLDER}")
    print(f"🗄️ Hedef Veritabanı: {DATABASE_NAME}")
    print()
    
    # MongoDB bağlantısı
    client = connect_mongodb()
    if client is None:
        return
    
    db = client[DATABASE_NAME]
    
    # Import edilecek dosyalar
    import_files = [
        {"file": "knn_results.json", "collection": "knn_results", "single": True},
        {"file": "kmeans_results.json", "collection": "kmeans_results", "single": True},
        {"file": "us_accidents_raw.json", "collection": "us_accidents_raw", "single": False},
        {"file": "us_accidents_cleaned.json", "collection": "us_accidents_cleaned", "single": False},
        {"file": "us_accidents_clustered.json", "collection": "us_accidents_clustered", "single": False},
    ]
    
    print("\n📥 Dosyalar import ediliyor...")
    print("-"*60)
    
    success_count = 0
    for item in import_files:
        filepath = os.path.join(JSON_FOLDER, item["file"])
        
        if not os.path.exists(filepath):
            print(f"   ⚠️ Dosya bulunamadı: {item['file']}")
            continue
        
        data = load_json_file(filepath)
        if data is None:
            continue
        
        if import_to_mongodb(db, item["collection"], data, item["single"]):
            success_count += 1
    
    # Sonuç özeti
    print("\n" + "="*60)
    print("                    📊 ÖZET")
    print("="*60)
    
    print(f"\n✅ Başarılı: {success_count}/{len(import_files)} koleksiyon")
    
    print("\n📁 MongoDB Koleksiyonları:")
    for collection_name in db.list_collection_names():
        count = db[collection_name].count_documents({})
        print(f"   • {collection_name}: {count:,} döküman")
    
    print("\n💡 Verileri görüntülemek için:")
    print("   - MongoDB Compass'ı açın")
    print(f"   - '{DATABASE_NAME}' veritabanına gidin")
    print("   - Koleksiyonları inceleyin")
    
    # Bağlantıyı kapat
    client.close()
    print("\n✅ MongoDB bağlantısı kapatıldı.")
    print("="*60)

if __name__ == "__main__":
    main()
