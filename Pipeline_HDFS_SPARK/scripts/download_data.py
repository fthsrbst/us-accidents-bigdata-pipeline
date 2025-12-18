#!/usr/bin/env python3
"""
US Accidents Veri Seti İndirme Script'i
Kaggle API kullanarak veri setini indirir
"""

import os
import sys
import zipfile
import subprocess

# Kaggle dataset bilgileri
DATASET_NAME = "sobhanmoosavi/us-accidents"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")

def check_kaggle_credentials():
    """Kaggle credentials kontrolü"""
    kaggle_dir = os.path.expanduser("~/.kaggle")
    kaggle_json = os.path.join(kaggle_dir, "kaggle.json")
    
    if not os.path.exists(kaggle_json):
        print("=" * 60)
        print("HATA: Kaggle API anahtarı bulunamadı!")
        print("=" * 60)
        print("\nLütfen aşağıdaki adımları takip edin:")
        print("1. https://www.kaggle.com adresine gidin")
        print("2. Hesabınıza giriş yapın")
        print("3. Sağ üstten profil ikonuna tıklayın -> Settings")
        print("4. API bölümünden 'Create New Token' butonuna tıklayın")
        print("5. İndirilen kaggle.json dosyasını şuraya kopyalayın:")
        print(f"   {kaggle_dir}")
        print("\nWindows için:")
        print(f"   mkdir {kaggle_dir}")
        print(f"   copy kaggle.json {kaggle_json}")
        return False
    
    # Dosya izinlerini ayarla (Unix sistemler için)
    if os.name != 'nt':
        os.chmod(kaggle_json, 0o600)
    
    return True

def install_kaggle():
    """Kaggle kütüphanesini yükle"""
    try:
        import kaggle
        print("✓ Kaggle kütüphanesi zaten yüklü")
    except ImportError:
        print("Kaggle kütüphanesi yükleniyor...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "kaggle"])
        print("✓ Kaggle kütüphanesi yüklendi")

def download_dataset():
    """Veri setini indir"""
    from kaggle.api.kaggle_api_extended import KaggleApi
    
    # Output dizinini oluştur
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Kaggle API'yi başlat
    api = KaggleApi()
    api.authenticate()
    
    print(f"\nVeri seti indiriliyor: {DATASET_NAME}")
    print(f"Hedef dizin: {OUTPUT_DIR}")
    print("-" * 60)
    
    # Dataset'i indir
    api.dataset_download_files(
        dataset=DATASET_NAME,
        path=OUTPUT_DIR,
        unzip=True
    )
    
    print("\n✓ Veri seti başarıyla indirildi!")
    
    # İndirilen dosyaları listele
    print("\nİndirilen dosyalar:")
    for file in os.listdir(OUTPUT_DIR):
        file_path = os.path.join(OUTPUT_DIR, file)
        size_mb = os.path.getsize(file_path) / (1024 * 1024)
        print(f"  - {file} ({size_mb:.2f} MB)")

def main():
    print("=" * 60)
    print("US ACCIDENTS VERİ SETİ İNDİRME ARACI")
    print("=" * 60)
    
    # Kaggle kütüphanesini kontrol et/yükle
    install_kaggle()
    
    # Credentials kontrolü
    if not check_kaggle_credentials():
        sys.exit(1)
    
    # Veri setini indir
    try:
        download_dataset()
    except Exception as e:
        print(f"\nHATA: Veri seti indirilemedi!")
        print(f"Detay: {str(e)}")
        sys.exit(1)
    
    print("\n" + "=" * 60)
    print("İndirme tamamlandı!")
    print("Sonraki adım: Kafka Producer'ı çalıştırın")
    print("  python scripts/kafka_producer.py")
    print("=" * 60)

if __name__ == "__main__":
    main()
