# Büyük Veri Pipeline - B Seçeneği

## Uçtan Uca Data Pipeline: HDFS + Hive + Kafka + Spark + MongoDB

Bu proje, Büyük Veri ve Analitiği dersi kapsamında hazırlanmış uçtan uca bir veri işleme pipeline'ıdır.

---

## 📋 İçindekiler

1. [Mimari](#mimari)
2. [Gereksinimler](#gereksinimler)
3. [Kurulum](#kurulum)
4. [Kullanım](#kullanım)
5. [Pipeline Bileşenleri](#pipeline-bileşenleri)
6. [Veri Seti](#veri-seti)
7. [Model Sonuçları](#model-sonuçları)
8. [Web Arayüzleri](#web-arayüzleri)

---

## 🏗️ Mimari

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          DATA PIPELINE ARCHITECTURE                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   ┌──────────┐     ┌──────────┐     ┌──────────────────┐                │
│   │  Kaggle  │────▶│  Kafka   │────▶│  Spark Streaming │                │
│   │   CSV    │     │ Producer │     │                  │                │
│   └──────────┘     └──────────┘     └────────┬─────────┘                │
│                                               │                          │
│                                               ▼                          │
│   ┌──────────────────────────────────────────────────────┐              │
│   │                      HDFS                             │              │
│   │                 (Parquet Format)                      │              │
│   └──────────────────────────┬───────────────────────────┘              │
│                              │                                           │
│                              ▼                                           │
│   ┌──────────────────────────────────────────────────────┐              │
│   │                    Apache Hive                        │              │
│   │               (Metadata & SQL Query)                  │              │
│   └──────────────────────────┬───────────────────────────┘              │
│                              │                                           │
│                              ▼                                           │
│   ┌──────────────────────────────────────────────────────┐              │
│   │                   PySpark ML                          │              │
│   │         ┌─────────────┬─────────────┐                │              │
│   │         │    kNN      │   K-Means   │                │              │
│   │         │ (Classify)  │  (Cluster)  │                │              │
│   │         └─────────────┴─────────────┘                │              │
│   └──────────────────────────┬───────────────────────────┘              │
│                              │                                           │
│                              ▼                                           │
│   ┌──────────────────────────────────────────────────────┐              │
│   │                     MongoDB                           │              │
│   │              (Results & Predictions)                  │              │
│   └──────────────────────────────────────────────────────┘              │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 💻 Gereksinimler

### Sistem Gereksinimleri
- **İşletim Sistemi:** Windows 10/11, macOS, Linux
- **RAM:** Minimum 8GB (16GB önerilir)
- **Disk:** En az 20GB boş alan
- **CPU:** 4+ çekirdek önerilir

### Yazılım Gereksinimleri
- Docker Desktop (v20.10+)
- Docker Compose (v2.0+)
- Python 3.8+ (lokal geliştirme için)
- Kaggle hesabı ve API anahtarı

---

## 🚀 Kurulum

### 1. Docker Desktop Kurulumu

**Windows:**
1. [Docker Desktop](https://www.docker.com/products/docker-desktop/) indirin
2. Kurulum sihirbazını takip edin
3. WSL 2 backend'i etkinleştirin (önerilir)
4. Docker Desktop'ı başlatın

**Doğrulama:**
```bash
docker --version
docker-compose --version
```

### 2. Kaggle API Anahtarı

1. [Kaggle](https://www.kaggle.com) hesabınıza giriş yapın
2. Profil > Settings > API > Create New Token
3. İndirilen `kaggle.json` dosyasını şuraya kopyalayın:
   - Windows: `C:\Users\<kullanıcı>\.kaggle\kaggle.json`
   - macOS/Linux: `~/.kaggle/kaggle.json`

### 3. Servisleri Başlatma

```bash
# Pipeline_B klasörüne gidin
cd Pipeline_B

# Docker container'ları başlatın
docker-compose up -d

# Servislerin durumunu kontrol edin
docker-compose ps
```

**İlk başlatma 5-10 dakika sürebilir (image'lar indirilirken).**

### 4. Python Bağımlılıkları (Lokal Geliştirme)

```bash
pip install -r scripts/requirements.txt
```

---

## 📖 Kullanım

### Otomatik Pipeline Çalıştırma

Tüm adımları sırayla çalıştırmak için:

```bash
python scripts/run_pipeline.py
```

### Manuel Adım Adım Çalıştırma

```bash
# 1. Veri setini indir
python scripts/download_data.py

# 2. (Opsiyonel) Kafka'ya stream et
python scripts/kafka_producer.py

# 3. HDFS'e yükle (batch mode)
python scripts/spark_streaming.py --batch

# 4. Hive tabloları oluştur
python scripts/hdfs_to_hive.py

# 5. Veri temizleme
python scripts/data_cleaning.py

# 6. kNN sınıflandırma
python scripts/knn_classification.py

# 7. K-Means kümeleme
python scripts/kmeans_clustering.py

# 8. MongoDB'ye export
python scripts/mongodb_export.py
```

### Jupyter Notebook

```bash
# Jupyter'e erişim
http://localhost:8888

# notebooks/analysis.ipynb dosyasını açın
```

---

## 🔧 Pipeline Bileşenleri

### 1. Veri Alma (Data Ingestion)
- **Script:** `download_data.py`
- **Kaynak:** Kaggle API
- **Format:** CSV

### 2. Stream İşleme
- **Script:** `kafka_producer.py`, `spark_streaming.py`
- **Kafka Topic:** `us_accidents`
- **Çıktı:** HDFS (Parquet)

### 3. Veri Depolama
- **HDFS Path:** `/user/bigdata/accidents`
- **Format:** Parquet (sıkıştırılmış)
- **Hive Database:** `bigdata_db`
- **Hive Table:** `us_accidents`

### 4. Veri Temizleme
- **Script:** `data_cleaning.py`
- **İşlemler:**
  - Eksik değer doldurma
  - Aykırı değer tespiti
  - Feature engineering
  - Standardizasyon

### 5. kNN Sınıflandırma
- **Script:** `knn_classification.py`
- **Hedef:** Severity (1-4)
- **Metrikler:**
  - Accuracy
  - Precision (Macro/Weighted)
  - Recall (Macro/Weighted)
  - F1-Score (Macro/Weighted)
  - AUC-ROC (Multi-class)

### 6. K-Means Kümeleme
- **Script:** `kmeans_clustering.py`
- **Metrikler:**
  - Silhouette Score
  - Calinski-Harabasz Index
  - Davies-Bouldin Index
- **Görselleştirmeler:**
  - Elbow Curve
  - 2D PCA Projeksiyon
  - Küme Profilleri
  - Coğrafi Dağılım

### 7. MongoDB Export
- **Script:** `mongodb_export.py`
- **Collections:**
  - `model_results`: Model sonuçları
  - `accidents_sample`: Örnek tahminler
  - `statistics`: Agregat istatistikler
  - `pipeline_metadata`: Pipeline bilgileri

---

## 📊 Veri Seti

### US Accidents Dataset

- **Kaynak:** [Kaggle](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents)
- **Boyut:** ~1.5GB (3+ milyon kayıt)
- **Dönem:** 2016-2023
- **Özellikler:** 47 sütun

**Ana Özellikler:**
| Özellik | Açıklama |
|---------|----------|
| Severity | Kaza ciddiyeti (1-4) |
| Start_Lat/Lng | Konum koordinatları |
| Temperature | Sıcaklık (°F) |
| Humidity | Nem (%) |
| Weather_Condition | Hava durumu |
| Traffic_Signal | Trafik ışığı varlığı |
| Hour/DayOfWeek | Zaman özellikleri |

---

## 📈 Model Sonuçları

### kNN Sınıflandırma (Beklenen Sonuçlar)

| Metrik | Değer |
|--------|-------|
| Accuracy | ~0.75-0.85 |
| Precision (Macro) | ~0.60-0.70 |
| Recall (Macro) | ~0.55-0.65 |
| F1-Score (Macro) | ~0.57-0.67 |
| AUC-ROC (Macro) | ~0.80-0.90 |

### K-Means Kümeleme (Beklenen Sonuçlar)

| Metrik | Değer |
|--------|-------|
| Optimal k | 4-6 |
| Silhouette Score | ~0.20-0.40 |
| Calinski-Harabasz | ~1000-5000 |
| Davies-Bouldin | ~1.0-2.0 |

---

## 🌐 Web Arayüzleri

| Servis | URL | Kullanıcı | Şifre |
|--------|-----|-----------|-------|
| HDFS NameNode | http://localhost:9870 | - | - |
| Spark Master | http://localhost:8080 | - | - |
| Spark Jobs | http://localhost:4040 | - | - |
| Hive Server | http://localhost:10002 | - | - |
| MongoDB Express | http://localhost:8082 | admin | admin123 |
| Jupyter Notebook | http://localhost:8888 | - | - |

---

## 📁 Dosya Yapısı

```
Pipeline_B/
├── docker-compose.yml          # Docker servisleri
├── README.md                   # Bu dosya
├── config/
│   ├── hive-site.xml          # Hive konfigürasyonu
│   └── environment.env        # Ortam değişkenleri
├── data/
│   └── raw/                   # Ham veri (CSV)
├── scripts/
│   ├── download_data.py       # Veri indirme
│   ├── kafka_producer.py      # Kafka producer
│   ├── spark_streaming.py     # Spark streaming
│   ├── hdfs_to_hive.py        # Hive tablo oluşturma
│   ├── data_cleaning.py       # Veri temizleme
│   ├── knn_classification.py  # kNN modeli
│   ├── kmeans_clustering.py   # K-Means modeli
│   ├── mongodb_export.py      # MongoDB export
│   ├── run_pipeline.py        # Pipeline orchestrator
│   └── requirements.txt       # Python bağımlılıkları
├── notebooks/
│   └── analysis.ipynb         # Analiz notebook'u
└── results/
    └── visualizations/        # Grafik çıktıları
```

---

## 🛑 Servisleri Durdurma

```bash
# Tüm container'ları durdur
docker-compose down

# Volume'ları da sil (veri kaybı!)
docker-compose down -v
```

---

## 🔍 Sorun Giderme

### Docker bellek hatası
```bash
# Docker Desktop ayarlarından RAM'i artırın (en az 8GB)
```

### Kafka bağlantı hatası
```bash
# Kafka'nın başlamasını bekleyin
docker-compose logs -f kafka
```

### HDFS yazma hatası
```bash
# HDFS'i kontrol edin
docker exec -it namenode hdfs dfs -ls /
```

### MongoDB bağlantı hatası
```bash
# MongoDB durumunu kontrol edin
docker exec -it mongodb mongosh --eval "db.adminCommand('ping')"
```

---

## 👥 Proje Ekibi

- **Öğrenci 1:** [İsim]
- **Öğrenci 2:** [İsim]

**Ders:** Büyük Veri ve Analitiği  
**Dönem:** 2024-2025 Güz

---

## 📚 Kaynaklar

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Hive Documentation](https://hive.apache.org/)
- [MongoDB Documentation](https://docs.mongodb.com/)
- [US Accidents Dataset](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents)
