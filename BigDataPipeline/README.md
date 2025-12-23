# 🚗 Büyük Veri ve Analitiği - Dönem Projesi

## Uçtan Uca Data Pipeline: US Accidents Dataset

### Docker-Based Big Data Architecture

---

## 📋 Proje Özeti

Bu proje, **US Accidents** veri seti (3GB+) kullanılarak Docker container'ları üzerinde uçtan uca bir **Data Pipeline** tasarımı ve uygulamasını içermektedir.

**Kullanılan Teknolojiler:**

- 🐳 **Docker & Docker Compose** - Container orkestrasyon
- 📦 **HDFS** - Dağıtık dosya sistemi
- 🐝 **Apache Hive** - SQL sorgu motoru
- 📨 **Apache Kafka** - Streaming mesaj kuyruğu
- ⚡ **Apache Spark** - Dağıtık veri işleme
- 🍃 **MongoDB** - NoSQL veritabanı

---

## 🏗️ Sistem Mimarisi

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA PIPELINE AKIŞI                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  DATA INGESTION                                           │  │
│  │  CSV → Kafka Producer → HDFS                             │  │
│  └──────────────────────────────────────────────────────────┘  │
│                              ↓                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  DATA STORAGE                                             │  │
│  │  HDFS → Hive External Tables                             │  │
│  └──────────────────────────────────────────────────────────┘  │
│                              ↓                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  DATA PROCESSING (Spark)                                  │  │
│  │  1. Data Cleaning                                         │  │
│  │  2. kNN Classification                                    │  │
│  │  3. Random Forest Classification                          │  │
│  │  4. K-Means Clustering                                    │  │
│  └──────────────────────────────────────────────────────────┘  │
│                              ↓                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  DATA STORAGE                                             │  │
│  │  Results → MongoDB Collections                            │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📊 Veri Seti

| Özellik            | Değer                                                                               |
| ------------------ | ----------------------------------------------------------------------------------- |
| **Kaynak**         | [Kaggle - US Accidents](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents) |
| **Boyut**          | ~3 GB                                                                               |
| **Kayıt Sayısı**   | ~7.7 milyon kaza kaydı                                                              |
| **Özellik Sayısı** | 46 feature                                                                          |
| **Zaman Aralığı**  | Şubat 2016 - Mart 2023                                                              |

---

## 🐳 Docker Services

| Service            | Port         | Açıklama              |
| ------------------ | ------------ | --------------------- |
| **NameNode**       | 9870, 8020   | HDFS Name Node        |
| **DataNode**       | 9864         | HDFS Data Node        |
| **Hive Metastore** | 9083         | Hive metadata servisi |
| **HiveServer2**    | 10000, 10002 | Hive SQL servisi      |
| **Zookeeper**      | 2181         | Kafka koordinasyonu   |
| **Kafka**          | 9092, 29092  | Mesaj kuyruğu         |
| **Spark Master**   | 8080, 7077   | Spark cluster master  |
| **Spark Worker**   | 8081         | Spark işçi node       |
| **MongoDB**        | 27017        | NoSQL veritabanı      |
| **Jupyter**        | 8888         | Geliştirme ortamı     |

---

## 📁 Proje Yapısı

```
BigDataPipeline/
├── docker-compose.yml          # Docker orkestrasyon
├── README.md                   # Bu dosya
│
├── config/                     # Konfigürasyon dosyaları
│   ├── hadoop.env              # Hadoop environment
│   ├── hive-site.xml           # Hive konfigürasyonu
│   └── spark-defaults.conf     # Spark ayarları
│
├── hive/                       # Hive scriptleri
│   └── create_tables.hql       # Tablo tanımları
│
├── spark/                      # Spark işleri
│   ├── data_cleaning.py        # Veri temizleme
│   ├── knn_classification.py   # kNN sınıflandırma
│   ├── random_forest.py        # Random Forest
│   └── kmeans_clustering.py    # K-Means kümeleme
│
├── scripts/                    # Yardımcı scriptler
│   ├── kafka_producer.py       # Kafka producer
│   ├── hdfs_upload.sh          # HDFS yükleme
│   ├── mongo-init.js           # MongoDB init
│   └── run_pipeline.sh         # Pipeline çalıştırma
│
├── data/                       # Veri dizini (CSV buraya)
└── output/                     # Çıktı dizini
    ├── visualizations/         # Görseller
    └── models/                 # Model dosyaları
```

---

## 🚀 Kurulum ve Çalıştırma

### Ön Gereksinimler

- Docker Desktop (Windows/Mac) veya Docker Engine (Linux)
- En az 16GB RAM
- En az 20GB boş disk alanı

### Adım 1: Veri Setini Hazırlama

```powershell
# CSV dosyasını data klasörüne kopyalayın
copy "C:\Users\fatih\OneDrive\Masaüstü\BuyukVeri\Pipeline_Collab\US_Accidents_March23.csv" ".\data\"
```

### Adım 2: Docker Servislerini Başlatma

```powershell
# Proje dizinine gidin
cd BigDataPipeline

# Tüm servisleri başlatın
docker-compose up -d

# Servislerin durumunu kontrol edin
docker-compose ps
```

### Adım 3: HDFS'e Veri Yükleme

```powershell
# HDFS upload scriptini çalıştırın
docker exec namenode bash /scripts/hdfs_upload.sh
```

### Adım 4: Hive Tablolarını Oluşturma

```powershell
# Hive tablolarını oluşturun
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /opt/hive/scripts/create_tables.hql
```

### Adım 5: Spark Pipeline'ı Çalıştırma

```powershell
# Tüm pipeline'ı çalıştırın
docker exec spark-master bash /opt/spark-apps/../scripts/run_pipeline.sh

# Veya tek tek:
# 1. Data Cleaning
docker exec spark-master spark-submit --master spark://spark-master:7077 --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 /opt/spark-apps/data_cleaning.py

# 2. kNN Classification
docker exec spark-master spark-submit --master spark://spark-master:7077 --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 /opt/spark-apps/knn_classification.py

# 3. Random Forest
docker exec spark-master spark-submit --master spark://spark-master:7077 --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 /opt/spark-apps/random_forest.py

# 4. K-Means Clustering
docker exec spark-master spark-submit --master spark://spark-master:7077 --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 /opt/spark-apps/kmeans_clustering.py
```

---

## 📈 Pipeline Bileşenleri

### 4.1 Veri Alma (Data Ingestion)

- Kafka Producer ile CSV'den streaming veri akışı
- HDFS'e batch veri yükleme

### 4.2 Veri Depolama (Data Storage)

- HDFS üzerinde ham veri depolama
- Hive external table ile SQL erişimi

### 4.3 Veri Temizleme (Data Cleaning)

- Eksik değer analizi ve doldurma
- Feature selection (20+ özellik)
- Temporal feature extraction (saat, gün, ay)
- Kategorik encoding

### 4.4 kNN Sınıflandırma

- Distributed kNN implementasyonu
- Optimal k seçimi
- **Metrikler:** Accuracy, Precision, Recall, F1-Score

### 4.5 Random Forest Sınıflandırma

- 100 ağaçlı ensemble model
- Feature importance analizi
- **Metrikler:** Accuracy, Precision, Recall, F1-Score, AUC-ROC

### 4.6 K-Means Kümeleme

- Elbow method ile optimal K
- Silhouette score hesaplama
- Coğrafi küme görselleştirmesi

### 4.7 MongoDB Output

- Ham veri örneği: `us_accidents_raw`
- Temizlenmiş veri: `us_accidents_cleaned`
- Kümelenmiş veri: `us_accidents_clustered`
- Model sonuçları: `knn_results`, `random_forest_results`, `kmeans_results`

---

## 📊 Beklenen Çıktılar

### Görselleştirmeler

- `kmeans_elbow_method.png` - Elbow grafiği
- `kmeans_geographic_clusters.png` - Coğrafi kümeleme haritası
- `kmeans_cluster_distribution.png` - Küme dağılımı
- `rf_feature_importance.png` - Feature importance
- `rf_confusion_matrix.png` - Confusion matrix

### JSON Çıktıları

- `knn_results.json` - kNN model metrikleri
- `random_forest_results.json` - RF metrikleri
- `kmeans_results.json` - Kümeleme sonuçları

---

## 🌐 Web Arayüzleri

| Servis           | URL                   |
| ---------------- | --------------------- |
| Spark Master UI  | http://localhost:8080 |
| HDFS NameNode UI | http://localhost:9870 |
| Jupyter Notebook | http://localhost:8888 |

---

## 🔧 Sorun Giderme

### Docker Bellek Hatası

```powershell
# Docker Desktop ayarlarından bellek limitini artırın (en az 12GB)
```

### MongoDB Bağlantı Hatası

```powershell
# MongoDB container'ının çalıştığını kontrol edin
docker-compose ps mongodb
docker-compose logs mongodb
```

### Spark Job Hatası

```powershell
# Spark loglarını kontrol edin
docker-compose logs spark-master
docker-compose logs spark-worker
```

---

## 📚 Kaynaklar

- [US Accidents Dataset](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Hive Documentation](https://hive.apache.org/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [MongoDB Documentation](https://docs.mongodb.com/)

---

## 👨‍💻 Proje Bilgileri

**Ders:** Büyük Veri ve Analitiği - Dönem Projesi

---

_Bu proje eğitim amaçlı hazırlanmıştır._
