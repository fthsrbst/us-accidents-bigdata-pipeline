# 🚗 Büyük Veri ve Analitiği - Dönem Projesi

## Uçtan Uca Data Pipeline: US Accidents Dataset

---

## 📋 Proje Özeti

Bu proje, **US Accidents** veri seti kullanılarak uçtan uca bir **Data Pipeline** tasarımı ve uygulamasını içermektedir. Proje kapsamında büyük veri teknolojileri (PySpark), NoSQL veritabanı (MongoDB) ve makine öğrenmesi algoritmaları (kNN, K-Means) kullanılmıştır.

---

## 🎯 Proje Amaçları

- ✅ Büyük veri ortamında veri akışı kurma
- ✅ Veri depolama ve ön işleme adımlarını birleştirme
- ✅ **kNN Sınıflandırma** algoritması uygulama
- ✅ **K-Means Kümeleme** algoritması uygulama
- ✅ Performans metrikleri hesaplama (Accuracy, Precision, Recall, F1-Score, AUC-ROC)
- ✅ Sonuçların MongoDB'ye yazılması

---

## 📊 Veri Seti

| Özellik | Değer |
|---------|-------|
| **Kaynak** | [Kaggle - US Accidents](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents) |
| **Boyut** | ~1.2 GB |
| **Kayıt Sayısı** | ~7.7 milyon kaza kaydı |
| **Özellik Sayısı** | 46 feature |
| **Zaman Aralığı** | Şubat 2016 - Mart 2023 |
| **Kapsam** | 49 ABD Eyaleti |

---

## 🛠️ Teknolojiler

| Teknoloji | Kullanım Amacı |
|-----------|----------------|
| **Google Colab** | Veri işleme ve analiz ortamı |
| **PySpark** | Büyük veri işleme |
| **MongoDB (Lokal)** | NoSQL veritabanı - sonuçların depolanması |
| **Python** | Programlama dili |
| **Scikit-learn** | ML algoritmaları |
| **Matplotlib/Seaborn** | Görselleştirme |

---

## 📁 Proje Yapısı

```
BuyukVeri/
│
├── BuyukVeri_Pipeline_Projesi.ipynb   # Ana Colab notebook
├── mongodb_local_import.py            # MongoDB'ye veri yükleme scripti
├── README.md                          # Bu dosya
│
└── mongodb_export/                    # Colab'dan indirilen dosyalar
    ├── knn_results.json               # kNN model sonuçları
    ├── kmeans_results.json            # K-Means sonuçları
    ├── us_accidents_raw.json          # Ham veri örneği
    ├── us_accidents_cleaned.json      # Temizlenmiş veri
    ├── us_accidents_clustered.json    # Kümelenmiş veri
    └── *.png                          # Görselleştirmeler
```

---

## 🚀 Kurulum ve Çalıştırma

### 📌 AŞAMA 1: Google Colab'da Analiz

#### Adım 1.1: Kaggle API Token Hazırlığı
1. [Kaggle.com](https://www.kaggle.com) → Settings → API → **Create New Token**
2. `kaggle.json` dosyasını indirin

#### Adım 1.2: Notebook'u Çalıştırma
1. `BuyukVeri_Pipeline_Projesi.ipynb` dosyasını [Google Colab](https://colab.research.google.com)'a yükleyin
2. Hücreleri sırasıyla çalıştırın
3. `kaggle.json` dosyasını yükleyin (istendiğinde)
4. Analiz tamamlandığında **ZIP dosyasını indirin**

---

### 📌 AŞAMA 2: Lokal MongoDB Kurulumu

#### Adım 2.1: MongoDB Kurulumu
1. [MongoDB Community Server](https://www.mongodb.com/try/download/community) indirin
2. Kurulumu tamamlayın
3. [MongoDB Compass](https://www.mongodb.com/try/download/compass) (GUI) indirin (opsiyonel)

#### Adım 2.2: MongoDB'yi Başlatma

**Windows:**
```bash
# Komut satırında (CMD veya PowerShell)
mongod
```

**Veya MongoDB Compass'ı açın** - otomatik olarak bağlanır.

---

### 📌 AŞAMA 3: Verileri MongoDB'ye Yükleme

#### Adım 3.1: Dosyaları Hazırlama
1. Colab'dan indirdiğiniz `bigdata_project_output.zip` dosyasını çıkartın
2. `mongodb_export` klasörünü `BuyukVeri` klasörüne kopyalayın

#### Adım 3.2: Python Script'i Çalıştırma

```bash
# Önce pymongo kütüphanesini kurun
pip install pymongo

# Script'i çalıştırın
python mongodb_local_import.py
```

#### Adım 3.3: Verileri Görüntüleme
1. **MongoDB Compass**'ı açın
2. `mongodb://localhost:27017` adresine bağlanın
3. `bigdata_project` veritabanını seçin
4. Koleksiyonları inceleyin

---

## 📊 Pipeline Akışı

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA PIPELINE AKIŞI                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  [GOOGLE COLAB]                                                 │
│  ═══════════════                                                │
│                                                                 │
│  1. VERİ ALMA          ──►  Kaggle API ile veri indirme        │
│        │                                                        │
│        ▼                                                        │
│  2. VERİ TEMİZLEME     ──►  PySpark ile ön işleme              │
│        │                    • Eksik değer analizi               │
│        │                    • Özellik seçimi                    │
│        │                    • Encoding                          │
│        ▼                                                        │
│  3. kNN SINIFLANDIRMA  ──►  Severity tahmini                   │
│        │                    • Train/Test split                  │
│        │                    • Model eğitimi                     │
│        │                    • Performans metrikleri             │
│        ▼                                                        │
│  4. K-MEANS KÜMELEME   ──►  Kaza kümeleme                      │
│        │                    • Elbow method                      │
│        │                    • Küme analizi                      │
│        │                    • Görselleştirme                    │
│        ▼                                                        │
│  5. JSON EXPORT        ──►  Sonuçları JSON olarak kaydet       │
│        │                                                        │
│        ▼                                                        │
│  ═══════════════════════════════════════════════════════════   │
│        │     ZIP dosyasını indir                                │
│        ▼                                                        │
│  [LOKAL BİLGİSAYAR]                                            │
│  ══════════════════                                             │
│        │                                                        │
│        ▼                                                        │
│  6. MONGODB IMPORT     ──►  JSON'ları MongoDB'ye yükle         │
│                             (mongodb_local_import.py)           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📈 Beklenen Çıktılar

### kNN Sınıflandırma

| Metrik | Açıklama |
|--------|----------|
| **Accuracy** | Genel doğruluk oranı |
| **Precision** | Pozitif tahminlerin doğruluğu |
| **Recall** | Gerçek pozitiflerin yakalama oranı |
| **F1-Score** | Precision ve Recall'un harmonik ortalaması |
| **AUC-ROC** | ROC eğrisi altındaki alan |

### K-Means Kümeleme

| Çıktı | Açıklama |
|-------|----------|
| **Elbow Grafiği** | Optimal k belirleme |
| **Küme Dağılımı** | Her kümedeki kayıt sayısı |
| **Coğrafi Görselleştirme** | Kümelerin harita üzerinde gösterimi |
| **Küme Karakteristikleri** | Her kümenin ortalama özellikleri |
| **Silhouette Score** | Kümeleme kalitesi |

---

## 🗄️ MongoDB Koleksiyonları

| Koleksiyon | İçerik |
|------------|--------|
| `us_accidents_raw` | Ham veri örneği (10,000 kayıt) |
| `us_accidents_cleaned` | Temizlenmiş veri (10,000 kayıt) |
| `us_accidents_clustered` | Kümelenmiş veri (10,000 kayıt) |
| `knn_results` | kNN model sonuçları ve metrikleri |
| `kmeans_results` | K-Means sonuçları ve küme merkezleri |

---

## 📊 Görselleştirmeler

Notebook çalıştırıldığında aşağıdaki görselleştirmeler üretilir:

1. **severity_distribution.png** - Kaza şiddeti dağılımı
2. **confusion_matrix.png** - kNN Confusion Matrix
3. **roc_curves.png** - Multi-class ROC eğrileri
4. **knn_k_accuracy.png** - k değerine göre accuracy
5. **elbow_method.png** - K-Means Elbow grafiği
6. **cluster_distribution.png** - Küme dağılımı
7. **geographic_clusters.png** - Coğrafi kümeleme haritası
8. **cluster_characteristics.png** - Küme karakteristikleri

---

## ⚠️ Önemli Notlar

- Veri seti büyük olduğundan, Colab'da bellek yönetimi için örnekleme yapılmaktadır
- MongoDB'nin çalışır durumda olduğundan emin olun
- İlk çalıştırmada veri indirme ~5-10 dakika sürebilir
- Tüm Colab analizi ~20-30 dakika sürebilir

---

## 🔧 Sorun Giderme

### MongoDB Bağlantı Hatası
```
❌ MongoDB bağlantı hatası
```
**Çözüm:** MongoDB servisinin çalıştığından emin olun:
```bash
# Windows
mongod

# Veya MongoDB Compass'ı açın
```

### pymongo Modül Hatası
```
ModuleNotFoundError: No module named 'pymongo'
```
**Çözüm:**
```bash
pip install pymongo
```

### JSON Dosya Bulunamadı
```
⚠️ Dosya bulunamadı
```
**Çözüm:** ZIP dosyasını doğru klasöre çıkarttığınızdan emin olun.

---

## 📚 Kaynaklar

- [US Accidents Dataset](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [MongoDB Documentation](https://docs.mongodb.com/)
- [Scikit-learn Documentation](https://scikit-learn.org/stable/)

---

## 👨‍💻 Geliştirici

**Büyük Veri ve Analitiği - Dönem Projesi**

---

*Bu proje eğitim amaçlı hazırlanmıştır.*

