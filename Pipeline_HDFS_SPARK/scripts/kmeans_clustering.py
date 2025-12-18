#!/usr/bin/env python3
"""
K-Means Kümeleme Script'i
US Accidents veri seti üzerinde kümeleme analizi
"""

import os
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans as SparkKMeans
from pyspark.ml.evaluation import ClusteringEvaluator

from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler as SklearnScaler
from sklearn.metrics import silhouette_score, calinski_harabasz_score, davies_bouldin_score
from sklearn.decomposition import PCA

# Konfigürasyon
HDFS_CLEANED_PATH = 'hdfs://namenode:9000/user/bigdata/accidents_cleaned'
RESULTS_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "results")
VIZ_PATH = os.path.join(RESULTS_PATH, "visualizations")

def create_spark_session():
    """Spark Session oluştur"""
    spark = SparkSession.builder \
        .appName("US_Accidents_KMeans_Clustering") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def load_and_prepare_data(spark):
    """Veri yükle ve hazırla"""
    print("Veri yükleniyor...")
    df = spark.read.parquet(HDFS_CLEANED_PATH)
    
    # Kümeleme için özellikler
    feature_columns = [
        'Start_Lat', 'Start_Lng', 'Temperature_F', 'Humidity_pct',
        'Pressure_in', 'Visibility_mi', 'Wind_Speed_mph', 'Hour',
        'DayOfWeek', 'Traffic_Feature_Count'
    ]
    
    # Mevcut sütunları kontrol et
    existing_features = [c for c in feature_columns if c in df.columns]
    
    # Seçili sütunlar + Severity (analiz için)
    columns_to_select = existing_features + ['Severity']
    df_selected = df.select([col(c) for c in columns_to_select if c in df.columns])
    
    # Null değerleri kaldır
    df_clean = df_selected.dropna()
    
    print(f"✓ Toplam kayıt: {df_clean.count():,}")
    print(f"✓ Özellik sayısı: {len(existing_features)}")
    
    return df_clean, existing_features

def spark_to_pandas_sample(df, sample_size=50000):
    """Spark DataFrame'i pandas'a çevir (örneklem al)"""
    total = df.count()
    
    if total > sample_size:
        fraction = sample_size / total
        df_sampled = df.sample(fraction=fraction, seed=42)
        print(f"Örneklem alındı: {sample_size:,} kayıt ({fraction*100:.2f}%)")
    else:
        df_sampled = df
        print(f"Tüm veri kullanılıyor: {total:,} kayıt")
    
    return df_sampled.toPandas()

def find_optimal_k_elbow(X_scaled, k_range=range(2, 11)):
    """Elbow yöntemi ile optimal k bul"""
    print("\nElbow yöntemi ile optimal k aranıyor...")
    
    inertias = []
    silhouette_scores = []
    
    for k in k_range:
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        kmeans.fit(X_scaled)
        
        inertias.append(kmeans.inertia_)
        
        if k > 1:
            sil_score = silhouette_score(X_scaled, kmeans.labels_)
            silhouette_scores.append(sil_score)
            print(f"  k={k}: Inertia={kmeans.inertia_:.2f}, Silhouette={sil_score:.4f}")
        else:
            silhouette_scores.append(0)
            print(f"  k={k}: Inertia={kmeans.inertia_:.2f}")
    
    # En iyi k (silhouette score'a göre)
    best_k_idx = np.argmax(silhouette_scores[1:]) + 2  # k=2'den başlıyor
    print(f"\n✓ Optimal k = {best_k_idx} (Silhouette: {max(silhouette_scores):.4f})")
    
    return best_k_idx, list(k_range), inertias, silhouette_scores

def train_kmeans(X_scaled, n_clusters):
    """K-Means modeli eğit"""
    print(f"\nK-Means modeli eğitiliyor (k={n_clusters})...")
    
    kmeans = KMeans(
        n_clusters=n_clusters,
        random_state=42,
        n_init=10,
        max_iter=300
    )
    
    cluster_labels = kmeans.fit_predict(X_scaled)
    
    print(f"✓ Model eğitimi tamamlandı")
    print(f"  Iterasyon sayısı: {kmeans.n_iter_}")
    print(f"  Inertia: {kmeans.inertia_:.2f}")
    
    return kmeans, cluster_labels

def calculate_clustering_metrics(X_scaled, labels):
    """Kümeleme metriklerini hesapla"""
    print("\n" + "=" * 60)
    print("KÜMELEME METRİKLERİ")
    print("=" * 60)
    
    metrics = {}
    
    # Silhouette Score (-1 ile 1 arası, yüksek = iyi)
    metrics['silhouette_score'] = silhouette_score(X_scaled, labels)
    print(f"\nSilhouette Score: {metrics['silhouette_score']:.4f}")
    print("  (1'e yakın = iyi ayrılmış kümeler)")
    
    # Calinski-Harabasz Index (yüksek = iyi)
    metrics['calinski_harabasz'] = calinski_harabasz_score(X_scaled, labels)
    print(f"\nCalinski-Harabasz Index: {metrics['calinski_harabasz']:.2f}")
    print("  (Yüksek = iyi tanımlı kümeler)")
    
    # Davies-Bouldin Index (düşük = iyi)
    metrics['davies_bouldin'] = davies_bouldin_score(X_scaled, labels)
    print(f"\nDavies-Bouldin Index: {metrics['davies_bouldin']:.4f}")
    print("  (0'a yakın = iyi ayrılmış kümeler)")
    
    return metrics

def analyze_clusters(pdf, labels, feature_columns):
    """Küme analizi"""
    print("\n" + "=" * 60)
    print("KÜME ANALİZİ")
    print("=" * 60)
    
    pdf['Cluster'] = labels
    
    # Küme dağılımı
    cluster_dist = pdf['Cluster'].value_counts().sort_index()
    print("\nKüme Dağılımı:")
    for cluster, count in cluster_dist.items():
        pct = count / len(pdf) * 100
        print(f"  Küme {cluster}: {count:,} ({pct:.2f}%)")
    
    # Her küme için ortalama özellik değerleri
    print("\nKüme Profilleri (Ortalamalar):")
    cluster_profiles = pdf.groupby('Cluster')[feature_columns].mean()
    print(cluster_profiles.round(2).to_string())
    
    # Severity dağılımı (eğer varsa)
    if 'Severity' in pdf.columns:
        print("\nKümelerdeki Severity Dağılımı:")
        severity_by_cluster = pd.crosstab(pdf['Cluster'], pdf['Severity'], normalize='index') * 100
        print(severity_by_cluster.round(2).to_string())
    
    return cluster_profiles

def plot_elbow_curve(k_values, inertias, silhouette_scores, optimal_k):
    """Elbow eğrisi görselleştir"""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    # Inertia (Elbow)
    ax1.plot(k_values, inertias, 'bo-', linewidth=2, markersize=8)
    ax1.axvline(x=optimal_k, color='r', linestyle='--', label=f'Optimal k={optimal_k}')
    ax1.set_xlabel('Küme Sayısı (k)', fontsize=12)
    ax1.set_ylabel('Inertia (WCSS)', fontsize=12)
    ax1.set_title('Elbow Yöntemi', fontsize=14)
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Silhouette Score
    ax2.plot(k_values, silhouette_scores, 'go-', linewidth=2, markersize=8)
    ax2.axvline(x=optimal_k, color='r', linestyle='--', label=f'Optimal k={optimal_k}')
    ax2.set_xlabel('Küme Sayısı (k)', fontsize=12)
    ax2.set_ylabel('Silhouette Score', fontsize=12)
    ax2.set_title('Silhouette Analizi', fontsize=14)
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    save_path = os.path.join(VIZ_PATH, 'kmeans_elbow_curve.png')
    plt.savefig(save_path, dpi=150)
    plt.close()
    print(f"✓ Elbow curve kaydedildi: {save_path}")

def plot_clusters_2d(X_scaled, labels, feature_columns):
    """2D küme görselleştirmesi (PCA)"""
    # PCA ile 2D'ye indir
    pca = PCA(n_components=2)
    X_pca = pca.fit_transform(X_scaled)
    
    plt.figure(figsize=(12, 8))
    
    unique_labels = np.unique(labels)
    colors = plt.cm.Set1(np.linspace(0, 1, len(unique_labels)))
    
    for label, color in zip(unique_labels, colors):
        mask = labels == label
        plt.scatter(X_pca[mask, 0], X_pca[mask, 1], 
                   c=[color], label=f'Küme {label}', 
                   alpha=0.6, s=20)
    
    plt.xlabel(f'PC1 ({pca.explained_variance_ratio_[0]*100:.1f}% varyans)', fontsize=12)
    plt.ylabel(f'PC2 ({pca.explained_variance_ratio_[1]*100:.1f}% varyans)', fontsize=12)
    plt.title('K-Means Kümeleme (PCA Projeksiyon)', fontsize=14)
    plt.legend(loc='best')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    save_path = os.path.join(VIZ_PATH, 'kmeans_clusters_2d.png')
    plt.savefig(save_path, dpi=150)
    plt.close()
    print(f"✓ 2D küme görselleştirmesi kaydedildi: {save_path}")
    
    return pca

def plot_cluster_profiles(cluster_profiles, feature_columns):
    """Küme profilleri radar chart"""
    # Normalize et (0-1 arasına)
    profiles_norm = (cluster_profiles - cluster_profiles.min()) / (cluster_profiles.max() - cluster_profiles.min())
    
    # Bar plot
    fig, axes = plt.subplots(2, 2, figsize=(14, 12))
    axes = axes.flatten()
    
    n_clusters = len(cluster_profiles)
    colors = plt.cm.Set1(np.linspace(0, 1, n_clusters))
    
    for idx, (cluster_idx, row) in enumerate(cluster_profiles.iterrows()):
        if idx < 4:  # Max 4 küme göster
            ax = axes[idx]
            bars = ax.bar(range(len(feature_columns)), row.values, color=colors[idx])
            ax.set_xticks(range(len(feature_columns)))
            ax.set_xticklabels(feature_columns, rotation=45, ha='right', fontsize=8)
            ax.set_title(f'Küme {cluster_idx} Profili', fontsize=12)
            ax.set_ylabel('Ortalama Değer')
            ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    
    save_path = os.path.join(VIZ_PATH, 'kmeans_cluster_profiles.png')
    plt.savefig(save_path, dpi=150)
    plt.close()
    print(f"✓ Küme profilleri kaydedildi: {save_path}")

def plot_cluster_distribution(pdf):
    """Küme dağılımı görselleştir"""
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # Küme boyutları
    cluster_counts = pdf['Cluster'].value_counts().sort_index()
    colors = plt.cm.Set1(np.linspace(0, 1, len(cluster_counts)))
    
    axes[0].bar(cluster_counts.index, cluster_counts.values, color=colors)
    axes[0].set_xlabel('Küme', fontsize=12)
    axes[0].set_ylabel('Kayıt Sayısı', fontsize=12)
    axes[0].set_title('Küme Boyutları', fontsize=14)
    for i, v in enumerate(cluster_counts.values):
        axes[0].text(i, v + 100, f'{v:,}', ha='center', fontsize=10)
    
    # Severity by Cluster (eğer varsa)
    if 'Severity' in pdf.columns:
        severity_cluster = pd.crosstab(pdf['Cluster'], pdf['Severity'])
        severity_cluster.plot(kind='bar', ax=axes[1], colormap='YlOrRd')
        axes[1].set_xlabel('Küme', fontsize=12)
        axes[1].set_ylabel('Kayıt Sayısı', fontsize=12)
        axes[1].set_title('Kümelerdeki Severity Dağılımı', fontsize=14)
        axes[1].legend(title='Severity')
        axes[1].tick_params(axis='x', rotation=0)
    
    plt.tight_layout()
    
    save_path = os.path.join(VIZ_PATH, 'kmeans_distribution.png')
    plt.savefig(save_path, dpi=150)
    plt.close()
    print(f"✓ Dağılım grafiği kaydedildi: {save_path}")

def plot_geographic_clusters(pdf):
    """Coğrafi küme dağılımı"""
    if 'Start_Lat' not in pdf.columns or 'Start_Lng' not in pdf.columns:
        print("Coğrafi sütunlar bulunamadı, harita oluşturulmadı.")
        return
    
    plt.figure(figsize=(14, 8))
    
    unique_clusters = pdf['Cluster'].unique()
    colors = plt.cm.Set1(np.linspace(0, 1, len(unique_clusters)))
    
    # Örneklem al (çok fazla nokta varsa)
    sample_size = min(10000, len(pdf))
    pdf_sample = pdf.sample(n=sample_size, random_state=42)
    
    for cluster, color in zip(sorted(unique_clusters), colors):
        mask = pdf_sample['Cluster'] == cluster
        plt.scatter(pdf_sample.loc[mask, 'Start_Lng'], 
                   pdf_sample.loc[mask, 'Start_Lat'],
                   c=[color], label=f'Küme {cluster}',
                   alpha=0.3, s=5)
    
    plt.xlabel('Boylam', fontsize=12)
    plt.ylabel('Enlem', fontsize=12)
    plt.title('Kazaların Coğrafi Kümelenmesi (ABD)', fontsize=14)
    plt.legend(loc='lower left')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    save_path = os.path.join(VIZ_PATH, 'kmeans_geographic.png')
    plt.savefig(save_path, dpi=150)
    plt.close()
    print(f"✓ Coğrafi kümeleme haritası kaydedildi: {save_path}")

def save_results(metrics, optimal_k, k_values, inertias, silhouette_scores, cluster_profiles):
    """Sonuçları JSON olarak kaydet"""
    results = {
        'model': 'K-Means',
        'optimal_k': optimal_k,
        'timestamp': datetime.now().isoformat(),
        'metrics': metrics,
        'elbow_analysis': {
            'k_values': k_values,
            'inertias': inertias,
            'silhouette_scores': silhouette_scores
        },
        'cluster_profiles': cluster_profiles.to_dict()
    }
    
    os.makedirs(RESULTS_PATH, exist_ok=True)
    
    save_path = os.path.join(RESULTS_PATH, 'kmeans_results.json')
    with open(save_path, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"✓ Sonuçlar kaydedildi: {save_path}")
    
    return results

def main():
    print("=" * 60)
    print("K-MEANS KÜMELEME - US ACCIDENTS")
    print("=" * 60)
    
    # Dizinleri oluştur
    os.makedirs(VIZ_PATH, exist_ok=True)
    
    # Spark session oluştur
    spark = create_spark_session()
    print("✓ Spark Session oluşturuldu")
    
    try:
        # Veri yükle
        df, feature_columns = load_and_prepare_data(spark)
        
        # Pandas'a çevir
        pdf = spark_to_pandas_sample(df, sample_size=50000)
        
        # X hazırla
        X = pdf[feature_columns]
        
        # Standardizasyon
        print("\nVeri standardize ediliyor...")
        scaler = SklearnScaler()
        X_scaled = scaler.fit_transform(X)
        print("✓ Standardizasyon tamamlandı")
        
        # Optimal k bul
        optimal_k, k_values, inertias, silhouette_scores = find_optimal_k_elbow(
            X_scaled, k_range=range(2, 11)
        )
        
        # Model eğit
        kmeans, labels = train_kmeans(X_scaled, optimal_k)
        
        # Metrikleri hesapla
        metrics = calculate_clustering_metrics(X_scaled, labels)
        
        # Küme analizi
        cluster_profiles = analyze_clusters(pdf, labels, feature_columns)
        
        # Görselleştirmeler
        print("\n" + "=" * 60)
        print("GÖRSELLEŞTİRMELER")
        print("=" * 60)
        
        plot_elbow_curve(k_values, inertias, silhouette_scores, optimal_k)
        plot_clusters_2d(X_scaled, labels, feature_columns)
        plot_cluster_profiles(cluster_profiles, feature_columns)
        plot_cluster_distribution(pdf)
        plot_geographic_clusters(pdf)
        
        # Sonuçları kaydet
        results = save_results(
            metrics, optimal_k, k_values, inertias, 
            silhouette_scores, cluster_profiles
        )
        
        print("\n" + "=" * 60)
        print("K-MEANS KÜMELEME TAMAMLANDI!")
        print("=" * 60)
        print("\nSonraki adım: MongoDB'ye export")
        print("  python scripts/mongodb_export.py")
        
        return results
        
    except Exception as e:
        print(f"\nHATA: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
