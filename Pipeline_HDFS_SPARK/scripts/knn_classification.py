#!/usr/bin/env python3
"""
kNN Sınıflandırma Script'i
US Accidents veri seti üzerinde Severity tahmini
"""

import os
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml import Pipeline

from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler as SklearnScaler
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    confusion_matrix, classification_report, roc_curve, auc,
    roc_auc_score
)
from sklearn.preprocessing import label_binarize

# Konfigürasyon
HDFS_CLEANED_PATH = 'hdfs://namenode:9000/user/bigdata/accidents_cleaned'
RESULTS_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "results")
VIZ_PATH = os.path.join(RESULTS_PATH, "visualizations")

def create_spark_session():
    """Spark Session oluştur"""
    spark = SparkSession.builder \
        .appName("US_Accidents_kNN_Classification") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def load_and_prepare_data(spark):
    """Veri yükle ve hazırla"""
    print("Veri yükleniyor...")
    df = spark.read.parquet(HDFS_CLEANED_PATH)
    
    # Özellikleri seç
    feature_columns = [
        'Start_Lat', 'Start_Lng', 'Temperature_F', 'Humidity_pct',
        'Pressure_in', 'Visibility_mi', 'Wind_Speed_mph',
        'Hour', 'DayOfWeek', 'Month', 'IsWeekend', 'Traffic_Feature_Count'
    ]
    
    # Mevcut sütunları kontrol et
    existing_features = [c for c in feature_columns if c in df.columns]
    
    # Hedef değişken ve özellikler
    df_selected = df.select(['Severity'] + existing_features)
    
    # Null değerleri kaldır
    df_clean = df_selected.dropna()
    
    print(f"✓ Toplam kayıt: {df_clean.count():,}")
    print(f"✓ Özellik sayısı: {len(existing_features)}")
    
    return df_clean, existing_features

def spark_to_pandas_sample(df, sample_size=100000):
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

def train_knn_model(X_train, X_test, y_train, y_test, k=5):
    """kNN modeli eğit"""
    print(f"\nkNN modeli eğitiliyor (k={k})...")
    
    # Standardizasyon
    scaler = SklearnScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # kNN modeli
    knn = KNeighborsClassifier(n_neighbors=k, n_jobs=-1)
    knn.fit(X_train_scaled, y_train)
    
    # Tahmin
    y_pred = knn.predict(X_test_scaled)
    y_prob = knn.predict_proba(X_test_scaled)
    
    return knn, scaler, y_pred, y_prob

def find_optimal_k(X_train, y_train, k_range=range(1, 21)):
    """Optimal k değerini bul"""
    print("\nOptimal k değeri aranıyor...")
    
    scaler = SklearnScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    
    scores = []
    for k in k_range:
        knn = KNeighborsClassifier(n_neighbors=k, n_jobs=-1)
        cv_scores = cross_val_score(knn, X_train_scaled, y_train, cv=5, scoring='accuracy')
        scores.append({
            'k': k,
            'mean_accuracy': cv_scores.mean(),
            'std': cv_scores.std()
        })
        print(f"  k={k}: Accuracy={cv_scores.mean():.4f} (+/- {cv_scores.std():.4f})")
    
    # En iyi k
    best = max(scores, key=lambda x: x['mean_accuracy'])
    print(f"\n✓ Optimal k = {best['k']} (Accuracy: {best['mean_accuracy']:.4f})")
    
    return best['k'], scores

def calculate_metrics(y_test, y_pred, y_prob, classes):
    """Performans metriklerini hesapla"""
    print("\n" + "=" * 60)
    print("PERFORMANS METRİKLERİ")
    print("=" * 60)
    
    metrics = {}
    
    # Temel metrikler
    metrics['accuracy'] = accuracy_score(y_test, y_pred)
    metrics['precision_macro'] = precision_score(y_test, y_pred, average='macro', zero_division=0)
    metrics['precision_weighted'] = precision_score(y_test, y_pred, average='weighted', zero_division=0)
    metrics['recall_macro'] = recall_score(y_test, y_pred, average='macro', zero_division=0)
    metrics['recall_weighted'] = recall_score(y_test, y_pred, average='weighted', zero_division=0)
    metrics['f1_macro'] = f1_score(y_test, y_pred, average='macro', zero_division=0)
    metrics['f1_weighted'] = f1_score(y_test, y_pred, average='weighted', zero_division=0)
    
    print(f"\nAccuracy: {metrics['accuracy']:.4f}")
    print(f"\nPrecision (Macro): {metrics['precision_macro']:.4f}")
    print(f"Precision (Weighted): {metrics['precision_weighted']:.4f}")
    print(f"\nRecall (Macro): {metrics['recall_macro']:.4f}")
    print(f"Recall (Weighted): {metrics['recall_weighted']:.4f}")
    print(f"\nF1-Score (Macro): {metrics['f1_macro']:.4f}")
    print(f"F1-Score (Weighted): {metrics['f1_weighted']:.4f}")
    
    # AUC-ROC (multiclass)
    try:
        y_test_bin = label_binarize(y_test, classes=classes)
        
        if y_test_bin.shape[1] > 1:
            # Her sınıf için AUC
            auc_scores = {}
            for i, cls in enumerate(classes):
                if i < y_prob.shape[1]:
                    fpr, tpr, _ = roc_curve(y_test_bin[:, i], y_prob[:, i])
                    auc_scores[f'class_{cls}'] = auc(fpr, tpr)
            
            metrics['auc_per_class'] = auc_scores
            metrics['auc_macro'] = roc_auc_score(y_test_bin, y_prob, average='macro', multi_class='ovr')
            metrics['auc_weighted'] = roc_auc_score(y_test_bin, y_prob, average='weighted', multi_class='ovr')
            
            print(f"\nAUC-ROC (Macro): {metrics['auc_macro']:.4f}")
            print(f"AUC-ROC (Weighted): {metrics['auc_weighted']:.4f}")
            
            print("\nSınıf bazında AUC:")
            for cls, auc_val in auc_scores.items():
                print(f"  {cls}: {auc_val:.4f}")
    except Exception as e:
        print(f"AUC hesaplanamadı: {e}")
    
    # Classification Report
    print("\n" + "-" * 60)
    print("CLASSIFICATION REPORT")
    print("-" * 60)
    print(classification_report(y_test, y_pred, zero_division=0))
    
    return metrics

def plot_confusion_matrix(y_test, y_pred, classes):
    """Confusion matrix görselleştir"""
    cm = confusion_matrix(y_test, y_pred)
    
    plt.figure(figsize=(10, 8))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues',
                xticklabels=classes, yticklabels=classes)
    plt.title('Confusion Matrix - kNN Sınıflandırma', fontsize=14)
    plt.xlabel('Tahmin Edilen', fontsize=12)
    plt.ylabel('Gerçek', fontsize=12)
    plt.tight_layout()
    
    save_path = os.path.join(VIZ_PATH, 'knn_confusion_matrix.png')
    plt.savefig(save_path, dpi=150)
    plt.close()
    print(f"✓ Confusion matrix kaydedildi: {save_path}")
    
    return cm

def plot_roc_curves(y_test, y_prob, classes):
    """ROC eğrileri görselleştir"""
    y_test_bin = label_binarize(y_test, classes=classes)
    
    plt.figure(figsize=(10, 8))
    
    colors = plt.cm.Set1(np.linspace(0, 1, len(classes)))
    
    for i, (cls, color) in enumerate(zip(classes, colors)):
        if i < y_prob.shape[1]:
            fpr, tpr, _ = roc_curve(y_test_bin[:, i], y_prob[:, i])
            roc_auc = auc(fpr, tpr)
            plt.plot(fpr, tpr, color=color, lw=2,
                    label=f'Severity {cls} (AUC = {roc_auc:.3f})')
    
    plt.plot([0, 1], [0, 1], 'k--', lw=2, label='Random (AUC = 0.500)')
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('False Positive Rate', fontsize=12)
    plt.ylabel('True Positive Rate', fontsize=12)
    plt.title('ROC Curves - kNN Sınıflandırma (Multi-class)', fontsize=14)
    plt.legend(loc='lower right')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    save_path = os.path.join(VIZ_PATH, 'knn_roc_curves.png')
    plt.savefig(save_path, dpi=150)
    plt.close()
    print(f"✓ ROC curves kaydedildi: {save_path}")

def plot_k_optimization(k_scores):
    """K optimizasyon grafiği"""
    k_vals = [s['k'] for s in k_scores]
    accuracies = [s['mean_accuracy'] for s in k_scores]
    stds = [s['std'] for s in k_scores]
    
    plt.figure(figsize=(10, 6))
    plt.errorbar(k_vals, accuracies, yerr=stds, marker='o', capsize=5, capthick=2)
    plt.xlabel('k Değeri', fontsize=12)
    plt.ylabel('Cross-Validation Accuracy', fontsize=12)
    plt.title('kNN - Optimal k Değeri Seçimi', fontsize=14)
    plt.grid(True, alpha=0.3)
    plt.xticks(k_vals)
    
    # En iyi k'yı işaretle
    best_idx = np.argmax(accuracies)
    plt.scatter([k_vals[best_idx]], [accuracies[best_idx]], 
               color='red', s=200, zorder=5, label=f'Optimal k={k_vals[best_idx]}')
    plt.legend()
    plt.tight_layout()
    
    save_path = os.path.join(VIZ_PATH, 'knn_k_optimization.png')
    plt.savefig(save_path, dpi=150)
    plt.close()
    print(f"✓ K optimizasyon grafiği kaydedildi: {save_path}")

def save_results(metrics, k_scores, optimal_k):
    """Sonuçları JSON olarak kaydet"""
    results = {
        'model': 'kNN',
        'optimal_k': optimal_k,
        'timestamp': datetime.now().isoformat(),
        'metrics': metrics,
        'k_optimization': k_scores
    }
    
    # Results dizinini oluştur
    os.makedirs(RESULTS_PATH, exist_ok=True)
    
    save_path = os.path.join(RESULTS_PATH, 'knn_results.json')
    with open(save_path, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"✓ Sonuçlar kaydedildi: {save_path}")
    
    return results

def main():
    print("=" * 60)
    print("kNN SINIFLANDIRMA - US ACCIDENTS")
    print("=" * 60)
    
    # Görselleştirme dizini oluştur
    os.makedirs(VIZ_PATH, exist_ok=True)
    
    # Spark session oluştur
    spark = create_spark_session()
    print("✓ Spark Session oluşturuldu")
    
    try:
        # Veri yükle
        df, feature_columns = load_and_prepare_data(spark)
        
        # Pandas'a çevir (örneklem)
        pdf = spark_to_pandas_sample(df, sample_size=100000)
        
        # X ve y ayır
        X = pdf[feature_columns]
        y = pdf['Severity']
        
        print(f"\nSeverity dağılımı:")
        print(y.value_counts().sort_index())
        
        classes = sorted(y.unique())
        print(f"Sınıflar: {classes}")
        
        # Train/Test split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        print(f"\nEğitim seti: {len(X_train):,}")
        print(f"Test seti: {len(X_test):,}")
        
        # Optimal k bul
        optimal_k, k_scores = find_optimal_k(X_train, y_train, k_range=range(1, 16))
        
        # Model eğit
        knn, scaler, y_pred, y_prob = train_knn_model(
            X_train, X_test, y_train, y_test, k=optimal_k
        )
        
        # Metrikleri hesapla
        metrics = calculate_metrics(y_test, y_pred, y_prob, classes)
        
        # Görselleştirmeler
        print("\n" + "=" * 60)
        print("GÖRSELLEŞTİRMELER")
        print("=" * 60)
        
        plot_confusion_matrix(y_test, y_pred, classes)
        plot_roc_curves(y_test, y_prob, classes)
        plot_k_optimization(k_scores)
        
        # Sonuçları kaydet
        results = save_results(metrics, k_scores, optimal_k)
        
        print("\n" + "=" * 60)
        print("kNN SINIFLANDIRMA TAMAMLANDI!")
        print("=" * 60)
        print("\nSonraki adım: K-Means Kümeleme")
        print("  python scripts/kmeans_clustering.py")
        
        return results
        
    except Exception as e:
        print(f"\nHATA: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
