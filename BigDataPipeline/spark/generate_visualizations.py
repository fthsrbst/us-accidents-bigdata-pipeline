"""
Visualization Script - Generate all pipeline charts
Creates: confusion matrix, feature importance, elbow method, 
geographic clusters, severity distribution, cluster characteristics
"""

import json
import os
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Paths
OUTPUT_DIR = "/home/jovyan/output"
VIZ_DIR = f"{OUTPUT_DIR}/visualizations"

os.makedirs(VIZ_DIR, exist_ok=True)

# Set style
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("husl")

def load_results():
    """Load JSON results"""
    with open(f"{OUTPUT_DIR}/random_forest_results.json", "r") as f:
        rf_results = json.load(f)
    with open(f"{OUTPUT_DIR}/kmeans_results.json", "r") as f:
        km_results = json.load(f)
    return rf_results, km_results

def plot_confusion_matrix(rf_results):
    """Plot confusion matrix"""
    print("Creating confusion matrix...")
    cm = np.array(rf_results["confusion_matrix"])
    
    plt.figure(figsize=(10, 8))
    sns.heatmap(cm, annot=True, fmt='.0f', cmap='Blues',
                xticklabels=['Severity 1', 'Severity 2', 'Severity 3', 'Severity 4'],
                yticklabels=['Severity 1', 'Severity 2', 'Severity 3', 'Severity 4'])
    plt.title('Random Forest - Confusion Matrix', fontsize=14, fontweight='bold')
    plt.xlabel('Predicted', fontsize=12)
    plt.ylabel('Actual', fontsize=12)
    plt.tight_layout()
    plt.savefig(f"{VIZ_DIR}/confusion_matrix.png", dpi=150)
    plt.close()
    print("✓ confusion_matrix.png saved")

def plot_feature_importance(rf_results):
    """Plot feature importance"""
    print("Creating feature importance chart...")
    importance = rf_results["feature_importance"]
    
    # Sort
    sorted_imp = sorted(importance.items(), key=lambda x: x[1], reverse=True)
    features = [x[0] for x in sorted_imp]
    values = [x[1] for x in sorted_imp]
    
    plt.figure(figsize=(12, 8))
    colors = plt.cm.Blues(np.linspace(0.4, 0.8, len(features)))
    bars = plt.barh(range(len(features)), values, color=colors)
    plt.yticks(range(len(features)), features)
    plt.xlabel('Importance', fontsize=12)
    plt.ylabel('Feature', fontsize=12)
    plt.title('Random Forest - Feature Importance', fontsize=14, fontweight='bold')
    
    # Add values on bars
    for i, (bar, val) in enumerate(zip(bars, values)):
        plt.text(bar.get_width() + 0.01, bar.get_y() + bar.get_height()/2, 
                f'{val:.4f}', va='center', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(f"{VIZ_DIR}/rf_feature_importance.png", dpi=150)
    plt.close()
    print("✓ rf_feature_importance.png saved")

def plot_elbow_method(km_results):
    """Plot elbow method"""
    print("Creating elbow method chart...")
    elbow = km_results["elbow_analysis"]
    
    k_values = [e["k"] for e in elbow]
    wssse = [e["wssse"] for e in elbow]
    silhouette = [e["silhouette"] for e in elbow]
    optimal_k = km_results["optimal_k"]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    # WSSSE plot
    ax1.plot(k_values, wssse, 'bo-', linewidth=2, markersize=8)
    ax1.axvline(x=optimal_k, color='r', linestyle='--', linewidth=2, label=f'Optimal K={optimal_k}')
    ax1.set_xlabel('Number of Clusters (K)', fontsize=12)
    ax1.set_ylabel('Within Cluster Sum of Squares (WSSSE)', fontsize=12)
    ax1.set_title('Elbow Method', fontsize=14, fontweight='bold')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Silhouette plot
    ax2.plot(k_values, silhouette, 'go-', linewidth=2, markersize=8)
    ax2.axvline(x=optimal_k, color='r', linestyle='--', linewidth=2, label=f'Optimal K={optimal_k}')
    ax2.set_xlabel('Number of Clusters (K)', fontsize=12)
    ax2.set_ylabel('Silhouette Score', fontsize=12)
    ax2.set_title('Silhouette Score by K', fontsize=14, fontweight='bold')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f"{VIZ_DIR}/elbow_method.png", dpi=150)
    plt.close()
    print("✓ elbow_method.png saved")

def plot_cluster_distribution(km_results):
    """Plot cluster distribution"""
    print("Creating cluster distribution chart...")
    clusters = km_results["cluster_statistics"]
    
    cluster_ids = [c["cluster"] for c in clusters]
    counts = [c["count"] for c in clusters]
    
    plt.figure(figsize=(12, 6))
    colors = plt.cm.Set3(np.linspace(0, 1, len(cluster_ids)))
    bars = plt.bar(cluster_ids, counts, color=colors, edgecolor='black', linewidth=1.2)
    
    # Add labels
    for bar, count in zip(bars, counts):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 500,
                f'{count:,}', ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    plt.xlabel('Cluster ID', fontsize=12)
    plt.ylabel('Number of Accidents', fontsize=12)
    plt.title('Distribution of Accidents by Cluster', fontsize=14, fontweight='bold')
    plt.xticks(cluster_ids)
    plt.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()
    plt.savefig(f"{VIZ_DIR}/cluster_distribution.png", dpi=150)
    plt.close()
    print("✓ cluster_distribution.png saved")

def plot_cluster_characteristics(km_results):
    """Plot cluster characteristics (severity)"""
    print("Creating cluster characteristics chart...")
    clusters = km_results["cluster_statistics"]
    
    cluster_ids = [c["cluster"] for c in clusters]
    avg_severity = [c["avg_severity"] for c in clusters]
    
    plt.figure(figsize=(12, 6))
    colors = plt.cm.RdYlGn_r(np.linspace(0.2, 0.8, len(cluster_ids)))
    bars = plt.bar(cluster_ids, avg_severity, color=colors, edgecolor='black', linewidth=1.2)
    
    # Mean line
    mean_sev = np.mean(avg_severity)
    plt.axhline(y=mean_sev, color='red', linestyle='--', linewidth=2, label=f'Mean: {mean_sev:.2f}')
    
    # Add labels
    for bar, sev in zip(bars, avg_severity):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.02,
                f'{sev:.2f}', ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    plt.xlabel('Cluster ID', fontsize=12)
    plt.ylabel('Average Severity', fontsize=12)
    plt.title('Average Severity by Cluster', fontsize=14, fontweight='bold')
    plt.xticks(cluster_ids)
    plt.ylim(0, max(avg_severity) + 0.5)
    plt.legend()
    plt.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()
    plt.savefig(f"{VIZ_DIR}/cluster_characteristics.png", dpi=150)
    plt.close()
    print("✓ cluster_characteristics.png saved")

def plot_geographic_clusters():
    """Plot geographic clusters map"""
    print("Creating geographic clusters map...")
    
    # Load clustered data
    df = pd.read_csv(f"{OUTPUT_DIR}/clustered_sample.csv")
    
    plt.figure(figsize=(16, 10))
    
    # Scatter plot
    scatter = plt.scatter(df['Start_Lng'], df['Start_Lat'], 
                         c=df['cluster'], cmap='tab10', alpha=0.6, s=8)
    
    plt.colorbar(scatter, label='Cluster', shrink=0.8)
    plt.xlabel('Longitude', fontsize=12)
    plt.ylabel('Latitude', fontsize=12)
    plt.title('K-Means Geographic Clustering of US Accidents', fontsize=14, fontweight='bold')
    
    # US boundaries
    plt.xlim(-130, -65)
    plt.ylim(24, 50)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{VIZ_DIR}/geographic_clusters.png", dpi=150)
    plt.close()
    print("✓ geographic_clusters.png saved")

def plot_severity_distribution():
    """Plot severity distribution"""
    print("Creating severity distribution chart...")
    
    df = pd.read_csv(f"{OUTPUT_DIR}/cleaned_sample.csv")
    
    severity_counts = df['Severity'].value_counts().sort_index()
    
    plt.figure(figsize=(10, 6))
    colors = ['#2ecc71', '#f1c40f', '#e67e22', '#e74c3c']
    bars = plt.bar(severity_counts.index, severity_counts.values, color=colors, edgecolor='black')
    
    # Add labels
    for bar, count in zip(bars, severity_counts.values):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                f'{count:,}', ha='center', va='bottom', fontsize=11, fontweight='bold')
    
    plt.xlabel('Severity Level', fontsize=12)
    plt.ylabel('Number of Accidents', fontsize=12)
    plt.title('Distribution of Accident Severity', fontsize=14, fontweight='bold')
    plt.xticks([1, 2, 3, 4], ['Severity 1\n(Low)', 'Severity 2', 'Severity 3', 'Severity 4\n(High)'])
    plt.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()
    plt.savefig(f"{VIZ_DIR}/severity_distribution.png", dpi=150)
    plt.close()
    print("✓ severity_distribution.png saved")

def plot_metrics_summary(rf_results, km_results):
    """Plot metrics summary"""
    print("Creating metrics summary chart...")
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # RF metrics
    metrics = rf_results["metrics"]
    metric_names = list(metrics.keys())
    metric_values = list(metrics.values())
    
    colors = ['#3498db', '#2ecc71', '#e74c3c', '#9b59b6']
    bars1 = axes[0].bar(metric_names, metric_values, color=colors, edgecolor='black')
    axes[0].set_ylim(0, 1)
    axes[0].set_title('Random Forest Classification Metrics', fontsize=12, fontweight='bold')
    axes[0].set_ylabel('Score', fontsize=10)
    for bar, val in zip(bars1, metric_values):
        axes[0].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.02,
                    f'{val:.4f}', ha='center', fontsize=10, fontweight='bold')
    
    # K-Means metrics
    km_metrics = ['Optimal K', 'Silhouette']
    km_values = [km_results["optimal_k"] / 10, km_results["silhouette_score"]]  # Normalize K
    
    bars2 = axes[1].bar(km_metrics, km_values, color=['#e67e22', '#1abc9c'], edgecolor='black')
    axes[1].set_ylim(0, 1)
    axes[1].set_title('K-Means Clustering Metrics', fontsize=12, fontweight='bold')
    axes[1].set_ylabel('Score (K normalized /10)', fontsize=10)
    axes[1].text(bars2[0].get_x() + bars2[0].get_width()/2, bars2[0].get_height() + 0.02,
                f'K={km_results["optimal_k"]}', ha='center', fontsize=10, fontweight='bold')
    axes[1].text(bars2[1].get_x() + bars2[1].get_width()/2, bars2[1].get_height() + 0.02,
                f'{km_results["silhouette_score"]:.4f}', ha='center', fontsize=10, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(f"{VIZ_DIR}/metrics_summary.png", dpi=150)
    plt.close()
    print("✓ metrics_summary.png saved")

def main():
    print("=" * 60)
    print("GENERATING VISUALIZATIONS")
    print("=" * 60)
    
    rf_results, km_results = load_results()
    
    plot_confusion_matrix(rf_results)
    plot_feature_importance(rf_results)
    plot_elbow_method(km_results)
    plot_cluster_distribution(km_results)
    plot_cluster_characteristics(km_results)
    plot_geographic_clusters()
    plot_severity_distribution()
    plot_metrics_summary(rf_results, km_results)
    
    print("\n" + "=" * 60)
    print("ALL VISUALIZATIONS COMPLETE!")
    print(f"Saved to: {VIZ_DIR}")
    print("=" * 60)

if __name__ == "__main__":
    main()
