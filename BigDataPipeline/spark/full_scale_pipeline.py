"""
FULL SCALE PIPELINE - 7.7 Million Records
US Accidents Big Data Pipeline
All algorithms: Random Forest + K-Means on maximum data
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, hour, dayofweek, month, avg, count
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, ClusteringEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
import json
import os
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FullPipeline")

INPUT_PATH = "/home/jovyan/data/US_Accidents_March23.csv"
OUTPUT_DIR = "/home/jovyan/output"
VIZ_DIR = f"{OUTPUT_DIR}/visualizations"

def create_spark_session():
    return SparkSession.builder \
        .appName("FullPipeline_7M") \
        .config("spark.driver.memory", "10g") \
        .config("spark.executor.memory", "10g") \
        .config("spark.sql.shuffle.partitions", "50") \
        .config("spark.default.parallelism", "16") \
        .config("spark.driver.maxResultSize", "4g") \
        .getOrCreate()

def load_and_clean_data(spark):
    """Load and clean full dataset"""
    logger.info("=" * 60)
    logger.info("STAGE 1: LOADING & CLEANING 7.7M RECORDS")
    logger.info("=" * 60)
    
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(INPUT_PATH)
    total = df.count()
    logger.info(f"Loaded {total:,} records")
    
    # Select features
    cols = ['ID', 'Severity', 'Start_Lat', 'Start_Lng', 'Distance(mi)', 
            'City', 'State', 'Temperature(F)', 'Humidity(%)', 'Visibility(mi)',
            'Wind_Speed(mph)', 'Weather_Condition', 'Amenity', 'Crossing', 
            'Junction', 'Traffic_Signal', 'Sunrise_Sunset', 'Start_Time']
    df = df.select([c for c in cols if c in df.columns])
    
    # Rename columns
    renames = {'Distance(mi)': 'Distance_mi', 'Temperature(F)': 'Temperature_F',
               'Humidity(%)': 'Humidity_Percent', 'Visibility(mi)': 'Visibility_mi',
               'Wind_Speed(mph)': 'Wind_Speed_mph'}
    for old, new in renames.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    
    # Fill nulls
    for c in ['Temperature_F', 'Humidity_Percent', 'Visibility_mi', 'Wind_Speed_mph', 'Distance_mi']:
        if c in df.columns:
            df = df.fillna({c: 0.0})
    
    # Boolean to int
    for c in ['Amenity', 'Crossing', 'Junction', 'Traffic_Signal']:
        if c in df.columns:
            df = df.withColumn(c, when(col(c) == True, 1).otherwise(0).cast(IntegerType()))
    
    # Temporal features
    if 'Start_Time' in df.columns:
        df = df.withColumn('Hour', hour(col('Start_Time')))
        df = df.withColumn('DayOfWeek', dayofweek(col('Start_Time')))
        df = df.withColumn('Month', month(col('Start_Time')))
        df = df.drop('Start_Time')
    
    # Filter valid data
    df = df.filter(col('Start_Lat').isNotNull() & col('Start_Lng').isNotNull())
    df = df.filter((col('Severity') >= 1) & (col('Severity') <= 4))
    df = df.fillna({'Weather_Condition': 'Unknown', 'City': 'Unknown', 'State': 'Unknown'})
    
    cleaned = df.count()
    logger.info(f"Cleaned: {cleaned:,} records")
    
    return df, total, cleaned

def run_random_forest(df, data_size):
    """Run Random Forest on full data"""
    logger.info("=" * 60)
    logger.info(f"STAGE 2: RANDOM FOREST ({data_size:,} records)")
    logger.info("=" * 60)
    
    # Prepare features
    features = ['Start_Lat', 'Start_Lng', 'Distance_mi', 'Temperature_F', 
                'Humidity_Percent', 'Visibility_mi', 'Wind_Speed_mph',
                'Amenity', 'Crossing', 'Junction', 'Traffic_Signal', 'Hour', 'DayOfWeek', 'Month']
    existing = [f for f in features if f in df.columns]
    
    for f in existing:
        df = df.withColumn(f, col(f).cast(DoubleType()))
        df = df.fillna({f: 0.0})
    
    df = df.withColumn("label", (col("Severity") - 1).cast(DoubleType()))
    
    assembler = VectorAssembler(inputCols=existing, outputCol="features", handleInvalid="skip")
    df_ml = assembler.transform(df).select("ID", "Severity", "label", "features")
    
    # Use stratified sample for training (10% for speed with large data)
    train_sample = df_ml.sample(False, 0.1, seed=42)
    test_sample = df_ml.sample(False, 0.02, seed=43)
    
    train_count = train_sample.count()
    test_count = test_sample.count()
    logger.info(f"Train: {train_count:,}, Test: {test_count:,}")
    
    # Train RF
    logger.info("Training Random Forest...")
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", 
                                numTrees=100, maxDepth=10, seed=42)
    model = rf.fit(train_sample)
    
    logger.info("Making predictions...")
    predictions = model.transform(test_sample)
    
    # Metrics
    evaluators = {
        "accuracy": MulticlassClassificationEvaluator(metricName="accuracy"),
        "precision": MulticlassClassificationEvaluator(metricName="weightedPrecision"),
        "recall": MulticlassClassificationEvaluator(metricName="weightedRecall"),
        "f1": MulticlassClassificationEvaluator(metricName="f1")
    }
    
    metrics = {}
    for name, ev in evaluators.items():
        metrics[name] = round(ev.evaluate(predictions), 4)
        logger.info(f"{name}: {metrics[name]}")
    
    # Confusion matrix
    pred_labels = predictions.select("prediction", "label").rdd.map(lambda r: (float(r[0]), float(r[1])))
    cm = MulticlassMetrics(pred_labels).confusionMatrix().toArray().tolist()
    
    # Feature importance
    importance = dict(zip(existing, [round(x, 4) for x in model.featureImportances.toArray().tolist()]))
    
    # Save
    results = {
        "model": "RandomForest",
        "timestamp": datetime.now().isoformat(),
        "data_size": data_size,
        "train_size": train_count,
        "test_size": test_count,
        "params": {"numTrees": 100, "maxDepth": 10},
        "metrics": metrics,
        "confusion_matrix": cm,
        "feature_importance": importance
    }
    
    with open(f"{OUTPUT_DIR}/random_forest_results_7m.json", "w") as f:
        json.dump(results, f, indent=2)
    
    # Visualization
    plot_rf_results(results)
    
    return results

def run_kmeans(df, data_size):
    """Run K-Means on full data"""
    logger.info("=" * 60)
    logger.info(f"STAGE 3: K-MEANS CLUSTERING ({data_size:,} records)")
    logger.info("=" * 60)
    
    # Prepare
    df = df.select("ID", "Severity", "Start_Lat", "Start_Lng", "State")
    for c in ["Start_Lat", "Start_Lng"]:
        df = df.withColumn(c, col(c).cast(DoubleType()))
    
    df.cache()
    
    # Feature preparation
    assembler = VectorAssembler(inputCols=["Start_Lat", "Start_Lng"], outputCol="features_raw", handleInvalid="skip")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
    
    df_assembled = assembler.transform(df)
    df_scaled = scaler.fit(df_assembled).transform(df_assembled)
    df_scaled.cache()
    
    # Find optimal K
    logger.info("Finding optimal K...")
    k_range = [4, 6, 8, 10, 12]
    results = []
    
    for k in k_range:
        logger.info(f"Testing K={k}...")
        kmeans = KMeans(k=k, seed=42, maxIter=50)
        model = kmeans.fit(df_scaled)
        predictions = model.transform(df_scaled)
        
        silhouette = ClusteringEvaluator().evaluate(predictions)
        wssse = model.summary.trainingCost
        
        results.append({"k": k, "silhouette": round(silhouette, 4), "wssse": round(wssse, 2)})
        logger.info(f"K={k}: Silhouette={silhouette:.4f}")
    
    best = max(results, key=lambda x: x["silhouette"])
    optimal_k = best["k"]
    logger.info(f"Optimal K: {optimal_k}")
    
    # Final model
    logger.info(f"Training final model with K={optimal_k}...")
    kmeans = KMeans(k=optimal_k, seed=42, maxIter=100)
    model = kmeans.fit(df_scaled)
    predictions = model.transform(df_scaled)
    
    final_silhouette = ClusteringEvaluator().evaluate(predictions)
    
    # Stats
    cluster_stats = predictions.groupBy("prediction").agg(
        count("*").alias("count"),
        avg("Start_Lat").alias("avg_lat"),
        avg("Start_Lng").alias("avg_lng"),
        avg("Severity").alias("avg_severity")
    ).orderBy("prediction").collect()
    
    stats = []
    for row in cluster_stats:
        stats.append({
            "cluster": int(row["prediction"]),
            "count": int(row["count"]),
            "center": {"lat": round(row["avg_lat"], 4), "lng": round(row["avg_lng"], 4)},
            "avg_severity": round(row["avg_severity"], 2)
        })
    
    # Save
    kmeans_results = {
        "algorithm": "K-Means",
        "timestamp": datetime.now().isoformat(),
        "data_size": data_size,
        "optimal_k": optimal_k,
        "silhouette_score": round(final_silhouette, 4),
        "elbow_analysis": results,
        "cluster_statistics": stats
    }
    
    with open(f"{OUTPUT_DIR}/kmeans_results_7m.json", "w") as f:
        json.dump(kmeans_results, f, indent=2)
    
    # Sample for viz
    clustered = predictions.select("ID", "Severity", "Start_Lat", "Start_Lng", "State", "prediction")
    clustered = clustered.withColumnRenamed("prediction", "cluster")
    clustered.sample(False, 0.005).toPandas().to_csv(f"{OUTPUT_DIR}/clustered_7m_sample.csv", index=False)
    
    # Visualization
    plot_kmeans_results(kmeans_results, clustered)
    
    return kmeans_results

def plot_rf_results(rf_results):
    """Generate RF visualizations"""
    logger.info("Creating RF visualizations...")
    
    # Confusion Matrix
    cm = np.array(rf_results["confusion_matrix"])
    plt.figure(figsize=(10, 8))
    sns.heatmap(cm, annot=True, fmt='.0f', cmap='Blues',
                xticklabels=['Sev 1', 'Sev 2', 'Sev 3', 'Sev 4'],
                yticklabels=['Sev 1', 'Sev 2', 'Sev 3', 'Sev 4'])
    plt.title(f'Confusion Matrix (7.7M Data)\nAccuracy: {rf_results["metrics"]["accuracy"]:.4f}', fontsize=14)
    plt.xlabel('Predicted')
    plt.ylabel('Actual')
    plt.tight_layout()
    plt.savefig(f"{VIZ_DIR}/confusion_matrix_7m.png", dpi=150)
    plt.close()
    
    # Feature Importance
    importance = rf_results["feature_importance"]
    sorted_imp = sorted(importance.items(), key=lambda x: x[1], reverse=True)
    
    plt.figure(figsize=(12, 8))
    colors = plt.cm.Blues(np.linspace(0.4, 0.9, len(sorted_imp)))
    plt.barh(range(len(sorted_imp)), [x[1] for x in sorted_imp], color=colors)
    plt.yticks(range(len(sorted_imp)), [x[0] for x in sorted_imp])
    plt.xlabel('Importance')
    plt.title(f'Feature Importance (7.7M Data)', fontsize=14)
    plt.tight_layout()
    plt.savefig(f"{VIZ_DIR}/rf_feature_importance_7m.png", dpi=150)
    plt.close()

def plot_kmeans_results(km_results, clustered_df):
    """Generate K-Means visualizations"""
    logger.info("Creating K-Means visualizations...")
    
    # Elbow
    elbow = km_results["elbow_analysis"]
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    k_vals = [e["k"] for e in elbow]
    sil = [e["silhouette"] for e in elbow]
    wssse = [e["wssse"] for e in elbow]
    
    ax1.plot(k_vals, wssse, 'bo-', linewidth=2, markersize=10)
    ax1.axvline(x=km_results["optimal_k"], color='r', linestyle='--', linewidth=2)
    ax1.set_xlabel('K')
    ax1.set_ylabel('WSSSE')
    ax1.set_title('Elbow Method (7.7M Data)')
    ax1.grid(True, alpha=0.3)
    
    ax2.plot(k_vals, sil, 'go-', linewidth=2, markersize=10)
    ax2.axvline(x=km_results["optimal_k"], color='r', linestyle='--', linewidth=2)
    ax2.set_xlabel('K')
    ax2.set_ylabel('Silhouette')
    ax2.set_title(f'Silhouette Score (Optimal K={km_results["optimal_k"]})')
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f"{VIZ_DIR}/elbow_method_7m.png", dpi=150)
    plt.close()
    
    # Geographic clusters
    sample = clustered_df.sample(False, 0.002).toPandas()
    
    plt.figure(figsize=(16, 10))
    scatter = plt.scatter(sample['Start_Lng'], sample['Start_Lat'], 
                         c=sample['cluster'], cmap='tab10', alpha=0.4, s=2)
    plt.colorbar(scatter, label='Cluster')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.title(f'Geographic Clusters (7.7M Data, K={km_results["optimal_k"]})', fontsize=14)
    plt.xlim(-130, -65)
    plt.ylim(24, 50)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{VIZ_DIR}/geographic_clusters_7m.png", dpi=150)
    plt.close()
    
    # Cluster distribution
    stats = km_results["cluster_statistics"]
    clusters = [s["cluster"] for s in stats]
    counts = [s["count"] for s in stats]
    
    plt.figure(figsize=(12, 6))
    colors = plt.cm.Set3(np.linspace(0, 1, len(clusters)))
    bars = plt.bar(clusters, counts, color=colors, edgecolor='black')
    for bar, cnt in zip(bars, counts):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                f'{cnt:,}', ha='center', va='bottom', fontsize=9)
    plt.xlabel('Cluster')
    plt.ylabel('Count')
    plt.title('Cluster Distribution (7.7M Data)')
    plt.tight_layout()
    plt.savefig(f"{VIZ_DIR}/cluster_distribution_7m.png", dpi=150)
    plt.close()

def main():
    logger.info("=" * 70)
    logger.info("   FULL SCALE BIG DATA PIPELINE - 7.7 MILLION RECORDS")
    logger.info("=" * 70)
    
    os.makedirs(VIZ_DIR, exist_ok=True)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Stage 1: Load and Clean
        df, total, cleaned = load_and_clean_data(spark)
        df.cache()
        
        # Stage 2: Random Forest
        rf_results = run_random_forest(df, cleaned)
        
        # Stage 3: K-Means
        km_results = run_kmeans(df, cleaned)
        
        # Summary
        summary = {
            "pipeline": "Full Scale Pipeline",
            "timestamp": datetime.now().isoformat(),
            "total_records": total,
            "cleaned_records": cleaned,
            "random_forest": {
                "accuracy": rf_results["metrics"]["accuracy"],
                "f1_score": rf_results["metrics"]["f1"],
                "train_size": rf_results["train_size"]
            },
            "kmeans": {
                "optimal_k": km_results["optimal_k"],
                "silhouette": km_results["silhouette_score"]
            }
        }
        
        with open(f"{OUTPUT_DIR}/pipeline_summary_7m.json", "w") as f:
            json.dump(summary, f, indent=2)
        
        logger.info("\n" + "=" * 70)
        logger.info("   FULL SCALE PIPELINE COMPLETE!")
        logger.info("=" * 70)
        logger.info(f"Total Records: {total:,}")
        logger.info(f"Cleaned Records: {cleaned:,}")
        logger.info(f"Random Forest Accuracy: {rf_results['metrics']['accuracy']:.4f}")
        logger.info(f"K-Means Silhouette: {km_results['silhouette_score']:.4f}")
        logger.info(f"Optimal K: {km_results['optimal_k']}")
        
    except Exception as e:
        logger.error(f"Pipeline error: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
