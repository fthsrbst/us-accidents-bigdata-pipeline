"""
K-Means Clustering - Large Scale (2 Million Records)
US Accidents Big Data Pipeline
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import json
import os
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KMeans_LargeScale")

INPUT_PATH = "/home/jovyan/data/US_Accidents_March23.csv"
OUTPUT_DIR = "/home/jovyan/output"
SAMPLE_SIZE = 2000000  # 2 million records

def create_spark_session():
    return SparkSession.builder \
        .appName("KMeans_LargeScale_2M") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "20") \
        .config("spark.default.parallelism", "8") \
        .getOrCreate()

def main():
    logger.info("=" * 60)
    logger.info("K-MEANS CLUSTERING - 2 MILLION RECORDS")
    logger.info("=" * 60)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load data
    logger.info("Loading data...")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(INPUT_PATH)
    total_count = df.count()
    logger.info(f"Total records: {total_count:,}")
    
    # Sample to 2 million
    sample_fraction = min(SAMPLE_SIZE / total_count, 1.0)
    df = df.sample(False, sample_fraction, seed=42)
    actual_count = df.count()
    logger.info(f"Sampled to: {actual_count:,} records")
    
    # Select and prepare
    df = df.select("ID", "Severity", "Start_Lat", "Start_Lng", "City", "State")
    df = df.filter(col("Start_Lat").isNotNull() & col("Start_Lng").isNotNull())
    
    for c in ["Start_Lat", "Start_Lng"]:
        df = df.withColumn(c, col(c).cast(DoubleType()))
    
    df.cache()
    logger.info(f"After filtering: {df.count():,} records")
    
    # Feature preparation
    logger.info("Preparing features...")
    assembler = VectorAssembler(inputCols=["Start_Lat", "Start_Lng"], outputCol="features_raw", handleInvalid="skip")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
    
    df_assembled = assembler.transform(df)
    df_scaled = scaler.fit(df_assembled).transform(df_assembled)
    df_scaled.cache()
    
    # Find optimal K (smaller range for speed)
    logger.info("Finding optimal K...")
    k_range = [4, 6, 8, 10, 12]
    results = []
    
    for k in k_range:
        logger.info(f"Testing K={k}...")
        kmeans = KMeans(k=k, seed=42, maxIter=50)
        model = kmeans.fit(df_scaled)
        predictions = model.transform(df_scaled)
        
        evaluator = ClusteringEvaluator()
        silhouette = evaluator.evaluate(predictions)
        wssse = model.summary.trainingCost
        
        results.append({"k": k, "silhouette": round(silhouette, 4), "wssse": round(wssse, 2)})
        logger.info(f"K={k}: Silhouette={silhouette:.4f}, WSSSE={wssse:.2f}")
    
    best = max(results, key=lambda x: x["silhouette"])
    optimal_k = best["k"]
    logger.info(f"Optimal K: {optimal_k}")
    
    # Final model
    logger.info(f"Training final model with K={optimal_k}...")
    kmeans = KMeans(k=optimal_k, seed=42, maxIter=100)
    model = kmeans.fit(df_scaled)
    predictions = model.transform(df_scaled)
    
    final_silhouette = ClusteringEvaluator().evaluate(predictions)
    
    # Cluster statistics
    logger.info("Calculating cluster statistics...")
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
    
    # Save results
    kmeans_results = {
        "algorithm": "K-Means",
        "timestamp": datetime.now().isoformat(),
        "data_size": actual_count,
        "optimal_k": optimal_k,
        "silhouette_score": round(final_silhouette, 4),
        "elbow_analysis": results,
        "cluster_statistics": stats
    }
    
    with open(f"{OUTPUT_DIR}/kmeans_results_2m.json", "w") as f:
        json.dump(kmeans_results, f, indent=2)
    
    # Save sample for visualization
    logger.info("Saving clustered data sample...")
    clustered = predictions.select("ID", "Severity", "Start_Lat", "Start_Lng", "State", "prediction")
    clustered = clustered.withColumnRenamed("prediction", "cluster")
    clustered.sample(False, 0.01).toPandas().to_csv(f"{OUTPUT_DIR}/clustered_2m_sample.csv", index=False)
    
    # Generate visualization
    logger.info("Creating geographic visualization...")
    sample_df = clustered.sample(False, 0.005).toPandas()  # 0.5% for viz
    
    plt.figure(figsize=(16, 10))
    scatter = plt.scatter(sample_df['Start_Lng'], sample_df['Start_Lat'], 
                         c=sample_df['cluster'], cmap='tab10', alpha=0.5, s=3)
    plt.colorbar(scatter, label='Cluster', shrink=0.8)
    plt.xlabel('Longitude', fontsize=12)
    plt.ylabel('Latitude', fontsize=12)
    plt.title(f'K-Means Geographic Clustering ({actual_count:,} records, K={optimal_k})', fontsize=14, fontweight='bold')
    plt.xlim(-130, -65)
    plt.ylim(24, 50)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/visualizations/geographic_clusters_2m.png", dpi=150)
    plt.close()
    
    # Print summary
    logger.info("\n" + "=" * 60)
    logger.info("K-MEANS LARGE SCALE COMPLETE!")
    logger.info("=" * 60)
    logger.info(f"Data size: {actual_count:,} records")
    logger.info(f"Optimal K: {optimal_k}")
    logger.info(f"Silhouette Score: {final_silhouette:.4f}")
    logger.info("\nCluster Distribution:")
    for s in stats:
        logger.info(f"  Cluster {s['cluster']}: {s['count']:,} records")
    
    spark.stop()

if __name__ == "__main__":
    main()
