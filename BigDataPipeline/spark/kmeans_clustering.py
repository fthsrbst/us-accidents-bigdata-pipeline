"""
K-Means Clustering Job - PySpark
US Accidents Big Data Pipeline

This script performs:
1. Load cleaned data
2. Prepare geographic features for clustering
3. Find optimal K using Elbow method
4. Train K-Means model
5. Visualize clusters on map
6. Calculate cluster statistics
7. Save results to MongoDB
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, avg, count, stddev, udf
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KMeansClustering")

def create_spark_session():
    """Create Spark session"""
    return SparkSession.builder \
        .appName("US_Accidents_KMeans") \
        .config("spark.mongodb.output.uri", "mongodb://admin:admin123@mongodb:27017/bigdata_project.kmeans_results?authSource=admin") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2") \
        .getOrCreate()

def load_cleaned_data(spark, input_path):
    """Load cleaned parquet data"""
    logger.info(f"Loading cleaned data from: {input_path}")
    
    df = spark.read.parquet(input_path)
    logger.info(f"Loaded {df.count()} records")
    return df

def prepare_features(df):
    """Prepare geographic features for K-Means"""
    logger.info("Preparing features for K-Means clustering...")
    
    # Features for geographic clustering
    geo_features = ['Start_Lat', 'Start_Lng']
    
    # Additional features for richer clustering
    additional_features = [
        'Temperature_F', 'Humidity_Percent',
        'Visibility_mi', 'Wind_Speed_mph',
        'Hour_of_Day'
    ]
    
    # Check which additional features exist
    existing_additional = [f for f in additional_features if f in df.columns]
    all_features = geo_features + existing_additional
    
    logger.info(f"Using features: {all_features}")
    
    # Fill nulls
    for col_name in all_features:
        df = df.withColumn(col_name, 
            when(col(col_name).isNull(), 0.0).otherwise(col(col_name).cast(DoubleType())))
    
    # Assemble features
    assembler = VectorAssembler(
        inputCols=all_features,
        outputCol="features_raw",
        handleInvalid="skip"
    )
    
    # Scale features
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    
    # Transform
    df_assembled = assembler.transform(df)
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled)
    
    logger.info(f"Prepared {df_scaled.count()} records for clustering")
    return df_scaled, all_features

def elbow_method(df, k_range=range(2, 12)):
    """Find optimal K using Elbow method"""
    logger.info("Running Elbow method to find optimal K...")
    
    costs = []
    silhouettes = []
    
    for k in k_range:
        logger.info(f"Testing K={k}")
        
        kmeans = KMeans(
            featuresCol="features",
            predictionCol="cluster",
            k=k,
            seed=42,
            maxIter=50
        )
        
        model = kmeans.fit(df)
        
        # Within Set Sum of Squared Errors
        wssse = model.summary.trainingCost
        costs.append(wssse)
        
        # Silhouette score
        predictions = model.transform(df)
        evaluator = ClusteringEvaluator(
            featuresCol="features",
            predictionCol="cluster",
            metricName="silhouette"
        )
        silhouette = evaluator.evaluate(predictions)
        silhouettes.append(silhouette)
        
        logger.info(f"K={k}: WSSSE={wssse:.2f}, Silhouette={silhouette:.4f}")
    
    # Find optimal K (elbow point)
    # Using the point with maximum silhouette score
    optimal_k = list(k_range)[np.argmax(silhouettes)]
    
    logger.info(f"Optimal K: {optimal_k} (Silhouette: {max(silhouettes):.4f})")
    
    return list(k_range), costs, silhouettes, optimal_k

def plot_elbow(k_values, costs, silhouettes, optimal_k, output_path):
    """Plot Elbow method results"""
    logger.info("Plotting Elbow method results...")
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    # WSSSE plot
    ax1.plot(k_values, costs, 'bo-', linewidth=2, markersize=8)
    ax1.axvline(x=optimal_k, color='r', linestyle='--', label=f'Optimal K={optimal_k}')
    ax1.set_xlabel('Number of Clusters (K)', fontsize=12)
    ax1.set_ylabel('Within Cluster Sum of Squares', fontsize=12)
    ax1.set_title('Elbow Method - WCSS', fontsize=14)
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Silhouette plot
    ax2.plot(k_values, silhouettes, 'go-', linewidth=2, markersize=8)
    ax2.axvline(x=optimal_k, color='r', linestyle='--', label=f'Optimal K={optimal_k}')
    ax2.set_xlabel('Number of Clusters (K)', fontsize=12)
    ax2.set_ylabel('Silhouette Score', fontsize=12)
    ax2.set_title('Silhouette Score by K', fontsize=14)
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f"{output_path}/kmeans_elbow_method.png", dpi=150)
    plt.close()
    
    logger.info(f"Saved Elbow plot to {output_path}/kmeans_elbow_method.png")

def train_kmeans(df, k):
    """Train K-Means with optimal K"""
    logger.info(f"Training K-Means with K={k}")
    
    kmeans = KMeans(
        featuresCol="features",
        predictionCol="cluster",
        k=k,
        seed=42,
        maxIter=100
    )
    
    model = kmeans.fit(df)
    predictions = model.transform(df)
    
    # Calculate silhouette score
    evaluator = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="cluster",
        metricName="silhouette"
    )
    silhouette = evaluator.evaluate(predictions)
    
    logger.info(f"Final Silhouette Score: {silhouette:.4f}")
    
    return model, predictions, silhouette

def calculate_cluster_statistics(predictions):
    """Calculate statistics for each cluster"""
    logger.info("Calculating cluster statistics...")
    
    stats = predictions.groupBy("cluster").agg(
        count("*").alias("count"),
        avg("Start_Lat").alias("avg_lat"),
        avg("Start_Lng").alias("avg_lng"),
        avg("Severity").alias("avg_severity"),
        stddev("Severity").alias("std_severity")
    ).orderBy("cluster")
    
    stats_list = stats.collect()
    
    cluster_stats = []
    for row in stats_list:
        stat = {
            "cluster_id": row["cluster"],
            "count": row["count"],
            "avg_lat": float(row["avg_lat"]),
            "avg_lng": float(row["avg_lng"]),
            "avg_severity": float(row["avg_severity"]) if row["avg_severity"] else 0,
            "std_severity": float(row["std_severity"]) if row["std_severity"] else 0
        }
        cluster_stats.append(stat)
        logger.info(f"Cluster {stat['cluster_id']}: {stat['count']} points, Avg Severity: {stat['avg_severity']:.2f}")
    
    return cluster_stats

def plot_geographic_clusters(predictions, output_path):
    """Plot clusters on a geographic map"""
    logger.info("Plotting geographic clusters...")
    
    # Sample for visualization
    sample_df = predictions.select("Start_Lat", "Start_Lng", "cluster", "Severity").sample(False, 0.01)
    data = sample_df.toPandas()
    
    plt.figure(figsize=(16, 10))
    
    # Create scatter plot
    scatter = plt.scatter(
        data['Start_Lng'], 
        data['Start_Lat'],
        c=data['cluster'],
        cmap='tab10',
        alpha=0.5,
        s=5
    )
    
    plt.colorbar(scatter, label='Cluster')
    plt.xlabel('Longitude', fontsize=12)
    plt.ylabel('Latitude', fontsize=12)
    plt.title('K-Means Geographic Clustering of US Accidents', fontsize=14)
    plt.grid(True, alpha=0.3)
    
    # Set US boundaries
    plt.xlim(-130, -65)
    plt.ylim(24, 50)
    
    plt.tight_layout()
    plt.savefig(f"{output_path}/kmeans_geographic_clusters.png", dpi=150)
    plt.close()
    
    logger.info(f"Saved geographic clusters plot to {output_path}/kmeans_geographic_clusters.png")

def plot_cluster_distribution(cluster_stats, output_path):
    """Plot cluster size distribution"""
    logger.info("Plotting cluster distribution...")
    
    clusters = [s['cluster_id'] for s in cluster_stats]
    counts = [s['count'] for s in cluster_stats]
    
    plt.figure(figsize=(12, 6))
    
    bars = plt.bar(clusters, counts, color='steelblue', edgecolor='black')
    
    # Add count labels on bars
    for bar, count in zip(bars, counts):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 100,
                f'{count:,}', ha='center', va='bottom', fontsize=10)
    
    plt.xlabel('Cluster ID', fontsize=12)
    plt.ylabel('Number of Accidents', fontsize=12)
    plt.title('Distribution of Accidents by Cluster', fontsize=14)
    plt.xticks(clusters)
    plt.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(f"{output_path}/kmeans_cluster_distribution.png", dpi=150)
    plt.close()
    
    logger.info(f"Saved cluster distribution plot to {output_path}/kmeans_cluster_distribution.png")

def plot_cluster_characteristics(cluster_stats, output_path):
    """Plot cluster characteristics (severity comparison)"""
    logger.info("Plotting cluster characteristics...")
    
    clusters = [s['cluster_id'] for s in cluster_stats]
    avg_severity = [s['avg_severity'] for s in cluster_stats]
    std_severity = [s['std_severity'] for s in cluster_stats]
    
    plt.figure(figsize=(12, 6))
    
    bars = plt.bar(clusters, avg_severity, yerr=std_severity, 
                   capsize=5, color='coral', edgecolor='black', alpha=0.8)
    
    plt.axhline(y=np.mean(avg_severity), color='r', linestyle='--', 
                label=f'Mean: {np.mean(avg_severity):.2f}')
    
    plt.xlabel('Cluster ID', fontsize=12)
    plt.ylabel('Average Severity', fontsize=12)
    plt.title('Average Severity by Cluster', fontsize=14)
    plt.xticks(clusters)
    plt.legend()
    plt.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(f"{output_path}/kmeans_cluster_characteristics.png", dpi=150)
    plt.close()
    
    logger.info(f"Saved cluster characteristics plot to {output_path}/kmeans_cluster_characteristics.png")

def save_results_to_mongodb(spark, cluster_stats, silhouette, optimal_k, elbow_data):
    """Save clustering results to MongoDB"""
    logger.info("Saving results to MongoDB...")
    
    # Prepare results document
    results_doc = {
        "algorithm": "K-Means",
        "timestamp": datetime.now().isoformat(),
        "optimal_k": optimal_k,
        "silhouette_score": silhouette,
        "cluster_statistics": cluster_stats,
        "elbow_data": {
            "k_values": elbow_data[0],
            "wssse_values": elbow_data[1],
            "silhouette_values": elbow_data[2]
        }
    }
    
    # Save to MongoDB
    results_df = spark.createDataFrame([{
        "algorithm": "K-Means",
        "timestamp": datetime.now().isoformat(),
        "optimal_k": optimal_k,
        "silhouette_score": silhouette,
        "num_clusters": len(cluster_stats)
    }])
    
    results_df.write \
        .format("mongo") \
        .mode("append") \
        .option("uri", "mongodb://admin:admin123@mongodb:27017/bigdata_project.kmeans_results?authSource=admin") \
        .save()
    
    # Save JSON locally
    with open("/opt/spark-output/kmeans_results.json", "w") as f:
        json.dump(results_doc, f, indent=2, default=str)
    
    logger.info("Results saved to MongoDB and local file")

def save_clustered_data_to_mongodb(spark, predictions, sample_size=10000):
    """Save clustered data sample to MongoDB"""
    logger.info("Saving clustered data to MongoDB...")
    
    # Select relevant columns
    clustered_data = predictions.select(
        "ID", "Severity", "Start_Lat", "Start_Lng",
        "State", "City", "cluster"
    ).limit(sample_size)
    
    clustered_data.write \
        .format("mongo") \
        .mode("overwrite") \
        .option("uri", "mongodb://admin:admin123@mongodb:27017/bigdata_project.us_accidents_clustered?authSource=admin") \
        .save()
    
    logger.info(f"Saved {sample_size} clustered records to MongoDB")

def main():
    """Main execution function"""
    logger.info("=" * 60)
    logger.info("K-MEANS CLUSTERING PIPELINE")
    logger.info("=" * 60)
    
    input_path = "/opt/spark-output/cleaned_data"
    output_path = "/opt/spark-output/visualizations"
    
    try:
        # Create Spark session
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        
        # Load data
        df = load_cleaned_data(spark, input_path)
        
        # Sample for faster processing
        df_sampled = df.sample(False, 0.1, seed=42)
        logger.info(f"Using {df_sampled.count()} samples for clustering")
        
        # Prepare features
        df_prepared, features_used = prepare_features(df_sampled)
        df_prepared.cache()
        
        # Create output directory
        import os
        os.makedirs(output_path, exist_ok=True)
        
        # Run Elbow method
        k_values, costs, silhouettes, optimal_k = elbow_method(df_prepared)
        
        # Plot Elbow results
        plot_elbow(k_values, costs, silhouettes, optimal_k, output_path)
        
        # Train with optimal K
        model, predictions, final_silhouette = train_kmeans(df_prepared, optimal_k)
        predictions.cache()
        
        # Calculate cluster statistics
        cluster_stats = calculate_cluster_statistics(predictions)
        
        # Create visualizations
        plot_geographic_clusters(predictions, output_path)
        plot_cluster_distribution(cluster_stats, output_path)
        plot_cluster_characteristics(cluster_stats, output_path)
        
        # Save results
        elbow_data = (k_values, costs, silhouettes)
        save_results_to_mongodb(spark, cluster_stats, final_silhouette, optimal_k, elbow_data)
        save_clustered_data_to_mongodb(spark, predictions)
        
        # Print summary
        logger.info("\n" + "=" * 60)
        logger.info("K-MEANS CLUSTERING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Optimal K: {optimal_k}")
        logger.info(f"Silhouette Score: {final_silhouette:.4f}")
        logger.info(f"Total Clusters: {len(cluster_stats)}")
        logger.info("\nCluster Distribution:")
        for stat in cluster_stats:
            logger.info(f"  Cluster {stat['cluster_id']}: {stat['count']:,} accidents")
        
        spark.stop()
        logger.info("K-Means Clustering completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during K-Means clustering: {str(e)}")
        raise

if __name__ == "__main__":
    main()
