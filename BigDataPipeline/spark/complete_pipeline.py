"""
Complete Pipeline - File Output Version
US Accidents Big Data Pipeline
Saves all results to JSON/Parquet files
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, hour, dayofweek, month, lit, avg, count, stddev
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, ClusteringEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
import json
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Pipeline")

# ============================================
# CONFIGURATION
# ============================================
INPUT_PATH = "/home/jovyan/data/US_Accidents_March23.csv"
OUTPUT_DIR = "/home/jovyan/output"
SAMPLE_FRACTION = 0.05  # 5% of data for faster processing

def create_spark_session():
    return SparkSession.builder \
        .appName("US_Accidents_Complete_Pipeline") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()

# ============================================
# DATA CLEANING
# ============================================
def clean_data(spark):
    logger.info("=" * 60)
    logger.info("STAGE 1: DATA CLEANING")
    logger.info("=" * 60)
    
    # Load
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(INPUT_PATH)
    original_count = df.count()
    logger.info(f"Loaded {original_count} records")
    
    # Sample
    df = df.sample(False, SAMPLE_FRACTION, seed=42)
    logger.info(f"Sampled to {df.count()} records")
    
    # Select features
    cols = ['ID', 'Severity', 'Start_Lat', 'Start_Lng', 'Distance(mi)', 
            'City', 'State', 'Temperature(F)', 'Humidity(%)', 'Visibility(mi)',
            'Wind_Speed(mph)', 'Weather_Condition', 'Amenity', 'Crossing', 
            'Junction', 'Traffic_Signal', 'Sunrise_Sunset', 'Start_Time']
    df = df.select([c for c in cols if c in df.columns])
    
    # Rename
    renames = {'Distance(mi)': 'Distance_mi', 'Temperature(F)': 'Temperature_F',
               'Humidity(%)': 'Humidity_Percent', 'Visibility(mi)': 'Visibility_mi',
               'Wind_Speed(mph)': 'Wind_Speed_mph'}
    for old, new in renames.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    
    # Fill nulls
    for c in ['Temperature_F', 'Humidity_Percent', 'Visibility_mi', 'Wind_Speed_mph', 'Distance_mi']:
        if c in df.columns:
            med = df.approxQuantile(c, [0.5], 0.1)
            if med:
                df = df.fillna({c: med[0]})
    
    # Boolean to int
    for c in ['Amenity', 'Crossing', 'Junction', 'Traffic_Signal']:
        if c in df.columns:
            df = df.withColumn(c, when(col(c) == True, 1).otherwise(0).cast(IntegerType()))
    
    # Temporal
    if 'Start_Time' in df.columns:
        df = df.withColumn('Hour', hour(col('Start_Time')))
        df = df.withColumn('DayOfWeek', dayofweek(col('Start_Time')))
        df = df.withColumn('Month', month(col('Start_Time')))
        df = df.drop('Start_Time')
    
    # Filter
    df = df.filter(col('Start_Lat').isNotNull() & col('Start_Lng').isNotNull())
    df = df.filter((col('Severity') >= 1) & (col('Severity') <= 4))
    
    # Fill remaining nulls
    df = df.fillna({'Weather_Condition': 'Unknown', 'City': 'Unknown', 'State': 'Unknown'})
    
    logger.info(f"Cleaned: {df.count()} records, {len(df.columns)} features")
    
    # Save
    df.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/cleaned_data")
    df.limit(1000).toPandas().to_csv(f"{OUTPUT_DIR}/cleaned_sample.csv", index=False)
    
    return df

# ============================================
# RANDOM FOREST CLASSIFICATION
# ============================================
def run_random_forest(df):
    logger.info("=" * 60)
    logger.info("STAGE 2: RANDOM FOREST CLASSIFICATION")
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
    
    train, test = df_ml.randomSplit([0.8, 0.2], seed=42)
    logger.info(f"Train: {train.count()}, Test: {test.count()}")
    
    # Train
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=50, maxDepth=8, seed=42)
    model = rf.fit(train)
    predictions = model.transform(test)
    
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
    
    # Save results
    results = {
        "model": "RandomForest",
        "timestamp": datetime.now().isoformat(),
        "params": {"numTrees": 50, "maxDepth": 8},
        "metrics": metrics,
        "confusion_matrix": cm,
        "feature_importance": importance
    }
    
    with open(f"{OUTPUT_DIR}/random_forest_results.json", "w") as f:
        json.dump(results, f, indent=2)
    
    logger.info(f"Random Forest completed. Accuracy: {metrics['accuracy']}")
    return metrics

# ============================================
# K-MEANS CLUSTERING
# ============================================
def run_kmeans(df):
    logger.info("=" * 60)
    logger.info("STAGE 3: K-MEANS CLUSTERING")
    logger.info("=" * 60)
    
    # Prepare features
    geo_features = ['Start_Lat', 'Start_Lng']
    for f in geo_features:
        df = df.withColumn(f, col(f).cast(DoubleType()))
    
    assembler = VectorAssembler(inputCols=geo_features, outputCol="features_raw", handleInvalid="skip")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
    
    df_assembled = assembler.transform(df)
    df_scaled = scaler.fit(df_assembled).transform(df_assembled)
    
    # Find optimal K
    k_range = range(3, 10)
    results = []
    
    for k in k_range:
        kmeans = KMeans(k=k, seed=42, maxIter=30)
        model = kmeans.fit(df_scaled)
        predictions = model.transform(df_scaled)
        
        evaluator = ClusteringEvaluator()
        silhouette = evaluator.evaluate(predictions)
        wssse = model.summary.trainingCost
        
        results.append({"k": k, "silhouette": round(silhouette, 4), "wssse": round(wssse, 2)})
        logger.info(f"K={k}: Silhouette={silhouette:.4f}, WSSSE={wssse:.2f}")
    
    # Best K
    best = max(results, key=lambda x: x["silhouette"])
    optimal_k = best["k"]
    logger.info(f"Optimal K: {optimal_k}")
    
    # Final model
    kmeans = KMeans(k=optimal_k, seed=42, maxIter=50)
    model = kmeans.fit(df_scaled)
    predictions = model.transform(df_scaled)
    
    final_silhouette = ClusteringEvaluator().evaluate(predictions)
    
    # Cluster stats
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
        "optimal_k": optimal_k,
        "silhouette_score": round(final_silhouette, 4),
        "elbow_analysis": results,
        "cluster_statistics": stats
    }
    
    with open(f"{OUTPUT_DIR}/kmeans_results.json", "w") as f:
        json.dump(kmeans_results, f, indent=2)
    
    # Save clustered data sample
    clustered = predictions.select("ID", "Severity", "Start_Lat", "Start_Lng", "prediction")
    clustered = clustered.withColumnRenamed("prediction", "cluster")
    clustered.limit(5000).toPandas().to_csv(f"{OUTPUT_DIR}/clustered_sample.csv", index=False)
    
    logger.info(f"K-Means completed. Silhouette: {final_silhouette:.4f}")
    return kmeans_results

# ============================================
# MAIN
# ============================================
def main():
    logger.info("=" * 60)
    logger.info("US ACCIDENTS BIG DATA PIPELINE")
    logger.info("=" * 60)
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Stage 1: Clean
        df = clean_data(spark)
        df.cache()
        
        # Stage 2: Classification
        rf_metrics = run_random_forest(df)
        
        # Stage 3: Clustering
        km_results = run_kmeans(df)
        
        # Summary
        summary = {
            "pipeline": "US Accidents Big Data Pipeline",
            "timestamp": datetime.now().isoformat(),
            "sample_fraction": SAMPLE_FRACTION,
            "classification": {
                "model": "RandomForest",
                "accuracy": rf_metrics["accuracy"],
                "f1_score": rf_metrics["f1"]
            },
            "clustering": {
                "algorithm": "K-Means",
                "optimal_k": km_results["optimal_k"],
                "silhouette": km_results["silhouette_score"]
            },
            "output_files": [
                "cleaned_data/ (parquet)",
                "cleaned_sample.csv",
                "random_forest_results.json",
                "kmeans_results.json",
                "clustered_sample.csv"
            ]
        }
        
        with open(f"{OUTPUT_DIR}/pipeline_summary.json", "w") as f:
            json.dump(summary, f, indent=2)
        
        logger.info("\n" + "=" * 60)
        logger.info("PIPELINE COMPLETE!")
        logger.info("=" * 60)
        logger.info(f"Output directory: {OUTPUT_DIR}")
        logger.info(f"Random Forest Accuracy: {rf_metrics['accuracy']}")
        logger.info(f"K-Means Silhouette: {km_results['silhouette_score']}")
        
    except Exception as e:
        logger.error(f"Pipeline error: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
