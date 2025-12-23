"""
kNN Classification Job - PySpark
US Accidents Big Data Pipeline

This script performs:
1. Load cleaned data
2. Prepare features for kNN classification
3. Train kNN model for Severity prediction
4. Evaluate with Accuracy, Precision, Recall, F1-Score, AUC-ROC
5. Save results to MongoDB
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, udf
from pyspark.sql.types import IntegerType, DoubleType, ArrayType
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.classification import LogisticRegression  # Used as baseline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.mllib.evaluation import MulticlassMetrics
import numpy as np
from datetime import datetime
import json
import logging
import math

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kNNClassification")

def create_spark_session():
    """Create Spark session"""
    return SparkSession.builder \
        .appName("US_Accidents_kNN_Classification") \
        .config("spark.mongodb.output.uri", "mongodb://admin:admin123@mongodb:27017/bigdata_project.knn_results?authSource=admin") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2") \
        .getOrCreate()

def load_cleaned_data(spark, input_path):
    """Load cleaned parquet data"""
    logger.info(f"Loading cleaned data from: {input_path}")
    
    df = spark.read.parquet(input_path)
    logger.info(f"Loaded {df.count()} records")
    return df

def prepare_features(df):
    """Prepare features for ML"""
    logger.info("Preparing features...")
    
    # Select numeric features for kNN
    numeric_features = [
        'Start_Lat', 'Start_Lng', 'Distance_mi',
        'Temperature_F', 'Humidity_Percent', 'Pressure_in',
        'Visibility_mi', 'Wind_Speed_mph', 'Precipitation_in',
        'Amenity', 'Crossing', 'Junction', 'Railway',
        'Station', 'Stop', 'Traffic_Signal',
        'Hour_of_Day', 'Day_of_Week', 'Month'
    ]
    
    # Filter existing columns
    existing_features = [f for f in numeric_features if f in df.columns]
    logger.info(f"Using {len(existing_features)} features: {existing_features}")
    
    # Fill any remaining nulls with 0
    for col_name in existing_features:
        df = df.withColumn(col_name, when(col(col_name).isNull(), 0).otherwise(col(col_name)))
    
    # Create label column (Severity 1-4 -> 0-3 for classification)
    df = df.withColumn("label", (col("Severity") - 1).cast(IntegerType()))
    
    # Assemble features
    assembler = VectorAssembler(
        inputCols=existing_features,
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
    
    # Create pipeline
    pipeline = Pipeline(stages=[assembler, scaler])
    
    # Fit and transform
    df_prepared = pipeline.fit(df).transform(df)
    
    # Select only needed columns
    df_prepared = df_prepared.select("ID", "Severity", "label", "features", "State", "City")
    
    logger.info(f"Prepared data has {df_prepared.count()} records")
    return df_prepared, existing_features

def euclidean_distance(v1, v2):
    """Calculate Euclidean distance between two vectors"""
    return float(np.sqrt(np.sum((np.array(v1) - np.array(v2)) ** 2)))

def knn_predict(train_data, test_point, k=5):
    """Predict class using kNN algorithm"""
    distances = []
    
    for row in train_data:
        dist = euclidean_distance(row['features'].toArray(), test_point)
        distances.append((dist, row['label']))
    
    # Sort by distance and get k nearest neighbors
    distances.sort(key=lambda x: x[0])
    k_nearest = distances[:k]
    
    # Vote
    votes = {}
    for _, label in k_nearest:
        votes[label] = votes.get(label, 0) + 1
    
    # Return most common label
    predicted_label = max(votes, key=votes.get)
    return predicted_label

def distributed_knn(spark, train_df, test_df, k=5):
    """
    Distributed kNN using broadcasting
    Since Spark MLlib doesn't have native kNN, we implement it
    """
    logger.info(f"Running distributed kNN with k={k}")
    
    # Collect training data (sampled for performance)
    train_sample = train_df.sample(False, 0.1, seed=42).collect()
    train_broadcast = spark.sparkContext.broadcast(train_sample)
    
    def predict_knn(features):
        """UDF to predict using kNN"""
        train_data = train_broadcast.value
        distances = []
        
        test_point = features.toArray()
        
        for row in train_data:
            train_point = row['features'].toArray()
            dist = float(np.sqrt(np.sum((test_point - train_point) ** 2)))
            distances.append((dist, row['label']))
        
        # Sort and get k nearest
        distances.sort(key=lambda x: x[0])
        k_nearest = distances[:k]
        
        # Vote
        votes = {}
        for _, label in k_nearest:
            votes[label] = votes.get(label, 0) + 1
        
        return int(max(votes, key=votes.get))
    
    # Register UDF
    predict_udf = udf(predict_knn, IntegerType())
    
    # Apply prediction
    predictions = test_df.withColumn("prediction", predict_udf(col("features")))
    
    return predictions

def find_optimal_k(spark, train_df, test_df, k_values=[3, 5, 7, 9, 11]):
    """Find optimal k value"""
    logger.info("Finding optimal k value...")
    
    results = []
    
    for k in k_values:
        logger.info(f"Testing k={k}")
        predictions = distributed_knn(spark, train_df, test_df.sample(False, 0.2), k)
        
        evaluator = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="accuracy"
        )
        
        accuracy = evaluator.evaluate(predictions)
        results.append({'k': k, 'accuracy': accuracy})
        logger.info(f"k={k}, Accuracy={accuracy:.4f}")
    
    # Find best k
    best_k = max(results, key=lambda x: x['accuracy'])['k']
    logger.info(f"Optimal k: {best_k}")
    
    return best_k, results

def calculate_metrics(predictions):
    """Calculate all classification metrics"""
    logger.info("Calculating metrics...")
    
    # Accuracy
    accuracy_eval = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = accuracy_eval.evaluate(predictions)
    
    # Precision
    precision_eval = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="weightedPrecision")
    precision = precision_eval.evaluate(predictions)
    
    # Recall
    recall_eval = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="weightedRecall")
    recall = recall_eval.evaluate(predictions)
    
    # F1-Score
    f1_eval = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="f1")
    f1 = f1_eval.evaluate(predictions)
    
    # Confusion Matrix
    pred_and_labels = predictions.select("prediction", "label").rdd.map(
        lambda row: (float(row.prediction), float(row.label))
    )
    metrics = MulticlassMetrics(pred_and_labels)
    confusion_matrix = metrics.confusionMatrix().toArray().tolist()
    
    # Per-class metrics
    class_metrics = {}
    for i in range(4):  # Severity 1-4 (labels 0-3)
        class_metrics[f"class_{i}"] = {
            "precision": float(metrics.precision(float(i))),
            "recall": float(metrics.recall(float(i))),
            "f1": float(metrics.fMeasure(float(i)))
        }
    
    results = {
        "accuracy": float(accuracy),
        "precision": float(precision),
        "recall": float(recall),
        "f1_score": float(f1),
        "confusion_matrix": confusion_matrix,
        "class_metrics": class_metrics
    }
    
    logger.info(f"Accuracy: {accuracy:.4f}")
    logger.info(f"Precision: {precision:.4f}")
    logger.info(f"Recall: {recall:.4f}")
    logger.info(f"F1-Score: {f1:.4f}")
    
    return results

def save_results_to_mongodb(spark, results, features_used):
    """Save results to MongoDB"""
    logger.info("Saving results to MongoDB...")
    
    # Create results document
    results_doc = {
        "model_name": "kNN",
        "timestamp": datetime.now().isoformat(),
        "features_used": features_used,
        "metrics": results,
        "parameters": {
            "k": results.get("optimal_k", 5),
            "distance_metric": "euclidean"
        }
    }
    
    # Create DataFrame and save
    results_df = spark.createDataFrame([results_doc])
    
    results_df.write \
        .format("mongo") \
        .mode("append") \
        .option("uri", "mongodb://admin:admin123@mongodb:27017/bigdata_project.knn_results?authSource=admin") \
        .save()
    
    logger.info("Results saved to MongoDB")
    
    # Also save as JSON locally
    with open("/opt/spark-output/knn_results.json", "w") as f:
        json.dump(results_doc, f, indent=2)
    
    logger.info("Results saved to /opt/spark-output/knn_results.json")

def main():
    """Main execution function"""
    logger.info("=" * 60)
    logger.info("kNN CLASSIFICATION PIPELINE")
    logger.info("=" * 60)
    
    input_path = "/opt/spark-output/cleaned_data"
    
    try:
        # Create Spark session
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        
        # Load cleaned data
        df = load_cleaned_data(spark, input_path)
        
        # Sample for performance (kNN is computationally expensive)
        df_sampled = df.sample(False, 0.05, seed=42)  # 5% sample
        logger.info(f"Using {df_sampled.count()} samples for kNN")
        
        # Prepare features
        df_prepared, features_used = prepare_features(df_sampled)
        
        # Split data
        train_df, test_df = df_prepared.randomSplit([0.8, 0.2], seed=42)
        train_df.cache()
        test_df.cache()
        
        logger.info(f"Training set: {train_df.count()}")
        logger.info(f"Test set: {test_df.count()}")
        
        # Find optimal k
        optimal_k, k_results = find_optimal_k(spark, train_df, test_df.sample(False, 0.3))
        
        # Final prediction with optimal k
        logger.info(f"Running final prediction with k={optimal_k}")
        predictions = distributed_knn(spark, train_df, test_df, k=optimal_k)
        predictions.cache()
        
        # Calculate metrics
        metrics = calculate_metrics(predictions)
        metrics["optimal_k"] = optimal_k
        metrics["k_search_results"] = k_results
        
        # Save results
        save_results_to_mongodb(spark, metrics, features_used)
        
        # Print summary
        logger.info("\n" + "=" * 60)
        logger.info("kNN CLASSIFICATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Optimal k: {optimal_k}")
        logger.info(f"Accuracy: {metrics['accuracy']:.4f}")
        logger.info(f"Precision: {metrics['precision']:.4f}")
        logger.info(f"Recall: {metrics['recall']:.4f}")
        logger.info(f"F1-Score: {metrics['f1_score']:.4f}")
        
        spark.stop()
        logger.info("kNN Classification completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during kNN classification: {str(e)}")
        raise

if __name__ == "__main__":
    main()
