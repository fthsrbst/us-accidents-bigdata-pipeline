"""
Random Forest Classification Job - PySpark
US Accidents Big Data Pipeline

This script performs:
1. Load cleaned data
2. Prepare features for Random Forest classification
3. Train Random Forest model for Severity prediction
4. Evaluate with Accuracy, Precision, Recall, F1-Score, AUC-ROC
5. Feature importance analysis
6. Save results to MongoDB
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import (
    MulticlassClassificationEvaluator, 
    BinaryClassificationEvaluator
)
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml import Pipeline
from pyspark.mllib.evaluation import MulticlassMetrics, BinaryClassificationMetrics
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RandomForestClassification")

def create_spark_session():
    """Create Spark session"""
    return SparkSession.builder \
        .appName("US_Accidents_RandomForest") \
        .config("spark.mongodb.output.uri", "mongodb://admin:admin123@mongodb:27017/bigdata_project.random_forest_results?authSource=admin") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

def load_cleaned_data(spark, input_path):
    """Load cleaned parquet data"""
    logger.info(f"Loading cleaned data from: {input_path}")
    
    df = spark.read.parquet(input_path)
    logger.info(f"Loaded {df.count()} records")
    return df

def prepare_features(df):
    """Prepare features for Random Forest"""
    logger.info("Preparing features...")
    
    # Numeric features
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
    logger.info(f"Using {len(existing_features)} features")
    
    # Fill nulls
    for col_name in existing_features:
        df = df.withColumn(col_name, when(col(col_name).isNull(), 0.0).otherwise(col(col_name).cast(DoubleType())))
    
    # Create label (Severity 1-4 -> 0-3)
    df = df.withColumn("label", (col("Severity") - 1).cast(DoubleType()))
    
    # Assemble features
    assembler = VectorAssembler(
        inputCols=existing_features,
        outputCol="features",
        handleInvalid="skip"
    )
    
    df_assembled = assembler.transform(df)
    
    # Select needed columns
    df_prepared = df_assembled.select("ID", "Severity", "label", "features")
    
    logger.info(f"Prepared {df_prepared.count()} records")
    return df_prepared, existing_features

def train_random_forest(train_df, test_df, feature_names):
    """Train Random Forest with hyperparameter tuning"""
    logger.info("Training Random Forest classifier...")
    
    # Create Random Forest classifier
    rf = RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        numTrees=100,
        maxDepth=10,
        minInstancesPerNode=10,
        seed=42
    )
    
    # Train model
    logger.info("Fitting model...")
    model = rf.fit(train_df)
    
    # Predictions
    predictions = model.transform(test_df)
    
    # Feature importance
    feature_importance = dict(zip(feature_names, model.featureImportances.toArray().tolist()))
    sorted_importance = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)
    
    logger.info("Top 10 Feature Importances:")
    for feature, importance in sorted_importance[:10]:
        logger.info(f"  {feature}: {importance:.4f}")
    
    return model, predictions, feature_importance

def calculate_metrics(predictions):
    """Calculate comprehensive classification metrics"""
    logger.info("Calculating metrics...")
    
    # Multiclass metrics
    evaluators = {
        "accuracy": MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy"),
        "precision": MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision"),
        "recall": MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall"),
        "f1": MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
    }
    
    metrics = {}
    for name, evaluator in evaluators.items():
        metrics[name] = float(evaluator.evaluate(predictions))
    
    # Confusion Matrix
    pred_and_labels = predictions.select("prediction", "label").rdd.map(
        lambda row: (float(row.prediction), float(row.label))
    )
    multi_metrics = MulticlassMetrics(pred_and_labels)
    confusion_matrix = multi_metrics.confusionMatrix().toArray().tolist()
    
    # Per-class metrics
    class_metrics = {}
    for i in range(4):
        try:
            class_metrics[f"severity_{i+1}"] = {
                "precision": float(multi_metrics.precision(float(i))),
                "recall": float(multi_metrics.recall(float(i))),
                "f1": float(multi_metrics.fMeasure(float(i)))
            }
        except:
            class_metrics[f"severity_{i+1}"] = {
                "precision": 0.0,
                "recall": 0.0,
                "f1": 0.0
            }
    
    # AUC-ROC (for binary - using One-vs-Rest for multiclass)
    try:
        # Convert to binary problem (Severity >= 3 is "severe")
        predictions_binary = predictions.withColumn(
            "binary_label", 
            when(col("label") >= 2, 1.0).otherwise(0.0)
        )
        predictions_binary = predictions_binary.withColumn(
            "binary_prediction",
            when(col("prediction") >= 2, 1.0).otherwise(0.0)
        )
        
        binary_eval = BinaryClassificationEvaluator(
            labelCol="binary_label",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )
        
        auc_roc = float(binary_eval.evaluate(predictions_binary))
    except:
        auc_roc = 0.0
    
    metrics["auc_roc"] = auc_roc
    metrics["confusion_matrix"] = confusion_matrix
    metrics["class_metrics"] = class_metrics
    
    # Log results
    logger.info(f"Accuracy: {metrics['accuracy']:.4f}")
    logger.info(f"Precision: {metrics['precision']:.4f}")
    logger.info(f"Recall: {metrics['recall']:.4f}")
    logger.info(f"F1-Score: {metrics['f1']:.4f}")
    logger.info(f"AUC-ROC: {metrics['auc_roc']:.4f}")
    
    return metrics

def plot_feature_importance(feature_importance, output_path):
    """Plot feature importance chart"""
    logger.info("Plotting feature importance...")
    
    sorted_features = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)[:15]
    features = [f[0] for f in sorted_features]
    importances = [f[1] for f in sorted_features]
    
    plt.figure(figsize=(12, 8))
    plt.barh(range(len(features)), importances, color='steelblue')
    plt.yticks(range(len(features)), features)
    plt.xlabel('Importance')
    plt.ylabel('Feature')
    plt.title('Random Forest - Feature Importance')
    plt.tight_layout()
    plt.savefig(f"{output_path}/rf_feature_importance.png", dpi=150)
    plt.close()
    
    logger.info(f"Saved feature importance plot to {output_path}/rf_feature_importance.png")

def plot_confusion_matrix(confusion_matrix, output_path):
    """Plot confusion matrix"""
    logger.info("Plotting confusion matrix...")
    
    plt.figure(figsize=(10, 8))
    cm = np.array(confusion_matrix)
    
    plt.imshow(cm, interpolation='nearest', cmap='Blues')
    plt.colorbar()
    
    classes = ['Severity 1', 'Severity 2', 'Severity 3', 'Severity 4']
    tick_marks = np.arange(len(classes))
    plt.xticks(tick_marks, classes, rotation=45)
    plt.yticks(tick_marks, classes)
    
    # Add text annotations
    thresh = cm.max() / 2.
    for i in range(cm.shape[0]):
        for j in range(cm.shape[1]):
            plt.text(j, i, format(int(cm[i, j]), 'd'),
                    ha="center", va="center",
                    color="white" if cm[i, j] > thresh else "black")
    
    plt.xlabel('Predicted')
    plt.ylabel('Actual')
    plt.title('Random Forest - Confusion Matrix')
    plt.tight_layout()
    plt.savefig(f"{output_path}/rf_confusion_matrix.png", dpi=150)
    plt.close()
    
    logger.info(f"Saved confusion matrix to {output_path}/rf_confusion_matrix.png")

def save_results_to_mongodb(spark, metrics, feature_importance, model_params):
    """Save results to MongoDB"""
    logger.info("Saving results to MongoDB...")
    
    # Prepare results document
    results_doc = {
        "model_name": "RandomForest",
        "timestamp": datetime.now().isoformat(),
        "metrics": {
            "accuracy": metrics["accuracy"],
            "precision": metrics["precision"],
            "recall": metrics["recall"],
            "f1_score": metrics["f1"],
            "auc_roc": metrics["auc_roc"]
        },
        "confusion_matrix": metrics["confusion_matrix"],
        "class_metrics": metrics["class_metrics"],
        "feature_importance": feature_importance,
        "parameters": model_params
    }
    
    # Save to MongoDB
    results_df = spark.createDataFrame([{
        "model_name": "RandomForest",
        "timestamp": datetime.now().isoformat(),
        "accuracy": metrics["accuracy"],
        "precision": metrics["precision"],
        "recall": metrics["recall"],
        "f1_score": metrics["f1"],
        "auc_roc": metrics["auc_roc"]
    }])
    
    results_df.write \
        .format("mongo") \
        .mode("append") \
        .option("uri", "mongodb://admin:admin123@mongodb:27017/bigdata_project.random_forest_results?authSource=admin") \
        .save()
    
    # Save JSON locally
    with open("/opt/spark-output/random_forest_results.json", "w") as f:
        json.dump(results_doc, f, indent=2, default=str)
    
    logger.info("Results saved to MongoDB and local file")

def main():
    """Main execution function"""
    logger.info("=" * 60)
    logger.info("RANDOM FOREST CLASSIFICATION PIPELINE")
    logger.info("=" * 60)
    
    input_path = "/opt/spark-output/cleaned_data"
    output_path = "/opt/spark-output/visualizations"
    
    try:
        # Create Spark session
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        
        # Load data
        df = load_cleaned_data(spark, input_path)
        
        # Sample for faster training
        df_sampled = df.sample(False, 0.1, seed=42)
        logger.info(f"Using {df_sampled.count()} samples")
        
        # Prepare features
        df_prepared, feature_names = prepare_features(df_sampled)
        
        # Split data
        train_df, test_df = df_prepared.randomSplit([0.8, 0.2], seed=42)
        train_df.cache()
        test_df.cache()
        
        logger.info(f"Training: {train_df.count()}, Testing: {test_df.count()}")
        
        # Train model
        model_params = {
            "numTrees": 100,
            "maxDepth": 10,
            "minInstancesPerNode": 10
        }
        
        model, predictions, feature_importance = train_random_forest(
            train_df, test_df, feature_names
        )
        
        # Calculate metrics
        metrics = calculate_metrics(predictions)
        
        # Create visualizations
        import os
        os.makedirs(output_path, exist_ok=True)
        
        plot_feature_importance(feature_importance, output_path)
        plot_confusion_matrix(metrics["confusion_matrix"], output_path)
        
        # Save results
        save_results_to_mongodb(spark, metrics, feature_importance, model_params)
        
        # Print summary
        logger.info("\n" + "=" * 60)
        logger.info("RANDOM FOREST CLASSIFICATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Accuracy: {metrics['accuracy']:.4f}")
        logger.info(f"Precision: {metrics['precision']:.4f}")
        logger.info(f"Recall: {metrics['recall']:.4f}")
        logger.info(f"F1-Score: {metrics['f1']:.4f}")
        logger.info(f"AUC-ROC: {metrics['auc_roc']:.4f}")
        
        spark.stop()
        logger.info("Random Forest Classification completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during Random Forest classification: {str(e)}")
        raise

if __name__ == "__main__":
    main()
