#!/bin/bash
# ============================================
# COMPLETE PIPELINE EXECUTION SCRIPT
# Runs all pipeline stages in sequence
# ============================================

set -e

echo "============================================================"
echo "     US ACCIDENTS BIG DATA PIPELINE"
echo "     Complete Execution Script"
echo "============================================================"
echo ""

# Configuration
SPARK_MASTER="spark://spark-master:7077"
SPARK_APPS="/opt/spark-apps"
SPARK_OUTPUT="/opt/spark-output"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# ============================================
# STAGE 1: Data Cleaning
# ============================================
run_data_cleaning() {
    echo ""
    echo "============================================"
    echo "STAGE 1: DATA CLEANING"
    echo "============================================"
    
    log_info "Starting data cleaning job..."
    
    spark-submit \
        --master $SPARK_MASTER \
        --deploy-mode client \
        --driver-memory 4g \
        --executor-memory 4g \
        --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 \
        $SPARK_APPS/data_cleaning.py
    
    if [ $? -eq 0 ]; then
        log_info "Data cleaning completed successfully!"
    else
        log_error "Data cleaning failed!"
        exit 1
    fi
}

# ============================================
# STAGE 2: kNN Classification
# ============================================
run_knn_classification() {
    echo ""
    echo "============================================"
    echo "STAGE 2: kNN CLASSIFICATION"
    echo "============================================"
    
    log_info "Starting kNN classification job..."
    
    spark-submit \
        --master $SPARK_MASTER \
        --deploy-mode client \
        --driver-memory 4g \
        --executor-memory 4g \
        --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 \
        $SPARK_APPS/knn_classification.py
    
    if [ $? -eq 0 ]; then
        log_info "kNN classification completed successfully!"
    else
        log_error "kNN classification failed!"
        exit 1
    fi
}

# ============================================
# STAGE 3: Random Forest Classification
# ============================================
run_random_forest() {
    echo ""
    echo "============================================"
    echo "STAGE 3: RANDOM FOREST CLASSIFICATION"
    echo "============================================"
    
    log_info "Starting Random Forest classification job..."
    
    spark-submit \
        --master $SPARK_MASTER \
        --deploy-mode client \
        --driver-memory 4g \
        --executor-memory 4g \
        --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 \
        $SPARK_APPS/random_forest.py
    
    if [ $? -eq 0 ]; then
        log_info "Random Forest classification completed successfully!"
    else
        log_error "Random Forest classification failed!"
        exit 1
    fi
}

# ============================================
# STAGE 4: K-Means Clustering
# ============================================
run_kmeans_clustering() {
    echo ""
    echo "============================================"
    echo "STAGE 4: K-MEANS CLUSTERING"
    echo "============================================"
    
    log_info "Starting K-Means clustering job..."
    
    spark-submit \
        --master $SPARK_MASTER \
        --deploy-mode client \
        --driver-memory 4g \
        --executor-memory 4g \
        --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 \
        $SPARK_APPS/kmeans_clustering.py
    
    if [ $? -eq 0 ]; then
        log_info "K-Means clustering completed successfully!"
    else
        log_error "K-Means clustering failed!"
        exit 1
    fi
}

# ============================================
# SUMMARY
# ============================================
show_summary() {
    echo ""
    echo "============================================================"
    echo "     PIPELINE EXECUTION COMPLETE"
    echo "============================================================"
    echo ""
    echo "Output files:"
    echo "  - Cleaned data: $SPARK_OUTPUT/cleaned_data/"
    echo "  - kNN results: $SPARK_OUTPUT/knn_results.json"
    echo "  - Random Forest results: $SPARK_OUTPUT/random_forest_results.json"
    echo "  - K-Means results: $SPARK_OUTPUT/kmeans_results.json"
    echo ""
    echo "Visualizations:"
    ls -la $SPARK_OUTPUT/visualizations/ 2>/dev/null || echo "  (No visualizations yet)"
    echo ""
    echo "MongoDB Collections (bigdata_project):"
    echo "  - us_accidents_raw"
    echo "  - us_accidents_cleaned"
    echo "  - us_accidents_clustered"
    echo "  - knn_results"
    echo "  - random_forest_results"
    echo "  - kmeans_results"
    echo ""
    echo "Access points:"
    echo "  - Spark UI: http://localhost:8080"
    echo "  - MongoDB: mongodb://localhost:27017"
    echo "  - Jupyter: http://localhost:8888"
    echo ""
}

# ============================================
# MAIN EXECUTION
# ============================================
main() {
    log_info "Starting US Accidents Big Data Pipeline..."
    
    # Create output directories
    mkdir -p $SPARK_OUTPUT/visualizations
    mkdir -p $SPARK_OUTPUT/models
    
    # Run all stages
    run_data_cleaning
    run_knn_classification
    run_random_forest
    run_kmeans_clustering
    
    # Show summary
    show_summary
}

# Run main function
main "$@"
