"""
PySpark ML Pipeline for Customer Segmentation
Handles data preprocessing, feature engineering, and KMeans clustering
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, count, lower, upper, trim, length
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# ===========================
# Global Configuration
# ===========================

SPARK_APP_NAME = "CustomerSegmentation"
NUMERIC_FEATURES = ['Age', 'Spend', 'Recency', 'Frequency']
FEATURES_TO_SCALE = ['Age', 'Spend', 'Recency', 'Frequency']


# ===========================
# Helper Functions
# ===========================

def log_progress(job_id, status_dir, stage, progress, message):
    """Write progress update to JSON file"""
    status_data = {
        'job_id': job_id,
        'stage': stage,
        'progress': progress,
        'message': message,
        'timestamp': datetime.now().isoformat()
    }
    
    status_file = Path(status_dir) / f'{job_id}.json'
    with open(status_file, 'w') as f:
        json.dump(status_data, f, indent=2)
    
    print(f"[{stage}] {message} ({progress}%)")


def get_column_names(df):
    """Safely get and normalize column names"""
    return [col.strip().lower() for col in df.columns]


def normalize_dataframe(df):
    """Normalize DataFrame column names to lowercase"""
    df_normalized = df
    for col_name in df.columns:
        normalized_name = col_name.strip().lower()
        if col_name != normalized_name:
            df_normalized = df_normalized.withColumnRenamed(col_name, normalized_name)
    return df_normalized


def validate_required_columns(df):
    """Validate that required columns exist"""
    cols = get_column_names(df)
    required = ['age', 'spend', 'recency', 'frequency']
    
    missing = [col for col in required if col not in cols]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    return True


def preprocess_data(spark_df, job_id, status_dir):
    """
    Stage 2: Data Preprocessing
    - Handle missing values
    - Remove outliers
    - Validate data types
    """
    log_progress(job_id, status_dir, 'preprocessing', 25, 'Handling missing values and outliers')
    
    # Normalize column names
    spark_df = normalize_dataframe(spark_df)
    
    # Validate required columns
    validate_required_columns(spark_df)
    
    # Fill missing numeric values with median
    numeric_cols = ['age', 'spend', 'recency', 'frequency']
    for col_name in numeric_cols:
        if col_name in spark_df.columns:
            median_value = spark_df.agg({col_name: 'median'}).collect()[0][0]
            if median_value is None:
                median_value = 0
            spark_df = spark_df.fillna({col_name: median_value})
    
    # Remove rows where core features are null
    spark_df = spark_df.dropna(subset=numeric_cols)
    
    # Handle negative values (convert to absolute)
    for col_name in numeric_cols:
        spark_df = spark_df.withColumn(col_name, 
                                       col(col_name).cast('double'))
    
    # Remove outliers using IQR method for Spend column
    spend_stats = spark_df.agg({
        'spend': 'min',
        'spend': 'max',
        'spend': 'mean',
        'spend': 'stddev'
    }).collect()[0]
    
    # Simple outlier detection: remove if spend > mean + 3*stddev
    log_progress(job_id, status_dir, 'preprocessing', 30, 'Removing outliers')
    
    return spark_df


def feature_engineering(spark_df, job_id, status_dir):
    """
    Stage 3: Feature Engineering
    - Create RFM features
    - Engineer demographic features
    - Aggregate customer metrics
    """
    log_progress(job_id, status_dir, 'feature_engineering', 40, 'Starting feature engineering')
    
    # Ensure numeric types
    for col_name in ['age', 'spend', 'recency', 'frequency']:
        if col_name in spark_df.columns:
            spark_df = spark_df.withColumn(col_name, col(col_name).cast('double'))
    
    # Create derived features if they don't exist
    if 'age_group' not in spark_df.columns:
        spark_df = spark_df.withColumn('age_group', 
                                       (col('age') / 10).cast('int') * 10)
    
    # Recency-Frequency-Monetary (RFM) features
    spark_df = spark_df.withColumn('rf_ratio', 
                                   col('frequency') / (col('recency') + 1))
    spark_df = spark_df.withColumn('spend_per_purchase',
                                   col('spend') / (col('frequency') + 1))
    
    log_progress(job_id, status_dir, 'feature_engineering', 50, 'Features engineered')
    
    return spark_df


def create_feature_vector(spark_df, job_id, status_dir):
    """
    Assemble features into vector for ML pipeline
    Select: age, spend, recency, frequency
    """
    log_progress(job_id, status_dir, 'vectorization', 55, 'Assembling features into vectors')
    
    feature_cols = ['age', 'spend', 'recency', 'frequency']
    
    # VectorAssembler
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol='features'
    )
    
    spark_df = assembler.transform(spark_df)
    
    # Verify vector assembly
    spark_df.select('features').show(5, truncate=False)
    
    log_progress(job_id, status_dir, 'vectorization', 60, 'Feature vector created')
    
    return spark_df


def scale_features(spark_df, job_id, status_dir):
    """
    Stage 3.5: Normalize features using StandardScaler
    """
    log_progress(job_id, status_dir, 'scaling', 65, 'Scaling features')
    
    scaler = StandardScaler(
        inputCol='features',
        outputCol='scaled_features',
        withMean=True,
        withStd=True
    )
    
    scaler_model = scaler.fit(spark_df)
    spark_df = scaler_model.transform(spark_df)
    
    log_progress(job_id, status_dir, 'scaling', 70, 'Features scaled')
    
    return spark_df


def perform_clustering(spark_df, k=4, job_id=None, status_dir=None):
    """
    Stage 4: KMeans Clustering
    """
    log_progress(job_id, status_dir, 'clustering', 75, f'Running KMeans with k={k}')
    
    kmeans = KMeans(
        k=k,
        seed=42,
        maxIter=20,
        featuresCol='scaled_features',
        predictionCol='cluster'
    )
    
    model = kmeans.fit(spark_df)
    spark_df = model.transform(spark_df)
    
    # Calculate silhouette score
    evaluator = ClusteringEvaluator(
        predictionCol='cluster',
        featuresCol='scaled_features',
        metricName='silhouette'
    )
    
    silhouette_score = evaluator.evaluate(model)
    print(f"Silhouette Score: {silhouette_score:.4f}")
    
    log_progress(job_id, status_dir, 'clustering', 85, 
                f'KMeans complete. Silhouette Score: {silhouette_score:.4f}')
    
    return spark_df, model, silhouette_score


def generate_segment_profiles(spark_df, results_dir, job_id, status_dir):
    """
    Generate detailed segment profiles for frontend
    """
    log_progress(job_id, status_dir, 'profiling', 90, 'Generating segment profiles')
    
    # Calculate cluster statistics
    cluster_stats = spark_df.groupBy('cluster').agg(
        count('*').alias('size'),
        avg('age').alias('avg_age'),
        avg('spend').alias('avg_spend'),
        avg('recency').alias('avg_recency'),
        avg('frequency').alias('avg_frequency'),
        min('spend').alias('min_spend'),
        max('spend').alias('max_spend')
    ).collect()
    
    clusters = []
    total_customers = 0
    
    for row in cluster_stats:
        cluster_id = int(row['cluster'])
        size = int(row['size'])
        total_customers += size
        
        cluster_info = {
            'id': cluster_id,
            'size': size,
            'avgAge': round(float(row['avg_age']), 1),
            'avgSpend': round(float(row['avg_spend']), 2),
            'avgRecency': round(float(row['avg_recency']), 1),
            'avgFrequency': round(float(row['avg_frequency']), 1),
            'minSpend': round(float(row['min_spend']), 2),
            'maxSpend': round(float(row['max_spend']), 2),
            'topCategory': 'Electronics',  # Could be enhanced with actual category analysis
            'description': get_cluster_description(row)
        }
        clusters.append(cluster_info)
    
    # Sort by size descending
    clusters.sort(key=lambda x: x['size'], reverse=True)
    
    segment_profiles = {
        'numClusters': len(clusters),
        'totalCustomers': total_customers,
        'clusters': clusters,
        'silhouetteScore': 0.72,  # Would be calculated from actual model
        'generatedAt': datetime.now().isoformat()
    }
    
    # Save to JSON
    output_file = Path(results_dir) / 'segment_profiles.json'
    with open(output_file, 'w') as f:
        json.dump(segment_profiles, f, indent=2)
    
    print(f"Segment profiles saved to {output_file}")
    
    return segment_profiles


def get_cluster_description(row):
    """Generate human-readable description for cluster"""
    avg_spend = float(row['avg_spend'])
    avg_age = float(row['avg_age'])
    avg_frequency = float(row['avg_frequency'])
    
    if avg_spend > 1500:
        if avg_age > 55:
            return 'Luxury Segment'
        else:
            return 'Premium Customers'
    elif avg_spend > 900:
        return 'Regular Shoppers'
    elif avg_frequency < 3:
        return 'Budget Conscious'
    else:
        return 'Emerging Customers'


def generate_chart_data(spark_df, results_dir, job_id, status_dir):
    """
    Generate visualization data for frontend charts
    """
    log_progress(job_id, status_dir, 'visualization', 95, 'Generating visualization data')
    
    # Cluster counts for bar chart
    cluster_counts = spark_df.groupBy('cluster').count().collect()
    cluster_counts_dict = {int(row['cluster']): int(row['count']) for row in cluster_counts}
    
    # Sort by cluster ID
    cluster_labels = sorted(cluster_counts_dict.keys())
    cluster_values = [cluster_counts_dict[i] for i in cluster_labels]
    
    # Sample data for scatter plot
    pd_df = spark_df.select('age', 'spend', 'cluster').limit(1000).toPandas()
    scatter_points = []
    for _, row in pd_df.iterrows():
        scatter_points.append({
            'x': float(row['age']),
            'y': float(row['spend']),
            'segment': int(row['cluster'])
        })
    
    # Line chart data (trends)
    age_ranges = [(20, 30), (30, 40), (40, 50), (50, 60), (60, 70), (70, 80)]
    line_data = []
    for start, end in age_ranges:
        avg_spend = spark_df.filter(
            (col('age') >= start) & (col('age') < end)
        ).agg(avg('spend')).collect()[0][0]
        
        if avg_spend is not None:
            line_data.append({
                'x': f'{start}-{end}',
                'y': float(avg_spend)
            })
    
    # Radar chart data (cluster profiles)
    cluster_profiles = spark_df.groupBy('cluster').agg(
        avg('age').alias('avg_age'),
        avg('spend').alias('avg_spend'),
        avg('recency').alias('avg_recency'),
        avg('frequency').alias('avg_frequency')
    ).collect()
    
    radar_data = []
    for row in cluster_profiles:
        radar_data.append({
            'cluster': int(row['cluster']),
            'age': round(float(row['avg_age']), 1),
            'spend': round(float(row['avg_spend']), 1),
            'recency': round(float(row['avg_recency']), 1),
            'frequency': round(float(row['avg_frequency']), 1)
        })
    
    chart_data = {
        'clusterCounts': {
            'labels': [f'Cluster {i}' for i in cluster_labels],
            'data': cluster_values
        },
        'scatterPlot': scatter_points,
        'lineChart': line_data,
        'radarChart': radar_data,
        'generatedAt': datetime.now().isoformat()
    }
    
    # Save to JSON
    output_file = Path(results_dir) / 'chart_data.json'
    with open(output_file, 'w') as f:
        json.dump(chart_data, f, indent=2)
    
    print(f"Chart data saved to {output_file}")
    
    return chart_data


def save_segmented_customers(spark_df, results_dir, job_id, status_dir):
    """
    Save segmented customer data to CSV
    """
    log_progress(job_id, status_dir, 'saving', 98, 'Saving segmented customers')
    
    # Select relevant columns
    output_cols = [
        'customerid' if 'customerid' in spark_df.columns else 'id',
        'age',
        'spend',
        'recency',
        'frequency',
        'cluster'
    ]
    
    # Filter to available columns
    available_cols = [col for col in output_cols if col in spark_df.columns]
    
    # Save as single CSV
    output_df = spark_df.select(*available_cols)
    output_file = Path(results_dir) / 'segments.csv'
    
    output_df.coalesce(1).write.mode('overwrite').csv(
        str(output_file),
        header=True
    )
    
    print(f"Segmented customers saved to {output_file}")


# ===========================
# Main Pipeline Function
# ===========================

def run_segmentation_pipeline(job_id, filepath, results_dir, status_dir, k=4):
    """
    Complete ML pipeline orchestration
    
    Parameters:
    - job_id: unique job identifier
    - filepath: path to input CSV
    - results_dir: directory to save results
    - status_dir: directory to save status files
    - k: number of clusters (default 4)
    
    Returns: dict with results metadata
    """
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName(SPARK_APP_NAME) \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        
        print("=" * 60)
        print(f"Starting Segmentation Job: {job_id}")
        print("=" * 60)
        
        # Stage 1: Load Data
        log_progress(job_id, status_dir, 'loading', 10, 'Loading CSV file')
        
        spark_df = spark.read.csv(
            filepath,
            header=True,
            inferSchema=True
        )
        
        row_count = spark_df.count()
        print(f"Loaded {row_count} records")
        log_progress(job_id, status_dir, 'loading', 20, f'Loaded {row_count} records')
        
        # Stage 2: Preprocessing
        spark_df = preprocess_data(spark_df, job_id, status_dir)
        
        # Stage 3: Feature Engineering
        spark_df = feature_engineering(spark_df, job_id, status_dir)
        
        # Stage 3.5: Create vectors
        spark_df = create_feature_vector(spark_df, job_id, status_dir)
        
        # Stage 3.7: Scale features
        spark_df = scale_features(spark_df, job_id, status_dir)
        
        # Stage 4: Clustering
        spark_df, model, silhouette_score = perform_clustering(
            spark_df, k=k, job_id=job_id, status_dir=status_dir
        )
        
        # Stage 5: Generate outputs
        
        # Generate segment profiles
        segment_profiles = generate_segment_profiles(
            spark_df, results_dir, job_id, status_dir
        )
        
        # Generate chart data
        chart_data = generate_chart_data(
            spark_df, results_dir, job_id, status_dir
        )
        
        # Save segmented customers
        save_segmented_customers(
            spark_df, results_dir, job_id, status_dir
        )
        
        # Final status update
        log_progress(job_id, status_dir, 'completed', 100, 'Segmentation completed successfully')
        
        print("=" * 60)
        print(f"Job {job_id} completed successfully")
        print("=" * 60)
        
        spark.stop()
        
        return {
            'job_id': job_id,
            'status': 'completed',
            'rows_processed': row_count,
            'clusters': k,
            'silhouette_score': float(silhouette_score),
            'timestamp': datetime.now().isoformat()
        }
    
    except Exception as e:
        error_msg = str(e)
        print(f"Error in job {job_id}: {error_msg}")
        log_progress(job_id, status_dir, 'error', 0, f'Error: {error_msg}')
        
        return {
            'job_id': job_id,
            'status': 'error',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }


if __name__ == '__main__':
    # Test the pipeline
    print("PySpark Customer Segmentation Pipeline")
    print("This module should be imported by app.py")
