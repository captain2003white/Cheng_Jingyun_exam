import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, count, desc

def main():
    # 1. Initialize SparkSession for Batch Analytics
    spark = SparkSession.builder \
        .appName("DataLakeAnalytics") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    
    CURATED_PATH = "/tmp/datalake/curated"

    # Get the absolute path of the current script (src/analytics.py)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Navigate one level up to the project root and point to outputs/analytics
    OUTPUT_CSV_DIR = os.path.abspath(os.path.join(script_dir, "..", "outputs", "analytics"))
    
    # Ensure the required deliverable directory exists
    os.makedirs(OUTPUT_CSV_DIR, exist_ok=True)
    
    print("=" * 60)
    print("LOADING CURATED ZONE DATA INTO SPARK SQL...")
    print("=" * 60)
    
    # 2. Read the partitioned Parquet files from Curated zone
    df_curated = spark.read.parquet(CURATED_PATH)
    df_curated.createOrReplaceTempView("curated_sensors")
    
    # ---------------------------------------------------------
    # Task 1: Analytical Query - Top 5 Hours with Most Anomalies
    # ---------------------------------------------------------
    print("\n[TASK 1] Executing Query: Top 5 Hours with Highest Anomaly Counts...")
    
    df_with_hour = df_curated.withColumn("hour_of_day", hour(col("event_time")))
    df_with_hour.createOrReplaceTempView("curated_sensors_with_hour")
    
    query_anomalies = """
        SELECT 
            sensor_type,
            year,
            month,
            day,
            hour_of_day,
            COUNT(*) AS anomaly_count
        FROM 
            curated_sensors_with_hour
        WHERE 
            is_anomaly = true
        GROUP BY 
            sensor_type, year, month, day, hour_of_day
        ORDER BY 
            anomaly_count DESC
        LIMIT 5
    """
    
    df_top_anomalies = spark.sql(query_anomalies)
    df_top_anomalies.show()
    
    # EXPORT REQUIREMENT: Save the analytical query result as a CSV file in the deliverables folder
    # Coalesce to 1 to produce a single clean CSV file instead of multiple distributed shards
    df_top_anomalies.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(OUTPUT_CSV_DIR)
        
    print(f"-> Successfully exported analytical query results to: {OUTPUT_CSV_DIR}/")
    
    # ---------------------------------------------------------
    # Task 2: Experimenting Partition Pruning Performance
    # ---------------------------------------------------------
    print("\n[TASK 2] Starting Partition Pruning Performance Experiment...")
    
    # Experiment A: With Partition Pruning
    start_time_a = time.time()
    df_pruned = spark.read.parquet(f"{CURATED_PATH}/sensor_type=temperature")
    count_a = df_pruned.filter(col("is_anomaly") == True).count()
    end_time_a = time.time()
    duration_a = (end_time_a - start_time_a) * 1000
    print(f"-> Experiment A (With Pruning) executed in: {duration_a:.2f} ms (Result count: {count_a})")
    
    # Experiment B: Without Partition Pruning
    start_time_b = time.time()
    df_full_scan = spark.read.parquet(CURATED_PATH)
    count_b = df_full_scan.filter((col("sensor_type") == "temperature") & (col("is_anomaly") == True)).count()
    end_time_b = time.time()
    duration_b = (end_time_b - start_time_b) * 1000
    print(f"-> Experiment B (Without Pruning) executed in: {duration_b:.2f} ms (Result count: {count_b})")
    
    # Calculate performance boost
    if duration_a > 0:
        speedup = duration_b / duration_a
        print(f"\n[CONCLUSION] Partition Pruning provided a {speedup:.2f}x speedup factor.")
    else:
        print("\n[CONCLUSION] Execution time too short to quantify exact speedup factor.")
        
    print("=" * 60)
    spark.stop()

if __name__ == "__main__":
    main()