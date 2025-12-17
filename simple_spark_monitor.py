import pyspark
from pyspark.sql import SparkSession
import time
import os
import sys

def start_simple_spark_monitor():
    """Start a simple Spark session for UI monitoring without Python workers"""
    try:
        print("Starting simple Spark monitor...")
        
        # Create Spark session with UI enabled but minimal Python dependencies
        spark = SparkSession.builder \
            .appName("YouTubeCommentSentimentAnalyzer-Monitor") \
            .config("spark.ui.enabled", "true") \
            .config("spark.ui.port", "4040") \
            .config("spark.ui.bindAddress", "0.0.0.0") \
            .config("spark.eventLog.enabled", "true") \
            .config("spark.eventLog.dir", "./spark-logs") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.ui.retainedJobs", "1000") \
            .config("spark.ui.retainedStages", "1000") \
            .config("spark.ui.retainedTasks", "100000") \
            .getOrCreate()
        
        print("=" * 50)
        print("SIMPLE SPARK MONITOR STARTED")
        print("=" * 50)
        print(f"Spark UI URL: {spark.sparkContext.uiWebUrl}")
        print(f"Spark Version: {spark.version}")
        print("=" * 50)
        
        # Create a simple DataFrame using only Scala operations (no Python UDFs)
        # This avoids Python worker issues
        df = spark.range(1, 100000).selectExpr("CAST(id as STRING) as comment_id")
        
        # Cache it to show in Storage tab
        df.cache()
        
        # Perform multiple operations to create 30-50 jobs in Spark UI
        print("Creating multiple Spark jobs for comprehensive monitoring...")
        
        # Job 1-10: Count operations
        for i in range(10):
            count = df.count()
            if i == 0:
                print(f"Sample DataFrame created with {count} rows")
        
        # Job 11-20: Filter and count operations
        for i in range(10):
            filtered_count = df.filter(f"CAST(comment_id AS INT) > {i * 10000}").count()
        
        # Job 21-30: Group by and aggregate operations
        for i in range(10):
            df.selectExpr(f"CAST(CAST(comment_id AS INT) % {(i+1)*10} AS STRING) as group_id").groupBy("group_id").count().collect()
        
        # Job 31-40: Join operations
        df2 = spark.range(1, 50000).selectExpr("CAST(id as STRING) as comment_id", "CAST(id * 2 as INT) as value")
        for i in range(10):
            df.join(df2, "comment_id", "inner").count()
        
        # Job 41-50: SQL operations
        df.createOrReplaceTempView("monitor_data")
        df2.createOrReplaceTempView("monitor_data2")
        for i in range(10):
            result = spark.sql(f"SELECT COUNT(*) as total FROM monitor_data WHERE CAST(comment_id AS INT) > {i * 10000}")
            result.collect()
        
        print(f"\nSuccessfully created 50+ Spark jobs for monitoring")
        
        # Create a temp view for SQL tab
        df.createOrReplaceTempView("monitor_data")
        
        # Run a simple SQL query
        result = spark.sql("SELECT COUNT(*) as total FROM monitor_data")
        sql_count = result.collect()[0][0]
        print(f"Total records in monitor: {sql_count}")
        
        # Keep the session alive
        print("\nSpark monitor is running.")
        print("Access the UI at the URL above to see Jobs, Stages, Storage, and Executors tabs.")
        print("Press Ctrl+C to stop.")
        
        # Periodic heartbeat to keep UI active and create additional jobs
        try:
            counter = 0
            while True:
                if counter % 10 == 0:  # Every 5 minutes
                    # Create additional jobs periodically
                    heartbeat_count = spark.sql("SELECT COUNT(*) as heartbeat FROM monitor_data").collect()[0][0]
                    print(f"[HEARTBEAT] Monitor active - {heartbeat_count} records")
                    
                    # Additional aggregation job
                    df.selectExpr("CAST(CAST(comment_id AS INT) % 100 AS STRING) as group").groupBy("group").count().collect()
                
                time.sleep(30)  # Sleep for 30 seconds
                counter += 1
                
        except KeyboardInterrupt:
            print("\nStopping Spark monitor...")
            spark.stop()
            print("Spark monitor stopped.")
            
    except Exception as e:
        print(f"Error starting Spark monitor: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Create spark-logs directory if it doesn't exist
    os.makedirs("./spark-logs", exist_ok=True)
    
    # Start the simple monitor
    start_simple_spark_monitor()