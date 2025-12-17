import pyspark
from pyspark.sql import SparkSession
import time
import os
import sys

def start_spark_with_ui():
    """Start Spark with UI enabled and proper Python configuration"""
    try:
        # Get the current Python executable
        python_executable = sys.executable
        
        print(f"Using Python executable: {python_executable}")
        
        # Create Spark session with comprehensive UI configuration
        spark = SparkSession.builder \
            .appName("YouTubeCommentSentimentAnalyzer") \
            .config("spark.ui.enabled", "true") \
            .config("spark.ui.port", "4040") \
            .config("spark.ui.bindAddress", "0.0.0.0") \
            .config("spark.eventLog.enabled", "true") \
            .config("spark.eventLog.dir", "./spark-logs") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.ui.retainedExecutions", "100") \
            .config("spark.ui.retainedJobs", "1000") \
            .config("spark.ui.retainedStages", "1000") \
            .config("spark.ui.retainedTasks", "100000") \
            .config("spark.ui.liveUpdate.period", "100ms") \
            .config("spark.ui.liveUpdate.minFlushPeriod", "1s") \
            .config("spark.pyspark.python", python_executable) \
            .config("spark.pyspark.driver.python", python_executable) \
            .getOrCreate()
        
        print("=" * 50)
        print("SPARK UI STARTED SUCCESSFULLY")
        print("=" * 50)
        print(f"Spark UI URL: {spark.sparkContext.uiWebUrl}")
        print(f"Spark Version: {spark.version}")
        print("=" * 50)
        
        # Create a simple DataFrame using Scala operations (avoiding Python workers)
        # We'll use a simple range and map to avoid Python UDFs
        df = spark.range(1, 1000).selectExpr("CAST(id as STRING) as comment_id")
        
        # Cache and perform operations to populate UI
        df.cache()
        count = df.count()
        print(f"Created sample DataFrame with {count} rows")
        
        # Create a temporary view for SQL tab
        df.createOrReplaceTempView("sample_data")
        
        # Run a simple SQL query
        result = spark.sql("SELECT COUNT(*) as total FROM sample_data")
        sql_result = result.collect()
        print(f"SQL query result: {sql_result[0]['total']} rows")
        
        # Keep Spark alive
        print("\nSpark is running with UI enabled.")
        print("You can access the Spark UI at the URL above.")
        print("Press Ctrl+C to stop.")
        
        try:
            while True:
                time.sleep(30)  # Keep alive for 30 seconds between checks
        except KeyboardInterrupt:
            print("\nStopping Spark...")
            spark.stop()
            print("Spark stopped.")
            
    except Exception as e:
        print(f"Error starting Spark: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Create spark-logs directory if it doesn't exist
    os.makedirs("./spark-logs", exist_ok=True)
    
    # Start Spark UI
    start_spark_with_ui()