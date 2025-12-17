import pyspark
from pyspark.sql import SparkSession
import time
import threading
import webbrowser
import os

def start_spark_with_ui():
    """Start Spark with UI enabled"""
    try:
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
            .getOrCreate()
        
        print("=" * 50)
        print("SPARK UI STARTED SUCCESSFULLY")
        print("=" * 50)
        print(f"Spark UI URL: {spark.sparkContext.uiWebUrl}")
        print(f"Spark Version: {spark.version}")
        print("=" * 50)
        
        # Create a simple DataFrame to generate UI activity
        data = [("Sample comment 1",), ("Sample comment 2",), ("Sample comment 3",)]
        df = spark.createDataFrame(data, ["comment"])
        
        # Cache and perform operations to populate UI
        df.cache()
        count = df.count()
        print(f"Created sample DataFrame with {count} rows")
        
        # Keep Spark alive
        print("\nSpark is running. Press Ctrl+C to stop.")
        print("You can access the Spark UI at the URL above.")
        
        try:
            while True:
                time.sleep(30)  # Keep alive for 30 seconds between checks
        except KeyboardInterrupt:
            print("\nStopping Spark...")
            spark.stop()
            print("Spark stopped.")
            
    except Exception as e:
        print(f"Error starting Spark: {e}")
        print("This might be due to Python worker connection issues.")

if __name__ == "__main__":
    # Create spark-logs directory if it doesn't exist
    os.makedirs("./spark-logs", exist_ok=True)
    
    # Start Spark UI
    start_spark_with_ui()