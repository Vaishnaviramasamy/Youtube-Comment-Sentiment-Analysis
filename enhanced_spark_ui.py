import pyspark
from pyspark.sql import SparkSession
import time
import os
import sys

def start_enhanced_spark_with_ui():
    """Start Spark with enhanced UI configuration for better job and executor visualization"""
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
            .config("spark.executor.memory", "2g") \
            .config("spark.executor.cores", "2") \
            .config("spark.executor.instances", "2") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()
        
        print("=" * 50)
        print("SPARK UI STARTED SUCCESSFULLY")
        print("=" * 50)
        print(f"Spark UI URL: {spark.sparkContext.uiWebUrl}")
        print(f"Spark Version: {spark.version}")
        print("=" * 50)
        
        # Create sample DataFrames to generate UI activity
        # This will help populate the Jobs and Executors tabs
        print("Creating sample data to populate UI...")
        
        # Create a larger dataset to generate more jobs
        data = [(f"This is sample comment number {i}",) for i in range(1, 1000)]
        df = spark.createDataFrame(data, ["comment"])
        
        # Cache and perform operations to populate UI
        df.cache()
        count = df.count()
        print(f"Created sample DataFrame with {count} rows")
        
        # Create a temporary view for SQL tab
        df.createOrReplaceTempView("sample_comments")
        
        # Run multiple SQL queries to generate jobs
        queries = [
            "SELECT COUNT(*) as total FROM sample_comments",
            "SELECT * FROM sample_comments WHERE comment LIKE '%sample%' LIMIT 100",
            "SELECT LENGTH(comment) as length FROM sample_comments ORDER BY length DESC LIMIT 50"
        ]
        
        for i, query in enumerate(queries):
            result = spark.sql(query)
            sql_result = result.collect()
            print(f"SQL query {i+1} result: {len(sql_result)} rows")
        
        # Perform some transformations to generate more stages
        transformed_df = df.filter(df.comment.contains("sample")) \
                          .withColumn("word_count", spark.sql.functions.size(spark.sql.functions.split(spark.sql.functions.col("comment"), " "))) \
                          .groupBy("word_count").count()
        
        transformed_count = transformed_df.count()
        print(f"Transformed DataFrame count: {transformed_count}")
        
        # Keep Spark alive with periodic activity
        print("\nSpark is running with enhanced UI enabled.")
        print("You can access the Spark UI at the URL above.")
        print("All tabs (Jobs, Stages, Storage, Environment, Executors) should now be populated.")
        print("Press Ctrl+C to stop.")
        
        try:
            counter = 0
            while True:
                # Periodic activity to keep UI updated
                if counter % 10 == 0:  # Every 5 minutes
                    # Run a quick query to update the UI
                    quick_count = spark.sql("SELECT COUNT(*) as quick_count FROM sample_comments").collect()[0][0]
                    print(f"[HEARTBEAT] Quick check - DataFrame still has {quick_count} rows")
                
                time.sleep(30)  # Check every 30 seconds
                counter += 1
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
    start_enhanced_spark_with_ui()