import pyspark
from pyspark.sql import SparkSession
import time

# Create Spark session with UI enabled
spark = SparkSession.builder \
    .appName("YouTubeCommentSentimentAnalyzer") \
    .config("spark.ui.enabled", "true") \
    .config("spark.ui.port", "4040") \
    .config("spark.ui.bindAddress", "0.0.0.0") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "./spark-logs") \
    .getOrCreate()

print("Spark UI URL:", spark.sparkContext.uiWebUrl)
print("Spark Version:", spark.version)

# Keep the Spark session alive
try:
    # Run a simple operation to ensure UI is active
    data = [("Test comment 1",), ("Test comment 2",)]
    df = spark.createDataFrame(data, ["comment"])
    df.show()
    
    print("Spark is running with UI enabled. Press Ctrl+C to stop.")
    
    # Keep the application running
    while True:
        time.sleep(10)
        
except KeyboardInterrupt:
    print("Stopping Spark...")
    spark.stop()