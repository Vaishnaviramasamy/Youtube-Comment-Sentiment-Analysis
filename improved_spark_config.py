import sparknlp
from pyspark.sql import SparkSession

# Create Spark session with proper configuration for UI
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

# Initialize Spark NLP with the existing session
spark = sparknlp.start(spark)

print("Spark UI URL:", spark.sparkContext.uiWebUrl)
print("Spark Version:", spark.version)

# Test with a simple operation
data = [("This is a positive comment",), ("This is a negative comment",)]
df = spark.createDataFrame(data, ["text"])
df.show()

print("Configuration test completed!")