import sparknlp
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Spark Session with Spark NLP
spark = sparknlp.start()

# Configure Spark for complete UI functionality
spark.sparkContext.setLogLevel("INFO")
spark.conf.set("spark.eventLog.enabled", "true")
spark.conf.set("spark.eventLog.dir", "./spark-logs")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Set UI port explicitly
spark.conf.set("spark.ui.port", "4040")

# Enable all UI components
spark.conf.set("spark.ui.enabled", "true")
spark.conf.set("spark.ui.liveUpdate.period", "100ms")
spark.conf.set("spark.ui.liveUpdate.minFlushPeriod", "1s")
spark.conf.set("spark.ui.retainedJobs", "1000")
spark.conf.set("spark.ui.retainedStages", "1000")
spark.conf.set("spark.ui.retainedTasks", "100000")
spark.conf.set("spark.sql.ui.retainedExecutions", "1000")
spark.conf.set("spark.streaming.ui.retainedBatches", "1000")
spark.conf.set("spark.ui.retainedDeadExecutors", "100")

print("Spark UI URL:", spark.sparkContext.uiWebUrl)

# Create a simple DataFrame to test
schema = StructType([StructField("text", StringType(), True)])
data = [("This is a test comment",)]
df = spark.createDataFrame(data, schema)

# Perform some operations to generate UI events
df.cache()
count = df.count()
print(f"Processed {count} rows")

# Show the DataFrame
df.show()

# Print configuration
print("\nSpark Configuration:")
print("spark.ui.enabled:", spark.conf.get("spark.ui.enabled"))
print("spark.eventLog.enabled:", spark.conf.get("spark.eventLog.enabled"))
print("spark.ui.port:", spark.conf.get("spark.ui.port"))

print("\nTest completed successfully!")