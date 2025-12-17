import sparknlp

# Initialize Spark Session with Spark NLP
spark = sparknlp.start()

print("Spark UI URL:", spark.sparkContext.uiWebUrl)
print("Spark Version:", spark.version)