from flask import Flask, render_template, request, jsonify, redirect, url_for
import pandas as pd
import os
import json
from pyspark.sql import SparkSession
import sparknlp
from pyspark.ml import Pipeline
import shutil

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['PROCESSED_FOLDER'] = 'processed'

# Create necessary directories
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['PROCESSED_FOLDER'], exist_ok=True)

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

# Import Spark NLP components after session initialization
from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp.common import *

# Ensure Spark UI is enabled and events are logged
spark.sparkContext.setLogLevel("INFO")

# Create Spark NLP pipeline for sentiment analysis
documentAssembler = DocumentAssembler()\
    .setInputCol("text")\
    .setOutputCol("document")

sentimentDetector = SentimentDetectorModel.pretrained()\
    .setInputCols(["document"])\
    .setOutputCol("sentiment_score")

finisher = Finisher()\
    .setInputCols(["sentiment_score"])\
    .setOutputCols(["sentiment"])

pipeline = Pipeline(stages=[
    documentAssembler,
    sentimentDetector,
    finisher
])

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if file:
        filename = file.filename
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        # Process the file based on extension
        if filename.endswith('.csv'):
            df = pd.read_csv(filepath)
        elif filename.endswith('.json'):
            df = pd.read_json(filepath)
        else:
            return jsonify({'error': 'Unsupported file format. Please upload CSV or JSON.'}), 400
        
        # Save processed data
        processed_filename = f"processed_{filename}"
        processed_filepath = os.path.join(app.config['PROCESSED_FOLDER'], processed_filename)
        
        # Assuming the column with comments is named 'comment'
        comments_column = None
        for col in df.columns:
            if 'comment' in col.lower():
                comments_column = col
                break
        
        if not comments_column:
            # If no comment column found, use the first column
            comments_column = df.columns[0]
        
        # Preserve the original comment column and rename it to 'comment' if needed
        if comments_column != 'comment':
            df['comment'] = df[comments_column]
        
        # Convert to Spark DataFrame
        spark_df = spark.createDataFrame(df[['comment']])
        spark_df = spark_df.withColumnRenamed("comment", "text")
        
        # Cache the DataFrame to ensure Spark processes it and generates events
        spark_df.cache()
        
        # Perform actions to trigger job execution and event logging for all UI components
        row_count = spark_df.count()
        print(f"Processing {row_count} rows of comment data")
        
        # Show DataFrame schema for debugging
        spark_df.printSchema()
        
        # Apply sentiment analysis
        model = pipeline.fit(spark_df)
        result = model.transform(spark_df)
        
        # Cache the result to ensure Spark processes it and generates events
        result.cache()
        
        # Perform actions on the result to trigger job execution and event logging
        result_count = result.count()
        print(f"Generated sentiment analysis for {result_count} comments")
        
        # Show some results for debugging
        result.show(5, truncate=False)
        
        # Trigger actions that will populate various UI tabs
        result.select("text").count()  # Stages tab
        result.persist()  # Storage tab
        
        # Trigger SQL operations that will populate the SQL tab
        result.createOrReplaceTempView("sentiment_results")
        sql_result = spark.sql("SELECT sentiment, COUNT(*) as count FROM sentiment_results GROUP BY sentiment")
        sql_result.show()
        sql_result.collect()  # Ensure the SQL job completes
        
        # Collect results
        result_pd = result.select("text", "sentiment").toPandas()
        # Rename 'text' back to 'comment' for consistency with template
        result_pd.rename(columns={'text': 'comment'}, inplace=True)
        result_pd.to_csv(processed_filepath, index=False)
        
        # Unpersist cached DataFrames to free memory
        spark_df.unpersist()
        result.unpersist()
        
        # Drop the temporary view
        spark.catalog.dropTempView("sentiment_results")
        
        # Trigger garbage collection to update UI
        import gc
        gc.collect()
        
        return jsonify({
            'success': True,
            'filename': processed_filename,
            'rows': len(result_pd)
        })

@app.route('/results/<filename>')
def results(filename):
    filepath = os.path.join(app.config['PROCESSED_FOLDER'], filename)
    if not os.path.exists(filepath):
        return jsonify({'error': 'File not found'}), 404
    
    df = pd.read_csv(filepath)
    results_data = df.to_dict('records')
    
    return render_template('results.html', results=results_data, filename=filename)

@app.route('/spark-ui')
def spark_ui():
    # Get Spark UI URL
    spark_ui_url = spark.sparkContext.uiWebUrl
    if spark_ui_url:
        return redirect(spark_ui_url)
    else:
        # Check common Spark UI ports
        import socket
        hostname = socket.gethostname()
        for port in [4040, 4041, 4042, 4043]:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1)
                result = sock.connect_ex((hostname, port))
                sock.close()
                if result == 0:
                    return redirect(f'http://{hostname}:{port}')
            except:
                continue
        # Fallback to the known Spark UI endpoint
        return redirect('http://MSI:4040')

@app.route('/spark-test')
def spark_test():
    return render_template('spark_test.html')

@app.route('/test-spark-ui')
def test_spark_ui():
    """
    Test endpoint to verify Spark UI connectivity
    """
    # Try to connect to common Spark UI ports
    import socket
    hostname = socket.gethostname()
    available_ports = []
    
    for port in [4040, 4041, 4042, 4043]:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex((hostname, port))
            sock.close()
            if result == 0:
                available_ports.append(port)
        except Exception as e:
            print(f"Error checking port {port}: {e}")
    
    if available_ports:
        return jsonify({
            "status": "success",
            "available_ports": available_ports,
            "message": f"Spark UI available on ports: {available_ports}"
        })
    else:
        return jsonify({
            "status": "error",
            "available_ports": [],
            "message": "No Spark UI instances found on common ports"
        })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)