from flask import Flask, render_template, request, jsonify, redirect
import pandas as pd
import os
import json
import socket
import threading
import time
import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['PROCESSED_FOLDER'] = 'processed'

# Create necessary directories
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['PROCESSED_FOLDER'], exist_ok=True)

# Global variable to store Spark session
spark_session = None

def initialize_spark():
    """Initialize Spark session in a separate thread"""
    global spark_session
    try:
        # Create Spark session with proper configuration for UI
        spark_session = SparkSession.builder \
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
        
        print("Spark initialized successfully!")
        print("Spark UI URL:", spark_session.sparkContext.uiWebUrl)
        
        # Run a simple test to ensure everything works
        run_simple_test()
        
    except Exception as e:
        print(f"Error initializing Spark: {e}")

def run_simple_test():
    """Run a simple test to ensure Spark is working properly"""
    global spark_session
    if spark_session:
        try:
            # Create sample data
            data = [("This is a test comment",)]
            df = spark_session.createDataFrame(data, ["text"])
            
            # Perform a simple operation
            count = df.count()
            print(f"Test successful: Processed {count} rows")
            
        except Exception as e:
            print(f"Error in simple test: {e}")

def mock_sentiment_analysis(text):
    """Mock sentiment analysis function to avoid Python worker issues"""
    sentiments = ['positive', 'negative', 'neutral']
    # Simple heuristic-based sentiment analysis
    positive_words = ['good', 'great', 'excellent', 'amazing', 'love', 'like', 'best', 'perfect', 'wonderful']
    negative_words = ['bad', 'terrible', 'awful', 'hate', 'dislike', 'worst', 'horrible', 'poor']
    
    text_lower = text.lower()
    positive_count = sum(1 for word in positive_words if word in text_lower)
    negative_count = sum(1 for word in negative_words if word in text_lower)
    
    if positive_count > negative_count:
        return 'positive'
    elif negative_count > positive_count:
        return 'negative'
    else:
        return random.choice(sentiments)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    global spark_session
    
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if file:
        try:
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
            
            # Apply mock sentiment analysis
            df['sentiment'] = df['comment'].apply(mock_sentiment_analysis)
            
            # Save results
            df.to_csv(processed_filepath, index=False)
            
            # If Spark is available, also process with Spark for UI demonstration
            if spark_session:
                try:
                    # Convert to Spark DataFrame
                    spark_df = spark_session.createDataFrame(df[['comment']])
                    
                    # Perform a simple count operation to show in Spark UI
                    row_count = spark_df.count()
                    print(f"Spark processed {row_count} rows")
                    
                    # Cache and persist to show in Storage tab
                    spark_df.cache()
                    spark_df.persist()
                    
                    # Create a temp view for SQL tab
                    spark_df.createOrReplaceTempView("comments")
                    
                    # Run a simple SQL query
                    result = spark_session.sql("SELECT COUNT(*) as count FROM comments")
                    result.collect()
                    
                    # Clean up
                    spark_df.unpersist()
                    spark_session.catalog.dropTempView("comments")
                    
                except Exception as e:
                    print(f"Warning: Spark processing failed: {e}")
                    # This is just for UI demonstration, so we continue even if Spark fails
            
            return jsonify({
                'success': True,
                'filename': processed_filename,
                'rows': len(df)
            })
            
        except Exception as e:
            print(f"Error processing file: {e}")
            return jsonify({'error': f'An error occurred while processing the file: {str(e)}'}), 500

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
    # Check common Spark UI ports
    hostname = socket.gethostname()
    for port in [4040, 4041, 4042, 4043, 4044, 4045]:
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
    hostname = socket.gethostname()
    available_ports = []
    
    for port in [4040, 4041, 4042, 4043, 4044, 4045]:
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

@app.route('/spark-status')
def spark_status():
    """Check if Spark is initialized and running"""
    global spark_session
    if spark_session:
        try:
            ui_url = spark_session.sparkContext.uiWebUrl
            return jsonify({
                "status": "running",
                "ui_url": ui_url,
                "message": "Spark is running and UI is available"
            })
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": f"Spark is initialized but UI is not available: {e}"
            })
    else:
        return jsonify({
            "status": "not_initialized",
            "message": "Spark is not yet initialized"
        })

if __name__ == '__main__':
    # Start Spark initialization in a separate thread
    spark_thread = threading.Thread(target=initialize_spark)
    spark_thread.daemon = True
    spark_thread.start()
    
    # Start Flask app
    app.run(debug=True, host='0.0.0.0', port=5000)