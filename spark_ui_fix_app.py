from flask import Flask, render_template, jsonify, redirect
import socket
import threading
import time
from pyspark.sql import SparkSession

app = Flask(__name__)

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
        
        # Run some operations to populate UI tabs
        run_sample_operations()
        
    except Exception as e:
        print(f"Error initializing Spark: {e}")

def run_sample_operations():
    """Run sample operations to populate all UI tabs"""
    global spark_session
    if spark_session:
        try:
            # Create sample data
            data = [("This is a positive comment",), ("This is a negative comment",), ("This is a neutral comment",)]
            df = spark_session.createDataFrame(data, ["text"])
            
            # Cache and perform operations to populate UI tabs
            df.cache()
            count = df.count()
            print(f"Processed {count} rows")
            
            # Show data
            df.show()
            
            # Create temp view for SQL tab
            df.createOrReplaceTempView("sample_data")
            result = spark_session.sql("SELECT * FROM sample_data WHERE text LIKE '%positive%'")
            result.show()
            
            # Persist for storage tab
            df.persist()
            
        except Exception as e:
            print(f"Error running sample operations: {e}")

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/spark-ui')
def spark_ui():
    # Check common Spark UI ports
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