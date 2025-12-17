from flask import Flask, render_template, request, jsonify, redirect, url_for
import pandas as pd
import os
import json
import random

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['PROCESSED_FOLDER'] = 'processed'

# Create necessary directories
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['PROCESSED_FOLDER'], exist_ok=True)

def mock_sentiment_analysis(text):
    """Mock sentiment analysis function"""
    sentiments = ['positive', 'negative', 'neutral']
    return random.choice(sentiments)

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
            # Remove the original column if it's different from 'comment'
            df.drop(columns=[comments_column], inplace=True)
        
        # Apply mock sentiment analysis
        df['sentiment'] = df['comment'].apply(mock_sentiment_analysis)
        
        # Save results
        df.to_csv(processed_filepath, index=False)
        
        return jsonify({
            'success': True,
            'filename': processed_filename,
            'rows': len(df)
        })

@app.route('/results/<filename>')
def results(filename):
    filepath = os.path.join(app.config['PROCESSED_FOLDER'], filename)
    if not os.path.exists(filepath):
        return jsonify({'error': 'File not found'}), 404
    
    df = pd.read_csv(filepath)
    results_data = df.to_dict('records')
    
    # Create a simple sentiment timeline for the chart
    sentiment_timeline = []
    for i, record in enumerate(results_data):
        # Map sentiment to numeric values for timeline chart
        sentiment_value = 0  # neutral
        if record.get('sentiment') == 'positive':
            sentiment_value = 1
        elif record.get('sentiment') == 'negative':
            sentiment_value = -1
            
        sentiment_timeline.append({
            'index': i + 1,
            'value': sentiment_value,
            'sentiment': record.get('sentiment', 'neutral')
        })
    
    return render_template('results.html', results=results_data, filename=filename, sentiment_timeline=sentiment_timeline)

@app.route('/spark-ui')
def spark_ui():
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