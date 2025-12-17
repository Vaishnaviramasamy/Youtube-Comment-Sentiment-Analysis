from flask import Flask, render_template, request, jsonify, redirect, session, url_for
import pandas as pd
import os
import json
import socket
import random
import threading
import time
from collections import Counter
from datetime import datetime

app = Flask(__name__)
app.secret_key = 'your_secret_key_here'  # Required for sessions
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['PROCESSED_FOLDER'] = 'processed'

# Simple user storage (in a real app, this would be a database)
users = {
    'admin': {'password': 'admin123', 'email': 'admin@example.com', 'role': 'admin'},
    'user1': {'password': 'password123', 'email': 'user1@example.com', 'role': 'user'}
}

# Query storage (in a real app, this would be a database)
queries = []  # List of queries with {id, user, subject, message, status, created_at, response, responded_at, responded_by}
query_counter = 1

# Create necessary directories
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['PROCESSED_FOLDER'], exist_ok=True)

# Global variables
spark_available = False
spark_ui_url = None

def initialize_spark_check():
    """Check if Spark UI is available in a separate thread"""
    global spark_available, spark_ui_url
    # Check common Spark UI ports
    hostname = socket.gethostname()
    for port in [4040, 4041, 4042, 4043, 4044, 4045]:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex((hostname, port))
            sock.close()
            if result == 0:
                spark_available = True
                spark_ui_url = f'http://{hostname}:{port}'
                print(f"Spark UI detected at: {spark_ui_url}")
                return
        except:
            continue
    print("No Spark UI detected")

def mock_sentiment_analysis(text):
    """Mock sentiment analysis function"""
    sentiments = ['positive', 'negative', 'neutral']
    # Simple heuristic-based sentiment analysis
    positive_words = ['good', 'great', 'excellent', 'amazing', 'love', 'like', 'best', 'perfect', 'wonderful', 'awesome', 'fantastic', 'happy', 'pleased', 'satisfied', 'delighted']
    negative_words = ['bad', 'terrible', 'awful', 'hate', 'dislike', 'worst', 'horrible', 'poor', 'boring', 'confusing', 'waste', 'annoying', 'frustrating', 'disappointed', 'angry']
    
    text_lower = text.lower()
    positive_count = sum(1 for word in positive_words if word in text_lower)
    negative_count = sum(1 for word in negative_words if word in text_lower)
    
    if positive_count > negative_count:
        return 'positive'
    elif negative_count > positive_count:
        return 'negative'
    else:
        return random.choice(sentiments)

# Login required decorator
def login_required(f):
    def wrap(*args, **kwargs):
        if 'username' not in session:
            return redirect(url_for('login'))
        else:
            return f(*args, **kwargs)
    wrap.__name__ = f.__name__
    return wrap

# Admin required decorator
def admin_required(f):
    def wrap(*args, **kwargs):
        if 'username' not in session:
            return redirect(url_for('login'))
        username = session['username']
        if username not in users or users[username].get('role') != 'admin':
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    wrap.__name__ = f.__name__
    return wrap

@app.route('/')
@login_required
def index():
    return render_template('index.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    """User login page"""
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        # Check if user exists and password matches
        if username in users and users[username]['password'] == password:
            # Only allow non-admin users here
            if users[username].get('role') == 'admin':
                return render_template('login.html', error='Admin users must use the admin login page')
            
            session['username'] = username
            session['role'] = users[username].get('role', 'user')
            return redirect(url_for('index'))
        else:
            return render_template('login.html', error='Invalid username or password')
    
    # Check if user just registered
    registered = request.args.get('registered', 'false')
    success_msg = 'Registration successful! Please login to continue.' if registered == 'true' else None
    return render_template('login.html', success=success_msg)

@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    """Admin login page"""
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        # Check if user exists and password matches
        if username in users and users[username]['password'] == password:
            # Only allow admin users here
            if users[username].get('role') != 'admin':
                return render_template('admin_login.html', error='This login is for administrators only')
            
            session['username'] = username
            session['role'] = users[username].get('role', 'user')
            return redirect(url_for('admin_dashboard'))
        else:
            return render_template('admin_login.html', error='Invalid username or password')
    
    return render_template('admin_login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        email = request.form['email']
        password = request.form['password']
        confirm_password = request.form['confirmPassword']
        
        # Simple validation
        if password != confirm_password:
            return render_template('register.html', error='Passwords do not match')
        
        if len(password) < 6:
            return render_template('register.html', error='Password must be at least 6 characters long')
        
        if username in users:
            return render_template('register.html', error='Username already exists')
        
        # Add new user with default 'user' role
        users[username] = {'password': password, 'email': email, 'role': 'user'}
        # Don't log in automatically, redirect to login page
        return redirect(url_for('login', registered='true'))
    
    return render_template('register.html')

@app.route('/logout')
def logout():
    session.pop('username', None)
    return redirect(url_for('login'))

@app.route('/about')
@login_required
def about():
    return render_template('about.html')

@app.route('/contact')
@login_required
def contact():
    return render_template('contact.html')

@app.route('/queries')
@login_required
def user_queries():
    """User query page - users can submit and view their queries"""
    username = session['username']
    user_queries = [q for q in queries if q['user'] == username]
    return render_template('queries.html', queries=user_queries)

@app.route('/submit-query', methods=['POST'])
@login_required
def submit_query():
    """Submit a new query"""
    global query_counter
    username = session['username']
    subject = request.form.get('subject')
    message = request.form.get('message')
    
    if not subject or not message:
        return jsonify({'error': 'Subject and message are required'}), 400
    
    query = {
        'id': query_counter,
        'user': username,
        'subject': subject,
        'message': message,
        'status': 'pending',
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'response': None,
        'responded_at': None,
        'responded_by': None
    }
    queries.append(query)
    query_counter += 1
    
    return jsonify({'success': True, 'message': 'Query submitted successfully'})

@app.route('/admin/dashboard')
@admin_required
def admin_dashboard():
    """Admin dashboard - view all queries and respond"""
    # Calculate statistics
    total_queries = len(queries)
    pending_queries = len([q for q in queries if q['status'] == 'pending'])
    resolved_queries = len([q for q in queries if q['status'] == 'resolved'])
    
    return render_template('admin_dashboard.html', 
                         queries=queries,
                         total_queries=total_queries,
                         pending_queries=pending_queries,
                         resolved_queries=resolved_queries)

@app.route('/admin/respond-query', methods=['POST'])
@admin_required
def respond_query():
    """Admin responds to a query"""
    query_id = int(request.form.get('query_id'))
    response = request.form.get('response')
    username = session['username']
    
    if not response:
        return jsonify({'error': 'Response is required'}), 400
    
    # Find and update the query
    for query in queries:
        if query['id'] == query_id:
            query['response'] = response
            query['status'] = 'resolved'
            query['responded_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            query['responded_by'] = username
            return jsonify({'success': True, 'message': 'Response sent successfully'})
    
    return jsonify({'error': 'Query not found'}), 404

@app.route('/summary')
@login_required
def summary():
    # Check if we have processed files
    processed_files = os.listdir(app.config['PROCESSED_FOLDER'])
    if processed_files:
        # Get the most recent processed file
        latest_file = max(processed_files, key=lambda f: os.path.getctime(os.path.join(app.config['PROCESSED_FOLDER'], f)))
        filepath = os.path.join(app.config['PROCESSED_FOLDER'], latest_file)
        
        # Read the data
        df = pd.read_csv(filepath)
        results_data = df.to_dict('records')
        
        # Calculate statistics
        total_comments = len(results_data)
        sentiment_counts = Counter(item['sentiment'] for item in results_data)
        positive_count = sentiment_counts.get('positive', 0)
        negative_count = sentiment_counts.get('negative', 0)
        neutral_count = sentiment_counts.get('neutral', 0)
        
        positive_percentage = round((positive_count / total_comments) * 100, 2) if total_comments > 0 else 0
        negative_percentage = round((negative_count / total_comments) * 100, 2) if total_comments > 0 else 0
        neutral_percentage = round((neutral_count / total_comments) * 100, 2) if total_comments > 0 else 0
        
        # Generate insights
        insights = []
        if positive_percentage > 70:
            insights.append("High positive sentiment indicates strong audience approval")
        elif negative_percentage > 50:
            insights.append("Predominantly negative sentiment suggests content improvements are needed")
        else:
            insights.append("Mixed sentiment indicates varied audience opinions")
            
        if positive_count > negative_count:
            insights.append("Overall sentiment is positive, indicating good audience reception")
        elif negative_count > positive_count:
            insights.append("Overall sentiment is negative, suggesting areas for improvement")
            
        # Generate recommendations
        recommendations = []
        if positive_percentage > 70:
            recommendations.append("Continue creating content in this style as it resonates well with your audience")
        if negative_percentage > 30:
            recommendations.append("Review content quality and delivery based on negative feedback")
        if neutral_percentage > 50:
            recommendations.append("Try to create more engaging content to elicit stronger emotional responses")
            
        return render_template('summary.html', 
                             total_comments=total_comments,
                             positive_count=positive_count,
                             negative_count=negative_count,
                             neutral_count=neutral_count,
                             positive_percentage=positive_percentage,
                             negative_percentage=negative_percentage,
                             neutral_percentage=neutral_percentage,
                             insights=insights,
                             recommendations=recommendations,
                             results=results_data[:100])  # Limit to 100 for performance
    else:
        return render_template('summary.html', 
                             total_comments=0,
                             positive_count=0,
                             negative_count=0,
                             neutral_count=0,
                             positive_percentage=0,
                             negative_percentage=0,
                             neutral_percentage=0,
                             insights=["Upload a dataset to see analysis results"],
                             recommendations=["Based on your analysis, recommendations will appear here"],
                             results=[])

@app.route('/upload', methods=['POST'])
@login_required
def upload_file():
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
            
            return jsonify({
                'success': True,
                'filename': processed_filename,
                'rows': len(df)
            })
            
        except Exception as e:
            print(f"Error processing file: {e}")
            return jsonify({'error': f'An error occurred while processing the file: {str(e)}'}), 500

@app.route('/results/<filename>')
@login_required
def results(filename):
    filepath = os.path.join(app.config['PROCESSED_FOLDER'], filename)
    if not os.path.exists(filepath):
        return jsonify({'error': 'File not found'}), 404
    
    df = pd.read_csv(filepath)
    results_data = df.to_dict('records')
    
    # Calculate sentiment counts for the timeline chart
    sentiment_timeline = []
    for i, item in enumerate(results_data[:100]):  # Limit to 100 for performance
        sentiment_value = 1 if item['sentiment'] == 'positive' else (-1 if item['sentiment'] == 'negative' else 0)
        sentiment_timeline.append({
            'index': i+1,
            'sentiment': item['sentiment'],
            'value': sentiment_value
        })
    
    return render_template('results.html', 
                         results=results_data, 
                         filename=filename,
                         sentiment_timeline=sentiment_timeline)

@app.route('/spark-ui')
@login_required
def spark_ui():
    global spark_available, spark_ui_url
    
    # If we already know Spark UI URL, redirect to it
    if spark_ui_url:
        return redirect(spark_ui_url)
    
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

if __name__ == '__main__':
    # Start Spark availability check in a separate thread
    spark_thread = threading.Thread(target=initialize_spark_check)
    spark_thread.daemon = True
    spark_thread.start()
    
    # Start Flask app
    app.run(debug=True, host='0.0.0.0', port=5000)