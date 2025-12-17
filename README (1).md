# YouTube Comment Sentiment Analyzer

A web application that analyzes YouTube comment sentiments using two approaches:
1. Spark NLP for advanced sentiment analysis (full version)
2. Mock sentiment analysis for demonstration purposes (simplified version)

This application allows you to upload comment datasets (CSV/JSON) and performs sentiment analysis to classify comments as Positive, Negative, or Neutral.

## Features

- Colorful and responsive web interface
- Upload CSV or JSON files containing YouTube comments
- Sentiment analysis using Spark NLP
- Visualization of sentiment distribution
- Access to Spark Web UI for monitoring

## Requirements

- Python 3.7+
- Java 8 or 11 (for Spark)
- pip (Python package installer)

## Installation

1. Clone or download this repository
2. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

## Usage

We provide two versions of the application:

### Option 1: Simplified Demo Version (Recommended for testing)
1. Run the simplified application:
   ```
   python simple_app.py
   ```

2. Open your web browser and go to `http://localhost:5000`

3. Upload a CSV or JSON file containing YouTube comments

4. View the sentiment analysis results (using mock data)

### Option 2: Full Version with Spark NLP
1. Run the full application:
   ```
   python app.py
   ```

2. Open your web browser and go to `http://localhost:5000`

3. Upload a CSV or JSON file containing YouTube comments

4. View the sentiment analysis results

5. Click on "Spark UI" button to access the Spark Web UI for monitoring

## File Format

The application expects a CSV or JSON file with a column containing the comments. If your CSV has a column named "comment" (case-insensitive), it will be automatically detected. Otherwise, the first column will be used.

Sample CSV format:
```csv
comment
This is a great video!
I didn't like this content.
The explanation was okay.
```

## How It Works

1. The application uses Spark NLP's pretrained sentiment detection model
2. Comments are processed through a pipeline that includes document assembly and sentiment analysis
3. Results are displayed with visualizations showing the distribution of sentiments

## Accessing Spark UI

Click the "Spark UI" button in the navigation bar to access the Spark Web UI, which provides monitoring and debugging information for the Spark jobs.