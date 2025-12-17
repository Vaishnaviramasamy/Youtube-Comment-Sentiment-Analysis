# YouTube Comment Sentiment Analyzer - Summary

## Overview
This application analyzes YouTube comment sentiments using two approaches:
1. A full version using Spark NLP for advanced sentiment analysis
2. A simplified demo version using mock sentiment analysis

## Features Implemented
- Colorful and responsive web interface using Bootstrap and custom CSS
- Upload functionality for CSV and JSON files containing YouTube comments
- Sentiment analysis and visualization
- Spark Web UI integration button
- Sample dataset for testing

## Files Created
1. `app.py` - Full application with Spark NLP integration
2. `simple_app.py` - Simplified demo version with mock sentiment analysis
3. `templates/index.html` - Main upload page with colorful design
4. `templates/results.html` - Results page with sentiment visualization
5. `requirements.txt` - Dependencies list
6. `sample_comments.csv` - Sample dataset for testing
7. `start_app.bat` - Windows batch script to start the application
8. `start_app.ps1` - PowerShell script to start the application
9. `run_app.py` - Interactive script to choose which version to run
10. `README.md` - Detailed instructions and documentation

## How to Run the Application

### Method 1: Interactive Script (Recommended)
1. Open a terminal/command prompt
2. Navigate to the project directory: `cd c:\Users\mithu\comments`
3. Run the interactive script: `python run_app.py`
4. Choose which version to run when prompted
5. Open your web browser and go to `http://localhost:5000`

### Method 2: Run the Simplified Demo Version Directly
1. Open a terminal/command prompt
2. Navigate to the project directory: `cd c:\Users\mithu\comments`
3. Run the simplified version: `python simple_app.py`
4. Open your web browser and go to `http://localhost:5000`

### Method 3: Run the Full Version (Requires Spark NLP)
1. Make sure all dependencies are installed:
   ```
   pip install -r requirements.txt
   ```
2. Run the full version:
   ```
   python app.py
   ```
3. Open your web browser and go to `http://localhost:5000`

## Using the Application
1. On the main page, click the upload area or button to select a CSV or JSON file
2. The application will automatically detect the comment column
3. After processing, you'll be redirected to the results page
4. The results page shows:
   - A doughnut chart with sentiment distribution
   - Individual comments with color-coded sentiment labels
5. Use the "Spark UI" button to access Spark monitoring (in the full version)

## Sample Data Format
The application expects a CSV or JSON file with at least one column containing comments. If a column named "comment" (case-insensitive) exists, it will be used automatically. Otherwise, the first column will be used.

Sample CSV format:
```csv
comment
This is a great video!
I didn't like this content.
The explanation was okay.
```

## Technical Details
- Built with Flask web framework
- Uses Pandas for data processing
- Implements Spark NLP for advanced sentiment analysis in the full version
- Features a responsive design that works on desktop and mobile devices
- Includes Chart.js for data visualization

## Known Limitations
- The full version requires significant dependencies (PySpark, Spark NLP) that may take time to install
- The simplified version uses mock sentiment analysis for demonstration purposes
- Large datasets may take longer to process

## Future Enhancements
- Add support for direct YouTube API integration
- Implement more advanced sentiment analysis models
- Add export functionality for results
- Include additional data visualizations
- Add user authentication and data persistence