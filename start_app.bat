@echo off
echo Installing required packages...
pip install -r requirements.txt

echo Starting YouTube Comment Sentiment Analyzer...
python app.py

pause