Write-Host "Installing required packages..." -ForegroundColor Green
pip install -r requirements.txt

Write-Host "Starting YouTube Comment Sentiment Analyzer..." -ForegroundColor Green
python app.py