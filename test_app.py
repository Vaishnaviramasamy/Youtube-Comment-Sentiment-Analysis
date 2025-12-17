import pandas as pd
import os

# Test the simplified app logic
def test_simplified_app():
    # Read the sample CSV
    df = pd.read_csv('sample_comments.csv')
    print("Original DataFrame:")
    print(df.head())
    
    # Simulate the column detection logic
    comments_column = None
    for col in df.columns:
        if 'comment' in col.lower():
            comments_column = col
            break
    
    if not comments_column:
        comments_column = df.columns[0]
    
    print(f"\nDetected comment column: {comments_column}")
    
    # Simulate the column renaming logic
    if comments_column != 'comment':
        df['comment'] = df[comments_column]
    
    print("\nDataFrame after adding 'comment' column:")
    print(df.head())
    
    # Simulate sentiment analysis
    import random
    def mock_sentiment(text):
        return random.choice(['positive', 'negative', 'neutral'])
    
    df['sentiment'] = df['comment'].apply(mock_sentiment)
    
    print("\nFinal DataFrame with sentiment:")
    print(df.head())
    
    # Save to a test file
    df.to_csv('test_output.csv', index=False)
    print("\nSaved to test_output.csv")

if __name__ == "__main__":
    test_simplified_app()