import os
import sys

def main():
    print("YouTube Comment Sentiment Analyzer")
    print("==================================")
    print()
    print("Choose which version to run:")
    print("1. Full version with Spark NLP (requires heavy dependencies)")
    print("2. Simplified demo version (mock sentiment analysis)")
    print()
    
    choice = input("Enter your choice (1 or 2): ").strip()
    
    if choice == "1":
        print("Running full version with Spark NLP...")
        print("Note: This may take a while to start if dependencies need to be loaded.")
        os.system("python app.py")
    elif choice == "2":
        print("Running simplified demo version...")
        os.system("python simple_app.py")
    else:
        print("Invalid choice. Please run again and select 1 or 2.")
        sys.exit(1)

if __name__ == "__main__":
    main()