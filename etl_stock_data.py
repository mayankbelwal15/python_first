import os
import requests
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from .env
load_dotenv()

#config
API_KEY = os.getenv("API_KEY")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

STOCK_SYMBOL = "AAPL"
INTERVAL = "1min"
OUTPUT_SIZE = 5
BASE_URL = "https://api.twelvedata.com/time_series"

#Step 1: extract stock data
def fetch_stock_data(symbol):
    params = { 
        "symbol": symbol,
        "interval": INTERVAL,
        "apikey": API_KEY,
        "outputsize": OUTPUT_SIZE,
        "format": "JSON" 
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()
    return data["values"] # Transform into DataFrame


# Step 2: Load into PostgreSQL
def insert_into_db(records, symbol):
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()

    for record in records:
        dt = datetime.strptime(record['datetime'], '%Y-%m-%d %H:%M:%S')
        cur.execute("""
            INSERT INTO stock_prices (symbol, datetime, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            symbol,
            dt,
            float(record['open']),
            float(record['high']),
            float(record['low']),
            float(record['close']),
            int(record['volume'])
        ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted {len(records)} records successfully!")

# Run the pipeline
def etl():
    print(f"Fetching stock data for {STOCK_SYMBOL}...")
    stock_data = fetch_stock_data(STOCK_SYMBOL)
    print(f"Fetched {len(stock_data)} records.")
    print("Inserting into database...")
    insert_into_db(stock_data, STOCK_SYMBOL)
    print("Done.")

if __name__ == "__main__":
    etl()
    