import logging
from logging.handlers import TimedRotatingFileHandler
import argparse             # for parsing command-line arguments
import os                   # to interact with environment variables
import requests
import pandas as pd         #for handling  and transforming tabular data
import psycopg2
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from .env into the script
load_dotenv()


# Logging configuration
logger = logging.getLogger("ETLLogger")
logger.setLevel(logging.INFO)

# File handler (rotates at midnight, keeps last 7 days)
file_handler = TimedRotatingFileHandler("etl.log", when="midnight", interval=1, backupCount=7)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

# Attach handlers
logger.addHandler(file_handler)
logger.addHandler(console_handler)


#config
API_KEY = os.getenv("API_KEY")
# Load PostgreSQL DB credentials from .env (use defaults if not set)
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

# STOCK_SYMBOL = "AAPL"
# INTERVAL = "1min"
OUTPUT_SIZE = 5000
BASE_URL = "https://api.twelvedata.com/time_series"

#Step 1: extract stock data
def fetch_stock_data(symbol, interval, start_date, end_date):
    
    """
    Fetch historical stock data from the Twelve Data API based on user input.

    Parameters:
        symbol (str): Stock symbol (e.g. INFY.NSE)
        interval (str): Time interval (e.g. 1min, 1h, daily)
        start_date, end_date (str): (YYYY-MM-DD)

    Returns: 
        pd.DataFrame: DataFrame with price and volume data
    """
    # BASE_URL = "https://api.twelvedata.com/time_series"

    logger.info(f"Fetching data for {symbol} from {start_date} to {end_date} at interval {interval}")


    params = { 
        "symbol": symbol,
        "interval": interval,
        "start_date": start_date,
        "end_date": end_date,
        "apikey": API_KEY,
        "outputsize": OUTPUT_SIZE,
        "format": "JSON" 
    }
    #Make API call
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()
    # return data["values"] # Transform into DataFrame

#Handle error response
    if "values" not in data:
        raise Exception(f"API Error: {data.get('message', 'Unknown error')}")

# Covert API response to DataFrame
    df = pd.DataFrame(data["values"])
    df["symbol"] = symbol # Add symbol as a column
    df["datetime"] = pd.to_datetime(df["datetime"]) #Convert to datetime object

    return df 


# Step 2: Load into PostgreSQL
def insert_into_db(df):
    """
    Insert DataFrame rows into PostreSQL database.

    Parameters:
        df (pd.DataFrame): Cleaned stock data
    """

    logger.info("Inserting data into PostgreSQL database...")


    try:
        # Establish connection to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cur = conn.cursor()

        # create table if doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS stock_data (
            symbol TEXT,
            datetime TIMESTAMP,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT
        );
        """
        cur.execute(create_table_query)
        conn.commit()

        #Insert each row into DB
        for _, row in df.iterrows():
            # dt = datetime.strptime(row['datetime'], '%Y-%m-%d %H:%M:%S')
            cur.execute(
                """
                INSERT INTO stock_prices (symbol, datetime, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
                """, 
                (
                    row["symbol"],
                    row["datetime"],
                    float(row["open"]),
                    float(row["high"]),
                    float(row["low"]),
                    float(row["close"]),
                    int(row["volume"])
            ),
        )

        conn.commit()
        cur.close()
        conn.close()
        logger.info("Data inserted into database successfully.")

    except Exception as e:
        
        logger.error(f"Error inserting data into DB: {e}")

# Run the pipeline
def main():
    """
    Main execution function:
    - Parses arguments
    - Reads stock symbols from stocks.txt
    - Calls extract and load functions for each symbol
    """

    #Define cmd line args
    parser = argparse.ArgumentParser(description="Fetch historical stock data from Twelve Data.")
    # parser.add_argument("--symbol", required=True, help="Stock symbol (e.g., INFY.NSE)")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--interval", default="1h", help="Time interval (e.g., 1min, 5min, 1h, daily)")
    
    # Parse user input
    args = parser.parse_args()

    try:
        with open("stocks.txt", "r") as f:
            symbols = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        logger.error("stocks.txt file not found.")
        return



    for symbol in symbols:
        try:
            logger.info(f"Starting ETL for {symbol}")
            df = fetch_stock_data(symbol, args.interval, args.start, args.end)
            logger.info(f"Fetched {len(df)} records for {symbol}.")            
            insert_into_db(df)
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            

# Run the script
if __name__ == "__main__":
    main()