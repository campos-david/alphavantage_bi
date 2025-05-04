import requests
import pandas as pd
import time
from sqlalchemy import create_engine, text

API_KEY = "4H29A6N1IXR30ZSJ"
FUNCTION = "TIME_SERIES_DAILY"
OUTPUTSIZE = "compact"

# List of top 100 companies in Brazil (B3 exchange)
TOP_B3 = [
    'PETR4.SAO', 'VALE3.SAO', 'ITUB4.SAO',
]

# PostgreSQL configuration
PG_HOST = 'localhost'
PG_DB = 'alphavantage_bi'
PG_USER = 'postgres'
PG_PASSWORD = '46824682'
PG_PORT = '5432'

# Create PostgreSQL connection
engine = create_engine(f'postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}')

# Create table if not exists
try:
    with engine.connect() as conn:
        # Using text() wrapper for multi-line SQL
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS b3_stocks (
                symbol VARCHAR(10),
                date TIMESTAMP,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume NUMERIC,
                PRIMARY KEY (symbol, date)
            )
            """))
        conn.commit()  # Explicit commit
    print("Table verified/created successfully")
except Exception as e:
    print(f"Error creating table: {e}")

# Process each stock
for symbol in TOP_B3:
    try:
        # API request
        url = f"https://www.alphavantage.co/query?function={FUNCTION}&symbol={symbol}&apikey={API_KEY}&outputsize={OUTPUTSIZE}"
        headers = {'Accept-Charset': 'utf-8'}
        response = requests.get(url, headers=headers, timeout=15)
        data = response.json()
        
        # Extract time series data
        time_series = data.get("Time Series (Daily)", {})
        
        if not time_series:
            print(f"No data found for {symbol}")
            continue
            
        # Create temporary DataFrame
        df_temp = pd.DataFrame.from_dict(time_series, orient="index")
        df_temp.index = pd.to_datetime(df_temp.index)
        df_temp = df_temp.astype(float)
        
        # Standardize column names
        column_mapping = {
            '1. open': 'open',
            '2. high': 'high',
            '3. low': 'low',
            '4. close': 'close',
            '5. volume': 'volume'
        }
        df_temp = df_temp.rename(columns=column_mapping)
        
        # Add symbol and date columns
        df_temp['symbol'] = symbol
        df_temp['date'] = df_temp.index
        
        # Select only relevant columns
        df_temp = df_temp[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']]
        
        # Insert into PostgreSQL
        df_temp.to_sql(
            'b3_stocks',
            engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        
        print(f"Successfully processed {symbol}")
        time.sleep(15)  # Respect API rate limit (5 requests/minute)
        
    except requests.exceptions.RequestException as e:
        print(f"Request error for {symbol}: {str(e)[:100]}")
    except ValueError as e:
        print(f"Data processing error for {symbol}: {str(e)[:100]}")
    except Exception as e:
        print(f"Unexpected error for {symbol}: {str(e)[:100]}")

# Create indexes for better query performance
try:
    with engine.connect() as conn:
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_b3_stocks_symbol 
        ON b3_stocks (symbol)
        """))
        
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_b3_stocks_date 
        ON b3_stocks (date)
        """))
        
        conn.commit()  # Explicit commit
    
    print("Indexes created successfully")
except Exception as e:
    print(f"Error creating the Indexes: {e}")

print("Data processing complete!")