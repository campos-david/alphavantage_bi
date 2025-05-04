import requests
import pandas as pd
import time
from sqlalchemy import create_engine
from tqdm import tqdm

API_KEY = "4H29A6N1IXR30ZSJ"
FUNCTION = "TIME_SERIES_DAILY"
OUTPUTSIZE = "compact"

# List of top 100 companies in Brazil (B3 exchange)
TOP_B3 = [
    'PETR4.SAO', 'VALE3.SAO', 'ITUB4.SAO', 'BBDC4.SAO', 'B3SA3.SAO',
    'ABEV3.SAO', 'BBAS3.SAO', 'WEGE3.SAO', 'RENT3.SAO', 'ELET3.SAO',
    'ELET6.SAO', 'SUZB3.SAO', 'GGBR4.SAO', 'ITSA4.SAO', 'RAIZ4.SAO',
    'JBSS3.SAO', 'LREN3.SAO', 'PRIO3.SAO', 'EQTL3.SAO', 'BPAC11.SAO',
    'HAPV3.SAO', 'SANB11.SAO', 'VIVT3.SAO', 'CSAN3.SAO', 'BRFS3.SAO',
    'RADL3.SAO', 'CCRO3.SAO', 'UGPA3.SAO', 'EMBR3.SAO', 'TOTS3.SAO',
    'KLBN11.SAO', 'SBSP3.SAO', 'MGLU3.SAO', 'ENGI11.SAO', 'BRKM5.SAO',
    'CMIG4.SAO', 'TAEE11.SAO', 'CPLE6.SAO', 'BRDT3.SAO', 'CYRE3.SAO',
    'EZTC3.SAO', 'YDUQ3.SAO', 'MRFG3.SAO', 'MRVE3.SAO', 'CVCB3.SAO',
    'COGN3.SAO', 'PCAR3.SAO', 'BIDI11.SAO', 'BBDC3.SAO', 'QUAL3.SAO',
    'HYPE3.SAO', 'IRBR3.SAO', 'FLRY3.SAO', 'LWSA3.SAO', 'GOAU4.SAO',
    'USIM5.SAO', 'CRFB3.SAO', 'CPFE3.SAO', 'NTCO3.SAO', 'SOMA3.SAO',
    'VAMO3.SAO', 'CIEL3.SAO', 'AZUL4.SAO', 'CASH3.SAO', 'BEEF3.SAO',
    'MOVI3.SAO', 'GRND3.SAO', 'GOLL4.SAO', 'SMTO3.SAO', 'ECOR3.SAO',
    'ARZZ3.SAO', 'MULT3.SAO', 'AMER3.SAO', 'AURE3.SAO', 'TIMS3.SAO',
    'BPAN4.SAO', 'TRPL4.SAO', 'ENEV3.SAO', 'ALPA4.SAO', 'AESB3.SAO',
    'DIRR3.SAO', 'SLCE3.SAO', 'IGTI11.SAO', 'GMAT3.SAO', 'POMO4.SAO',
    'DMMO3.SAO', 'JHSF3.SAO', 'RCSL4.SAO', 'BRSR6.SAO', 'LEVE3.SAO',
    'TEND3.SAO', 'LIGT3.SAO', 'VULC3.SAO', 'SHUL4.SAO', 'FESA4.SAO',
    'TECN3.SAO', 'BMGB4.SAO', 'BGIP4.SAO', 'PARD3.SAO', 'PTBL3.SAO',
    'FRIO3.SAO', 'PNVL4.SAO', 'BRML3.SAO', 'SAPR11.SAO', 'OSXB3.SAO'
]

# PostgreSQL configuration
PG_HOST = 'localhost'
PG_DB = 'your_database'
PG_USER = 'postgres'
PG_PASSWORD = '46824682'
PG_PORT = '5432'

# Create PostgreSQL connection
engine = create_engine(f'postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}')

# Create table if not exists
try:
    with engine.connect() as conn:
        conn.execute("""
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
        """)
    print("Table verified/created successfully")
except Exception as e:
    print(f"Error creating table: {e}")

# Process each stock
for symbol in tqdm(TOP_B3, desc="Processing stocks"):
    try:
        # API request
        url = f"https://www.alphavantage.co/query?function={FUNCTION}&symbol={symbol}&apikey={API_KEY}&outputsize={OUTPUTSIZE}"
        response = requests.get(url, timeout=15)
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
        conn.execute("CREATE INDEX IF NOT EXISTS idx_b3_stocks_symbol ON b3_stocks (symbol)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_b3_stocks_date ON b3_stocks (date)")
    print("Indexes created successfully")
except Exception as e:
    print(f"Error creating indexes: {e}")

print("Data processing complete!")