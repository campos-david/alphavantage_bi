import requests
import pandas as pd
import time
import os
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Google Cloud BigQuery configuration
CREDENTIALS_PATH = os.getenv("CREDENTIALS_PATH")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = "alphavantage_bi"
TABLE_ID = "b3_stocks"

# BigQuery client creation
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
table_ref = client.dataset(DATASET_ID).table(TABLE_ID)

# Alpha Vantage API configuration
API_KEY = os.getenv("API_KEY")
FUNCTION = "TIME_SERIES_DAILY"
OUTPUTSIZE = "compact"

B3_energy_stocks = [
    'EGIE3.SAO', 'NEOE3.SAO', 'RNEW11.SAO', 'CPLE6.SAO', 'TAEE11.SAO', 'BOVA11.SAO',
]

# Table schema definition and creation, if it does not exist
schema = [
    bigquery.SchemaField("symbol", "STRING"),
    bigquery.SchemaField("date", "TIMESTAMP"),
    bigquery.SchemaField("open", "FLOAT"),
    bigquery.SchemaField("high", "FLOAT"),
    bigquery.SchemaField("low", "FLOAT"),
    bigquery.SchemaField("close", "FLOAT"),
    bigquery.SchemaField("volume", "FLOAT"),
]

try:
    client.get_table(table_ref)
    print("Table already exists.")
except:
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table)
    print("Successfully created table.")

# Each stock processing
for symbol in B3_energy_stocks:
    try:
        url = f"https://www.alphavantage.co/query?function={FUNCTION}&symbol={symbol}&apikey={API_KEY}&outputsize={OUTPUTSIZE}"
        response = requests.get(url, timeout=15)
        data = response.json()
        time_series = data.get("Time Series (Daily)", {})

        if not time_series:
            print(f"No data found for {symbol} stock.")
            continue

        df_temp = pd.DataFrame.from_dict(time_series, orient="index")
        df_temp.index = pd.to_datetime(df_temp.index)
        df_temp = df_temp.astype(float)

        df_temp = df_temp.rename(columns={
            '1. open': 'open',
            '2. high': 'high',
            '3. low': 'low',
            '4. close': 'close',
            '5. volume': 'volume'
        })

        df_temp["symbol"] = symbol
        df_temp["date"] = df_temp.index
        df_temp = df_temp[["symbol", "date", "open", "high", "low", "close", "volume"]]

        # Load DataFrame into BigQuery
        job = client.load_table_from_dataframe(df_temp, table_ref)
        job.result()  # Wait for the job to complete

        print(f"{symbol} processado com sucesso.")
        time.sleep(15)  # Respect API rate limits

    except Exception as e:
        print(f"Error while processing {symbol}: {e}")

print("Processing completed.")
