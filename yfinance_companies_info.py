import os
import yfinance as yf
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Stocks list
tickers = ['EGIE3.SA', 'NEOE3.SA', 'RNEW11.SA', 'CPLE6.SA', 'TAEE11.SA', 'BOVA11.SA']

# BigQuery configuration
CREDENTIALS_PATH = os.getenv("CREDENTIALS_PATH")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = "alphavantage_bi"
TABLE_ID = "dimension_companies"

# Bigquery client creation
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
table_ref = client.dataset(DATASET_ID).table(TABLE_ID)

# Dataframe for data collection
dimension_companies = pd.DataFrame()

# Ticker data collection
for ticker in tickers:
    try:
        stock = yf.Ticker(ticker)
        info = stock.info

        company_data = {
            "ticker": ticker.replace('.SA', '.SAO'),
            "cmpy_name": info.get("longName", None),
            "cmpy_sector": info.get("sector", None),
            "cmpy_industry": info.get("industry", None),
            "cmpy_market": info.get("market", None),
            "cmpy_country": info.get("country", None),
            "cmpy_marketcap": info.get("marketCap", None),
            "cmpy_trailingpe": info.get("trailingPE", None),
            "cmpy_forwardpe": info.get("forwardPE", None),
            "cmpy_dividendyield": info.get("dividendYield", None),
            "cmpy_roe": info.get("returnOnEquity", None),
            "cmpy_eps": info.get("trailingEps", None),
            "cmpy_pricetobook": info.get("priceToBook", None),
            "last_updated": pd.Timestamp.now(),
        }

        dimension_companies = pd.concat([dimension_companies, pd.DataFrame([company_data])], ignore_index=True)

    except Exception as e:
        print(f"Erro ao buscar {ticker}: {e}")

# Define the BigQuery job configuration
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",  # 'replace' equivalent in BigQuery
    autodetect=True,
)

# Sends to BigQuery
try:
    job = client.load_table_from_dataframe(
        dataframe=dimension_companies,
        destination=table_ref,
        job_config=job_config,
    )
    job.result()  # Wait for the job to be completed
    print("Table successfully loaded into the database!")
except Exception as e:
    print(f"Error while loading: {e}")
