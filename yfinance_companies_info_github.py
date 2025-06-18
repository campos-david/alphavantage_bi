import logging
import time
import yfinance as yf
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """Fetch company info from yfinance and load to BigQuery."""
    # Load credentials
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_path:
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable not set")
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    project_id = os.getenv("GCP_PROJECT_ID")
    if not project_id:
        raise ValueError("GCP_PROJECT_ID environment variable not set")
    client = bigquery.Client(credentials=credentials, project=project_id)
    table_ref = client.dataset("alphavantage_bi").table("dimension_companies")

    # Define schema for BigQuery table
    schema = [
        bigquery.SchemaField("ticker", "STRING"),
        bigquery.SchemaField("cmpy_name", "STRING"),
        bigquery.SchemaField("cmpy_sector", "STRING"),
        bigquery.SchemaField("cmpy_industry", "STRING"),
        bigquery.SchemaField("cmpy_market", "STRING"),
        bigquery.SchemaField("cmpy_country", "STRING"),
        bigquery.SchemaField("cmpy_marketcap", "FLOAT"),
        bigquery.SchemaField("cmpy_trailingpe", "FLOAT"),
        bigquery.SchemaField("cmpy_forwardpe", "FLOAT"),
        bigquery.SchemaField("cmpy_dividendyield", "FLOAT"),
        bigquery.SchemaField("cmpy_roe", "FLOAT"),
        bigquery.SchemaField("cmpy_eps", "FLOAT"),
        bigquery.SchemaField("cmpy_pricetobook", "FLOAT"),
        bigquery.SchemaField("last_updated", "TIMESTAMP"),
    ]

    # Check if table exists, create if it doesn't
    try:
        client.get_table(table_ref)
        logging.info("Table dimension_companies already exists.")
    except NotFound:
        logging.info("Creating table dimension_companies.")
        try:
            client.create_table(bigquery.Table(table_ref, schema=schema))
        except Exception as e:
            logging.error(f"Error creating table dimension_companies: {e}")
            raise

    # List of tickers
    tickers = ['EGIE3.SA', 'NEOE3.SA', 'RNEW11.SA', 'CPLE6.SA', 'TAEE11.SA', 'BOVA11.SA']
    data_list = []

    for ticker in tickers:
        try:
            # Fetch ticker data
            stock = yf.Ticker(ticker)
            info = stock.info

            # Select relevant fields
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
            data_list.append(company_data)
            logging.info(f"Successfully fetched data for {ticker}")
            time.sleep(1)  # Avoid overwhelming yfinance API
        except Exception as e:
            logging.error(f"Error fetching {ticker}: {str(e)[:100]}")
            continue

    # Create DataFrame from collected data
    df = pd.DataFrame(data_list)
    if not df.empty:
        # Convert numeric columns to float
        numeric_cols = ['cmpy_marketcap', 'cmpy_trailingpe', 'cmpy_forwardpe', 
                        'cmpy_dividendyield', 'cmpy_roe', 'cmpy_eps', 'cmpy_pricetobook']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

        # Load data to BigQuery
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=schema)
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        logging.info("Dimension table saved successfully!")
    else:
        logging.warning("No data to load into dimension_companies table.")

if __name__ == "__main__":
    main()