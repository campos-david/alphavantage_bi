import logging
import time
import requests
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """Fetch stock data from Alpha Vantage and load to BigQuery."""
    # Load credentials
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "electric-power-sector-stocks-fe99899c6911.json")
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project="electric-power-sector-stocks")
    table_ref = client.dataset("alphavantage_bi").table("b3_stocks")

    # Define schema for BigQuery table
    schema = [
        bigquery.SchemaField("symbol", "STRING"),
        bigquery.SchemaField("date", "TIMESTAMP"),
        bigquery.SchemaField("open", "FLOAT"),
        bigquery.SchemaField("high", "FLOAT"),
        bigquery.SchemaField("low", "FLOAT"),
        bigquery.SchemaField("close", "FLOAT"),
        bigquery.SchemaField("volume", "FLOAT"),
    ]

    # Check if table exists, create if it doesn't
    try:
        client.get_table(table_ref)
        logging.info("Table b3_stocks already exists.")
    except NotFound:
        logging.info("Creating table b3_stocks.")
        try:
            client.create_table(bigquery.Table(table_ref, schema=schema))
        except Exception as e:
            logging.error(f"Error creating table b3_stocks: {e}")
            raise

    # Alpha Vantage API configuration
    API_KEY = "4H29A6N1IXR30ZSJ"
    FUNCTION = "TIME_SERIES_DAILY"
    OUTPUTSIZE = "compact"
    symbols = ['EGIE3.SAO', 'NEOE3.SAO', 'RNEW11.SAO', 'CPLE6.SAO', 'TAEE11.SAO', 'BOVA11.SAO']

    for symbol in symbols:
        try:
            # Build and execute API request
            url = f"https://www.alphavantage.co/query?function={FUNCTION}&symbol={symbol}&apikey={API_KEY}&outputsize={OUTPUTSIZE}"
            headers = {'Accept-Charset': 'utf-8'}
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            data = response.json()
            time_series = data.get("Time Series (Daily)", {})

            if not time_series:
                logging.warning(f"No data found for {symbol}")
                continue

            # Create DataFrame
            df_temp = pd.DataFrame.from_dict(time_series, orient="index")
            df_temp.index = pd.to_datetime(df_temp.index)
            df_temp = df_temp.rename(columns={
                '1. open': 'open',
                '2. high': 'high',
                '3. low': 'low',
                '4. close': 'close',
                '5. volume': 'volume'
            })
            df_temp[['open', 'high', 'low', 'close', 'volume']] = df_temp[['open', 'high', 'low', 'close', 'volume']].apply(pd.to_numeric, errors='coerce')
            df_temp['symbol'] = symbol
            df_temp['date'] = df_temp.index
            df_temp = df_temp[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']]

            # Load data to BigQuery
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", schema=schema)
            job = client.load_table_from_dataframe(df_temp, table_ref, job_config=job_config)
            job.result()
            logging.info(f"Successfully processed {symbol}")
            time.sleep(12)  # Respect API rate limit (5 requests/minute)
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error for {symbol}: {str(e)[:100]}")
            continue
        except ValueError as e:
            logging.error(f"Data processing error for {symbol}: {str(e)[:100]}")
            continue
        except Exception as e:
            logging.error(f"Unexpected error for {symbol}: {str(e)[:100]}")
            continue

    logging.info("Data processing complete!")

if __name__ == "__main__":
    main()