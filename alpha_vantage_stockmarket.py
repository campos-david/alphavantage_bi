from prefect import task
import requests
import pandas as pd
import time
from google.cloud import bigquery
from google.oauth2 import service_account

@task
def run_b3_stocks_etl():
    credentials = service_account.Credentials.from_service_account_file(
        "electric-power-sector-stocks-fe99899c6911.json"
    )
    client = bigquery.Client(credentials=credentials, project="electric-power-sector-stocks")
    table_ref = client.dataset("alphavantage_bi").table("b3_stocks")

    API_KEY = "4H29A6N1IXR30ZSJ"
    FUNCTION = "TIME_SERIES_DAILY"
    OUTPUTSIZE = "compact"

    symbols = ['EGIE3.SAO', 'NEOE3.SAO', 'RNEW11.SAO', 'CPLE6.SAO', 'TAEE11.SAO', 'BOVA11.SAO']

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
    except:
        client.create_table(bigquery.Table(table_ref, schema=schema))

    for symbol in symbols:
        try:
            url = f"https://www.alphavantage.co/query?function={FUNCTION}&symbol={symbol}&apikey={API_KEY}&outputsize={OUTPUTSIZE}"
            response = requests.get(url, timeout=15)
            data = response.json()
            time_series = data.get("Time Series (Daily)", {})

            if not time_series:
                print(f"No data for {symbol}")
                continue

            df = pd.DataFrame.from_dict(time_series, orient="index")
            df.index = pd.to_datetime(df.index)
            df = df.astype(float)
            df = df.rename(columns={
                '1. open': 'open',
                '2. high': 'high',
                '3. low': 'low',
                '4. close': 'close',
                '5. volume': 'volume'
            })
            df["symbol"] = symbol
            df["date"] = df.index
            df = df[["symbol", "date", "open", "high", "low", "close", "volume"]]

            job = client.load_table_from_dataframe(df, table_ref)
            job.result()
            print(f"{symbol} processado com sucesso.")
            time.sleep(15)
        except Exception as e:
            print(f"Erro: {e}")
