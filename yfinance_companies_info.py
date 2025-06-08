from prefect import task
import os
import yfinance as yf
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

@task
def run_dimension_companies_etl():
    tickers = ['EGIE3.SA', 'NEOE3.SA', 'RNEW11.SA', 'CPLE6.SA', 'TAEE11.SA', 'BOVA11.SA']

    credentials = service_account.Credentials.from_service_account_file(
        "electric-power-sector-stocks-fe99899c6911.json"
    )
    client = bigquery.Client(credentials=credentials, project="electric-power-sector-stocks")
    table_ref = client.dataset("alphavantage_bi").table("dimension_companies")

    df = pd.DataFrame()
    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            info = stock.info

            data = {
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

            df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)
        except Exception as e:
            print(f"Erro ao buscar {ticker}: {e}")

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
