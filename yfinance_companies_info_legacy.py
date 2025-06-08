import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text

tickers = ['EGIE3.SA', 'NEOE3.SA', 'RNEW11.SA', 'CPLE6.SA', 'TAEE11.SA', 'BOVA11.SA']

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
            CREATE TABLE IF NOT EXISTS companies_info (
                ticker VARCHAR(10) PRIMARY KEY,
                cmpy_name VARCHAR(100),
                cmpy_sector VARCHAR(50),
                cmpy_industry VARCHAR(100),
                cmpy_market VARCHAR(50),
                cmpy_country VARCHAR(50),
                cmpy_marketcap NUMERIC(20,2),
                cmpy_trailingpe NUMERIC(10,4),
                cmpy_forwardpe NUMERIC(10,4),
                cmpy_dividendyield NUMERIC(10,4),
                cmpy_roe NUMERIC(10,4),
                cmpy_eps NUMERIC(10,4),
                cmpy_pricetobook NUMERIC(10,4),
                last_updated TIMESTAMP
            )
            """))
        conn.commit()  # Explicit commit
    print("Table verified/created successfully")
except Exception as e:
    print(f"Error creating table: {e}")

# Create empty DataFrame for the dimension table
dimension_companies = pd.DataFrame()

for ticker in tickers:
    try:
        # Fetch ticker data
        stock = yf.Ticker(ticker)
        info = stock.info  # Dictionary with 100+ fields

        # Select relevant fields
        company_data = {
            "ticker": ticker.replace('.SA', '.SAO'),  # Replace .SA with .SAO for B3
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

        # Add to DataFrame
        dimension_companies = pd.concat([dimension_companies, pd.DataFrame([company_data])], ignore_index=True)

    except Exception as e:
        print(f"Error fetching {ticker}: {e}")

# Save DataFrame to PostgreSQL
dimension_companies.to_sql(
            'companies_info',
            engine,
            if_exists='replace',
            index=False,
            method='multi'
    )
print("Dimension table saved successfully!")