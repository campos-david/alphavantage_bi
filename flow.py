from prefect import flow
from alpha_vantage_stockmarket import run_b3_stocks_etl
from yfinance_companies_info import run_dimension_companies_etl

@flow(name="ETL Orchestration Flow")
def main_flow():
    run_b3_stocks_etl()
    run_dimension_companies_etl()

if __name__ == "__main__":
    main_flow()
