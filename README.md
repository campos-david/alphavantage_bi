# Stock Data Pipeline: Python to PostgreSQL to Power BI

## Project Description
# A complete data pipeline that:
# 1. Collects daily stock market data from Alpha Vantage API (Brazilian B3 exchange)
# 2. Processes and cleans the financial data using Python/Pandas
# 3. Stores the structured data in PostgreSQL with optimized indexing
# 4. Enables visualization through Power BI dashboards

## Technical Components
# - **Data Collection**: Python scripts with error handling for API rate limits
# - **Database**: PostgreSQL tables designed for time-series financial data
# - **ETL Process**: Automated transformation of raw API responses to clean tables
# - **Visualization**: Pre-configured Power BI templates for market analysis

## Data Flow
# Alpha Vantage API → Python (Pandas) → PostgreSQL → Power BI

## Supported Metrics
# - Daily Open/High/Low/Close prices
# - Trading volume
# - 100+ Brazilian stocks (PETR4, VALE3, etc.)

## Design Features
# - Automated daily updates
# - Data validation checks
# - Historical data preservation
# - Optimized query performance

# It's important to mention that the API could retrieve live data and the dashboard could be in live mode, however it's paid and #expensive. Another point is that as it' # a simple project, the ETL could be done directly into the PowerBI or the dataset could #be handled only in Python, it were not necessary to add SQL to the tech stack, # its just for research and study purposes.