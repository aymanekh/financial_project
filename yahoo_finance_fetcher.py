import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_stock_data(tickers: List[str], start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Fetch stock data for given tickers and date range
    """
    try:
        data = yf.download(tickers, start=start_date, end=end_date)
        if data.empty:
            logging.warning(f"No data retrieved for tickers {tickers} between {start_date} and {end_date}")
        return data
    except Exception as e:
        logging.error(f"Error fetching stock data: {str(e)}")
        return pd.DataFrame()

def fetch_company_info(ticker: str) -> Dict[str, Any]:
    """
    Fetch company information for a given ticker
    """
    try:
        company = yf.Ticker(ticker)
        info = company.info
        if not info:
            logging.warning(f"No information retrieved for ticker {ticker}")
        return info
    except Exception as e:
        logging.error(f"Error fetching company info for {ticker}: {str(e)}")
        return {}

def validate_data(data: pd.DataFrame) -> bool:
    """
    Validate the fetched data
    """
    if data.empty:
        logging.error("Data validation failed: DataFrame is empty")
        return False
    if data.isnull().values.any():
        logging.warning("Data contains null values")
    return True

def main():
    # List of stock tickers to fetch
    tickers = ["AAPL", "GOOGL", "MSFT", "AMZN", "META"]
    
    # Date range for historical data
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)  # Last 30 days
    
    # Fetch historical stock data
    stock_data = fetch_stock_data(tickers, start_date, end_date)
    
    if validate_data(stock_data):
        logging.info("Stock data validation passed")
        print("Stock Data Sample:")
        print(stock_data.head())
    else:
        logging.error("Stock data validation failed")
    
    # Fetch company info for each ticker
    for ticker in tickers:
        company_info = fetch_company_info(ticker)
        if company_info:
            print(f"\nCompany Info for {ticker}:")
            print(f"Name: {company_info.get('longName', 'N/A')}")
            print(f"Sector: {company_info.get('sector', 'N/A')}")
            print(f"Industry: {company_info.get('industry', 'N/A')}")
            print(f"Market Cap: {company_info.get('marketCap', 'N/A')}")

if __name__ == "__main__":
    main()