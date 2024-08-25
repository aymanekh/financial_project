import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any
import json
from kafka import KafkaProducer

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'financial-data'

def create_kafka_producer() -> KafkaProducer:
    """Create and return a Kafka producer"""
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def fetch_stock_data(tickers: List[str], start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """Fetch stock data for given tickers and date range"""
    try:
        data = yf.download(tickers, start=start_date, end=end_date)
        if data.empty:
            logging.warning(f"No data retrieved for tickers {tickers} between {start_date} and {end_date}")
        return data
    except Exception as e:
        logging.error(f"Error fetching stock data: {str(e)}")
        return pd.DataFrame()

def fetch_company_info(ticker: str) -> Dict[str, Any]:
    """Fetch company information for a given ticker"""
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
    """Validate the fetched data"""
    if data.empty:
        logging.error("Data validation failed: DataFrame is empty")
        return False
    if data.isnull().values.any():
        logging.warning("Data contains null values")
    return True

def send_to_kafka(producer: KafkaProducer, topic: str, key: str, value: Dict[str, Any]):
    """Send data to Kafka topic"""
    try:
        producer.send(topic, key=key.encode('utf-8'), value=value)
        producer.flush()
        logging.info(f"Sent data to Kafka topic {topic} with key {key}")
    except Exception as e:
        logging.error(f"Error sending data to Kafka: {str(e)}")

def main():
    producer = create_kafka_producer()

    # List of stock tickers to fetch
    tickers = ["AAPL", "GOOGL", "MSFT", "AMZN", "META"]
    
    # Date range for historical data
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)  # Last 30 days
    
    # Fetch historical stock data
    stock_data = fetch_stock_data(tickers, start_date, end_date)
    
    if validate_data(stock_data):
        logging.info("Stock data validation passed")
        # Convert DataFrame to dictionary and send to Kafka
        stock_dict = stock_data.to_dict(orient='index')
        send_to_kafka(producer, KAFKA_TOPIC, 'stock_data', stock_dict)
    else:
        logging.error("Stock data validation failed")
    
    # Fetch company info for each ticker
    for ticker in tickers:
        company_info = fetch_company_info(ticker)
        if company_info:
            send_to_kafka(producer, KAFKA_TOPIC, f'company_info_{ticker}', company_info)
    
    producer.close()

if __name__ == "__main__":
    main()