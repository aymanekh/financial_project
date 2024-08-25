from kafka import KafkaConsumer
import json
import logging
from typing import Dict, Any
import sqlite3
from datetime import datetime
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'financial-data'

# SQLite configuration
DB_NAME = 'financial_data.db'

def create_kafka_consumer() -> KafkaConsumer:
    """Create and return a Kafka consumer"""
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='financial-data-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

def setup_database():
    """Set up SQLite database and create tables if they don't exist"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stock_data (
        date TEXT,
        ticker TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        PRIMARY KEY (date, ticker)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS company_info (
        ticker TEXT PRIMARY KEY,
        name TEXT,
        sector TEXT,
        market_cap REAL,
        last_updated TEXT
    )
    ''')
    
    conn.commit()
    conn.close()

def calculate_moving_average(data: pd.DataFrame, window: int = 5) -> pd.Series:
    """Calculate moving average for the closing price"""
    return data['Close'].rolling(window=window).mean()

def calculate_daily_returns(data: pd.DataFrame) -> pd.Series:
    """Calculate daily returns"""
    return data['Close'].pct_change()

def process_stock_data(data: Dict[str, Any]):
    """Process stock data and store in database"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for date, values in data.items():
        for ticker, price_data in values.items():
            cursor.execute('''
            INSERT OR REPLACE INTO stock_data (date, ticker, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (date, ticker, price_data['Open'], price_data['High'], price_data['Low'], price_data['Close'], price_data['Volume']))
    
    conn.commit()
    
    # Perform analysis
    for ticker in set(ticker for values in data.values() for ticker in values.keys()):
        cursor.execute('SELECT date, close FROM stock_data WHERE ticker = ? ORDER BY date', (ticker,))
        df = pd.DataFrame(cursor.fetchall(), columns=['Date', 'Close'])
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        
        ma = calculate_moving_average(df)
        returns = calculate_daily_returns(df)
        
        logging.info(f"Analysis for {ticker}:")
        logging.info(f"  Latest closing price: {df['Close'].iloc[-1]:.2f}")
        logging.info(f"  5-day moving average: {ma.iloc[-1]:.2f}")
        logging.info(f"  Latest daily return: {returns.iloc[-1]:.2%}")
    
    conn.close()

def process_company_info(data: Dict[str, Any]):
    """Process company info data and store in database"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    ticker = data.get('symbol', 'Unknown')
    name = data.get('longName', 'Unknown Company')
    sector = data.get('sector', 'Unknown Sector')
    market_cap = data.get('marketCap', 0)
    last_updated = datetime.now().isoformat()

    cursor.execute('''
    INSERT OR REPLACE INTO company_info (ticker, name, sector, market_cap, last_updated)
    VALUES (?, ?, ?, ?, ?)
    ''', (ticker, name, sector, market_cap, last_updated))

    conn.commit()
    conn.close()

    logging.info(f"Updated company info for {name} ({ticker})")

def main():
    setup_database()
    consumer = create_kafka_consumer()
    logging.info("Starting the advanced financial data consumer...")

    for message in consumer:
        try:
            key = message.key.decode('utf-8') if message.key else 'Unknown'
            value = message.value

            if 'timestamp' in value:
                logging.info(f"Received message with key: {key}, timestamp: {value['timestamp']}")
                data = value['data']
            else:
                logging.info(f"Received message with key: {key}")
                data = value

            if key == 'stock_data':
                process_stock_data(data)
            elif key.startswith('company_info_'):
                process_company_info(data)
            else:
                logging.warning(f"Unknown message type with key: {key}")

        except json.JSONDecodeError:
            logging.error(f"Failed to decode JSON from message: {message.value}")
        except Exception as e:
            logging.error(f"Error processing message: {str(e)}")

if __name__ == "__main__":
    main()