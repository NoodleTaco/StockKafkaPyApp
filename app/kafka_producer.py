import os
import yfinance as yf
from kafka import KafkaProducer
import json
import time
import logging
from kafka.errors import KafkaError
# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Read Kafka bootstrap servers from environment variable
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

def create_producer():
    retry_count = 0
    while retry_count < 5:
        try:
            #Holds instance of KafkaProducer class, configtured to send messages to a Kafka Broken running on 9002
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            logger.info(f"Successfully connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except Exception as e:
            logger.error(f"Error connecting to Kafka: {str(e)}")
            retry_count += 1
            time.sleep(5)  # Wait 5 seconds before retrying
    
    raise Exception("Failed to connect to Kafka after multiple attempts")

#Takes a stock symbol as an arg and fetches latest sotkc data for it
def fetch_stock_data(symbol):
    try:
        #Creates an instance of yfinance's Ticker class, gives access to financial data for a given stock symbol
        stock = yf.Ticker(symbol) 
        #Fetch market data within the last day 
        data = stock.history(period = "1d")

        if data.empty:
            logger.warning(f"No data retrieved for symbol {symbol}")
            return None

        #Returns a dictionary containing the stock symbol, closing price, and unix timestamp
        return {
            "symbol": symbol,
            "price": data['Close'].iloc[-1],
            "timestamp": int(time.time())
        }
    except Exception as e:
        logger.error(f"Error fetching stock data for {symbol}: {str(e)}")
        return None

#Continously fetches and publishes stock data for a given symbol
def publish_stock_data(symbol):
    producer = create_producer()
    while True:
        try:
            data = fetch_stock_data(symbol)
            if data:
                future = producer.send('stock_data', value=data)
                record_metadata = future.get(timeout=10)
                logger.info(f"Published data for {symbol} to topic {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
            else:
                logger.warning(f"No data to publish for {symbol}")
            time.sleep(60)
        except KafkaError as e:
            logger.error(f"Error publishing data to Kafka: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
        time.sleep(60)  # Wait before trying again 

if __name__ == "__main__":
    import sys
    symbol = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    publish_stock_data(symbol)