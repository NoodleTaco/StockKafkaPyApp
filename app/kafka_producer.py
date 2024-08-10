import os
import yfinance as yf
from kafka import KafkaProducer
import json
import time

# Read Kafka bootstrap servers from environment variable
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

#Holds instance of KafkaProducer class, configtured to send messages to a Kafka Broken running on 9002
producer = KafkaProducer(
    bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
    #Defines how the data will be serialized, in this case convert dict to JSON then encode to bytes
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

#Takes a stock symbol as an arg and fetches latest sotkc data for it
def fetch_stock_data(symbol):
    #Creates an instance of yfinance's Ticker class, gives access to financial data for a given stock symbol
    stock = yf.Ticker(symbol) 
    #Fetch market data within the last day 
    data = stock.history(period = "1d")
    #Returns a dictionary containing the stock symbol, closing price, and unix timestamp
    return {
        "symbol": symbol,
        "price": data['Close'].iloc[-1],
        "timestamp": int(time.time())
    }

#Continously fetches and publishes stock data for a given symbol
def publish_stock_data(symbol):
    while True:
        data = fetch_stock_data(symbol)
        producer.send('stock_data', value=data)
        print(f"Published data for {symbol}")
        time.sleep(60)  

if __name__ == "__main__":
    import sys
    symbol = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    publish_stock_data(symbol)