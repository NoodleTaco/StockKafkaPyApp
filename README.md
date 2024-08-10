# Stock Data Analyzer

A stock data analysis application that integrates various technologies to provide real-time stock data and analytics.

## Tech Stack

- **Python**: language used for backend development
- **yfinance**: Library used to retrieve stock data
- **Apache Kafka**: Creates a data stream for the stock information
- **Apache Flink**: Processes stream and adds data to the DB
- **FastAPI**: For building the RESTful API
- **SQLAlchemy**: ORM for SQLite database management
- **Docker**: Containerizies backend elements
- **Docker Compose**: Orhcestrates the backend app, Kafka, and Flink
