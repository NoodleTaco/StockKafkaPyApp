import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
import json
import logging
from app.database import SessionLocal
from app.stock_service import create_stock_data
from datetime import datetime


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

def process_stock_data():
    #Initialize context in which the streaming job will run
    env = StreamExecutionEnvironment.get_execution_environment()

    #Start config of a new environment, specify the environment is configured for streaming, 
    # use blink planner to optimize batch and streaming process, build environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()

    #Create the execution environment 
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    #Creates a Kafka consumer that reads messages from the stock data topic
    kafka_consumer = FlinkKafkaConsumer(
        'stock_data',
        #Specifies that messages are expected to be simple JSON strings
        SimpleStringSchema(),
        #Dictionary of configuration properties
        properties={'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS, 'group.id': 'stock_processor'}
    )

    #Creates a Kafka producer that sends messages to the processed_stock_data topic
    kafka_producer = FlinkKafkaProducer(
        'processed_stock_data',
        SimpleStringSchema(),
        properties={'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
    )

    #Creates a data stream by adding the kafka consumer as a source to the flink streaming env 
    # (will receieve data from stock_data Kafka topic)
    stream = env.add_source(kafka_consumer)


    #Process each piece of stock data from Kafka
    def calculate_moving_average(data):
        logger.info(f"Received data: {data}")
        #parse incoming data to a dictionary
        data = json.loads(data)

        price = data['price']
        
        # Simplified moving average calculation (could be enhanced)
        moving_average = price  # This is a placeholder

        # Insert the processed data into the database
        db = SessionLocal()
        try:
            create_stock_data(
                db=db,
                symbol=data['symbol'],
                price=price,
                moving_average=moving_average,
                timestamp=datetime.fromtimestamp(data['timestamp'])
            )
        finally:
            db.close()

        # Prepare the processed data for Kafka
        data['moving_average'] = moving_average
        return json.dumps(data)

        return json.dumps(data)

    #applies calculate avg function to each element in the stream, creates a new stream of processed data
    processed_stream = stream.map(calculate_moving_average)

    #Send processed stream to Kafka producer 
    processed_stream.add_sink(kafka_producer)

    #Starts flink job, will continously process stock data in real time from Kafka, writing results back to Kafka
    env.execute("Stock Data Processor")

if __name__ == "__main__":
    process_stock_data()