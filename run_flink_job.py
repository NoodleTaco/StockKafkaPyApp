import os
import sys

jar_path = os.path.join(os.path.dirname(__file__), 'lib', 'flink-connector-kafka-3.2.0-1.19.jar')
# Set the JAR file to be used by the Flink environment
os.environ['FLINK_ENV_JARFILES'] = f"file://{jar_path}"
os.environ['FLINK_JOB_MANAGER_RPC_ADDRESS'] = 'localhost'
os.environ['FLINK_JOB_MANAGER_PORT'] = '8081'

import logging
logging.getLogger("apache_beam").setLevel(logging.ERROR)

from app.flink_processor import process_stock_data

if __name__ == "__main__":
    process_stock_data()