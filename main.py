from sensor.configuration.mongo_db_connection import MongoDBClient
from dotenv import load_dotenv
from sensor.pipeline.training_pipeline import TrainPipeline
from sensor.exception import SensorException
from sensor.logger import logging
import sys


if __name__ == "__main__":
    try:
        load_dotenv()
        train_pipeline = TrainPipeline()
        train_pipeline.run_pipeline()
    except Exception as e:
        raise SensorException(e,sys)