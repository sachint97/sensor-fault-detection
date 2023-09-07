from sensor.configuration.mongo_db_connection import MongoDBClient
from dotenv import load_dotenv
from sensor.pipeline.training_pipeline import TrainPipeline



if __name__ == "__main__":
    load_dotenv()
    train_pipeline = TrainPipeline()
    train_pipeline.run_pipeline()