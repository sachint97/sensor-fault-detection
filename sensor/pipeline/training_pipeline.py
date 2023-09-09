from sensor.entity.config_entity import (TrainingPipelineConfig,
                                            DataIngestionConfig,
                                            DataValidationConfig,
                                            DataTransformationConfig)
from sensor.entity.artifact_entity import (DataIngestionArtifact,
                                           DataValidationArtifact,
                                           DataTransformationArtifact)
from sensor.components.data_ingestion import DataIngestion
from sensor.components.data_validation import DataValidation
from sensor.components.data_transformation import DataTransformation
from sensor.exception import SensorException
from sensor.logger import logging
import sys
class TrainPipeline:

    def __init__(self):
        self.training_pipeline_config = TrainingPipelineConfig()
        self.data_validation_config = DataValidationConfig(self.training_pipeline_config)
        self.data_transformation_config = DataTransformationConfig(self.training_pipeline_config)

    def start_data_ingestion(self) -> DataIngestionArtifact:
        try:
            self.data_ingestion_config = DataIngestionConfig(training_pipeline_config=self.training_pipeline_config)

            logging.info("Starting data ingestion.")
            data_ingestion = DataIngestion(data_ingestion_config=self.data_ingestion_config)
            data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
            logging.info(f"Data ingestion completed and artifact : {data_ingestion_artifact}")

            return data_ingestion_artifact

        except Exception as e:
            SensorException(e,sys)

    def start_data_validation(
        self, data_ingestion_artifact: DataIngestionArtifact,
    ) -> DataValidationArtifact:

        logging.info("Entered the start_data_validation method of TrainPipeline class")

        try:
            data_validation = DataValidation(
                data_ingestion_artifact=data_ingestion_artifact,
                data_validation_config=self.data_validation_config,
            )

            data_validation_artifact = data_validation.initiate_data_validation()

            logging.info("Performed the data validation operation")

            logging.info(
                "Exited the start_data_validation method of TrainPipeline class"
            )

            return data_validation_artifact

        except Exception as e:
            raise SensorException(e, sys) from e

    def start_data_transformation(self, data_validation_artifact: DataValidationArtifact
    ) -> DataTransformationArtifact:
        try:
            data_transformation = DataTransformation(
                data_validation_artifact, self.data_transformation_config
            )

            data_transformation_artifact = (
                data_transformation.initiate_data_transformation()
            )

            return data_transformation_artifact
        except Exception as e:
            SensorException(e,sys)

    def star_model_trainer(self):
        try:
            pass
        except Exception as e:
            SensorException(e,sys)

    def start_model_evaluation(self):
        try:
            pass
        except Exception as e:
            SensorException(e,sys)

    def start_model_pusher(self):
        try:
            pass
        except Exception as e:
            SensorException(e,sys)

    def run_pipeline(self):
        try:
            data_ingestion_artifact:DataIngestionArtifact = self.start_data_ingestion()
            data_validation_artifact:DataValidationArtifact = self.start_data_validation(
                data_ingestion_artifact
            )
            data_transformation_artifact:DataTransformationArtifact = self.start_data_transformation(
                data_validation_artifact=data_validation_artifact)
        except Exception as e:
            SensorException(e,sys)