from sensor.constant.training_pipeline import SCHEMA_FILE_PATH
from sensor.entity.artifact_entity import DataIngestionArtifact,DataValidationArtifact
from sensor.entity.config_entity import DataValidationConfig
from sensor.exception import SensorException
from sensor.logger import logging
from sensor.utils.main_utils import read_yaml_file,write_yaml_file
import pandas as pd
import sys,os
from scipy.stats import ks_2samp

class DataValidation:

    def __init__(self,
                 data_ingestion_artifact:DataIngestionArtifact,
                 data_validation_config:DataValidationConfig):
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self._schema_config = read_yaml_file(SCHEMA_FILE_PATH)

        except Exception as e:
            raise SensorException(e,sys)

    def drop_zero_std_column(self,dataframe:pd.DataFrame):
        pass

    def validate_number_of_columns(self, dataframe:pd.DataFrame)->bool:
        try:
            no_of_cols = len(self._schema_config["columns"])
            if len(dataframe.columns)== no_of_cols:
                return True
            return False
        except Exception as e:
            raise SensorException(e,sys)

    def is_numerical_column_exist(self, dataframe:pd.DataFrame)->bool:
        try:
            no_of_cols = self._schema_config["numerical_columns"]
            dataframe_cols = dataframe.columns

            numerical_column_present = True
            missing_numerical_columns = []
            for num_column in no_of_cols:
                if num_column not in dataframe_cols:
                    numerical_column_present = False
                    missing_numerical_columns.append(num_column)

            logging.info(f"Missing numerical columns: [{missing_numerical_columns}]")
            return numerical_column_present

        except Exception as e:
            raise SensorException(e,sys)

    @staticmethod
    def read_data(file_path)->pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise SensorException(e,sys)

    def detect_dataset_drift(self, base_df, current_df, threshold=0.05):
        try:
            status = True
            report = {}
            for column in base_df.columns:
                d1 = base_df[column]
                d2 = current_df[column]
                is_same_dist = ks_2samp(d1, d2)
                if threshold<=is_same_dist.pvalue:
                    is_found = False
                else:
                    is_found = True
                    status = False
                report.update({column :{
                    "p_value":float(is_same_dist.pvalue),
                    "drift_status": is_found
                }})

            drift_report_file_path = self.data_validation_config.drift_report_file_path

            # Create direcctory
            dir_path = os.path.dirname(drift_report_file_path)
            os.makedirs(dir_path,exist_ok=True)
            write_yaml_file(file_path=drift_report_file_path,content=report)

            return status

        except Exception as e:
            raise SensorException(e,sys)


    def initiate_data_validation(self)->DataValidationArtifact:
        try:
            error_msg = ""
            logging.info("Starting data validation")
            train_file_path = self.data_ingestion_artifact.trained_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            # Reading data from train and test file location
            train_dataframe = DataValidation.read_data(train_file_path)
            test_dataframe = DataValidation.read_data(test_file_path)

            # Validate number of columns
            status = self.validate_number_of_columns(train_dataframe)
            if not status:
                error_msg = f"{error_msg}Train dataframe doesnot contain all columns."
            status = self.validate_number_of_columns(test_dataframe)
            if not status:
                error_msg = f"{error_msg}Test dataframe doesnot contain all columns."

            #Validate numerical columns
            status = self.is_numerical_column_exist(train_dataframe)
            if not status:
                error_msg = f"{error_msg}Train dataframe doesnot contain all numerical columns."
            status = self.is_numerical_column_exist(test_dataframe)
            if not status:
                error_msg = f"{error_msg}Test dataframe doesnot contain all numerical columns."

            if len(error_msg)>0:
                raise Exception(error_msg)


            # Checking for data drift(checking if training and testing data distrubution is same or not)
            validation_status = self.detect_dataset_drift(base_df=train_dataframe,current_df=test_dataframe)

            data_validation_artifact = DataValidationArtifact(
                validation_status=validation_status,
                valid_train_file_path=self.data_ingestion_artifact.trained_file_path,
                valid_test_file_path=self.data_ingestion_artifact.test_file_path,
                # invalid_train_file_path=self.data_validation_config.invalid_train_file_path,
                invalid_train_file_path=False,
                # invalid_test_file_path=self.data_validation_config.invalid_test_file_path,
                invalid_test_file_path=False,
                drift_report_file_path=self.data_validation_config.drift_report_file_path,
            )

            return data_validation_artifact
        except Exception as e:
            raise SensorException(e,sys)