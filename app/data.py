

from utils import download_s3_folder, import_dataset, get_local_directory
import pandas as pd
import numpy as np
import boto3
from enum import Enum

class DataType(Enum):
    MDF="model"
    CSV='df'

class DataLocationType(Enum):
    S3="s3"
    KAFKA="kafka"
    LOCAL="local"
    ESGF='esgf'

class Data:
    """
    This class contains logic that handles the actual downloading / uploading of data to respective data sources.
    The Data object is standardized across all Hypervisor use locations, client, container, and server.
    """
    def __init__(self, dtype, data_location, s3_key=None, s3_bucket_name=None,  path=None, df=None):
        self.path = path
        self.dtype = dtype
        self.df = df
        self.data_location = data_location
        self.s3_key = s3_key
        self.s3_bucket_name = s3_bucket_name
        self.directory = get_local_directory()
        print("Data properties:", self.path, self.dtype, self.data_location, self.s3_key, self.s3_bucket_name)

        if self.data_location == DataLocationType.S3:
            self.s3 = boto3.resource("s3")
            
            # S3 bucket identifier
            self.s3_bucket = self.s3.Bucket(name=self.s3_bucket_name)
        else:
            pass

        self._load_data()

    def _load_data(self):
        data_object = self._load_object()
        if self.dtype == DataType.CSV:
            self.df = self._object_to_pandas(data_object)
        elif self.dtype == DataType.MDF:
            self.df = data_object
        print("Loaded object: {}".format(self.df))
        
    def _load_object(self):
        if self.data_location == DataLocationType.S3:
            print("Loading data from S3 bucket: {} Key: {}".format(self.s3_bucket_name, self.s3_key))
            data_object = self._load_object_from_s3()
            return data_object
        elif self.data_location == DataLocationType.ESGF:
            print("Loading data from ESGF bucket")
            # TODO implement this using the lazy loaders and selecting down to location and only relevant factors fir
            pass
        elif self.data_location == DataLocationType.KAFKA:
            pass
        else:
            pass

    def _load_object_from_s3(self):
        if self.dtype ==  DataType.MDF:
            download_s3_folder(self.s3_bucket, self.s3_key, local_dir=self.s3_key)
            return import_dataset(self.directory, self.s3_key)

        if self.dtype ==  DataType.CSV:
            
            # S3 object identifier
            obj = self.s3.Object(bucket_name=self.s3_bucket_name, key=self.s3_key)
            response = obj.get()

            print("Retrieved object from S3 {} Body:,".format(response['Body']))
            return response['Body']

    def _object_to_pandas(self, data_object):
        df = pd.read_csv(data_object)
        return df

   
    def upload_data(self, path, filename):
        self.df.save_csv(filename)
        self.s3_bucket.upload_file(path+filename, filename)

    def __str__(self):
        string = "Data Object"
        string += "\t Data Type {}".format(self.dtype)
        string += "\t Data Location Type {}".format(self.data_location)
        string += "\n S3 Location s3://{}/{}".format(self.s3_bucket_name, self.s3_key)
        string += "\n Data: {}".format(self.df)

        return string
