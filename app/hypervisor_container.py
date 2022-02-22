

from typing import Container
from climate_ensembles.data import Data, DataLocationType, DataType
from climate_tasks import aggregate_models, apply_bias_correction, select_location
import pandas as pd
import numpy as np
from enum import Enum
import argparse

# TEST_DATA_S3_URI = "s3://climate-ensembling/test_data.csv"
TEST_DATA_KEY = "tst/EC-Earth3/"
TEST_DATA_BUCKET = "climate-ensembling"

"""
How to structure the data in S3 for flexible / automatic data finding?

Possibilities:

Example 1:

climate-ensembling/<task-1>/<task-2>/..../<task-n>
climate-ensembling/<model>/<select_city>/<bias_correction>/<aggregation>

Example 2:
???

"""

class ContainerHypervisor():
    def __init__(self):
        self.data = {}


    def parse(self, verbose=True):
        assert False, "Please use a subclass the implements this method."

    def retrieve_inputs(self, inputs, *args):
        assert False, "Please use a subclass the implements this method."

    def upload_outputs(self, outputs):
        assert False, "Please use a subclass the implements this method."
    
    def run_task(self, task, data, parameters):
        """
        Returns the dictionary of outputs from the task.
        """
        assert False, "Please use a subclass the implements this method."

class ClimateHypervisor(ContainerHypervisor):

    def parse(self, verbose=True):

        parser = argparse.ArgumentParser(description='Choice of Climate Ensembling task and parameters for it')
        parser.add_argument('model', type=ascii, nargs='1',
                            help='the model that your task will run on')
        parser.add_argument('task', type=ascii, nargs='1',
                            help='the task that you want to run (what output you want)')
        parser.add_argument('coordinates', type=ascii, nargs='1',
                            help='if the select_location task, the location you want')
        parser.add_argument('bias correction method', type=ascii, nargs='1',
                            help='if the select city task, the city you want')

        args = parser.parse_args()

        return args

    def retrieve_data(self, inputs, *args):
        """
        Returns a dictionary of type {"name of Data object": Data}
        """
        for arg in args:
            # Grab the argument and stick it in the dictionary?
            self.data['selected_model'] = Data(DataType.MDF, DataLocationType.S3, s3_key=TEST_DATA_KEY, s3_bucket_name=TEST_DATA_BUCKET)
            break
        self.data['base_model'] = Data(DataType.MDF, DataLocationType.S3, s3_key=TEST_DATA_KEY, s3_bucket_name=TEST_DATA_BUCKET)

        return self.data

    def upload_outputs(self, outputs):
        return super().upload_outputs(outputs)
        
    def run_task(self, task, data, parameters):
        """
        :param task: String of the task name
        :param data: {'name of data': Data}
        :rtype {'output name': Dataframe}

        5 Tasks from the paper 
        
        1.
        2.
        3.
        4.
        5.

        """
        

        task = "select_location"
        print("Task: {}".format(task))
        print("Data: {}".format(data))
        print("Parameters: {}".format(parameters))
        if task == "select_location":
            return select_location(**data, **parameters)
        
        elif task == "bias_correction":
            return apply_bias_correction(**data, **parameters)

        elif task == "aggregate":
            return aggregate_models(**data, **parameters)
