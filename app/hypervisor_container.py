from data import Data, DataLocationType, DataType
from climate_tasks import aggregate_models, apply_bias_correction, select_location
import argparse
import json
import sys
import boto3
from datetime import datetime
import pandas as pd
import s3fs

# TEST_DATA_S3_URI = "s3://climate-ensembling/test_data.csv"
TEST_DATA_KEY = "tst/EC-Earth3/"
TEST_DATA_BUCKET = "climate-ensembling"

"""
How to structure the data in S3 for flexible / automatic data finding?

Possibilities:

Example 1:

s3://climate-ensembling/<task-1>/<task-2>/..../<task-n>
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

        print("Arguments passed {} \n\n".format(sys.argv))

        parser = argparse.ArgumentParser(description='Choice of Climate Ensembling task and parameters for it')
        parser.add_argument('--parameters',
                            help='the parameters dictionary')
        args = parser.parse_args()
        real_args = json.loads(args.parameters)
        print("Parser arguments output: {}".format(real_args))

        return real_args

    def _parse_data(self, key, value): 
        if value.startswith("s3://"): # TODO Change to "if self.is_input_data(value)"
            components = value[5:].split('/')
            print("Components: {} {}".format((key, value), components))
            s3_key = '/'.join(components[1:])
            s3_bucket = components[0]
            if key not in self.data:
                self.data[key] = {}
            dtype = DataType.CSV if '.csv' in value else DataType.MDF
            self.data[key][value] = (Data(dtype, DataLocationType.S3, s3_key=s3_key, s3_bucket_name=s3_bucket))
        else:
            return value
        return self.data[key][value]

    def load_data(self, inputs, parameters):
        """
        Returns the parameters ditionary, combined with the inputs, having loaded any parameters in either that were from S3 and replaced them with a Data object.
        
        e.g
        {'base_model' : Data(s3_key='s3://.....', ...)}
        """

        loaded_parameters = {}

        for key, value in {**inputs, **parameters}.items():
            if key in parameters and key in inputs:
                print("************** WARNING ************* inputs and parameters share a key ({}), this will cause issues, please rename one of them to a unique name.".format(key))
            print("Inputs to load in : ")
            print("Key: {} Value: {}".format(key, value))
            if isinstance(value, list):
                loaded = []
                listvalue = value
                for v in listvalue:
                    loaded.append(self._parse_data(key, v))
            else:
                loaded = self._parse_data(key, value)
            loaded_parameters[key] = loaded

        return loaded_parameters

        
    def run_task(self, task, loaded_parameters):
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
        print("Task: {}".format(task))
        print("Parameters: {}".format(loaded_parameters))
        outputs = None
        if task == "SelectLocation":
            outputs = select_location(loaded_parameters)
        
        elif task == "BiasCorrection":
            outputs =  apply_bias_correction(loaded_parameters)

        elif task == "AggregateModels":
            outputs = aggregate_models(loaded_parameters)
        else:
            assert False, "No valid task chosen!"
        
        return outputs

    def upload_outputs(self, outputs, bucket_name='climate-ensembling'):
        """
        Assumes outputs is a list of DataFrames [df, df, ...]
        """

        s3 = boto3.resource("s3")
        s3_bucket = s3.Bucket(name=bucket_name)
        print("Upload these outputs! Outputs: {}---".format(outputs))

        # datetime object containing current date and time
        now = datetime.now()
        
        print("now =", now)
        dt_string = now.strftime("%d:%m:%Y:%H:%M")
        # dd/mm/YY H:M:S
        for output, location in outputs.values():
                if isinstance(output, pd.DataFrame):
                    filename=dt_string+'.csv'
                    outputs[output].to_csv(filename)
                else:#is MFD type then
                    filename=dt_string+'.nc'
                    output.to_netcdf(filename)

                bucket_name = 'climate-ensembling'
                obj_name = location + filename
                s3_bucket.upload_file(filename, bucket_name, obj_name)

        print ("Finished uploading data!")
