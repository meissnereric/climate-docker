from data import Data, DataLocationType, DataType
from climate_tasks import aggregate_models, apply_bias_correction, select_location
import argparse
import json
import sys

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
                self.data[key][value] = (Data(DataType.MDF, DataLocationType.S3, s3_key=s3_key, s3_bucket_name=s3_bucket))

    def retrieve_data(self, args):
        """
        Returns a dictionary of type {"name of Data object": Data}
        """
        print("Args!: {}".format(args))
        inputs = args['inputs']
        parameters = args['parameters'][args['service_name']]
        for key, value in {**inputs, **parameters}.items():
            print("Key: {} Value: {}".format(key, value))
            # Grab the argument and stick it in the dictionary?
            if isinstance(value, list):
                listvalue = value
                for v in listvalue:
                    self._parse_data(key, v)
            else:
                self._parse_data(key, value)

        return self.data

        
    def run_task(self, task, data, args):
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
        print("Args: {}".format(args))
        print("Task: {}".format(task))
        print("Data: {}".format(data))
        parameters = args['parameters'][task]
        print("Parameters: {}".format(parameters))
        if task == "SelectLocation":
            return select_location(data, parameters)
        
        elif task == "BiasCorrection":
            return apply_bias_correction(data, parameters)

        elif task == "AggregateModels":
            return aggregate_models(data, parameters)
        else:
            assert False, "No valid task chosen!"

    def upload_outputs(self, outputs, output_locations):
        print("TODO Upload those outputs! {} {} ".format(outputs, output_locations))