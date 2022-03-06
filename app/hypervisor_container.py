from data import Data, DataLocationType, DataType
from climate_tasks import aggregate_models, apply_bias_correction, select_location
import argparse

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

        parser = argparse.ArgumentParser(description='Choice of Climate Ensembling task and parameters for it')
        parser.add_argument('--task',
                            help='the task that you want to run (what output you want)')
        parser.add_argument('--coordinates',
                            help='if the select_location task, the location you want')
        parser.add_argument('--bias',
                            help='bias correction method')
        parser.add_argument('--aggregation',
                            help='aggregation method')
        parser.add_argument('--inputs',
                            help='dictionary of s3 URIs for input data. Ex. {\'selected_model\': \'s3://...\', ...} ')
        parser.add_argument('--outputs',
                            help='dictionary of s3 URIs for output data. Ex. {\'selected_model\': \'s3://...\', ...}')

        
        args = parser.parse_args()
        args = vars(args)
        print("Parser arguments output: {}".format(args))

        return args

    def retrieve_data(self, input_data):
        """
        Returns a dictionary of type {"name of Data object": Data}
        """
        for i in input_data.items():
            # Grab the argument and stick it in the dictionary?
            components = i[1][5:].split('/')
            print("Components: {} {}".format(i, components))
            s3_key = ''.join(components[1:])
            s3_bucket = components[0]
            self.data['selected_model'] = Data(DataType.MDF, DataLocationType.S3, s3_key=s3_key, s3_bucket_name=s3_bucket)
            break
        self.data['base_model'] = Data(DataType.MDF, DataLocationType.S3, s3_key=s3_key, s3_bucket_name=s3_bucket)

        return self.data

    def upload_outputs(self, outputs, output_locations):
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
        print("Task: {}".format(task))
        print("Data: {}".format(data))
        print("Parameters: {}".format(parameters))
        if task == "select_location":
            return select_location(data, parameters)
        
        elif task == "bias_correction":
            return apply_bias_correction(data, parameters)

        elif task == "aggregate":
            return aggregate_models(data, parameters)
