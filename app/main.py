#!/usr/bin/env python

import pandas as pd
from hypervisor_container import ClimateHypervisor

"""
Interface Design

Parameters dictionary is passed for a single task run - any lists in this dictionary are treated as single parameters still, and not "looped over" to form multiple outputs.

Outputs are not lists but single layer dictionaries of strings, if there is more than one input or output they must all be named. 


"""

if __name__ == "__main__":
    print("Hello and welcome to the Climate Ensembling Dreamworld!")

    hv = ClimateHypervisor()

    args = hv.parse()
    service_name = args['service_name']
    inputs = args['inputs']
    output_locations = args['outputs']
    parameters = args['parameters'][service_name]

    print("Running task with parameters {}".format(args))

    # input_data_locations input_data_locations= {'selected_model': 's3://climate-ensembling/', 'base-model': 's3://climate-ensembling/'}
    # input_data_locations = {'selected_model': 's3://climate-ensembling/EC-Earth3/', 'base-model': 's3://climate-ensembling/era5/t2m/'}
    loaded_parameters = hv.load_data(inputs, parameters) # -> [Data]

    # input_data = {'selected_model':  pd.DataFrame([[1,2], [3,4]]), 'base-model': pd.DataFrame([[1,2], [3,4]])}
    print("************************ Data ********************* \n {}".format(loaded_parameters))
    outputs = hv.run_task(service_name, loaded_parameters)
    combined_output_locations = {}
    for i, (k) in enumerate(output_locations.keys()):
        combined_output_locations[k] = (output_locations[k], outputs[i])

    hv.upload_outputs(combined_output_locations)
    print("\n\nWe're done!!!!!\n\n\n")
 