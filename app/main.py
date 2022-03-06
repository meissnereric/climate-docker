#!/usr/bin/env python

import pandas as pd
from hypervisor_container import ClimateHypervisor

if __name__ == "__main__":
    print("Hello and welcome to the Climate Ensembling Dreamworld!")

    hv = ClimateHypervisor()

    args = hv.parse()

    print("Running task {}".format(args.task))
    input_data_locations = dict(args['inputs'])
    output_data_locations = dict(args['outputs'])

    # input_data_locations input_data_locations= {'selected_model': 's3://climate-ensembling/', 'base-model': 's3://climate-ensembling/'}
    # input_data_locations = {'selected_model': 's3://climate-ensembling/EC-Earth3/', 'base-model': 's3://climate-ensembling/era5/t2m/'}
    input_data = hv.retrieve_data(input_data_locations) # -> [Data]

    # input_data = {'selected_model':  pd.DataFrame([[1,2], [3,4]]), 'base-model': pd.DataFrame([[1,2], [3,4]])}
    print("*********************************************", input_data, input_data['selected_model'])
    outputs = hv.run_task(args.task, input_data, args)

    hv.upload_outputs(outputs, output_data_locations)
