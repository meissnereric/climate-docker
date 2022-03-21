#!/usr/bin/env python

import pandas as pd
from hypervisor_container import ClimateHypervisor

if __name__ == "__main__":
    print("Hello and welcome to the Climate Ensembling Dreamworld!")

    hv = ClimateHypervisor()

    args = hv.parse()

    print("Running task with parameters {}".format(args))

    # input_data_locations input_data_locations= {'selected_model': 's3://climate-ensembling/', 'base-model': 's3://climate-ensembling/'}
    # input_data_locations = {'selected_model': 's3://climate-ensembling/EC-Earth3/', 'base-model': 's3://climate-ensembling/era5/t2m/'}
    input_data = hv.retrieve_data(args) # -> [Data]

    # input_data = {'selected_model':  pd.DataFrame([[1,2], [3,4]]), 'base-model': pd.DataFrame([[1,2], [3,4]])}
    print("************************ Data ********************* \n {}".format(input_data))
    outputs = hv.run_task(args['service_name'], input_data, args)

    hv.upload_outputs(outputs, args)
    print("\n\nWe're done!!!!!\n\n\n")
