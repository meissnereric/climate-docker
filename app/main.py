#!/usr/bin/env python

import pandas as pd
from hypervisor_container import ClimateHypervisor
from data import Data, DataLocationType, DataType

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

    print("Running task with parameters {}".format(args))

    if service_name == "CalculateCostsAll": #Fast experiment, all tasks in one run mode
        parameters = args['parameters']
        loaded_parameters = hv.load_data(inputs, parameters) # -> [Data]
        print("************************ Data ********************* \n {}".format(loaded_parameters))

        pd_output = hv.run_task("ProcessData", loaded_parameters)
        print("Direct output of ProcessData: {} ".format(pd_output))
        loaded_parameters['model'] = Data(DataType.MDF, DataLocationType.LOCAL, df=pd_output[0])
        print("Parameters after ProcessData update: {}".format(loaded_parameters))

        sl_output, quantiles = hv.run_task("SelectLocation", loaded_parameters)
        print("Direct output of SelectLocation: {} {}".format(sl_output, quantiles))
        print("Attributes of SelectLocation sl_output: {}".format(sl_output.attrs))
        loaded_parameters['model'] = Data(DataType.MDF, DataLocationType.LOCAL, df=sl_output)
        print("Parameters after SelectLocation update: {}".format(loaded_parameters))

        bc_output = hv.run_task("BiasCorrection", loaded_parameters)
        print("Direct output of BiasCorrection: {} ".format(bc_output))
        loaded_parameters['model'] = Data(DataType.MDF, DataLocationType.LOCAL, df=bc_output[0])
        print("Parameters after BiasCorrection update: {}".format(loaded_parameters))

        for quantile in quantiles:
            loaded_parameters['threshold'] = quantile
            cc_output = hv.run_task("CalculateCosts", loaded_parameters)

            # Save / upload
            combined_output_locations = {}
            for i, (k) in enumerate(output_locations.keys()):
                quantile_location = output_locations[k]+'quantile-{}/'.format(quantile)
                combined_output_locations[k] = (quantile_location, cc_output[i])
            print("combined_output_locations: {}".format(combined_output_locations))

            hv.upload_outputs(combined_output_locations)            

    else: # Normal mode
        parameters = args['parameters'][service_name]
        loaded_parameters = hv.load_data(inputs, parameters) # -> [Data]
        print("************************ Data ********************* \n {}".format(loaded_parameters))

        outputs = hv.run_task(service_name, loaded_parameters)
        print("Returned outputs from run_task: {}".format(outputs))
        combined_output_locations = {}
        for i, (k) in enumerate(output_locations.keys()):
            combined_output_locations[k] = (output_locations[k], outputs[i])
        print("combined_output_locations: {}".format(combined_output_locations))

        hv.upload_outputs(combined_output_locations)
        print("\n\nWe're done!!!!!\n\n\n")
 
