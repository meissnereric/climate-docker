from utils import *
import numpy as np
import xarray as xr

def process_data(data, parameters):
    '''
    Processing steps:
    - standardise calendar across models + reference datasets
    - normalise time index format (i.e. remove "12:00:00:00" from daily data)
    - standardise coordinate names (i.e. "latitude" -> "lat", "t2m" -> "tas" etc"
    note: reindex_like requires standardised_calendar xarray dataset - small file, can be stored on github
    '''
    print("Processing datasets...")
    models = {}

    #*** create upload standard calendar xr dataset
    standardised_calendar = xr.open_dataset("<path_to_standardised_calendar>", engine="netcdf4")

    for model_name, model_data in data["models"].items()
        print("Processing {}...".format(model_name))

        if model_name in ['ERA5', 'ERA-Interim']:
            processed_data = process_reference(model_data)
            processed_data.attrs["is_reference"]==True
            models[model_name] = processed_data

            # *** save reference dataset to s3 here, so that following functions can be per model

        else:
            processed_data = process_models(model_data, standardised_calendar)
            processed_data.attrs["is_reference"]==False
            models[model_name] = processed_data

    return models
    #return {'s3://climate-ensembling/tst/EC-Earth3/': pd.DataFrame(np.ones((10,10)))}


def select_location(models, parameters):
    '''
    Select data by locations and time ranges specified in parameters
    '''
    print("Selecting location...")

    start = parameters['time_range'][0] # np.datetime64
    end = parameters['time_range'][1]

    models_selection = {}

    for model_name, model_data in models.items():
        for loc in parameters['locations']:
            selected_data = select_location(model_data, loc, start, end)

            models_selection[model_name] = selected_data

    return models_selection
    #return {'s3://climate-ensembling/tst/EC-Earth3/': pd.DataFrame(np.ones((10,10)))}

    #path = 's3://climate-ensembling/' + ...?
    #return {path: models}

def apply_bias_correction(models, parameters):
    '''
    Take in models_selection dict i.e. 1-d, standardised time-range
    and bias-correction method specified in parameters
    returns dict of bias-corrected dataframes + reference data
    '''

    reference = xr.open_dataset("<path_to_reference_dataset>", engine="netcdf4")

    models_to_correct={}
    for model_name, model_data in models.items():
        if model_data.is_reference==True:
            continue # the reference dataset was previously saved

        elif model_data.is_reference==False:
            models_to_correct[model_name]=model_data

    past = parameters['past']
    future = parameters['future']

    bias_corrected_models={}
    for model_name, model_data in models_to_correct.items():
        if parameters[bias_correction_method]=="none":
            bias_corrected_models[model_name] = no_correction(model_name, model_data, reference, past, future)

        elif parameters[bias_correction_method]=="delta":
            bias_corrected_models[model_name] = delta_correction(model_name, model_data, reference, past, future)

        #elif etc for other b-c methods

    return bias_corrected_models

def compute_disruption_days(models, parameters):
    '''
    ...
    '''

    temperature_threshold = parameters['temperature_threshold']

    disruption_days={}
    for model_name, model_df in models.items():
        model_df['exc_{}'.format(temperature_threshold)] = np.where(model_df.model >= temperature_threshold, 1, 0)
        disruption_days[model_name] = model_df

    return disruption_days

def aggregate_models(models, parameters):
    '''
    ...
    '''

    temperature_threshold = parameters['temperature_threshold']

    annual_exc={}
    for model_name, model_df in models.items():
        annual_exc[model_name] = model_df['exc_{}'.format(temperature_threshold)].groupby(model_df.index.year).sum().to_frame(model_name)

    annual_exc['aggregate_mean_{}'.format(temperature_threshold)] = annual_exc.mean(axis=1).to_pandas()
    return annual_exc
