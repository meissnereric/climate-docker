from utils import process_models, no_correction, delta_correction, reordering_cost, select_location_mdf
import numpy as np
import xarray as xr

def process_data(parameters):
    '''
    Processing steps:
    - standardise calendar across models + reference datasets
    - normalise time index format (i.e. remove "12:00:00:00" from daily data)
    - standardise coordinate names (i.e. "latitude" -> "lat", "t2m" -> "tas" etc"
    '''
    print("Processing datasets...")

    standardised_calendar = parameters["standardised_calendar"]
    model_data = parameters["model"]
    processed_data = process_models(model_data, standardised_calendar)

    return processed_data

def select_location(parameters):
    '''
    Select time series by location and time ranges specified in parameters
    '''

    model_data = parameters["model"].df

    start = parameters["start"] # np.datetime64
    end = parameters["end"]
    location = parameters['location']

    print("Selecting location...")
    print(start, end, location)

    selected_data = select_location_mdf(model_data, location, start, end)
    return [selected_data]

def apply_bias_correction(parameters):
    '''
    takes bias-correction method specified in parameters
    options: 'none', 'delta', tbc...
    '''

    model_data = parameters['model'].df
    reference = parameters["reference"]

    past = parameters['past']
    future = parameters['future']

    bias_correction_method = parameters['bias_correction_method']
    
    if parameters[bias_correction_method]=="none":
        bias_corrected_model, bias_correction_reference = no_correction(model_data, reference, past, future)

    elif parameters[bias_correction_method]=="delta":
        bias_corrected_model, bias_correction_reference = delta_correction(model_data, reference, past, future)

    #elif etc.

    return [bias_corrected_model, bias_correction_reference]

def calculate_cost(parameters):
    '''
    apply reordering algorithm (see ref: ...)
    and calculate cost metric for specified extreme (upper/lower) threshold
    '''

    # these should generally be bias_corrected_model, bias_correction_reference from previous task
    model_data = parameters['model'].df
    reference = parameters['reference']

    window = parameters["window"]
    threshold = parameters["threshold"]
    threshold_type = parameters["threshold_type"] #'upper'/'lower'/'none'

    A = reference.tas.values#[start:stop]
    B = model_data.tas.values#[start:stop]  # A is fixed, B is reordered
    # at some point this should account for more other variables than tas...

    cost = reordering_cost(A, B, window, threshold, threshold_type)
    return [cost]

######## disregard tasks requiring multi model input for now
# def compute_disruption_days(models, parameters):
#     '''
#     ...
#     '''
#     model_data = parameters['model'].df
#
#     threshold = parameters['threshold']
#
#     disruption_days={}
#     for model_name, model_df in models.items():
#         model_df['exc_{}'.format(temperature_threshold)] = np.where(model_df.model >= temperature_threshold, 1, 0)
#         disruption_days[model_name] = model_df
#
#     return disruption_days
#
# def aggregate_models(models, parameters):
#     '''
#     ...
#     '''
#
#     temperature_threshold = parameters['temperature_threshold']
#
#     annual_exc={}
#     for model_name, model_df in models.items():
#         annual_exc[model_name] = model_df['exc_{}'.format(temperature_threshold)].groupby(model_df.index.year).sum().to_frame(model_name)
#
#     annual_exc['aggregate_mean_{}'.format(temperature_threshold)] = annual_exc.mean(axis=1).to_pandas()
#     return annual_exc
