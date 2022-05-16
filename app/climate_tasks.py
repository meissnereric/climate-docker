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

        else:
            processed_data = process_models(model_data, standardised_calendar)
            processed_data.attrs["is_reference"]==False
            models[model_name] = processed_data

    #return models
    return {'s3://climate-ensembling/tst/EC-Earth3/': pd.DataFrame(np.ones((10,10)))}


def select_location(data, parameters):
    '''
    Select data by locations and time ranges specified in parameters
    '''
    print("Selecting location...")

    start = parameters['time_range'][0] # np.datetime64
    end = parameters['time_range'][1]

    models = {}

    for model_name, model_data in data["models"].items():
        for loc in parameters['locations']:
            selected_data = select_location(model_data, loc, start, end)

            models[model_name] = selected_data

    #return models
    return {'s3://climate-ensembling/tst/EC-Earth3/': pd.DataFrame(np.ones((10,10)))}

    #path = 's3://climate-ensembling/' + ...?
    #return {path: models}


#**************************************************************************************


def apply_bias_correction(model_data, reference_data, bias_corrector, past, future):
    '''apply a specified bias correction method (incl. None)'''
    model = model_data.df
    reference = reference.df

    model_past = select_time(model, reference.time[0], reference.time[-1])


def apply_bias_correction(era5_model_data, model_data, bc_method,
                            past_start='1979-01-01', past_end='2000-12-31',
                            future_start='2000-01-01', future_end='2014-12-31'):

    model = ClimateModel("climate_model", data=model_data)
    era5_model = ClimateModel("climate_model", data=era5_model_data)
    data = model.data.df
    era5 = era5_model.data.df

    ps_past = [np.datetime64(past_start), np.datetime64(past_end)]
    ps_future = [np.datetime64(future_start), np.datetime64(future_end)]

    ERA5_past = era5.loc[ps_past[0]:ps_past[1]].t2m

    past = data.loc[ps_past[0]:ps_past[1]].tas
    future = data.loc[ps_future[0]:ps_future[1]].tas

    if bc_method == 'delta':
        corrected = delta_correction(ERA5_past, past, future)

    return {'bias_corrected': corrected.to_pandas()}

def compute_disruption_days(model, temperature_threshold):
    data = model.data.df

    celsius = temperature_threshold - 273.13
    EC_Earth_df = data.to_frame()
    EC_Earth_df['exc_{}'.format(celsius)] = np.where(data['tas'] >= temperature_threshold, 1,0)
    return {'disruption_days': EC_Earth_df.to_pandas()}

def aggregate_models(EC_Earth, models, temp=30):
    data = model.data.df
    annual_exc = EC_Earth['exc_{}'.format(temp)].groupby(EC_Earth.index.year).sum().to_frame('EC_Earth')
    for model in models:
        # groupby year, aggregate count on 'exc'
        annual_exc[model.name] = data['exc_{}'.format(temp)].groupby(data.index.year).sum().to_frame()

    annual_exc['ensemble_mean_{}'.format(temp)] = annual_exc.mean(axis=1)
    return {'aggregated_results': annual_exc.to_pandas()}
