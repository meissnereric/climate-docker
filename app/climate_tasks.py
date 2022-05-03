from utils import *
import numpy as np


def select_locations(data, parameters):
    print('Selecting location...')

    # ensure time range is same across models
    start, end = parameters['time_range']

    coordinates = {}
    for city in parameters['cities']:
        # this is redundant since get_1d takes city:str ?
        coordinates[city] = get_coords(city) # returns lat, lon
        print("***************"), city)

    # for now, move reference (ERA5) location selection elsewhere
    model = {}
    for name, data in parameters['models'].items():

        model_name = name
        model_data = data
        model_df = data.df #?

        print("***************"), model_name, model_df.head())

        start_date = np.datetime64(start)
        end_date = np.datetime64(end)

        for coord in coordinates.values():

            model_1d = get_1d(model_data, city=city, start=start_date, end=end_date)
            model_1d_df = model_1d.df #?

            print("***************"), model_1d_df.head())

            models[model_name]=model_1d_df

        return {'s3://climate-ensembling/tst/EC-Earth3/': pd.DataFrame(np.ones((10,10)))}
        #path = 's3://climate-ensembling/' + ...
        #return {path: models}


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
