from utils import *
import numpy as np

class ClimateModel:

    def __init__(self, name, data=None):
        self.name = name
        self.data = data
        # TODO Add all the rest of the models here / structured as needed.
        if name == "t2m":
            self.data.df = normalize_time(self.data.df).sel(bnds=0)
        elif name =='EC_Earth':
            self.data.df = normalize_time(self.data.df).rename({'lat': 'latitude','lon': 'longitude'}).sel(bnds=0)
        elif name =='GFDL_ESM4':
            self.data.df = normalize_time(cfnoleap_to_datetime(self.data.df)).rename({'lat': 'latitude','lon': 'longitude'}).sel(bnds=1)
        else:
            print("ERROR: Please  pass the type of model.")

## Coordinates
## Different methods can be used to choose coordinate places (nearest is default)
def select_location(data, parameters):
    """
    :param data: Data type object
    """
    print("Made it inside select_location call!")
    isERA5=False # parameters?
    start='1979-01-01'
    end='2014-12-31'
    coordinates = {}
    for l in parameters['locations']:
        coordinates[l]=get_coords(l)
    models = {}
    for model_name, model_data in data['models'].items():

        model = ClimateModel("EC_Earth", data=model_data)
        data = model.data.df
        print("**********************************select_location data***************************", data)

        start_date = np.datetime64(start)
        end_date = np.datetime64(end)
        for coordinate in coordinates.values():
            # note that longitudes are 0->360 not -180->+180
            if isERA5:
                location_data = select_time(data, start_date, end_date).sel(
                    latitude=coordinate[0], longitude=180+coordinate[1], method='nearest').t2m.to_dataframe()
            else:
                location_data = select_time(data, start_date, end_date).sel(
                    latitude=coordinate[0], longitude=180+coordinate[1], method='nearest').tas.to_dataframe()
            models[model_name] = location_data
    # return models

    return {'s3://climate-ensembling/tst/EC-Earth3/': pd.DataFrame(np.ones((10,10)))}

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
