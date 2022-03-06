import numpy as np
import xarray as xr
import pandas as pd
import scipy.interpolate as interp
import calendar
import os
from geopy.geocoders import Nominatim

def filesize(dataset):
    return str(round(dataset.nbytes * (2 ** -30), 2))+'GB'

def get_local_directory():
    cwd = os.getcwd()
    print("CWD: {}".format(cwd))
    return cwd

def cfnoleap_to_datetime(ds):
    datetimeindex = ds.indexes['time'].to_datetimeindex()
    #ds = da.to_dataset()
    ds_dt=ds
    ds_dt['time_dt']= ('time', datetimeindex)
    ds_dt = ds_dt.swap_dims({'time': 'time_dt'})
    assert len(ds.time) == len(ds_dt.time_dt)
    return ds_dt.set_index(time='time_dt')

def rename_coord(da, old_name, new_name):
    ''' Rename coordinate in xr data array
    '''
    if old_name in list(da.coords.keys()):
        da = da.rename({old_name:new_name})
    return da

def get_nearest(da, latitude, longitude):
    '''
    get point nearest to specified lat, lon
    '''
    lat = da.sel(lat=latitude, lon=longitude, method="nearest")['lat'].values
    lon = da.sel(lat=latitude, lon=longitude, method="nearest")['lon'].values
    return float(lat), float(lon)
    
def import_mdf_dataset(directory, folder):
    print("Import dataset: dir {} folder {}".format(directory, folder))
    path = os.path.join(directory+"/"+folder+'/*.nc')
    return xr.open_mfdataset(path, engine="netcdf4")

def cfnoleap_to_datetime(ds):
    '''some models use cftimeindex - need to convert these to datetimeindex'''
    datetimeindex = ds.indexes['time'].to_datetimeindex()
    #ds = da.to_dataset()
    ds_dt=ds
    ds_dt['time']= ('time', datetimeindex)
    assert len(ds.time) == len(ds_dt.time)
    return ds_dt

def normalize_time(ds):
    '''remove times from daily-averaged data'''
    ds['time'] = ds.indexes['time'].normalize()
    return ds

def get_coords(city):
    '''return lat, lon for city (string)'''
    geolocator = Nominatim(user_agent='http')
    location = geolocator.geocode(city)
    latitude, longitude = location.latitude, location.longitude
    print(location, (latitude, longitude))  
    return (latitude, longitude)

def select_time(ds, start, end):
    return ds.sel(time=slice(start, end))


# #### 2. For each of the models used, at each location, a bias correction to the raw data is calculated based on the observational data. This defines a transfer function that is then used on model predictions to bias-correct each model's future output.
def delta_correction(observation, model_past, model_future):
    '''simplest mean-shift delta bias correction method
    adding the mean change signal to observations'''
    c = model_future + (np.nanmean(observation) - np.nanmean(model_past))
    return c

######################## Utils for Task Management #######################
def load_object_from_s3(s3_client):
    response = s3_client.get_object(
                Bucket='climate-ensembling',
                Key='test_data.txt')
    return response


def download_s3_folder(bucket, s3_folder, local_dir=None):
    """
    Taken from: https://stackoverflow.com/a/62945526
    Download the contents of a folder directory
    Args:
        bucket_name: the name of the s3 bucket
        s3_folder: the folder path in the s3 bucket
        local_dir: a relative or absolute directory path in the local file system
    """
    print("Downloading S3 folder... {} / {}".format(bucket, s3_folder))
    if os.path.exists(os.path.join(local_dir)):
        print("Skipping the download because it's already downloaded")
        return
    for obj in bucket.objects.filter(Prefix=s3_folder):
        print("Looping for downloading S3 object {}".format(obj))
        target = obj.key if local_dir is None \
            else os.path.join(local_dir, os.path.relpath(obj.key, s3_folder))
        if not os.path.exists(os.path.dirname(target)):
            os.makedirs(os.path.dirname(target))
        if obj.key[-1] == '/':
            continue
        bucket.download_file(obj.key, target)