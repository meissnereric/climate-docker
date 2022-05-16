import numpy as np
import xarray as xr
import pandas as pd
import scipy.interpolate as interp
import calendar
import os
from geopy.geocoders import Nominatim

##################### import data #####################

def get_local_directory():
    cwd = os.getcwd()
    print("CWD: {}".format(cwd))
    return cwd

def import_dataset(folder):
    '''
    read xarray multi-file dataset for CMIP5/CMIP6 global climate models
    folders are 'cmip5/<model_name>' or 'cmip6/<model_name>'
    see: https://xarray.pydata.org/en/stable/generated/xarray.open_mfdataset.html
    '''
    #directory = '/Users/malavirdee/Documents/climate_data/'
    print("Import model: dir {} folder {}".format(directory, folder))
    path = os.path.join(directory+"/"+folder+"/*.nc")
    return xr.open_mfdataset(path, engine="netcdf4")

def import_reference(folder="reference/ERA5"):
    '''
    read xarray reference dataset (i.e. ERA5 or ERA-Interim gridded observational reanalysis)
    see: https://www.ecmwf.int/en/forecasts/datasets/reanalysis-datasets/era5
    '''
    print("Import reference: dir {} folder {}".format(directory, folder))
    path = os.path.join(directory+"/"+folder+"/*.nc")
    return xr.open_mfdataset(path, engine="netcdf4")


##################### data processing #####################

def remove_feb_29_30(ds):
    '''
    if time index contains Feb 29 or Feb 30 as in some cftime formats, it cannot be converted,
    therefore these are removed from all
    '''
    feb_29 = ds.time.values[(ds.time.dt.month == 2) & (ds.time.dt.day == 29)]
    feb_30 = ds.time.values[(ds.time.dt.month == 2) & (ds.time.dt.day == 30)]
    return ds.drop_sel(time=feb_29).drop_sel(time=feb_30)

def normalize_time(ds):
    '''
    remove time string (e.g. '12:00:00' every day) from daily-averaged data
    '''
    ds['time'] = ds.indexes['time'].normalize()
    return ds

def calendar_type(ds):
    '''
    see cftime calendar types: https://unidata.github.io/cftime/api.html
    '''
    return type(ds.time.values[0])

def rename_coord(ds, names_dict):
    '''
    Rename coordinates in xr Dataset
    takes dictionary {"old_name":"new_name"}
    '''
    for old_name, new_name in names_dict.items():
        if old_name in list(ds.coords.keys()):
            ds.rename({old_name:new_name})
    return ds

def cf_to_datetime(ds):
    '''
    take cftime calendar types and convert to datetime64
    note: requires Feb 29 and Feb 30 to be removed first
    '''
    datetimeindex = ds.indexes['time'].to_datetimeindex().normalize()
    ds_dt=ds
    ds_dt['time']= ('time', datetimeindex)
    assert len(ds.time) == len(ds_dt.time)
    return ds_dt

def process_reference(ds):
    '''
    data processing steps for reference (i.e. ERA5) dataset
    '''
    a = remove_feb_29_30(ds)
    b = normalize_time(a)
    # rename ERA 2m surface temperature "t2m" to temperature at surface "tas", as is model convention
    names_dict = {"latitude":"lat", "longitude":"lon", "t2m":"tas"}
    c = rename_coord(b, names_dict)
    return b

def process_models(ds, reference):
    '''
    data processing steps for climate model dataset
    takes a reference dataset (can use ERA5 after applying process_reference) with correct calendar
    see: https://xarray.pydata.org/en/stable/generated/xarray.DataArray.reindex_like.html
    "ffill": propagate last valid index value forward
    '''
    #a = import_dataset(ds)
    b = remove_feb_29_30(ds)#(a)
    if calendar_type(b) in {np.datetime64}:
        c = b
    elif calendar_type(b) in {cftime._cftime.DatetimeNoLeap, cftime._cftime.Datetime360Day}:
        # most common types in cmip5/cmip6 - may need to search for any others
        c = cf_to_datetime(b)
    else:
        print("Error: unknown calendar type")
    d = c.reindex_like(reference, method="ffill")
    e = normalize_time(d)
    return e

##################### data selection #####################

def get_coords(city:str):
    '''
    return lat, lon for city
    see: https://geopy.readthedocs.io/en/stable/#nominatim
    '''
    geolocator = Nominatim(user_agent='http')
    location = geolocator.geocode(city)
    latitude, longitude = location.latitude, location.longitude
    #print(location, (latitude, longitude))
    return (latitude, longitude)

def select_time(ds, start, end):
    '''
    select time range from np.datetime start and end dates e.g. np.datetime64('1999-01-31')
    returns Dataset
    '''
    return ds.sel(time=slice(start, end))

def select_location(ds, city, start=None, end=None):
    '''
    select 1-d time series for specified city, optionally select time range
    returns DataArray
    '''
    coords = get_coords(city)
    # note: model + ERA latitudes are -90 -> +90, longitudes are 0 -> +360
    ds_1d = ds.sel(lat=coords[0], lon=180+coords[1], method='nearest')
    ds_1d_sl = select_time(model_1d, start, end)
    return(ds_1d_sl)

def get_nearest(ds, latitude, longitude):
    '''
    get grid-point nearest to specified lat, lon
    (i.e. for checking how far city coordinate is from closest grid-point in dataset)
    '''
    lat = ds.sel(lat=latitude, lon=longitude, method="nearest")['lat'].values
    lon = ds.sel(lat=latitude, lon=longitude, method="nearest")['lon'].values
    return float(lat), float(lon)

def rename_coord(da, old_name, new_name):
    '''
    rename coordinate in xr data array
    '''
    if old_name in list(da.coords.keys()):
        da = da.rename({old_name:new_name})
    return da


##################### calendar utils #####################

def calendar_check(da_1, da_2):
    '''
    check that 2 DataArrays contain same calendar dates
    '''
    m = set(da_1.index).symmetric_difference(set(da_2.index))
    if len(m)>0:
        mismatch = True
    elif len(m) == 0:
        mismatch = False
    return(mismatch, m)

def calendar_days(start, end):
    '''
    return real number of days between two np.datetime64 dates
    (i.e. to check how many days have been removed from data during standardisation)
    '''
    delta = end - start
    return(delta.astype('timedelta64[D]'))

##################### bias correction #####################

#**************************************************************************************

def delta_correction(observation, model_past, model_future):
    '''
    simplest mean-shift delta bias correction method
    adding the mean change signal to observations
    '''
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
