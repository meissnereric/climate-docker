import numpy as np
import xarray as xr
import pandas as pd
import scipy.interpolate as interp
import calendar

def filesize(dataset):
    return str(round(dataset.nbytes * (2 ** -30), 2))+'GB'

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