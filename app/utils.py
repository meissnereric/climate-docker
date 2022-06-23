import numpy as np
import xarray as xr
import pandas as pd
import scipy.interpolate as interp
import calendar
import os
from geopy.geocoders import Nominatim
from scipy.optimize import linear_sum_assignment

##################### import data #####################

def get_local_directory():
    cwd = os.getcwd()
    print("CWD: {}".format(cwd))
    return cwd

def import_dataset(directory, folder):
    '''
    read xarray multi-file dataset for CMIP5/CMIP6 global climate models
    folders are arranged per model as 'cmip5/<model_name>' or 'cmip6/<model_name>'
    see: https://xarray.pydata.org/en/stable/generated/xarray.open_mfdataset.html
    '''
    #directory = '/Users/malavirdee/Documents/climate_data/'
    print("Import model: dir {} folder {}".format(directory, folder))
    path = os.path.join(directory+"/"+folder+"/*.nc")
    return xr.open_mfdataset(path, engine="netcdf4")

def import_reference(directory, folder="reference/ERA5"):
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
    therefore these are removed from all models + reference dataset
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
#   *** "coord" -> "dimension" to avoid confusion
    ds_renamed = ds.rename(names_dict)
    return ds_renamed

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
    return c

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

def select_location_mdf(ds, city, start=None, end=None): #, to_pandas=False):
    '''
    select 1-d time series for specified city, optionally select time range
    returns DataArray
    *** should this output a pandas df? unsure where to convert
    '''
    coords = get_coords(city)
    # note: model + ERA latitudes are -90 -> +90, longitudes are 0 -> +360
    da = ds.sel(lat=coords[0], lon=180+coords[1], method='nearest')
    da_sl = select_time(da, start, end)#.to_dataframe()
    return(da_sl)

def get_nearest(ds, latitude, longitude):
    '''
    get grid-point nearest to specified lat, lon
    (i.e. for checking how far city coordinate is from closest grid-point in dataset)
    '''
    lat = ds.sel(lat=latitude, lon=longitude, method="nearest")['lat'].values
    lon = ds.sel(lat=latitude, lon=longitude, method="nearest")['lon'].values
    return float(lat), float(lon)



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

##################### bias-correction #####################

# see description of 4 standard bias-correction methods:
# https://www.metoffice.gov.uk/binaries/content/assets/metofficegovuk/pdf/research/ukcp/ukcp18-guidance---how-to-bias-correct.pdf

def no_correction(model, reference, past, future):
    '''
    return reference + model dataframe without applying any bias correction method
    takes past, future lists e.g. past = [past_start, past_end]
    *** make bias correction functions flexible to other variables than t2m
    '''
    reference_past = reference.loc[dict(time=slice(past[0],past[1]))].tas
    reference_future = reference.loc[dict(time=slice(future[0],future[1]))].tas

    model_past = model.loc[dict(time=slice(past[0],past[1]))].tas
    model_future = model.loc[dict(time=slice(future[0],future[1]))].tas

    uncorrected = model_future
    #uncorrected = model_future.to_frame("model")
    #uncorrected['model'] = model_name

    #reference_df = reference_future.to_frame("reference")

    #df = pd.concat([reference_df.t2m, corrected_df.t2m, corrected_df.model], axis=1)
    #df.rename(columns={"reference": "T_obs", "model": "T_model"}, inplace=True)
    return uncorrected, reference_future


def delta_correction(model, reference, past, future):
    '''
    simplest additive mean-shift bias correction
    i.e. model_future = model_future + (observation_past.mean - model_past.mean)
    returns reference + bias-corrected model dataframe
    '''

    reference_past = reference.loc[past[0],past[1]].tas
    reference_future = reference.loc[future[0],future[1]].tas

    model_past = model.loc[past[0],past[1]].tas
    model_future = model.loc[future[0],future[1]].tas

    corrected = model_future + (np.nanmean(reference_past) - np.nanmean(model_past))

    #corrected = (model_future + (np.nanmean(reference_past) - np.nanmean(model_past))).to_frame("model")
    #corrected['model']=model_name

    #reference_df = reference_future.to_frame("reference")

    #df = pd.concat([reference_df.t2m, corrected_df.t2m, corrected_df.model], axis=1)
    #df.rename(columns={"reference": "T_obs", "model": "T_model"}, inplace=True)
    return corrected, reference_future

def relative_delta_correction(model_name, model, reference, past, future):
    '''
    relative mean-shift bias correction
    i.e. model_future = model_future * (observation_past.mean / model_past.mean)
    this is important for e.g. precipitation to avoid returning unphysical < 0 values
    returns reference + bias-corrected model dataframe
    '''

    reference_past = reference.loc[past[0]:past[1]].tas
    reference_future = reference.loc[future[0]:future[1]].tas

    model_past = model.loc[past[0]:past[1]].tas
    model_future = model.loc[future[0]:future[1]].tas

    corrected = model_future * (np.nanmean(reference_past) / np.nanmean(model_past))

    #corrected = (model_future * (np.nanmean(reference_past) / np.nanmean(model_past))).to_frame("model")
    #corrected['model']=model_name

    #reference_df = reference_future.to_frame("reference")

    #df = pd.concat([reference_df.t2m, corrected_df.t2m, corrected_df.model], axis=1)
    #df.rename(columns={"reference": "T_obs", "model": "T_model"}, inplace=True)
    return corrected, reference_future

##################### reordering utils #####################

def levenshtein(a,b):
    '''
    Calculates the Levenshtein matching distance between series a and b
    see: https://en.wikipedia.org/wiki/Levenshtein_distance
    '''
    n, m = len(a), len(b)
    if n > m:
        # Make sure n <= m, to use O(min(n,m)) space
        a,b = b,a
        n,m = m,n

    current = range(n+1)
    for i in range(1,m+1):
        previous, current = current, [i]+[0]*n
        for j in range(1,n+1):
            add, delete = previous[j]+1, current[j-1]+1
            change = previous[j-1]
            if a[j-1] != b[i-1]:
                change = change + 1
            current[j] = min(add, delete, change)

    return current[n]

def band_matrix(window: int, N):
    '''
    return NxN band matrix so that matrices multiplied by band matrix retain a +/- window
    on each side of diagonal element and discard points outside
    '''
    if (window % 2) == 0:
        return("error: window must be odd number") # require symmetry around central element
    else:
        w = int((window - 1)/2) # w is +/- distance from central point
        a = np.zeros((N,N))
        i,j = np.indices(a.shape)
        for n in range(w+1):
            a[i==j] = 1.
            a[i==j+n] = 1.
            a[i==j-n] = 1.
        a[a==0]='nan'
        return a

def reorder(A, B, window):
    '''
    apply optimal matching algorithm to series B to match series A
    for sliding time windows up to specified window.
    windows must be odd numbers (symmetrical around central day)
    threshold_type 'lower' -> match high temperature extremes
    threshold_type 'upper' -> match low temperature extremes
    '''

    cost_matrix = (A[:, None] - B[None, :])**2
    exclude_cost = cost_matrix.max()*2 # set arbitrarily high cost to prevent reordering of these points

    if window % 2 == 0:
        window += 1
    #windows = np.arange(1, max_window+1, 2)

    #for window in windows: ## outputs should be per time window ?
        # generate band matrix with 'nan' outside allowed sliding window
    b = band_matrix(window, cost_matrix.shape[0])
    banded_cost_matrix = cost_matrix * b
    banded_cost_matrix[np.isnan(banded_cost_matrix)] = exclude_cost
    
    print("********** Banded Cost Matrix ************* \n {}".format(banded_cost_matrix))
    row_index, column_index = linear_sum_assignment(np.abs(banded_cost_matrix))
    B_matched = [B[i] for i in column_index]

    return B_matched

def rms(A, B):
    '''
    root mean sq error of 2 series
    '''
    return ((A.mean()-B.mean())**2)**0.5

def threshold_cost(A, B, threshold, threshold_type):
    '''
    calculate rms (?) cost of B wrt A above/below specified threshold
    threshold_type = "lower" i.e. evaluate high-temperature extremes, and vice-versa
    '''

    if threshold_type == 'none':
        include_indices = [i for i in range(len(B))]

    elif threshold_type == 'lower':
        exclude_indices = [i for i in range(len(B)) if B[i] < threshold] #???
        include_indices = [i for i in range(len(B)) if B[i]>= threshold]

    elif threshold_type == 'upper':
        include_indices = [i for i in range(len(B)) if B[i] < threshold]
        exclude_indices = [i for i in range(len(B)) if B[i]>= threshold]

    else:
        print("error: select threshold 'none', 'lower', 'upper'")

    B_selected = B[include_indices]

    # if cost_metric == 'rms' ?
    cost = rms(A, B_selected)

    return cost

def reordering_cost(A, B, window=7, threshold=10, threshold_type="lower"):
    '''
    combined function for reordering + calculate cost
    '''
    B_matched = reorder(A, B, window)
    print("B_matched: {}".format(type(B_matched)))
    np_A = np.array(A)
    np_B_matched = np.array(B_matched)
    cost = threshold_cost(np_A, np_B_matched, threshold, threshold_type)
    return cost, B_matched

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
