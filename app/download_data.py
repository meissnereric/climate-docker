#!/usr/bin/env python

import os
import boto3
import numpy as np
import pandas as pd
import xarray as xr
import xclim as xc

import matplotlib.pyplot as plt
import seaborn as sns
from utils import *


print("Hello world!")

client = boto3.client('s3')

response = client.get_object(
            Bucket='climate-ensembling',
            Key='test_data.txt')

print("Retrieved object from S3 {},".format(response))

response = client.put_object(
            Bucket='climate-ensembling',
            Body='test_local_data.txt',
            Key='test_local_data.txt')

print("Uploaded object to S3 {},".format(response))


