#!/home/nivag/2023-Linux/.python-3.10/bin/python3

import dask.dataframe as dask_pd
import dask_geopandas as dask_gpd
import geopandas as gpd
import pandas as pd
from dask.distributed import Client
import time

dask_df_mdt =  dask_pd.read_csv('result/cr-result-xlarea.csv/*.part', assume_missing=True)

dask_df_mdt = dask_df_mdt[(dask_df_mdt['absolute_rf_channel_number'] > 1200) & (dask_df_mdt['absolute_rf_channel_number'] < 1949)]

dask_df_mdt['isnumberic'] = dask_df_mdt['mobile_operator'].str.isnumeric()