#!/home/nivag/2023-Linux/.python-3.10/bin/python3


import dask.dataframe as dask_pd
import dask_geopandas as dask_gpd
import geopandas as gpd
import pandas as pd
from dask.distributed import Client
import time

if __name__ == '__main__':
	
    client = Client(n_workers=4, threads_per_worker=6, processes=True)
    
    dask_df_mdt =  dask_pd.read_csv('result/cr-result-xlarea.csv/*.part', assume_missing=True)
    dask_df_mdt = dask_df_mdt[(dask_df_mdt['absolute_rf_channel_number'] > 1200) & (dask_df_mdt['absolute_rf_channel_number'] < 1949)]

    dask_df_mdt['Band'] = 'Band-3'

    dask_gdf_mdt = dask_gpd.from_dask_dataframe(dask_df_mdt, geometry=dask_df_mdt["geometry"].map_partitions(gpd.GeoSeries.from_wkt, meta=gpd.GeoSeries([])),)

    dask_gdf_mdt = dask_gdf_mdt.set_crs(4326)

    badgrid = pd.read_csv('Grid_Folder/result-polygon-l1800-june23.csv')
    badgrid = gpd.GeoDataFrame(badgrid, geometry=gpd.GeoSeries.from_wkt(badgrid['polygon_shape']), crs=4326)

    dask_gdf_mdt = dask_gdf_mdt.sjoin(badgrid, how='inner', predicate='within')

    dask_gdf_mdt['mobile_operator'] = dask_gdf_mdt['mobile_operator'].astype('category')
    dask_gdf_mdt['mobile_operator'] = dask_gdf_mdt['mobile_operator'].cat.as_known()

    print('Create Pivot')

    pivot_mean = dask_gdf_mdt.pivot_table(index='polygon_shape', columns='mobile_operator', values='reference_signal_received_power', aggfunc='mean')
    pivot_count = dask_gdf_mdt.pivot_table(index='polygon_shape', columns='mobile_operator', values='reference_signal_received_power', aggfunc='count')

    pivot_mean.to_csv('result/cr-june23-meanrsrp-l1800.csv')
    pivot_count.to_csv('result/cr-june23-meanrsrp-l1800-pop.csv')