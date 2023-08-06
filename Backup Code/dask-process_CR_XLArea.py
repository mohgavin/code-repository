#!/home/nivag/.env/bin/python

# by @mohgavin
import dask.dataframe as dask_pd
import dask_geopandas as dask_gpd
import geopandas as gpd
import pandas as pd
from dask.distributed import Client
import time

if __name__ == '__main__':
	s = time.perf_counter()
	client = Client(n_workers=3, threads_per_worker=4, processes=True)

	print('Loading Files...')
	dask_df_mdt =  dask_pd.read_csv('cellrebel-rf/cellrebel*.csv', assume_missing=True)
	dask_df_mdt['pointer'] = "POINT (" + dask_df_mdt['longitude'].astype(str) + " " + dask_df_mdt['latitude'].astype(str) + ")"

	dask_gdf_mdt = dask_gpd.from_dask_dataframe(dask_df_mdt, geometry=dask_df_mdt["pointer"].map_partitions(gpd.GeoSeries.from_wkt, meta=gpd.GeoSeries([])),)
	dask_gdf_mdt = dask_gdf_mdt.set_crs(4326)

	kecamatan = pd.read_csv('grid_folder/Kecamatan_Jabotabek.csv')
	kecamatan = gpd.GeoDataFrame(kecamatan, geometry=gpd.GeoSeries.from_wkt(kecamatan['WKT']), crs=4326)

	print('Join Operation...')
	dask_gdf_mdt = dask_gdf_mdt.sjoin(kecamatan, how='inner', predicate='within')
	dask_gdf_mdt = dask_gdf_mdt.drop(columns=['pointer', 'index_right', 'WKT', 'Kecamatan', 'ID_Kecamatan', 'Luas_Kec_Sq_Km', 'Unique_XL', 'Kecamatan_XL', 'City_XL', 'Commercial_XL', 'Clutter_Type'])

	dask_gdf_mdt.to_csv('result/cr-rsrp-result-xlarea.csv')
