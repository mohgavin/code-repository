#!/home/nivag/.env/bin/python

# by @mohgavin
import dask.dataframe as dask_pd
import dask_geopandas as dask_gpd
import geopandas as gpd
from dask.distributed import Client
import time

if __name__ == '__main__':
	s = time.perf_counter()
	client = Client(n_workers=3, threads_per_worker=4, processes=True)
	
	print('Loading Files')
	dask_df_mdt =  dask_pd.read_csv('Compile-MDT/mdt*.csv', usecols=[0,1,2,3,4,6,7,8,9], assume_missing=True)
	dask_df_mdt['rsrp-l2100'] = 'rsrp-l2100'

	values_to_query = [7,8,9,17,18,19,27,28,29,37,38,39,47,48,49,57,58,59,67,68,69,77]	
	dask_df_mdt = dask_df_mdt[dask_df_mdt['ci'].isin(values_to_query)]

	grid = dask_pd.read_csv('grid_folder/result-bad-grid-l2100.csv')

	#dask_df_mdt['combined'] = dask_df_mdt['date'].astype(str) + "@" + dask_df_mdt['hour'].astype(str) + "@" + dask_df_mdt['enodebid'].astype(str) + "@" + dask_df_mdt['ci'].astype(str)
	print('Processing')
	grid['WKT_polygon'] = grid['WKT'].astype(str)
	dask_df_mdt['pointer'] = "POINT (" + dask_df_mdt['longitude'].astype(str) + " " + dask_df_mdt['latitude'].astype(str) + ")"
	dask_gdf_mdt = dask_gpd.from_dask_dataframe(dask_df_mdt, geometry=dask_df_mdt["pointer"].map_partitions(gpd.GeoSeries.from_wkt, meta=gpd.GeoSeries([])),)
	#dask_gdf_mdt = dask_gdf_mdt.set_crs(4326)

	dask_gdf_grid = dask_gpd.from_dask_dataframe(grid, geometry=grid['WKT_polygon'].map_partitions(gpd.GeoSeries.from_wkt, meta=gpd.GeoSeries([])),)
	#dask_gdf_grid = dask_gdf_grid.set_crs(4326)

	print('Join Operation')
	dask_gdf_mdt = dask_gdf_mdt.sjoin(dask_gdf_grid, how='inner', predicate='within')
	dask_gdf_mdt = dask_gdf_mdt.drop(columns=['pointer'])

	dask_gdf_mdt['combined'] = dask_gdf_mdt['site'].astype(str) + "@" + dask_gdf_mdt['enodebid'].astype(str) + "@" + dask_gdf_mdt['ci'].astype(str) + "@" + dask_gdf_mdt['WKT_polygon'].astype(str)
	dask_gdf_mdt['rsrp-l2100'] = dask_gdf_mdt['rsrp-l2100'].astype('category')
	dask_gdf_mdt['rsrp-l2100'] = dask_gdf_mdt['rsrp-l2100'].cat.as_known()

	print('Create Pivot')
	pivot_mean = dask_gdf_mdt.pivot_table(index='combined', columns='rsrp-l2100', values='rsrp_serving', aggfunc='mean')
	pivot_count = dask_gdf_mdt.pivot_table(index='combined', columns='rsrp-l2100', values='rsrp_serving', aggfunc='count')	

	#pivot = pivot.compute()
	pivot_mean.to_csv('result/rsrp-polygon-l2100.csv')
	pivot_count.to_csv('result/rsrp-polygon-l2100-pop.csv')

	print('Sucess')
	elapsed = time.perf_counter() - s
	print(f"{__file__} executed in {elapsed:0.2f} seconds.")
