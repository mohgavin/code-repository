#!/home/nivag/.env/bin/python

# by @mohgavin
import dask.dataframe as dask_pd
import dask_geopandas as dask_gpd
import geopandas as gpd
from dask.distributed import Client
import time

if __name__ == '__main__':
	s = time.perf_counter()
	client = Client(n_workers=2, threads_per_worker=3, processes=True)
	
	print('Loading Files')
	dask_df_mdt =  dask_pd.read_csv('Compile-MDT/mdt*.csv', usecols=[0,1,2,3,4,6,7,9], assume_missing=True)
	dask_df_mdt['rsrp'] = 'rsrp'
	#grid = dask_pd.read_parquet('100x100_gridjabo.parquet')
	grid = dask_pd.read_csv('grid_folder/Universitas Indonesia_Depok.csv')

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

	dask_gdf_mdt['combined'] = dask_gdf_mdt['site'].astype(str) + "@" + dask_gdf_mdt['id'].astype(str) + "@" + dask_gdf_mdt['enodebid'].astype(str) + "@" + dask_gdf_mdt['hour'].astype(str) + "@" + dask_gdf_mdt['ci'].astype(str) + "@" + dask_gdf_mdt['WKT_polygon'].astype(str)
	dask_gdf_mdt['rsrp'] = dask_gdf_mdt['rsrp'].astype('category')
	dask_gdf_mdt['rsrp'] = dask_gdf_mdt['rsrp'].cat.as_known()

	print('Create Pivot')
	pivot = dask_gdf_mdt.pivot_table(index='combined', columns='rsrp', values='rsrp_serving', aggfunc='mean')
	
	#pivot = pivot.compute()
	pivot.to_csv('result/rsrp-polugon.csv')
	print('Sucess')
	elapsed = time.perf_counter() - s
	print(f"{__file__} executed in {elapsed:0.2f} seconds.")
