#!/home/nivag/.env/bin/python

# by @mohgavin
import dask.dataframe as dask_pd
import dask_geopandas as dask_gpd
import geopandas as gpd
import pandas as pd
from dask.distributed import Client

binning = [f"{i}".format(i) for i in range(-40, 80, 5)]
labeling = [f"{i} to {i+5}".format(i) for i in range(-40, 75, 5)]

if __name__ == '__main__':
	client = Client(n_workers=18, threads_per_worker=2, processes=True, env={"MALLOC_TRIM_THRESHOLD_":0})
	
	print('Loading Files...')
	dask_df_mdt =  dask_pd.read_csv('Compile-MDT/mdt*.csv', usecols=[2,3,4,6,7,8,9], low_memory=False, assume_missing=True, blocksize="125MB")
	dask_df_mdt['rsrp'] = 'rsrp'

	#grid = dask_pd.read_parquet('100x100_gridjabo.parquet')
	grid = dask_pd.read_csv('grid_folder/inbuilding-2.csv')

	print('Processing...')
	
	dask_df_mdt['pointer'] = "POINT (" + dask_df_mdt['longitude'].astype(str) + " " + dask_df_mdt['latitude'].astype(str) + ")"
		
	dask_gdf_mdt = dask_gpd.from_dask_dataframe(dask_df_mdt, geometry=dask_df_mdt["pointer"].map_partitions(gpd.GeoSeries.from_wkt, meta=gpd.GeoSeries([])),)
	#dask_gdf_mdt = dask_gdf_mdt.set_crs(4326)

	dask_gdf_grid = dask_gpd.from_dask_dataframe(grid, geometry=grid['WKT'].map_partitions(gpd.GeoSeries.from_wkt, meta=gpd.GeoSeries([])),)
	#dask_gdf_grid = dask_gdf_grid.set_crs(4326)

	#binning = binning.append(400)
	#labeling = labeling.append(f"76 - 400")

	#dask_gdf_mdt['Altitude-Category'] = dask_gdf_mdt['altitude'].map_partitions(pd.cut, bins=binning, labels=labeling)

	print('Join Operation...')
	dask_gdf_mdt = dask_gdf_mdt.sjoin(dask_gdf_grid, how='inner', predicate='intersects')
	dask_gdf_mdt = dask_gdf_mdt.drop(columns=['pointer'])

	dask_gdf_mdt['combined'] = dask_gdf_mdt['site'].astype(str) + "@" + dask_gdf_mdt['enodebid'].astype(str) + "@" + dask_gdf_mdt['ci'].astype(str) + "@" + dask_gdf_mdt['polygon_name'].astype(str) 
	dask_gdf_mdt['rsrp'] = dask_gdf_mdt['rsrp'].astype('category')
	dask_gdf_mdt['rsrp'] = dask_gdf_mdt['rsrp'].cat.as_known()

	print('Create Pivot...')
	pivot_mean = dask_gdf_mdt.pivot_table(index='combined', columns='rsrp', values='rsrp_serving', aggfunc='mean')
	pivot_count = dask_gdf_mdt.pivot_table(index='combined', columns='rsrp', values='rsrp_serving', aggfunc='count')	

	#pivot = pivot.compute()
	pivot_mean.to_csv('result/rsrp-combined-cellcontributor-polygon.csv')
	pivot_count.to_csv('result/rsrp-combined-cellcontributor-polygon-pop.csv')

	print('Finished...')
