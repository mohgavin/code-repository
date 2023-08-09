#!/home/nivag/.env/bin/python

# by @mohgavin
import dask.dataframe as dask_pd
import dask_geopandas as dask_gpd
import geopandas as gpd
from dask.distributed import Client

if __name__ == '__main__':
	client = Client(n_workers=7, threads_per_worker=2, processes=True, env={"MALLOC_TRIM_THRESHOLD_":0})
	
	print('Loading Files...')
	dask_df_mdt =  dask_pd.read_csv('Compile-MDT-Polygon/mdt*.csv', usecols=[2,4,6,7,9], low_memory=False, assume_missing=True, blocksize="100MB")
	dask_df_mdt['rsrp-combined'] = 'rsrp-combined'
	grid = dask_pd.read_parquet('grid_folder/5x5grid_GBK.parquet')

	print('Processing...')
	grid['geometry_polygon'] = grid['geometry'].astype(str)
	dask_df_mdt['pointer'] = "POINT (" + dask_df_mdt['longitude'].astype(str) + " " + dask_df_mdt['latitude'].astype(str) + ")"

	dask_gdf_mdt = dask_gpd.from_dask_dataframe(dask_df_mdt, geometry=dask_df_mdt["pointer"].map_partitions(gpd.GeoSeries.from_wkt, meta=gpd.GeoSeries([])),)
	#dask_gdf_mdt = dask_gdf_mdt.set_crs(4326)

	dask_gdf_grid = dask_gpd.from_dask_dataframe(grid, geometry=grid['geometry_polygon'].map_partitions(gpd.GeoSeries.from_wkt, meta=gpd.GeoSeries([])),)
	#dask_gdf_grid = dask_gdf_grid.set_crs(4326)

	print('Join Operation...')
	dask_gdf_mdt = dask_gdf_mdt.sjoin(dask_gdf_grid, how='inner', predicate='intersects')
	dask_gdf_mdt = dask_gdf_mdt.drop(columns=['pointer'])

	dask_gdf_mdt['rsrp-combined'] = dask_gdf_mdt['rsrp-combined'].astype('category')
	dask_gdf_mdt['rsrp-combined'] = dask_gdf_mdt['rsrp-combined'].cat.as_known()

	print('Create Pivot...')
	pivot_mean = dask_gdf_mdt.pivot_table(index='geometry_polygon', columns='rsrp-combined', values='rsrp_serving', aggfunc='mean')
	pivot_count = dask_gdf_mdt.pivot_table(index='geometry_polygon', columns='rsrp-combined', values='rsrp_serving', aggfunc='count')

	print('Saving Files...')	
	#pivot = pivot.compute()
	pivot_mean.to_csv('result/rsrp-combined-polygon-5x5.csv')
	pivot_count.to_csv('result/rsrp-combined-polygon-5x5-pop.csv')

	print('Finished...')
