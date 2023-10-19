#!/home/nivag/.env/bin/python

# by @mohgavin
import dask.dataframe as dask_pd
import dask_geopandas as dask_gpd
import geopandas as gpd
from dask.distributed import Client

if __name__ == '__main__':
	client = Client(n_workers=2, threads_per_worker=4, processes=True)
	
	print('Loading Files')
	dask_df_mdt =  dask_pd.read_csv('Compile-MDT/mdt*.csv', usecols=[0,1,2,3,4,6,7,8,9], assume_missing=True)
	boundary = dask_pd.read_csv('grid_folder/polygon_xl-eid_LautIncluded.csv')
	grid = dask_pd.read_csv('grid_folder/40x40grid_alljabo.csv')

	dask_df_mdt['rsrp'] = 'rsrp'

	print('Processing')
	grid['geometry_polygon'] = grid['geometry'].astype(str)
	dask_df_mdt['pointer'] = "POINT (" + dask_df_mdt['longitude'].astype(str) + " " + dask_df_mdt['latitude'].astype(str) + ")"

	dask_gdf_mdt = dask_gpd.from_dask_dataframe(dask_df_mdt, geometry=dask_df_mdt["pointer"].map_partitions(gpd.GeoSeries.from_wkt, meta=gpd.GeoSeries([])),)
	#dask_gdf_mdt = dask_gdf_mdt.set_crs(4326)

	dask_gdf_grid = dask_gpd.from_dask_dataframe(grid, geometry=grid['geometry_polygon'].map_partitions(gpd.GeoSeries.from_wkt, meta=gpd.GeoSeries([])),)
	#dask_gdf_grid = dask_gdf_grid.set_crs(4326)

	dask_gdf_boundary = dask_gpd.from_dask_dataframe(boundary, geometry=boundary['WKT'].map_partitions(gpd.GeoSeries.from_wkt, meta=gpd.GeoSeries([])),)

	print('Join Operation')
	dask_gdf_mdt = dask_gdf_mdt.sjoin(dask_gdf_boundary, how='inner', predicate='within')
	dask_gdf_mdt = dask_gdf_mdt.sjoin(dask_gdf_grid, how='inner', predicate='within')
	dask_gdf_mdt = dask_gdf_mdt.drop(columns=['index_right', 'id', 'pointer'])

	dask_gdf_mdt['rsrp'] = dask_gdf_mdt['rsrp'].astype('category')
	dask_gdf_mdt['rsrp'] = dask_gdf_mdt['rsrp'].cat.as_known()

	print('Create Pivot')
	pivot = dask_gdf_mdt.pivot_table(index='geometry_polygon', columns='rsrp', values='rsrp_serving', aggfunc='mean')
	
	#pivot = pivot.compute()
	pivot.to_csv('result/rsrp-40x40grid-alljabo.csv')