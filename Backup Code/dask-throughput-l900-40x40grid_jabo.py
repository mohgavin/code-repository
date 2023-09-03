#!/home/nivag/.env/bin/python

# by @mohgavin
import dask.dataframe as dask_pd
import dask_geopandas as dask_gpd
import geopandas as gpd
from dask.distributed import Client

if __name__ == '__main__':
	client = Client(n_workers=5, threads_per_worker=2, processes=True, env={"MALLOC_TRIM_THRESHOLD_":0})
	
	print('Loading Files')
	dask_df_throughput = dask_pd.read_csv('Compile-UETraffic/ue_traffic*.csv', usecols=[4,10,11,12,13], low_memory=False, assume_missing=True, blocksize="125MB")
	
	values_to_query = [1, 2, 3, 11, 12, 13, 21, 22, 23, 31, 33, 41, 42, 43, 51]
	dask_df_throughput = dask_df_throughput[dask_df_throughput['ci'].isin(values_to_query)]

	grid = dask_pd.read_parquet('grid_folder/40x40grid_alljabo_filtered.parquet')
	
	print('Processing Data...')

	grid['geometry_polygon'] = grid['geometry'].astype(str)
	dask_df_throughput['UE_Throughput-L900'] = 'UE_Throughput-L900'

	dask_df_throughput['pointer'] = "POINT (" + dask_df_throughput['longitude'].astype(str) + " " + dask_df_throughput['latitude'].astype(str) + ")"
	dask_gdf_throughput = dask_gpd.from_dask_dataframe(dask_df_throughput, geometry=dask_df_throughput["pointer"].map_partitions(gpd.GeoSeries.from_wkt, meta=gpd.GeoSeries([])),)
	
	dask_gdf_grid = dask_gpd.from_dask_dataframe(grid, geometry=grid['geometry_polygon'].map_partitions(gpd.GeoSeries.from_wkt, meta=gpd.GeoSeries([])),)

	print('Join Operation')
	dask_gdf_throughput = dask_gdf_throughput.sjoin(dask_gdf_grid, how='inner', predicate='intersects')
	dask_gdf_throughput = dask_gdf_throughput.drop(columns=['pointer'])

	#dask_gdf_throughput['combined'] = dask_gdf_throughput['site'].astype(str) + "@" + dask_gdf_throughput['enodebid'].astype(str) + "@" + dask_gdf_throughput['ci'].astype(str) + "@" + dask_gdf_throughput['geometry_polygon'].astype(str)
	
	dask_gdf_throughput['UE_Throughput-L900'] = dask_gdf_throughput['UE_Throughput-L900'].astype('category')
	dask_gdf_throughput['UE_Throughput-L900'] = dask_gdf_throughput['UE_Throughput-L900'].cat.as_known()

	dask_gdf_dl_throughput = dask_gdf_throughput[dask_gdf_throughput['ue_throughput_dl_drb_kbps'] > 0]
	dask_gdf_ul_throughput = dask_gdf_throughput[dask_gdf_throughput['ue_throughput_ul_drb_kbps'] > 0]

	print('Create Pivot')
	pivot_dl_mean = dask_gdf_dl_throughput.pivot_table(index='geometry_polygon', columns='UE_Throughput-L900', values='ue_throughput_dl_drb_kbps', aggfunc='mean')
	pivot_dl_count = dask_gdf_dl_throughput.pivot_table(index='geometry_polygon', columns='UE_Throughput-L900', values='ue_throughput_dl_drb_kbps', aggfunc='count')

	pivot_ul_mean = dask_gdf_ul_throughput.pivot_table(index='geometry_polygon', columns='UE_Throughput-L900', values='ue_throughput_ul_drb_kbps', aggfunc='mean')
	pivot_ul_count = dask_gdf_ul_throughput.pivot_table(index='geometry_polygon', columns='UE_Throughput-L900', values='ue_throughput_ul_drb_kbps', aggfunc='count')

	pivot_dl_mean.to_csv('result/throughput-dl-l900-40x40.csv')
	pivot_dl_count.to_csv('result/throughput-dl-l900-40x40-pop.csv')

	pivot_ul_mean.to_csv('result/throughput-ul-l900-40x40.csv')
	pivot_ul_count.to_csv('result/throughput-ul-l900-40x40-pop.csv')

	print('Finished...')
