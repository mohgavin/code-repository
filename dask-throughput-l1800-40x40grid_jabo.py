#!/home/nivag/.env/bin/python

# by @mohgavin
import dask.dataframe as dask_pd
import dask_geopandas as dask_gpd
import geopandas as gpd
from dask.distributed import Client

if __name__ == '__main__':
	client = Client(n_workers=3, threads_per_worker=4, processes=True)
	
	print('Loading Files')
	dask_df_throughput = dask_pd.read_csv('Compile-UETraffic/ue_traffic*.csv', usecols=[0,1,2,3,4,10,12,13], assume_missing=True)
	
	values_to_query = [4,5,6,14,15,16,17,18,24,25,26,34,35,36,44,45,46,54,55,56,64,65,66,74]
	dask_df_throughput = dask_df_throughput[dask_df_throughput['ci'].isin(values_to_query)]

	grid = dask_pd.read_parquet('grid_folder/40x40grid_alljabo_filtered.parquet')
	
	print('Processing Data...')

	grid['geometry_polygon'] = grid['geometry'].astype(str)
	dask_df_throughput = dask_df_throughput[dask_df_throughput['ue_throughput_dl_drb_kbps'] > 0]
	dask_df_split = dask_df_throughput['hour'].str.split(pat='.', expand=True, n=1)
	dask_df_split = dask_df_split.rename(columns={0:"Hour + 7"})

	dask_df_throughput = dask_pd.concat([dask_df_throughput, dask_df_split['Hour + 7']], axis=1,join='inner', ignore_unknown_divisions=True)
	dask_df_throughput['Hour + 7'] = dask_df_throughput['Hour + 7'].astype(int) + 7
	dask_df_throughput['UE_Throughput-L1800'] = 'UE_Throughput-L1800'

	dask_df_throughput['pointer'] = "POINT (" + dask_df_throughput['longitude'].astype(str) + " " + dask_df_throughput['latitude'].astype(str) + ")"
	dask_gdf_throughput = dask_gpd.from_dask_dataframe(dask_df_throughput, geometry=dask_df_throughput["pointer"].map_partitions(gpd.GeoSeries.from_wkt, meta=gpd.GeoSeries([])),)
	
	dask_gdf_grid = dask_gpd.from_dask_dataframe(grid, geometry=grid['geometry_polygon'].map_partitions(gpd.GeoSeries.from_wkt, meta=gpd.GeoSeries([])),)

	print('Join Operation')
	dask_gdf_throughput = dask_gdf_throughput.sjoin(dask_gdf_grid, how='inner', predicate='within')
	dask_gdf_throughput = dask_gdf_throughput.drop(columns=['pointer'])

	dask_gdf_throughput['combined'] = dask_gdf_throughput['site'].astype(str) + "@" + dask_gdf_throughput['enodebid'].astype(str) + "@" + dask_gdf_throughput['ci'].astype(str) + "@" + dask_gdf_throughput['geometry_polygon'].astype(str) + "@" + dask_gdf_throughput['Hour + 7'].astype(str)
	dask_gdf_throughput['UE_Throughput-L1800'] = dask_gdf_throughput['UE_Throughput-L1800'].astype('category')
	dask_gdf_throughput['UE_Throughput-L1800'] = dask_gdf_throughput['UE_Throughput-L1800'].cat.as_known()

	print('Create Pivot')
	pivot_mean = dask_gdf_throughput.pivot_table(index='combined', columns='UE_Throughput-L1800', values='ue_throughput_dl_drb_kbps', aggfunc='mean')
	pivot_count = dask_gdf_throughput.pivot_table(index='combined', columns='UE_Throughput-L1800', values='ue_throughput_dl_drb_kbps', aggfunc='count')

	pivot_mean.to_csv('result/throughput-l1800-40x40.csv')
	pivot_count.to_csv('result/throughput-l1800-40x40-pop.csv')
