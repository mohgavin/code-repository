#!/home/nivag/.env/bin/python

# by @mohgavin
import dask.dataframe as dask_pd
import dask_geopandas as dask_gpd
import geopandas as gpd
from dask.distributed import Client

if __name__ == '__main__':
	client = Client(n_workers=2, threads_per_worker=4, processes=True)
	
	print('Loading Files')
	dask_df_throughput = dask_pd.read_csv('Compile-UETraffic/ue_traffic*.csv', usecols=[0,1,2,3,4,10,12,13], assume_missing=True)
	grid = dask_pd.read_csv('grid_folder/Universitas Indonesia_Depok.csv')
	
	print('Processing Data')
	#dask_df_throughput = dask_df_throughput[dask_df_throughput['ue_throughput_dl_drb_kbps'] > 0]
	dask_df_split = dask_df_throughput['hour'].str.split(pat='.', expand=True, n=1)
	dask_df_split = dask_df_split.rename(columns={0:"Hour + 7"})

	dask_df_throughput = dask_pd.concat([dask_df_throughput, dask_df_split['Hour + 7']], axis=1,join='inner', ignore_unknown_divisions=True)
	dask_df_throughput['Hour + 7'] = dask_df_throughput['Hour + 7'].astype(int) + 7
	dask_df_throughput['UE_Throughput'] = 'UE_Throughput'

	dask_df_throughput['pointer'] = "POINT (" + dask_df_throughput['longitude'].astype(str) + " " + dask_df_throughput['latitude'].astype(str) + ")"
	dask_gdf_throughput = dask_gpd.from_dask_dataframe(dask_df_throughput, geometry=dask_df_throughput["pointer"].map_partitions(gpd.GeoSeries.from_wkt, meta=gpd.GeoSeries([])),)
	
	grid['WKT_polygon'] = grid['WKT'].astype(str)
	dask_gdf_grid = dask_gpd.from_dask_dataframe(grid, geometry=grid['WKT_polygon'].map_partitions(gpd.GeoSeries.from_wkt, meta=gpd.GeoSeries([])),)

	print('Join Operation')
	dask_gdf_throughput = dask_gdf_throughput.sjoin(dask_gdf_grid, how='inner', predicate='within')
	dask_gdf_throughput = dask_gdf_throughput.drop(columns=['pointer'])

	dask_gdf_throughput['combined'] = dask_gdf_throughput['site'].astype(str) + "@" + dask_gdf_throughput['enodebid'].astype(str) + "@" + dask_gdf_throughput['ci'].astype(str) + "@" + dask_gdf_throughput['date'].astype(str) + "@" + dask_gdf_throughput['Hour + 7'].astype(str)
	dask_gdf_throughput['UE_Throughput'] = dask_gdf_throughput['UE_Throughput'].astype('category')
	dask_gdf_throughput['UE_Throughput'] = dask_gdf_throughput['UE_Throughput'].cat.as_known()

	print('Create Pivot')
	pivot = dask_gdf_throughput.pivot_table(index='combined', columns='UE_Throughput', values='ue_throughput_dl_drb_kbps', aggfunc='mean')
	
	pivot.to_csv('result/throughput-train.csv')
