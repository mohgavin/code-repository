#!/home/nivag/.env/bin/python

import pandas as pd
import geopandas as gpd

print('Loading...')
polygon   = pd.read_csv('grid_folder/yogyasleman_xlarea.csv')
polygon   = gpd.GeoDataFrame(polygon, geometry=gpd.GeoSeries.from_wkt(polygon['WKT']), crs=4326)

grid	= pd.read_csv('grid_folder/40x40grid_yogyasleman.csv')
grid	= gpd.GeoDataFrame(grid, geometry=gpd.GeoSeries.from_wkt(grid['geometry']), crs=4326)

print('Processing...')
grid = gpd.sjoin(left_df= grid, right_df=polygon, how='inner', predicate='within')
grid.drop(columns=['index_right','WKT'], inplace=True)
grid.rename(columns={'id_left':'id'}, inplace=True)

grid['geometry'] = grid['geometry'].astype(str)
grid = pd.DataFrame(grid)

print('Saving...')
grid.to_parquet('grid_folder/40x40grid_yogyasleman_filtered.parquet', row_group_size=800000, index=False)
