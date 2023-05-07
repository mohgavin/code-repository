#!/home/nivag/cr_population_grid/.env/bin/python

import pandas as pd
import geopandas as gpd
import os
import numpy as np
from shapely.geometry import Polygon

print('Loading Polygon File....')
boundary = gpd.read_file('Polygon/Yogyakarta&Sleman.TAB')
boundary = boundary.to_crs(4326)

dir_path = 'CR_Data/'
csv_files = [f for f in os.listdir(dir_path) if f.endswith('.csv')]
cellrebel_consolidate = pd.DataFrame()
boundary['geometry-boundary'] = boundary['geometry'].astype(str)

print('Loading Polygon Complete...Now Loading Batched CR-File')
for file in csv_files:
    
    csv_path = os.path.join(dir_path, file)
    print(f'Loading File {file}')
    cellrebel = pd.read_csv(csv_path)
    cellrebel = cellrebel[~cellrebel['reference_signal_received_power'].isnull()]
    #cellrebel = cellrebel[cellrebel['province'].str.contains('Jawa Barat')]

    cellrebel = gpd.GeoDataFrame(cellrebel, geometry=gpd.points_from_xy(cellrebel.longitude, cellrebel.latitude), crs=4326)
    cellrebel['geometry-cellrebel'] = cellrebel['geometry'].astype(str)
    
    cellrebel = gpd.sjoin(left_df= cellrebel, right_df=boundary, how='left', predicate='within')

    cellrebel.drop(cellrebel.iloc[:,11:21],axis=1, inplace=True)
    cellrebel.drop(cellrebel.iloc[:,2:7],axis=1, inplace=True)
    
    cellrebel = cellrebel[~cellrebel['geometry-boundary'].isnull()]
    # cellrebel.fillna(0, inplace=True)
    cellrebel_consolidate = pd.concat([cellrebel, cellrebel_consolidate], ignore_index=True)

cellrebel_gpd = gpd.GeoDataFrame(cellrebel_consolidate, geometry=gpd.GeoSeries.from_wkt(cellrebel_consolidate['geometry-cellrebel']), crs=4326)

print('Consolidate Complete.... Now Creating Grid')

xmin, ymin, xmax, ymax = cellrebel_gpd.total_bounds

length =  0.0008985 * 4 / 10  ### (the distance for 100 metres is 0.0008985, so if you want to get 250 metres, need to multiply 2.5)
wide =  0.0008985 * 4 / 10  ### (the distance for 100 metres is 0.0008985, so if you want to get 250 metres, need to multiply 2.5)

cols = list(np.arange(xmin, xmax + wide, wide))
rows = list(np.arange(ymin, ymax + length, length))

polygons = []
for x in cols[:-1]: ### [:-1] these are used stop -1 of position from the end of list 
    for y in rows[:-1]:
        polygons.append(Polygon([(x,y), (x+wide, y), (x+wide, y+length), (x, y+length)]))

grid = gpd.GeoDataFrame({'geometry':polygons}, crs=4326)

grid.reset_index(inplace=True)
grid.rename(columns={'index':'id'}, inplace = True)
grid['geometry-grid'] = grid['geometry'].astype(str)

print('Grid Complete... Now processing the consolidate')
cellrebel_gpd = gpd.sjoin(left_df= cellrebel_gpd, right_df=grid, how='left', predicate='within')
cellrebel_gpd = pd.pivot_table(cellrebel_gpd, values = 'reference_signal_received_power', index = 'geometry-grid', columns = 'mobile_operator', aggfunc= len)

cellrebel_gpd.reset_index(inplace=True, col_level=1)
cellrebel_gpd.replace(np.nan, 0, inplace=True)

cellrebel_gpd['Grand Total'] = cellrebel_gpd['3'] + cellrebel_gpd['Indosat Ooredoo'] + cellrebel_gpd['PT Telekomunikasi Indonesia'] + cellrebel_gpd['Smartfren'] + cellrebel_gpd['Telkomsel'] + cellrebel_gpd['XL Axiata'] 

print('Saving To file....')

#cellrebel_gpd.to_csv('result_20230505.csv', index=False)
cellrebel_gpd = gpd.GeoDataFrame(cellrebel_gpd, geometry=gpd.GeoSeries.from_wkt(cellrebel_gpd['geometry-grid']), crs=4326)
cellrebel_gpd.to_file('result/cellrebel_populationmap_yogyakarta&sleman.tab', driver='MapInfo File')
