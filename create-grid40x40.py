#!/home/nivag/.env/bin/python

import os
import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import Polygon

print('Loading Files')
cellrebel_pd =pd.read_csv('grid_folder/alljabo_xlarea.csv')
cellrebel_gpd = gpd.GeoDataFrame(cellrebel_pd, geometry=gpd.GeoSeries.from_wkt(cellrebel_pd['WKT']), crs=4326)

xmin, ymin, xmax, ymax = cellrebel_gpd.total_bounds

length =  0.0008985 / 2.5  ### (the distance for 100 metres is 0.0008985, so if you want to get 250 metres, need to multiply 2.5)
wide =  0.0008985 / 2.5 ### (the distance for 100 metres is 0.0008985, so if you want to get 250 metres, need to multiply 2.5)

print('Processing')
cols = list(np.arange(xmin, xmax + wide, wide))
rows = list(np.arange(ymin, ymax + length, length))

polygons = []
for x in cols[:-1]: ### [:-1] these are used stop -1 of position from the end of list 
    for y in rows[:-1]:
        polygons.append(Polygon([(x,y), (x+wide, y), (x+wide, y+length), (x, y+length)]))

grid = gpd.GeoDataFrame({'geometry':polygons}, crs=4326)

grid.reset_index(inplace=True)
grid.rename(columns={'index':'id'}, inplace = True)

print('Saving to File....')
grid.to_csv('grid_folder/40x40grid_alljabo.csv', index=False)
