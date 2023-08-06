#!/home/nivag/.env/bin/python

import os
import pandas as pd
import geopandas as gpd

dir_path = 'cellrebel-rf'
csv_files = [f for f in os.listdir(dir_path) if f.endswith('.csv')]
cellrebel_consolidate = pd.DataFrame()

kecamatan = pd.read_csv('grid_folder/Kecamatan_Jabotabek.csv')
kecamatan = gpd.GeoDataFrame(kecamatan, geometry=gpd.GeoSeries.from_wkt(kecamatan['WKT']), crs=4326)

for file in csv_files:

    csv_path = os.path.join(dir_path, file)
    cellrebel = pd.read_csv(csv_path)
    cellrebel = gpd.GeoDataFrame(cellrebel, geometry=gpd.points_from_xy(cellrebel['longitude'], cellrebel['latitude']), crs=4326)

    cellrebel = gpd.sjoin(left_df= cellrebel, right_df=kecamatan, how='inner', predicate='within')
    # cellrebel = cellrebel[~cellrebel['reference_signal_received_power'].isnull()]
    cellrebel_consolidate = pd.concat([cellrebel, cellrebel_consolidate], ignore_index=True)

cellrebel_consolidate.to_parquet('result/cellrebel_processed.parquet', index=False)
# cellrebel_consolidate.to_csv('result/consolidate_file.csv')
