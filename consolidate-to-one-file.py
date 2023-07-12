#!/home/nivag/.env/bin/python

import os
import pandas as pd
import geopandas as gpd

dir_path = 'result/cr-result-xlarea.csv'
csv_files = [f for f in os.listdir(dir_path) if f.endswith('.part')]
cellrebel_consolidate = pd.DataFrame()

for file in csv_files:
	
	csv_path = os.path.join(dir_path, file)
	cellrebel = pd.read_csv(csv_path)
	cellrebel_consolidate = pd.concat([cellrebel, cellrebel_consolidate], ignore_index=True)
	
cellrebel_consolidate.to_parquet('result/cr-may23-rsrp.parquet', index=False)
