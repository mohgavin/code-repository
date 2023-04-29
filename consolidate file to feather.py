#!/home/nivag/2023/2023 XL - Prophet Model Forecasting/.env/bin/python3.10

import pandas as pd
import os
import numpy as np

# check = pd.read_csv('/home/nivag/ookla-open-data/Cell_Capmon_2022_csv/4GCell_Cap_Data_2022_W2.csv')
consolidate = pd.DataFrame()

for x in os.listdir('Cell_Capmon_2022_csv'):
    files = pd.read_csv(os.path.join('Cell_Capmon_2022_csv', x), low_memory=False)
    files.columns = files.columns.str.strip()
    files.rename(columns={'W22' : 'Week'}, inplace=True, errors="ignore")
    files['filename'] = x
    # print(f'{x} have {len(files.columns)} columns')
    # print(files.columns)
    
    consolidate = pd.concat([consolidate, files], axis=0, ignore_index=True)

test = consolidate.iloc[:, list(range(33, 37, 1)) + [0] + [22] + [24] + list(range(26, 32, 1))]

consolidate.replace(to_replace='CENTRAL', value='Central', inplace=True)
consolidate.replace(np.nan, '0', inplace=True)
consolidate.drop(test, axis=1, inplace=True, errors='ignore')

# consolidate[['Long', 'Lat', 'Payload_GB']] = consolidate[['Long', 'Lat', 'Payload_GB']].astype(str)
consolidate.iloc[:, [13] + [14] + list(range(17, 23, 1))] = consolidate.iloc[:, [13] + [14] + list(range(17, 23, 1))].astype(str)
consolidate.to_csv('2022_cell_capacity.csv')
# consolidate.to_feather('2022_cell_capacity.feather')