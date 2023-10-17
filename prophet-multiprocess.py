#!/home/nivag/2023-Linux/.python-3.10/bin/python3.10

import pandas as pd
from multiprocessing import Pool
from functools import partial
import holidays
from prophet import Prophet

average_RRC = pd.read_csv('sample-folder/IOH-Active User-3.csv')

list1 = average_RRC.columns[0:]
result = pd.DataFrame()

for x in range(1, len(average_RRC.columns), 1):

    sitelist = average_RRC[['SECTOR-ID']]
    duration_week = pd.DataFrame(data={'date': [list1[x]] * len(average_RRC.index)})
    content = pd.DataFrame(average_RRC.iloc[:,x]).reset_index(drop=True)
    content.rename({content.columns[0]:'RRC'}, axis=1, inplace=True)

    concat = pd.concat([sitelist, duration_week, content], axis=1)
    result = pd.concat([result, concat], axis=0)

result.reset_index(inplace=True, drop=True)
result['Year'] = result['date'].str.split('-W').str[0]
result['WeekNumber'] = result['date'].str.split('-W').str[1].astype(int)

result['StartOfWeek'] = pd.to_datetime(result['Year'], format='%Y') + pd.to_timedelta((result['WeekNumber'] - 1) * 7 + 1, unit='D')
result.drop(columns=['Year', 'WeekNumber'], inplace=True)

result.rename(columns={'RRC': 'y', 'StartOfWeek':'ds'}, inplace=True)

liburan = pd.concat([pd.DataFrame(holidays.ID(years=2023).items()), pd.DataFrame(holidays.ID(years=2024).items()), pd.DataFrame(holidays.ID(years=2025).items())], ignore_index=True)

liburan.rename(columns={0:'ds', 1:'holiday'}, inplace=True)

concatenate = pd.DataFrame()

# Define the processing function
def process_data(x):

    query = result[(result['SECTOR-ID'] == x) & (~result['y'].isnull())] [['ds', 'y' , 'SECTOR-ID']]
    query.reset_index(inplace=True, drop=True)

    test = query[['ds', 'y']]
    test.insert(2, 'floor', test['y'].mean() * 1.1)
    test.insert(3, 'cap', test['y'].mean() * 1.6)

    # model = Prophet(holidays=liburan, daily_seasonality= False, weekly_seasonality=False, yearly_seasonality=True, changepoint_prior_scale=0.9, seasonality_prior_scale=0.03)

    model = Prophet(growth='logistic', holidays=liburan, holidays_prior_scale= 1, daily_seasonality= True, weekly_seasonality= False, yearly_seasonality= True, changepoint_prior_scale = 1, seasonality_prior_scale = 0.05)
    
    model.fit(test)

    future = model.make_future_dataframe(periods=500)
    
    future.insert(1, 'floor', test['y'].mean() * 1.1)
    future.insert(2, 'cap', test['y'].mean() * 1.6)

    forecast = model.predict(future)
    forecast.insert(2, 'SECTOR-ID', x)
    print(x)
    
    forecast = forecast[forecast['ds'] > pd.to_datetime('2023-09-10')][['ds','yhat','SECTOR-ID']]
    forecast.rename(columns={'yhat':'y'}, inplace=True)
    forecast = pd.concat([forecast, query], axis=0, ignore_index=True).reset_index(drop=True)
    return forecast

# Function to handle the parallel processing
def parallel_processing(inputs, num_processes=12):
    with Pool(num_processes) as pool:
        results = pool.map(process_data, inputs)
    return pd.concat(results, ignore_index=True)

# List of unique 'SECTOR-ID' values
unique_sector_ids = result['SECTOR-ID'].unique().tolist()

# Define the partial function for the process_data function
# partial_process_data = partial(process_data)

# Process the data in parallel
concatenated_result = parallel_processing(unique_sector_ids)

# Save the concatenated result to a file
concatenated_result.to_csv('result/prohet-activeuser-result-multiprocess-3.csv', index=False)
