#!/home/nivag/2023-Linux/.python-3.10/bin/python3.10

from statsmodels.tsa.arima.model import ARIMA
import pandas as pd

average_RRC = pd.read_csv('sample-folder/IOH-Active User.csv')

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

result.rename(columns={'RRC': 0, 'StartOfWeek':'ds'}, inplace=True)

concatenate = pd.DataFrame()

for x in result['SECTOR-ID'].unique().tolist():
    query = result[result['SECTOR-ID'] == x][['ds', 0]]
    query.set_index('ds', inplace=True)

    test = query
    test.index = test.index.to_period('W')
    model = ARIMA(test, order=(12,0,0), seasonal_order=(0, 0, 0, 2), trend=[0,0,0,1], enforce_stationarity=False)
    model = model.fit()
    #make forecast for the next 69 weeks
    forecast = model.forecast(steps=70)

    forecast = pd.concat([query, forecast], axis=0)
    forecast.reset_index(inplace=True)

    # query
    combine = pd.concat([forecast, pd.DataFrame({'SECTOR-ID':[x]*len(forecast.index)})], axis=1)

    concatenate = pd.concat([concatenate, combine], ignore_index=True, axis=0)

concatenate.rename(columns={'0':'y'}, inplace=True)
concatenate.to_csv('result/ARIMA-activeuser-result.csv', index=False)