#!/home/nivag/2023-Linux/.python-3.10/bin/python

import pandas as pd
from prophet import Prophet
from datetime import datetime
import holidays

average_RRC = pd.read_csv('sample-folder/IOH - Average of RRC - MC.csv')

list1 = average_RRC.columns[0:]
result = pd.DataFrame()

for x in range(2, len(average_RRC.columns), 1):

    sitelist = average_RRC[['Sector', 'Category']]
    duration_week = pd.DataFrame(data={'date': [list1[x]] * len(average_RRC.index)})
    content = pd.DataFrame(average_RRC.iloc[:,x]).reset_index(drop=True)
    content.rename({content.columns[0]:'RRC'}, axis=1, inplace=True)

    concat = pd.concat([sitelist, duration_week, content], axis=1)
    result = pd.concat([result, concat], axis=0)

result.reset_index(inplace=True, drop=True)
result['Year'] = result['date'].str.split('-WEEK').str[0]
result['WeekNumber'] = result['date'].str.split('-WEEK').str[1].astype(int)

result['StartOfWeek'] = pd.to_datetime(result['Year'], format='%Y') + pd.to_timedelta((result['WeekNumber'] - 1) * 7 + 1, unit='D')
result.drop(columns=['Year', 'WeekNumber'], inplace=True)

result.rename(columns={'RRC': 'y', 'StartOfWeek':'ds'}, inplace=True)

concatenate = pd.DataFrame()

for x in result['Sector'].unique().tolist():
    
    test = result[result['Sector'] == x][['ds', 'y']]
    
    model = Prophet()
    model.fit(test)

    future = model.make_future_dataframe(periods=7)
    
    forecast = model.predict(future)
    forecast = forecast[forecast['ds'] == '2023-06-12'][['ds', 'yhat']]

    forecast.insert(2, 'Sector', x)
    print(x)
    concatenate = pd.concat([concatenate, forecast], axis=0)

concatenate.to_excel('result/prophet-MC-RRC-result.xlsx')
