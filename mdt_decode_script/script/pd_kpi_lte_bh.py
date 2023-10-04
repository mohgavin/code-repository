import pandas as pd
import numpy as np 
from datetime import datetime, timedelta
import os
import sys
from dateutil import rrule
import time
import warnings
import zipfile
from zipfile import ZipFile
import lte_kpi



if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    tanggal = datetime.now()
    delta_day = 1
    s_hour_2 = s_hour = 0
    s_date = s_date_2 = ""
    start_datetime = tanggal - timedelta(days=delta_day)
    stop_datetime = start_datetime
    if len(sys.argv) > 1:
        s_date = sys.argv[1]
        if len(sys.argv) >2:
            s_date_2 = sys.argv[2]
        else:
            s_date_2 = s_date

        start_datetime = datetime.strptime(s_date, '%Y%m%d')
        stop_datetime = datetime.strptime(s_date_2, '%Y%m%d')


    python_file_path = os.path.dirname(os.path.realpath(__file__))
    
    for dt in rrule.rrule(rrule.DAILY, dtstart=start_datetime, until=stop_datetime):
        s_date = dt.strftime("%Y%m%d")
        print('Calculating BH for %s'%s_date)
        list_df = []

        for hour in range(24):
            file = '/var/opt/common5/eniq/hourly/%s/hourly_cell_%s_%02d.zip'%(s_date, s_date, hour)
            if os.path.exists(file):
                df = pd.read_csv(file)
                list_df.append(df)
        if len(list_df) > 0:
            df = pd.concat(list_df, axis=0)
            df['RRC_Connected_User'] = df['pmRrcConnLevSum'] / df['pmRrcConnLevSamp']
            df = df.sort_values(['date_id', 'eutrancellfdd', 'RRC_Connected_User'], ascending=[True, True, False])
            if not 'pmCount' in df.columns:
                df['pmCount'] = 4
            df['rank'] = df.groupby('eutrancellfdd').cumcount()+1
            df = df.loc[df['rank'] == 1]
            df, df_view = lte_kpi.calculateKPI(df, ['date_id', 'hour_id', 'erbs', 'eutrancellfdd', 'eniqid'])
            
            id_date = '%s'%(s_date)
            folder = '/var/opt/common5/eniq/bh/%s' %(s_date)
            lte_kpi.saveResult(df, 'lte_kpi_cell_bh', id_date, folder)
            lte_kpi.saveResult(df_view, 'viewLte_cell_bh', id_date, folder)
        
        
        print('ReCalculating Hourly for %s'%s_date)
        for hour in range(24):
            file = '/var/opt/common5/eniq/hourly/%s/hourly_cell_%s_%02d.zip'%(s_date, s_date, hour)
            if os.path.exists(file):
                df = pd.read_csv(file)
                print('    ReCalculating KPI %s %02d ------------------------'%(s_date, hour))
        
                id_date = '%s_%02d'%(s_date, hour)
                folder = '/var/opt/common5/eniq/hourly/%s' %(s_date)
                lte_kpi.saveResult(df, 'lte_kpi_cell_hourly', id_date, folder)
                lte_kpi.saveResult(df_view, 'viewLte_cell_hourly', id_date, folder)
            
        
    print("finish")


