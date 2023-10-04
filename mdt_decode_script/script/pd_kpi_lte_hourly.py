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


def q1(x):
    return x.quantile(0.1)
def q2(x):
    return x.quantile(0.5)
def q3(x):
    return x.quantile(0.9)

if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    tanggal = datetime.now()
    delta_day = 1
    delta_hour = 3
    s_hour_2 = s_hour = 0
    s_date = s_date_2 = ""

    start_datetime = tanggal - timedelta(hours=delta_hour)
    stop_datetime = tanggal
    if len(sys.argv) > 1:
        s_date = sys.argv[1]
        if s_date == "1":
            start_datetime = tanggal - timedelta(days=1)
            start_datetime = start_datetime.replace(hour=0, minute=00)

            stop_datetime = start_datetime
            stop_datetime = stop_datetime.replace(hour=23, minute=59)

        else:
            if len(sys.argv) >2:
                s_hour = int(sys.argv[2])
                if len(sys.argv) > 3:
                    s_date_2 = sys.argv[3]
                    if len(sys.argv) >4:
                        s_hour_2 = int(sys.argv[4])
                    else:
                        s_hour_2 = s_hour
                else:
                    s_date_2 = s_date
                    s_hour_2 = s_hour
            else:
                s_hour = s_hour_2 = 0
                s_date_2 = s_date
            
            start_datetime = datetime.strptime(s_date, '%Y%m%d')
            start_datetime = start_datetime.replace(hour=s_hour)
            stop_datetime = datetime.strptime(s_date_2, '%Y%m%d')
            stop_datetime = stop_datetime.replace(hour=s_hour_2, minute=59)
        print('start date: %s'%start_datetime)
        print('stop date: %s'%stop_datetime)


    python_file_path = os.path.dirname(os.path.realpath(__file__))
    list_df = []
    for dt in rrule.rrule(rrule.HOURLY, dtstart=start_datetime, until=stop_datetime):
        s_date = dt.strftime("%Y%m%d")
        hour = dt.hour
        print('0. Calculating KPI %s %02d ------------------------'%(s_date, hour))
        
        
        file = '/var/opt/common5/eniq/hourly/%s/hourly_cell_%s_%02d.zip'%(s_date, s_date, hour)
        if os.path.exists(file):
            df = pd.read_csv(file)
            df, df_view = lte_kpi.calculateKPI(df, ['date_id', 'hour_id', 'erbs', 'eutrancellfdd', 'eniqid'])
            
            id_date = '%s_%02d'%(s_date, hour)
            folder = '/var/opt/common5/eniq/hourly/%s' %(s_date)
            lte_kpi.saveResult(df, 'lte_kpi_cell_hourly', id_date, folder)
            lte_kpi.saveResult(df_view, 'viewLte_cell_hourly', id_date, folder)


        
        
    print("finish")


