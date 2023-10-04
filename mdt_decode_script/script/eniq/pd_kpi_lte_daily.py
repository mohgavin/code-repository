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
    print("Query data to SQL")
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
    list_df = []
    for dt in rrule.rrule(rrule.DAILY, dtstart=start_datetime, until=stop_datetime):
        s_date = dt.strftime("%Y%m%d")
        print('0. Calculating KPI %s ------------------------'%s_date)
        
        
        file = '/var/opt/common5/eniq/lte/raw/daily/all/daily_cell_%s.zip'%(s_date)
        df = pd.read_csv(file)
        df, df_view = lte_kpi.calculateKPI(df, ['date_id', 'erbs', 'eutrancellfdd', 'eniqid'])
        
        lte_kpi.saveResult(df, 'lte_cell_daily', s_date, '/var/opt/common5/eniq/lte/kpi/daily')
        lte_kpi.saveResult(df_view, 'viewLte_cell_daily', s_date, '/var/opt/common5/eniq/lte/kpi/daily')


        
        
    print("finish")


