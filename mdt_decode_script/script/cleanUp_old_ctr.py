from __future__ import print_function
import os
import sys
import shutil
from datetime import datetime, timedelta
from dateutil import rrule

def delete_data(s_date):
    #list_enm = ['enm7','enm8','enm9','oss_kalimantan','oss_sumbagut2']

    
    path = '/var/opt/common5/ctr_mdt/%s' %(s_date)
    if os.path.exists(path):
        try:
            shutil.rmtree(path, ignore_errors=True)
            print("deleting ctr mdt on %s " %(path))
        except:
            pass

    dt = datetime.strptime(s_date, '%Y%m%d')
    start_datetime = dt - timedelta(days=60)
    s_date2 = start_datetime.strftime("%Y%m%d")

    path = '/var/opt/common5/ctr/%s' %(s_date)
    if os.path.exists(path):
        try:
            shutil.rmtree(path, ignore_errors=True)
            print("deleting ctr selected on %s " %(path))
        except:
            pass
    


if __name__ == '__main__':
    tanggal = datetime.now()
    delta_days = 2
    s_hour_2 = s_hour = 0
    s_date = s_date_2 = ""
    start_datetime = tanggal - timedelta(days=delta_days)
    stop_datetime = start_datetime
    if len(sys.argv) > 1:
        delta_days = int(sys.argv[1])
        start_datetime = tanggal - timedelta(days=delta_days)
        stop_datetime = start_datetime

    python_file_path = os.path.dirname(os.path.realpath(__file__))
    for dt in rrule.rrule(rrule.DAILY, dtstart=start_datetime, until=stop_datetime):
        sekarang = datetime.now()
        menit_sekarang = sekarang.minute
        s_date = dt.strftime("%Y%m%d")
        print("deleting ctr mdt %s " %(s_date))


        delete_data(s_date)



    
    

    
    

