from __future__ import print_function
import os
import sys
import os.path
import paramiko
import shutil
import pandas as pd
from datetime import datetime, timedelta
import zipfile	
from zipfile import ZipFile
from dateutil import rrule
import multiprocessing
import re

def get_ctr(list_conn):
    num_worker = 4
    start_time_paralel = datetime.now()
    with multiprocessing.Pool(num_worker) as pool:
        pool.map(get_ctr_enm, list_conn)
    
    duration = datetime.now() - start_time_paralel
    print("Total duration time: %s" %(duration))

def get_ctr_enm(dict_conn):
    listFileError = []
    host = dict_conn['host']
    port = dict_conn['port']
    username = dict_conn['user']
    password = dict_conn['passwd']
    enm = dict_conn['enm']
    pmic = dict_conn['pmic']
    s_date = dict_conn['s_date']
    s_hour = dict_conn['s_hour']

    download_folder = '/var/opt/common5/ctr_mdt/%s' %(s_date)
    if not os.path.exists(download_folder):
        try:
            os.mkdir(download_folder)
        except:
            pass
    download_folder = '/var/opt/common5/ctr_mdt/%s/%02d' %(s_date, s_hour)
    if not os.path.exists(download_folder):
        try:
            os.mkdir(download_folder)
        except:
            pass
        
    transport = paramiko.Transport((host,port))
    transport.connect(None,username,password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    dict_pm = {
        'pmic1':'/ericsson/pmic1/CELLTRACE',
        'pmic2':'/ericsson/pmic2/CELLTRACE'
    }

    s_hour2 = '%02d'%s_hour
    list_hour = [s_hour2]
    #print("List hour: %s" %list_hour)


    set_site = set()
    
    folder = dict_pm[pmic]
    site_data = sftp.listdir(folder)
    for site in site_data:
        if not site in set_site:
            if site:
                set_site.add(site)

    #set to list
    
    list_ctr = []
    if set_site:
        if len(set_site) > 0:
            list_site = list(set_site)
            list_site = sorted(list_site,reverse=False)
            
            for i in range (len(list_site)):
                list_ctr.append(list_site[i])
            
    #======================================
    # Downloading selected CTR
    #======================================
    print("    %s Start Downloading %s CTR out of %s files on %s, hour: %s"%(enm, len(list_ctr), len(list_site), datetime.now(), list_hour))
    list_hour = [s_hour2]

    i = 0
    
    folder = dict_pm[pmic]
    site_data = sftp.listdir(folder)
    for enm_site in site_data:
        if enm_site in list_ctr:
            site_local = '%s/%s' %(download_folder, enm_site)
            try:
                if  not os.path.exists(site_local):
                    os.mkdir(site_local)
            except:
                pass

            #print(' downloading %s'%enm_site)
            folder_site = '%s/%s'%(folder, enm_site)

            
            try:
                rop_data = sftp.listdir(folder_site)
                #print("    found %s rop for site %s"%(len(rop_data), site))
                for rop in rop_data:
                    pathfile = os.path.join(folder_site, rop)
                    if pathfile.endswith('.gz'):
                        
                        s = re.split('\.',rop)
                        s_hour = s[1][:2]
                        #print('   found %s on %s'%(s_hour, pathfile))
                        if s_hour in list_hour:
                            #print('   downloading %s'%pathfile)
                            localpath = "%s/%s" %(site_local, rop)
                            filepath = pathfile

                            filesize = sftp.stat(filepath).st_size
                            chunksize = pow(4, 12)#<-- adjust this and benchmark speed
                            if filesize < chunksize:
                                sftp.get(filepath,localpath)
                            else:
                                chunks = [(offset, chunksize) for offset in range(0, filesize, chunksize)]
                                with sftp.open(filepath, "rb") as infile:
                                    with open(localpath, "wb") as outfile:
                                        for chunk in infile.readv(chunks):
                                            outfile.write(chunk)

                            if i % 800 == 0:
                                print('   Complete downloading %s ctr %s, on %s'%(i, enm, datetime.now()))
                            i += 1
                            #print('       download completed for %s'%rop)
            except Exception as e:
                print('error on %s'%enm_site)
                print('    error: %s'%e)
                
                dict_error = {}
                dict_error['site_local'] = localpath
                dict_error['pathfile'] = pathfile
                listFileError.append(dict_error)
                pass
                
    if len(listFileError) > 0:
        print('   ada %s rop yang error pas download'%(len(listFileError)))
        list2FileError = []
        for dict_error in listFileError:
            try:
                #os.chdir(dict_error['site_local'])
                sftp.get(dict_error['pathfile'],dict_error['site_local'])
            except:
                list2FileError.append(dict_error)
                pass
                            
                            
    
    
    print("    %s Complete Downloading %s CTR out of %s files on %s"%(enm, len(list_ctr), len(list_site), datetime.now()))

    if sftp: sftp.close()
    if transport: transport.close()
    


    



if __name__ == '__main__':
    now = datetime.now()
    delta_hours = 2
    s_hour_2 = s_hour = 0
    start_datetime = now - timedelta(hours=delta_hours) #60*3 +30
    stop_datetime = start_datetime.replace(minute=59)

    list_conn = [
        {
        'port': 22
        ,'enm': 'enm6'
        ,'host': '10.167.34.20'
        ,'passwd': 'Password*1'
        ,'user': 'eidtriyul'
        ,'pmic': 'pmic1'
        },
        {
        'port': 22
        ,'enm': 'enm6'
        ,'host': '10.167.34.21'
        ,'passwd': 'Password*1'
        ,'user': 'eidtriyul'
        ,'pmic': 'pmic2'
        },
    ]

    list_conn = [
	{
        'port': 22
        ,'enm': 'enm4'
        ,'host': '10.24.220.29'
        ,'passwd': 'Gambir@123'
        ,'user': 'eidtriyul'
	,'pmic': 'pmic1'
        ,'list_ctr': []
        },
        {
        'port': 22
        ,'enm': 'enm4'
        ,'host': '10.24.220.30'
        ,'passwd': 'Gambir@123'
        ,'user': 'eidtriyul'
	,'pmic': 'pmic2'
        ,'list_ctr': []
        },
        {
        'port': 22
        ,'enm': 'enm4'
        ,'host': '10.24.220.31'
        ,'passwd': 'Gambir@123'
        ,'user': 'eidtriyul'
	,'pmic': 'pmic1'
        ,'list_ctr': []
        },
        {
        'port': 22
        ,'enm': 'enm4'
        ,'host': '10.24.220.32'
        ,'passwd': 'Gambir@123'
        ,'user': 'eidtriyul'
	,'pmic': 'pmic2'
        ,'list_ctr': []
        },
    ]

    if len(sys.argv) > 1:
        s_date = sys.argv[1]
        if len(sys.argv) >2:
            s_hour = int(sys.argv[2])
            if len(sys.argv) > 3:
                s_date_2 = sys.argv[3]
                if len(sys.argv) >4:
                    s_hour_2 = int(sys.argv[4])
                    if len(sys.argv) > 5:
                        option = int(sys.argv[5])
                        if option == 0:
                            collect_data = True
                            calculate_kpi = False
                        elif option == 1:
                            collect_data = True
                            calculate_kpi = True
                        elif option == 2:
                            collect_data = False
                            calculate_kpi = True
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

    
    list_pmic = ['pmic1', 'pmic2']
    for dt in rrule.rrule(rrule.HOURLY, dtstart=start_datetime, until=stop_datetime):
        sekarang = datetime.now()
        menit_sekarang = sekarang.minute
        s_hour = dt.hour
        s_date = dt.strftime("%Y%m%d")
        s_date2 = dt.strftime("%Y-%m-%d")

        for i in range(len(list_conn)):
            list_conn[i]['s_date'] = s_date
            list_conn[i]['s_hour'] = s_hour
            
        print('CTR Collection %s %02d'%(s_date, s_hour))
        get_ctr(list_conn)
