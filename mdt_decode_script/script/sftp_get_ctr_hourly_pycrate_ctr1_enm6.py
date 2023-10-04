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
    num_worker = 2
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
    list_fix_site = ['SH4464816E', 'SH4464764E', 'SH4443476E', 'SH4443153E', 'SH4431789E', 'SH4431777E', 'SH4415645E', 'MC4464806E', 'MC4464745E', 'MC4464721E', 'MC4443373E', 'MC4443154E', 'MC4443128E', 'MC4443112E', 'MC4443109E', 'MC4443107E', 'MC4443103E', 'MC4431893E', 'MC4431804E', 'MC4415681E', 'MC4412405E', 'MC4410627E', 'BH4443115E', '446V243E', '446V021E', '4465554E', '4465333E', '4464827E', '4464814E', '4464746E', '4464732E', '4464681E', '4464521E', '4464416E', '4464385E', '4464364E', '4463764E', '4462783E', '4462591E', '4462553E', '4462552E', '4445779E', '4445660E', '4445241E', '4445183E', '4443532E', '4443514E', '4443475E', '4443458E', '4443390E', '4443339E', '4443325E', '4443303E', '4443110E', '4443101E', '4443096E', '4441047E', '4441043E', '4440948E', '4440940E', '443V060E', '443PC791E', '443PC413E', '4435724E', '4435536E', '4435065E', '4435039E', '4431994E', '4431990E', '4431939E', '4431907E', '4431874E', '4431867E', '4431842E', '4431821E', '4431800E', '441PXH259E', '441PX469E', '441PX432E', '441PX421E', '441PX358E', '441PX185E', '441PX146E', '441PX044E', '441PL509E', '441PL369E', '441PL347E', '441PL322E', '441PL285E', '441PL268E', '441PL003E', '441PCH516E', '441PC961E', '441PC780E', '441PC774E', '441PC528E', '441PC195E', '4415180E', '4413740E', '4410951E', '4410714E',
                     'SH4431849EB','441PL081E_CO','SH4431855E_CO','SH4431844EA','4435502E_CO','4431881E','441PX464E_BB','SH4431856E','4431922E_CO','5506G_CO','441PL420E_CO','4431522E','4435008E_CO','4431524E','4431338E','4431435E','4431353E','441PL482E_CO','4435026E_CO','442PL629E','4424784E','3424823G_CO','442PC186E','442PC002E_CO','441PX057E_CO','4424867E','5021G_CO','SH4424819E_CO','4425483E','4435496E_CO','441PL057E_CO','4431521E','4425461E_CO','441PL325E','4435085E_CO','SH3424840G_CO','441PC210E','4424769E_CO','MC4424690E_CO','3424816G_CO','5109G_CO','SH4424898E_CO','442V271E_CO','442PC232E_CO','BH4424741E','441PX154E','4425679E_CO','4424824E','4424822E','4424733E','BH4424843E','4424813E','4411115E_CO','4424814E','442PX428E_BB','4413415E_CO','4425492E','4424831E_CO','SH4424849E_CO','4425469E_CO','4424881E_CO','4424891E_CO','441PC092E_CO','MC4424812E','4425478E_CO','4424821E','4425071E_CO','4424924E','SH4424957E','4425115E_CO','4425070E_CO','4424240E_CO','4424303E','442A153E','SH4424350E','MC4424222E_CO','441PC003E','4424438E','4424216E','4425251E','4424306E','4424315E','4424369E','SH4422022E','4424311E','4425102E_CO','4424312E','4424387E_CO','4424308E','4424313E','4475052E_CO','4413423E_C2','SH4431845E_CO','441PL246E_CO','MC4430788E_CO','441PL062E_CO','4435023E_CO','441PC199E_CO','SH4431788E','4425000E','4424949E_CO','442PL658E','4424975E','SH4424783E','441PL466E','441PX180E','4425060E_CO','4424736E_CO','SH4415758E_CO','MC4424219E','4424220E_CO','441PL014E_CO','4425192E','4413420E_CO','4424482E','442A499E_CO','442V052E_CO','MC4424256E','BH4424232E','442V053E','3424455G_CO','4424367E_CO','441PL444E_CO','442PC015E_CO','4424405E_CO','441PL457E_CO','4425796E_CO','4424406E','4410676E_CO','SH4424249E','4422090E_CO','4424400E','4425592E','4424401E_C2','SH4424425E','4424385E_CO','4425247E_CO','4423438E','4424608E','MC4424202E_CO','MC4424234E','442V050E_CO','4424325E_CO','444PL577E_CO','MC4424271E','4424432E_CO','4424119E','4424381E_CO','MC4424204E_CO','4422116E_BB','4425462E_CO','4424296E']
    if set_site:
        if len(set_site) > 0:
            list_site = list(set_site)
            list_site = sorted(list_site,reverse=False)
            
            for i in range (len(list_site)):
                for fix_site in list_fix_site:
                    if fix_site in list_site[i]:
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
