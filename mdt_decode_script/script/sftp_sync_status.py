import pandas as pd
import numpy as np 
from datetime import datetime, timedelta
import os
import sys
import paramiko
from dateutil import rrule
from scp import SCPClient
import zipfile
from zipfile import ZipFile

def getConnection(enm, host, port, username, password, download_folder, s_date):
    transport = paramiko.Transport((host,port))
    transport.connect(None,username, password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    scp = SCPClient(transport)

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, port, username=username, password=password)
    print('Connected')

    sftp = client.open_sftp()
    scp = SCPClient(client.get_transport())

    return client, sftp, scp

def closeConnection(sftp, scp, client):
    if sftp: sftp.close()
    if scp: scp.close()
    if client: client.close()

    print('SFTP closed on %s'%(datetime.now()))



def get_syncStatus(enm, host, port, username, password, download_folder, s_date):
    
    print('Collecting %s data %s'%(s_date, enm))
    client, sftp, scp = getConnection(enm, host, port, username, password, download_folder, s_date)
    
    os.chdir(download_folder)
    target_folder = '/home/shared/eidtriyul/cmData/'
    numFile = 0
    try:
        list_file = sftp.listdir(target_folder)
        print('      Listing %s file on %s done'%(len(list_file),target_folder))
        target_file = '%s.zip'%s_date
        for file in list_file:
            if target_file in file:
                sync_file = "%s/%s" %(target_folder,file)
                print('        downloading %s'%file)
                scp.get(sync_file)
                numFile += 1
                if numFile %5 == 4:
                    closeConnection(sftp, scp, client)
                    client, sftp, scp = getConnection(enm, host, port, username, password, download_folder, s_date)

                #os.rename(localfile, "%s/%s_%s" %(download_folder, enm, file))
        #target_file = '%s.csv'%s_date
        #for file in list_file:
        #    if target_file in file:
        #        sync_file = "%s/%s" %(target_folder,file)
        #        print('        downloading %s'%file)
        #        localfile = "%s/%s" %(download_folder, file)
        #        scp.get(sync_file)
        #        numFile += 1
        #        #os.rename(localfile, "%s/%s_%s" %(download_folder, enm, file))
        
        if numFile == 0:
            print('No data available yet')
            command = 'python /home/shared/eidtriyul/sync_status.py'
            try:
                stdin, stdout, stderr = client.exec_command(command)
            except Exception as e:
                print('Error: %s'%e)
    except Exception as e:
        print("Error: %s"%e)


    # Close
    closeConnection(sftp, scp, client)


def importToDatabase(df, schema, tablename, s_date):
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df = df.where(pd.notnull(df), None)

    conn_str = "host={} dbname={} user={} password={}".format('localhost', 'pmt', 'pmt', 'PMT@2020#123')
    conn = pg.connect(conn_str)
    sql = '''delete from %s.%s where date = '%s'
        ''' %(schema, tablename, s_date)
    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
    except Exception as e:
        print("Error: %s"%e)
        pass
    print('   deletion existing data complete')

    sql = 'INSERT INTO %s.%s (' %(schema, tablename)
    sValue = ') VALUES ('
    i = 0
    for col in df.columns:
        if i == 0:
            sql = '%s %s'%(sql, col)
            sValue = '%s %%s'%(sValue)
        else:
            sql = '%s, %s'%(sql, col)
            sValue = '%s, %%s'%(sValue)
        i += 1
    sql = '%s %s )'%(sql, sValue)

    cur = conn.cursor()
    data = [tuple(x) for x in df.values]
    try:
        cur.executemany(sql, data)
        conn.commit()
    except Exception as e:
        print("Error: %s"%e)
        conn.close()
        conn = pg.connect(conn_str)
        pass
    
    print('Importing data complete')
    
    conn.close()

def zipDF (df, dir, moName, s_date):
    df.to_csv('%s/%s_%s.csv'%(dir, moName, s_date), index=None)
    zip_file = '%s/%s_%s.zip'%(dir, moName, s_date)
    zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
    zf.write('%s/%s_%s'%(dir, moName, s_date), '%s_%s.csv'%(moName, s_date))
    zf.close()
    os.remove('%s/%s_%s.csv'%(dir, moName, s_date)) 


def saveResult(df, moName):
    df.to_csv('/var/opt/common5/cm/%s_%s'%(moName, s_date), index=None)
    zip_file = '/var/opt/common5/cm/%s_%s.zip'%(moName, s_date)
    zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
    zf.write('/var/opt/common5/cm/%s_%s'%(moName, s_date), '%s_%s.csv'%(moName, s_date))
    zf.close()
    os.remove('/var/opt/common5/cm/%s_%s'%(moName, s_date))
    


if __name__ == '__main__':
    python_file_path = os.path.dirname(os.path.realpath(__file__))
    tanggal = datetime.now()
    delta_day = 0
    start_datetime = tanggal - timedelta(days=delta_day)
    stop_datetime = start_datetime
    getSFTP = ''
    df = pd.DataFrame()
    if len(sys.argv) > 1:
        start_date = sys.argv[1]
        if len(sys.argv) > 2:
            stop_date = sys.argv[2]
            if len(sys.argv) > 3:
                getSFTP = sys.argv[3]
        else:
            stop_date = start_date
        start_datetime = datetime.strptime(start_date, '%Y%m%d')
        stop_datetime = datetime.strptime(stop_date, '%Y%m%d')
    
    list_enm = ['enm1', 'enm3', 'enm2']
    
    #Direct Connection with VPN IOH
    list_ssh = [ 
             {'host': '10.23.205.45', 'port':22, 'name': 'ENM1', 'user':'eidtriyul', 'pwd':'Gambir@123'},
             {'host': '10.167.34.21', 'port':22, 'name': 'ENM6', 'user':'eidtriyul', 'pwd':'Password*1'}, 
             {'host': '10.24.220.29', 'port':22, 'name': 'ENM4', 'user':'eidtriyul', 'pwd':'Gambir@123'},  
             {'host': '10.23.202.29', 'port':22, 'name': 'ENM3', 'user':'eidtriyul', 'pwd':'Enm#2021'},  
             {'host': '10.167.30.28', 'port':22, 'name': 'ENM5', 'user':'eidtriyul', 'pwd':'Enm#2021'},   
            ]
    for dt in rrule.rrule(rrule.DAILY, dtstart=start_datetime, until=stop_datetime):
        s_date = dt.strftime("%Y%m%d")
        download_folder = '/var/opt/common5/cm/%s' %(s_date)
        if not os.path.exists(download_folder):
            os.mkdir(download_folder)
        if getSFTP == '':
            for config in list_ssh:
                print('Collecting data from %s'%config['name'])
                try:
                    get_syncStatus(config['name'], config['host'], config['port'], config['user'], config['pwd'], download_folder, s_date)
                except Exception as e:
                    print('Error: %s'%e)
        
        #Combining data
        #ENodebFunction
        print('Combining ENodebFunction')
        list_df = []
        for file in os.listdir(download_folder):
            file_mo = 'enbFunction_%s'%s_date
            if file_mo in file:
                try:
                    df_new = pd.read_csv('%s/%s'%(download_folder, file), delimiter=",", index_col=None, header='infer', usecols=[0, 1, 2, 3, 4])
                    initialData = len(df_new)
                    df_new = df_new.loc[pd.notnull(df_new['eNBId'])]
                    df_new = df_new.loc[(df_new['NodeId'] != 'NodeId')]
                    df_new = df_new.loc[(df_new['NodeId'] != 'SubNetwork')]
                    print('    Initial Data: %s, Valid Data: %s'%(initialData, len(df_new)))
                    if len(df_new)>0:
                        list_df.append(df_new)
                except Exception as e:
                    print('Error: %s'%e)
        print('DF Data: %s'%len(list_df))
        if len(list_df):
            df_enb = pd.concat(list_df, axis=0)
            dict_enb = df_enb.set_index('NodeId').to_dict()['eNBId']
            print('Total data eNodeb: %s'%len(df_enb))


        #EUtranCellFDD
        print('Combining EUtranCellFDD')
        list_df = []
        for file in os.listdir(download_folder):
            file_mo = 'eUtranCellFDD_%s'%s_date
            if file_mo in file:
                try:
                    df_new = pd.read_csv('%s/%s'%(download_folder, file), delimiter=",", index_col=None, header='infer')
                    initialData = len(df_new)
                    df_new = df_new.loc[pd.notnull(df_new['EUtranCellFDDId'])]
                    df_new = df_new.loc[(df_new['NodeId'] != 'NodeId')]
                    df_new = df_new.loc[(df_new['NodeId'] != 'FDN')]
                    print('    Initial Data: %s, Valid Data: %s'%(initialData, len(df_new)))
                    if len(df_new)>0:
                        list_df.append(df_new)
                except Exception as e:
                    print('Error: %s'%e)
        if len(list_df):
            df = pd.concat(list_df, axis=0)
            df['eNBId'] = df['NodeId'].map(dict_enb)
            print('Total data Cell: %s'%len(df))
            print('Unique site: %s'%len(df['NodeId'].unique().tolist()))

            dict_earfcn = df.set_index('EUtranCellFDDId').to_dict()['earfcndl']
            dict_nodeb = df.set_index('EUtranCellFDDId').to_dict()['eNBId']

            saveResult(df, 'eUtranCellFDD')
            
        #SectorCarrier
        print('Combining SectorCarrier')
        list_df = []
        for file in os.listdir(download_folder):
            file_mo = 'sectorcarrier_%s'%s_date
            if file_mo in file:
                try:
                    df_new = pd.read_csv('%s/%s'%(download_folder, file), delimiter=",", index_col=None, header='infer', usecols=list(range(0, 11)))
                    initialData = len(df_new)
                    df_new = df_new.loc[pd.notnull(df_new['SectorCarrierId'])]
                    df_new = df_new.loc[(df_new['NodeId'] != 'NodeId')]
                    print('    Initial Data: %s, Valid Data: %s'%(initialData, len(df_new)))
                    if len(df_new)>0:
                        list_df.append(df_new)
                except Exception as e:
                    print('Error: %s'%e)
        if len(list_df):
            df_sectorcarrier = pd.concat(list_df, axis=0)
            df_sectorcarrier.to_csv('/var/opt/common5/script/sector_carrier.csv', index=None)
            df.to_csv('/var/opt/common5/script/eutrancellfdd.csv', index=None)

            print('check result...')
            df_sectorcarrier['id_sector_carrier'] = df_sectorcarrier['NodeId'].astype('str') + "_" +df_sectorcarrier['SectorCarrierId'].astype('str')
            print(df.columns)
            df[['temp','sectorCarrierId']] = df['sectorCarrierRef'].str.split('SectorCarrier=',expand=True)
            del df['sectorCarrierRef']
            df['sectorCarrierId'] =df['sectorCarrierId'].str.replace(']','')
            df['id_sector_carrier'] = df['NodeId'].astype('str') + "_" +df['sectorCarrierId'].astype('str')
            del df['temp']
            dict_tx = df_sectorcarrier.set_index('id_sector_carrier').to_dict()['noOfTxAntennas']
            df['noOfTxAntennas'] = df['id_sector_carrier'].map(dict_tx)

            dict_rx = df_sectorcarrier.set_index('id_sector_carrier').to_dict()['noOfRxAntennas']
            df['noOfRxAntennas'] = df['id_sector_carrier'].map(dict_rx)

            dict_confpower = df_sectorcarrier.set_index('id_sector_carrier').to_dict()['configuredMaxTxPower']
            df['configuredMaxTxPower'] = df['id_sector_carrier'].map(dict_confpower)

            dict_maxpower = df_sectorcarrier.set_index('id_sector_carrier').to_dict()['maximumTransmissionPower']
            df['maximumTransmissionPower'] = df['id_sector_carrier'].map(dict_maxpower)

            df.to_csv('/var/opt/common5/script/cm_%s.csv'%s_date, index=None)

           
        
        




                
                



    

