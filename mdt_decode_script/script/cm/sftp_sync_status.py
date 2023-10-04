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
import json

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


def saveResult(df, moName, s_date, path):
    df.to_csv('%s/%s_%s.csv'%(path, moName, s_date), index=None)
    zip_file = '%s/%s_%s.zip'%(path, moName, s_date)
    zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
    zf.write('%s/%s_%s.csv'%(path, moName, s_date), '%s_%s.csv'%(moName, s_date))
    zf.close()
    os.remove('%s/%s_%s.csv'%(path, moName, s_date))
    


if __name__ == '__main__':
    python_file_path = os.path.dirname(os.path.realpath(__file__))    
    f = open('/var/opt/common5/script/credential.json')
    access = json.load(f)
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
    list_ssh = []
    list_ssh.append(access['enm_scripting']['enm1'])
    list_ssh.append(access['enm_scripting']['enm3'])
    list_ssh.append(access['enm_scripting']['enm4'])
    list_ssh.append(access['enm_scripting']['enm5'])
    list_ssh.append(access['enm_scripting']['enm6'])
    
    for dt in rrule.rrule(rrule.DAILY, dtstart=start_datetime, until=stop_datetime):
        s_date = dt.strftime("%Y%m%d")
        download_folder = '/var/opt/common5/cm/raw/%s' %(s_date)
        if not os.path.exists(download_folder):
            os.mkdir(download_folder)
        combined_folder = '/var/opt/common5/cm/all/%s' %(s_date)
        if not os.path.exists(combined_folder):
            os.mkdir(combined_folder)
        if getSFTP == '':
            for config in list_ssh:
                print('Collecting data from %s'%config['enm'])
                try:
                    get_syncStatus(config['enm'], config['host'], config['port'], config['user'], config['passwd'], download_folder, s_date)
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
                    #df_new = pd.read_csv('%s/%s'%(download_folder, file), delimiter=",", index_col=None, header='infer', usecols=list(range(0, 160)))
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

            saveResult(df, 'eUtranCellFDD', s_date, combined_folder)
            
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
            df_sectorcarrier['id_sector_carrier'] = df_sectorcarrier['NodeId'].astype('str') + "_" +df_sectorcarrier['SectorCarrierId'].astype('str')
            df[['temp','sectorCarrierId']] = df['sectorCarrierRef'].str.split('SectorCarrier=',expand=True)
            df['sectorCarrierId'] =df['sectorCarrierId'].str.replace(']','')
            df['id_sector_carrier'] = df['NodeId'].astype('str') + "_" +df['sectorCarrierId'].astype('str')
            del df['temp']
            del df['sectorCarrierRef']
            dict_tx = df_sectorcarrier.set_index('id_sector_carrier').to_dict()['noOfTxAntennas']
            df['noOfTxAntennas'] = df['id_sector_carrier'].map(dict_tx)

            dict_rx = df_sectorcarrier.set_index('id_sector_carrier').to_dict()['noOfRxAntennas']
            df['noOfRxAntennas'] = df['id_sector_carrier'].map(dict_rx)

            dict_confpower = df_sectorcarrier.set_index('id_sector_carrier').to_dict()['configuredMaxTxPower']
            df['configuredMaxTxPower'] = df['id_sector_carrier'].map(dict_confpower)

            dict_maxpower = df_sectorcarrier.set_index('id_sector_carrier').to_dict()['maximumTransmissionPower']
            df['maximumTransmissionPower'] = df['id_sector_carrier'].map(dict_maxpower)

           
        
        #SyncStatus
        print('Combining SyncStatus')
        list_df = []
        for file in os.listdir(download_folder):
            file_mo = 'syncStatus_%s'%s_date
            if file_mo in file:
                try:
                    df_new = pd.read_csv('%s/%s'%(download_folder, file), delimiter=",", index_col=None, header='infer')
                    initialData = len(df_new)
                    df_new = df_new.loc[pd.notnull(df_new['syncStatus'])]
                    df_new = df_new.loc[(df_new['NodeId'] != 'NodeId')]
                    print('    Initial Data: %s, Valid Data: %s'%(initialData, len(df_new)))
                    if len(df_new)>0:
                        list_df.append(df_new)
                except Exception as e:
                    print('Error: %s'%e)
        if len(list_df):
            df_sync = pd.concat(list_df, axis=0)
            dict_sync = df_sync.set_index('NodeId').to_dict()['syncStatus']
            df['syncStatus'] = df['NodeId'].map(dict_sync)
            
        
        #OAM IP
        print('Combining AddressIPv4')
        list_df = []
        for file in os.listdir(download_folder):
            file_mo = 'oamIP_%s'%s_date
            if file_mo in file:
                try:
                    df_new = pd.read_csv('%s/%s'%(download_folder, file), delimiter=",", index_col=None, header='infer')
                    initialData = len(df_new)
                    df_new = df_new.loc[pd.notnull(df_new['address'])]
                    df_new = df_new.loc[(df_new['NodeId'] != 'NodeId')]
                    print('    Initial Data: %s, Valid Data: %s'%(initialData, len(df_new)))
                    #filter OAM only
                    df_new = df_new.loc[df_new['RouterId'].isin(['OAM', 'vr_OAM'])]
                    if len(df_new)>0:
                        list_df.append(df_new)
                except Exception as e:
                    print('Error: %s'%e)
        if len(list_df):
            df_ip = pd.concat(list_df, axis=0)
            df_ip[['address','subnet']] = df_ip['address'].str.split('/',expand=True)
            dict_ip = df_ip.set_index('NodeId').to_dict()['address']
            df['oam_ip'] = df['NodeId'].map(dict_ip)           

        #sectorequipmentfunction
        print('Combining sectorequipmentfunction')
        list_df = []
        list_cols = ['date', 'enm', 'NodeId', 'NodeSupportId', 'SectorEquipmentFunctionId', 'availableHwOutputPower']
        for file in os.listdir(download_folder):
            file_mo = 'availableHwOutputPower_%s'%s_date
            if file_mo in file:
                try:
                    df_new = pd.read_csv('%s/%s'%(download_folder, file), delimiter=",", index_col=None, header='infer', usecols=[0,1,2,3,4,5], names=list_cols)
                    if len(df_new)>0:
                        df_new[['SectorEquipmentFunctionId','NodeSupportId']] = df_new[['SectorEquipmentFunctionId','NodeSupportId']].apply(pd.to_numeric, errors='coerce')
                        initialData = len(df_new)
                        df_new.loc[(df_new['SectorEquipmentFunctionId'] > 1000), 'availableHwOutputPower'] = df_new['SectorEquipmentFunctionId']
                        df_new.loc[(df_new['SectorEquipmentFunctionId'] > 1000), 'SectorEquipmentFunctionId'] = df_new['NodeSupportId']
                        df_new = df_new.loc[pd.notnull(df_new['availableHwOutputPower'])]
                        df_new = df_new.loc[(df_new['NodeId'] != 'NodeId')]
                        print('    Initial Data: %s, Valid Data: %s'%(initialData, len(df_new)))
                        list_df.append(df_new)
                except Exception as e:
                    print('Error for %s: %s'%(file, e))
        if len(list_df):
            df_sector = pd.concat(list_df, axis=0)
            if len(df_sector) > 0:
                try:
                    df_sector = df_sector.loc[pd.notnull(df_sector['SectorEquipmentFunctionId'])]
                    df_sector['id_sector_carrier'] = df_sector['NodeId'].astype('str') + "_" +df_sector['SectorEquipmentFunctionId'].astype(int).astype(str)
                    dict_availHwPower = df_sector.set_index('id_sector_carrier').to_dict()['availableHwOutputPower']

                    df['availableHwOutputPower'] = df['id_sector_carrier'].map(dict_availHwPower)
                except Exception as e:
                    print("Error on sectorequipmentfunction: %s"%e)        
        
        #EutranFreqRelation
        print('Combining EutranFreqRelation')
        list_df = []
        for file in os.listdir(download_folder):
            file_mo = 'EutranFreqRelation_%s'%s_date
            if file_mo in file:
                try:
                    df_new = pd.read_csv('%s/%s'%(download_folder, file), delimiter=",", index_col=None, header='infer', usecols=list(range(0, 27)))
                    initialData = len(df_new)
                    df_new = df_new.loc[(df_new['NodeId'] != 'NodeId')]
                    print('    Initial Data: %s, Valid Data: %s'%(initialData, len(df_new)))
                    if len(df_new)>0:
                        list_df.append(df_new)
                except Exception as e:
                    print('Error: %s'%e)
        if len(list_df):
            df_freq = pd.concat(list_df, axis=0)
            saveResult(df_freq, 'EutranFreqRelation', s_date, combined_folder)
            #f_out = '/var/opt/rca-ioh/cm_audit/EutranFreqRelation_%s.csv'%s_date
            #df_freq.to_csv(f_out, index=None)
        
        #ReportConfigSearch
        print('Combining ReportConfigSearch')
        list_df = []
        for file in os.listdir(download_folder):
            file_mo = 'ReportConfigSearch_%s'%s_date
            if file_mo in file:
                try:
                    df_new = pd.read_csv('%s/%s'%(download_folder, file), delimiter=",", index_col=None, header='infer')
                    initialData = len(df_new)
                    df_new = df_new.loc[(df_new['NodeId'] != 'NodeId')]
                    print('    Initial Data: %s, Valid Data: %s'%(initialData, len(df_new)))
                    if len(df_new)>0:
                        list_df.append(df_new)
                except Exception as e:
                    print('Error: %s'%e)
        if len(list_df):
            df_freq = pd.concat(list_df, axis=0)
            saveResult(df_freq, 'ReportConfigSearch', s_date, combined_folder)

        
        if len(df) > 0:
            print('Calculating RS Power')
            dict_re = {
                20000 : 1200,
                15000 : 900,
                10000 : 600,
                5000 : 300,
                3000 : 180,
                1400 : 72,
            }
            df[['configuredMaxTxPower','noOfRxAntennas', 'crsGain', 'dlChannelBandwidth']] = df[['configuredMaxTxPower','noOfRxAntennas', 'crsGain', 'dlChannelBandwidth']].apply(pd.to_numeric, errors='coerce')
            df['num_RE'] = df['dlChannelBandwidth'].map(dict_re)
            df['epre_mwatt'] = (df['configuredMaxTxPower']/df['noOfRxAntennas'])/df['num_RE']
            df['RE_Power'] = 10 * np.log10(df['epre_mwatt'])
            #Collect eutrancellfdd.adjustCrsPowerEnhEnabled and SectorCarrier.(essScLocalId, essScPairId) 
            # --> different rule for crsGain when adjustCrsPowerEnhEnabled off or ess active
            df['RS_Power'] = df['RE_Power'] + (df['crsGain']/100)

            del df['id_sector_carrier']
            saveResult(df, 'power', s_date, combined_folder)
            #file_output = '/var/opt/rca-ioh/cm_audit/power_%s.csv' %(s_date)
            #df.to_csv(file_output, index=None)

            

        '''
        #EUtranCellRelation
        print('Combining EUtranCellRelation')
        list_df = []
        for file in os.listdir(download_folder):
            file_mo = 'EUtranCellRelation_%s'%s_date
            if file_mo in file:
                df_new = pd.read_csv('%s/%s'%(download_folder, file), delimiter=",", index_col=None, header='infer')
                initialData = len(df_new)
                df_new = df_new.loc[pd.notnull(df_new['EUtranCellFDDId'])]
                df_new = df_new.loc[(df_new['NodeId'] != 'NodeId')]
                print('    Initial Data: %s, Valid Data: %s'%(initialData, len(df_new)))
                if len(df_new)>0:
                    list_df.append(df_new)
        if len(list_df):
            df_relation = pd.concat(list_df, axis=0)
            df_relation['eNBId'] = df_relation['NodeId'].map(dict_enb)
            df_relation['earfcn'] = df_relation['EUtranCellFDDId'].map(dict_earfcn)
            df_relation['earfcn'] = df_relation[['earfcn']].apply(pd.to_numeric, errors='coerce')
            df_relation['carrier'] = ''
            df_relation.loc[df_relation['earfcn'] == 525, 'carrier']  = 'L21_15'
            df_relation.loc[df_relation['earfcn'] == 500, 'carrier']  = 'L21_10'
            df_relation.loc[df_relation['earfcn'] == 75, 'carrier']  = 'L21_15'
            df_relation.loc[df_relation['earfcn'] == 100, 'carrier']  = 'L21_10'
            df_relation.loc[df_relation['earfcn'] == 1475, 'carrier']  = 'L18_10'
            df_relation.loc[df_relation['earfcn'] == 1625, 'carrier']  = 'L18_20'
            df_relation.loc[(df_relation['earfcn'] >= 3450) & (df_relation['earfcn'] < 3800), 'carrier']  = 'L9'
            df_relation.loc[(df_relation['earfcn'] == '') , 'carrier']  = df_relation['earfcn']


            df_relation['eNb_tgt'] = df_relation['EUtranCellRelationId'].map(dict_nodeb)
            df_relation['tgt_cell'] = df_relation['EUtranCellRelationId']
            df_relation['tgt_cell'] = df_relation['tgt_cell'].str.replace('5101-','')
            df_relation['tgt_cell'] = df_relation['tgt_cell'].str.replace('51089-','')
            df_relation['tgt_cell'] = df_relation['tgt_cell'].str.replace('5101_','')
            df_relation['tgt_cell'] = df_relation['tgt_cell'].str.replace('51089_','')
            df_relation[['eNb_tgt2']] = df_relation['tgt_cell'].str.split('-', 1, expand=True)[0]
            #df_relation['eNb_tgt2'] = df_relation[['eNb_tgt2']].apply(pd.to_numeric, errors='coerce')
            df_relation['cosite'] = 0
            df_relation.loc[df_relation['eNBId'] == df_relation['eNb_tgt'], 'cosite'] = 1
            df_relation.loc[(df_relation['cosite'] != 1) & (df_relation['eNBId'].astype('str') == df_relation['eNb_tgt2']), 'cosite'] = 1
            df_relation.loc[pd.notnull(df_relation['EUtranCellFDDId']), 'Sector'] = df_relation['EUtranCellFDDId'].str[-1:]
            dict_sector ={'1':'1', '4':'1', '7':'1',
                    '2':'2', '5':'2', '8':'2',
                    '3':'3', '6':'3', '9':'3'}
            df_relation = df_relation.replace({"Sector": dict_sector})

            df_relation.loc[pd.notnull(df_relation['EUtranCellRelationId']), 'SectorTgt'] = df_relation['EUtranCellRelationId'].str[-1:]
            df_relation = df_relation.replace({"SectorTgt": dict_sector})
            df_relation['cosector'] = 0
            df_relation.loc[(df_relation['cosite'] == 1)&(df_relation['Sector'] == df_relation['SectorTgt']), 'cosector'] = 1
            df_relation2 = df_relation.loc[(df_relation['cosite'] == 1 ) &(df_relation['cosector'] == 1)]
            #saveResult(df_relation2, 'eutrancellrelation_cosite')

            df_relation3 = df_relation2.loc[(df_relation2['EUtranFreqRelationId'].astype('int') < 3000) & (df_relation2['loadBalancing'] == 'NOT_ALLOWED')]
            #saveResult(df_relation3, 'loadbalancing_discrepancy')



            #saveResult(df_relation3, 'eutrancellrelation_test')
            '''




                
                



    

