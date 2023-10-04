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


def q1(x):
    return x.quantile(0.1)
def q2(x):
    return x.quantile(0.5)
def q3(x):
    return x.quantile(0.9)

def saveResult(df, moName, s_date):
    df.to_csv('/var/opt/rca-ioh/data/%s_%s.csv'%(moName, s_date), index=None)
    zip_file = '/var/opt/rca-ioh/data/%s_%s.zip'%(moName, s_date)
    zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
    zf.write('/var/opt/rca-ioh/data/%s_%s.csv'%(moName, s_date), '%s_%s.csv'%(moName, s_date))
    zf.close()
    os.remove('/var/opt/rca-ioh/data/%s_%s.csv'%(moName, s_date))

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
    list_df_site = []
    for dt in rrule.rrule(rrule.DAILY, dtstart=start_datetime, until=stop_datetime):
        s_date = dt.strftime("%Y%m%d")
        print('0. Collecting data %s ------------------------'%s_date)

        
        
        
        file = '/var/opt/common5/eniq/hourly_cell_%s.zip'%(s_date)
        file_cluster = '/var/opt/common5/script/gcell.csv'
        #filter2 = '/var/opt/common5/eniq/Cell_Jakarta.csv'
        
        df = pd.read_csv(file)
        df_kab = pd.read_csv(file_cluster)

        dict_kab = df_kab.set_index('cellname').to_dict()['Kab_kot']
        dict_region = df_kab.set_index('cellname').to_dict()['region']
        df['kab'] = df['eutrancellfdd'].map(dict_kab)
        df['region'] = df['eutrancellfdd'].map(dict_region)
        print('Original Data: %s'%len(df))

        df['RRC_Connected_User'] = df['pmRrcConnLevSum'] / df['pmRrcConnLevSamp']
        df = df.sort_values(['date_id', 'eutrancellfdd', 'RRC_Connected_User'], ascending=[True, True, False])
        if not 'pmCount' in df.columns:
            df['pmCount'] = 4
        
        df['rank'] = df.groupby('eutrancellfdd').cumcount()+1
        df = df.loc[df['rank'] == 1]

        df['Payload_GB'] = (df['pmPdcpVolDlDrb'] + df['pmPdcpVolDlSrb'] + df['pmPdcpVolUlDrb'] + df['pmPdcpVolULSrb']) / 8000000
        df['SE'] = 1000.0 * df['pmPdcpVolDlDrb'] /(180.0 * df['pmPrbUsedDlDtch'])
        df['PRB_Utilization'] = (df['pmPrbUsedDlDtch'] + df['pmPrbUsedDlBcch'] + df['pmPrbUsedDlSrbFirstTrans'] + df['pmPrbUsedDlPcch']) /(df['pmPrbAvailDl'])
        df['S1SR'] = 100.0 * df['pmS1SigConnEstabSucc'] / df['pmS1SigConnEstabAtt']
        df['CSSR_PS'] = 100.0* (df['pmRrcConnEstabSucc'] / (df['pmRrcConnEstabAtt'] - df['pmRrcConnEstabAttReAtt'])) * ( df['pmS1SigConnEstabSucc'] / df['pmS1SigConnEstabAtt']) * ( df['pmErabEstabSuccInit'] / df['pmErabEstabAttInit'])
        df['CSFB_SR'] = 100.0 * (df['pmUeCtxtRelCsfbGsm']+df['pmUeCtxtRelCsfbWcdma']) / (df['pmUeCtxtEstabAttCsfb']+df['pmUeCtxtModAttCsfb'])
        df['Availability'] = 100.0 * (1 - (df['pmCellDowntimeMan'] + df['pmCellDowntimeAuto'])/(df['pmCount'] * 900))
        df['HOSR_Intra'] = 100.0 * (df['pmHoExeSuccLteIntraF'] / df['pmHoExeAttLteIntraF']) * (df['pmHoPrepSuccLteIntraF'] / df['pmHoPrepAttLteIntraF'])
        df['HOSR_Inter'] = 100.0 * (df['pmHoExeSuccLteInterF'] / df['pmHoExeAttLteInterF']) * (df['pmHoPrepSuccLteInterF'] / df['pmHoPrepAttLteInterF'])
        df['RadioRecInterferencePwr'] = df['DC_pmRadioRecInterferencePwr'] / df['pmRadioRecInterferencePwr']
        df['MIMO_Gain'] = ((1.0*(df['pmRadioTxRankDistr_1'] + df['pmRadioTxRankDistr_3']) )+ (2.0*(df['pmRadioTxRankDistr_2'] + df['pmRadioTxRankDistr_4'])) + (3*(df['pmRadioTxRankDistr_8'] + df['pmRadioTxRankDistr_12'])) + (4*(df['pmRadioTxRankDistr_9'] + df['pmRadioTxRankDistr_13'])))/ (df['pmRadioTxRankDistr_1'] + df['pmRadioTxRankDistr_3'] + df['pmRadioTxRankDistr_2'] + df['pmRadioTxRankDistr_4'] + df['pmRadioTxRankDistr_8'] + df['pmRadioTxRankDistr_12'] + df['pmRadioTxRankDistr_9'] + df['pmRadioTxRankDistr_13'])
        df['CDR_PS'] = 100.0 * df['pmErabRelAbnormalEnbAct'] / (df['pmErabRelAbnormalEnb'] + df['pmErabRelNormalEnb'] + df['pmErabRelMme'])
        df['User_DL_Throughput'] = (df['pmPdcpVolDlDrb'] - df['pmPdcpVolDlDrbLastTTI']) / df['pmUeThpTimeDl']
        df['User_UL_Throughput'] = (df['pmUeThpVolUl']) / df['pmUeThpTimeUl']
        df['CQI'] = (df['pmRadioUeRepCqiDistr_N']) / df['pmRadioUeRepCqiDistr_D']
        df['UL_Pathloss'] = (df['pmUlPathlossDistr_N']) / df['pmUlPathlossDistr_D']
        df['Productivity'] = 1000.0 * df['pmPdcpVolDlDrb'] / df['pmPrbAvailDl']
        df['DLUT_p10'] = df['User_DL_Throughput']
        
        #filter
        #df_region = df.loc[df['region'] == 'DKI JAKARTA']
        #df = df.loc[df['cluster'] == 'GC']
        #print('Jakarta Data: %s'%len(df))

        #df2 = df[['date_id', 'hour_id','erbs', 'eutrancellfdd', 'PRB_Utilization', 'CQI', 'User_DL_Throughput', 'RRC_Connected_User', 'Payload_GB', 'SE', 'MIMO_Gain', 'User_UL_Throughput', 'UL_Pathloss', 'Productivity']]
        #list_df_site.append(df2)
        

        df.to_csv('/var/opt/common5/eniq/bh_cell_%s.csv'%s_date, index=None)
        zip_file = '/var/opt/common5/eniq/bh_cell_%s.zip'%(s_date)
        zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
        zf.write('/var/opt/common5/eniq/bh_cell_%s.csv'%(s_date), 'bh_cell_%s.csv'%(s_date))
        zf.close()
        os.remove('/var/opt/common5/eniq/bh_cell_%s.csv'%(s_date))

        #Kab
        df_cluster = df.groupby(['date_id', 'kab'], as_index=False).agg({
            'RRC_Connected_User': 'sum',
            'Payload_GB': 'sum',
            'DLUT_p10': q1,
            'pmPdcpVolDlDrb': 'sum',
            'pmPrbUsedDlDtch': 'sum',
            'pmPrbUsedDlBcch': 'sum',
            'pmPrbUsedDlPcch': 'sum',
            'pmPrbUsedDlSrbFirstTrans': 'sum',
            'pmPrbAvailDl' : 'sum',
            'pmRrcConnLevSum' : 'sum',
            'pmRrcConnLevSamp' : 'sum',
            'pmPdcpVolDlDrb' : 'sum',
            'pmPdcpVolDlSrb' : 'sum',
            'pmPdcpVolUlDrb' : 'sum',
            'pmPdcpVolULSrb' : 'sum',
            'pmS1SigConnEstabSucc' : 'sum',
            'pmS1SigConnEstabAtt' : 'sum',
            'pmRrcConnEstabSucc' : 'sum',
            'pmRrcConnEstabAtt' : 'sum',
            'pmRrcConnEstabAttReAtt' : 'sum',
            'pmErabEstabSuccInit' : 'sum',
            'pmErabEstabAttInit' : 'sum',
            'pmUeCtxtRelCsfbGsm' : 'sum',
            'pmUeCtxtEstabAttCsfb' : 'sum',
            'pmUeCtxtModAttCsfb' : 'sum',
            'pmUeCtxtRelCsfbWcdma' : 'sum',
            'pmCount' : 'sum',
            'pmCellDowntimeAuto' : 'sum',
            'pmCellDowntimeMan' : 'sum',
            'pmHoExeSuccLteIntraF' : 'sum',
            'pmHoExeAttLteIntraF' : 'sum',
            'pmHoPrepSuccLteIntraF' : 'sum',
            'pmHoPrepAttLteIntraF' : 'sum',
            'pmHoExeSuccLteInterF' : 'sum',
            'pmHoExeAttLteInterF' : 'sum',
            'pmHoPrepSuccLteInterF' : 'sum',
            'pmHoPrepAttLteInterF' : 'sum',
            'DC_pmRadioRecInterferencePwr': 'sum',
            'pmRadioRecInterferencePwr': 'sum',
            'pmRadioTxRankDistr_1' : 'sum',
            'pmRadioTxRankDistr_2' : 'sum',
            'pmRadioTxRankDistr_3' : 'sum',
            'pmRadioTxRankDistr_4' : 'sum',
            'pmRadioTxRankDistr_8' : 'sum',
            'pmRadioTxRankDistr_9' : 'sum',
            'pmRadioTxRankDistr_12' : 'sum',
            'pmRadioTxRankDistr_13' : 'sum',
            'pmErabRelAbnormalEnbAct' : 'sum',
            'pmErabRelAbnormalEnb' : 'sum',
            'pmErabRelNormalEnb' : 'sum',
            'pmErabRelMme' : 'sum',
            'pmPdcpVolDlDrbLastTTI' : 'sum',
            'pmUeThpTimeDl' : 'sum',
            'pmUeThpVolUl' : 'sum',
            'pmUeThpTimeUl' : 'sum',
            'pmRadioUeRepCqiDistr_N' : 'sum',
            'pmRadioUeRepCqiDistr_D' : 'sum',
            'pmUlPathlossDistr_N' : 'sum',
            'pmUlPathlossDistr_D' : 'sum',
        })

        df_cluster['SE'] = 1000.0 * df_cluster['pmPdcpVolDlDrb'] /(180.0 * df_cluster['pmPrbUsedDlDtch'])
        df_cluster['PRB_Utilization'] = (df_cluster['pmPrbUsedDlDtch'] + df_cluster['pmPrbUsedDlBcch'] + df_cluster['pmPrbUsedDlSrbFirstTrans'] + df_cluster['pmPrbUsedDlPcch']) /(df_cluster['pmPrbAvailDl'])
        df_cluster['S1SR'] = 100.0 * df_cluster['pmS1SigConnEstabSucc'] / df_cluster['pmS1SigConnEstabAtt']
        df_cluster['CSSR_PS'] = 100.0* (df_cluster['pmRrcConnEstabSucc'] / (df_cluster['pmRrcConnEstabAtt'] - df_cluster['pmRrcConnEstabAttReAtt'])) * ( df_cluster['pmS1SigConnEstabSucc'] / df_cluster['pmS1SigConnEstabAtt']) * ( df_cluster['pmErabEstabSuccInit'] / df_cluster['pmErabEstabAttInit'])
        df_cluster['CSFB_SR'] = 100.0 * (df_cluster['pmUeCtxtRelCsfbGsm']+df_cluster['pmUeCtxtRelCsfbWcdma']) / (df_cluster['pmUeCtxtEstabAttCsfb']+df_cluster['pmUeCtxtModAttCsfb'])
        df_cluster['Availability'] = 100.0 * (1 - (df_cluster['pmCellDowntimeMan'] + df_cluster['pmCellDowntimeAuto'])/(df_cluster['pmCount'] * 900))
        df_cluster['HOSR_Intra'] = 100.0 * (df_cluster['pmHoExeSuccLteIntraF'] / df_cluster['pmHoExeAttLteIntraF']) * (df_cluster['pmHoPrepSuccLteIntraF'] / df_cluster['pmHoPrepAttLteIntraF'])
        df_cluster['HOSR_Inter'] = 100.0 * (df_cluster['pmHoExeSuccLteInterF'] / df_cluster['pmHoExeAttLteInterF']) * (df_cluster['pmHoPrepSuccLteInterF'] / df_cluster['pmHoPrepAttLteInterF'])
        df_cluster['RadioRecInterferencePwr'] = df_cluster['DC_pmRadioRecInterferencePwr'] / df_cluster['pmRadioRecInterferencePwr']
        df_cluster['MIMO_Gain'] = ((1.0*(df_cluster['pmRadioTxRankDistr_1'] + df_cluster['pmRadioTxRankDistr_3']) )+ (2.0*(df_cluster['pmRadioTxRankDistr_2'] + df_cluster['pmRadioTxRankDistr_4'])) + (3*(df_cluster['pmRadioTxRankDistr_8'] + df_cluster['pmRadioTxRankDistr_12'])) + (4*(df_cluster['pmRadioTxRankDistr_9'] + df_cluster['pmRadioTxRankDistr_13'])))/ (df_cluster['pmRadioTxRankDistr_1'] + df_cluster['pmRadioTxRankDistr_3'] + df_cluster['pmRadioTxRankDistr_2'] + df_cluster['pmRadioTxRankDistr_4'] + df_cluster['pmRadioTxRankDistr_8'] + df_cluster['pmRadioTxRankDistr_12'] + df_cluster['pmRadioTxRankDistr_9'] + df_cluster['pmRadioTxRankDistr_13'])
        df_cluster['CDR_PS'] = 100.0 * df_cluster['pmErabRelAbnormalEnbAct'] / (df_cluster['pmErabRelAbnormalEnb'] + df_cluster['pmErabRelNormalEnb'] + df_cluster['pmErabRelMme'])
        df_cluster['User_DL_Throughput'] = (df_cluster['pmPdcpVolDlDrb'] - df_cluster['pmPdcpVolDlDrbLastTTI']) / df_cluster['pmUeThpTimeDl']
        df_cluster['User_UL_Throughput'] = (df_cluster['pmUeThpVolUl']) / df_cluster['pmUeThpTimeUl']
        df_cluster['CQI'] = (df_cluster['pmRadioUeRepCqiDistr_N']) / df_cluster['pmRadioUeRepCqiDistr_D']
        df_cluster['UL_Pathloss'] = (df_cluster['pmUlPathlossDistr_N']) / df_cluster['pmUlPathlossDistr_D']
        df_cluster['Productivity'] = 1000.0 * df_cluster['pmPdcpVolDlDrb'] / df_cluster['pmPrbAvailDl']

        df_cluster = df_cluster.rename(columns={'kab': 'cluster'})

        filter_columns = ['date_id', 'cluster', 'PRB_Utilization', 'CQI', 'User_DL_Throughput', 'RRC_Connected_User', 'Payload_GB', 'SE', 'MIMO_Gain', 'User_UL_Throughput', 'DLUT_p10', 'UL_Pathloss', 'Productivity', 'CSSR_PS', 'S1SR', 'CSFB_SR', 'Availability', 'HOSR_Intra', 'HOSR_Inter', 'RadioRecInterferencePwr', 'CDR_PS']
        df_cluster = df_cluster[filter_columns]
        list_df.append(df_cluster)

        #Region
        df_cluster = df.groupby(['date_id', 'region'], as_index=False).agg({
            'RRC_Connected_User': 'sum',
            'Payload_GB': 'sum',
            'DLUT_p10': q1,
            'pmPdcpVolDlDrb': 'sum',
            'pmPrbUsedDlDtch': 'sum',
            'pmPrbUsedDlBcch': 'sum',
            'pmPrbUsedDlPcch': 'sum',
            'pmPrbUsedDlSrbFirstTrans': 'sum',
            'pmPrbAvailDl' : 'sum',
            'pmRrcConnLevSum' : 'sum',
            'pmRrcConnLevSamp' : 'sum',
            'pmPdcpVolDlDrb' : 'sum',
            'pmPdcpVolDlSrb' : 'sum',
            'pmPdcpVolUlDrb' : 'sum',
            'pmPdcpVolULSrb' : 'sum',
            'pmS1SigConnEstabSucc' : 'sum',
            'pmS1SigConnEstabAtt' : 'sum',
            'pmRrcConnEstabSucc' : 'sum',
            'pmRrcConnEstabAtt' : 'sum',
            'pmRrcConnEstabAttReAtt' : 'sum',
            'pmErabEstabSuccInit' : 'sum',
            'pmErabEstabAttInit' : 'sum',
            'pmUeCtxtRelCsfbGsm' : 'sum',
            'pmUeCtxtEstabAttCsfb' : 'sum',
            'pmUeCtxtModAttCsfb' : 'sum',
            'pmUeCtxtRelCsfbWcdma' : 'sum',
            'pmCount' : 'sum',
            'pmCellDowntimeAuto' : 'sum',
            'pmCellDowntimeMan' : 'sum',
            'pmHoExeSuccLteIntraF' : 'sum',
            'pmHoExeAttLteIntraF' : 'sum',
            'pmHoPrepSuccLteIntraF' : 'sum',
            'pmHoPrepAttLteIntraF' : 'sum',
            'pmHoExeSuccLteInterF' : 'sum',
            'pmHoExeAttLteInterF' : 'sum',
            'pmHoPrepSuccLteInterF' : 'sum',
            'pmHoPrepAttLteInterF' : 'sum',
            'DC_pmRadioRecInterferencePwr': 'sum',
            'pmRadioRecInterferencePwr': 'sum',
            'pmRadioTxRankDistr_1' : 'sum',
            'pmRadioTxRankDistr_2' : 'sum',
            'pmRadioTxRankDistr_3' : 'sum',
            'pmRadioTxRankDistr_4' : 'sum',
            'pmRadioTxRankDistr_8' : 'sum',
            'pmRadioTxRankDistr_9' : 'sum',
            'pmRadioTxRankDistr_12' : 'sum',
            'pmRadioTxRankDistr_13' : 'sum',
            'pmErabRelAbnormalEnbAct' : 'sum',
            'pmErabRelAbnormalEnb' : 'sum',
            'pmErabRelNormalEnb' : 'sum',
            'pmErabRelMme' : 'sum',
            'pmPdcpVolDlDrbLastTTI' : 'sum',
            'pmUeThpTimeDl' : 'sum',
            'pmUeThpVolUl' : 'sum',
            'pmUeThpTimeUl' : 'sum',
            'pmRadioUeRepCqiDistr_N' : 'sum',
            'pmRadioUeRepCqiDistr_D' : 'sum',
            'pmUlPathlossDistr_N' : 'sum',
            'pmUlPathlossDistr_D' : 'sum',
        })

        df_cluster['SE'] = 1000.0 * df_cluster['pmPdcpVolDlDrb'] /(180.0 * df_cluster['pmPrbUsedDlDtch'])
        df_cluster['PRB_Utilization'] = (df_cluster['pmPrbUsedDlDtch'] + df_cluster['pmPrbUsedDlBcch'] + df_cluster['pmPrbUsedDlSrbFirstTrans'] + df_cluster['pmPrbUsedDlPcch']) /(df_cluster['pmPrbAvailDl'])
        df_cluster['S1SR'] = 100.0 * df_cluster['pmS1SigConnEstabSucc'] / df_cluster['pmS1SigConnEstabAtt']
        df_cluster['CSSR_PS'] = 100.0* (df_cluster['pmRrcConnEstabSucc'] / (df_cluster['pmRrcConnEstabAtt'] - df_cluster['pmRrcConnEstabAttReAtt'])) * ( df_cluster['pmS1SigConnEstabSucc'] / df_cluster['pmS1SigConnEstabAtt']) * ( df_cluster['pmErabEstabSuccInit'] / df_cluster['pmErabEstabAttInit'])
        df_cluster['CSFB_SR'] = 100.0 * (df_cluster['pmUeCtxtRelCsfbGsm']+df_cluster['pmUeCtxtRelCsfbWcdma']) / (df_cluster['pmUeCtxtEstabAttCsfb']+df_cluster['pmUeCtxtModAttCsfb'])
        df_cluster['Availability'] = 100.0 * (1 - (df_cluster['pmCellDowntimeMan'] + df_cluster['pmCellDowntimeAuto'])/(df_cluster['pmCount'] * 900))
        df_cluster['HOSR_Intra'] = 100.0 * (df_cluster['pmHoExeSuccLteIntraF'] / df_cluster['pmHoExeAttLteIntraF']) * (df_cluster['pmHoPrepSuccLteIntraF'] / df_cluster['pmHoPrepAttLteIntraF'])
        df_cluster['HOSR_Inter'] = 100.0 * (df_cluster['pmHoExeSuccLteInterF'] / df_cluster['pmHoExeAttLteInterF']) * (df_cluster['pmHoPrepSuccLteInterF'] / df_cluster['pmHoPrepAttLteInterF'])
        df_cluster['RadioRecInterferencePwr'] = df_cluster['DC_pmRadioRecInterferencePwr'] / df_cluster['pmRadioRecInterferencePwr']
        df_cluster['MIMO_Gain'] = ((1.0*(df_cluster['pmRadioTxRankDistr_1'] + df_cluster['pmRadioTxRankDistr_3']) )+ (2.0*(df_cluster['pmRadioTxRankDistr_2'] + df_cluster['pmRadioTxRankDistr_4'])) + (3*(df_cluster['pmRadioTxRankDistr_8'] + df_cluster['pmRadioTxRankDistr_12'])) + (4*(df_cluster['pmRadioTxRankDistr_9'] + df_cluster['pmRadioTxRankDistr_13'])))/ (df_cluster['pmRadioTxRankDistr_1'] + df_cluster['pmRadioTxRankDistr_3'] + df_cluster['pmRadioTxRankDistr_2'] + df_cluster['pmRadioTxRankDistr_4'] + df_cluster['pmRadioTxRankDistr_8'] + df_cluster['pmRadioTxRankDistr_12'] + df_cluster['pmRadioTxRankDistr_9'] + df_cluster['pmRadioTxRankDistr_13'])
        df_cluster['CDR_PS'] = 100.0 * df_cluster['pmErabRelAbnormalEnbAct'] / (df_cluster['pmErabRelAbnormalEnb'] + df_cluster['pmErabRelNormalEnb'] + df_cluster['pmErabRelMme'])
        df_cluster['User_DL_Throughput'] = (df_cluster['pmPdcpVolDlDrb'] - df_cluster['pmPdcpVolDlDrbLastTTI']) / df_cluster['pmUeThpTimeDl']
        df_cluster['User_UL_Throughput'] = (df_cluster['pmUeThpVolUl']) / df_cluster['pmUeThpTimeUl']
        df_cluster['CQI'] = (df_cluster['pmRadioUeRepCqiDistr_N']) / df_cluster['pmRadioUeRepCqiDistr_D']
        df_cluster['UL_Pathloss'] = (df_cluster['pmUlPathlossDistr_N']) / df_cluster['pmUlPathlossDistr_D']
        df_cluster['Productivity'] = 1000.0 * df_cluster['pmPdcpVolDlDrb'] / df_cluster['pmPrbAvailDl']

        df_cluster = df_cluster.rename(columns={'region': 'cluster'})
        filter_columns = ['date_id', 'cluster', 'PRB_Utilization', 'CQI', 'User_DL_Throughput', 'RRC_Connected_User', 'Payload_GB', 'SE', 'MIMO_Gain', 'User_UL_Throughput', 'DLUT_p10', 'UL_Pathloss', 'Productivity', 'CSSR_PS', 'S1SR', 'CSFB_SR', 'Availability', 'HOSR_Intra', 'HOSR_Inter', 'RadioRecInterferencePwr', 'CDR_PS']
        df_cluster = df_cluster[filter_columns]
        list_df.append(df_cluster)

    if len(list_df) > 0:
        df = pd.concat(list_df, axis=0)
        df.to_csv('/var/opt/common5/eniq/cluster_bh_%s.csv'%s_date, index=None)
    
    #if len(list_df_site) > 0:
    #    df = pd.concat(list_df_site, axis=0)
    #    df.to_csv('/var/opt/common5/eniq/gc_cell_bh_%s.csv'%s_date, index=None)



        
        
    print("finish")


