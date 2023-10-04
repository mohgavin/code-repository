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
    for dt in rrule.rrule(rrule.DAILY, dtstart=start_datetime, until=stop_datetime):
        s_date = dt.strftime("%Y%m%d")
        print('0. Collecting data %s ------------------------'%s_date)

        
        
        
        file = '/var/opt/common5/eniq/daily_cell_%s.zip'%(s_date)
        filter = '/var/opt/common5/eniq/Cell_Jakarta.csv'
        
        df = pd.read_csv(file)
        df_filter = pd.read_csv(filter)
        dict_cluster = df_filter.set_index('Cellname').to_dict()['Prov']
        df['cluster'] = df['eutrancellfdd'].map(dict_cluster)
        print('Original Data: %s'%len(df))
        df = df.loc[df['cluster'] == 'DKI JAKARTA']
        print('Jakarta Data: %s'%len(df))
        df['RRC_Connected_User'] = df['pmRrcConnLevSum'] / df['pmRrcConnLevSamp']
        df['Payload_GB'] = (df['pmPdcpVolDlDrb'] + df['pmPdcpVolDlSrb'] + df['pmPdcpVolUlDrb'] + df['pmPdcpVolULSrb']) / 8000000
        

        df_cluster = df.groupby(['date_id'], as_index=False).agg({
            'RRC_Connected_User': 'sum',
            'Payload_GB': 'sum',
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

        filter_columns = ['date_id', 'RRC_Connected_User', 'SE', 'PRB_Utilization', 'Payload_GB', 'CSSR_PS', 'S1SR', 'CSFB_SR', 'Availability', 'HOSR_Intra', 'HOSR_Inter', 'RadioRecInterferencePwr', 'MIMO_Gain', 'CDR_PS', 'User_DL_Throughput', 'User_UL_Throughput', 'CQI']
        df_cluster = df_cluster[filter_columns]
        list_df.append(df_cluster)

    if len(list_df) > 0:
        df = pd.concat(list_df, axis=0)
        df.to_csv('/var/opt/common5/eniq/jakarta_daily_%s.csv'%s_date, index=None)


        
        
    print("finish")


