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
    #dictionary region
    file_reg = '/var/opt/common5/script/gcell.csv'
    df_reg = pd.read_csv(file_reg)
    dict_region = df_reg.set_index('cellname').to_dict()['region']
    dict_kab = df_reg.set_index('cellname').to_dict()['kabupaten']
    dict_long = df_reg.set_index('cellname').to_dict()['longitude']
    dict_lat = df_reg.set_index('cellname').to_dict()['latitude']

    #cm
    file_cm = '/var/opt/common5/cm/all/20230517/power_20230517.zip'
    df_cm = pd.read_csv(file_cm)
    dict_bw = df_cm.set_index('EUtranCellFDDId').to_dict()['dlChannelBandwidth']
    dict_tx = df_cm.set_index('EUtranCellFDDId').to_dict()['noOfTxAntennas']
    dict_conf = df_cm.set_index('EUtranCellFDDId').to_dict()['configuredMaxTxPower']
    dict_crs = df_cm.set_index('EUtranCellFDDId').to_dict()['crsGain']
    dict_pb = df_cm.set_index('EUtranCellFDDId').to_dict()['pdschTypeBGain']
    
    list_df = []
    for dt in rrule.rrule(rrule.DAILY, dtstart=start_datetime, until=stop_datetime):
        s_date = dt.strftime("%Y%m%d")
        print('Filtering Jabo for %s'%s_date)

        file = '/var/opt/common5/eniq/lte/kpi/bh/lte_kpi_cell_bh_%s.zip'%(s_date)
        if os.path.exists(file):
            df = pd.read_csv(file)
            df['region'] = df['eutrancellfdd'].map(dict_region)
            df['kabupaten'] = df['eutrancellfdd'].map(dict_kab)
            df['longitude'] = df['eutrancellfdd'].map(dict_long)
            df['latitude'] = df['eutrancellfdd'].map(dict_lat)
            df = df.loc[df['region'].isin(['JABO1', 'JABO2'])]
            list_df.append(df)
        

        
    if len(list_df) > 0:
        df = pd.concat(list_df, axis=0)
        df['RRC_Connected_User'] = df['pmRrcConnLevSum'] / df['pmRrcConnLevSamp']

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

        print(df.columns)
        df['dlChannelBandwidth'] = df['eutrancellfdd'].map(dict_bw)
        df['noOfTxAntennas'] = df['eutrancellfdd'].map(dict_tx)
        df['configuredMaxTxPower'] = df['eutrancellfdd'].map(dict_conf)
        df['crsGain'] = df['eutrancellfdd'].map(dict_crs)
        df['pdschTypeBGain'] = df['eutrancellfdd'].map(dict_pb)
        filter_columns = ['date_id', 'hour_id', 'region', 'kabupaten', 'erbs', 'eutrancellfdd', 'longitude', 'latitude', 'dlChannelBandwidth', 'noOfTxAntennas', 'configuredMaxTxPower', 'crsGain', 'pdschTypeBGain', 'PRB_Utilization', 'CQI', 'User_DL_Throughput', 'RRC_Connected_User', 'Payload_GB', 'SE', 'MIMO_Gain', 'User_UL_Throughput', 'DLUT_p10', 'UL_Pathloss', 'Productivity', 'CSSR_PS', 'S1SR', 'CSFB_SR', 'Availability', 'HOSR_Intra', 'HOSR_Inter', 'RadioRecInterferencePwr', 'CDR_PS']

        df2 = df[filter_columns]
        df2.to_csv('/var/opt/common5/eniq/lte/kpi/bh_cell_temp_%s.csv'%s_date, index=None)
        zip_file = '/var/opt/common5/eniq/lte/kpi/bh_cell_temp_%s.zip'%(s_date)
        zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
        zf.write('/var/opt/common5/eniq/lte/kpi/bh_cell_temp_%s.csv'%(s_date), 'bh_cell_temp_%s.csv'%(s_date))
        zf.close()
        os.remove('/var/opt/common5/eniq/lte/kpi/bh_cell_temp_%s.csv'%(s_date))

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

        df_cluster.to_csv('/var/opt/common5/eniq/lte/kpi/cluster_bh_%s.csv'%s_date, index=None)

        #Kabupaten
        df_cluster = df.groupby(['date_id', 'kabupaten'], as_index=False).agg({
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

        df_cluster = df_cluster.rename(columns={'kabupaten': 'cluster'})

        filter_columns = ['date_id', 'cluster', 'PRB_Utilization', 'CQI', 'User_DL_Throughput', 'RRC_Connected_User', 'Payload_GB', 'SE', 'MIMO_Gain', 'User_UL_Throughput', 'DLUT_p10', 'UL_Pathloss', 'Productivity', 'CSSR_PS', 'S1SR', 'CSFB_SR', 'Availability', 'HOSR_Intra', 'HOSR_Inter', 'RadioRecInterferencePwr', 'CDR_PS']
        df_cluster = df_cluster[filter_columns]

        df_cluster.to_csv('/var/opt/common5/eniq/lte/kpi/kabupaten_bh_%s.csv'%s_date, index=None)
            
        
    print("finish")


