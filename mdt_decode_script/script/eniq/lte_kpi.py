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

def saveResult(df, moName, s_date, path):
    if not os.path.exists(path):
        os.mkdir(path)
    df.to_csv('%s/%s_%s.csv'%(path, moName, s_date), index=None)
    zip_file = '%s/%s_%s.zip'%(path, moName, s_date)
    zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
    zf.write('%s/%s_%s.csv'%(path, moName, s_date), '%s_%s.csv'%(moName, s_date))
    zf.close()
    os.remove('%s/%s_%s.csv'%(path, moName, s_date))

def calculateKPI(df, listObject):
    df['SE'] = 1000.0 * df['pmPdcpVolDlDrb'] /(180.0 * df['pmPrbUsedDlDtch'])
    df['Availability'] = 100.0 * (1 - (df['pmCellDowntimeMan'] + df['pmCellDowntimeAuto'])/(df['pmCount'] * 900))
    df['SSSR'] = 100.0* (df['pmRrcConnEstabSucc'] / (df['pmRrcConnEstabAtt'] - df['pmRrcConnEstabAttReAtt'])) * ( df['pmS1SigConnEstabSucc'] / df['pmS1SigConnEstabAtt']) * ( df['pmErabEstabSuccInit'] / df['pmErabEstabAttInit'])
    df['S1SR'] = 100.0 * df['pmS1SigConnEstabSucc'] / df['pmS1SigConnEstabAtt']
    df['SARR'] = 100.0 * df['pmErabRelAbnormalEnbAct'] / (df['pmErabRelAbnormalEnb'] + df['pmErabRelNormalEnb'] + df['pmErabRelMme'])
    df['RSSI'] = df['DC_pmRadioRecInterferencePwr'] / df['pmRadioRecInterferencePwr']
    df['UL_Packet_Loss_Rate'] = 100.0 * (df['pmPdcpPktLostUl'] / (df['pmPdcpPktLostUl'] + df['pmPdcpPktReceivedUl']))
    df['DL_Packet_Loss_Rate'] = 100.0 * (df['pmPdcpPktDiscDlPelr'] + df['pmPdcpPktDiscDlHo'] + df['pmPdcpPktDiscDlPelrUu']) / (df['pmPdcpPktDiscDlPelr'] + df['pmPdcpPktDiscDlHo'] + df['pmPdcpPktDiscDlPelrUu'] + df['pmPdcpPktTransDl'])
    df['DL_uu_Latency'] =  df['pmPdcpLatTimeDl'] / df['pmPdcpLatPktTransDl']
    df['DL_PRB_Utilization'] =  100.0 * (df['pmPrbUsedDlFirstTrans'] * (1 + (df['pmPrbUsedDlReTrans']/df['pmPrbUsedDlFirstTrans']))) / df['pmPrbAvailDl']
    df['UL_PRB_Utilization'] =  100.0 * df['pmPrbUsedUlDtch'] / df['pmPrbAvailUl']
    df['PRB_Utilization2'] = (df['pmPrbUsedDlDtch'] + df['pmPrbUsedDlBcch'] + df['pmPrbUsedDlSrbFirstTrans'] + df['pmPrbUsedDlPcch']) /(df['pmPrbAvailDl'])
    df['PDCP_DL_User_Throughput'] =  (df['pmPdcpVolDlDrb'] - df['pmPdcpVolDlDrbLastTTI']) / df['pmUeThpTimeDl']
    df['RRC_Conn_max'] = df['pmRrcConnMax']
    df['RRC_Connected_User'] = df['pmRrcConnLevSum'] / df['pmRrcConnLevSamp']
    df['PDCP_DL_Cell_Throughput_Mbps'] =  df['pmPdcpVolDlDrb'] / df['pmSchedActivityCellDl']
    df['PDCP_UL_Cell_Throughput_Mbps'] =  df['pmPdcpVolUlDrb'] / df['pmSchedActivityCellUl']
    df['DL_Volume_GB'] = (df['pmPdcpVolDlDrb'] + df['pmPdcpVolDlSrb']) / 1000/1000/8
    df['UL_Volume_GB'] =  (df['pmPdcpVolUlDrb'] + df['pmPdcpVolUlDrb']) / 1000/1000/8
    df['Payload_GB'] = (df['DL_Volume_GB'] + df['UL_Volume_GB'])
    df['CBRA_SR'] =  100.0 * df['pmRaSuccCbra'] / df['pmRaAttCbra']
    df['CFRA_SR'] =  100.0 * df['pmRaSuccCfra'] / df['pmRaAttCfra']
    df['RRC_Setup_SR'] = 100.0 * (df['pmRrcConnEstabSucc'] / (df['pmRrcConnEstabAtt'] - df['pmRrcConnEstabAttReAtt']))
    df['ERAB_Setup_SR'] =  df['pmErabEstabSuccInit'] / df['pmErabEstabAttInit']
    df['HOSR_IntraF'] = 100.0 * (df['pmHoExeSuccLteIntraF'] / df['pmHoExeAttLteIntraF']) * (df['pmHoPrepSuccLteIntraF'] / df['pmHoPrepAttLteIntraF'])
    df['HOSR_InterF'] = 100.0 * (df['pmHoExeSuccLteInterF'] / df['pmHoExeAttLteInterF']) * (df['pmHoPrepSuccLteInterF'] / df['pmHoPrepAttLteInterF'])
    df['HOSR_IRAT'] = 100.0 * (df['pmHoPrepSucc'] / df['pmHoPrepAtt']) * (df['pmHoExeSucc'] / df['pmHoExeAtt'])

    
    df['IFLB_SR'] = 100.0 * (df['pmHoPrepSuccLteInterFLb'] / df['pmHoPrepAttLteInterFLb']) * (df['pmHoExeSuccLteInterFLb'] / df['pmHoExeAttLteInterFLb'])
    df['CA_Attempt'] = df['pmHoExecAttLteInterFCaRedirect']
    df['CA_SR'] = 100.0 * (df['pmHoExecSuccLteInterFCaRedirect'] / df['pmHoExecAttLteInterFCaRedirect'])
    
    df['CQI'] = (df['pmRadioUeRepCqiDistr_N']) / df['pmRadioUeRepCqiDistr_D']
    df['TX_Rank2'] = (df['pmRadioTxRankDistr_2'] + df['pmRadioTxRankDistr_4'] + df['pmRadioTxRankDistr_8'] + df['pmRadioTxRankDistr_9'] + df['pmRadioTxRankDistr_11'] + df['pmRadioTxRankDistr_12'] + df['pmRadioTxRankDistr_13'])/ (df['pmRadioTxRankDistr_1'] + df['pmRadioTxRankDistr_2'] + df['pmRadioTxRankDistr_3'] + df['pmRadioTxRankDistr_4'] + df['pmRadioTxRankDistr_5'] + df['pmRadioTxRankDistr_6'] + df['pmRadioTxRankDistr_7'] + df['pmRadioTxRankDistr_8'] + df['pmRadioTxRankDistr_9'] + df['pmRadioTxRankDistr_10'] + df['pmRadioTxRankDistr_11'] + df['pmRadioTxRankDistr_12'] + df['pmRadioTxRankDistr_13'])
    
    df['DL_QPSK_Distribution'] = 100.0 * df['pmMacHarqDlAckQpsk'] / (df['pmMacHarqDlAckQpsk'] + df['pmMacHarqDlAck16qam'] + df['pmMacHarqDlAck64qam'] +df['pmMacHarqDlAck256qam'])
    df['DL_16QAM_Distribution'] = 100.0 * df['pmMacHarqDlAck16qam'] / (df['pmMacHarqDlAckQpsk'] + df['pmMacHarqDlAck16qam'] + df['pmMacHarqDlAck64qam'] +df['pmMacHarqDlAck256qam'])
    df['DL_64QAM_Distribution'] = 100.0 * df['pmMacHarqDlAck64qam'] / (df['pmMacHarqDlAckQpsk'] + df['pmMacHarqDlAck16qam'] + df['pmMacHarqDlAck64qam'] +df['pmMacHarqDlAck256qam'])

    df['LTE_PUSCH_SINR'] = (df['N_pmSinrPuschDistr']) / df['pmSinrPuschDistr']
    df['LTE_PUCCH_SINR'] = (df['N_pmSinrPucchDistr']) / df['pmSinrPucchDistr']
    df['CSFB_SR'] = 100.0 * (df['pmUeCtxtRelCsfbGsm']+df['pmUeCtxtRelCsfbWcdma']) / (df['pmUeCtxtEstabAttCsfb']+df['pmUeCtxtModAttCsfb'])
    df['MIMO_Gain'] = ((1.0*(df['pmRadioTxRankDistr_1'] + df['pmRadioTxRankDistr_3']) )+ (2.0*(df['pmRadioTxRankDistr_2'] + df['pmRadioTxRankDistr_4'])) + (3*(df['pmRadioTxRankDistr_8'] + df['pmRadioTxRankDistr_12'])) + (4*(df['pmRadioTxRankDistr_9'] + df['pmRadioTxRankDistr_13'])))/ (df['pmRadioTxRankDistr_1'] + df['pmRadioTxRankDistr_3'] + df['pmRadioTxRankDistr_2'] + df['pmRadioTxRankDistr_4'] + df['pmRadioTxRankDistr_8'] + df['pmRadioTxRankDistr_12'] + df['pmRadioTxRankDistr_9'] + df['pmRadioTxRankDistr_13'])

    listKpi = listObject + ['Availability', 'PDCP_DL_User_Throughput', 'DL_PRB_Utilization', 'RRC_Connected_User', 'Payload_GB', 'SE', 'SSSR', 'S1SR', 'SARR', 'RSSI', 'HOSR_IntraF', 'HOSR_InterF', 'UL_Packet_Loss_Rate', 'DL_Packet_Loss_Rate', 'DL_uu_Latency', 'UL_PRB_Utilization', 'PRB_Utilization2', 'RRC_Conn_max', 'PDCP_DL_Cell_Throughput_Mbps', 'PDCP_UL_Cell_Throughput_Mbps', 'DL_Volume_GB', 'UL_Volume_GB', 'CBRA_SR', 'CFRA_SR', 'RRC_Setup_SR', 'ERAB_Setup_SR', 'HOSR_IRAT', 'IFLB_SR', 'CA_SR', 'CA_Attempt', 'CQI', 'TX_Rank2', 'MIMO_Gain', 'DL_QPSK_Distribution', 'DL_16QAM_Distribution', 'DL_64QAM_Distribution', 'LTE_PUSCH_SINR', 'LTE_PUCCH_SINR', 'pmCellDowntimeMan', 'pmCellDowntimeAuto', 'pmHoPrepAttLteIntraF', 'pmHoPrepAttLteInterF', 'pmHoPrepAttLteInterFLb', 'pmRrcConnEstabAtt', 'pmRrcConnEstabAttReAtt', 'pmErabEstabAttInit', 'pmErabRelAbnormalEnbAct', 'pmRaAttCbra']
    df_view = df[listKpi]
        

    
    return df, df_view
