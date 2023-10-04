#!/usr/bin/python
# -- coding: utf-8 --


import enmscripting
from datetime import datetime, timedelta
import re
import os
import zipfile

enm = 'enm6'
session = enmscripting.open()
s_date = datetime.now().strftime("%Y%m%d")
# ------------------------------
# Clean old sync status data
dir_path = os.path.dirname(os.path.realpath(__file__))
result_path = '%s/cmData' % dir_path
if not os.path.exists(result_path):
    os.mkdir(result_path)
old_date = datetime.now() - timedelta(days=7)
s_date2 = old_date.strftime("%Y%m%d")

for file in os.listdir(result_path):
    if s_date2 in file:
        old_file = "%s/%s" % (result_path, file)
        os.remove(old_file)


def get_mo(mo, params, result_path, enm, s_date):

    dict_exception = {
        'AddressIPv4': 'oamIP',
        'CmFunction': 'syncStatus',
        'retsubunit': 'RET',
        'EUtranCellFDD': 'eUtranCellFDD'
    }

    print('Collecting %s' % mo)
    cmd = "cmedit get * %s.(%s) -t" % (mo, params)
    result = session.terminal().execute(cmd)
    if result.is_command_result_available():

        if (mo in dict_exception):
            mo = dict_exception[mo]

        fOutput = open('%s/%s_%s_%s.csv' % (result_path, enm, mo, s_date), 'w')
        i = 0
        for line in result.get_output():
            if line == '':
                break
            elif i == 1:
                fOutput.write('date,enm,%s\n' % (re.sub("\s+", ",", line)))
            elif i > 1:
                line = re.sub(",", ";", line)
                line = re.sub("\s+", ",", line)
                line = re.sub('(?<=;)(,)(?={|\w)', '', line)
                if not 'Error' in line:
                    fOutput.write('%s,%s,%s\n' % (s_date, enm, line))
            i += 1
            if i % 5000 == 0:
                print('   print %s site %s' % (mo, i))

        fOutput.close()
        zip_file = '%s/%s_%s_%s.zip' % (result_path, enm, mo, s_date)
        zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
        zf.write('%s/%s_%s_%s.csv' % (result_path, enm, mo, s_date),
                 '%s_%s_%s.csv' % (enm, mo, s_date))
        zf.close()
        os.remove('%s/%s_%s_%s.csv' % (result_path, enm, mo, s_date))


def get_mo_split(mo, params, split, result_path, enm, s_date):

    print('Collecting %s' % mo)
    fOutput = open('%s/%s_%s_%s.csv' % (result_path, enm, mo, s_date), 'w')
    batch = split
    irow = 0
    max_iter = (len(list_eNb) // batch) + 1
    for iter in range(max_iter):
        start = iter * batch
        stop = (iter + 1) * batch
        listBatchENb = list_eNb[start:stop]
        tempENb = ''
        for eNb in listBatchENb:
            if tempENb == '':
                tempENb = eNb
            else:
                tempENb = '%s;%s' % (tempENb, eNb)
        cmd = "cmedit get %s %s.(%s) -t" % (tempENb, mo, params)
        result = session.terminal().execute(cmd)
        if result.is_command_result_available():
            i = 0
            for line in result.get_output():
                if line == '':
                    break
                elif i == 1:
                    fOutput.write('date,enm,%s\n' % (re.sub("\s+", ",", line)))
                elif i > 1:
                    line = re.sub(",", ";", line)
                    line = re.sub("\s+", ",", line)
                    line = re.sub('(?<=;)(,)(?={|\w)', '', line)
                    if not 'Error' in line:
                        fOutput.write('%s,%s,%s\n' % (s_date, enm, line))
                i += 1
                irow += 1
                if irow % 10000 == 0:
                    print('   print %s %s' % (mo, irow))

    fOutput.close()
    if (mo == 'EUtranCellFDD'):
        zip_file = '%s/%s_%s_%s.zip' % (result_path,
                                        enm, 'eUtranCellFDD', s_date)
    else:
        zip_file = '%s/%s_%s_%s.zip' % (result_path, enm, mo, s_date)
    zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
    zf.write('%s/%s_%s_%s.csv' % (result_path, enm, mo, s_date),
             '%s_%s_%s.csv' % (enm, mo, s_date))
    zf.close()
    os.remove('%s/%s_%s_%s.csv' % (result_path, enm, mo, s_date))


def get_featureState(list_eNb, fOutput, feature):
    print('Collecting Feature State %s DL COMP' % feature)
    batch = 100
    irow = 0
    max_iter = (len(list_eNb) // batch) + 1
    for iter in range(max_iter):
        start = iter * batch
        stop = (iter + 1) * batch
        listBatchENb = list_eNb[start:stop]
        tempENb = ''
        for eNb in listBatchENb:
            if tempENb == '':
                tempENb = eNb
            else:
                tempENb = '%s;%s' % (tempENb, eNb)
        cmd = "cmedit get %s FeatureState.(featurestateid==%s, featurestate, licensestate) -t" % (
            tempENb, feature)
        result = session.terminal().execute(cmd)
        if result.is_command_result_available():
            i = 0
            for line in result.get_output():
                if line == '':
                    break
                elif i == 1:
                    fOutput.write('date,enm,%s\n' % (re.sub("\s+", ",", line)))
                elif i > 1:

                    line = re.sub(",", ";", line)
                    if not 'Error' in line:
                        fOutput.write('%s,%s,%s\n' %
                                      (s_date, enm, re.sub("\s+", ",", line)))
                i += 1
                irow += 1
                if irow % 5000 == 0:
                    print('   print FeatureState_%s %s' % (feature, irow))
    return fOutput


# ============RUN CMEDIT ====================
#cmd = "cmedit get * sectorequipmentfunction.availableHwOutputPower -t"

# Collecting eNodeB data
cmd = "cmedit get * enodebfunction.eNBId -t"
result = session.terminal().execute(cmd)
list_eNb = []
if result.is_command_result_available():
    fOutput = open('%s/%s_enbFunction_%s.csv' %
                   (result_path, enm, s_date), 'w')
    i = 0
    for line in result.get_output():

        if line == '':
            break
        elif i == 1:
            fOutput.write('date,enm,%s\n' % (re.sub("\s+", ",", line)))
        elif i > 1:
            fOutput.write('%s,%s,%s\n' %
                          (s_date, enm, re.sub("\s+", ",", line)))
            s = line.split()
            if len(s) > 1:
                list_eNb.append(s[0])
        i += 1
    fOutput.close()
    zip_file = '%s/%s_enbFunction_%s.zip' % (result_path, enm, s_date)
    zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
    zf.write('%s/%s_enbFunction_%s.csv' % (result_path, enm, s_date),
             '%s_enbFunction_%s.csv' % (enm, s_date))
    zf.close()
    os.remove('%s/%s_enbFunction_%s.csv' % (result_path, enm, s_date))
print("Found %s eNb on %s" % (len(list_eNb), enm))

if len(list_eNb) > 0:
    print("Start at %s" % (datetime.now().strftime("%Y%m%d %H.%M %p")))

    dict_param = {
        'sectorcarrier': 'radioTransmitPerfMode,noOfTxAntennas,noOfRxAntennas,ConfiguredMaxTxPower,MaximumtransmissionPower,sectorFunctionRef',
        'CmFunction': 'syncStatus',
        'ReportConfigA5': 'a5Threshold1Rsrp,a5Threshold2Rsrp,hysteresisA5,a5Threshold1Rsrq,a5Threshold2Rsrq',
        'ReportConfigEUtraInterFreqLb': 'a5Threshold1Rsrp, hysteresisA5, a5Threshold2Rsrp',
        'ReportConfigEUtraBestCell': 'a3offset, timeToTriggerA3, hysteresisA3',
        'ReportConfigEUtraBadCovPrim': 'a2ThresholdRsrpPrim, hysteresisA2Prim',
        'ReportConfigA1Prim': 'hysteresisA1Prim, a1ThresholdRsrpPrim',
        'ReportConfigSearch': 'a1a2SearchThresholdRsrp, a1a2SearchThresholdRsrq, a2CriticalThresholdRsrp, a2CriticalThresholdRsrq, hysteresisA2CriticalRsrp,hysteresisA2CriticalRsrq, inhibitA2SearchConfig, hysteresisA1A2SearchRsrp, hysteresisA1A2SearchRsrq',
        'retsubunit': 'electricalAntennaTilt,userLabel',
        'AddressIPv4': 'address',
        'Rrc': 't300,t301,t304,t311,t320,tRrcConnReest,tRrcConnectionReconfiguration,tWaitForRrcConnReest',
    }

    dict_param_split = {
        'EUtranCellFDD': {'params': 'cellid,dlChannelBandwidth,earfcndl,freqBand,crsgain,administrativeState,operationalState,pdschTypeBGain,sectorCarrierRef,pZeroNominalPucch,pZeroNominalPusch,qRxLevMin,threshServingLow,commonCqiPeriodicity',
                          'split': 1000
                          },
        'EUtranCellRelation': {'params': 'loadBalancing, cellIndividualOffsetEUtran, SCellCandidate, lbBnrAllowed, isHoAllowed,sCellPriority,amoAllowed,isRemoveAllowed',
                               'split': 30
                               },
        'EutranFreqRelation': {'params': 'lbActivationThreshold, voicePrio, lbA5Thr1RsrpFreqOffset, connectedModeMobilityPrio, interFreqMeasType, a5Thr2RsrpFreqOffset, a5Thr1RsrpFreqOffset, threshXHigh, threshXLow, qRxLevMin, qQualMin,'
                               + 'cellReselectionPriority, EUtranFreqToQciProfileRelation, endcHoFreqPriority, lbBnrPolicy, mobilityAction, caFreqPriority, anrMeasOn, allowedMeasBandwidth, caFreqProportion, amoAllowed',
                               'split': 1000
                               },
        
    }
    for mo in dict_param:
         get_mo(mo,
                dict_param[mo],
                result_path, enm, s_date
                )

    for mo in dict_param_split:
        get_mo_split(mo,
                     dict_param_split[mo]['params'], dict_param_split[mo]['split'],
                     result_path, enm, s_date
                     )    

    

