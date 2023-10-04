import pyodbc
import pandas as pd
from datetime import date, datetime, timedelta
import os
import sys
import multiprocessing
import zipfile
from dateutil import rrule
import warnings
import json


def get_sql(q, enid):
    #=============================================
    # q = date (yyyymmdd)
    # enid : 
    #   LRAN : 141
    #   CENTRAL : new
    #   WRAN : en4
    #============================================


    query = """            
        select 
            date_id, erbs, eutrancellfdd, pmCount, pmCellDowntimeAuto, pmCellDowntimeMan, pmRrcConnEstabSucc, pmRrcConnEstabAtt, pmRrcConnEstabAttReAtt, pmS1SigConnEstabSucc, pmS1SigConnEstabAtt, pmErabEstabSuccInit, pmErabEstabAttInit, pmErabRelAbnormalEnbAct, pmErabRelAbnormalMmeAct, pmErabRelAbnormalEnb, pmErabRelNormalEnb, pmErabRelMme, pmPdcpPktLostUl, pmPdcpPktReceivedUl, pmPdcpPktDiscDlPelr, pmPdcpPktDiscDlHo, pmPdcpPktDiscDlPelrUu, pmPdcpPktTransDl, pmPdcpLatTimeDl, pmPdcpLatPktTransDl, pmPrbUsedDlFirstTrans, pmPrbUsedDlReTrans, pmPrbAvailDl, pmRrcConnMax, pmPrbUsedUlDtch, pmPrbAvailUl, pmPdcpVolDlDrb, pmPdcpVolDlDrbLastTTI, pmUeThpTimeDl, pmUeThpVolUl, pmUeThpTimeUl, pmRrcConnLevSum, pmRrcConnLevSamp, pmSchedActivityCellDl, pmPdcpVolUlDrb, pmSchedActivityCellUl, pmPdcpVolDlSrb, pmPdcpVolULSrb, pmRaSuccCbra, pmRaAttCbra, pmRaSuccCfra, pmRaAttCfra, pmRrcConnEstabFailLic, pmCaCapableDlSamp, pmCaConfiguredDlSamp, pmPdcpVolDlDrbCa, pmPdcpVolDlDrbLastTTICa, pmRadioThpVolDl, pmRadioThpVolDlSCell, pmRadioThpVolDlSCellExt, pmUeThpTimeDlCa, pmUeThpTimeUlCa, pmUeThpVolUlCa, pmLbSubRatioSamp, pmHoPrepSuccLteIntraF, pmHoPrepAttLteIntraF, pmHoPrepSuccLteInterF, pmHoPrepAttLteInterF, pmHoExeSuccLteIntraF, pmHoExeAttLteIntraF, pmHoExeSuccLteInterF, pmHoExeAttLteInterF, pmHoExeAttNonMob, pmHoExeSuccNonMob, pmHoPrepAttNonMob, pmHoPrepSuccNonMob, pmHoPrepAttLteInterFCaRedirect, pmHoPrepSuccLteInterFCaRedirect, pmHoExecAttLteInterFCaRedirect, pmHoExecSuccLteInterFCaRedirect, pmCaRedirectMeasRepUe, pmCaRedirectQualifiedUe, pmHoPrepAttLteInterFLb, pmHoPrepSuccLteInterFLb, pmHoExeAttLteInterFLb, pmHoExeSuccLteInterFLb, pmLbQualifiedUe, pmLbMeasRepUe, pmHoPrepSucc, pmHoPrepAtt, pmHoExeSucc, pmHoExeAtt, pmRadioUeRepCqiDistr_0, pmRadioUeRepCqiDistr_1, pmRadioUeRepCqiDistr_2, pmRadioUeRepCqiDistr_3, pmRadioUeRepCqiDistr_4, pmRadioUeRepCqiDistr_5, pmRadioUeRepCqiDistr_6, pmRadioUeRepCqiDistr_7, pmRadioUeRepCqiDistr_8, pmRadioUeRepCqiDistr_9, pmRadioUeRepCqiDistr_10, pmRadioUeRepCqiDistr_11, pmRadioUeRepCqiDistr_12, pmRadioUeRepCqiDistr_13, pmRadioUeRepCqiDistr_14, pmRadioUeRepCqiDistr_15, pmCaActivatedDlSum, pmCaCapableDlSum, pmCaConfiguredDlSum, pmCaScheduledDlSum, pmUeThp2DlDistr, pmUeThp2UlDistr, DC_pmRadioRecInterferencePwr, pmRadioRecInterferencePwr, pmRadioTxRankDistr_0, pmRadioTxRankDistr_1, pmRadioTxRankDistr_2, pmRadioTxRankDistr_3, pmRadioTxRankDistr_4, pmRadioTxRankDistr_5, pmRadioTxRankDistr_6, pmRadioTxRankDistr_7, pmRadioTxRankDistr_8, pmRadioTxRankDistr_9, pmRadioTxRankDistr_10, pmRadioTxRankDistr_11, pmRadioTxRankDistr_12, pmRadioTxRankDistr_13, pmUeCtxtRelSCWcdma, pmCriticalBorderEvalReport, pmMacHarqDlAckQpsk, pmMacHarqDlAck16qam, pmMacHarqDlAck64qam, pmRadioUeRepCqiDistr_D, pmRadioUeRepCqiDistr_N, pmUeCtxtRelCsfbWcdmaEm, pmUeCtxtRelCsfbWcdma, pmUeCtxtRelNormalEnb, pmCaCapableDlSum_0, pmCaCapableDlSum_1, pmCaCapableDlSum_2, pmCaConfiguredDlSum_0, pmCaConfiguredDlSum_1, pmCaConfiguredDlSum_2, pmRadioUeRepRankDistr_0, pmRadioUeRepRankDistr_1, pmRadioUeRepRankDistr_2, pmRadioUeRepRankDistr_3, pmSinrPuschDistr, pmSinrPucchDistr, N_pmSinrPuschDistr, N_pmSinrPucchDistr, pmHoExeSuccCsfb, pmHoExeAttCsfb, pmActiveUeDlSum, pmActiveUeUlSum, pmMacHarqDlAck256qam, pmUeCtxtRelCsfbGsm, pmUeCtxtEstabAttCsfb, pmUeCtxtModAttCsfb, pmRaAllocCfra, pmRaUnassignedCfraFalse, pmRaUnassignedCfraSum, pmRaFailCbraMsg2Disc, pmRaFailCfraMsg2Disc, pmRaFailCbraMsg1DiscOoc, pmRaFailCfraMsg1DiscOoc, pmRaFailCbraMsg1DiscSched, pmRaFailCfraMsg1DiscSched, pmRaBackoffDistr, pmRaContResOnly, pmRaContResAndRrcRsp, pmRaRrcRspDistr, DC2_pmRadioRecInterferencePwr, pmMacHarqDlDtx16qam, pmMacHarqDlDtx64qam, pmMacHarqDlDtxQpsk, pmMacHarqDlNack16qam, pmMacHarqDlNack64qam, pmMacHarqDlNackQpsk, pmRadioThpResUl, pmRadioThpVolUl, pmRlcArqDlAck, pmRlcArqDlNack, pmCellHoExeSuccLteIntraF, pmCellHoExeSuccLteInterF, pmCellHoExeAttLteIntraF, pmCellHoExeAttLteInterF, pmPrbUsedDlDtch, pmPrbUsedDlSrbFirstTrans, pmPdcpVolDlSrbTrans, pmPrbUsedDlPcch, pmPrbUsedDlBcch
            , '%s'eniqid, pmErabRelMmeActEutra,pmUeCtxtRelSCGsm
        from(
        select 
            a.date_id,a.erbs,a.eutrancellfdd
            
            ,sum(pmCount) as pmCount
            ,count( a.eutrancellfdd)numcell
            ,'228'eniqid
            
                    
            ,cast(sum(isnull(pmCellDowntimeAuto,0)) as float)pmCellDowntimeAuto
            ,cast(sum(isnull(pmCellDowntimeMan,0)) as float)pmCellDowntimeMan
            ,cast(sum(isnull(pmRrcConnEstabSucc,0)) as float)pmRrcConnEstabSucc
            ,cast(sum(isnull(pmRrcConnEstabAtt,0)) as float)pmRrcConnEstabAtt
            ,cast(sum(isnull(pmRrcConnEstabAttReAtt,0)) as float)pmRrcConnEstabAttReAtt
            ,cast(sum(isnull(pmS1SigConnEstabSucc,0)) as float)pmS1SigConnEstabSucc
            ,cast(sum(isnull(pmS1SigConnEstabAtt,0)) as float)pmS1SigConnEstabAtt
            ,cast(sum(isnull(pmErabEstabSuccInit,0)) as float)pmErabEstabSuccInit
            ,cast(sum(isnull(pmErabEstabAttInit,0)) as float)pmErabEstabAttInit
            ,cast(sum(isnull(pmErabRelAbnormalEnbAct,0)) as float)pmErabRelAbnormalEnbAct
            ,cast(sum(isnull(pmErabRelAbnormalMmeAct,0)) as float)pmErabRelAbnormalMmeAct
            ,cast(sum(isnull(pmErabRelAbnormalEnb,0)) as float)pmErabRelAbnormalEnb
            ,cast(sum(isnull(pmErabRelNormalEnb,0)) as float)pmErabRelNormalEnb
            ,cast(sum(isnull(pmErabRelMme,0)) as float)pmErabRelMme
            ,cast(sum(isnull(pmPdcpPktLostUl,0)) as float)pmPdcpPktLostUl
            ,cast(sum(isnull(pmPdcpPktReceivedUl,0)) as float)pmPdcpPktReceivedUl
            ,cast(sum(isnull(pmPdcpPktDiscDlPelr,0)) as float)pmPdcpPktDiscDlPelr
            ,cast(sum(isnull(pmPdcpPktDiscDlHo,0)) as float)pmPdcpPktDiscDlHo
            ,cast(sum(isnull(pmPdcpPktDiscDlPelrUu,0)) as float)pmPdcpPktDiscDlPelrUu
            ,cast(sum(isnull(pmPdcpPktTransDl,0)) as float)pmPdcpPktTransDl
            ,cast(sum(isnull(pmPdcpLatTimeDl,0)) as float)pmPdcpLatTimeDl
            ,cast(sum(isnull(pmPdcpLatPktTransDl,0)) as float)pmPdcpLatPktTransDl
            ,cast(sum(isnull(pmPrbUsedDlFirstTrans,0)) as float)pmPrbUsedDlFirstTrans
            ,cast(sum(isnull(pmPrbUsedDlReTrans,0)) as float)pmPrbUsedDlReTrans
            ,cast(sum(isnull(pmPrbAvailDl,0)) as float)pmPrbAvailDl
            ,cast(sum(isnull(pmRrcConnMax,0)) as float)pmRrcConnMax
            ,cast(sum(isnull(pmPrbUsedUlDtch,0)) as float)pmPrbUsedUlDtch
            ,cast(sum(isnull(pmPrbAvailUl,0)) as float)pmPrbAvailUl
            ,cast(sum(isnull(pmPdcpVolDlDrb,0)) as float)pmPdcpVolDlDrb
            ,cast(sum(isnull(pmPdcpVolDlDrbLastTTI,0)) as float)pmPdcpVolDlDrbLastTTI
            ,cast(sum(isnull(pmUeThpTimeDl,0)) as float)pmUeThpTimeDl
            ,cast(sum(isnull(pmUeThpVolUl,0)) as float)pmUeThpVolUl
            ,cast(sum(isnull(pmUeThpTimeUl,0)) as float)pmUeThpTimeUl
            ,cast(sum(isnull(pmRrcConnLevSum,0)) as float)pmRrcConnLevSum
            ,cast(sum(isnull(pmRrcConnLevSamp,0)) as float)pmRrcConnLevSamp
            ,cast(sum(isnull(pmSchedActivityCellDl,0)) as float)pmSchedActivityCellDl
            ,cast(sum(isnull(pmPdcpVolUlDrb,0)) as float)pmPdcpVolUlDrb
            ,cast(sum(isnull(pmSchedActivityCellUl,0)) as float)pmSchedActivityCellUl
            ,cast(sum(isnull(pmPdcpVolDlSrb,0)) as float)pmPdcpVolDlSrb
            ,cast(sum(isnull(pmPdcpVolULSrb,0)) as float)pmPdcpVolULSrb
            ,cast(sum(isnull(pmRaSuccCbra,0)) as float)pmRaSuccCbra
            ,cast(sum(isnull(pmRaAttCbra,0)) as float)pmRaAttCbra
            ,cast(sum(isnull(pmRaSuccCfra,0)) as float)pmRaSuccCfra
            ,cast(sum(isnull(pmRaAttCfra,0)) as float)pmRaAttCfra
            ,cast(sum(isnull(pmRrcConnEstabFailLic,0)) as float)pmRrcConnEstabFailLic
            ,cast(sum(isnull(pmCaCapableDlSamp,0)) as float)pmCaCapableDlSamp
            ,cast(sum(isnull(pmCaConfiguredDlSamp,0)) as float)pmCaConfiguredDlSamp
            ,cast(sum(isnull(pmPdcpVolDlDrbCa,0)) as float)pmPdcpVolDlDrbCa
            ,cast(sum(isnull(pmPdcpVolDlDrbLastTTICa,0)) as float)pmPdcpVolDlDrbLastTTICa
            ,cast(sum(isnull(pmRadioThpVolDl,0)) as float)pmRadioThpVolDl
            ,cast(sum(isnull(pmRadioThpVolDlSCell,0)) as float)pmRadioThpVolDlSCell
            ,cast(sum(isnull(pmRadioThpVolDlSCellExt,0)) as float)pmRadioThpVolDlSCellExt
            ,cast(sum(isnull(pmUeThpTimeDlCa,0)) as float)pmUeThpTimeDlCa
            ,cast(sum(isnull(pmUeThpTimeUlCa,0)) as float)pmUeThpTimeUlCa
            ,cast(sum(isnull(pmUeThpVolUlCa,0)) as float)pmUeThpVolUlCa
            ,cast(sum(isnull(pmLbSubRatioSamp,0)) as float)pmLbSubRatioSamp
            
            ,cast(sum(isnull(pmHoPrepSuccLteIntraF,0)) as float)pmHoPrepSuccLteIntraF
            ,cast(sum(isnull(pmHoPrepAttLteIntraF,0)) as float)pmHoPrepAttLteIntraF
            ,cast(sum(isnull(pmHoPrepSuccLteInterF,0)) as float)pmHoPrepSuccLteInterF
            ,cast(sum(isnull(pmHoPrepAttLteInterF,0)) as float)pmHoPrepAttLteInterF
            ,cast(sum(isnull(pmHoExeSuccLteIntraF,0)) as float)pmHoExeSuccLteIntraF
            ,cast(sum(isnull(pmHoExeAttLteIntraF,0)) as float)pmHoExeAttLteIntraF
            ,cast(sum(isnull(pmHoExeSuccLteInterF,0)) as float)pmHoExeSuccLteInterF
            ,cast(sum(isnull(pmHoExeAttLteInterF,0)) as float)pmHoExeAttLteInterF
            ,cast(sum(isnull(pmHoExeAttNonMob,0)) as float)pmHoExeAttNonMob
            ,cast(sum(isnull(pmHoExeSuccNonMob,0)) as float)pmHoExeSuccNonMob
            ,cast(sum(isnull(pmHoPrepAttNonMob,0)) as float)pmHoPrepAttNonMob
            ,cast(sum(isnull(pmHoPrepSuccNonMob,0)) as float)pmHoPrepSuccNonMob
            ,cast(sum(isnull(pmHoPrepAttLteInterFCaRedirect,0)) as float)pmHoPrepAttLteInterFCaRedirect
            ,cast(sum(isnull(pmHoPrepSuccLteInterFCaRedirect,0)) as float)pmHoPrepSuccLteInterFCaRedirect
            ,cast(sum(isnull(pmHoExecAttLteInterFCaRedirect,0)) as float)pmHoExecAttLteInterFCaRedirect
            ,cast(sum(isnull(pmHoExecSuccLteInterFCaRedirect,0)) as float)pmHoExecSuccLteInterFCaRedirect
            ,cast(sum(isnull(pmCaRedirectMeasRepUe,0)) as float)pmCaRedirectMeasRepUe
            ,cast(sum(isnull(pmCaRedirectQualifiedUe,0)) as float)pmCaRedirectQualifiedUe
            ,cast(sum(isnull(pmHoPrepAttLteInterFLb ,0)) as float)pmHoPrepAttLteInterFLb 
            ,cast(sum(isnull(pmHoPrepSuccLteInterFLb,0)) as float)pmHoPrepSuccLteInterFLb
            ,cast(sum(isnull(pmHoExeAttLteInterFLb  ,0)) as float)pmHoExeAttLteInterFLb  
            ,cast(sum(isnull(pmHoExeSuccLteInterFLb ,0)) as float)pmHoExeSuccLteInterFLb    
            ,cast(sum(isnull(pmLbQualifiedUe,0)) as float)pmLbQualifiedUe
            ,cast(sum(isnull(pmLbMeasRepUe,0)) as float)pmLbMeasRepUe
            
            ,cast(sum(isnull(pmHoPrepSucc,0)) as float)pmHoPrepSucc
            ,cast(sum(isnull(pmHoPrepAtt,0)) as float)pmHoPrepAtt
            ,cast(sum(isnull(pmHoExeSucc,0)) as float)pmHoExeSucc
            ,cast(sum(isnull(pmHoExeAtt,0)) as float)pmHoExeAtt

            ,cast(sum(isnull(pmRadioUeRepCqiDistr_0 ,0)) as float)pmRadioUeRepCqiDistr_0
            ,cast(sum(isnull(pmRadioUeRepCqiDistr_1 ,0)) as float)pmRadioUeRepCqiDistr_1
            ,cast(sum(isnull(pmRadioUeRepCqiDistr_2 ,0)) as float)pmRadioUeRepCqiDistr_2
            ,cast(sum(isnull(pmRadioUeRepCqiDistr_3 ,0)) as float)pmRadioUeRepCqiDistr_3
            ,cast(sum(isnull(pmRadioUeRepCqiDistr_4 ,0)) as float)pmRadioUeRepCqiDistr_4
            ,cast(sum(isnull(pmRadioUeRepCqiDistr_5 ,0)) as float)pmRadioUeRepCqiDistr_5
            ,cast(sum(isnull(pmRadioUeRepCqiDistr_6 ,0)) as float)pmRadioUeRepCqiDistr_6
            ,cast(sum(isnull(pmRadioUeRepCqiDistr_7 ,0)) as float)pmRadioUeRepCqiDistr_7
            ,cast(sum(isnull(pmRadioUeRepCqiDistr_8 ,0)) as float)pmRadioUeRepCqiDistr_8
            ,cast(sum(isnull(pmRadioUeRepCqiDistr_9 ,0)) as float)pmRadioUeRepCqiDistr_9
            ,cast(sum(isnull(pmRadioUeRepCqiDistr_10,0)) as float)pmRadioUeRepCqiDistr_10
            ,cast(sum(isnull(pmRadioUeRepCqiDistr_11,0)) as float)pmRadioUeRepCqiDistr_11
            ,cast(sum(isnull(pmRadioUeRepCqiDistr_12,0)) as float)pmRadioUeRepCqiDistr_12
            ,cast(sum(isnull(pmRadioUeRepCqiDistr_13,0)) as float)pmRadioUeRepCqiDistr_13
            ,cast(sum(isnull(pmRadioUeRepCqiDistr_14,0)) as float)pmRadioUeRepCqiDistr_14
            ,cast(sum(isnull(pmRadioUeRepCqiDistr_15,0)) as float)pmRadioUeRepCqiDistr_15
            ,cast(sum(isnull(pmCaActivatedDlSum,0)) as float)pmCaActivatedDlSum
            ,cast(sum(isnull(pmCaCapableDlSum_,0)) as float)pmCaCapableDlSum
            ,cast(sum(isnull(pmCaConfiguredDlSum_,0)) as float)pmCaConfiguredDlSum
            ,cast(sum(isnull(pmCaScheduledDlSum,0)) as float)pmCaScheduledDlSum
            ,cast(sum(isnull(pmUeThp2DlDistr,0)) as float)pmUeThp2DlDistr
            ,cast(sum(isnull(pmUeThp2UlDistr,0)) as float)pmUeThp2UlDistr
            ,cast(sum(isnull(DC_pmRadioRecInterferencePwr,0)) as float)DC_pmRadioRecInterferencePwr
            ,cast(sum(isnull(pmRadioRecInterferencePwr,0)) as float)pmRadioRecInterferencePwr
            
            
            ,cast(sum(isnull(pmRadioTxRankDistr_0,0)) as float)pmRadioTxRankDistr_0
            ,cast(sum(isnull(pmRadioTxRankDistr_1,0)) as float)pmRadioTxRankDistr_1
            ,cast(sum(isnull(pmRadioTxRankDistr_2,0)) as float)pmRadioTxRankDistr_2
            ,cast(sum(isnull(pmRadioTxRankDistr_3,0)) as float)pmRadioTxRankDistr_3
            ,cast(sum(isnull(pmRadioTxRankDistr_4,0)) as float)pmRadioTxRankDistr_4
            ,cast(sum(isnull(pmRadioTxRankDistr_5,0)) as float)pmRadioTxRankDistr_5
            ,cast(sum(isnull(pmRadioTxRankDistr_6,0)) as float)pmRadioTxRankDistr_6
            ,cast(sum(isnull(pmRadioTxRankDistr_7,0)) as float)pmRadioTxRankDistr_7
            ,cast(sum(isnull(pmRadioTxRankDistr_8,0)) as float)pmRadioTxRankDistr_8
            ,cast(sum(isnull(pmRadioTxRankDistr_9,0)) as float)pmRadioTxRankDistr_9
            ,cast(sum(isnull(pmRadioTxRankDistr_10,0)) as float)pmRadioTxRankDistr_10
            ,cast(sum(isnull(pmRadioTxRankDistr_11,0)) as float)pmRadioTxRankDistr_11
            ,cast(sum(isnull(pmRadioTxRankDistr_12,0)) as float)pmRadioTxRankDistr_12
            ,cast(sum(isnull(pmRadioTxRankDistr_13,0)) as float)pmRadioTxRankDistr_13
        --	,cast(sum(isnull(pmRadioTxRankDistr_14,0)) as float)pmRadioTxRankDistr_14
        --	,cast(sum(isnull(pmRadioTxRankDistr_15,0)) as float)pmRadioTxRankDistr_15
            
            ,cast(sum(isnull(pmUeCtxtRelSCWcdma,0)) as float)pmUeCtxtRelSCWcdma
            ,cast(sum(isnull(pmCriticalBorderEvalReport,0)) as float)pmCriticalBorderEvalReport
            
            ,cast(sum(isnull(pmMacHarqDlAckQpsk,0)) as float)pmMacHarqDlAckQpsk
            ,cast(sum(isnull(pmMacHarqDlAck16qam,0)) as float)pmMacHarqDlAck16qam
            ,cast(sum(isnull(pmMacHarqDlAck64qam,0)) as float)pmMacHarqDlAck64qam
            ,cast(sum(isnull(pmRadioUeRepCqiDistr_D,0)) as float)pmRadioUeRepCqiDistr_D
            ,cast(sum(isnull(pmRadioUeRepCqiDistr_N,0)) as float)pmRadioUeRepCqiDistr_N
            
            ,cast(sum(isnull(pmUeCtxtRelCsfbWcdmaEm,0)) as float)pmUeCtxtRelCsfbWcdmaEm
            ,cast(sum(isnull(pmUeCtxtRelCsfbWcdma,0)) as float)pmUeCtxtRelCsfbWcdma
            ,cast(sum(isnull(pmUeCtxtRelNormalEnb,0)) as float)pmUeCtxtRelNormalEnb
            
            ,cast(sum(isnull(pmUeCtxtModAttCsfb,0)) as float)pmUeCtxtModAttCsfb
            ,cast(sum(isnull(pmUeCtxtEstabAttCsfb,0)) as float)pmUeCtxtEstabAttCsfb
            
            
            
            ,cast(sum(isnull(pmCaCapableDlSum_0,0)) as float)pmCaCapableDlSum_0
            ,cast(sum(isnull(pmCaCapableDlSum_1,0)) as float)pmCaCapableDlSum_1
            ,cast(sum(isnull(pmCaCapableDlSum_2,0)) as float)pmCaCapableDlSum_2
            
            ,cast(sum(isnull(pmCaConfiguredDlSum_0,0)) as float)pmCaConfiguredDlSum_0
            ,cast(sum(isnull(pmCaConfiguredDlSum_1,0)) as float)pmCaConfiguredDlSum_1
            ,cast(sum(isnull(pmCaConfiguredDlSum_2,0)) as float)pmCaConfiguredDlSum_2
            
            ,cast(sum(isnull(pmRadioUeRepRankDistr_0,0)) as float)pmRadioUeRepRankDistr_0
            ,cast(sum(isnull(pmRadioUeRepRankDistr_1,0)) as float)pmRadioUeRepRankDistr_1
            ,cast(sum(isnull(pmRadioUeRepRankDistr_2,0)) as float)pmRadioUeRepRankDistr_2
            ,cast(sum(isnull(pmRadioUeRepRankDistr_3,0)) as float)pmRadioUeRepRankDistr_3
            
            
            ,cast(sum(isnull(pmSinrPuschDistr,0)) as float) pmSinrPuschDistr
            ,cast(sum(isnull(pmSinrPucchDistr,0)) as float) pmSinrPucchDistr
            ,cast(sum(isnull(N_pmSinrPuschDistr,0)) as float) N_pmSinrPuschDistr
            ,cast(sum(isnull(N_pmSinrPucchDistr,0)) as float) N_pmSinrPucchDistr
            
            
            
            
            ,cast(sum(isnull(pmHoExeSuccCsfb,0)) as float)pmHoExeSuccCsfb
            ,cast(sum(isnull(pmHoExeAttCsfb,0)) as float)pmHoExeAttCsfb
            
            ,cast(sum(isnull(pmActiveUeDlSum,0))as float)pmActiveUeDlSum
            ,cast(sum(isnull(pmActiveUeUlSum,0))as float)pmActiveUeUlSum
            ,cast(sum(isnull(pmMacHarqDlAck256qam,0))as float)pmMacHarqDlAck256qam

            ,cast(sum(isnull(pmUeCtxtRelCsfbGsm,0))as float)pmUeCtxtRelCsfbGsm

            ,cast(sum(isnull(pmRaAllocCfra,0))as float)pmRaAllocCfra
            ,cast(sum(isnull(pmRaUnassignedCfraFalse,0))as float)pmRaUnassignedCfraFalse
            ,cast(sum(isnull(pmRaUnassignedCfraSum,0))as float)pmRaUnassignedCfraSum
            ,cast(sum(isnull(pmRaFailCbraMsg2Disc,0))as float)pmRaFailCbraMsg2Disc
            ,cast(sum(isnull(pmRaFailCfraMsg2Disc,0))as float)pmRaFailCfraMsg2Disc
            ,cast(sum(isnull(pmRaFailCbraMsg1DiscOoc,0))as float)pmRaFailCbraMsg1DiscOoc
            ,cast(sum(isnull(pmRaFailCfraMsg1DiscOoc,0))as float)pmRaFailCfraMsg1DiscOoc
            ,cast(sum(isnull(pmRaFailCbraMsg1DiscSched,0))as float)pmRaFailCbraMsg1DiscSched
            ,cast(sum(isnull(pmRaFailCfraMsg1DiscSched,0))as float)pmRaFailCfraMsg1DiscSched
            ,cast(sum(isnull(pmRaBackoffDistr,0))as float)pmRaBackoffDistr
            ,cast(sum(isnull(pmRaContResOnly,0))as float)pmRaContResOnly
            ,cast(sum(isnull(pmRaContResAndRrcRsp,0))as float)pmRaContResAndRrcRsp
            ,cast(sum(isnull(pmRaRrcRspDistr,0))as float)pmRaRrcRspDistr

            ,cast(sum(isnull(DC2_pmRadioRecInterferencePwr,0)) as float) DC2_pmRadioRecInterferencePwr
            ,cast(sum(isnull(pmMacHarqDlDtx16qam,0)) as float) pmMacHarqDlDtx16qam
            ,cast(sum(isnull(pmMacHarqDlDtx64qam,0)) as float) pmMacHarqDlDtx64qam
            ,cast(sum(isnull(pmMacHarqDlDtxQpsk,0)) as float) pmMacHarqDlDtxQpsk
            ,cast(sum(isnull(pmMacHarqDlNack16qam,0)) as float) pmMacHarqDlNack16qam
            ,cast(sum(isnull(pmMacHarqDlNack64qam,0)) as float) pmMacHarqDlNack64qam
            ,cast(sum(isnull(pmMacHarqDlNackQpsk,0)) as float) pmMacHarqDlNackQpsk
            ,cast(sum(isnull(pmRadioThpResUl,0)) as float) pmRadioThpResUl
            ,cast(sum(isnull(pmRadioThpVolUl,0)) as float) pmRadioThpVolUl
            ,cast(sum(isnull(pmRlcArqDlAck,0)) as float) pmRlcArqDlAck
            ,cast(sum(isnull(pmRlcArqDlNack,0)) as float) pmRlcArqDlNack
            
            ,cast(sum(isnull(pmCellHoExeSuccLteIntraF,0)) as float)pmCellHoExeSuccLteIntraF
            ,cast(sum(isnull(pmCellHoExeSuccLteInterF,0)) as float)pmCellHoExeSuccLteInterF
            ,cast(sum(isnull(pmCellHoExeAttLteIntraF,0)) as float)pmCellHoExeAttLteIntraF
            ,cast(sum(isnull(pmCellHoExeAttLteInterF,0)) as float)pmCellHoExeAttLteInterF
            
            ,cast(sum(isnull(pmPrbUsedDlDtch,0)) as float)pmPrbUsedDlDtch
            ,cast(sum(isnull(pmPrbUsedDlSrbFirstTrans,0)) as float)pmPrbUsedDlSrbFirstTrans
            ,cast(sum(isnull(pmPdcpVolDlSrbTrans,0)) as float)pmPdcpVolDlSrbTrans
            ,cast(sum(isnull(pmPrbUsedDlPcch,0)) as float)pmPrbUsedDlPcch
            ,cast(sum(isnull(pmPrbUsedDlBcch,0)) as float)pmPrbUsedDlBcch
            ,cast(sum(isnull(pmErabRelMmeActEutra,0)) as float)pmErabRelMmeActEutra

            ,cast(sum(isnull(pmUeCtxtRelSCGsm,0)) as float)pmUeCtxtRelSCGsm
            
            
        from
        (
            select 
                Date_id,erbs,eutrancellfdd
                ,count(*) as pmCount
                ,count(distinct eutrancellfdd)numcell
                ,sum(isnull(pmCellDowntimeAuto,0))pmCellDowntimeAuto
                ,sum(isnull(pmCellDowntimeMan,0))pmCellDowntimeMan
                ,sum(isnull(pmRrcConnEstabSucc,0))pmRrcConnEstabSucc
                ,sum(isnull(pmRrcConnEstabAtt,0))pmRrcConnEstabAtt
                ,sum(isnull(pmRrcConnEstabAttReAtt,0))pmRrcConnEstabAttReAtt
                ,sum(isnull(pmS1SigConnEstabSucc,0))pmS1SigConnEstabSucc
                ,sum(isnull(pmS1SigConnEstabAtt,0))pmS1SigConnEstabAtt
                ,sum(isnull(pmErabEstabSuccInit,0))pmErabEstabSuccInit
                ,sum(isnull(pmErabEstabAttInit,0))pmErabEstabAttInit
                ,sum(isnull(pmErabRelAbnormalEnbAct,0))pmErabRelAbnormalEnbAct
                ,sum(isnull(pmErabRelAbnormalMmeAct,0))pmErabRelAbnormalMmeAct
                ,sum(isnull(pmErabRelAbnormalEnb,0))pmErabRelAbnormalEnb
                ,sum(isnull(pmErabRelNormalEnb,0))pmErabRelNormalEnb
                ,sum(isnull(pmErabRelMme,0))pmErabRelMme
                ,sum(isnull(pmPdcpPktLostUl,0))pmPdcpPktLostUl
                ,sum(isnull(pmPdcpPktReceivedUl,0))pmPdcpPktReceivedUl
                ,sum(isnull(pmPdcpPktDiscDlPelr,0))pmPdcpPktDiscDlPelr
                ,sum(isnull(pmPdcpPktDiscDlHo,0))pmPdcpPktDiscDlHo
                ,sum(isnull(pmPdcpPktDiscDlPelrUu,0))pmPdcpPktDiscDlPelrUu
                ,sum(isnull(pmPdcpPktTransDl,0))pmPdcpPktTransDl
                ,sum(isnull(pmPdcpLatTimeDl,0))pmPdcpLatTimeDl
                ,sum(isnull(pmPdcpLatPktTransDl,0))pmPdcpLatPktTransDl
                ,sum(isnull(pmPrbUsedDlFirstTrans,0))pmPrbUsedDlFirstTrans
                ,sum(isnull(pmPrbUsedDlReTrans,0))pmPrbUsedDlReTrans
                ,sum(isnull(pmPrbAvailDl,0))pmPrbAvailDl
                ,sum(isnull(pmRrcConnMax,0))pmRrcConnMax
                ,sum(isnull(pmPrbUsedUlDtch,0))pmPrbUsedUlDtch
                ,sum(isnull(pmPrbAvailUl,0))pmPrbAvailUl
                ,sum(isnull(pmPdcpVolDlDrb,0))pmPdcpVolDlDrb
                ,sum(isnull(pmPdcpVolDlDrbLastTTI,0))pmPdcpVolDlDrbLastTTI
                ,sum(isnull(pmUeThpTimeDl,0))pmUeThpTimeDl
                ,sum(isnull(pmUeThpVolUl,0))pmUeThpVolUl
                ,sum(isnull(pmUeThpTimeUl,0))pmUeThpTimeUl
                ,sum(isnull(pmRrcConnLevSum,0))pmRrcConnLevSum
                ,sum(isnull(pmRrcConnLevSamp,0))pmRrcConnLevSamp
                ,sum(isnull(pmSchedActivityCellDl,0))pmSchedActivityCellDl
                ,sum(isnull(pmPdcpVolUlDrb,0))pmPdcpVolUlDrb
                ,sum(isnull(pmSchedActivityCellUl,0))pmSchedActivityCellUl
                ,sum(isnull(pmPdcpVolDlSrb,0))pmPdcpVolDlSrb
                ,sum(isnull(pmPdcpVolULSrb,0))pmPdcpVolULSrb
                ,sum(isnull(pmRaSuccCbra,0))pmRaSuccCbra
                ,sum(isnull(pmRaAttCbra,0))pmRaAttCbra
                ,sum(isnull(pmRaSuccCfra,0))pmRaSuccCfra
                ,sum(isnull(pmRaAttCfra,0))pmRaAttCfra
                ,sum(isnull(pmRrcConnEstabFailLic,0))pmRrcConnEstabFailLic

                ,sum(isnull(pmCaCapableDlSamp,0))pmCaCapableDlSamp
                ,sum(isnull(pmCaConfiguredDlSamp,0))pmCaConfiguredDlSamp
                ,sum(isnull(pmPdcpVolDlDrbCa,0))pmPdcpVolDlDrbCa
                ,sum(isnull(pmPdcpVolDlDrbLastTTICa,0))pmPdcpVolDlDrbLastTTICa
                ,sum(isnull(pmRadioThpVolDl,0))pmRadioThpVolDl
                ,sum(isnull(pmRadioThpVolDlSCell,0))pmRadioThpVolDlSCell
                ,sum(isnull(pmRadioThpVolDlSCellExt,0))pmRadioThpVolDlSCellExt
                ,sum(isnull(pmUeThpTimeDlCa,0))pmUeThpTimeDlCa
                ,sum(isnull(pmUeThpTimeUlCa,0))pmUeThpTimeUlCa
                ,sum(isnull(pmUeThpVolUlCa,0))pmUeThpVolUlCa
                ,sum(isnull(pmLbSubRatioSamp,0))pmLbSubRatioSamp
                
                ,sum(isnull(pmUeCtxtRelSCWcdma,0))pmUeCtxtRelSCWcdma
                ,sum(isnull(pmCriticalBorderEvalReport,0))pmCriticalBorderEvalReport
                
                ,sum(isnull(pmMacHarqDlAckQpsk,0))pmMacHarqDlAckQpsk
                ,sum(isnull(pmMacHarqDlAck16qam,0))pmMacHarqDlAck16qam
                ,sum(isnull(pmMacHarqDlAck64qam,0))pmMacHarqDlAck64qam
                
                ,sum(isnull(pmUeCtxtRelCsfbWcdmaEm,0))pmUeCtxtRelCsfbWcdmaEm
                ,sum(isnull(pmUeCtxtRelCsfbWcdma,0))pmUeCtxtRelCsfbWcdma
                --,sum(isnull(pmRrcConnEstabSucc,0))pmRrcConnEstabSucc
                ,sum(isnull(pmUeCtxtRelNormalEnb,0))pmUeCtxtRelNormalEnb

                ,sum(isnull(pmUeCtxtModAttCsfb,0)) pmUeCtxtModAttCsfb
                ,sum(isnull(pmUeCtxtEstabAttCsfb,0)) pmUeCtxtEstabAttCsfb

                ,sum(isnull(pmActiveUeDlSum,0))pmActiveUeDlSum
                ,sum(isnull(pmActiveUeUlSum,0))pmActiveUeUlSum
                ,sum(isnull(pmMacHarqDlAck256qam,0))pmMacHarqDlAck256qam

                ,sum(isnull(pmUeCtxtRelCsfbGsm,0))pmUeCtxtRelCsfbGsm

                ,sum(isnull(pmRaAllocCfra,0))pmRaAllocCfra
                ,sum(isnull(pmRaUnassignedCfraFalse,0))pmRaUnassignedCfraFalse
                ,sum(isnull(pmRaUnassignedCfraSum,0))pmRaUnassignedCfraSum
                ,sum(isnull(pmRaFailCbraMsg2Disc,0))pmRaFailCbraMsg2Disc
                ,sum(isnull(pmRaFailCfraMsg2Disc,0))pmRaFailCfraMsg2Disc
                ,sum(isnull(pmRaFailCbraMsg1DiscOoc,0))pmRaFailCbraMsg1DiscOoc
                ,sum(isnull(pmRaFailCfraMsg1DiscOoc,0))pmRaFailCfraMsg1DiscOoc
                ,sum(isnull(pmRaFailCbraMsg1DiscSched,0))pmRaFailCbraMsg1DiscSched
                ,sum(isnull(pmRaFailCfraMsg1DiscSched,0))pmRaFailCfraMsg1DiscSched
                --,sum(isnull(pmRaBackoffDistr,0))pmRaBackoffDistr
                ,sum(isnull(pmRaContResOnly,0))pmRaContResOnly
                ,sum(isnull(pmRaContResAndRrcRsp,0))pmRaContResAndRrcRsp
                --,sum(isnull(pmRaRrcRspDistr,0))pmRaRrcRspDistr
                
                ,cast(sum(isnull(pmCellHoExeSuccLteIntraF,0)) as float)pmCellHoExeSuccLteIntraF
                ,cast(sum(isnull(pmCellHoExeSuccLteInterF,0)) as float)pmCellHoExeSuccLteInterF
                ,cast(sum(isnull(pmCellHoExeAttLteIntraF,0)) as float)pmCellHoExeAttLteIntraF
                ,cast(sum(isnull(pmCellHoExeAttLteInterF,0)) as float)pmCellHoExeAttLteInterF
                ,cast(sum(isnull(pmMacHarqDlDtx16qam,0)) as float) pmMacHarqDlDtx16qam
                ,cast(sum(isnull(pmMacHarqDlDtx64qam,0)) as float) pmMacHarqDlDtx64qam
                ,cast(sum(isnull(pmMacHarqDlDtxQpsk,0)) as float) pmMacHarqDlDtxQpsk
                ,cast(sum(isnull(pmMacHarqDlNack16qam,0)) as float) pmMacHarqDlNack16qam
                ,cast(sum(isnull(pmMacHarqDlNack64qam,0)) as float) pmMacHarqDlNack64qam
                ,cast(sum(isnull(pmMacHarqDlNackQpsk,0)) as float) pmMacHarqDlNackQpsk
                ,cast(sum(isnull(pmRadioThpResUl,0)) as float) pmRadioThpResUl
                ,cast(sum(isnull(pmRadioThpVolUl,0)) as float) pmRadioThpVolUl
                ,cast(sum(isnull(pmRlcArqDlAck,0)) as float) pmRlcArqDlAck
                ,cast(sum(isnull(pmRlcArqDlNack,0)) as float) pmRlcArqDlNack
                
                ,cast(sum(isnull(pmPrbUsedDlDtch,0)) as float)pmPrbUsedDlDtch
                ,cast(sum(isnull(pmPrbUsedDlSrbFirstTrans,0)) as float)pmPrbUsedDlSrbFirstTrans
                ,cast(sum(isnull(pmPdcpVolDlSrbTrans,0)) as float)pmPdcpVolDlSrbTrans
                ,cast(sum(isnull(pmPrbUsedDlPcch,0)) as float)pmPrbUsedDlPcch
                ,cast(sum(isnull(pmPrbUsedDlBcch,0)) as float)pmPrbUsedDlBcch

                ,cast(sum(isnull(pmErabRelMmeActEutra,0)) as float)pmErabRelMmeActEutra	
                ,cast(sum(isnull(pmUeCtxtRelSCGsm,0)) as float)pmUeCtxtRelSCGsm
                
            from dcpublic.dc_e_ERBS_EUTRANCELLFDD_RAW
            where (DATETIME_ID BETWEEN '%s 00:00:00' AND  '%s 23:59:59') 
            group by 
                Date_id,erbs,eutrancellfdd
        )a left join 
        (
            select 
                Date_id,erbs,eutrancellfdd
                ,sum(isnull(pmHoPrepSuccLteIntraF,0))pmHoPrepSuccLteIntraF
                ,sum(isnull(pmHoPrepAttLteIntraF,0))pmHoPrepAttLteIntraF
                ,sum(isnull(pmHoPrepSuccLteInterF,0))pmHoPrepSuccLteInterF
                ,sum(isnull(pmHoPrepAttLteInterF,0))pmHoPrepAttLteInterF
                ,sum(isnull(pmHoExeSuccLteIntraF,0))pmHoExeSuccLteIntraF
                ,sum(isnull(pmHoExeAttLteIntraF,0))pmHoExeAttLteIntraF
                ,sum(isnull(pmHoExeSuccLteInterF,0))pmHoExeSuccLteInterF
                ,sum(isnull(pmHoExeAttLteInterF,0))pmHoExeAttLteInterF


                ,sum(isnull(pmHoExeAttNonMob,0))pmHoExeAttNonMob
                ,sum(isnull(pmHoExeSuccNonMob,0))pmHoExeSuccNonMob
                ,sum(isnull(pmHoPrepAttNonMob,0))pmHoPrepAttNonMob
                ,sum(isnull(pmHoPrepSuccNonMob,0))pmHoPrepSuccNonMob
                ,sum(isnull(pmHoPrepAttLteInterFCaRedirect,0))pmHoPrepAttLteInterFCaRedirect
                ,sum(isnull(pmHoPrepSuccLteInterFCaRedirect,0))pmHoPrepSuccLteInterFCaRedirect
                ,sum(isnull(pmHoExecAttLteInterFCaRedirect,0))pmHoExecAttLteInterFCaRedirect
                ,sum(isnull(pmHoExecSuccLteInterFCaRedirect,0))pmHoExecSuccLteInterFCaRedirect
                ,sum(isnull(pmCaRedirectMeasRepUe,0))pmCaRedirectMeasRepUe
                ,sum(isnull(pmCaRedirectQualifiedUe,0))pmCaRedirectQualifiedUe



                ,sum(isnull(pmHoPrepAttLteInterFLb ,0))pmHoPrepAttLteInterFLb 
                ,sum(isnull(pmHoPrepSuccLteInterFLb,0))pmHoPrepSuccLteInterFLb
                ,sum(isnull(pmHoExeAttLteInterFLb  ,0))pmHoExeAttLteInterFLb  
                ,sum(isnull(pmHoExeSuccLteInterFLb ,0))pmHoExeSuccLteInterFLb    
                ,sum(isnull(pmLbQualifiedUe,0))pmLbQualifiedUe
                ,sum(isnull(pmLbMeasRepUe,0))pmLbMeasRepUe
            from dcpublic.dc_e_ERBS_EUTRANCELLRELATION_RAW
            where (DATETIME_ID BETWEEN '%s 00:00:00' AND  '%s 23:59:59') 
            group by 
                Date_id,erbs,eutrancellfdd
        )b on a.date_id=b.date_id and a.erbs=b.erbs and a.eutrancellfdd=b.eutrancellfdd
        left join 
        (
            select 
                Date_id,erbs,eutrancellfdd
                ,sum(isnull(pmHoPrepSucc,0))pmHoPrepSucc
                ,sum(isnull(pmHoPrepAtt,0))pmHoPrepAtt
                ,sum(isnull(pmHoExeSucc,0))pmHoExeSucc
                ,sum(isnull(pmHoExeAtt,0))pmHoExeAtt
                
                ,sum(isnull(pmHoExeSuccCsfb,0))pmHoExeSuccCsfb
                ,sum(isnull(pmHoExeAttCsfb,0))pmHoExeAttCsfb
            from dcpublic.dc_e_ERBS_UTRANCELLRELATION_raw
            where (DATETIME_ID BETWEEN '%s 00:00:00' AND  '%s 23:59:59') 
            group by 
                Date_id,erbs,eutrancellfdd
        )c on a.date_id=c.date_id and a.erbs=c.erbs and a.eutrancellfdd=c.eutrancellfdd

        left join 

        (
            select 
            [date_id], [ERBS], [EUtranCellFdd]
            ,sum(isnull(DC_pmRadioRecInterferencePwr,0))DC_pmRadioRecInterferencePwr
            ,sum(isnull(DC2_pmRadioRecInterferencePwr,0))DC2_pmRadioRecInterferencePwr
            ,sum(isnull(pmRadioRecInterferencePwr_,0)) as pmRadioRecInterferencePwr
            
            ,sum(isnull(pmSinrPuschDistr,0)) as pmSinrPuschDistr
            ,sum(isnull(pmSinrPucchDistr,0)) as pmSinrPucchDistr
            ,sum(isnull(N_pmSinrPuschDistr,0)) as N_pmSinrPuschDistr
            ,sum(isnull(N_pmSinrPucchDistr,0)) as N_pmSinrPucchDistr
            
            ,sum(case when DCVECTOR_INDEX = 0 then pmRadioUeRepRankDistr end)pmRadioUeRepRankDistr_0
            ,sum(case when DCVECTOR_INDEX = 1 then pmRadioUeRepRankDistr end)pmRadioUeRepRankDistr_1
            ,sum(case when DCVECTOR_INDEX = 2 then pmRadioUeRepRankDistr end)pmRadioUeRepRankDistr_2
            ,sum(case when DCVECTOR_INDEX = 3 then pmRadioUeRepRankDistr end)pmRadioUeRepRankDistr_3
            
            ,sum(case when DCVECTOR_INDEX = 0 then pmRadioTxRankDistr end)pmRadioTxRankDistr_0
            ,sum(case when DCVECTOR_INDEX = 1 then pmRadioTxRankDistr end)pmRadioTxRankDistr_1
            ,sum(case when DCVECTOR_INDEX = 2 then pmRadioTxRankDistr end)pmRadioTxRankDistr_2
            ,sum(case when DCVECTOR_INDEX = 3 then pmRadioTxRankDistr end)pmRadioTxRankDistr_3
            ,sum(case when DCVECTOR_INDEX = 4 then pmRadioTxRankDistr end)pmRadioTxRankDistr_4
            ,sum(case when DCVECTOR_INDEX = 5 then pmRadioTxRankDistr end)pmRadioTxRankDistr_5
            ,sum(case when DCVECTOR_INDEX = 6 then pmRadioTxRankDistr end)pmRadioTxRankDistr_6
            ,sum(case when DCVECTOR_INDEX = 7 then pmRadioTxRankDistr end)pmRadioTxRankDistr_7
            ,sum(case when DCVECTOR_INDEX = 8 then pmRadioTxRankDistr end)pmRadioTxRankDistr_8
            ,sum(case when DCVECTOR_INDEX = 9 then pmRadioTxRankDistr end)pmRadioTxRankDistr_9
            ,sum(case when DCVECTOR_INDEX = 10 then pmRadioTxRankDistr end)pmRadioTxRankDistr_10
            ,sum(case when DCVECTOR_INDEX = 11 then pmRadioTxRankDistr end)pmRadioTxRankDistr_11
            ,sum(case when DCVECTOR_INDEX = 12 then pmRadioTxRankDistr end)pmRadioTxRankDistr_12
            ,sum(case when DCVECTOR_INDEX = 13 then pmRadioTxRankDistr end)pmRadioTxRankDistr_13
            
            ,sum(case when DCVECTOR_INDEX = 0 then pmRadioUeRepCqiDistr end )pmRadioUeRepCqiDistr_0
            ,sum(case when DCVECTOR_INDEX = 1 then pmRadioUeRepCqiDistr end )pmRadioUeRepCqiDistr_1
            ,sum(case when DCVECTOR_INDEX = 2 then pmRadioUeRepCqiDistr end )pmRadioUeRepCqiDistr_2
            ,sum(case when DCVECTOR_INDEX = 3 then pmRadioUeRepCqiDistr end )pmRadioUeRepCqiDistr_3
            ,sum(case when DCVECTOR_INDEX = 4 then pmRadioUeRepCqiDistr end )pmRadioUeRepCqiDistr_4
            ,sum(case when DCVECTOR_INDEX = 5 then pmRadioUeRepCqiDistr end )pmRadioUeRepCqiDistr_5
            ,sum(case when DCVECTOR_INDEX = 6 then pmRadioUeRepCqiDistr end )pmRadioUeRepCqiDistr_6
            ,sum(case when DCVECTOR_INDEX = 7 then pmRadioUeRepCqiDistr end )pmRadioUeRepCqiDistr_7
            ,sum(case when DCVECTOR_INDEX = 8 then pmRadioUeRepCqiDistr end )pmRadioUeRepCqiDistr_8
            ,sum(case when DCVECTOR_INDEX = 9 then pmRadioUeRepCqiDistr end )pmRadioUeRepCqiDistr_9
            ,sum(case when DCVECTOR_INDEX = 10 then pmRadioUeRepCqiDistr end)pmRadioUeRepCqiDistr_10
            ,sum(case when DCVECTOR_INDEX = 11 then pmRadioUeRepCqiDistr end)pmRadioUeRepCqiDistr_11
            ,sum(case when DCVECTOR_INDEX = 12 then pmRadioUeRepCqiDistr end)pmRadioUeRepCqiDistr_12
            ,sum(case when DCVECTOR_INDEX = 13 then pmRadioUeRepCqiDistr end)pmRadioUeRepCqiDistr_13
            ,sum(case when DCVECTOR_INDEX = 14 then pmRadioUeRepCqiDistr end)pmRadioUeRepCqiDistr_14
            ,sum(case when DCVECTOR_INDEX = 15 then pmRadioUeRepCqiDistr end)pmRadioUeRepCqiDistr_15

            ,sum(isnull(pmCaActivatedDlSum,0))pmCaActivatedDlSum
            ,sum(isnull(pmCaCapableDlSum,0))pmCaCapableDlSum_
            ,sum(isnull(pmCaConfiguredDlSum,0))pmCaConfiguredDlSum_
            
            ,sum(case when DCVECTOR_INDEX = 0 then pmCaCapableDlSum end )pmCaCapableDlSum_0
            ,sum(case when DCVECTOR_INDEX = 1 then pmCaCapableDlSum end )pmCaCapableDlSum_1
            ,sum(case when DCVECTOR_INDEX = 2 then pmCaCapableDlSum end )pmCaCapableDlSum_2
            ,sum(case when DCVECTOR_INDEX = 0 then pmCaConfiguredDlSum end )pmCaConfiguredDlSum_0
            ,sum(case when DCVECTOR_INDEX = 1 then pmCaConfiguredDlSum end )pmCaConfiguredDlSum_1
            ,sum(case when DCVECTOR_INDEX = 2 then pmCaConfiguredDlSum end )pmCaConfiguredDlSum_2
            
            
            ,sum(isnull(pmCaScheduledDlSum,0))pmCaScheduledDlSum
            ,sum(isnull(pmUeThp2DlDistr,0))pmUeThp2DlDistr
            ,sum(isnull(pmUeThp2UlDistr,0))pmUeThp2UlDistr
            ,sum(isnull(pmRadioUeRepCqiDistr_D,0))pmRadioUeRepCqiDistr_D
            ,sum(isnull(pmRadioUeRepCqiDistr_N,0))pmRadioUeRepCqiDistr_N
            
            ,sum(isnull(pmRaBackoffDistr,0))pmRaBackoffDistr
            ,sum(isnull(pmRaRrcRspDistr,0))pmRaRrcRspDistr
        from 
        (
            select 
                date_id,[ERBS], [EUtranCellFdd], [DCVECTOR_INDEX]
                ,case 
                    when [DCVECTOR_INDEX] = 0 then -122
                    when [DCVECTOR_INDEX] = 1 then -120.5
                    when [DCVECTOR_INDEX] = 2 then -119.5
                    when [DCVECTOR_INDEX] = 3 then -118.5
                    when [DCVECTOR_INDEX] = 4 then -117.5
                    when [DCVECTOR_INDEX] = 5 then -116.5
                    when [DCVECTOR_INDEX] = 6 then -115.5
                    when [DCVECTOR_INDEX] = 7 then -114.5
                    when [DCVECTOR_INDEX] = 8 then -113.5
                    when [DCVECTOR_INDEX] = 9 then -112.5
                    when [DCVECTOR_INDEX] = 10 then -110
                    when [DCVECTOR_INDEX] = 11 then -106
                    when [DCVECTOR_INDEX] = 12 then -102
                    when [DCVECTOR_INDEX] = 13 then -98
                    when [DCVECTOR_INDEX] = 14 then -94
                    when [DCVECTOR_INDEX] = 15 then -90
                    else null end * sum(pmRadioRecInterferencePwr) as DC_pmRadioRecInterferencePwr
                ,case
                    when [DCVECTOR_INDEX] = 0 then 0.79432
                    when [DCVECTOR_INDEX] = 1 then 0.89716
                    when [DCVECTOR_INDEX] = 2 then 1.12946
                    when [DCVECTOR_INDEX] = 3 then 1.42191
                    when [DCVECTOR_INDEX] = 4 then 1.79008
                    when [DCVECTOR_INDEX] = 5 then 2.25357
                    when [DCVECTOR_INDEX] = 6 then 2.83708
                    when [DCVECTOR_INDEX] = 7 then 3.57167
                    when [DCVECTOR_INDEX] = 8 then 4.49647
                    when [DCVECTOR_INDEX] = 9 then 5.66072
                    when [DCVECTOR_INDEX] = 10 then 11.07925
                    when [DCVECTOR_INDEX] = 11 then 27.82982
                    when [DCVECTOR_INDEX] = 12 then 69.90536
                    when [DCVECTOR_INDEX] = 13 then 175.59432
                    when [DCVECTOR_INDEX] = 14 then 441.07
                    when [DCVECTOR_INDEX] = 15 then 630.95
                else null end * sum(pmRadioRecInterferencePwr) as DC2_pmRadioRecInterferencePwr	
                    
                    
                ,case
                    when DCVECTOR_INDEX =0 then (pmSinrPuschDistr * -5   )
                    when DCVECTOR_INDEX =1 then (pmSinrPuschDistr * -3.25)
                    when DCVECTOR_INDEX =2 then (pmSinrPuschDistr * 0.45 )
                    when DCVECTOR_INDEX =3 then (pmSinrPuschDistr * 4.45 )
                    when DCVECTOR_INDEX =4 then (pmSinrPuschDistr * 8.45 )
                    when DCVECTOR_INDEX =5 then (pmSinrPuschDistr * 12.45)
                    when DCVECTOR_INDEX =6 then (pmSinrPuschDistr * 15.75)
                    when DCVECTOR_INDEX =7 then (pmSinrPuschDistr * 18.75)
                    when DCVECTOR_INDEX =8 then (pmSinrPuschDistr * 20   )
                    else 0 end as N_pmSinrPuschDistr
                ,case
                    when DCVECTOR_INDEX =0 then (pmSinrPucchDistr * -15   )
                    when DCVECTOR_INDEX =1 then (pmSinrPucchDistr * -13.25)
                    when DCVECTOR_INDEX =2 then (pmSinrPucchDistr * -10.25)
                    when DCVECTOR_INDEX =3 then (pmSinrPucchDistr * -7.25 )
                    when DCVECTOR_INDEX =4 then (pmSinrPucchDistr * -4.25 )
                    when DCVECTOR_INDEX =5 then (pmSinrPucchDistr * -1.25 )
                    when DCVECTOR_INDEX =6 then (pmSinrPucchDistr * 1.75  )
                    when DCVECTOR_INDEX =7 then (pmSinrPucchDistr * 3     )
                    else 0 end as N_pmSinrPucchDistr
                    
                ,sum(isnull(pmRadioRecInterferencePwr,0)) as pmRadioRecInterferencePwr_
                ,sum(isnull(pmRadioTxRankDistr,0))pmRadioTxRankDistr
                ,sum(isnull(pmRadioUeRepCqiDistr,0))pmRadioUeRepCqiDistr
                ,sum(isnull(pmRadioUeRepRankDistr,0))pmRadioUeRepRankDistr
                ,sum(isnull(pmCaActivatedDlSum,0))pmCaActivatedDlSum
                ,sum(isnull(pmCaCapableDlSum,0))pmCaCapableDlSum
                ,sum(isnull(pmCaConfiguredDlSum,0))pmCaConfiguredDlSum
                ,sum(isnull(pmCaScheduledDlSum,0))pmCaScheduledDlSum
                ,sum(isnull(pmUeThp2DlDistr,0))pmUeThp2DlDistr
                ,sum(isnull(pmUeThp2UlDistr,0))pmUeThp2UlDistr
                
                ,sum(isnull(pmSinrPuschDistr,0))pmSinrPuschDistr
                ,sum(isnull(pmSinrPucchDistr,0))pmSinrPucchDistr
                ,sum(isnull(pmRaBackoffDistr,0))pmRaBackoffDistr
                ,sum(isnull(pmRaRrcRspDistr,0))pmRaRrcRspDistr

                ,pmRadioUeRepCqiDistr as pmRadioUeRepCqiDistr_D

                ,case
                    when DCVECTOR_INDEX = 0 then  (pmRadioUeRepCqiDistr *0)
                    when DCVECTOR_INDEX = 1 then  (pmRadioUeRepCqiDistr *1)
                    when DCVECTOR_INDEX = 2 then  (pmRadioUeRepCqiDistr *2)
                    when DCVECTOR_INDEX = 3 then  (pmRadioUeRepCqiDistr *3)
                    when DCVECTOR_INDEX = 4 then  (pmRadioUeRepCqiDistr *4)
                    when DCVECTOR_INDEX = 5 then  (pmRadioUeRepCqiDistr *5)
                    when DCVECTOR_INDEX = 6 then  (pmRadioUeRepCqiDistr *6)
                    when DCVECTOR_INDEX = 7 then  (pmRadioUeRepCqiDistr *7)
                    when DCVECTOR_INDEX = 8 then  (pmRadioUeRepCqiDistr *8)
                    when DCVECTOR_INDEX = 9 then  (pmRadioUeRepCqiDistr *9)
                    when DCVECTOR_INDEX = 10 then (pmRadioUeRepCqiDistr *10)
                    when DCVECTOR_INDEX = 11 then (pmRadioUeRepCqiDistr *11)
                    when DCVECTOR_INDEX = 12 then (pmRadioUeRepCqiDistr *12)
                    when DCVECTOR_INDEX = 13 then (pmRadioUeRepCqiDistr *13)
                    when DCVECTOR_INDEX = 14 then (pmRadioUeRepCqiDistr *14)
                    when DCVECTOR_INDEX = 15 then (pmRadioUeRepCqiDistr *15)
                    when DCVECTOR_INDEX = 16 then (pmRadioUeRepCqiDistr *16)
                    when DCVECTOR_INDEX = 17 then (pmRadioUeRepCqiDistr *17)
                    when DCVECTOR_INDEX = 18 then (pmRadioUeRepCqiDistr *18)
                    when DCVECTOR_INDEX = 19 then (pmRadioUeRepCqiDistr *19)
                    when DCVECTOR_INDEX = 20 then (pmRadioUeRepCqiDistr *20)
                    when DCVECTOR_INDEX = 21 then (pmRadioUeRepCqiDistr *21)
                    when DCVECTOR_INDEX = 22 then (pmRadioUeRepCqiDistr *22)
                    when DCVECTOR_INDEX = 23 then (pmRadioUeRepCqiDistr *23)
                    when DCVECTOR_INDEX = 24 then (pmRadioUeRepCqiDistr *24)
                    when DCVECTOR_INDEX = 25 then (pmRadioUeRepCqiDistr *25)
                    when DCVECTOR_INDEX = 26 then (pmRadioUeRepCqiDistr *26)
                    when DCVECTOR_INDEX = 27 then (pmRadioUeRepCqiDistr *27)
                    when DCVECTOR_INDEX = 28 then (pmRadioUeRepCqiDistr *28)
                    when DCVECTOR_INDEX = 29 then (pmRadioUeRepCqiDistr *29)
                    when DCVECTOR_INDEX = 30 then (pmRadioUeRepCqiDistr *30)
                end pmRadioUeRepCqiDistr_N
            from 
                dcpublic.dc_e_ERBS_EUTRANCELLFDD_V_raw
            where (DATETIME_ID BETWEEN '%s 00:00:00' AND  '%s 23:59:59') 
                and [DCVECTOR_INDEX] <= 30
            group by date_id,[ERBS], [EUtranCellFdd], [DCVECTOR_INDEX]
        ) g      
        group by 
            [date_id], [ERBS], [EUtranCellFdd]
        )f on a.date_id=f.date_id and a.erbs=f.erbs and a.eutrancellfdd=f.eutrancellfdd

        group by 
            a.date_id,a.erbs,a.eutrancellfdd
        )xx	
            ;


                        """ %(enid, q,q, q,q,q,q,q,q)
    
    return query


def get_data(dict_id):
    start_time = datetime.now()

    s_date = dict_id['date']
    s_date2 = dict_id['date2']
    s_port = dict_id['eniq']['enid']
    dict_conn = dict_id['eniq']
    
    path_daily = '/var/opt/common5/eniq/%s'%(s_port)
    if not os.path.exists(path_daily):
        try:
            os.mkdir(path_daily)
        except:
            pass

    
    conn = False
    sql_query = get_sql(s_date2, s_port)

    try:
        conn = pyodbc.connect('DRIVER=freetds;SERVER=%s;PORT=%s;UID=%s;PWD=%s;TDS _Version=5.0;' %(dict_conn['host'], dict_conn['port'], dict_conn['user'], dict_conn['passwd']))

        sql_query = pd.read_sql_query(sql_query,conn) # here, the 'conn' is the variable that contains your database connection information from step 2
        df = pd.DataFrame(sql_query)
        #print('   Query success........')
        df.to_csv("%s/daily_cell_%s.csv" %(path_daily, s_date2), index=None)
        
        duration = datetime.now() - start_time
        print("    %s Processing done with duration: %s" %(s_port, duration))
        conn.close()
    except Exception as e:
        print("Failed connection %s : %s" % (s_port, e))
        if conn:
            conn.close()
            print("Connection closed for %s" %s_port)
        pass


def get_daily_data(list_data):
    print("Collecting data %s on %s" %(s_date, datetime.now())) 

    num_worker = 2
    start_time_paralel = datetime.now()
    with multiprocessing.Pool(num_worker) as pool:
        pool.map(get_data, list_data)
    
    duration = datetime.now() - start_time_paralel
    print("Total duration time: %s" %(duration))


def combineData(list_data):
    list_df = []
    s_date2 = list_data[0]['date2']
    for dict_id in list_data:
        s_date2 = dict_id['date2']
        s_port = dict_id['eniq']['enid']
        
        file = '/var/opt/common5/eniq/%s/daily_cell_%s.csv'%(s_port, s_date2)
        if os.path.exists(file):
            df_new = pd.read_csv(file, delimiter=",", index_col=None, header='infer')
            if len(df_new) > 0:
                list_df.append(df_new)
    if len(list_df) > 0:
        df = pd.concat(list_df, sort=False)
        path_daily = '/var/opt/common5/eniq'
        file = '%s/daily_cell_%s.csv'%(path_daily, s_date2)
        df.to_csv(file, index=None)

        #zip result
        zip_file = "%s/daily_cell_%s.zip" %(path_daily, s_date2)
        zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
        zf.write(file,'daily_cell_%s.csv'%s_date2)
        zf.close()
        os.remove(file)
        print('Result on %s'%zip_file)

def zipCellDaily(s_date2):
    print()
    path_daily = '/var/opt/common5/eniq'
    filename = 'daily_cell_%s.csv'%(s_date2)
    zip_file = "%s/daily_cell_%s.zip" %(path_daily, s_date2)

    

    with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as zipf:
        zipf.write(os.path.join(path_daily,filename), arcname=filename)

    #os.remove(os.path.join(path_daily,filename))
    print('Result on %s'%zip_file)



if __name__ == '__main__':
    warnings.filterwarnings('ignore')
    f = open('credential.json')
    access = json.load(f)
    num_worker = 4
    tanggal = datetime.now()
    delta_day = 1
    port = '8503'
    
    start_datetime = tanggal - timedelta(days=delta_day)
    stop_datetime = start_datetime

    if len(sys.argv) > 1:
        start_date = sys.argv[1]
        if len(sys.argv) > 2:
            stop_date = sys.argv[2]
        else:
            stop_date = start_date

        start_datetime = datetime.strptime(start_date, '%Y%m%d')
        stop_datetime = datetime.strptime(stop_date, '%Y%m%d')
    
    list_eniq = []
    list_eniq.append(access['eniq']['WRAN2'])
    list_eniq.append(access['eniq']['ENIQ5'])
    
    for dt in rrule.rrule(rrule.DAILY, dtstart=start_datetime, until=stop_datetime):
        sekarang = datetime.now()
        menit_sekarang = sekarang.minute
        s_date = dt.strftime("%Y-%m-%d")
        s_date2 = dt.strftime("%Y%m%d")
        print("Collecting data %s " %(s_date))

        list_data = []
        for eniq in list_eniq:
            folder_data = '/var/opt/common5/eniq/%s/%s'%(eniq['enid'], s_date2)
            if not os.path.exists(folder_data):
                os.mkdir(folder_data)

            dict_data = {}
            dict_data['eniq'] = eniq
            dict_data['date'] = s_date
            dict_data['date2'] = s_date2
            list_data.append(dict_data)

        get_daily_data(list_data)
        combineData(list_data)
        #zipCellDaily(s_date2)
        #file = '/var/opt/common5/eniq/daily_cell_%s.csv'%(s_date2)
