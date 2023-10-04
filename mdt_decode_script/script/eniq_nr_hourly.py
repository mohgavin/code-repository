import pyodbc
import pandas as pd
from datetime import date, datetime, timedelta
import os
import sys
import multiprocessing
import zipfile
from dateutil import rrule
import warnings


def get_sql(q, s_hour, enid):
    #=============================================
    # q = date (yyyymmdd)
    # enid : 
    #   LRAN : 141
    #   CENTRAL : new
    #   WRAN : en4
    #============================================


    query = """            
        select 
            date_id, hour_id, NR_NAME, NRCELLCU, pmCount, numcell, pmCellDowntimeAuto, pmCellDowntimeMan, pmEndcSetupUeSucc, pmEndcSetupUeAtt, pmEndcRelUeAbnormalMenb, pmEndcRelUeAbnormalSgnb, pmEndcRelUeNormal, pmEndcPSCellChangeSuccIntraSgnb, pmEndcPSCellChangeAttIntraSgnb, pmEndcPSCellChangeSuccInterSgnb, pmEndcPSCellChangeAttInterSgnb, pmRrcConnLevelMaxEnDc, pmRrcConnLevelSumEnDc, pmRrcConnLevelSamp,
            pmRadioRaCbSuccMsg3, pmRadioRaCbAttMsg2, pmMacVolDlDrb, pmMacTimeDlDrb, pmMacVolUlResUe, pmMacTimeUlResUe, pmMacVolDl, pmMacVolUl,
            pmMacLatTimeDlDrxSyncQos, pmMacLatTimeDlNoDrxSyncQos, pmMacLatTimeDlNoDrxSyncSampQos, pmMacLatTimeDlDrxSyncSampQos,
            pmRadioRecInterferencePwrDistr_D, pmRadioRecInterferencePwrDistr_N, pmRadioUeRepRankDistr_0, pmRadioUeRepRankDistr_1, pmRadioUeRepRankDistr_2, pmRadioUeRepRankDistr_3, pmMacRBSymUtilDlDistr_D, pmMacRBSymUtilDlDistr_N, pmMacRBSymUtilUlDistr_D, pmMacRBSymUtilUlDistr_N,
            pmRadioUeRepCqi64QamRank1Distr_D, pmRadioUeRepCqi64QamRank2Distr_D, pmRadioUeRepCqi64QamRank3Distr_D, pmRadioUeRepCqi64QamRank4Distr_D,
            pmradioUeRepCqi256Qamrank1distr_D, pmradioUeRepCqi256Qamrank2distr_D, pmradioUeRepCqi256Qamrank3distr_D, pmradioUeRepCqi256Qamrank4distr_D,
            pmRadioUeRepCqi64QamRank1Distr_N, pmRadioUeRepCqi64QamRank2Distr_N, pmRadioUeRepCqi64QamRank3Distr_N, pmRadioUeRepCqi64QamRank4Distr_N,
            pmradioUeRepCqi256Qamrank1distr_N, pmradioUeRepCqi256Qamrank2distr_N, pmradioUeRepCqi256Qamrank3distr_N, pmradioUeRepCqi256Qamrank4distr_N
        from(
        select 
            a.date_id, a.hour_id,a.NR_NAME,a.NRCELLCU
            
            ,sum(pmCount) as pmCount
            ,count( a.NRCELLCU)numcell
            
                    
            ,cast(sum(isnull(pmCellDowntimeAuto,0)) as float)pmCellDowntimeAuto
            ,cast(sum(isnull(pmCellDowntimeMan,0)) as float)pmCellDowntimeMan
            ,cast(sum(isnull(pmEndcSetupUeSucc,0)) as float)pmEndcSetupUeSucc
            ,cast(sum(isnull(pmEndcSetupUeAtt,0)) as float)pmEndcSetupUeAtt
            ,cast(sum(isnull(pmEndcRelUeAbnormalMenb,0)) as float)pmEndcRelUeAbnormalMenb
            ,cast(sum(isnull(pmEndcRelUeAbnormalSgnb,0)) as float)pmEndcRelUeAbnormalSgnb
            ,cast(sum(isnull(pmEndcRelUeNormal,0)) as float)pmEndcRelUeNormal
            ,cast(sum(isnull(pmEndcPSCellChangeSuccIntraSgnb,0)) as float)pmEndcPSCellChangeSuccIntraSgnb
            ,cast(sum(isnull(pmEndcPSCellChangeAttIntraSgnb,0)) as float)pmEndcPSCellChangeAttIntraSgnb
            ,cast(sum(isnull(pmEndcPSCellChangeSuccInterSgnb,0)) as float)pmEndcPSCellChangeSuccInterSgnb
            ,cast(sum(isnull(pmEndcPSCellChangeAttInterSgnb,0)) as float)pmEndcPSCellChangeAttInterSgnb
            ,cast(sum(isnull(pmRrcConnLevelMaxEnDc,0)) as float)pmRrcConnLevelMaxEnDc
            ,cast(sum(isnull(pmRrcConnLevelSumEnDc,0)) as float)pmRrcConnLevelSumEnDc
            ,cast(sum(isnull(pmRrcConnLevelSamp,0)) as float)pmRrcConnLevelSamp

            
            ,cast(sum(isnull(pmRadioRaCbSuccMsg3,0)) as float)pmRadioRaCbSuccMsg3
            ,cast(sum(isnull(pmRadioRaCbAttMsg2,0)) as float)pmRadioRaCbAttMsg2
            ,cast(sum(isnull(pmMacVolDlDrb,0)) as float)pmMacVolDlDrb
            ,cast(sum(isnull(pmMacTimeDlDrb,0)) as float)pmMacTimeDlDrb
            ,cast(sum(isnull(pmMacVolUlResUe,0)) as float)pmMacVolUlResUe
            ,cast(sum(isnull(pmMacTimeUlResUe,0)) as float)pmMacTimeUlResUe
            ,cast(sum(isnull(pmMacVolDl,0)) as float)pmMacVolDl
            ,cast(sum(isnull(pmMacVolUl,0)) as float)pmMacVolUl

            
            ,cast(sum(isnull(pmMacLatTimeDlDrxSyncQos,0)) as float)pmMacLatTimeDlDrxSyncQos
            ,cast(sum(isnull(pmMacLatTimeDlNoDrxSyncQos,0)) as float)pmMacLatTimeDlNoDrxSyncQos
            ,cast(sum(isnull(pmMacLatTimeDlNoDrxSyncSampQos,0)) as float)pmMacLatTimeDlNoDrxSyncSampQos
            ,cast(sum(isnull(pmMacLatTimeDlDrxSyncSampQos,0)) as float)pmMacLatTimeDlDrxSyncSampQos
            ,cast(sum(isnull(pmRadioRecInterferencePwrDistr,0)) as float)pmRadioRecInterferencePwrDistr_D
            ,cast(sum(isnull(pmRadioRecInterferencePwrDistr_N,0)) as float)pmRadioRecInterferencePwrDistr_N
            
            ,cast(sum(isnull(pmRadioUeRepRankDistr_0,0)) as float)pmRadioUeRepRankDistr_0
            ,cast(sum(isnull(pmRadioUeRepRankDistr_1,0)) as float)pmRadioUeRepRankDistr_1
            ,cast(sum(isnull(pmRadioUeRepRankDistr_2,0)) as float)pmRadioUeRepRankDistr_2
            ,cast(sum(isnull(pmRadioUeRepRankDistr_3,0)) as float)pmRadioUeRepRankDistr_3
            ,cast(sum(isnull(pmMacRBSymUtilDlDistr,0)) as float)pmMacRBSymUtilDlDistr_D
            ,cast(sum(isnull(pmMacRBSymUtilDlDistr_N,0)) as float)pmMacRBSymUtilDlDistr_N
            ,cast(sum(isnull(pmMacRBSymUtilUlDistr,0)) as float)pmMacRBSymUtilUlDistr_D
            ,cast(sum(isnull(pmMacRBSymUtilUlDistr_N,0)) as float)pmMacRBSymUtilUlDistr_N
            ,cast(sum(isnull(pmRadioUeRepCqi64QamRank1Distr,0)) as float)pmRadioUeRepCqi64QamRank1Distr_D
            ,cast(sum(isnull(pmRadioUeRepCqi64QamRank2Distr,0)) as float)pmRadioUeRepCqi64QamRank2Distr_D
            ,cast(sum(isnull(pmRadioUeRepCqi64QamRank3Distr,0)) as float)pmRadioUeRepCqi64QamRank3Distr_D
            ,cast(sum(isnull(pmRadioUeRepCqi64QamRank4Distr,0)) as float)pmRadioUeRepCqi64QamRank4Distr_D
            ,cast(sum(isnull(pmradioUeRepCqi256Qamrank1distr,0)) as float)pmradioUeRepCqi256Qamrank1distr_D
            ,cast(sum(isnull(pmradioUeRepCqi256Qamrank2distr,0)) as float)pmradioUeRepCqi256Qamrank2distr_D
            ,cast(sum(isnull(pmradioUeRepCqi256Qamrank3distr,0)) as float)pmradioUeRepCqi256Qamrank3distr_D
            ,cast(sum(isnull(pmradioUeRepCqi256Qamrank4distr,0)) as float)pmradioUeRepCqi256Qamrank4distr_D
            ,cast(sum(isnull(pmRadioUeRepCqi64QamRank1Distr_N,0)) as float)pmRadioUeRepCqi64QamRank1Distr_N
            ,cast(sum(isnull(pmRadioUeRepCqi64QamRank2Distr_N,0)) as float)pmRadioUeRepCqi64QamRank2Distr_N
            ,cast(sum(isnull(pmRadioUeRepCqi64QamRank3Distr_N,0)) as float)pmRadioUeRepCqi64QamRank3Distr_N
            ,cast(sum(isnull(pmRadioUeRepCqi64QamRank4Distr_N,0)) as float)pmRadioUeRepCqi64QamRank4Distr_N
            ,cast(sum(isnull(pmradioUeRepCqi256Qamrank1distr_N,0)) as float)pmradioUeRepCqi256Qamrank1distr_N
            ,cast(sum(isnull(pmradioUeRepCqi256Qamrank2distr_N,0)) as float)pmradioUeRepCqi256Qamrank2distr_N
            ,cast(sum(isnull(pmradioUeRepCqi256Qamrank3distr_N,0)) as float)pmradioUeRepCqi256Qamrank3distr_N
            ,cast(sum(isnull(pmradioUeRepCqi256Qamrank4distr_N,0)) as float)pmradioUeRepCqi256Qamrank4distr_N

            
            
        from
        (
            select 
                left(STR_REPLACE( CONVERT( CHAR(10), dateadd(hh,+7,UTC_DATETIME_ID), 102), '.', '-'),10) as date_id
                , left(STR_REPLACE( CONVERT( CHAR(10), dateadd(hh,+7,UTC_DATETIME_ID), 108), '.', '-'), 2) as hour_id
                , NR_NAME, NRCELLCU
                ,count(*) as pmCount
                ,count(distinct NRCELLCU)numcell
                ,sum(isnull(pmCellDowntimeAuto,0))pmCellDowntimeAuto
                ,sum(isnull(pmCellDowntimeMan,0))pmCellDowntimeMan
                ,sum(isnull(pmEndcSetupUeSucc,0))pmEndcSetupUeSucc
                ,sum(isnull(pmEndcSetupUeAtt,0))pmEndcSetupUeAtt
                ,sum(isnull(pmEndcRelUeAbnormalMenb,0))pmEndcRelUeAbnormalMenb
                ,sum(isnull(pmEndcRelUeAbnormalSgnb,0))pmEndcRelUeAbnormalSgnb
                ,sum(isnull(pmEndcRelUeNormal,0))pmEndcRelUeNormal
                ,sum(isnull(pmEndcPSCellChangeSuccIntraSgnb,0))pmEndcPSCellChangeSuccIntraSgnb
                ,sum(isnull(pmEndcPSCellChangeAttIntraSgnb,0))pmEndcPSCellChangeAttIntraSgnb
                ,sum(isnull(pmEndcPSCellChangeSuccInterSgnb,0))pmEndcPSCellChangeSuccInterSgnb
                ,sum(isnull(pmEndcPSCellChangeAttInterSgnb,0))pmEndcPSCellChangeAttInterSgnb
                ,sum(isnull(pmRrcConnLevelMaxEnDc,0))pmRrcConnLevelMaxEnDc
                ,sum(isnull(pmRrcConnLevelSumEnDc,0))pmRrcConnLevelSumEnDc
                ,sum(isnull(pmRrcConnLevelSamp,0))pmRrcConnLevelSamp
                
            from dwhdb.dcpublic.DC_E_NR_NRCELLCU_RAW
            where (DATETIME_ID BETWEEN '%s %02d:00:00' AND  '%s %02d:59:59') 
            group by 
                date_id, hour_id, NR_NAME, NRCELLCU
        ) a 
        left join 
        (
            select 
                left(STR_REPLACE( CONVERT( CHAR(10), dateadd(hh,+7,UTC_DATETIME_ID), 102), '.', '-'),10) as date_id
                , left(STR_REPLACE( CONVERT( CHAR(10), dateadd(hh,+7,UTC_DATETIME_ID), 108), '.', '-'), 2) as hour_id
                , NR_NAME, NRCELLDU
                ,sum(isnull(pmRadioRaCbSuccMsg3,0))pmRadioRaCbSuccMsg3
                ,sum(isnull(pmRadioRaCbAttMsg2,0))pmRadioRaCbAttMsg2
                ,sum(isnull(pmMacVolDlDrb,0))pmMacVolDlDrb
                ,sum(isnull(pmMacTimeDlDrb,0))pmMacTimeDlDrb
                ,sum(isnull(pmMacVolUlResUe,0))pmMacVolUlResUe
                ,sum(isnull(pmMacTimeUlResUe,0))pmMacTimeUlResUe
                ,sum(isnull(pmMacVolDl,0))pmMacVolDl
                ,sum(isnull(pmMacVolUl,0))pmMacVolUl
                
            from dwhdb.dcpublic.DC_E_NR_NRCELLDU_RAW
            where (DATETIME_ID BETWEEN '%s %02d:00:00' AND  '%s %02d:59:59') 
            group by 
                date_id, hour_id, NR_NAME, NRCELLDU
        
        ) b on a.date_id=b.date_id and a.NR_NAME=b.NR_NAME and a.NRCELLCU=b.NRCELLDU and a.hour_id = b.hour_id
        left join
        (
            select 
            date_id, hour_id, NR_NAME, NRCELLDU
            ,sum(isnull(pmMacLatTimeDlDrxSyncQos,0))pmMacLatTimeDlDrxSyncQos
            ,sum(isnull(pmMacLatTimeDlNoDrxSyncQos,0))pmMacLatTimeDlNoDrxSyncQos
            ,sum(isnull(pmMacLatTimeDlNoDrxSyncSampQos,0))pmMacLatTimeDlNoDrxSyncSampQos
            ,sum(isnull(pmMacLatTimeDlDrxSyncSampQos,0))pmMacLatTimeDlDrxSyncSampQos
            
            ,sum(isnull(pmRadioRecInterferencePwrDistr,0))pmRadioRecInterferencePwrDistr
            ,sum(isnull(pmRadioRecInterferencePwrDistr_N,0))pmRadioRecInterferencePwrDistr_N
            ,sum(case when DCVECTOR_INDEX = 0 then pmRadioUeRepRankDistr end)pmRadioUeRepRankDistr_0
            ,sum(case when DCVECTOR_INDEX = 1 then pmRadioUeRepRankDistr end)pmRadioUeRepRankDistr_1
            ,sum(case when DCVECTOR_INDEX = 2 then pmRadioUeRepRankDistr end)pmRadioUeRepRankDistr_2
            ,sum(case when DCVECTOR_INDEX = 3 then pmRadioUeRepRankDistr end)pmRadioUeRepRankDistr_3
            ,sum(isnull(pmMacRBSymUtilDlDistr,0))pmMacRBSymUtilDlDistr
            ,sum(isnull(pmMacRBSymUtilDlDistr_N,0))pmMacRBSymUtilDlDistr_N
            ,sum(isnull(pmMacRBSymUtilUlDistr,0))pmMacRBSymUtilUlDistr
            ,sum(isnull(pmMacRBSymUtilUlDistr_N,0))pmMacRBSymUtilUlDistr_N
            ,sum(isnull(pmRadioUeRepCqi64QamRank1Distr,0))pmRadioUeRepCqi64QamRank1Distr
            ,sum(isnull(pmRadioUeRepCqi64QamRank2Distr,0))pmRadioUeRepCqi64QamRank2Distr
            ,sum(isnull(pmRadioUeRepCqi64QamRank3Distr,0))pmRadioUeRepCqi64QamRank3Distr
            ,sum(isnull(pmRadioUeRepCqi64QamRank4Distr,0))pmRadioUeRepCqi64QamRank4Distr
            ,sum(isnull(pmradioUeRepCqi256Qamrank1distr,0))pmradioUeRepCqi256Qamrank1distr
            ,sum(isnull(pmradioUeRepCqi256Qamrank2distr,0))pmradioUeRepCqi256Qamrank2distr
            ,sum(isnull(pmradioUeRepCqi256Qamrank3distr,0))pmradioUeRepCqi256Qamrank3distr
            ,sum(isnull(pmradioUeRepCqi256Qamrank4distr,0))pmradioUeRepCqi256Qamrank4distr
            ,sum(isnull(pmRadioUeRepCqi64QamRank1Distr_N,0))pmRadioUeRepCqi64QamRank1Distr_N
            ,sum(isnull(pmRadioUeRepCqi64QamRank2Distr_N,0))pmRadioUeRepCqi64QamRank2Distr_N
            ,sum(isnull(pmRadioUeRepCqi64QamRank3Distr_N,0))pmRadioUeRepCqi64QamRank3Distr_N
            ,sum(isnull(pmRadioUeRepCqi64QamRank4Distr_N,0))pmRadioUeRepCqi64QamRank4Distr_N
            ,sum(isnull(pmradioUeRepCqi256Qamrank1distr_N,0))pmradioUeRepCqi256Qamrank1distr_N
            ,sum(isnull(pmradioUeRepCqi256Qamrank2distr_N,0))pmradioUeRepCqi256Qamrank2distr_N
            ,sum(isnull(pmradioUeRepCqi256Qamrank3distr_N,0))pmradioUeRepCqi256Qamrank3distr_N
            ,sum(isnull(pmradioUeRepCqi256Qamrank4distr_N,0))pmradioUeRepCqi256Qamrank4distr_N
            

            from (
                select
                    left(STR_REPLACE( CONVERT( CHAR(10), dateadd(hh,+7,UTC_DATETIME_ID), 102), '.', '-'),10) as date_id
                    , left(STR_REPLACE( CONVERT( CHAR(10), dateadd(hh,+7,UTC_DATETIME_ID), 108), '.', '-'), 2) as hour_id
                    , NR_NAME, NRCELLDU, DCVECTOR_INDEX
                    ,sum(isnull(pmMacLatTimeDlDrxSyncQos,0))pmMacLatTimeDlDrxSyncQos
                    ,sum(isnull(pmMacLatTimeDlNoDrxSyncQos,0))pmMacLatTimeDlNoDrxSyncQos
                    ,sum(isnull(pmMacLatTimeDlNoDrxSyncSampQos,0))pmMacLatTimeDlNoDrxSyncSampQos
                    ,sum(isnull(pmMacLatTimeDlDrxSyncSampQos,0))pmMacLatTimeDlDrxSyncSampQos
                    ,sum(isnull(pmRadioRecInterferencePwrDistr,0))pmRadioRecInterferencePwrDistr
                    ,sum(isnull(pmRadioUeRepRankDistr,0))pmRadioUeRepRankDistr
                    ,sum(isnull(pmMacRBSymUtilDlDistr,0))pmMacRBSymUtilDlDistr
                    ,sum(isnull(pmMacRBSymUtilUlDistr,0))pmMacRBSymUtilUlDistr
                    ,sum(isnull(pmRadioUeRepCqi64QamRank1Distr,0))pmRadioUeRepCqi64QamRank1Distr
                    ,sum(isnull(pmRadioUeRepCqi64QamRank2Distr,0))pmRadioUeRepCqi64QamRank2Distr
                    ,sum(isnull(pmRadioUeRepCqi64QamRank3Distr,0))pmRadioUeRepCqi64QamRank3Distr
                    ,sum(isnull(pmRadioUeRepCqi64QamRank4Distr,0))pmRadioUeRepCqi64QamRank4Distr
                    ,sum(isnull(pmradioUeRepCqi256Qamrank1distr,0))pmradioUeRepCqi256Qamrank1distr
                    ,sum(isnull(pmradioUeRepCqi256Qamrank2distr,0))pmradioUeRepCqi256Qamrank2distr
                    ,sum(isnull(pmradioUeRepCqi256Qamrank3distr,0))pmradioUeRepCqi256Qamrank3distr
                    ,sum(isnull(pmradioUeRepCqi256Qamrank4distr,0))pmradioUeRepCqi256Qamrank4distr

                    

                    ,case
                        when DCVECTOR_INDEX = 0 then 0.79432
                        when DCVECTOR_INDEX = 1 then 0.89716
                        when DCVECTOR_INDEX = 2 then 1.12946
                        when DCVECTOR_INDEX = 3 then 1.42191
                        when DCVECTOR_INDEX = 4 then 1.79008
                        when DCVECTOR_INDEX = 5 then 2.25357
                        when DCVECTOR_INDEX = 6 then 2.83708
                        when DCVECTOR_INDEX = 7 then 3.57167
                        when DCVECTOR_INDEX = 8 then 4.49647
                        when DCVECTOR_INDEX = 9 then 5.66072
                        when DCVECTOR_INDEX = 10 then 11.07925
                        when DCVECTOR_INDEX = 11 then 27.82982
                        when DCVECTOR_INDEX = 12 then 69.90536
                        when DCVECTOR_INDEX = 13 then 175.59432
                        when DCVECTOR_INDEX = 14 then 441.07
                        when DCVECTOR_INDEX = 15 then 630.95
                    else null end * pmRadioRecInterferencePwrDistr as pmRadioRecInterferencePwrDistr_N

                    ,case
                        when DCVECTOR_INDEX = 0 then  5
                        when DCVECTOR_INDEX = 1 then  10
                        when DCVECTOR_INDEX = 2 then  15
                        when DCVECTOR_INDEX = 3 then  20
                        when DCVECTOR_INDEX = 4 then  25
                        when DCVECTOR_INDEX = 5 then  30
                        when DCVECTOR_INDEX = 6 then  35
                        when DCVECTOR_INDEX = 7 then  40
                        when DCVECTOR_INDEX = 8 then  45
                        when DCVECTOR_INDEX = 9 then  50
                        when DCVECTOR_INDEX = 10 then 55
                        when DCVECTOR_INDEX = 11 then 60
                        when DCVECTOR_INDEX = 12 then 65
                        when DCVECTOR_INDEX = 13 then 70
                        when DCVECTOR_INDEX = 14 then 75
                        when DCVECTOR_INDEX = 15 then 80
                        when DCVECTOR_INDEX = 16 then 85
                        when DCVECTOR_INDEX = 17 then 90
                        when DCVECTOR_INDEX = 18 then 95
                        when DCVECTOR_INDEX = 19 then 100
                    else null end * pmMacRBSymUtilDlDistr as pmMacRBSymUtilDlDistr_N
                    ,case
                        when DCVECTOR_INDEX = 0 then  5
                        when DCVECTOR_INDEX = 1 then  10
                        when DCVECTOR_INDEX = 2 then  15
                        when DCVECTOR_INDEX = 3 then  20
                        when DCVECTOR_INDEX = 4 then  25
                        when DCVECTOR_INDEX = 5 then  30
                        when DCVECTOR_INDEX = 6 then  35
                        when DCVECTOR_INDEX = 7 then  40
                        when DCVECTOR_INDEX = 8 then  45
                        when DCVECTOR_INDEX = 9 then  50
                        when DCVECTOR_INDEX = 10 then 55
                        when DCVECTOR_INDEX = 11 then 60
                        when DCVECTOR_INDEX = 12 then 65
                        when DCVECTOR_INDEX = 13 then 70
                        when DCVECTOR_INDEX = 14 then 75
                        when DCVECTOR_INDEX = 15 then 80
                        when DCVECTOR_INDEX = 16 then 85
                        when DCVECTOR_INDEX = 17 then 90
                        when DCVECTOR_INDEX = 18 then 95
                        when DCVECTOR_INDEX = 19 then 100
                    else null end * pmMacRBSymUtilUlDistr as pmMacRBSymUtilUlDistr_N
                    ,case
                        when DCVECTOR_INDEX = 0 then  0
                        when DCVECTOR_INDEX = 1 then  1
                        when DCVECTOR_INDEX = 2 then  2
                        when DCVECTOR_INDEX = 3 then  3
                        when DCVECTOR_INDEX = 4 then  4
                        when DCVECTOR_INDEX = 5 then  5
                        when DCVECTOR_INDEX = 6 then  6
                        when DCVECTOR_INDEX = 7 then  7
                        when DCVECTOR_INDEX = 8 then  8
                        when DCVECTOR_INDEX = 9 then  9
                        when DCVECTOR_INDEX = 10 then 10
                        when DCVECTOR_INDEX = 11 then 11
                        when DCVECTOR_INDEX = 12 then 12
                        when DCVECTOR_INDEX = 13 then 13
                        when DCVECTOR_INDEX = 14 then 14
                        when DCVECTOR_INDEX = 15 then 15
                    else null end * pmRadioUeRepCqi64QamRank1Distr as pmRadioUeRepCqi64QamRank1Distr_N
                    ,case
                        when DCVECTOR_INDEX = 0 then  0
                        when DCVECTOR_INDEX = 1 then  1
                        when DCVECTOR_INDEX = 2 then  2
                        when DCVECTOR_INDEX = 3 then  3
                        when DCVECTOR_INDEX = 4 then  4
                        when DCVECTOR_INDEX = 5 then  5
                        when DCVECTOR_INDEX = 6 then  6
                        when DCVECTOR_INDEX = 7 then  7
                        when DCVECTOR_INDEX = 8 then  8
                        when DCVECTOR_INDEX = 9 then  9
                        when DCVECTOR_INDEX = 10 then 10
                        when DCVECTOR_INDEX = 11 then 11
                        when DCVECTOR_INDEX = 12 then 12
                        when DCVECTOR_INDEX = 13 then 13
                        when DCVECTOR_INDEX = 14 then 14
                        when DCVECTOR_INDEX = 15 then 15
                    else null end * pmRadioUeRepCqi64QamRank2Distr as pmRadioUeRepCqi64QamRank2Distr_N
                    ,case
                        when DCVECTOR_INDEX = 0 then  0
                        when DCVECTOR_INDEX = 1 then  1
                        when DCVECTOR_INDEX = 2 then  2
                        when DCVECTOR_INDEX = 3 then  3
                        when DCVECTOR_INDEX = 4 then  4
                        when DCVECTOR_INDEX = 5 then  5
                        when DCVECTOR_INDEX = 6 then  6
                        when DCVECTOR_INDEX = 7 then  7
                        when DCVECTOR_INDEX = 8 then  8
                        when DCVECTOR_INDEX = 9 then  9
                        when DCVECTOR_INDEX = 10 then 10
                        when DCVECTOR_INDEX = 11 then 11
                        when DCVECTOR_INDEX = 12 then 12
                        when DCVECTOR_INDEX = 13 then 13
                        when DCVECTOR_INDEX = 14 then 14
                        when DCVECTOR_INDEX = 15 then 15
                    else null end * pmRadioUeRepCqi64QamRank3Distr as pmRadioUeRepCqi64QamRank3Distr_N
                    ,case
                        when DCVECTOR_INDEX = 0 then  0
                        when DCVECTOR_INDEX = 1 then  1
                        when DCVECTOR_INDEX = 2 then  2
                        when DCVECTOR_INDEX = 3 then  3
                        when DCVECTOR_INDEX = 4 then  4
                        when DCVECTOR_INDEX = 5 then  5
                        when DCVECTOR_INDEX = 6 then  6
                        when DCVECTOR_INDEX = 7 then  7
                        when DCVECTOR_INDEX = 8 then  8
                        when DCVECTOR_INDEX = 9 then  9
                        when DCVECTOR_INDEX = 10 then 10
                        when DCVECTOR_INDEX = 11 then 11
                        when DCVECTOR_INDEX = 12 then 12
                        when DCVECTOR_INDEX = 13 then 13
                        when DCVECTOR_INDEX = 14 then 14
                        when DCVECTOR_INDEX = 15 then 15
                    else null end * pmRadioUeRepCqi64QamRank4Distr as pmRadioUeRepCqi64QamRank4Distr_N
                    ,case
                        when DCVECTOR_INDEX = 0 then  0
                        when DCVECTOR_INDEX = 1 then  1
                        when DCVECTOR_INDEX = 2 then  2
                        when DCVECTOR_INDEX = 3 then  3
                        when DCVECTOR_INDEX = 4 then  4
                        when DCVECTOR_INDEX = 5 then  5
                        when DCVECTOR_INDEX = 6 then  6
                        when DCVECTOR_INDEX = 7 then  7
                        when DCVECTOR_INDEX = 8 then  8
                        when DCVECTOR_INDEX = 9 then  9
                        when DCVECTOR_INDEX = 10 then 10
                        when DCVECTOR_INDEX = 11 then 11
                        when DCVECTOR_INDEX = 12 then 12
                        when DCVECTOR_INDEX = 13 then 13
                        when DCVECTOR_INDEX = 14 then 14
                        when DCVECTOR_INDEX = 15 then 15
                    else null end * pmradioUeRepCqi256Qamrank1distr as pmradioUeRepCqi256Qamrank1distr_N
                    ,case
                        when DCVECTOR_INDEX = 0 then  0
                        when DCVECTOR_INDEX = 1 then  1
                        when DCVECTOR_INDEX = 2 then  2
                        when DCVECTOR_INDEX = 3 then  3
                        when DCVECTOR_INDEX = 4 then  4
                        when DCVECTOR_INDEX = 5 then  5
                        when DCVECTOR_INDEX = 6 then  6
                        when DCVECTOR_INDEX = 7 then  7
                        when DCVECTOR_INDEX = 8 then  8
                        when DCVECTOR_INDEX = 9 then  9
                        when DCVECTOR_INDEX = 10 then 10
                        when DCVECTOR_INDEX = 11 then 11
                        when DCVECTOR_INDEX = 12 then 12
                        when DCVECTOR_INDEX = 13 then 13
                        when DCVECTOR_INDEX = 14 then 14
                        when DCVECTOR_INDEX = 15 then 15
                    else null end * pmradioUeRepCqi256Qamrank2distr as pmradioUeRepCqi256Qamrank2distr_N
                    ,case
                        when DCVECTOR_INDEX = 0 then  0
                        when DCVECTOR_INDEX = 1 then  1
                        when DCVECTOR_INDEX = 2 then  2
                        when DCVECTOR_INDEX = 3 then  3
                        when DCVECTOR_INDEX = 4 then  4
                        when DCVECTOR_INDEX = 5 then  5
                        when DCVECTOR_INDEX = 6 then  6
                        when DCVECTOR_INDEX = 7 then  7
                        when DCVECTOR_INDEX = 8 then  8
                        when DCVECTOR_INDEX = 9 then  9
                        when DCVECTOR_INDEX = 10 then 10
                        when DCVECTOR_INDEX = 11 then 11
                        when DCVECTOR_INDEX = 12 then 12
                        when DCVECTOR_INDEX = 13 then 13
                        when DCVECTOR_INDEX = 14 then 14
                        when DCVECTOR_INDEX = 15 then 15
                    else null end * pmradioUeRepCqi256Qamrank3distr as pmradioUeRepCqi256Qamrank3distr_N
                    ,case
                        when DCVECTOR_INDEX = 0 then  0
                        when DCVECTOR_INDEX = 1 then  1
                        when DCVECTOR_INDEX = 2 then  2
                        when DCVECTOR_INDEX = 3 then  3
                        when DCVECTOR_INDEX = 4 then  4
                        when DCVECTOR_INDEX = 5 then  5
                        when DCVECTOR_INDEX = 6 then  6
                        when DCVECTOR_INDEX = 7 then  7
                        when DCVECTOR_INDEX = 8 then  8
                        when DCVECTOR_INDEX = 9 then  9
                        when DCVECTOR_INDEX = 10 then 10
                        when DCVECTOR_INDEX = 11 then 11
                        when DCVECTOR_INDEX = 12 then 12
                        when DCVECTOR_INDEX = 13 then 13
                        when DCVECTOR_INDEX = 14 then 14
                        when DCVECTOR_INDEX = 15 then 15
                    else null end * pmradioUeRepCqi256Qamrank4distr as pmradioUeRepCqi256Qamrank4distr_N

                from dwhdb.dcpublic.DC_E_NR_NRCELLDU_V_RAW
                where (DATETIME_ID BETWEEN '%s %02d:00:00' AND  '%s %02d:59:59') 
                    and DCVECTOR_INDEX <= 30
                group by 
                    date_id, hour_id, NR_NAME, NRCELLDU, DCVECTOR_INDEX
            ) p 
            group by date_id, hour_id, NR_NAME, NRCELLDU
        ) c on a.date_id=c.date_id and a.NR_NAME=c.NR_NAME and a.NRCELLCU=c.NRCELLDU and a.hour_id = c.hour_id

        

        group by a.date_id, a.hour_id,a.NR_NAME,a.NRCELLCU
        ) xx
            ;
                        """ %(q, s_hour, q, s_hour, q, s_hour, q, s_hour, q, s_hour, q, s_hour)
    return query


def get_data(dict_id):
    start_time = datetime.now()

    s_date = dict_id['date']
    s_date2 = dict_id['date2']
    s_port = dict_id['eniq']['enid']
    s_hour = dict_id['hour']
    dict_conn = dict_id['eniq']
    
    path_daily = '/var/opt/common5/eniq/%s'%(s_port)
    if not os.path.exists(path_daily):
        try:
            os.mkdir(path_daily)
        except:
            pass
    path_daily = '/var/opt/common5/eniq/%s/%s'%(s_port, s_date2)
    if not os.path.exists(path_daily):
        try:
            os.mkdir(path_daily)
        except:
            pass
    
    conn = False
    sql_query = get_sql(s_date2, s_hour, s_port)

    try:
        conn = pyodbc.connect('DRIVER=freetds;SERVER=%s;PORT=%s;UID=%s;PWD=%s;TDS _Version=5.0;' %(dict_conn['host'], dict_conn['port'], dict_conn['user'], dict_conn['passwd']))

        sql_query = pd.read_sql_query(sql_query,conn) # here, the 'conn' is the variable that contains your database connection information from step 2
        df = pd.DataFrame(sql_query)
        #print('   Query success........')
        df.to_csv("%s/hourly_nr_cell_%s_%02d.csv" %(path_daily, s_date2, s_hour), index=None)
        
        duration = datetime.now() - start_time
        print("    %s %s %02d Processing done with duration: %s" %(s_port, s_date2, s_hour, duration))
        conn.close()
    except Exception as e:
        print("Failed connection %s : %s" % (s_port, e))
        if conn:
            conn.close()
            print("Connection closed for %s" %s_port)
        pass


def get_daily_data(list_data):
    print("Collecting data %s on %s" %(s_date, datetime.now())) 

    num_worker = 4
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
        s_hour = dict_id['hour']
        s_port = dict_id['eniq']['enid']
        path_daily = '/var/opt/common5/eniq/%s/%s'%(s_port, s_date2)
        file = "%s/hourly_nr_cell_%s_%02d.csv" %(path_daily, s_date2, s_hour)
        
        if os.path.exists(file):
            df_new = pd.read_csv(file, delimiter=",", index_col=None, header='infer')
            if len(df_new) > 0:
                list_df.append(df_new)
    if len(list_df) > 0:
        df = pd.concat(list_df, sort=False)
        path_daily = '/var/opt/common5/eniq'
        file = '%s/hourly_nr_cell_%s.csv'%(path_daily, s_date2)
        df.to_csv(file, index=None)

        #zip result
        zip_file = "%s/hourly_nr_cell_%s.zip" %(path_daily, s_date2)
        zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
        zf.write(file,'hourly_nr_cell_%s.csv'%s_date2)
        zf.close()
        os.remove(file)
        print('Result on %s'%zip_file)

def zipCellDaily(s_date2):
    print()
    path_daily = '/var/opt/common5/eniq'
    filename = 'hourly_nr_cell_%s.csv'%(s_date2)
    zip_file = "%s/hourly_nr_cell_%s.zip" %(path_daily, s_date2)

    

    with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as zipf:
        zipf.write(os.path.join(path_daily,filename), arcname=filename)

    #os.remove(os.path.join(path_daily,filename))
    print('Result on %s'%zip_file)



if __name__ == '__main__':
    warnings.filterwarnings('ignore')
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
    
    list_eniq = [
        {
        'port': '2642'
        ,'host': '10.167.249.79'
        ,'passwd': 'XLrsc@123'
        ,'user': 'dcpublic'
        ,'eniq': 'WRAN2'
        ,'enid': 'WRAN2'
        },
        {
        'port': '2642'
        ,'host': '10.167.36.79'
        ,'passwd': 'XLrsc@123'
        ,'user': 'dcpublic'
        ,'eniq': 'ENIQ5'
        ,'enid': 'ENIQ5'
        },
    ]
    
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

            for hour in range(24):
                dict_data = {}
                dict_data['eniq'] = eniq
                dict_data['date'] = s_date
                dict_data['date2'] = s_date2
                dict_data['hour'] = hour
                list_data.append(dict_data)

        get_daily_data(list_data)
        combineData(list_data)
        #zipCellDaily(s_date2)
        #file = '/var/opt/common5/eniq/daily_cell_%s.csv'%(s_date2)
