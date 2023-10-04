import pyodbc
import pandas as pd
from datetime import date, datetime, timedelta
import os
import sys
import multiprocessing
import zipfile
from dateutil import rrule
import warnings


def get_sql(q, enid):
    #=============================================
    # q = date (yyyymmdd)
    # enid : 
    #   LRAN : 141
    #   CENTRAL : new
    #   WRAN : en4
    #============================================


    query = """ 
        SELECT DISTINCT
            a.DATE_ID
            ,a.BSC
            ,a.CELL_NAME
            , NUMCELL_2G = Sum(a.NUMCELL_2G)


            , cast(((sum(TFTRALACC)+sum(THTRALACC))/360) as dec(19,5)) as TCH_Traffic_
            , cast(sum(CTRALACC)/sum(CNSCAN) as dec(19,5)) as SDCCH_Traffic_
            , cast(((sum([DLTHP1GDATA])+sum([DLTHP2GDATA])+sum([DLTHP3GDATA])+sum([DLBGGDATA]))/8000)+((sum([DLTHP1EGDATA])+sum([DLTHP2EGDATA])+sum([DLTHP3EGDATA])+sum([DLBGEGDATA]))/8000) as dec(19,5)) as PAYLOAD_GB
            , sum(CNROCNT) as CNROCNT
            , sum(RAACCFA) as RAACCFA
            , sum(RAEMCAL) as RAEMCAL
            , sum(RACALRE) as RACALRE
            , sum(RAANPAG) as RAANPAG
            , sum(RAOSREQ) as RAOSREQ
            , sum(RAOTHER) as RAOTHER
            , sum(RATRHFAEMCAL) as RATRHFAEMCAL
            , sum(RATRHFAOTHER) as RATRHFAOTHER
            , sum(RATRHFAREG) as RATRHFAREG
            , sum(RATRHFAANPAG) as RATRHFAANPAG
            , sum(RACALR1) as RACALR1
            , sum(RACALR2) as RACALR2
            , sum(RAAPAG1) as RAAPAG1
            , sum(RAAPAG2) as RAAPAG2
            , sum(RAAPOPS) as RAAPOPS
            , sum(RAORSPE) as RAORSPE
            , sum(RAORDAT) as RAORDAT
            , sum(CCONGS) as CCONGS
            , sum(CCALLS) as CCALLS
            , sum(CMSESTAB) as CMSESTAB
            , sum(CNRELCONG) as CNRELCONG
            , sum(CNDROP) as CNDROP
            , sum(CNUCHCNT) as CNUCHCNT

            , sum(CAVAACC) as CAVAACC
            , sum(CDISSS) as CDISSS
            , sum(CDISSSSUB) as CDISSSSUB

            , sum(CDISQA) as CDISQA
            , sum(CDISQASUB) as CDISQASUB
            , sum(CDISTA) as CDISTA
            , sum(TCASSALL) as TCASSALL
            , sum(TFCASSALL) as TFCASSALL
            , sum(TFCASSALLSUB) as TFCASSALLSUB
            , sum(THCASSALL) as THCASSALL
            , sum(THCASSALLSUB) as THCASSALLSUB
            , sum(TASSALL) as TASSALL
            , sum(TFNRELCONG) as TFNRELCONG
            , sum(TFNRELCONGSUB) as TFNRELCONGSUB
            , sum(THNRELCONG) as THNRELCONG
            , sum(THNRELCONGSUB) as THNRELCONGSUB
            , sum(TFNSCAN) as TFNSCAN
            , sum(THNSCAN) as THNSCAN
            , sum(TAVAACC) as TAVAACC
            , sum(TAVASCAN) as TAVASCAN
            , sum(TNUCHCNT) as TNUCHCNT
            , sum(TFNDROP) as TFNDROP
            , sum(TFNDROPSUB) as TFNDROPSUB
            , sum(THNDROP) as THNDROP
            , sum(THNDROPSUB) as THNDROPSUB
            , sum(TFDISSUL) as TFDISSUL
            , sum(TFDISSULSUB) as TFDISSULSUB
            , sum(THDISSUL) as THDISSUL
            , sum(THDISSULSUB) as THDISSULSUB
            , sum(TFDISSDL) as TFDISSDL
            , sum(TFDISSDLSUB) as TFDISSDLSUB
            , sum(THDISSDL) as THDISSDL
            , sum(THDISSDLSUB) as THDISSDLSUB
            , sum(TFDISSBL) as TFDISSBL
            , sum(TFDISSBLSUB) as TFDISSBLSUB
            , sum(THDISSBL) as THDISSBL
            , sum(THDISSBLSUB) as THDISSBLSUB
            , sum(TFDISQAUL) as TFDISQAUL
            , sum(TFDISQAULSUB) as TFDISQAULSUB
            , sum(THDISQAUL) as THDISQAUL
            , sum(THDISQAULSUB) as THDISQAULSUB
            , sum(TFDISQADL) as TFDISQADL
            , sum(TFDISQADLSUB) as TFDISQADLSUB
            , sum(THDISQADL) as THDISQADL
            , sum(THDISQADLSUB) as THDISQADLSUB
            , sum(TFDISQABL) as TFDISQABL
            , sum(TFDISQABLSUB) as TFDISQABLSUB
            , sum(THDISQABL) as THDISQABL
            , sum(THDISQABLSUB) as THDISQABLSUB
            , sum(THSUDLOS) as THSUDLOS
            , sum(TFSUDLOSSUB) as TFSUDLOSSUB
            , sum(TFDISTA) as TFDISTA
            , sum(THDISTA) as THDISTA
            , sum(TDWNACC) as TDWNACC
            , sum(TDWNSCAN) as TDWNSCAN
            , sum(TFMSESTB) as TFMSESTB
            , sum(THMSESTB) as THMSESTB
            , sum(DISNORM) as DISNORM
            , sum(ITFUSIB1) as ITFUSIB1
            , sum(ITFUSIB2) as ITFUSIB2
            , sum(ITFUSIB3) as ITFUSIB3
            , sum(ITFUSIB4) as ITFUSIB4
            , sum(ITFUSIB5) as ITFUSIB5
            , sum(ITHUSIB1) as ITHUSIB1
            , sum(ITHUSIB2) as ITHUSIB2
            , sum(ITHUSIB3) as ITHUSIB3
            , sum(ITHUSIB4) as ITHUSIB4
            , sum(ITHUSIB5) as ITHUSIB5
            , sum(QUAL00DL) as QUAL00DL
            , sum(QUAL10DL) as QUAL10DL
            , sum(QUAL20DL) as QUAL20DL
            , sum(QUAL30DL) as QUAL30DL
            , sum(QUAL40DL) as QUAL40DL
            , sum(QUAL50DL) as QUAL50DL
            , sum(QUAL60DL) as QUAL60DL
            , sum(QUAL70DL) as QUAL70DL
            , sum(QUAL00UL) as QUAL00UL
            , sum(QUAL10UL) as QUAL10UL
            , sum(QUAL20UL) as QUAL20UL
            , sum(QUAL30UL) as QUAL30UL
            , sum(QUAL40UL) as QUAL40UL
            , sum(QUAL50UL) as QUAL50UL
            , sum(QUAL60UL) as QUAL60UL
            , sum(QUAL70UL) as QUAL70UL
            , sum(TFTRALSUB) as TFTRALSUB
            , sum(THTRALSUB) as THTRALSUB
            , sum(TSQIGOOD) as TSQIGOOD
            , sum(TSQIGOODSUB) as TSQIGOODSUB
            , sum(TSQIGOODAF) as TSQIGOODAF
            , sum(TSQIGOODSUBAF) as TSQIGOODSUBAF
            , sum(TSQIGOODAH) as TSQIGOODAH
            , sum(TSQIGOODSUBAH) as TSQIGOODSUBAH
            , sum(TSQIACCPT) as TSQIACCPT
            , sum(TSQIACCPTSUB) as TSQIACCPTSUB
            , sum(TSQIBAD) as TSQIBAD
            , sum(TSQIBADSUB) as TSQIBADSUB
            , sum(TSQIACCPTAF) as TSQIACCPTAF
            , sum(TSQIACCPTSUBAF) as TSQIACCPTSUBAF
            , sum(TSQIBADAF) as TSQIBADAF
            , sum(TSQIBADSUBAF) as TSQIBADSUBAF
            , sum(TSQIACCPTAH) as TSQIACCPTAH
            , sum(TSQIACCPTSUBAH) as TSQIACCPTSUBAH
            , sum(TSQIBADAH) as TSQIBADAH
            , sum(TSQIBADSUBAH) as TSQIBADSUBAH
            , sum(CESTIMMASS) as CESTIMMASS
            , sum(CFAIL) as CFAIL
            , sum(C_TTRALACC) as C_TTRALACC
            , sum(C_TTRAL_HR_OL) as C_TTRAL_HR_OL
            , sum(C_TTRAL_FR_OL) as C_TTRAL_FR_OL
            , sum(C_TTRALACC_HR) as C_TTRALACC_HR
            , sum(C_TTRALACC_FR) as C_TTRALACC_FR
            , sum(C_TAVA) as C_TAVA
            
            

            from 

            (SELECT DISTINCT 
            BSC, CELL_NAME, DATE_ID,
            cast(SUM(RANDOMACC_CNROCNT) as dec(19,5)) AS CNROCNT,
            cast(SUM(RANDOMACC_RAACCFA) as dec(19,5)) AS RAACCFA,
            cast(SUM(RANDOMACC_RAEMCAL) as dec(19,5)) AS RAEMCAL,
            cast(SUM(RANDOMACC_RACALRE) as dec(19,5)) AS RACALRE,
            cast(SUM(RANDOMACC_RAANPAG) as dec(19,5)) AS RAANPAG,
            cast(SUM(RANDOMACC_RAOSREQ) as dec(19,5)) AS RAOSREQ,
            cast(SUM(RANDOMACC_RAOTHER) as dec(19,5)) AS RAOTHER,
            cast(SUM(RANDOMACC_RATRHFAEMCAL) as dec(19,5)) AS RATRHFAEMCAL,
            cast(SUM(RANDOMACC_RATRHFAOTHER) as dec(19,5)) AS RATRHFAOTHER,
            cast(SUM(RANDOMACC_RATRHFAREG) as dec(19,5)) AS RATRHFAREG,
            cast(SUM(RANDOMACC_RATRHFAANPAG) as dec(19,5)) AS RATRHFAANPAG,
            cast(SUM(RNDACCEXT_RACALR1) as dec(19,5)) AS RACALR1,
            cast(SUM(RNDACCEXT_RACALR2) as dec(19,5)) AS RACALR2,
            cast(SUM(RNDACCEXT_RAAPAG1) as dec(19,5)) AS RAAPAG1,
            cast(SUM(RNDACCEXT_RAAPAG2) as dec(19,5)) AS RAAPAG2,
            cast(SUM(RNDACCEXT_RAAPOPS) as dec(19,5)) AS RAAPOPS,
            cast(SUM(RNDACCEXT_RAORSPE) as dec(19,5)) AS RAORSPE,
            cast(SUM(RNDACCEXT_RAORDAT) as dec(19,5)) AS RAORDAT,
            cast(SUM(CLSDCCH_CCONGS) as dec(19,5)) AS CCONGS,
            cast(SUM(CLSDCCH_CCALLS) as dec(19,5)) AS CCALLS,
            cast(SUM(CLSDCCH_CMSESTAB) as dec(19,5)) AS CMSESTAB,
            cast(SUM(CLSDCCH_CNRELCONG) as dec(19,5)) AS CNRELCONG,
            cast(SUM(CLSDCCH_CNDROP) as dec(19,5)) AS CNDROP,
            cast(SUM(CLSDCCH_CNUCHCNT) as dec(19,5)) AS CNUCHCNT,

            cast(SUM(CLSDCCH_CTRALACC) as dec(19,5)) AS CTRALACC,
            cast(AVG(CLSDCCH_CNSCAN) as dec(19,5)) AS CNSCAN,
            cast(SUM(CLSDCCH_CAVAACC) as dec(19,5)) AS CAVAACC,
            cast(AVG(CLSDCCH_CAVASCAN) as dec(19,5)) AS CAVASCAN,
            cast(SUM(CELLCCHDR_CDISSS) as dec(19,5)) AS CDISSS,
            cast(SUM(CELLCCHDR_CDISSSSUB) as dec(19,5)) AS CDISSSSUB,

            cast(SUM(CELLCCHDR_CDISQA) as dec(19,5)) AS CDISQA,
            cast(SUM(CELLCCHDR_CDISQASUB) as dec(19,5)) AS CDISQASUB,
            cast(SUM(CELLCCHDR_CDISTA) as dec(19,5)) AS CDISTA,
            cast(SUM(CLTCH_TCASSALL) as dec(19,5)) AS TCASSALL,
            cast(SUM(CELTCHF_TFCASSALL) as dec(19,5)) AS TFCASSALL,
            cast(SUM(CELTCHF_TFCASSALLSUB) as dec(19,5)) AS TFCASSALLSUB,
            cast(SUM(CELTCHH_THCASSALL) as dec(19,5)) AS THCASSALL,
            cast(SUM(CELTCHH_THCASSALLSUB) as dec(19,5)) AS THCASSALLSUB,
            cast(SUM(CLTCH_TASSALL) as dec(19,5)) AS TASSALL,
            cast(SUM(CELTCHF_TFNRELCONG) as dec(19,5)) AS TFNRELCONG,
            cast(SUM(CELTCHF_TFNRELCONGSUB) as dec(19,5)) AS TFNRELCONGSUB,
            cast(SUM(CELTCHH_THNRELCONG) as dec(19,5)) AS THNRELCONG,
            cast(SUM(CELTCHH_THNRELCONGSUB) as dec(19,5)) AS THNRELCONGSUB,
            cast(SUM(CELTCHF_TFTRALACC) as dec(19,5)) AS TFTRALACC,
            cast(SUM(CELTCHH_THTRALACC) as dec(19,5)) AS THTRALACC,
            cast(AVG(CELTCHF_TFNSCAN) as dec(19,5)) AS TFNSCAN,
            cast(AVG(CELTCHH_THNSCAN) as dec(19,5)) AS THNSCAN,
            cast(SUM(CLTCH_TAVAACC) as dec(19,5)) AS TAVAACC,
            cast(AVG(CLTCH_TAVASCAN) as dec(19,5)) AS TAVASCAN,
            cast(SUM(CLTCH_TNUCHCNT) as dec(19,5)) AS TNUCHCNT,
            cast(SUM(CELTCHF_TFNDROP) as dec(19,5)) AS TFNDROP,
            cast(SUM(CELTCHF_TFNDROPSUB) as dec(19,5)) AS TFNDROPSUB,
            cast(SUM(CELTCHH_THNDROP) as dec(19,5)) AS THNDROP,
            cast(SUM(CELTCHH_THNDROPSUB) as dec(19,5)) AS THNDROPSUB,
            cast(SUM(CLTCHDRF_TFDISSUL) as dec(19,5)) AS TFDISSUL,
            cast(SUM(CLTCHDRF_TFDISSULSUB) as dec(19,5)) AS TFDISSULSUB,
            cast(SUM(CLTCHDRH_THDISSUL) as dec(19,5)) AS THDISSUL,
            cast(SUM(CLTCHDRH_THDISSULSUB) as dec(19,5)) AS THDISSULSUB,
            cast(SUM(CLTCHDRF_TFDISSDL) as dec(19,5)) AS TFDISSDL,
            cast(SUM(CLTCHDRF_TFDISSDLSUB) as dec(19,5)) AS TFDISSDLSUB,
            cast(SUM(CLTCHDRH_THDISSDL) as dec(19,5)) AS THDISSDL,
            cast(SUM(CLTCHDRH_THDISSDLSUB) as dec(19,5)) AS THDISSDLSUB,
            cast(SUM(CLTCHDRF_TFDISSBL) as dec(19,5)) AS TFDISSBL,
            cast(SUM(CLTCHDRF_TFDISSBLSUB) as dec(19,5)) AS TFDISSBLSUB,
            cast(SUM(CLTCHDRH_THDISSBL) as dec(19,5)) AS THDISSBL,
            cast(SUM(CLTCHDRH_THDISSBLSUB) as dec(19,5)) AS THDISSBLSUB,
            cast(SUM(CLTCHDRF_TFDISQAUL) as dec(19,5)) AS TFDISQAUL,
            cast(SUM(CLTCHDRF_TFDISQAULSUB) as dec(19,5)) AS TFDISQAULSUB,
            cast(SUM(CLTCHDRH_THDISQAUL) as dec(19,5)) AS THDISQAUL,
            cast(SUM(CLTCHDRH_THDISQAULSUB) as dec(19,5)) AS THDISQAULSUB,
            cast(SUM(CLTCHDRF_TFDISQADL) as dec(19,5)) AS TFDISQADL,
            cast(SUM(CLTCHDRF_TFDISQADLSUB) as dec(19,5)) AS TFDISQADLSUB,
            cast(SUM(CLTCHDRH_THDISQADL) as dec(19,5)) AS THDISQADL,
            cast(SUM(CLTCHDRH_THDISQADLSUB) as dec(19,5)) AS THDISQADLSUB,
            cast(SUM(CLTCHDRF_TFDISQABL) as dec(19,5)) AS TFDISQABL,
            cast(SUM(CLTCHDRF_TFDISQABLSUB) as dec(19,5)) AS TFDISQABLSUB,
            cast(SUM(CLTCHDRH_THDISQABL) as dec(19,5)) AS THDISQABL,
            cast(SUM(CLTCHDRH_THDISQABLSUB) as dec(19,5)) AS THDISQABLSUB,
            cast(SUM(CLTCHDRF_TFSUDLOS) as dec(19,5)) AS TFSUDLOS,
            cast(SUM(CLTCHDRF_TFSUDLOSSUB) as dec(19,5)) AS TFSUDLOSSUB,
            cast(SUM(CLTCHDRH_THSUDLOS) as dec(19,5)) AS THSUDLOS,
            cast(SUM(CLTCHDRH_THSUDLOSSUB) as dec(19,5)) AS THSUDLOSSUB,
            cast(SUM(CLTCHDRF_TFDISTA) as dec(19,5)) AS TFDISTA,
            cast(SUM(CLTCHDRH_THDISTA) as dec(19,5)) AS THDISTA,
            cast(SUM(DOWNTIME_TDWNACC) as dec(19,5)) AS TDWNACC,
            cast(SUM(DOWNTIME_TDWNSCAN) as dec(19,5)) AS TDWNSCAN,
            cast(SUM(CELTCHF_TFMSESTB) as dec(19,5)) AS TFMSESTB,
            cast(SUM(CELTCHH_THMSESTB) as dec(19,5)) AS THMSESTB,
            cast(SUM(CELEVENTD_DISNORM) as dec(19,5)) AS DISNORM,
            cast(SUM(IDLEUTCHF_ITFUSIB1) as dec(19,5)) AS ITFUSIB1,
            cast(SUM(IDLEUTCHF_ITFUSIB2) as dec(19,5)) AS ITFUSIB2,
            cast(SUM(IDLEUTCHF_ITFUSIB3) as dec(19,5)) AS ITFUSIB3,
            cast(SUM(IDLEUTCHF_ITFUSIB4) as dec(19,5)) AS ITFUSIB4,
            cast(SUM(IDLEUTCHF_ITFUSIB5) as dec(19,5)) AS ITFUSIB5,
            cast(SUM(IDLEUTCHH_ITHUSIB1) as dec(19,5)) as ITHUSIB1,
            cast(SUM(IDLEUTCHH_ITHUSIB2) as dec(19,5)) as ITHUSIB2,
            cast(SUM(IDLEUTCHH_ITHUSIB3) as dec(19,5)) as ITHUSIB3,
            cast(SUM(IDLEUTCHH_ITHUSIB4) as dec(19,5)) as ITHUSIB4,
            cast(SUM(IDLEUTCHH_ITHUSIB5) as dec(19,5)) as ITHUSIB5,
            cast(SUM(CLRXQUAL_QUAL00DL) as dec(19,5)) as QUAL00DL,
            cast(SUM(CLRXQUAL_QUAL10DL) as dec(19,5)) as QUAL10DL,
            cast(SUM(CLRXQUAL_QUAL20DL) as dec(19,5)) as QUAL20DL,
            cast(SUM(CLRXQUAL_QUAL30DL) as dec(19,5)) as QUAL30DL,
            cast(SUM(CLRXQUAL_QUAL40DL) as dec(19,5)) as QUAL40DL,
            cast(SUM(CLRXQUAL_QUAL50DL) as dec(19,5)) as QUAL50DL,
            cast(SUM(CLRXQUAL_QUAL60DL) as dec(19,5)) as QUAL60DL,
            cast(SUM(CLRXQUAL_QUAL70DL) as dec(19,5)) as QUAL70DL,
            cast(SUM(CLRXQUAL_QUAL00UL) as dec(19,5)) as QUAL00UL,
            cast(SUM(CLRXQUAL_QUAL10UL) as dec(19,5)) as QUAL10UL,
            cast(SUM(CLRXQUAL_QUAL20UL) as dec(19,5)) as QUAL20UL,
            cast(SUM(CLRXQUAL_QUAL30UL) as dec(19,5)) as QUAL30UL,
            cast(SUM(CLRXQUAL_QUAL40UL) as dec(19,5)) as QUAL40UL,
            cast(SUM(CLRXQUAL_QUAL50UL) as dec(19,5)) as QUAL50UL,
            cast(SUM(CLRXQUAL_QUAL60UL) as dec(19,5)) as QUAL60UL,
            cast(SUM(CLRXQUAL_QUAL70UL) as dec(19,5)) as QUAL70UL,
            cast(SUM(CELTCHF_TFTRALSUB) as dec(19,5)) AS TFTRALSUB,
            cast(SUM(CELTCHH_THTRALSUB) as dec(19,5)) AS THTRALSUB,
            cast(SUM(CELLSQI_TSQIGOOD) as dec(19,5)) AS TSQIGOOD,
            cast(SUM(CELLSQI_TSQIGOODSUB) as dec(19,5)) AS TSQIGOODSUB,
            cast(SUM(CELLSQI_TSQIGOODAF) as dec(19,5)) AS TSQIGOODAF,
            cast(SUM(CELLSQI_TSQIGOODSUBAF) as dec(19,5)) AS TSQIGOODSUBAF,
            cast(SUM(CELLSQI_TSQIGOODAH) as dec(19,5)) AS TSQIGOODAH,
            cast(SUM(CELLSQI_TSQIGOODSUBAH) as dec(19,5)) AS TSQIGOODSUBAH,
            cast(SUM(CELLSQI_TSQIACCPT) as dec(19,5)) AS TSQIACCPT,
            cast(SUM(CELLSQI_TSQIACCPTSUB) as dec(19,5)) AS TSQIACCPTSUB,
            cast(SUM(CELLSQI_TSQIBAD) as dec(19,5)) AS TSQIBAD,
            cast(SUM(CELLSQI_TSQIBADSUB) as dec(19,5)) AS TSQIBADSUB,
            cast(SUM(CELLSQI_TSQIACCPTAF) as dec(19,5)) AS TSQIACCPTAF,
            cast(SUM(CELLSQI_TSQIACCPTSUBAF) as dec(19,5)) AS TSQIACCPTSUBAF,
            cast(SUM(CELLSQI_TSQIBADAF) as dec(19,5)) AS TSQIBADAF,
            cast(SUM(CELLSQI_TSQIBADSUBAF) as dec(19,5)) AS TSQIBADSUBAF,
            cast(SUM(CELLSQI_TSQIACCPTAH) as dec(19,5)) AS TSQIACCPTAH,
            cast(SUM(CELLSQI_TSQIACCPTSUBAH) as dec(19,5)) AS TSQIACCPTSUBAH,
            cast(SUM(CELLSQI_TSQIBADAH) as dec(19,5)) AS TSQIBADAH,
            cast(SUM(CELLSQI_TSQIBADSUBAH) as dec(19,5)) AS TSQIBADSUBAH,
            cast(SUM(CLSDCCH_CESTIMMASS) as dec(19,5)) AS CESTIMMASS,
            convert(float,sum(PERIOD_DURATION)) as CFAIL,
            (NULLIF((convert(float,sum(CELTCHF_TFTRALACC))/NULLIF(convert(float,sum(CELTCHF_TFNSCAN)),0)),0)+NULLIF((convert(float,sum(CELTCHH_THTRALACC))/NULLIF(convert(float,sum(CELTCHH_THNSCAN)),0)),0)) as C_TTRALACC,
            (NULLIF((convert(float,sum(CELTCHH_THTRALSUB))/NULLIF(convert(float,sum(CELTCHH_THNSCAN)),0)),0)) as C_TTRAL_HR_OL,
            (NULLIF((convert(float,sum(CELTCHF_TFTRALSUB))/NULLIF(convert(float,sum(CELTCHF_TFNSCAN)),0)),0)) as C_TTRAL_FR_OL,
            (NULLIF((convert(float,sum(CELTCHH_THTRALACC))/NULLIF(convert(float,sum(CELTCHH_THNSCAN)),0)),0)) as C_TTRALACC_HR,
            (NULLIF((convert(float,sum(CELTCHF_TFTRALACC))/NULLIF(convert(float,sum(CELTCHF_TFNSCAN)),0)),0)) as C_TTRALACC_FR,
            convert(float,sum(CLTCH_TAVAACC))/NULLIF(convert(float,sum(CLTCH_TAVASCAN)),0) as C_TAVA,
            COUNT(DC_E_BSS_CELL_CS_DAY.CELL_NAME) AS NUMCELL_2G

            from DC_E_BSS_CELL_CS_DAY
            Where DATE_ID = '%s' 

            group by BSC, CELL_NAME, DATE_ID) A

            LEFT JOIN

            (SELECT DISTINCT
            BSC, CELL_NAME, DATE_ID, 
            cast(SUM(CCCHLOAD_CSIMMASS) as dec(19,5)) AS CSIMMASS,
            cast(SUM(CCCHLOAD_REJCSIMMASS) as dec(19,5)) AS REJCSIMMASS,
            cast(SUM(CCCHLOAD_PSIMMASS) as dec(19,5)) AS PSIMMASS,
            cast(SUM(CCCHLOAD_REJPSIMMASS) as dec(19,5)) AS REJPSIMMASS,
            cast(SUM(CELLGPRS_ALLPDCHACC) as dec(19,5)) AS ALLPDCHACC,
            cast(SUM(CELLGPRS_ALLPDCHSCAN) as dec(19,5)) AS ALLPDCHSCAN,
            cast(SUM(CELLGPRS_ALLPDCHACTACC) as dec(19,5)) AS ALLPDCHACTACC,
            cast(SUM(CELLGPRS_PCHALLFAIL) as dec(19,5)) AS PCHALLFAIL,
            cast(SUM(CELLGPRS_PCHALLATT) as dec(19,5)) AS PCHALLATT,
            cast(SUM(CELLGPRS_DLTBFEST) as dec(19,5)) AS DLTBFEST,
            cast(SUM(CELLGPRS_FAILDLTBFEST) as dec(19,5)) AS FAILDLTBFEST,
            cast(SUM(CELLGPRS2_PSCHREQ) as dec(19,5)) AS PSCHREQ,
            cast(SUM(CELLGPRS2_PREJTFI) as dec(19,5)) AS PREJTFI,
            cast(SUM(CELLGPRS2_PREJOTH) as dec(19,5)) AS PREJOTH,
            cast(SUM(TRAFDLGPRS_TBFDLGPRS) as dec(19,5)) AS TBFDLGPRS,
            cast(SUM(TRAFDLGPRS_TBFDLEGPRS) as dec(19,5)) AS TBFDLEGPRS,
            cast(SUM(CELLGPRS2_LDISRR) as dec(19,5)) AS LDISRR,
            cast(SUM(CELLGPRS2_FLUDISC) as dec(19,5)) AS FLUDISC,
            cast(SUM(CELLGPRS2_LDISTFI) as dec(19,5)) AS LDISTFI,
            cast(SUM(CELLQOSG_DLTHP1GTHR) as dec(19,5)) AS DLTHP1GTHR,
            cast(SUM(CELLQOSG_DLTHP2GTHR) as dec(19,5)) AS DLTHP2GTHR,
            cast(SUM(CELLQOSG_DLTHP3GTHR) as dec(19,5)) AS DLTHP3GTHR,
            cast(SUM(CELLQOSG_DLBGGTHR) as dec(19,5)) AS DLBGGTHR,
            cast(SUM(CELLQOSG_DLTHP1GDATA) as dec(19,5)) AS DLTHP1GDATA,
            cast(SUM(CELLQOSG_DLTHP2GDATA) as dec(19,5)) AS DLTHP2GDATA,
            cast(SUM(CELLQOSG_DLTHP3GDATA) as dec(19,5)) AS DLTHP3GDATA,
            cast(SUM(CELLQOSG_DLBGGDATA) as dec(19,5)) AS DLBGGDATA,
            cast(SUM(CELLQOSEG_DLTHP1EGTHR) as dec(19,5)) AS DLTHP1EGTHR,
            cast(SUM(CELLQOSEG_DLTHP2EGTHR) as dec(19,5)) AS DLTHP2EGTHR,
            cast(SUM(CELLQOSEG_DLTHP3EGTHR) as dec(19,5)) AS DLTHP3EGTHR,
            cast(SUM(CELLQOSEG_DLBGEGTHR) as dec(19,5)) AS DLBGEGTHR,
            cast(SUM(CELLQOSEG_DLTHP1EGDATA) as dec(19,5)) AS DLTHP1EGDATA,
            cast(SUM(CELLQOSEG_DLTHP2EGDATA) as dec(19,5)) AS DLTHP2EGDATA,
            cast(SUM(CELLQOSEG_DLTHP3EGDATA) as dec(19,5)) AS DLTHP3EGDATA,
            cast(SUM(CELLQOSEG_DLBGEGDATA) as dec(19,5)) AS DLBGEGDATA,
            cast(SUM(CELLQOSG_ULTHP1GTHR) as dec(19,5)) AS ULTHP1GTHR,
            cast(SUM(CELLQOSG_ULTHP2GTHR) as dec(19,5)) AS ULTHP2GTHR,
            cast(SUM(CELLQOSG_ULTHP3GTHR) as dec(19,5)) AS ULTHP3GTHR,
            cast(SUM(CELLQOSG_ULBGGTHR) as dec(19,5)) AS ULBGGTHR,
            cast(SUM(CELLQOSG_ULTHP1GDATA) as dec(19,5)) AS ULTHP1GDATA,
            cast(SUM(CELLQOSG_ULTHP2GDATA) as dec(19,5)) AS ULTHP2GDATA,
            cast(SUM(CELLQOSG_ULTHP3GDATA) as dec(19,5)) AS ULTHP3GDATA,
            cast(SUM(CELLQOSG_ULBGGDATA) as dec(19,5)) AS ULBGGDATA,
            cast(SUM(CELLQOSEG_ULTHP1EGTHR) as dec(19,5)) AS ULTHP1EGTHR,
            cast(SUM(CELLQOSEG_ULTHP2EGTHR) as dec(19,5)) AS ULTHP2EGTHR,
            cast(SUM(CELLQOSEG_ULTHP3EGTHR) as dec(19,5)) AS ULTHP3EGTHR,
            cast(SUM(CELLQOSEG_ULBGEGTHR) as dec(19,5)) AS ULBGEGTHR,
            cast(SUM(CELLQOSEG_ULTHP1EGDATA) as dec(19,5)) AS ULTHP1EGDATA,
            cast(SUM(CELLQOSEG_ULTHP2EGDATA) as dec(19,5)) AS ULTHP2EGDATA,
            cast(SUM(CELLQOSEG_ULTHP3EGDATA) as dec(19,5)) AS ULTHP3EGDATA,
            cast(SUM(CELLQOSEG_ULBGEGDATA) as dec(19,5)) AS ULBGEGDATA,
            cast(SUM(CELLGPRS2_LDISOTH) as dec(19,5)) AS LDISOTH,
            cast(SUM(CELLGPRS_PREEMPTTBF) as dec(19,5)) AS PREEMPTTBF,
            cast(SUM(CELLGPRS_MOVECELLTBF) as dec(19,5)) AS MOVECELLTBF,
            cast(SUM(CELLGPRS2_IAULREL) as dec(19,5)) AS IAULREL,
            cast(SUM(CELLGPRS2_PREEMPTULREL) as dec(19,5)) AS PREEMPTULREL,
            cast(SUM(CELLGPRS2_OTHULREL) as dec(19,5)) AS OTHULREL,
            cast(SUM(CELLGPRS_FAILDLANSW) as dec(19,5)) AS FAILDLANSW,
            cast(SUM(CELLGPRS2_MSESTULTBF) as dec(19,5)) AS MSESTULTBF,
            cast(SUM(CELLGPRS_CS12DLACK) as dec(19,5)) AS CS12DLACK,
            cast(SUM(RLINKBITR_INT8BRGPRSTBF) as dec(19,5)) AS INT8BRGPRSTBF,
            cast(SUM(RLINKBITR_INT10BRGPRSTBF) as dec(19,5)) AS INT10BRGPRSTBF,
            cast(SUM(RLINKBITR_INT12BRGPRSTBF) as dec(19,5)) AS INT12BRGPRSTBF,
            cast(SUM(RLINKBITR_INT14BRGPRSTBF) as dec(19,5)) AS INT14BRGPRSTBF,
            cast(SUM(RLINKBITR_INT16BRGPRSTBF) as dec(19,5)) AS INT16BRGPRSTBF,
            cast(SUM(RLINKBITR_INT18BRGPRSTBF) as dec(19,5)) AS INT18BRGPRSTBF,
            cast(SUM(CELLGPRS_CS12ULACK) as dec(19,5)) AS CS12ULACK,
            cast(SUM(RLINKBITR_INT10BREGPRSTBF) as dec(19,5)) AS INT10BREGPRSTBF,
            cast(SUM(RLINKBITR_INT15BREGPRSTBF) as dec(19,5)) AS INT15BREGPRSTBF,
            cast(SUM(RLINKBITR_INT20BREGPRSTBF) as dec(19,5)) AS INT20BREGPRSTBF,
            cast(SUM(RLINKBITR_INT25BREGPRSTBF) as dec(19,5)) AS INT25BREGPRSTBF,
            cast(SUM(RLINKBITR_INT30BREGPRSTBF) as dec(19,5)) AS INT30BREGPRSTBF,
            cast(SUM(RLINKBITR_INT35BREGPRSTBF) as dec(19,5)) AS INT35BREGPRSTBF,
            cast(SUM(RLINKBITR_INT40BREGPRSTBF) as dec(19,5)) AS INT40BREGPRSTBF,
            cast(SUM(RLINKBITR_INT45BREGPRSTBF) as dec(19,5)) AS INT45BREGPRSTBF,
            cast(SUM(RLINKBITR_INT50BREGPRSTBF) as dec(19,5)) AS INT50BREGPRSTBF,
            cast(SUM(RLINKBITR_INT55BREGPRSTBF) as dec(19,5)) AS INT55BREGPRSTBF,
            cast(SUM(CELLGPRS_MC19ULACK) as dec(19,5)) AS MC19ULACK,
            cast(SUM(CELLGPRS_PREEMPTPDCH) as dec(19,5)) AS PREEMPTPDCH
            from DC_E_BSS_CELL_PS_DAY
            Where DATE_ID >= '%s' 

            group by BSC, CELL_NAME, DATE_ID) B

            ON A.CELL_NAME=B.CELL_NAME 
            AND A.DATE_ID=B.DATE_ID


            GROUP BY
            a.DATE_ID
            ,a.BSC
            ,a.CELL_NAME
            ORDER BY a.DATE_ID,a.CELL_NAME
                        """  %(q,q)
    
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
    path_daily = '/var/opt/common5/eniq/%s/%s'%(s_port, s_date2)
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
        df.to_csv("%s/daily_2g_cell_%s.csv" %(path_daily, s_date2), index=None)
        
        duration = datetime.now() - start_time
        print("    %s %s Processing done with duration: %s" %(s_port, s_date2, duration))
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
        s_port = dict_id['eniq']['enid']
        path_daily = '/var/opt/common5/eniq/%s/%s'%(s_port, s_date2)
        file = "%s/daily_2g_cell_%s.csv" %(path_daily, s_date2)
        
        if os.path.exists(file):
            df_new = pd.read_csv(file, delimiter=",", index_col=None, header='infer')
            if len(df_new) > 0:
                list_df.append(df_new)
    if len(list_df) > 0:
        df = pd.concat(list_df, sort=False)
        path_daily = '/var/opt/common5/eniq'
        file = '%s/daily_2g_cell_%s.csv'%(path_daily, s_date2)
        df.to_csv(file, index=None)

        #zip result
        zip_file = "%s/daily_2g_cell_%s.zip" %(path_daily, s_date2)
        zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
        zf.write(file,'daily_2g_cell_%s.csv'%s_date2)
        zf.close()
        os.remove(file)
        print('Result on %s'%zip_file)

        #Calculate KPI
        print('Calculate KPI....')
        list_column = ['DATE_ID', 'BSC', 'CELL_NAME', 'NUMCELL_2G', 'TCH_Traffic_', 'SDCCH_Traffic_', 'PAYLOAD_GB', 'T_AVAIL_RATE', 'T_DOWN', 'CSSR_2G', 'SDCCH_BLOCKING_Rate', 'SDCCH_DROP_RATE', 'T_CONG_RATE']
        df['T_AVAIL_RATE'] = 100.0 * df['C_TAVA'] / df['TNUCHCNT']
        df['T_DOWN'] = 100.0 * df['TDWNACC'] / df['TDWNSCAN']
        df['CSSR_2G'] = 100.0 * ((1.0 - df['CCONGS'] / df['CCALLS']) * (1.0 - (df['CNDROP'] - df['CNRELCONG']) / df['CMSESTAB']) * df['TCASSALL'] / df['TASSALL'])
        df['SDCCH_BLOCKING_Rate'] = 100.0 * df['CCONGS']/df['CCALLS']
        df['SDCCH_DROP_RATE'] = 100.0 * df['CNDROP']/df['CMSESTAB']
        df['T_CONG_RATE'] = 100.0 * (df['CNRELCONG'] + df['TFNRELCONG'] + df['THNRELCONG'] + df['TFNRELCONGSUB'] + df['THNRELCONGSUB'])/df['TASSALL']
        #df['T_DROP_RATE'] = 100.0 * (df['TFNDROP'] + df['THNDROP'] + df['TFNDROPSUB'] + df['THNDROPSUB'])/((df['TCASSALL'] + df['SUMIHOSUCC'] - df['SUMIABSUCC'] - df['SUMIAWSUCC']) - (df['SUMOHOSUCC'] - df['SUMOABSUCC'] - df['SUMOAWSUCC'] ))
        #df['HO_SUCCESS_RATE'] = 100.0 * (df['SUMOHOSUCC'] + df['SUMEOHOSUCC']) / (df['SUMOHOATT'] + df['SUMEOHOATT'])
        df['RxQual_DL_0_5'] = 100.0 * (df['QUAL00DL'] + df['QUAL10DL'] + df['QUAL20DL'] + df['QUAL30DL'] + df['QUAL40DL'] + df['QUAL50DL']) / (df['QUAL00DL'] + df['QUAL10DL'] + df['QUAL20DL'] + df['QUAL30DL'] + df['QUAL40DL'] + df['QUAL50DL'] + df['QUAL60DL'] + df['QUAL70DL'])
        df['RxQual_UL_0_5'] = 100.0 * (df['QUAL00UL'] + df['QUAL10UL'] + df['QUAL20UL'] + df['QUAL30UL'] + df['QUAL40UL'] + df['QUAL50UL']) / (df['QUAL00UL'] + df['QUAL10UL'] + df['QUAL20UL'] + df['QUAL30UL'] + df['QUAL40UL'] + df['QUAL50UL'] + df['QUAL60UL'] + df['QUAL70UL'])

        
        file = '%s/daily_2g_kpi_cell_%s.csv'%(path_daily, s_date2)
        df = df[list_column]
        df.to_csv(file, index=None)

        #zip result
        zip_file = "%s/daily_2g_kpi_cell_%s.zip" %(path_daily, s_date2)
        zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
        zf.write(file,'daily_2g_kpi_cell_%s.csv'%s_date2)
        zf.close()
        os.remove(file)
        print('Result KPI on %s'%zip_file)

def zipCellDaily(s_date2):
    print()
    path_daily = '/var/opt/common5/eniq'
    filename = 'daily_2g_cell_%s.csv'%(s_date2)
    zip_file = "%s/daily_2g_cell_%s.zip" %(path_daily, s_date2)

    

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

            
            dict_data = {}
            dict_data['eniq'] = eniq
            dict_data['date'] = s_date
            dict_data['date2'] = s_date2
            list_data.append(dict_data)

        get_daily_data(list_data)
        combineData(list_data)
        #zipCellDaily(s_date2)
        #file = '/var/opt/common5/eniq/daily_cell_%s.csv'%(s_date2)
