import pyodbc
import pandas as pd
import numpy as np 
import json
import re
from datetime import datetime, timedelta
import os
import sys
import multiprocessing
from dateutil import rrule
import warnings
import json
import lte_kpi
import time



param_dic = {
    "host"      : "150.236.203.228",
    "database"  : "pmt",
    "user"      : "pmt",
    "password"  : "PMT@2020#123"
}


def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = pg.connect(**params_dic)
    except (Exception, pg.DatabaseError) as error:
        print(error)
        sys.exit(1) 
    print("Connection successful")
    return conn


def postgresql_to_dataframe(conn, select_query, column_names):
    """
    Tranform a SELECT query into a pandas dataframe
    """
    cursor = conn.cursor()
    try:
        cursor.execute(select_query)
    except (Exception, pg.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1
    
    # Naturally we get a list of tupples
    tupples = cursor.fetchall()
    cursor.close()
    
    # We just need to turn it into a pandas dataframe
    df = pd.DataFrame(tupples, columns=column_names)
    return df


def uploadDatabaseDate(df, schema, tablename, dateCol, s_date):
    conn_str = "host={} dbname={} user={} password={}".format('localhost', 'pmt', 'pmt', 'PMT@2020#123')
    conn = pg.connect(conn_str)
    sql = '''delete from %s.%s where %s = '%s'
        ''' %(schema, tablename, dateCol, s_date)
    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
    except Exception as e:
        print("Error: %s"%e)
        pass

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

    print('Importing %s.%s complete on %s'%(schema, tablename, datetime.now()))

def uploadDatabase(df, schema, tablename):
    conn_str = "host={} dbname={} user={} password={}".format('localhost', 'pmt', 'pmt', 'PMT@2020#123')
    conn = pg.connect(conn_str)
    sql = '''delete from %s.%s
        ''' %(schema, tablename)
    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
    except Exception as e:
        print("Error: %s"%e)
        pass

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

    print('Importing %s.%s complete on %s'%(schema, tablename, datetime.now()))


def connectSybase(eniq, host, port, usr, pwd):
    conn = None
    conn = pyodbc.connect('DRIVER=freetds;SERVER=%s;PORT=%s;UID=%s;PWD=%s;TDS _Version=5.0;' %(host, port, usr, pwd))

    return conn



def get_sql(q, enid):
    #=============================================
    # q = date (yyyymmdd)
    # enid : 
    #   LRAN : 141
    #   CENTRAL : new
    #   WRAN : en4
    #============================================


    query1 = '''select
            date_id
            ,erbs
            ,sum(datacoverage) as datacoverage
            ,sectorcarrier
            , sum(pmbranchdeltasinrdistr0_0) as pmbranchdeltasinrdistr0_0
            , sum(pmbranchdeltasinrdistr0_1) as pmbranchdeltasinrdistr0_1
            , sum(pmbranchdeltasinrdistr0_2) as pmbranchdeltasinrdistr0_2
            , sum(pmbranchdeltasinrdistr0_3) as pmbranchdeltasinrdistr0_3
            , sum(pmbranchdeltasinrdistr0_4) as pmbranchdeltasinrdistr0_4
            , sum(pmbranchdeltasinrdistr0_5) as pmbranchdeltasinrdistr0_5
            , sum(pmbranchdeltasinrdistr0_6) as pmbranchdeltasinrdistr0_6
            , sum(pmbranchdeltasinrdistr0_7) as pmbranchdeltasinrdistr0_7
            , sum(pmbranchdeltasinrdistr0_8) as pmbranchdeltasinrdistr0_8
            , sum(pmbranchdeltasinrdistr0_9) as pmbranchdeltasinrdistr0_9
            , sum(pmbranchdeltasinrdistr0_10) as pmbranchdeltasinrdistr0_10
            , sum(pmbranchdeltasinrdistr0_11) as pmbranchdeltasinrdistr0_11
            , sum(pmbranchdeltasinrdistr0_12) as pmbranchdeltasinrdistr0_12
            , sum(pmbranchdeltasinrdistr0_13) as pmbranchdeltasinrdistr0_13
            , sum(pmbranchdeltasinrdistr0_14) as pmbranchdeltasinrdistr0_14
            , sum(pmbranchdeltasinrdistr0_15) as pmbranchdeltasinrdistr0_15
            , sum(pmbranchdeltasinrdistr0_16) as pmbranchdeltasinrdistr0_16
            , sum(pmbranchdeltasinrdistr0_17) as pmbranchdeltasinrdistr0_17
            , sum(pmbranchdeltasinrdistr0_18) as pmbranchdeltasinrdistr0_18
            , sum(pmbranchdeltasinrdistr0_19) as pmbranchdeltasinrdistr0_19
            , sum(pmbranchdeltasinrdistr0_20) as pmbranchdeltasinrdistr0_20
            , sum(pmbranchdeltasinrdistr0_21) as pmbranchdeltasinrdistr0_21
            , sum(pmbranchdeltasinrdistr0_22) as pmbranchdeltasinrdistr0_22
            , sum(pmbranchdeltasinrdistr0_23) as pmbranchdeltasinrdistr0_23
            , sum(pmbranchdeltasinrdistr0_24) as pmbranchdeltasinrdistr0_24
            , sum(pmbranchdeltasinrdistr0_25) as pmbranchdeltasinrdistr0_25
            , sum(pmbranchdeltasinrdistr0_26) as pmbranchdeltasinrdistr0_26
            , sum(pmbranchdeltasinrdistr0_27) as pmbranchdeltasinrdistr0_27
            , sum(pmbranchdeltasinrdistr0_28) as pmbranchdeltasinrdistr0_28
            , sum(pmbranchdeltasinrdistr0_29) as pmbranchdeltasinrdistr0_29
            , sum(pmbranchdeltasinrdistr0_30) as pmbranchdeltasinrdistr0_30
            , sum(pmbranchdeltasinrdistr0_31) as pmbranchdeltasinrdistr0_31
            , sum(pmbranchdeltasinrdistr0_32) as pmbranchdeltasinrdistr0_32
            , sum(pmbranchdeltasinrdistr0_33) as pmbranchdeltasinrdistr0_33
            , sum(pmbranchdeltasinrdistr0_34) as pmbranchdeltasinrdistr0_34
            , sum(pmbranchdeltasinrdistr0_35) as pmbranchdeltasinrdistr0_35
            , sum(pmbranchdeltasinrdistr0_36) as pmbranchdeltasinrdistr0_36
            , sum(pmbranchdeltasinrdistr0_37) as pmbranchdeltasinrdistr0_37
            , sum(pmbranchdeltasinrdistr0_38) as pmbranchdeltasinrdistr0_38
            , sum(pmbranchdeltasinrdistr0_39) as pmbranchdeltasinrdistr0_39
            , sum(pmbranchdeltasinrdistr0_40) as pmbranchdeltasinrdistr0_40
            , sum(pmbranchdeltasinrdistr0_41) as pmbranchdeltasinrdistr0_41
            , sum(pmbranchdeltasinrdistr0_42) as pmbranchdeltasinrdistr0_42
            , sum(pmbranchdeltasinrdistr0_43) as pmbranchdeltasinrdistr0_43
            , sum(pmbranchdeltasinrdistr0_44) as pmbranchdeltasinrdistr0_44
            , sum(pmbranchdeltasinrdistr0_45) as pmbranchdeltasinrdistr0_45
            , sum(pmbranchdeltasinrdistr0_46) as pmbranchdeltasinrdistr0_46
            , sum(pmbranchdeltasinrdistr0_47) as pmbranchdeltasinrdistr0_47
            , sum(pmbranchdeltasinrdistr0_48) as pmbranchdeltasinrdistr0_48
            , sum(pmbranchdeltasinrdistr0_49) as pmbranchdeltasinrdistr0_49
            , sum(pmbranchdeltasinrdistr0_50) as pmbranchdeltasinrdistr0_50
            , sum(pmbranchdeltasinrdistr0_51) as pmbranchdeltasinrdistr0_51
            , sum(pmbranchdeltasinrdistr0_52) as pmbranchdeltasinrdistr0_52
            , sum(pmbranchdeltasinrdistr0_53) as pmbranchdeltasinrdistr0_53
            , sum(pmbranchdeltasinrdistr0_54) as pmbranchdeltasinrdistr0_54
            , sum(pmbranchdeltasinrdistr0_55) as pmbranchdeltasinrdistr0_55
            , sum(pmbranchdeltasinrdistr0_56) as pmbranchdeltasinrdistr0_56
            , sum(pmbranchdeltasinrdistr0_57) as pmbranchdeltasinrdistr0_57
            , sum(pmbranchdeltasinrdistr0_58) as pmbranchdeltasinrdistr0_58
            , sum(pmbranchdeltasinrdistr0_59) as pmbranchdeltasinrdistr0_59
        from (
        select
            date_id
            , DCVECTOR_INDEX as dcvector_index
            , ERBS as erbs
            , DATACOVERAGE as datacoverage
            , SectorCarrier as sectorcarrier
            , case when DCVECTOR_INDEX = 0 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_0
            , case when DCVECTOR_INDEX = 1 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_1
            , case when DCVECTOR_INDEX = 2 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_2
            , case when DCVECTOR_INDEX = 3 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_3
            , case when DCVECTOR_INDEX = 4 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_4
            , case when DCVECTOR_INDEX = 5 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_5
            , case when DCVECTOR_INDEX = 6 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_6
            , case when DCVECTOR_INDEX = 7 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_7
            , case when DCVECTOR_INDEX = 8 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_8
            , case when DCVECTOR_INDEX = 9 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_9
            , case when DCVECTOR_INDEX = 10 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_10
            , case when DCVECTOR_INDEX = 11 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_11
            , case when DCVECTOR_INDEX = 12 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_12
            , case when DCVECTOR_INDEX = 13 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_13
            , case when DCVECTOR_INDEX = 14 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_14
            , case when DCVECTOR_INDEX = 15 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_15
            , case when DCVECTOR_INDEX = 16 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_16
            , case when DCVECTOR_INDEX = 17 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_17
            , case when DCVECTOR_INDEX = 18 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_18
            , case when DCVECTOR_INDEX = 19 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_19
            , case when DCVECTOR_INDEX = 20 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_20
            , case when DCVECTOR_INDEX = 21 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_21
            , case when DCVECTOR_INDEX = 22 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_22
            , case when DCVECTOR_INDEX = 23 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_23
            , case when DCVECTOR_INDEX = 24 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_24
            , case when DCVECTOR_INDEX = 25 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_25
            , case when DCVECTOR_INDEX = 26 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_26
            , case when DCVECTOR_INDEX = 27 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_27
            , case when DCVECTOR_INDEX = 28 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_28
            , case when DCVECTOR_INDEX = 29 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_29
            , case when DCVECTOR_INDEX = 30 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_30
            , case when DCVECTOR_INDEX = 31 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_31
            , case when DCVECTOR_INDEX = 32 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_32
            , case when DCVECTOR_INDEX = 33 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_33
            , case when DCVECTOR_INDEX = 34 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_34
            , case when DCVECTOR_INDEX = 35 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_35
            , case when DCVECTOR_INDEX = 36 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_36
            , case when DCVECTOR_INDEX = 37 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_37
            , case when DCVECTOR_INDEX = 38 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_38
            , case when DCVECTOR_INDEX = 39 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_39
            , case when DCVECTOR_INDEX = 40 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_40
            , case when DCVECTOR_INDEX = 41 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_41
            , case when DCVECTOR_INDEX = 42 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_42
            , case when DCVECTOR_INDEX = 43 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_43
            , case when DCVECTOR_INDEX = 44 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_44
            , case when DCVECTOR_INDEX = 45 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_45
            , case when DCVECTOR_INDEX = 46 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_46
            , case when DCVECTOR_INDEX = 47 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_47
            , case when DCVECTOR_INDEX = 48 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_48
            , case when DCVECTOR_INDEX = 49 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_49
            , case when DCVECTOR_INDEX = 50 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_50
            , case when DCVECTOR_INDEX = 51 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_51
            , case when DCVECTOR_INDEX = 52 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_52
            , case when DCVECTOR_INDEX = 53 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_53
            , case when DCVECTOR_INDEX = 54 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_54
            , case when DCVECTOR_INDEX = 55 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_55
            , case when DCVECTOR_INDEX = 56 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_56
            , case when DCVECTOR_INDEX = 57 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_57
            , case when DCVECTOR_INDEX = 58 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_58
            , case when DCVECTOR_INDEX = 59 then pmBranchDeltaSinrDistr0 end  as pmbranchdeltasinrdistr0_59
        from dwhdb.dcpublic.DC_E_ERBS_SECTORCARRIER_V_DAY where 
            date_id = '%s'
            and dcvector_index <= 59
        ) a group by
            date_id
            ,erbs
            ,sectorcarrier
            ''' %(q)
    
    query2 = '''select
            date_id
            ,erbs
            ,sectorcarrier
            , sum(pmbranchdeltasinrdistr1_0) as pmbranchdeltasinrdistr1_0
            , sum(pmbranchdeltasinrdistr1_1) as pmbranchdeltasinrdistr1_1
            , sum(pmbranchdeltasinrdistr1_2) as pmbranchdeltasinrdistr1_2
            , sum(pmbranchdeltasinrdistr1_3) as pmbranchdeltasinrdistr1_3
            , sum(pmbranchdeltasinrdistr1_4) as pmbranchdeltasinrdistr1_4
            , sum(pmbranchdeltasinrdistr1_5) as pmbranchdeltasinrdistr1_5
            , sum(pmbranchdeltasinrdistr1_6) as pmbranchdeltasinrdistr1_6
            , sum(pmbranchdeltasinrdistr1_7) as pmbranchdeltasinrdistr1_7
            , sum(pmbranchdeltasinrdistr1_8) as pmbranchdeltasinrdistr1_8
            , sum(pmbranchdeltasinrdistr1_9) as pmbranchdeltasinrdistr1_9
            , sum(pmbranchdeltasinrdistr1_10) as pmbranchdeltasinrdistr1_10
            , sum(pmbranchdeltasinrdistr1_11) as pmbranchdeltasinrdistr1_11
            , sum(pmbranchdeltasinrdistr1_12) as pmbranchdeltasinrdistr1_12
            , sum(pmbranchdeltasinrdistr1_13) as pmbranchdeltasinrdistr1_13
            , sum(pmbranchdeltasinrdistr1_14) as pmbranchdeltasinrdistr1_14
            , sum(pmbranchdeltasinrdistr1_15) as pmbranchdeltasinrdistr1_15
            , sum(pmbranchdeltasinrdistr1_16) as pmbranchdeltasinrdistr1_16
            , sum(pmbranchdeltasinrdistr1_17) as pmbranchdeltasinrdistr1_17
            , sum(pmbranchdeltasinrdistr1_18) as pmbranchdeltasinrdistr1_18
            , sum(pmbranchdeltasinrdistr1_19) as pmbranchdeltasinrdistr1_19
            , sum(pmbranchdeltasinrdistr1_20) as pmbranchdeltasinrdistr1_20
            , sum(pmbranchdeltasinrdistr1_21) as pmbranchdeltasinrdistr1_21
            , sum(pmbranchdeltasinrdistr1_22) as pmbranchdeltasinrdistr1_22
            , sum(pmbranchdeltasinrdistr1_23) as pmbranchdeltasinrdistr1_23
            , sum(pmbranchdeltasinrdistr1_24) as pmbranchdeltasinrdistr1_24
            , sum(pmbranchdeltasinrdistr1_25) as pmbranchdeltasinrdistr1_25
            , sum(pmbranchdeltasinrdistr1_26) as pmbranchdeltasinrdistr1_26
            , sum(pmbranchdeltasinrdistr1_27) as pmbranchdeltasinrdistr1_27
            , sum(pmbranchdeltasinrdistr1_28) as pmbranchdeltasinrdistr1_28
            , sum(pmbranchdeltasinrdistr1_29) as pmbranchdeltasinrdistr1_29
            , sum(pmbranchdeltasinrdistr1_30) as pmbranchdeltasinrdistr1_30
            , sum(pmbranchdeltasinrdistr1_31) as pmbranchdeltasinrdistr1_31
            , sum(pmbranchdeltasinrdistr1_32) as pmbranchdeltasinrdistr1_32
            , sum(pmbranchdeltasinrdistr1_33) as pmbranchdeltasinrdistr1_33
            , sum(pmbranchdeltasinrdistr1_34) as pmbranchdeltasinrdistr1_34
            , sum(pmbranchdeltasinrdistr1_35) as pmbranchdeltasinrdistr1_35
            , sum(pmbranchdeltasinrdistr1_36) as pmbranchdeltasinrdistr1_36
            , sum(pmbranchdeltasinrdistr1_37) as pmbranchdeltasinrdistr1_37
            , sum(pmbranchdeltasinrdistr1_38) as pmbranchdeltasinrdistr1_38
            , sum(pmbranchdeltasinrdistr1_39) as pmbranchdeltasinrdistr1_39
            , sum(pmbranchdeltasinrdistr1_40) as pmbranchdeltasinrdistr1_40
            , sum(pmbranchdeltasinrdistr1_41) as pmbranchdeltasinrdistr1_41
            , sum(pmbranchdeltasinrdistr1_42) as pmbranchdeltasinrdistr1_42
            , sum(pmbranchdeltasinrdistr1_43) as pmbranchdeltasinrdistr1_43
            , sum(pmbranchdeltasinrdistr1_44) as pmbranchdeltasinrdistr1_44
            , sum(pmbranchdeltasinrdistr1_45) as pmbranchdeltasinrdistr1_45
            , sum(pmbranchdeltasinrdistr1_46) as pmbranchdeltasinrdistr1_46
            , sum(pmbranchdeltasinrdistr1_47) as pmbranchdeltasinrdistr1_47
            , sum(pmbranchdeltasinrdistr1_48) as pmbranchdeltasinrdistr1_48
            , sum(pmbranchdeltasinrdistr1_49) as pmbranchdeltasinrdistr1_49
            , sum(pmbranchdeltasinrdistr1_50) as pmbranchdeltasinrdistr1_50
            , sum(pmbranchdeltasinrdistr1_51) as pmbranchdeltasinrdistr1_51
            , sum(pmbranchdeltasinrdistr1_52) as pmbranchdeltasinrdistr1_52
            , sum(pmbranchdeltasinrdistr1_53) as pmbranchdeltasinrdistr1_53
            , sum(pmbranchdeltasinrdistr1_54) as pmbranchdeltasinrdistr1_54
            , sum(pmbranchdeltasinrdistr1_55) as pmbranchdeltasinrdistr1_55
            , sum(pmbranchdeltasinrdistr1_56) as pmbranchdeltasinrdistr1_56
            , sum(pmbranchdeltasinrdistr1_57) as pmbranchdeltasinrdistr1_57
            , sum(pmbranchdeltasinrdistr1_58) as pmbranchdeltasinrdistr1_58
            , sum(pmbranchdeltasinrdistr1_59) as pmbranchdeltasinrdistr1_59
        from (
        select
            date_id
            , DCVECTOR_INDEX as dcvector_index
            , ERBS as erbs
            , SectorCarrier as sectorcarrier
            , case when DCVECTOR_INDEX = 0 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_0
            , case when DCVECTOR_INDEX = 1 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_1
            , case when DCVECTOR_INDEX = 2 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_2
            , case when DCVECTOR_INDEX = 3 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_3
            , case when DCVECTOR_INDEX = 4 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_4
            , case when DCVECTOR_INDEX = 5 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_5
            , case when DCVECTOR_INDEX = 6 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_6
            , case when DCVECTOR_INDEX = 7 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_7
            , case when DCVECTOR_INDEX = 8 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_8
            , case when DCVECTOR_INDEX = 9 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_9
            , case when DCVECTOR_INDEX = 10 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_10
            , case when DCVECTOR_INDEX = 11 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_11
            , case when DCVECTOR_INDEX = 12 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_12
            , case when DCVECTOR_INDEX = 13 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_13
            , case when DCVECTOR_INDEX = 14 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_14
            , case when DCVECTOR_INDEX = 15 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_15
            , case when DCVECTOR_INDEX = 16 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_16
            , case when DCVECTOR_INDEX = 17 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_17
            , case when DCVECTOR_INDEX = 18 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_18
            , case when DCVECTOR_INDEX = 19 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_19
            , case when DCVECTOR_INDEX = 20 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_20
            , case when DCVECTOR_INDEX = 21 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_21
            , case when DCVECTOR_INDEX = 22 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_22
            , case when DCVECTOR_INDEX = 23 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_23
            , case when DCVECTOR_INDEX = 24 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_24
            , case when DCVECTOR_INDEX = 25 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_25
            , case when DCVECTOR_INDEX = 26 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_26
            , case when DCVECTOR_INDEX = 27 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_27
            , case when DCVECTOR_INDEX = 28 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_28
            , case when DCVECTOR_INDEX = 29 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_29
            , case when DCVECTOR_INDEX = 30 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_30
            , case when DCVECTOR_INDEX = 31 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_31
            , case when DCVECTOR_INDEX = 32 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_32
            , case when DCVECTOR_INDEX = 33 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_33
            , case when DCVECTOR_INDEX = 34 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_34
            , case when DCVECTOR_INDEX = 35 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_35
            , case when DCVECTOR_INDEX = 36 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_36
            , case when DCVECTOR_INDEX = 37 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_37
            , case when DCVECTOR_INDEX = 38 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_38
            , case when DCVECTOR_INDEX = 39 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_39
            , case when DCVECTOR_INDEX = 40 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_40
            , case when DCVECTOR_INDEX = 41 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_41
            , case when DCVECTOR_INDEX = 42 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_42
            , case when DCVECTOR_INDEX = 43 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_43
            , case when DCVECTOR_INDEX = 44 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_44
            , case when DCVECTOR_INDEX = 45 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_45
            , case when DCVECTOR_INDEX = 46 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_46
            , case when DCVECTOR_INDEX = 47 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_47
            , case when DCVECTOR_INDEX = 48 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_48
            , case when DCVECTOR_INDEX = 49 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_49
            , case when DCVECTOR_INDEX = 50 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_50
            , case when DCVECTOR_INDEX = 51 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_51
            , case when DCVECTOR_INDEX = 52 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_52
            , case when DCVECTOR_INDEX = 53 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_53
            , case when DCVECTOR_INDEX = 54 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_54
            , case when DCVECTOR_INDEX = 55 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_55
            , case when DCVECTOR_INDEX = 56 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_56
            , case when DCVECTOR_INDEX = 57 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_57
            , case when DCVECTOR_INDEX = 58 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_58
            , case when DCVECTOR_INDEX = 59 then pmBranchDeltaSinrDistr1 end  as pmbranchdeltasinrdistr1_59
        from dwhdb.dcpublic.DC_E_ERBS_SECTORCARRIER_V_DAY where 
            date_id = '%s'
            and dcvector_index <= 59
        ) a group by
            date_id
            ,erbs
            ,sectorcarrier
            ''' %(q)
    
    query3 = '''select
            date_id
            ,erbs
            ,sectorcarrier
            , sum(pmbranchdeltasinrdistr2_0) as pmbranchdeltasinrdistr2_0
            , sum(pmbranchdeltasinrdistr2_1) as pmbranchdeltasinrdistr2_1
            , sum(pmbranchdeltasinrdistr2_2) as pmbranchdeltasinrdistr2_2
            , sum(pmbranchdeltasinrdistr2_3) as pmbranchdeltasinrdistr2_3
            , sum(pmbranchdeltasinrdistr2_4) as pmbranchdeltasinrdistr2_4
            , sum(pmbranchdeltasinrdistr2_5) as pmbranchdeltasinrdistr2_5
            , sum(pmbranchdeltasinrdistr2_6) as pmbranchdeltasinrdistr2_6
            , sum(pmbranchdeltasinrdistr2_7) as pmbranchdeltasinrdistr2_7
            , sum(pmbranchdeltasinrdistr2_8) as pmbranchdeltasinrdistr2_8
            , sum(pmbranchdeltasinrdistr2_9) as pmbranchdeltasinrdistr2_9
            , sum(pmbranchdeltasinrdistr2_10) as pmbranchdeltasinrdistr2_10
            , sum(pmbranchdeltasinrdistr2_11) as pmbranchdeltasinrdistr2_11
            , sum(pmbranchdeltasinrdistr2_12) as pmbranchdeltasinrdistr2_12
            , sum(pmbranchdeltasinrdistr2_13) as pmbranchdeltasinrdistr2_13
            , sum(pmbranchdeltasinrdistr2_14) as pmbranchdeltasinrdistr2_14
            , sum(pmbranchdeltasinrdistr2_15) as pmbranchdeltasinrdistr2_15
            , sum(pmbranchdeltasinrdistr2_16) as pmbranchdeltasinrdistr2_16
            , sum(pmbranchdeltasinrdistr2_17) as pmbranchdeltasinrdistr2_17
            , sum(pmbranchdeltasinrdistr2_18) as pmbranchdeltasinrdistr2_18
            , sum(pmbranchdeltasinrdistr2_19) as pmbranchdeltasinrdistr2_19
            , sum(pmbranchdeltasinrdistr2_20) as pmbranchdeltasinrdistr2_20
            , sum(pmbranchdeltasinrdistr2_21) as pmbranchdeltasinrdistr2_21
            , sum(pmbranchdeltasinrdistr2_22) as pmbranchdeltasinrdistr2_22
            , sum(pmbranchdeltasinrdistr2_23) as pmbranchdeltasinrdistr2_23
            , sum(pmbranchdeltasinrdistr2_24) as pmbranchdeltasinrdistr2_24
            , sum(pmbranchdeltasinrdistr2_25) as pmbranchdeltasinrdistr2_25
            , sum(pmbranchdeltasinrdistr2_26) as pmbranchdeltasinrdistr2_26
            , sum(pmbranchdeltasinrdistr2_27) as pmbranchdeltasinrdistr2_27
            , sum(pmbranchdeltasinrdistr2_28) as pmbranchdeltasinrdistr2_28
            , sum(pmbranchdeltasinrdistr2_29) as pmbranchdeltasinrdistr2_29
            , sum(pmbranchdeltasinrdistr2_30) as pmbranchdeltasinrdistr2_30
            , sum(pmbranchdeltasinrdistr2_31) as pmbranchdeltasinrdistr2_31
            , sum(pmbranchdeltasinrdistr2_32) as pmbranchdeltasinrdistr2_32
            , sum(pmbranchdeltasinrdistr2_33) as pmbranchdeltasinrdistr2_33
            , sum(pmbranchdeltasinrdistr2_34) as pmbranchdeltasinrdistr2_34
            , sum(pmbranchdeltasinrdistr2_35) as pmbranchdeltasinrdistr2_35
            , sum(pmbranchdeltasinrdistr2_36) as pmbranchdeltasinrdistr2_36
            , sum(pmbranchdeltasinrdistr2_37) as pmbranchdeltasinrdistr2_37
            , sum(pmbranchdeltasinrdistr2_38) as pmbranchdeltasinrdistr2_38
            , sum(pmbranchdeltasinrdistr2_39) as pmbranchdeltasinrdistr2_39
            , sum(pmbranchdeltasinrdistr2_40) as pmbranchdeltasinrdistr2_40
            , sum(pmbranchdeltasinrdistr2_41) as pmbranchdeltasinrdistr2_41
            , sum(pmbranchdeltasinrdistr2_42) as pmbranchdeltasinrdistr2_42
            , sum(pmbranchdeltasinrdistr2_43) as pmbranchdeltasinrdistr2_43
            , sum(pmbranchdeltasinrdistr2_44) as pmbranchdeltasinrdistr2_44
            , sum(pmbranchdeltasinrdistr2_45) as pmbranchdeltasinrdistr2_45
            , sum(pmbranchdeltasinrdistr2_46) as pmbranchdeltasinrdistr2_46
            , sum(pmbranchdeltasinrdistr2_47) as pmbranchdeltasinrdistr2_47
            , sum(pmbranchdeltasinrdistr2_48) as pmbranchdeltasinrdistr2_48
            , sum(pmbranchdeltasinrdistr2_49) as pmbranchdeltasinrdistr2_49
            , sum(pmbranchdeltasinrdistr2_50) as pmbranchdeltasinrdistr2_50
            , sum(pmbranchdeltasinrdistr2_51) as pmbranchdeltasinrdistr2_51
            , sum(pmbranchdeltasinrdistr2_52) as pmbranchdeltasinrdistr2_52
            , sum(pmbranchdeltasinrdistr2_53) as pmbranchdeltasinrdistr2_53
            , sum(pmbranchdeltasinrdistr2_54) as pmbranchdeltasinrdistr2_54
            , sum(pmbranchdeltasinrdistr2_55) as pmbranchdeltasinrdistr2_55
            , sum(pmbranchdeltasinrdistr2_56) as pmbranchdeltasinrdistr2_56
            , sum(pmbranchdeltasinrdistr2_57) as pmbranchdeltasinrdistr2_57
            , sum(pmbranchdeltasinrdistr2_58) as pmbranchdeltasinrdistr2_58
            , sum(pmbranchdeltasinrdistr2_59) as pmbranchdeltasinrdistr2_59
        from (
        select
            date_id
            , DCVECTOR_INDEX as dcvector_index
            , ERBS as erbs
            , SectorCarrier as sectorcarrier
            , case when DCVECTOR_INDEX = 0 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_0
            , case when DCVECTOR_INDEX = 1 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_1
            , case when DCVECTOR_INDEX = 2 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_2
            , case when DCVECTOR_INDEX = 3 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_3
            , case when DCVECTOR_INDEX = 4 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_4
            , case when DCVECTOR_INDEX = 5 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_5
            , case when DCVECTOR_INDEX = 6 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_6
            , case when DCVECTOR_INDEX = 7 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_7
            , case when DCVECTOR_INDEX = 8 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_8
            , case when DCVECTOR_INDEX = 9 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_9
            , case when DCVECTOR_INDEX = 10 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_10
            , case when DCVECTOR_INDEX = 11 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_11
            , case when DCVECTOR_INDEX = 12 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_12
            , case when DCVECTOR_INDEX = 13 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_13
            , case when DCVECTOR_INDEX = 14 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_14
            , case when DCVECTOR_INDEX = 15 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_15
            , case when DCVECTOR_INDEX = 16 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_16
            , case when DCVECTOR_INDEX = 17 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_17
            , case when DCVECTOR_INDEX = 18 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_18
            , case when DCVECTOR_INDEX = 19 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_19
            , case when DCVECTOR_INDEX = 20 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_20
            , case when DCVECTOR_INDEX = 21 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_21
            , case when DCVECTOR_INDEX = 22 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_22
            , case when DCVECTOR_INDEX = 23 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_23
            , case when DCVECTOR_INDEX = 24 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_24
            , case when DCVECTOR_INDEX = 25 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_25
            , case when DCVECTOR_INDEX = 26 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_26
            , case when DCVECTOR_INDEX = 27 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_27
            , case when DCVECTOR_INDEX = 28 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_28
            , case when DCVECTOR_INDEX = 29 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_29
            , case when DCVECTOR_INDEX = 30 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_30
            , case when DCVECTOR_INDEX = 31 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_31
            , case when DCVECTOR_INDEX = 32 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_32
            , case when DCVECTOR_INDEX = 33 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_33
            , case when DCVECTOR_INDEX = 34 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_34
            , case when DCVECTOR_INDEX = 35 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_35
            , case when DCVECTOR_INDEX = 36 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_36
            , case when DCVECTOR_INDEX = 37 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_37
            , case when DCVECTOR_INDEX = 38 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_38
            , case when DCVECTOR_INDEX = 39 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_39
            , case when DCVECTOR_INDEX = 40 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_40
            , case when DCVECTOR_INDEX = 41 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_41
            , case when DCVECTOR_INDEX = 42 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_42
            , case when DCVECTOR_INDEX = 43 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_43
            , case when DCVECTOR_INDEX = 44 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_44
            , case when DCVECTOR_INDEX = 45 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_45
            , case when DCVECTOR_INDEX = 46 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_46
            , case when DCVECTOR_INDEX = 47 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_47
            , case when DCVECTOR_INDEX = 48 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_48
            , case when DCVECTOR_INDEX = 49 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_49
            , case when DCVECTOR_INDEX = 50 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_50
            , case when DCVECTOR_INDEX = 51 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_51
            , case when DCVECTOR_INDEX = 52 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_52
            , case when DCVECTOR_INDEX = 53 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_53
            , case when DCVECTOR_INDEX = 54 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_54
            , case when DCVECTOR_INDEX = 55 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_55
            , case when DCVECTOR_INDEX = 56 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_56
            , case when DCVECTOR_INDEX = 57 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_57
            , case when DCVECTOR_INDEX = 58 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_58
            , case when DCVECTOR_INDEX = 59 then pmBranchDeltaSinrDistr2 end  as pmbranchdeltasinrdistr2_59
        from dwhdb.dcpublic.DC_E_ERBS_SECTORCARRIER_V_DAY where 
            date_id = '%s'
            and dcvector_index <= 59
        ) a group by
            date_id
            ,erbs
            ,sectorcarrier
            ''' %(q)
        
    return query1, query2, query3


def get_data(dict_id):
    start_time = datetime.now()

    s_date = dict_id['date']
    s_date2 = dict_id['date2']
    s_port = dict_id['eniq']['enid']
    dict_conn = dict_id['eniq']
    path_daily = dict_id['path']

    
    conn = False
    query1, query2, query3 = get_sql(s_date2, s_port)

    try:
        conn = pyodbc.connect('DRIVER=freetds;SERVER=%s;PORT=%s;UID=%s;PWD=%s;TDS _Version=5.0;' %(dict_conn['host'], dict_conn['port'], dict_conn['user'], dict_conn['passwd']))

        sql_query = pd.read_sql_query(query1,conn) 
        df = pd.DataFrame(sql_query)
        lte_kpi.saveResult(df, 'dc_e_erbs_sectorcarrier_v_day', s_date2, path_daily)

        sql_query = pd.read_sql_query(query2,conn) 
        df = pd.DataFrame(sql_query)
        lte_kpi.saveResult(df, 'dc_e_erbs_sectorcarrier_v_day2', s_date2, path_daily)

        sql_query = pd.read_sql_query(query3,conn) 
        df = pd.DataFrame(sql_query)
        lte_kpi.saveResult(df, 'dc_e_erbs_sectorcarrier_v_day3', s_date2, path_daily)
        
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
    conn = None
    print("Collecting data %s on %s" %(s_date, datetime.now())) 

    num_worker = 2
    start_time_paralel = datetime.now()
    with multiprocessing.Pool(num_worker) as pool:
        pool.map(get_data, list_data)
    
    duration = datetime.now() - start_time_paralel
    print("Total duration time: %s" %(duration))


def combineData(list_data):
    df_asm = pd.DataFrame()
    df2 = pd.DataFrame()
    df3 = pd.DataFrame()

    #combine ASM
    list_df = []
    s_date = list_data[0]['date2']
    path_result = '/var/opt/common5/eniq/lte/kpi/daily'
    print('Combining ASM data')
    for data in list_data:
        path_hourly = data['path']
        s_date = data['date2']
        df_asm = pd.DataFrame()
        for file in os.listdir(path_hourly):
            if 'dc_e_erbs_sectorcarrier_v_day_%s.zip'%s_date in file:
                df_asm = pd.read_csv('%s/%s'%(path_hourly, file), delimiter=',', index_col=None, header='infer')
            elif 'dc_e_erbs_sectorcarrier_v_day2_%s.zip'%s_date in file:
                df2 = pd.read_csv('%s/%s'%(path_hourly, file), delimiter=',', index_col=None, header='infer')
            elif 'dc_e_erbs_sectorcarrier_v_day3_%s.zip'%s_date in file:
                df3 = pd.read_csv('%s/%s'%(path_hourly, file), delimiter=',', index_col=None, header='infer')
            

    
        if len(df_asm) > 0:
            if len(df2) > 0:
                df_asm = df_asm.merge(df2, how='outer', left_on=['date_id', 'erbs', 'sectorcarrier'], right_on=['date_id', 'erbs', 'sectorcarrier'])        
            if len(df3) > 0:
                df_asm = df_asm.merge(df3, how='outer', left_on=['date_id', 'erbs', 'sectorcarrier'], right_on=['date_id', 'erbs', 'sectorcarrier'])
            
            df_asm.fillna(0, inplace=True)
            #Calculate mean ASM
            df_asm['pmBranchDeltaSinr0Sample'] = df_asm['pmbranchdeltasinrdistr0_0'] + df_asm['pmbranchdeltasinrdistr0_1'] + df_asm['pmbranchdeltasinrdistr0_2'] + df_asm['pmbranchdeltasinrdistr0_3'] + df_asm['pmbranchdeltasinrdistr0_4'] + df_asm['pmbranchdeltasinrdistr0_5'] + df_asm['pmbranchdeltasinrdistr0_6'] + df_asm['pmbranchdeltasinrdistr0_7'] + df_asm['pmbranchdeltasinrdistr0_8'] + df_asm['pmbranchdeltasinrdistr0_9'] + df_asm['pmbranchdeltasinrdistr0_10'] + df_asm['pmbranchdeltasinrdistr0_11'] + df_asm['pmbranchdeltasinrdistr0_12'] + df_asm['pmbranchdeltasinrdistr0_13'] + df_asm['pmbranchdeltasinrdistr0_14'] + df_asm['pmbranchdeltasinrdistr0_15'] + df_asm['pmbranchdeltasinrdistr0_16'] + df_asm['pmbranchdeltasinrdistr0_17'] + df_asm['pmbranchdeltasinrdistr0_18'] + df_asm['pmbranchdeltasinrdistr0_19'] + df_asm['pmbranchdeltasinrdistr0_20'] + df_asm['pmbranchdeltasinrdistr0_21'] + df_asm['pmbranchdeltasinrdistr0_22'] + df_asm['pmbranchdeltasinrdistr0_23'] + df_asm['pmbranchdeltasinrdistr0_24'] + df_asm['pmbranchdeltasinrdistr0_25'] + df_asm['pmbranchdeltasinrdistr0_26'] + df_asm['pmbranchdeltasinrdistr0_27'] + df_asm['pmbranchdeltasinrdistr0_28'] + df_asm['pmbranchdeltasinrdistr0_29'] + df_asm['pmbranchdeltasinrdistr0_30'] + df_asm['pmbranchdeltasinrdistr0_31'] + df_asm['pmbranchdeltasinrdistr0_32'] + df_asm['pmbranchdeltasinrdistr0_33'] + df_asm['pmbranchdeltasinrdistr0_34'] + df_asm['pmbranchdeltasinrdistr0_35'] + df_asm['pmbranchdeltasinrdistr0_36'] + df_asm['pmbranchdeltasinrdistr0_37'] + df_asm['pmbranchdeltasinrdistr0_38'] + df_asm['pmbranchdeltasinrdistr0_39'] + df_asm['pmbranchdeltasinrdistr0_40'] + df_asm['pmbranchdeltasinrdistr0_41'] + df_asm['pmbranchdeltasinrdistr0_42'] + df_asm['pmbranchdeltasinrdistr0_43'] + df_asm['pmbranchdeltasinrdistr0_44'] + df_asm['pmbranchdeltasinrdistr0_45'] + df_asm['pmbranchdeltasinrdistr0_46'] + df_asm['pmbranchdeltasinrdistr0_47'] + df_asm['pmbranchdeltasinrdistr0_48'] + df_asm['pmbranchdeltasinrdistr0_49'] + df_asm['pmbranchdeltasinrdistr0_50'] + df_asm['pmbranchdeltasinrdistr0_51'] + df_asm['pmbranchdeltasinrdistr0_52'] + df_asm['pmbranchdeltasinrdistr0_53'] + df_asm['pmbranchdeltasinrdistr0_54'] + df_asm['pmbranchdeltasinrdistr0_55'] + df_asm['pmbranchdeltasinrdistr0_56'] + df_asm['pmbranchdeltasinrdistr0_57'] + df_asm['pmbranchdeltasinrdistr0_58'] + df_asm['pmbranchdeltasinrdistr0_59']
            df_asm['pmBranchDeltaSinrDistr0Means'] = ((-29.0 *  df_asm['pmbranchdeltasinrdistr0_0']) + (-28.5 * df_asm['pmbranchdeltasinrdistr0_1']) + (-27.5 * df_asm['pmbranchdeltasinrdistr0_2']) + (-26.5 * df_asm['pmbranchdeltasinrdistr0_3']) + (-25.5 * df_asm['pmbranchdeltasinrdistr0_4']) + (-24.5 * df_asm['pmbranchdeltasinrdistr0_5']) + (-23.5 * df_asm['pmbranchdeltasinrdistr0_6']) + (-22.5 * df_asm['pmbranchdeltasinrdistr0_7']) + (-21.5 * df_asm['pmbranchdeltasinrdistr0_8']) + (-20.5 * df_asm['pmbranchdeltasinrdistr0_9']) + (-19.5 * df_asm['pmbranchdeltasinrdistr0_10']) + (-18.5 * df_asm['pmbranchdeltasinrdistr0_11']) + (-17.5 * df_asm['pmbranchdeltasinrdistr0_12']) + (-16.5 * df_asm['pmbranchdeltasinrdistr0_13']) + (-15.5 * df_asm['pmbranchdeltasinrdistr0_14']) + (-14.5 * df_asm['pmbranchdeltasinrdistr0_15']) + (-13.5 * df_asm['pmbranchdeltasinrdistr0_16']) + (-12.5 * df_asm['pmbranchdeltasinrdistr0_17']) + (-11.5 * df_asm['pmbranchdeltasinrdistr0_18']) + (-10.5 * df_asm['pmbranchdeltasinrdistr0_19']) + (-9.5 *  df_asm['pmbranchdeltasinrdistr0_20']) + (-8.5 *  df_asm['pmbranchdeltasinrdistr0_21']) + (-7.5 *  df_asm['pmbranchdeltasinrdistr0_22']) + (-6.5 *  df_asm['pmbranchdeltasinrdistr0_23']) + (-5.5 *  df_asm['pmbranchdeltasinrdistr0_24']) + (-4.5 *  df_asm['pmbranchdeltasinrdistr0_25']) + (-3.5 *  df_asm['pmbranchdeltasinrdistr0_26']) + (-2.5 *  df_asm['pmbranchdeltasinrdistr0_27']) + (-1.5 *  df_asm['pmbranchdeltasinrdistr0_28']) + (-0.5 *  df_asm['pmbranchdeltasinrdistr0_29']) + (0.5 *   df_asm['pmbranchdeltasinrdistr0_30']) + (1.5 *   df_asm['pmbranchdeltasinrdistr0_31']) + (2.5 *   df_asm['pmbranchdeltasinrdistr0_32']) + (3.5 *   df_asm['pmbranchdeltasinrdistr0_33']) + (4.5 *   df_asm['pmbranchdeltasinrdistr0_34']) + (5.5 *   df_asm['pmbranchdeltasinrdistr0_35']) + (6.5 *   df_asm['pmbranchdeltasinrdistr0_36']) + (7.5 *   df_asm['pmbranchdeltasinrdistr0_37']) + (8.5 *   df_asm['pmbranchdeltasinrdistr0_38']) + (9.5 *   df_asm['pmbranchdeltasinrdistr0_39']) + (10.5 *  df_asm['pmbranchdeltasinrdistr0_40']) + (11.5 *  df_asm['pmbranchdeltasinrdistr0_41']) + (12.5 *  df_asm['pmbranchdeltasinrdistr0_42']) + (13.5 *  df_asm['pmbranchdeltasinrdistr0_43']) + (14.5 *  df_asm['pmbranchdeltasinrdistr0_44']) + (15.5 *  df_asm['pmbranchdeltasinrdistr0_45']) + (16.5 *  df_asm['pmbranchdeltasinrdistr0_46']) + (17.5 *  df_asm['pmbranchdeltasinrdistr0_47']) + (18.5 *  df_asm['pmbranchdeltasinrdistr0_48']) + (19.5 *  df_asm['pmbranchdeltasinrdistr0_49']) + (20.5 *  df_asm['pmbranchdeltasinrdistr0_50']) + (21.5 *  df_asm['pmbranchdeltasinrdistr0_51']) + (22.5 *  df_asm['pmbranchdeltasinrdistr0_52']) + (23.5 *  df_asm['pmbranchdeltasinrdistr0_53']) + (24.5 *  df_asm['pmbranchdeltasinrdistr0_54']) + (25.5 *  df_asm['pmbranchdeltasinrdistr0_55']) + (26.5 *  df_asm['pmbranchdeltasinrdistr0_56']) + (27.5 *  df_asm['pmbranchdeltasinrdistr0_57']) + (28.5 *  df_asm['pmbranchdeltasinrdistr0_58']) + (29.0 *  df_asm['pmbranchdeltasinrdistr0_59'])) / df_asm['pmBranchDeltaSinr0Sample']
            df_asm['pmBranchDeltaSinrDistr0Std'] = ( ( (((-29.0 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_0']) + (((-28.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_1']) + (((-27.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_2']) + (((-26.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_3']) + (((-25.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_4']) + (((-24.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_5']) + (((-23.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_6']) + (((-22.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_7']) + (((-21.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_8']) + (((-20.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_9']) + (((-19.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_10']) + (((-18.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_11']) + (((-17.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_12']) + (((-16.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_13']) + (((-15.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_14']) + (((-14.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_15']) + (((-13.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_16']) + (((-12.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_17']) + (((-11.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_18']) + (((-10.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_19']) + (((-9.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_20']) + (((-8.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_21']) + (((-7.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_22']) + (((-6.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_23']) + (((-5.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_24']) + (((-4.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_25']) + (((-3.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_26']) + (((-2.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_27']) + (((-1.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_28']) + (((-0.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_29']) + (((0.5   - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_30']) + (((1.5   - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_31']) + (((2.5   - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_32']) + (((3.5   - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_33']) + (((4.5   - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_34']) + (((5.5   - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_35']) + (((6.5   - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_36']) + (((7.5   - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_37']) + (((8.5   - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_38']) + (((9.5   - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_39']) + (((10.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_40']) + (((11.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_41']) + (((12.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_42']) + (((13.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_43']) + (((14.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_44']) + (((15.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_45']) + (((16.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_46']) + (((17.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_47']) + (((18.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_48']) + (((19.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_49']) + (((20.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_50']) + (((21.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_51']) + (((22.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_52']) + (((23.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_53']) + (((24.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_54']) + (((25.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_55']) + (((26.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_56']) + (((27.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_57']) + (((28.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_58']) + (((29.0  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_59'])) /  df_asm['pmBranchDeltaSinr0Sample'])**0.5
            df_asm['pmBranchDeltaSinrDistr0Remark'] = 'Normal (No Failure)'
            df_asm.loc[df_asm['pmBranchDeltaSinr0Sample'] == 0, 'pmBranchDeltaSinrDistr0Remark'] = 'No Data'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr0Means'].abs() <= 4) & (df_asm['pmBranchDeltaSinrDistr0Std'].abs() <= 10), 'pmBranchDeltaSinrDistr0Remark'] = 'Normal (No Failure)'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr0Means'].abs() > 4) & (df_asm['pmBranchDeltaSinrDistr0Std'].abs() <= (df_asm['pmBranchDeltaSinrDistr0Means'].abs() - 4 + 5)), 'pmBranchDeltaSinrDistr0Remark'] = 'Loss in RF Path'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr0Means'].abs() > 4) & (df_asm['pmBranchDeltaSinrDistr0Std'].abs() > (df_asm['pmBranchDeltaSinrDistr0Means'].abs() - 4 + 5)), 'pmBranchDeltaSinrDistr0Remark'] = 'Antenna diagram mismatch'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr0Std'].abs() > 10), 'pmBranchDeltaSinrDistr0Remark'] = 'Partial Swapped Feeder'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr0Means'].abs() >= 29), 'pmBranchDeltaSinrDistr0Remark'] = 'Missing Feeder'

            df_asm['pmBranchDeltaSinr1Sample'] = df_asm['pmbranchdeltasinrdistr1_0'] + df_asm['pmbranchdeltasinrdistr1_1'] + df_asm['pmbranchdeltasinrdistr1_2'] + df_asm['pmbranchdeltasinrdistr1_3'] + df_asm['pmbranchdeltasinrdistr1_4'] + df_asm['pmbranchdeltasinrdistr1_5'] + df_asm['pmbranchdeltasinrdistr1_6'] + df_asm['pmbranchdeltasinrdistr1_7'] + df_asm['pmbranchdeltasinrdistr1_8'] + df_asm['pmbranchdeltasinrdistr1_9'] + df_asm['pmbranchdeltasinrdistr1_10'] + df_asm['pmbranchdeltasinrdistr1_11'] + df_asm['pmbranchdeltasinrdistr1_12'] + df_asm['pmbranchdeltasinrdistr1_13'] + df_asm['pmbranchdeltasinrdistr1_14'] + df_asm['pmbranchdeltasinrdistr1_15'] + df_asm['pmbranchdeltasinrdistr1_16'] + df_asm['pmbranchdeltasinrdistr1_17'] + df_asm['pmbranchdeltasinrdistr1_18'] + df_asm['pmbranchdeltasinrdistr1_19'] + df_asm['pmbranchdeltasinrdistr1_20'] + df_asm['pmbranchdeltasinrdistr1_21'] + df_asm['pmbranchdeltasinrdistr1_22'] + df_asm['pmbranchdeltasinrdistr1_23'] + df_asm['pmbranchdeltasinrdistr1_24'] + df_asm['pmbranchdeltasinrdistr1_25'] + df_asm['pmbranchdeltasinrdistr1_26'] + df_asm['pmbranchdeltasinrdistr1_27'] + df_asm['pmbranchdeltasinrdistr1_28'] + df_asm['pmbranchdeltasinrdistr1_29'] + df_asm['pmbranchdeltasinrdistr1_30'] + df_asm['pmbranchdeltasinrdistr1_31'] + df_asm['pmbranchdeltasinrdistr1_32'] + df_asm['pmbranchdeltasinrdistr1_33'] + df_asm['pmbranchdeltasinrdistr1_34'] + df_asm['pmbranchdeltasinrdistr1_35'] + df_asm['pmbranchdeltasinrdistr1_36'] + df_asm['pmbranchdeltasinrdistr1_37'] + df_asm['pmbranchdeltasinrdistr1_38'] + df_asm['pmbranchdeltasinrdistr1_39'] + df_asm['pmbranchdeltasinrdistr1_40'] + df_asm['pmbranchdeltasinrdistr1_41'] + df_asm['pmbranchdeltasinrdistr1_42'] + df_asm['pmbranchdeltasinrdistr1_43'] + df_asm['pmbranchdeltasinrdistr1_44'] + df_asm['pmbranchdeltasinrdistr1_45'] + df_asm['pmbranchdeltasinrdistr1_46'] + df_asm['pmbranchdeltasinrdistr1_47'] + df_asm['pmbranchdeltasinrdistr1_48'] + df_asm['pmbranchdeltasinrdistr1_49'] + df_asm['pmbranchdeltasinrdistr1_50'] + df_asm['pmbranchdeltasinrdistr1_51'] + df_asm['pmbranchdeltasinrdistr1_52'] + df_asm['pmbranchdeltasinrdistr1_53'] + df_asm['pmbranchdeltasinrdistr1_54'] + df_asm['pmbranchdeltasinrdistr1_55'] + df_asm['pmbranchdeltasinrdistr1_56'] + df_asm['pmbranchdeltasinrdistr1_57'] + df_asm['pmbranchdeltasinrdistr1_58'] + df_asm['pmbranchdeltasinrdistr1_59']
            df_asm['pmBranchDeltaSinrDistr1Means'] = ((-29.0 *  df_asm['pmbranchdeltasinrdistr1_0']) + (-28.5 * df_asm['pmbranchdeltasinrdistr1_1']) + (-27.5 * df_asm['pmbranchdeltasinrdistr1_2']) + (-26.5 * df_asm['pmbranchdeltasinrdistr1_3']) + (-25.5 * df_asm['pmbranchdeltasinrdistr1_4']) + (-24.5 * df_asm['pmbranchdeltasinrdistr1_5']) + (-23.5 * df_asm['pmbranchdeltasinrdistr1_6']) + (-22.5 * df_asm['pmbranchdeltasinrdistr1_7']) + (-21.5 * df_asm['pmbranchdeltasinrdistr1_8']) + (-20.5 * df_asm['pmbranchdeltasinrdistr1_9']) + (-19.5 * df_asm['pmbranchdeltasinrdistr1_10']) + (-18.5 * df_asm['pmbranchdeltasinrdistr1_11']) + (-17.5 * df_asm['pmbranchdeltasinrdistr1_12']) + (-16.5 * df_asm['pmbranchdeltasinrdistr1_13']) + (-15.5 * df_asm['pmbranchdeltasinrdistr1_14']) + (-14.5 * df_asm['pmbranchdeltasinrdistr1_15']) + (-13.5 * df_asm['pmbranchdeltasinrdistr1_16']) + (-12.5 * df_asm['pmbranchdeltasinrdistr1_17']) + (-11.5 * df_asm['pmbranchdeltasinrdistr1_18']) + (-10.5 * df_asm['pmbranchdeltasinrdistr1_19']) + (-9.5 *  df_asm['pmbranchdeltasinrdistr1_20']) + (-8.5 *  df_asm['pmbranchdeltasinrdistr1_21']) + (-7.5 *  df_asm['pmbranchdeltasinrdistr1_22']) + (-6.5 *  df_asm['pmbranchdeltasinrdistr1_23']) + (-5.5 *  df_asm['pmbranchdeltasinrdistr1_24']) + (-4.5 *  df_asm['pmbranchdeltasinrdistr1_25']) + (-3.5 *  df_asm['pmbranchdeltasinrdistr1_26']) + (-2.5 *  df_asm['pmbranchdeltasinrdistr1_27']) + (-1.5 *  df_asm['pmbranchdeltasinrdistr1_28']) + (-0.5 *  df_asm['pmbranchdeltasinrdistr1_29']) + (0.5 *   df_asm['pmbranchdeltasinrdistr1_30']) + (1.5 *   df_asm['pmbranchdeltasinrdistr1_31']) + (2.5 *   df_asm['pmbranchdeltasinrdistr1_32']) + (3.5 *   df_asm['pmbranchdeltasinrdistr1_33']) + (4.5 *   df_asm['pmbranchdeltasinrdistr1_34']) + (5.5 *   df_asm['pmbranchdeltasinrdistr1_35']) + (6.5 *   df_asm['pmbranchdeltasinrdistr1_36']) + (7.5 *   df_asm['pmbranchdeltasinrdistr1_37']) + (8.5 *   df_asm['pmbranchdeltasinrdistr1_38']) + (9.5 *   df_asm['pmbranchdeltasinrdistr1_39']) + (10.5 *  df_asm['pmbranchdeltasinrdistr1_40']) + (11.5 *  df_asm['pmbranchdeltasinrdistr1_41']) + (12.5 *  df_asm['pmbranchdeltasinrdistr1_42']) + (13.5 *  df_asm['pmbranchdeltasinrdistr1_43']) + (14.5 *  df_asm['pmbranchdeltasinrdistr1_44']) + (15.5 *  df_asm['pmbranchdeltasinrdistr1_45']) + (16.5 *  df_asm['pmbranchdeltasinrdistr1_46']) + (17.5 *  df_asm['pmbranchdeltasinrdistr1_47']) + (18.5 *  df_asm['pmbranchdeltasinrdistr1_48']) + (19.5 *  df_asm['pmbranchdeltasinrdistr1_49']) + (20.5 *  df_asm['pmbranchdeltasinrdistr1_50']) + (21.5 *  df_asm['pmbranchdeltasinrdistr1_51']) + (22.5 *  df_asm['pmbranchdeltasinrdistr1_52']) + (23.5 *  df_asm['pmbranchdeltasinrdistr1_53']) + (24.5 *  df_asm['pmbranchdeltasinrdistr1_54']) + (25.5 *  df_asm['pmbranchdeltasinrdistr1_55']) + (26.5 *  df_asm['pmbranchdeltasinrdistr1_56']) + (27.5 *  df_asm['pmbranchdeltasinrdistr1_57']) + (28.5 *  df_asm['pmbranchdeltasinrdistr1_58']) + (29.0 *  df_asm['pmbranchdeltasinrdistr1_59'])) / df_asm['pmBranchDeltaSinr1Sample']
            df_asm['pmBranchDeltaSinrDistr1Std'] = ( ( (((-29.0 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_0']) + (((-28.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_1']) + (((-27.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_2']) + (((-26.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_3']) + (((-25.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_4']) + (((-24.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_5']) + (((-23.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_6']) + (((-22.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_7']) + (((-21.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_8']) + (((-20.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_9']) + (((-19.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_10']) + (((-18.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_11']) + (((-17.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_12']) + (((-16.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_13']) + (((-15.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_14']) + (((-14.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_15']) + (((-13.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_16']) + (((-12.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_17']) + (((-11.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_18']) + (((-10.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_19']) + (((-9.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_20']) + (((-8.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_21']) + (((-7.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_22']) + (((-6.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_23']) + (((-5.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_24']) + (((-4.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_25']) + (((-3.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_26']) + (((-2.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_27']) + (((-1.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_28']) + (((-0.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_29']) + (((0.5   - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_30']) + (((1.5   - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_31']) + (((2.5   - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_32']) + (((3.5   - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_33']) + (((4.5   - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_34']) + (((5.5   - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_35']) + (((6.5   - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_36']) + (((7.5   - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_37']) + (((8.5   - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_38']) + (((9.5   - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_39']) + (((10.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_40']) + (((11.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_41']) + (((12.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_42']) + (((13.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_43']) + (((14.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_44']) + (((15.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_45']) + (((16.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_46']) + (((17.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_47']) + (((18.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_48']) + (((19.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_49']) + (((20.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_50']) + (((21.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_51']) + (((22.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_52']) + (((23.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_53']) + (((24.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_54']) + (((25.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_55']) + (((26.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_56']) + (((27.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_57']) + (((28.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_58']) + (((29.0  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_59'])) /  df_asm['pmBranchDeltaSinr1Sample'])**0.5
            df_asm['pmBranchDeltaSinrDistr1Remark'] = 'Normal (No Failure)'
            df_asm.loc[df_asm['pmBranchDeltaSinr1Sample'] == 0, 'pmBranchDeltaSinrDistr1Remark'] = 'No Data'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr1Means'].abs() <= 4) & (df_asm['pmBranchDeltaSinrDistr1Std'].abs() <= 10), 'pmBranchDeltaSinrDistr1Remark'] = 'Normal (No Failure)'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr1Means'].abs() > 4) & (df_asm['pmBranchDeltaSinrDistr1Std'].abs() <= (df_asm['pmBranchDeltaSinrDistr0Means'].abs() - 4 + 5)), 'pmBranchDeltaSinrDistr1Remark'] = 'Loss in RF Path'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr1Means'].abs() > 4) & (df_asm['pmBranchDeltaSinrDistr1Std'].abs() > (df_asm['pmBranchDeltaSinrDistr0Means'].abs() - 4 + 5)), 'pmBranchDeltaSinrDistr1Remark'] = 'Antenna diagram mismatch'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr1Std'].abs() > 10), 'pmBranchDeltaSinrDistr1Remark'] = 'Partial Swapped Feeder'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr1Means'].abs() >= 29), 'pmBranchDeltaSinrDistr1Remark'] = 'Missing Feeder'
            
            df_asm['pmBranchDeltaSinr2Sample'] = df_asm['pmbranchdeltasinrdistr2_0'] + df_asm['pmbranchdeltasinrdistr2_1'] + df_asm['pmbranchdeltasinrdistr2_2'] + df_asm['pmbranchdeltasinrdistr2_3'] + df_asm['pmbranchdeltasinrdistr2_4'] + df_asm['pmbranchdeltasinrdistr2_5'] + df_asm['pmbranchdeltasinrdistr2_6'] + df_asm['pmbranchdeltasinrdistr2_7'] + df_asm['pmbranchdeltasinrdistr2_8'] + df_asm['pmbranchdeltasinrdistr2_9'] + df_asm['pmbranchdeltasinrdistr2_10'] + df_asm['pmbranchdeltasinrdistr2_11'] + df_asm['pmbranchdeltasinrdistr2_12'] + df_asm['pmbranchdeltasinrdistr2_13'] + df_asm['pmbranchdeltasinrdistr2_14'] + df_asm['pmbranchdeltasinrdistr2_15'] + df_asm['pmbranchdeltasinrdistr2_16'] + df_asm['pmbranchdeltasinrdistr2_17'] + df_asm['pmbranchdeltasinrdistr2_18'] + df_asm['pmbranchdeltasinrdistr2_19'] + df_asm['pmbranchdeltasinrdistr2_20'] + df_asm['pmbranchdeltasinrdistr2_21'] + df_asm['pmbranchdeltasinrdistr2_22'] + df_asm['pmbranchdeltasinrdistr2_23'] + df_asm['pmbranchdeltasinrdistr2_24'] + df_asm['pmbranchdeltasinrdistr2_25'] + df_asm['pmbranchdeltasinrdistr2_26'] + df_asm['pmbranchdeltasinrdistr2_27'] + df_asm['pmbranchdeltasinrdistr2_28'] + df_asm['pmbranchdeltasinrdistr2_29'] + df_asm['pmbranchdeltasinrdistr2_30'] + df_asm['pmbranchdeltasinrdistr2_31'] + df_asm['pmbranchdeltasinrdistr2_32'] + df_asm['pmbranchdeltasinrdistr2_33'] + df_asm['pmbranchdeltasinrdistr2_34'] + df_asm['pmbranchdeltasinrdistr2_35'] + df_asm['pmbranchdeltasinrdistr2_36'] + df_asm['pmbranchdeltasinrdistr2_37'] + df_asm['pmbranchdeltasinrdistr2_38'] + df_asm['pmbranchdeltasinrdistr2_39'] + df_asm['pmbranchdeltasinrdistr2_40'] + df_asm['pmbranchdeltasinrdistr2_41'] + df_asm['pmbranchdeltasinrdistr2_42'] + df_asm['pmbranchdeltasinrdistr2_43'] + df_asm['pmbranchdeltasinrdistr2_44'] + df_asm['pmbranchdeltasinrdistr2_45'] + df_asm['pmbranchdeltasinrdistr2_46'] + df_asm['pmbranchdeltasinrdistr2_47'] + df_asm['pmbranchdeltasinrdistr2_48'] + df_asm['pmbranchdeltasinrdistr2_49'] + df_asm['pmbranchdeltasinrdistr2_50'] + df_asm['pmbranchdeltasinrdistr2_51'] + df_asm['pmbranchdeltasinrdistr2_52'] + df_asm['pmbranchdeltasinrdistr2_53'] + df_asm['pmbranchdeltasinrdistr2_54'] + df_asm['pmbranchdeltasinrdistr2_55'] + df_asm['pmbranchdeltasinrdistr2_56'] + df_asm['pmbranchdeltasinrdistr2_57'] + df_asm['pmbranchdeltasinrdistr2_58'] + df_asm['pmbranchdeltasinrdistr2_59']
            df_asm['pmBranchDeltaSinrDistr2Means'] = ((-29.0 *  df_asm['pmbranchdeltasinrdistr2_0']) + (-28.5 * df_asm['pmbranchdeltasinrdistr2_1']) + (-27.5 * df_asm['pmbranchdeltasinrdistr2_2']) + (-26.5 * df_asm['pmbranchdeltasinrdistr2_3']) + (-25.5 * df_asm['pmbranchdeltasinrdistr2_4']) + (-24.5 * df_asm['pmbranchdeltasinrdistr2_5']) + (-23.5 * df_asm['pmbranchdeltasinrdistr2_6']) + (-22.5 * df_asm['pmbranchdeltasinrdistr2_7']) + (-21.5 * df_asm['pmbranchdeltasinrdistr2_8']) + (-20.5 * df_asm['pmbranchdeltasinrdistr2_9']) + (-19.5 * df_asm['pmbranchdeltasinrdistr2_10']) + (-18.5 * df_asm['pmbranchdeltasinrdistr2_11']) + (-17.5 * df_asm['pmbranchdeltasinrdistr2_12']) + (-16.5 * df_asm['pmbranchdeltasinrdistr2_13']) + (-15.5 * df_asm['pmbranchdeltasinrdistr2_14']) + (-14.5 * df_asm['pmbranchdeltasinrdistr2_15']) + (-13.5 * df_asm['pmbranchdeltasinrdistr2_16']) + (-12.5 * df_asm['pmbranchdeltasinrdistr2_17']) + (-11.5 * df_asm['pmbranchdeltasinrdistr2_18']) + (-10.5 * df_asm['pmbranchdeltasinrdistr2_19']) + (-9.5 *  df_asm['pmbranchdeltasinrdistr2_20']) + (-8.5 *  df_asm['pmbranchdeltasinrdistr2_21']) + (-7.5 *  df_asm['pmbranchdeltasinrdistr2_22']) + (-6.5 *  df_asm['pmbranchdeltasinrdistr2_23']) + (-5.5 *  df_asm['pmbranchdeltasinrdistr2_24']) + (-4.5 *  df_asm['pmbranchdeltasinrdistr2_25']) + (-3.5 *  df_asm['pmbranchdeltasinrdistr2_26']) + (-2.5 *  df_asm['pmbranchdeltasinrdistr2_27']) + (-1.5 *  df_asm['pmbranchdeltasinrdistr2_28']) + (-0.5 *  df_asm['pmbranchdeltasinrdistr2_29']) + (0.5 *   df_asm['pmbranchdeltasinrdistr2_30']) + (1.5 *   df_asm['pmbranchdeltasinrdistr2_31']) + (2.5 *   df_asm['pmbranchdeltasinrdistr2_32']) + (3.5 *   df_asm['pmbranchdeltasinrdistr2_33']) + (4.5 *   df_asm['pmbranchdeltasinrdistr2_34']) + (5.5 *   df_asm['pmbranchdeltasinrdistr2_35']) + (6.5 *   df_asm['pmbranchdeltasinrdistr2_36']) + (7.5 *   df_asm['pmbranchdeltasinrdistr2_37']) + (8.5 *   df_asm['pmbranchdeltasinrdistr2_38']) + (9.5 *   df_asm['pmbranchdeltasinrdistr2_39']) + (10.5 *  df_asm['pmbranchdeltasinrdistr2_40']) + (11.5 *  df_asm['pmbranchdeltasinrdistr2_41']) + (12.5 *  df_asm['pmbranchdeltasinrdistr2_42']) + (13.5 *  df_asm['pmbranchdeltasinrdistr2_43']) + (14.5 *  df_asm['pmbranchdeltasinrdistr2_44']) + (15.5 *  df_asm['pmbranchdeltasinrdistr2_45']) + (16.5 *  df_asm['pmbranchdeltasinrdistr2_46']) + (17.5 *  df_asm['pmbranchdeltasinrdistr2_47']) + (18.5 *  df_asm['pmbranchdeltasinrdistr2_48']) + (19.5 *  df_asm['pmbranchdeltasinrdistr2_49']) + (20.5 *  df_asm['pmbranchdeltasinrdistr2_50']) + (21.5 *  df_asm['pmbranchdeltasinrdistr2_51']) + (22.5 *  df_asm['pmbranchdeltasinrdistr2_52']) + (23.5 *  df_asm['pmbranchdeltasinrdistr2_53']) + (24.5 *  df_asm['pmbranchdeltasinrdistr2_54']) + (25.5 *  df_asm['pmbranchdeltasinrdistr2_55']) + (26.5 *  df_asm['pmbranchdeltasinrdistr2_56']) + (27.5 *  df_asm['pmbranchdeltasinrdistr2_57']) + (28.5 *  df_asm['pmbranchdeltasinrdistr2_58']) + (29.0 *  df_asm['pmbranchdeltasinrdistr2_59'])) / df_asm['pmBranchDeltaSinr2Sample']
            df_asm['pmBranchDeltaSinrDistr2Std'] = ( ( (((-29.0 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_0']) + (((-28.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_1']) + (((-27.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_2']) + (((-26.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_3']) + (((-25.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_4']) + (((-24.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_5']) + (((-23.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_6']) + (((-22.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_7']) + (((-21.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_8']) + (((-20.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_9']) + (((-19.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_10']) + (((-18.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_11']) + (((-17.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_12']) + (((-16.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_13']) + (((-15.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_14']) + (((-14.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_15']) + (((-13.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_16']) + (((-12.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_17']) + (((-11.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_18']) + (((-10.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_19']) + (((-9.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_20']) + (((-8.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_21']) + (((-7.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_22']) + (((-6.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_23']) + (((-5.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_24']) + (((-4.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_25']) + (((-3.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_26']) + (((-2.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_27']) + (((-1.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_28']) + (((-0.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_29']) + (((0.5   - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_30']) + (((1.5   - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_31']) + (((2.5   - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_32']) + (((3.5   - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_33']) + (((4.5   - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_34']) + (((5.5   - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_35']) + (((6.5   - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_36']) + (((7.5   - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_37']) + (((8.5   - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_38']) + (((9.5   - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_39']) + (((10.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_40']) + (((11.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_41']) + (((12.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_42']) + (((13.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_43']) + (((14.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_44']) + (((15.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_45']) + (((16.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_46']) + (((17.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_47']) + (((18.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_48']) + (((19.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_49']) + (((20.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_50']) + (((21.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_51']) + (((22.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_52']) + (((23.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_53']) + (((24.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_54']) + (((25.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_55']) + (((26.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_56']) + (((27.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_57']) + (((28.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_58']) + (((29.0  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_59'])) /  df_asm['pmBranchDeltaSinr2Sample'])**0.5
            df_asm['pmBranchDeltaSinrDistr2Remark'] = 'Normal (No Failure)'
            df_asm.loc[df_asm['pmBranchDeltaSinr2Sample'] == 0, 'pmBranchDeltaSinrDistr2Remark'] = 'No Data'
            
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr2Means'].abs() <= 4) & (df_asm['pmBranchDeltaSinrDistr2Std'].abs() <= 10), 'pmBranchDeltaSinrDistr2Remark'] = 'Normal (No Failure)'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr2Means'].abs() > 4) & (df_asm['pmBranchDeltaSinrDistr2Std'].abs() <= (df_asm['pmBranchDeltaSinrDistr0Means'].abs() - 4 + 5)), 'pmBranchDeltaSinrDistr2Remark'] = 'Loss in RF Path'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr2Means'].abs() > 4) & (df_asm['pmBranchDeltaSinrDistr2Std'].abs() > (df_asm['pmBranchDeltaSinrDistr0Means'].abs() - 4 + 5)), 'pmBranchDeltaSinrDistr2Remark'] = 'Antenna diagram mismatch'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr2Std'].abs() > 10), 'pmBranchDeltaSinrDistr2Remark'] = 'Partial Swapped Feeder'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr2Means'].abs() >= 29), 'pmBranchDeltaSinrDistr2Remark'] = 'Missing Feeder'

            list_df.append(df_asm)
            
    if len(list_df)> 0:
        df_asm = pd.concat(list_df, axis=0)
        df_asm.to_csv('%s/asm_%s.csv'%(path_result, s_date), index=None)
        file_power = '/var/opt/common5/cm/all/%s/power_%s.zip' % (s_date, s_date)
        if os.path.exists(file_power):
            df_power = pd.read_csv(file_power)
            df_power = df_power.loc[pd.notnull(df_power['sectorCarrierId'])]
            df_power = df_power.loc[pd.notnull(df_power['NodeId'])]
            df_power['sectorCarrierId'] = df_power['sectorCarrierId'].astype('str')
            #df_power['sectorCarrierId'] = pd.to_numeric(df_power["sectorCarrierId"], errors='coerce').fillna(0)
            df_power['id'] = df_power['NodeId'].astype('str') + '_' + df_power['sectorCarrierId'].astype('str')
            dict_cellname = df_power.set_index('id').to_dict()['EUtranCellFDDId']
            df_asm = df_asm.loc[pd.notnull(df_asm['sectorcarrier'])]
            df_asm['sectorcarrier'] = df_asm['sectorcarrier'].astype('str')
            #df_asm['sectorcarrier'] = pd.to_numeric(df_asm["sectorcarrier"], errors='coerce')
            df_asm['id'] = df_asm['erbs'].astype('str') + '_' + df_asm['sectorcarrier'].astype('str')
            df_asm['eutrancellfdd'] =  df_asm['id'].map(dict_cellname)
            print('   Found eutrancellfdd: %s'%len(df_asm.loc[pd.notnull(df_asm['eutrancellfdd'])]))
            df_asm['remark'] = 'normal'
            df_asm.loc[df_asm['pmBranchDeltaSinrDistr0Remark'].isin(['Potential RF Loss', 'Loss in RF Path', 'Mismatched Antenna Diagram', 'Partial Swapped Feeder', 'Antenna diagram mismatch', 'Missing Feeder']), 'remark'] = 'asm_problem'
            df_asm.loc[df_asm['pmBranchDeltaSinrDistr1Remark'].isin(['Potential RF Loss', 'Loss in RF Path', 'Mismatched Antenna Diagram', 'Partial Swapped Feeder', 'Antenna diagram mismatch', 'Missing Feeder']), 'remark'] = 'asm_problem'
            df_asm.loc[df_asm['pmBranchDeltaSinrDistr2Remark'].isin(['Potential RF Loss', 'Loss in RF Path', 'Mismatched Antenna Diagram', 'Partial Swapped Feeder', 'Antenna diagram mismatch', 'Missing Feeder']), 'remark'] = 'asm_problem'
            del df_asm['id']
            lte_kpi.saveResult(df_asm, 'asm', s_date2, path_result)
        #uploadDatabaseDate(df_asm, 'pm_lte', 'asm', 'date_id', s_date)
    
    


def calculate_kpi(s_date, list_eniq):
    df_asm = pd.DataFrame()
    df2 = pd.DataFrame()
    df3 = pd.DataFrame()

    #combine ASM
    list_df = []
    path_result = '/var/opt/rca-ioh/eniq/all/%s' %(s_date)
    if not os.path.exists(path_result):
        os.mkdir(path_result)
    print('Combining ASM data')
    for eniq in list_eniq:
        path_hourly = '/var/opt/rca-ioh/eniq/%s/%s/daily' %(eniq, s_date)
        df_asm = pd.DataFrame()
        for file in os.listdir(path_hourly):
            if 'dc_e_erbs_sectorcarrier_v_day.csv' in file:
                df_asm = pd.read_csv('%s/%s'%(path_hourly, file), delimiter=',', index_col=None, header='infer')
            elif 'dc_e_erbs_sectorcarrier_v_day2.csv' in file:
                df2 = pd.read_csv('%s/%s'%(path_hourly, file), delimiter=',', index_col=None, header='infer')
            elif 'dc_e_erbs_sectorcarrier_v_day3.csv' in file:
                df3 = pd.read_csv('%s/%s'%(path_hourly, file), delimiter=',', index_col=None, header='infer')
            

    
        if len(df_asm) > 0:
            if len(df2) > 0:
                df_asm = df_asm.merge(df2, how='outer', left_on=['date_id', 'erbs', 'sectorcarrier'], right_on=['date_id', 'erbs', 'sectorcarrier'])        
            if len(df3) > 0:
                df_asm = df_asm.merge(df3, how='outer', left_on=['date_id', 'erbs', 'sectorcarrier'], right_on=['date_id', 'erbs', 'sectorcarrier'])
            
            df_asm.fillna(0, inplace=True)
            #Calculate mean ASM
            df_asm['pmBranchDeltaSinr0Sample'] = df_asm['pmbranchdeltasinrdistr0_0'] + df_asm['pmbranchdeltasinrdistr0_1'] + df_asm['pmbranchdeltasinrdistr0_2'] + df_asm['pmbranchdeltasinrdistr0_3'] + df_asm['pmbranchdeltasinrdistr0_4'] + df_asm['pmbranchdeltasinrdistr0_5'] + df_asm['pmbranchdeltasinrdistr0_6'] + df_asm['pmbranchdeltasinrdistr0_7'] + df_asm['pmbranchdeltasinrdistr0_8'] + df_asm['pmbranchdeltasinrdistr0_9'] + df_asm['pmbranchdeltasinrdistr0_10'] + df_asm['pmbranchdeltasinrdistr0_11'] + df_asm['pmbranchdeltasinrdistr0_12'] + df_asm['pmbranchdeltasinrdistr0_13'] + df_asm['pmbranchdeltasinrdistr0_14'] + df_asm['pmbranchdeltasinrdistr0_15'] + df_asm['pmbranchdeltasinrdistr0_16'] + df_asm['pmbranchdeltasinrdistr0_17'] + df_asm['pmbranchdeltasinrdistr0_18'] + df_asm['pmbranchdeltasinrdistr0_19'] + df_asm['pmbranchdeltasinrdistr0_20'] + df_asm['pmbranchdeltasinrdistr0_21'] + df_asm['pmbranchdeltasinrdistr0_22'] + df_asm['pmbranchdeltasinrdistr0_23'] + df_asm['pmbranchdeltasinrdistr0_24'] + df_asm['pmbranchdeltasinrdistr0_25'] + df_asm['pmbranchdeltasinrdistr0_26'] + df_asm['pmbranchdeltasinrdistr0_27'] + df_asm['pmbranchdeltasinrdistr0_28'] + df_asm['pmbranchdeltasinrdistr0_29'] + df_asm['pmbranchdeltasinrdistr0_30'] + df_asm['pmbranchdeltasinrdistr0_31'] + df_asm['pmbranchdeltasinrdistr0_32'] + df_asm['pmbranchdeltasinrdistr0_33'] + df_asm['pmbranchdeltasinrdistr0_34'] + df_asm['pmbranchdeltasinrdistr0_35'] + df_asm['pmbranchdeltasinrdistr0_36'] + df_asm['pmbranchdeltasinrdistr0_37'] + df_asm['pmbranchdeltasinrdistr0_38'] + df_asm['pmbranchdeltasinrdistr0_39'] + df_asm['pmbranchdeltasinrdistr0_40'] + df_asm['pmbranchdeltasinrdistr0_41'] + df_asm['pmbranchdeltasinrdistr0_42'] + df_asm['pmbranchdeltasinrdistr0_43'] + df_asm['pmbranchdeltasinrdistr0_44'] + df_asm['pmbranchdeltasinrdistr0_45'] + df_asm['pmbranchdeltasinrdistr0_46'] + df_asm['pmbranchdeltasinrdistr0_47'] + df_asm['pmbranchdeltasinrdistr0_48'] + df_asm['pmbranchdeltasinrdistr0_49'] + df_asm['pmbranchdeltasinrdistr0_50'] + df_asm['pmbranchdeltasinrdistr0_51'] + df_asm['pmbranchdeltasinrdistr0_52'] + df_asm['pmbranchdeltasinrdistr0_53'] + df_asm['pmbranchdeltasinrdistr0_54'] + df_asm['pmbranchdeltasinrdistr0_55'] + df_asm['pmbranchdeltasinrdistr0_56'] + df_asm['pmbranchdeltasinrdistr0_57'] + df_asm['pmbranchdeltasinrdistr0_58'] + df_asm['pmbranchdeltasinrdistr0_59']
            df_asm['pmBranchDeltaSinrDistr0Means'] = ((-29.0 *  df_asm['pmbranchdeltasinrdistr0_0']) + (-28.5 * df_asm['pmbranchdeltasinrdistr0_1']) + (-27.5 * df_asm['pmbranchdeltasinrdistr0_2']) + (-26.5 * df_asm['pmbranchdeltasinrdistr0_3']) + (-25.5 * df_asm['pmbranchdeltasinrdistr0_4']) + (-24.5 * df_asm['pmbranchdeltasinrdistr0_5']) + (-23.5 * df_asm['pmbranchdeltasinrdistr0_6']) + (-22.5 * df_asm['pmbranchdeltasinrdistr0_7']) + (-21.5 * df_asm['pmbranchdeltasinrdistr0_8']) + (-20.5 * df_asm['pmbranchdeltasinrdistr0_9']) + (-19.5 * df_asm['pmbranchdeltasinrdistr0_10']) + (-18.5 * df_asm['pmbranchdeltasinrdistr0_11']) + (-17.5 * df_asm['pmbranchdeltasinrdistr0_12']) + (-16.5 * df_asm['pmbranchdeltasinrdistr0_13']) + (-15.5 * df_asm['pmbranchdeltasinrdistr0_14']) + (-14.5 * df_asm['pmbranchdeltasinrdistr0_15']) + (-13.5 * df_asm['pmbranchdeltasinrdistr0_16']) + (-12.5 * df_asm['pmbranchdeltasinrdistr0_17']) + (-11.5 * df_asm['pmbranchdeltasinrdistr0_18']) + (-10.5 * df_asm['pmbranchdeltasinrdistr0_19']) + (-9.5 *  df_asm['pmbranchdeltasinrdistr0_20']) + (-8.5 *  df_asm['pmbranchdeltasinrdistr0_21']) + (-7.5 *  df_asm['pmbranchdeltasinrdistr0_22']) + (-6.5 *  df_asm['pmbranchdeltasinrdistr0_23']) + (-5.5 *  df_asm['pmbranchdeltasinrdistr0_24']) + (-4.5 *  df_asm['pmbranchdeltasinrdistr0_25']) + (-3.5 *  df_asm['pmbranchdeltasinrdistr0_26']) + (-2.5 *  df_asm['pmbranchdeltasinrdistr0_27']) + (-1.5 *  df_asm['pmbranchdeltasinrdistr0_28']) + (-0.5 *  df_asm['pmbranchdeltasinrdistr0_29']) + (0.5 *   df_asm['pmbranchdeltasinrdistr0_30']) + (1.5 *   df_asm['pmbranchdeltasinrdistr0_31']) + (2.5 *   df_asm['pmbranchdeltasinrdistr0_32']) + (3.5 *   df_asm['pmbranchdeltasinrdistr0_33']) + (4.5 *   df_asm['pmbranchdeltasinrdistr0_34']) + (5.5 *   df_asm['pmbranchdeltasinrdistr0_35']) + (6.5 *   df_asm['pmbranchdeltasinrdistr0_36']) + (7.5 *   df_asm['pmbranchdeltasinrdistr0_37']) + (8.5 *   df_asm['pmbranchdeltasinrdistr0_38']) + (9.5 *   df_asm['pmbranchdeltasinrdistr0_39']) + (10.5 *  df_asm['pmbranchdeltasinrdistr0_40']) + (11.5 *  df_asm['pmbranchdeltasinrdistr0_41']) + (12.5 *  df_asm['pmbranchdeltasinrdistr0_42']) + (13.5 *  df_asm['pmbranchdeltasinrdistr0_43']) + (14.5 *  df_asm['pmbranchdeltasinrdistr0_44']) + (15.5 *  df_asm['pmbranchdeltasinrdistr0_45']) + (16.5 *  df_asm['pmbranchdeltasinrdistr0_46']) + (17.5 *  df_asm['pmbranchdeltasinrdistr0_47']) + (18.5 *  df_asm['pmbranchdeltasinrdistr0_48']) + (19.5 *  df_asm['pmbranchdeltasinrdistr0_49']) + (20.5 *  df_asm['pmbranchdeltasinrdistr0_50']) + (21.5 *  df_asm['pmbranchdeltasinrdistr0_51']) + (22.5 *  df_asm['pmbranchdeltasinrdistr0_52']) + (23.5 *  df_asm['pmbranchdeltasinrdistr0_53']) + (24.5 *  df_asm['pmbranchdeltasinrdistr0_54']) + (25.5 *  df_asm['pmbranchdeltasinrdistr0_55']) + (26.5 *  df_asm['pmbranchdeltasinrdistr0_56']) + (27.5 *  df_asm['pmbranchdeltasinrdistr0_57']) + (28.5 *  df_asm['pmbranchdeltasinrdistr0_58']) + (29.0 *  df_asm['pmbranchdeltasinrdistr0_59'])) / df_asm['pmBranchDeltaSinr0Sample']
            df_asm['pmBranchDeltaSinrDistr0Std'] = ( ( (((-29.0 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_0']) + (((-28.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_1']) + (((-27.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_2']) + (((-26.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_3']) + (((-25.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_4']) + (((-24.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_5']) + (((-23.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_6']) + (((-22.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_7']) + (((-21.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_8']) + (((-20.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_9']) + (((-19.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_10']) + (((-18.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_11']) + (((-17.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_12']) + (((-16.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_13']) + (((-15.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_14']) + (((-14.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_15']) + (((-13.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_16']) + (((-12.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_17']) + (((-11.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_18']) + (((-10.5 - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_19']) + (((-9.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_20']) + (((-8.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_21']) + (((-7.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_22']) + (((-6.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_23']) + (((-5.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_24']) + (((-4.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_25']) + (((-3.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_26']) + (((-2.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_27']) + (((-1.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_28']) + (((-0.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_29']) + (((0.5   - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_30']) + (((1.5   - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_31']) + (((2.5   - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_32']) + (((3.5   - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_33']) + (((4.5   - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_34']) + (((5.5   - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_35']) + (((6.5   - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_36']) + (((7.5   - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_37']) + (((8.5   - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_38']) + (((9.5   - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_39']) + (((10.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_40']) + (((11.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_41']) + (((12.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_42']) + (((13.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_43']) + (((14.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_44']) + (((15.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_45']) + (((16.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_46']) + (((17.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_47']) + (((18.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_48']) + (((19.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_49']) + (((20.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_50']) + (((21.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_51']) + (((22.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_52']) + (((23.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_53']) + (((24.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_54']) + (((25.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_55']) + (((26.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_56']) + (((27.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_57']) + (((28.5  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_58']) + (((29.0  - df_asm['pmBranchDeltaSinrDistr0Means'])**2)* df_asm['pmbranchdeltasinrdistr0_59'])) /  df_asm['pmBranchDeltaSinr0Sample'])**0.5
            df_asm['pmBranchDeltaSinrDistr0Remark'] = 'Normal (No Failure)'
            df_asm.loc[df_asm['pmBranchDeltaSinr0Sample'] == 0, 'pmBranchDeltaSinrDistr0Remark'] = 'No Data'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr0Means'].abs() <= 4) & (df_asm['pmBranchDeltaSinrDistr0Std'].abs() <= 10), 'pmBranchDeltaSinrDistr0Remark'] = 'Normal (No Failure)'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr0Means'].abs() > 4) & (df_asm['pmBranchDeltaSinrDistr0Std'].abs() <= (df_asm['pmBranchDeltaSinrDistr0Means'].abs() - 4 + 5)), 'pmBranchDeltaSinrDistr0Remark'] = 'Loss in RF Path'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr0Means'].abs() > 4) & (df_asm['pmBranchDeltaSinrDistr0Std'].abs() > (df_asm['pmBranchDeltaSinrDistr0Means'].abs() - 4 + 5)), 'pmBranchDeltaSinrDistr0Remark'] = 'Antenna diagram mismatch'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr0Std'].abs() > 10), 'pmBranchDeltaSinrDistr0Remark'] = 'Partial Swapped Feeder'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr0Means'].abs() >= 29), 'pmBranchDeltaSinrDistr0Remark'] = 'Missing Feeder'

            df_asm['pmBranchDeltaSinr1Sample'] = df_asm['pmbranchdeltasinrdistr1_0'] + df_asm['pmbranchdeltasinrdistr1_1'] + df_asm['pmbranchdeltasinrdistr1_2'] + df_asm['pmbranchdeltasinrdistr1_3'] + df_asm['pmbranchdeltasinrdistr1_4'] + df_asm['pmbranchdeltasinrdistr1_5'] + df_asm['pmbranchdeltasinrdistr1_6'] + df_asm['pmbranchdeltasinrdistr1_7'] + df_asm['pmbranchdeltasinrdistr1_8'] + df_asm['pmbranchdeltasinrdistr1_9'] + df_asm['pmbranchdeltasinrdistr1_10'] + df_asm['pmbranchdeltasinrdistr1_11'] + df_asm['pmbranchdeltasinrdistr1_12'] + df_asm['pmbranchdeltasinrdistr1_13'] + df_asm['pmbranchdeltasinrdistr1_14'] + df_asm['pmbranchdeltasinrdistr1_15'] + df_asm['pmbranchdeltasinrdistr1_16'] + df_asm['pmbranchdeltasinrdistr1_17'] + df_asm['pmbranchdeltasinrdistr1_18'] + df_asm['pmbranchdeltasinrdistr1_19'] + df_asm['pmbranchdeltasinrdistr1_20'] + df_asm['pmbranchdeltasinrdistr1_21'] + df_asm['pmbranchdeltasinrdistr1_22'] + df_asm['pmbranchdeltasinrdistr1_23'] + df_asm['pmbranchdeltasinrdistr1_24'] + df_asm['pmbranchdeltasinrdistr1_25'] + df_asm['pmbranchdeltasinrdistr1_26'] + df_asm['pmbranchdeltasinrdistr1_27'] + df_asm['pmbranchdeltasinrdistr1_28'] + df_asm['pmbranchdeltasinrdistr1_29'] + df_asm['pmbranchdeltasinrdistr1_30'] + df_asm['pmbranchdeltasinrdistr1_31'] + df_asm['pmbranchdeltasinrdistr1_32'] + df_asm['pmbranchdeltasinrdistr1_33'] + df_asm['pmbranchdeltasinrdistr1_34'] + df_asm['pmbranchdeltasinrdistr1_35'] + df_asm['pmbranchdeltasinrdistr1_36'] + df_asm['pmbranchdeltasinrdistr1_37'] + df_asm['pmbranchdeltasinrdistr1_38'] + df_asm['pmbranchdeltasinrdistr1_39'] + df_asm['pmbranchdeltasinrdistr1_40'] + df_asm['pmbranchdeltasinrdistr1_41'] + df_asm['pmbranchdeltasinrdistr1_42'] + df_asm['pmbranchdeltasinrdistr1_43'] + df_asm['pmbranchdeltasinrdistr1_44'] + df_asm['pmbranchdeltasinrdistr1_45'] + df_asm['pmbranchdeltasinrdistr1_46'] + df_asm['pmbranchdeltasinrdistr1_47'] + df_asm['pmbranchdeltasinrdistr1_48'] + df_asm['pmbranchdeltasinrdistr1_49'] + df_asm['pmbranchdeltasinrdistr1_50'] + df_asm['pmbranchdeltasinrdistr1_51'] + df_asm['pmbranchdeltasinrdistr1_52'] + df_asm['pmbranchdeltasinrdistr1_53'] + df_asm['pmbranchdeltasinrdistr1_54'] + df_asm['pmbranchdeltasinrdistr1_55'] + df_asm['pmbranchdeltasinrdistr1_56'] + df_asm['pmbranchdeltasinrdistr1_57'] + df_asm['pmbranchdeltasinrdistr1_58'] + df_asm['pmbranchdeltasinrdistr1_59']
            df_asm['pmBranchDeltaSinrDistr1Means'] = ((-29.0 *  df_asm['pmbranchdeltasinrdistr1_0']) + (-28.5 * df_asm['pmbranchdeltasinrdistr1_1']) + (-27.5 * df_asm['pmbranchdeltasinrdistr1_2']) + (-26.5 * df_asm['pmbranchdeltasinrdistr1_3']) + (-25.5 * df_asm['pmbranchdeltasinrdistr1_4']) + (-24.5 * df_asm['pmbranchdeltasinrdistr1_5']) + (-23.5 * df_asm['pmbranchdeltasinrdistr1_6']) + (-22.5 * df_asm['pmbranchdeltasinrdistr1_7']) + (-21.5 * df_asm['pmbranchdeltasinrdistr1_8']) + (-20.5 * df_asm['pmbranchdeltasinrdistr1_9']) + (-19.5 * df_asm['pmbranchdeltasinrdistr1_10']) + (-18.5 * df_asm['pmbranchdeltasinrdistr1_11']) + (-17.5 * df_asm['pmbranchdeltasinrdistr1_12']) + (-16.5 * df_asm['pmbranchdeltasinrdistr1_13']) + (-15.5 * df_asm['pmbranchdeltasinrdistr1_14']) + (-14.5 * df_asm['pmbranchdeltasinrdistr1_15']) + (-13.5 * df_asm['pmbranchdeltasinrdistr1_16']) + (-12.5 * df_asm['pmbranchdeltasinrdistr1_17']) + (-11.5 * df_asm['pmbranchdeltasinrdistr1_18']) + (-10.5 * df_asm['pmbranchdeltasinrdistr1_19']) + (-9.5 *  df_asm['pmbranchdeltasinrdistr1_20']) + (-8.5 *  df_asm['pmbranchdeltasinrdistr1_21']) + (-7.5 *  df_asm['pmbranchdeltasinrdistr1_22']) + (-6.5 *  df_asm['pmbranchdeltasinrdistr1_23']) + (-5.5 *  df_asm['pmbranchdeltasinrdistr1_24']) + (-4.5 *  df_asm['pmbranchdeltasinrdistr1_25']) + (-3.5 *  df_asm['pmbranchdeltasinrdistr1_26']) + (-2.5 *  df_asm['pmbranchdeltasinrdistr1_27']) + (-1.5 *  df_asm['pmbranchdeltasinrdistr1_28']) + (-0.5 *  df_asm['pmbranchdeltasinrdistr1_29']) + (0.5 *   df_asm['pmbranchdeltasinrdistr1_30']) + (1.5 *   df_asm['pmbranchdeltasinrdistr1_31']) + (2.5 *   df_asm['pmbranchdeltasinrdistr1_32']) + (3.5 *   df_asm['pmbranchdeltasinrdistr1_33']) + (4.5 *   df_asm['pmbranchdeltasinrdistr1_34']) + (5.5 *   df_asm['pmbranchdeltasinrdistr1_35']) + (6.5 *   df_asm['pmbranchdeltasinrdistr1_36']) + (7.5 *   df_asm['pmbranchdeltasinrdistr1_37']) + (8.5 *   df_asm['pmbranchdeltasinrdistr1_38']) + (9.5 *   df_asm['pmbranchdeltasinrdistr1_39']) + (10.5 *  df_asm['pmbranchdeltasinrdistr1_40']) + (11.5 *  df_asm['pmbranchdeltasinrdistr1_41']) + (12.5 *  df_asm['pmbranchdeltasinrdistr1_42']) + (13.5 *  df_asm['pmbranchdeltasinrdistr1_43']) + (14.5 *  df_asm['pmbranchdeltasinrdistr1_44']) + (15.5 *  df_asm['pmbranchdeltasinrdistr1_45']) + (16.5 *  df_asm['pmbranchdeltasinrdistr1_46']) + (17.5 *  df_asm['pmbranchdeltasinrdistr1_47']) + (18.5 *  df_asm['pmbranchdeltasinrdistr1_48']) + (19.5 *  df_asm['pmbranchdeltasinrdistr1_49']) + (20.5 *  df_asm['pmbranchdeltasinrdistr1_50']) + (21.5 *  df_asm['pmbranchdeltasinrdistr1_51']) + (22.5 *  df_asm['pmbranchdeltasinrdistr1_52']) + (23.5 *  df_asm['pmbranchdeltasinrdistr1_53']) + (24.5 *  df_asm['pmbranchdeltasinrdistr1_54']) + (25.5 *  df_asm['pmbranchdeltasinrdistr1_55']) + (26.5 *  df_asm['pmbranchdeltasinrdistr1_56']) + (27.5 *  df_asm['pmbranchdeltasinrdistr1_57']) + (28.5 *  df_asm['pmbranchdeltasinrdistr1_58']) + (29.0 *  df_asm['pmbranchdeltasinrdistr1_59'])) / df_asm['pmBranchDeltaSinr1Sample']
            df_asm['pmBranchDeltaSinrDistr1Std'] = ( ( (((-29.0 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_0']) + (((-28.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_1']) + (((-27.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_2']) + (((-26.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_3']) + (((-25.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_4']) + (((-24.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_5']) + (((-23.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_6']) + (((-22.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_7']) + (((-21.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_8']) + (((-20.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_9']) + (((-19.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_10']) + (((-18.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_11']) + (((-17.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_12']) + (((-16.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_13']) + (((-15.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_14']) + (((-14.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_15']) + (((-13.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_16']) + (((-12.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_17']) + (((-11.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_18']) + (((-10.5 - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_19']) + (((-9.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_20']) + (((-8.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_21']) + (((-7.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_22']) + (((-6.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_23']) + (((-5.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_24']) + (((-4.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_25']) + (((-3.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_26']) + (((-2.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_27']) + (((-1.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_28']) + (((-0.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_29']) + (((0.5   - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_30']) + (((1.5   - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_31']) + (((2.5   - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_32']) + (((3.5   - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_33']) + (((4.5   - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_34']) + (((5.5   - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_35']) + (((6.5   - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_36']) + (((7.5   - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_37']) + (((8.5   - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_38']) + (((9.5   - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_39']) + (((10.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_40']) + (((11.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_41']) + (((12.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_42']) + (((13.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_43']) + (((14.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_44']) + (((15.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_45']) + (((16.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_46']) + (((17.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_47']) + (((18.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_48']) + (((19.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_49']) + (((20.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_50']) + (((21.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_51']) + (((22.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_52']) + (((23.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_53']) + (((24.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_54']) + (((25.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_55']) + (((26.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_56']) + (((27.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_57']) + (((28.5  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_58']) + (((29.0  - df_asm['pmBranchDeltaSinrDistr1Means'])**2)* df_asm['pmbranchdeltasinrdistr1_59'])) /  df_asm['pmBranchDeltaSinr1Sample'])**0.5
            df_asm['pmBranchDeltaSinrDistr1Remark'] = 'Normal (No Failure)'
            df_asm.loc[df_asm['pmBranchDeltaSinr1Sample'] == 0, 'pmBranchDeltaSinrDistr1Remark'] = 'No Data'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr1Means'].abs() <= 4) & (df_asm['pmBranchDeltaSinrDistr1Std'].abs() <= 10), 'pmBranchDeltaSinrDistr1Remark'] = 'Normal (No Failure)'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr1Means'].abs() > 4) & (df_asm['pmBranchDeltaSinrDistr1Std'].abs() <= (df_asm['pmBranchDeltaSinrDistr0Means'].abs() - 4 + 5)), 'pmBranchDeltaSinrDistr1Remark'] = 'Loss in RF Path'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr1Means'].abs() > 4) & (df_asm['pmBranchDeltaSinrDistr1Std'].abs() > (df_asm['pmBranchDeltaSinrDistr0Means'].abs() - 4 + 5)), 'pmBranchDeltaSinrDistr1Remark'] = 'Antenna diagram mismatch'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr1Std'].abs() > 10), 'pmBranchDeltaSinrDistr1Remark'] = 'Partial Swapped Feeder'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr1Means'].abs() >= 29), 'pmBranchDeltaSinrDistr1Remark'] = 'Missing Feeder'
            
            df_asm['pmBranchDeltaSinr2Sample'] = df_asm['pmbranchdeltasinrdistr2_0'] + df_asm['pmbranchdeltasinrdistr2_1'] + df_asm['pmbranchdeltasinrdistr2_2'] + df_asm['pmbranchdeltasinrdistr2_3'] + df_asm['pmbranchdeltasinrdistr2_4'] + df_asm['pmbranchdeltasinrdistr2_5'] + df_asm['pmbranchdeltasinrdistr2_6'] + df_asm['pmbranchdeltasinrdistr2_7'] + df_asm['pmbranchdeltasinrdistr2_8'] + df_asm['pmbranchdeltasinrdistr2_9'] + df_asm['pmbranchdeltasinrdistr2_10'] + df_asm['pmbranchdeltasinrdistr2_11'] + df_asm['pmbranchdeltasinrdistr2_12'] + df_asm['pmbranchdeltasinrdistr2_13'] + df_asm['pmbranchdeltasinrdistr2_14'] + df_asm['pmbranchdeltasinrdistr2_15'] + df_asm['pmbranchdeltasinrdistr2_16'] + df_asm['pmbranchdeltasinrdistr2_17'] + df_asm['pmbranchdeltasinrdistr2_18'] + df_asm['pmbranchdeltasinrdistr2_19'] + df_asm['pmbranchdeltasinrdistr2_20'] + df_asm['pmbranchdeltasinrdistr2_21'] + df_asm['pmbranchdeltasinrdistr2_22'] + df_asm['pmbranchdeltasinrdistr2_23'] + df_asm['pmbranchdeltasinrdistr2_24'] + df_asm['pmbranchdeltasinrdistr2_25'] + df_asm['pmbranchdeltasinrdistr2_26'] + df_asm['pmbranchdeltasinrdistr2_27'] + df_asm['pmbranchdeltasinrdistr2_28'] + df_asm['pmbranchdeltasinrdistr2_29'] + df_asm['pmbranchdeltasinrdistr2_30'] + df_asm['pmbranchdeltasinrdistr2_31'] + df_asm['pmbranchdeltasinrdistr2_32'] + df_asm['pmbranchdeltasinrdistr2_33'] + df_asm['pmbranchdeltasinrdistr2_34'] + df_asm['pmbranchdeltasinrdistr2_35'] + df_asm['pmbranchdeltasinrdistr2_36'] + df_asm['pmbranchdeltasinrdistr2_37'] + df_asm['pmbranchdeltasinrdistr2_38'] + df_asm['pmbranchdeltasinrdistr2_39'] + df_asm['pmbranchdeltasinrdistr2_40'] + df_asm['pmbranchdeltasinrdistr2_41'] + df_asm['pmbranchdeltasinrdistr2_42'] + df_asm['pmbranchdeltasinrdistr2_43'] + df_asm['pmbranchdeltasinrdistr2_44'] + df_asm['pmbranchdeltasinrdistr2_45'] + df_asm['pmbranchdeltasinrdistr2_46'] + df_asm['pmbranchdeltasinrdistr2_47'] + df_asm['pmbranchdeltasinrdistr2_48'] + df_asm['pmbranchdeltasinrdistr2_49'] + df_asm['pmbranchdeltasinrdistr2_50'] + df_asm['pmbranchdeltasinrdistr2_51'] + df_asm['pmbranchdeltasinrdistr2_52'] + df_asm['pmbranchdeltasinrdistr2_53'] + df_asm['pmbranchdeltasinrdistr2_54'] + df_asm['pmbranchdeltasinrdistr2_55'] + df_asm['pmbranchdeltasinrdistr2_56'] + df_asm['pmbranchdeltasinrdistr2_57'] + df_asm['pmbranchdeltasinrdistr2_58'] + df_asm['pmbranchdeltasinrdistr2_59']
            df_asm['pmBranchDeltaSinrDistr2Means'] = ((-29.0 *  df_asm['pmbranchdeltasinrdistr2_0']) + (-28.5 * df_asm['pmbranchdeltasinrdistr2_1']) + (-27.5 * df_asm['pmbranchdeltasinrdistr2_2']) + (-26.5 * df_asm['pmbranchdeltasinrdistr2_3']) + (-25.5 * df_asm['pmbranchdeltasinrdistr2_4']) + (-24.5 * df_asm['pmbranchdeltasinrdistr2_5']) + (-23.5 * df_asm['pmbranchdeltasinrdistr2_6']) + (-22.5 * df_asm['pmbranchdeltasinrdistr2_7']) + (-21.5 * df_asm['pmbranchdeltasinrdistr2_8']) + (-20.5 * df_asm['pmbranchdeltasinrdistr2_9']) + (-19.5 * df_asm['pmbranchdeltasinrdistr2_10']) + (-18.5 * df_asm['pmbranchdeltasinrdistr2_11']) + (-17.5 * df_asm['pmbranchdeltasinrdistr2_12']) + (-16.5 * df_asm['pmbranchdeltasinrdistr2_13']) + (-15.5 * df_asm['pmbranchdeltasinrdistr2_14']) + (-14.5 * df_asm['pmbranchdeltasinrdistr2_15']) + (-13.5 * df_asm['pmbranchdeltasinrdistr2_16']) + (-12.5 * df_asm['pmbranchdeltasinrdistr2_17']) + (-11.5 * df_asm['pmbranchdeltasinrdistr2_18']) + (-10.5 * df_asm['pmbranchdeltasinrdistr2_19']) + (-9.5 *  df_asm['pmbranchdeltasinrdistr2_20']) + (-8.5 *  df_asm['pmbranchdeltasinrdistr2_21']) + (-7.5 *  df_asm['pmbranchdeltasinrdistr2_22']) + (-6.5 *  df_asm['pmbranchdeltasinrdistr2_23']) + (-5.5 *  df_asm['pmbranchdeltasinrdistr2_24']) + (-4.5 *  df_asm['pmbranchdeltasinrdistr2_25']) + (-3.5 *  df_asm['pmbranchdeltasinrdistr2_26']) + (-2.5 *  df_asm['pmbranchdeltasinrdistr2_27']) + (-1.5 *  df_asm['pmbranchdeltasinrdistr2_28']) + (-0.5 *  df_asm['pmbranchdeltasinrdistr2_29']) + (0.5 *   df_asm['pmbranchdeltasinrdistr2_30']) + (1.5 *   df_asm['pmbranchdeltasinrdistr2_31']) + (2.5 *   df_asm['pmbranchdeltasinrdistr2_32']) + (3.5 *   df_asm['pmbranchdeltasinrdistr2_33']) + (4.5 *   df_asm['pmbranchdeltasinrdistr2_34']) + (5.5 *   df_asm['pmbranchdeltasinrdistr2_35']) + (6.5 *   df_asm['pmbranchdeltasinrdistr2_36']) + (7.5 *   df_asm['pmbranchdeltasinrdistr2_37']) + (8.5 *   df_asm['pmbranchdeltasinrdistr2_38']) + (9.5 *   df_asm['pmbranchdeltasinrdistr2_39']) + (10.5 *  df_asm['pmbranchdeltasinrdistr2_40']) + (11.5 *  df_asm['pmbranchdeltasinrdistr2_41']) + (12.5 *  df_asm['pmbranchdeltasinrdistr2_42']) + (13.5 *  df_asm['pmbranchdeltasinrdistr2_43']) + (14.5 *  df_asm['pmbranchdeltasinrdistr2_44']) + (15.5 *  df_asm['pmbranchdeltasinrdistr2_45']) + (16.5 *  df_asm['pmbranchdeltasinrdistr2_46']) + (17.5 *  df_asm['pmbranchdeltasinrdistr2_47']) + (18.5 *  df_asm['pmbranchdeltasinrdistr2_48']) + (19.5 *  df_asm['pmbranchdeltasinrdistr2_49']) + (20.5 *  df_asm['pmbranchdeltasinrdistr2_50']) + (21.5 *  df_asm['pmbranchdeltasinrdistr2_51']) + (22.5 *  df_asm['pmbranchdeltasinrdistr2_52']) + (23.5 *  df_asm['pmbranchdeltasinrdistr2_53']) + (24.5 *  df_asm['pmbranchdeltasinrdistr2_54']) + (25.5 *  df_asm['pmbranchdeltasinrdistr2_55']) + (26.5 *  df_asm['pmbranchdeltasinrdistr2_56']) + (27.5 *  df_asm['pmbranchdeltasinrdistr2_57']) + (28.5 *  df_asm['pmbranchdeltasinrdistr2_58']) + (29.0 *  df_asm['pmbranchdeltasinrdistr2_59'])) / df_asm['pmBranchDeltaSinr2Sample']
            df_asm['pmBranchDeltaSinrDistr2Std'] = ( ( (((-29.0 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_0']) + (((-28.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_1']) + (((-27.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_2']) + (((-26.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_3']) + (((-25.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_4']) + (((-24.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_5']) + (((-23.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_6']) + (((-22.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_7']) + (((-21.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_8']) + (((-20.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_9']) + (((-19.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_10']) + (((-18.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_11']) + (((-17.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_12']) + (((-16.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_13']) + (((-15.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_14']) + (((-14.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_15']) + (((-13.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_16']) + (((-12.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_17']) + (((-11.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_18']) + (((-10.5 - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_19']) + (((-9.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_20']) + (((-8.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_21']) + (((-7.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_22']) + (((-6.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_23']) + (((-5.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_24']) + (((-4.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_25']) + (((-3.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_26']) + (((-2.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_27']) + (((-1.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_28']) + (((-0.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_29']) + (((0.5   - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_30']) + (((1.5   - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_31']) + (((2.5   - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_32']) + (((3.5   - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_33']) + (((4.5   - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_34']) + (((5.5   - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_35']) + (((6.5   - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_36']) + (((7.5   - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_37']) + (((8.5   - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_38']) + (((9.5   - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_39']) + (((10.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_40']) + (((11.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_41']) + (((12.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_42']) + (((13.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_43']) + (((14.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_44']) + (((15.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_45']) + (((16.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_46']) + (((17.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_47']) + (((18.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_48']) + (((19.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_49']) + (((20.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_50']) + (((21.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_51']) + (((22.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_52']) + (((23.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_53']) + (((24.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_54']) + (((25.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_55']) + (((26.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_56']) + (((27.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_57']) + (((28.5  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_58']) + (((29.0  - df_asm['pmBranchDeltaSinrDistr2Means'])**2)* df_asm['pmbranchdeltasinrdistr2_59'])) /  df_asm['pmBranchDeltaSinr2Sample'])**0.5
            df_asm['pmBranchDeltaSinrDistr2Remark'] = 'Normal (No Failure)'
            df_asm.loc[df_asm['pmBranchDeltaSinr2Sample'] == 0, 'pmBranchDeltaSinrDistr2Remark'] = 'No Data'
            
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr2Means'].abs() <= 4) & (df_asm['pmBranchDeltaSinrDistr2Std'].abs() <= 10), 'pmBranchDeltaSinrDistr2Remark'] = 'Normal (No Failure)'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr2Means'].abs() > 4) & (df_asm['pmBranchDeltaSinrDistr2Std'].abs() <= (df_asm['pmBranchDeltaSinrDistr0Means'].abs() - 4 + 5)), 'pmBranchDeltaSinrDistr2Remark'] = 'Loss in RF Path'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr2Means'].abs() > 4) & (df_asm['pmBranchDeltaSinrDistr2Std'].abs() > (df_asm['pmBranchDeltaSinrDistr0Means'].abs() - 4 + 5)), 'pmBranchDeltaSinrDistr2Remark'] = 'Antenna diagram mismatch'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr2Std'].abs() > 10), 'pmBranchDeltaSinrDistr2Remark'] = 'Partial Swapped Feeder'
            df_asm.loc[(df_asm['pmBranchDeltaSinrDistr2Means'].abs() >= 29), 'pmBranchDeltaSinrDistr2Remark'] = 'Missing Feeder'

            list_df.append(df_asm)
            
    if len(list_df)> 0:
        df_asm = pd.concat(list_df, axis=0)
        df_asm.to_csv('%s/asm_%s.csv'%(path_result, s_date), index=None)
        file_power = '/var/opt/rca-ioh/cm_audit/power_%s.csv' % s_date
        if os.path.exists(file_power):
            df_power = pd.read_csv(file_power)
            df_power = df_power.loc[pd.notnull(df_power['sectorCarrierId'])]
            df_power['sectorCarrierId'] = pd.to_numeric(df_power["sectorCarrierId"], errors='coerce')
            df_power['id'] = df_power['NodeId'].astype('str') + '_' + df_power['sectorCarrierId'].astype('int').astype('str')
            dict_cellname = df_power.set_index('id').to_dict()['EUtranCellFDDId']
            df_asm = df_asm.loc[pd.notnull(df_asm['sectorcarrier'])]
            df_asm['sectorcarrier'] = pd.to_numeric(df_asm["sectorcarrier"], errors='coerce')
            df_asm['id'] = df_asm['erbs'].astype('str') + '_' + df_asm['sectorcarrier'].astype('int').astype('str')
            df_asm['eutrancellfdd'] =  df_asm['id'].map(dict_cellname)
            print('   Found eutrancellfdd: %s'%len(df_asm.loc[pd.notnull(df_asm['eutrancellfdd'])]))
            df_asm['remark'] = 'normal'
            df_asm.loc[df_asm['pmBranchDeltaSinrDistr0Remark'].isin(['Potential RF Loss', 'Loss in RF Path', 'Mismatched Antenna Diagram', 'Partial Swapped Feeder', 'Antenna diagram mismatch', 'Missing Feeder']), 'remark'] = 'asm_problem'
            df_asm.loc[df_asm['pmBranchDeltaSinrDistr1Remark'].isin(['Potential RF Loss', 'Loss in RF Path', 'Mismatched Antenna Diagram', 'Partial Swapped Feeder', 'Antenna diagram mismatch', 'Missing Feeder']), 'remark'] = 'asm_problem'
            df_asm.loc[df_asm['pmBranchDeltaSinrDistr2Remark'].isin(['Potential RF Loss', 'Loss in RF Path', 'Mismatched Antenna Diagram', 'Partial Swapped Feeder', 'Antenna diagram mismatch', 'Missing Feeder']), 'remark'] = 'asm_problem'
            del df_asm['id']
        uploadDatabaseDate(df_asm, 'pm_lte', 'asm', 'date_id', s_date)
    
    


if __name__ == '__main__':
    warnings.filterwarnings('ignore')
    f = open('/var/opt/common5/script/credential.json')
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
            folder_data = '/var/opt/common5/eniq/lte/raw/daily/%s/%s'%(eniq['enid'], s_date2)
            if not os.path.exists(folder_data):
                os.mkdir(folder_data)

            dict_data = {}
            dict_data['eniq'] = eniq
            dict_data['date'] = s_date
            dict_data['date2'] = s_date2
            dict_data['path'] = folder_data
            list_data.append(dict_data)

        get_daily_data(list_data)
        combineData(list_data)
    
    print('Completed at %s'%datetime.now())



    
        




            




