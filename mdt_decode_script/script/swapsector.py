import pandas as pd
import numpy as np
import os
import sys
from pyproj import Geod
import shutil
from datetime import datetime, timedelta
from zipfile import ZipFile
from dateutil import rrule
import mercantile

wgs84_geod = Geod(ellps='WGS84')

def Distance(lat1,lon1,lat2,lon2):
  az12,az21,dist = wgs84_geod.inv(lon1,lat1,lon2,lat2) #Yes, this order is correct
  return dist

def Bearing1(lat1,lon1,lat2,lon2):
  az12,az21,dist = wgs84_geod.inv(lon1,lat1,lon2,lat2) #Yes, this order is correct
  return az12

def Bearing2(lat1,lon1,lat2,lon2):
  az12,az21,dist = wgs84_geod.inv(lon1,lat1,lon2,lat2) #Yes, this order is correct
  return az21


def q1(x):
    return x.quantile(0.90)

def q2(x):
    return x.quantile(0.75)


def getValidCoordinate(df, df_gcell):
    print("   Initial row: %s" %len(df))
    df_cell = df_gcell[['enodebid', 'ci', 'cell_longitude', 'cell_latitude']]

    df = pd.merge(df, df_cell, left_on=['enodebid', 'ci'], right_on=['enodebid', 'ci'], how='left')
    df['eci'] = df['enodebid'] * 256 + df['ci']
    df['dist'] = Distance(df['cell_latitude'].tolist(),df['cell_longitude'].tolist(),df['lat_grid'].tolist(),df['long_grid'].tolist())

    df_quantile =  df.groupby(['enodebid', 'ci', 'eci'], as_index=False).agg( {'dist': q1})
    dict_dist90 = df_quantile.set_index('eci').to_dict()['dist']
    df['dist_90'] = df['eci'].map(dict_dist90)

    df = df.loc[df['dist']< df['dist_90']]
    del df['cell_longitude']
    del df['cell_latitude']


    #q = df["long_grid"].quantile(0.95)
    #df = df.loc[df["long_grid"] < q]

    #q = df["latitude"].quantile(0.95)
    #df = df.loc[df["latitude"] < q]
    print("   Exclude percentile 95 coordinate: %s" %len(df))
    return df

def swapsector_mdt(file, df_gcell, file_swapsector_result, file_swapsector_summary):
    df_swap = pd.DataFrame()
    df_mdt = pd.read_csv(file, compression='zip', delimiter=",", index_col=None, header='infer')
    df_mdt['long_grid'] = df_mdt['quadkey20'].apply(lambda x: mercantile.ul(mercantile.quadkey_to_tile(x).x, mercantile.quadkey_to_tile(x).y, mercantile.quadkey_to_tile(x).z).lng)
    df_mdt['lat_grid'] = df_mdt['quadkey20'].apply(lambda x: mercantile.ul(mercantile.quadkey_to_tile(x).x, mercantile.quadkey_to_tile(x).y, mercantile.quadkey_to_tile(x).z).lat)
    df_mdt = df_mdt.loc[pd.notnull(df_mdt['long_grid'])]
    df_mdt = getValidCoordinate(df_mdt, df_gcell)
    df_mdt = df_mdt.loc[df_mdt['sample'] > 0]

    #get average coordinate per cell
    df_mdt['long1'] = df_mdt['long_grid'] *df_mdt['sample']
    df_mdt['lat1'] = df_mdt['lat_grid'] *df_mdt['sample']


    df_cov = df_mdt.groupby(['site','enodebid', 'ci'], as_index=False).agg({'long1': 'sum', 'lat1':'sum', 'sample':'sum'})
    df_cov['longitude'] = df_cov['long1'] / df_cov['sample']
    df_cov['latitude'] = df_cov['lat1'] / df_cov['sample']
    df_cov['eci'] = df_cov['enodebid'] * 256 + df_cov['ci']
    del df_mdt

    #get cell long lat and azimuth from gcell
    df_cov = pd.merge(df_cov, df_gcell, left_on=['enodebid', 'ci'], right_on=['enodebid', 'ci'], how='left')

    df_cov['dist'] = Distance(df_cov['cell_latitude'].tolist(),df_cov['cell_longitude'].tolist(),df_cov['latitude'].tolist(),df_cov['longitude'].tolist())
    df_cov['bearing'] = Bearing1(df_cov['cell_latitude'].tolist(),df_cov['cell_longitude'].tolist(),df_cov['latitude'].tolist(),df_cov['longitude'].tolist())
    #df_cov['bearing2'] = Bearing2(df_cov['cell_latitude'].tolist(),df_cov['cell_longitude'].tolist(),df_cov['latitude'].tolist(),df_cov['longitude'].tolist())
    df_cov.loc[df_cov['bearing'] < 0, 'bearing'] = 360 + df_cov['bearing']
    df_cov['div_bearing'] = df_cov['bearing'] - df_cov['cell_azimuth']
    df_cov.loc[df_cov['div_bearing'] < 0, 'div_bearing'] = -1 * df_cov['div_bearing']
    
    #To be check is it valid or not??
    df_cov.loc[df_cov['div_bearing'] > 180, 'div_bearing'] = 360 - df_cov['div_bearing']
    
    df_cov['Remark'] = ''
    df_cov.loc[df_cov['div_bearing'] > 90, 'Remark'] = 'Probably Swap Sector or Wrong Azimuth Info'

    


    df_swap = df_cov.loc[df_cov['Remark'] == 'Probably Swap Sector or Wrong Azimuth Info']
    
    return df_swap

    
if __name__ == '__main__':
    now = datetime.now()
    tanggal = datetime.now()
    delta_day = 1
    s_date = s_date_2 = ""
    start_datetime = tanggal - timedelta(days=delta_day)
    current_date = start_datetime.strftime("%Y%m%d")
    checking_datetime = tanggal - timedelta(days=30)
    check_date = checking_datetime.strftime("%Y%m%d")

    stop_datetime = start_datetime
    isManual = False
    if len(sys.argv) > 1:
        s_date = sys.argv[1]
        if len(sys.argv) >2:
            s_date_2 = sys.argv[2]
        else:
            s_date_2 = s_date

        start_datetime = datetime.strptime(s_date, '%Y%m%d')
        stop_datetime = datetime.strptime(s_date_2, '%Y%m%d')
    
    print("Date Start: %s Stop: %s"%(start_datetime, stop_datetime))
    python_file_path = os.path.dirname(os.path.realpath(__file__))
    file_gcell = '/var/opt/common5/script/gcell.csv'
    df_gcell = pd.read_csv(file_gcell, delimiter=",", index_col=None, header='infer')
    df_gcell = df_gcell.loc[pd.notnull(df_gcell['enbid'])]
    df_gcell = df_gcell.rename(columns={'enbid': 'enodebid'})
    df_gcell = df_gcell[['enodebid', 'ci', 'longitude', 'latitude', 'azimuth']]
    df_gcell = df_gcell.rename(columns={'longitude': 'cell_longitude', 'latitude': 'cell_latitude', 'azimuth': 'cell_azimuth'})


    for dt in rrule.rrule(rrule.DAILY, dtstart=start_datetime, until=stop_datetime):
        s_date = dt.strftime("%Y%m%d")
        print("Date: %s"%(s_date))
        list_df = []
        
        decoded_ctr_path = "/var/opt/common5/mdt/%s" % (s_date)
        file_mdt = '%s/grid_mdt_cell_%s.zip'%(decoded_ctr_path, s_date)
        file_swapsector_result = '%s/mdt_swapsector.csv' %decoded_ctr_path
        file_swapsector_summary = '%s/mdt_swapsector_summary.txt' %decoded_ctr_path

        try:
            df = swapsector_mdt(file_mdt, df_gcell, file_swapsector_result, file_swapsector_summary)
            df['date'] = s_date
            if 'earfcn_serv' in df.columns:
                df = df.rename(columns={'earfcn_serv': 'earfcn'})
            
            print('Found  %s cell '%len(df))
            df = df[['date', 'site', 'enodebid', 'ci', 'cell_longitude', 'cell_latitude', 'cell_azimuth', 'sample', 'longitude', 'latitude', 'dist', 'bearing', 'div_bearing']]
            df = df.rename(columns={'longitude': 'mdt_centroid_x', 'latitude': 'mdt_centroid_y', 'div_bearing': 'diff_bearing'})
            file = '/var/opt/common5/swapsector/suspect_swap_%s.csv'%(s_date)
            df.to_csv(file, index=None)



        except Exception as e:
            print("Error: %s"%e)
            pass

            



            


    
    
    