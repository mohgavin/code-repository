from __future__ import print_function
import os
import sys
import os.path
import shutil
import pandas as pd
from datetime import datetime, timedelta
import zipfile
from dateutil import rrule
import warnings
from pyquadkey2 import quadkey
import mercantile

def combine_mdt(s_date):
    path = '/var/opt/common5/mdt'
    dict_cellname = {}

    path_daily = '/var/opt/common5/mdt/%s' %s_date
    if not os.path.exists(path_daily):
        os.mkdir(path_daily)

    list_file = ['result_ue_cap', 'mdt_result', 'ue_traffic', 'ue_meas']
    dict_columns = {}
    dict_columns['mdt_result'] = ['date', 'site', 'enodebid', 'ci', 'longitude', 'latitude', 'mmes1apid', 'rsrp_serving', 'rsrq_serving', 'CA_capable', 'CA_3CC_capable', 'MIMO4x4', 'ENDC', 'NR_SA']
    dict_columns['mdt_result'] = ['date', 'site', 'enodebid', 'ci', 'longitude', 'latitude', 'mmes1apid', 'rsrp_serving', 'rsrq_serving']
    dict_columns['ue_traffic'] = []
    dict_columns['ue_meas'] = []
    dict_columns['result_ue_cap'] = []


    for file_type in list_file:
        list_df = []
        for hour in range(24):
            hour = '%02d'%hour
            ta_path = '%s/%s'%(path_daily, hour)
            print("   Checking %s"%ta_path)
            if os.path.exists(ta_path):
                for file in os.listdir(ta_path):
                    if file_type in file:
                        if file_type != 'result_ue_cap':
                            if file.endswith('.csv'):
                                zip_file = '%s/%s.zip'%(ta_path, file_type)
                                zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
                                zf.write('%s/%s'%(ta_path, file), '%s.csv'%file_type)
                                zf.close()
                                os.remove('%s/%s'%(ta_path, file))
                                file = '%s.zip'%file_type
                            
                        #print("    reading %s %s %s %s on %s"%(file_type, enm, s_date, hour, datetime.now()))
                        try:
                            if len(dict_columns[file_type]) == 0:
                                df_new = pd.read_csv('%s/%s'%(ta_path,file), delimiter=",", index_col=None, header='infer')
                                if len(df_new) > 0:
                                    list_df.append(df_new)
                            else:
                                df_new = pd.read_csv('%s/%s'%(ta_path,file), delimiter=",", index_col=None, header='infer', usecols=dict_columns[file_type])
                                if len(df_new) > 0:
                                    list_df.append(df_new)
                        except Exception as e:
                            print("Error: %s"%e)
        if len(list_df) > 0:
            df = pd.concat(list_df, axis=0)
            print('combining data for %s done. Number of columns: %s'%(file_type, len(df.columns)))
            if file_type == 'mdt_result':
                '''
                size = 80
                grid_size = float(size)
                grid_const = 111.319491666667
                grid_size_deg_long = (grid_size/1000)/grid_const
                grid_size_deg_lat = (grid_size/1000)/grid_const
                '''
                print('%s initial raw: %s' %(file_type, len(df)))
                df['date'] = s_date
                df = df.loc[df['ci'] < 257]
                print('%s valid raw: %s' %(file_type, len(df)))
                df = df.loc[pd.notnull(df['longitude'])]
                df = df.loc[pd.notnull(df['latitude'])]
                df['longitude'] = pd.to_numeric(df["longitude"], errors='coerce')
                df['latitude'] = pd.to_numeric(df["latitude"], errors='coerce')

                # quadkey binning
                print("     transform coordinate to quadkey20")
                df["coordinate"] = df[["latitude", "longitude"]].apply(tuple, axis=1)
                df['quadkey20'] = df['coordinate'].apply(lambda x: quadkey.from_geo(x, 20))
                df['quadkey20'] = df['quadkey20'].astype('str')
                df.drop(['coordinate'], axis=1, inplace=True)
                print("         ....done")

                '''
                file_data = '%s/mdt_%s.csv'%(path_daily, s_date)
                df.to_csv('%s/mdt_%s.csv'%(path_daily, s_date), index=None)
                zip_file = '%s/mdt_%s.zip'%(path_daily, s_date)
                zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
                zf.write(file_data, os.path.basename(file_data))
                zf.close()
                os.remove(file_data)
                '''
                '''
                print("     transform quadkey20 to coordinate")
                df['long_grid'] = df['quadkey20'].apply(lambda x: mercantile.ul(mercantile.quadkey_to_tile(x).x, mercantile.quadkey_to_tile(x).y, mercantile.quadkey_to_tile(x).z).lng)
                df['lat_grid'] = df['quadkey20'].apply(lambda x: mercantile.ul(mercantile.quadkey_to_tile(x).x, mercantile.quadkey_to_tile(x).y, mercantile.quadkey_to_tile(x).z).lat)

                precision = 6
                df['long_grid'] = df['long_grid'].apply(lambda x: round(x, precision))
                df['lat_grid'] = df['lat_grid'].apply(lambda x: round(x, precision))
                print("         ....done")
                '''

                #df['rsrp_90_percl'] = df['rsrp_serving']
                #df['rsrp_75_percl'] = df['rsrp_serving']
                df['rsrp_mean'] = df['rsrp_serving']
                #df['rsrp_max'] = df['rsrp_serving']
                #df['rsrp_min'] = df['rsrp_serving']
                #df['rsrq_90_percl'] = df['rsrq_serving']
                #df['rsrq_75_percl'] = df['rsrq_serving']
                df['rsrq_mean'] = df['rsrq_serving']
                #df['rsrq_max'] = df['rsrq_serving']
                #df['rsrq_min'] = df['rsrq_serving']

                df_grid_mdt = df.groupby(['date', 'site', 'enodebid','ci', 'quadkey20']).agg({'mmes1apid': 'count','rsrp_mean':'mean', 'rsrq_mean':'mean'})
                df_grid_mdt = df_grid_mdt.rename(columns={'mmes1apid': 'sample'})
                #df_grid_mdt['grid_size'] = grid_size
                df_grid_mdt.to_csv('%s/grid_mdt_meas3.csv' %path_daily)
                file = '%s/grid_mdt_meas3.csv' %path_daily
                #del df

                print('Processing done, continue with pandas on %s' %datetime.now())
                df_grid_mdt = pd.read_csv(file, delimiter=',', index_col=None, header='infer')
                df_grid_mdt[['enodebid','ci']] = df_grid_mdt[['enodebid','ci']].apply(pd.to_numeric, errors='coerce')
                df_grid_mdt = df_grid_mdt[['date', 'site', 'enodebid','ci', 'quadkey20', 'sample', 'rsrp_mean', 'rsrq_mean']]
                file_cell = '%s/grid_mdt_cell_%s.csv'%(path_daily, s_date)

                df_grid_mdt = df_grid_mdt.sort_values(['quadkey20', 'rsrp_mean'], ascending=[True, False])
                df_grid_mdt['rank'] = df_grid_mdt.groupby(['quadkey20'], as_index=False).cumcount()+1

                df_grid_mdt.to_csv(file_cell, index= None)

                '''
                df_grid_mdt_bestserver = df_grid_mdt.loc[df_grid_mdt['rank'] == 1]

                df_grid_mdt_bestserver[['enodebid','ci']] = df_grid_mdt_bestserver[['enodebid','ci']].apply(pd.to_numeric, errors='coerce')
                
                df_grid_mdt_bestserver = df_grid_mdt_bestserver[['date', 'site', 'enodebid','ci', 'long_grid', 'lat_grid', 'sample', 'rsrp_mean', 'rsrp_max', 'rsrp_min', 'rsrq_mean', 'rsrq_max', 'rsrq_min', 'grid_size']]

                file_data = '%s/grid_mdt_%s.csv'%(path_daily, s_date)
                df_grid_mdt_bestserver.to_csv(file_data, index= None)
                
                zip_file = '%s/grid_mdt_%s.zip'%(path_daily, s_date)
                zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
                zf.write(file_data, os.path.basename(file_data))
                zf.close()
                os.remove(file_data)
                '''

                print('grid_mdt complete on %s' %datetime.now())
                print('zipping grid_mdt.....')
                zip_file = '%s/grid_mdt_cell_%s.zip'%(path_daily, s_date)
                zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
                zf.write(file_cell, os.path.basename(file_cell))
                zf.close()
                os.remove(file)
                os.remove(file_cell)
                '''
                #=======================================
                #  Grid 10m x 10m
                #=======================================
                print('   Binning 10mx10m RSRP...........')
                size = 10
                grid_size = float(size)
                grid_const = 111.319491666667
                grid_size_deg_long = (grid_size/1000)/grid_const
                grid_size_deg_lat = (grid_size/1000)/grid_const

                #((floor((minx + 180)/(grid_size/111.319491666667))+ 0.5) * (grid_size/111.319491666667)) - 180
                df['long_grid'] =(((df['longitude']/grid_size_deg_long).astype(int) + 0.5)*grid_size_deg_long).astype(float)
                df['lat_grid'] =(((df['latitude']/grid_size_deg_lat).astype(int) + 0.5)*grid_size_deg_lat).astype(float)

                precision = 6
                df['long_grid'] = df['long_grid'].apply(lambda x: round(x, precision))
                df['lat_grid'] = df['lat_grid'].apply(lambda x: round(x, precision))

                df['rsrp_90_percl'] = df['rsrp_serving']
                df['rsrp_75_percl'] = df['rsrp_serving']
                df['rsrp_mean'] = df['rsrp_serving']
                df['rsrp_max'] = df['rsrp_serving']
                df['rsrp_min'] = df['rsrp_serving']
                df['rsrq_90_percl'] = df['rsrq_serving']
                df['rsrq_75_percl'] = df['rsrq_serving']
                df['rsrq_mean'] = df['rsrq_serving']
                df['rsrq_max'] = df['rsrq_serving']
                df['rsrq_min'] = df['rsrq_serving']

                df_grid_mdt = df.groupby(['date', 'site', 'enodebid','ci','long_grid', 'lat_grid']).agg({'mmes1apid': 'count','rsrp_mean':'mean', 'rsrp_max':'max', 'rsrp_min':'min', 'rsrq_mean':'mean', 'rsrq_max':'max', 'rsrq_min':'min'})
                df_grid_mdt = df_grid_mdt.rename(columns={'mmes1apid': 'sample'})
                df_grid_mdt['grid_size'] = grid_size
                df_grid_mdt.to_csv('%s/grid_mdt_meas3.csv' %path_daily)
                file = '%s/grid_mdt_meas3.csv' %path_daily

                print('Processing done, continue with pandas on %s' %datetime.now())
                df_grid_mdt = pd.read_csv(file, delimiter=',', index_col=None, header='infer')
                df_grid_mdt[['enodebid','ci']] = df_grid_mdt[['enodebid','ci']].apply(pd.to_numeric, errors='coerce')
                df_grid_mdt = df_grid_mdt[['date', 'site', 'enodebid','ci', 'long_grid', 'lat_grid', 'sample', 'rsrp_mean', 'rsrp_max', 'rsrp_min', 'rsrq_mean', 'rsrq_max', 'rsrq_min', 'grid_size']]
                file_cell = '%s/grid10_mdt_cell_%s.csv'%(path_daily, s_date)

                #df_grid_mdt['rank'] = df_grid_mdt.groupby(['long_grid', 'lat_grid'], as_index=False)['rsrp_mean'].rank(ascending=False)
                df_grid_mdt = df_grid_mdt.sort_values(['long_grid', 'lat_grid', 'rsrp_mean'], ascending=[True, True, False])
                df_grid_mdt['rank'] = df_grid_mdt.groupby(['long_grid', 'lat_grid'], as_index=False).cumcount()+1
                df_grid_mdt.to_csv(file_cell, index= None)

                '''
                '''
                df_grid_mdt_bestserver = df_grid_mdt.loc[df_grid_mdt['rank'] == 1]

                df_grid_mdt_bestserver[['enodebid','ci']] = df_grid_mdt_bestserver[['enodebid','ci']].apply(pd.to_numeric, errors='coerce')
                
                df_grid_mdt_bestserver = df_grid_mdt_bestserver[['date', 'site', 'enodebid','ci', 'long_grid', 'lat_grid', 'sample', 'rsrp_mean', 'rsrp_max', 'rsrp_min', 'rsrq_mean', 'rsrq_max', 'rsrq_min', 'grid_size']]

                file_data = '%s/grid10_mdt_%s.csv'%(path_daily, s_date)
                df_grid_mdt_bestserver.to_csv(file_data, index= None)

                
                zip_file = '%s/grid10_mdt_%s.zip'%(path_daily, s_date)
                zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
                zf.write(file_data, os.path.basename(file_data))
                zf.close()
                os.remove(file_data)
                '''
                '''

                print('grid10_mdt complete on %s' %datetime.now())
                print('zipping grid10_mdt.....')
                zip_file = '%s/grid10_mdt_cell_%s.zip'%(path_daily, s_date)
                zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
                zf.write(file_cell, os.path.basename(file_cell))
                zf.close()
                os.remove(file)
                os.remove(file_cell)
                '''
     
            elif file_type == 'ue_traffic':
                '''
                size = 80
                grid_size = float(size)
                grid_const = 111.319491666667
                grid_size_deg_long = (grid_size/1000)/grid_const
                grid_size_deg_lat = (grid_size/1000)/grid_const
                '''
                print('%s initial raw: %s' %(file_type, len(df)))
                df['date'] = s_date
                df = df.loc[df['ci'] < 257]
                print('%s valid raw: %s' %(file_type, len(df)))
                df = df.loc[pd.notnull(df['longitude'])]
                df = df.loc[pd.notnull(df['latitude'])]
                df['longitude'] = pd.to_numeric(df["longitude"], errors='coerce')
                df['latitude'] = pd.to_numeric(df["latitude"], errors='coerce')

                # quadkey binning
                print("     transform coordinate to quadkey20")
                df["coordinate"] = df[["latitude", "longitude"]].apply(tuple, axis=1)
                df['quadkey20'] = df['coordinate'].apply(lambda x: quadkey.from_geo(x, 20))
                df['quadkey20'] = df['quadkey20'].astype('str')
                df.drop(['coordinate'], axis=1, inplace=True)
                print("         ....done")

                '''
                print("     transform quadkey20 to coordinate")
                df['long_grid'] = df['quadkey20'].apply(lambda x: mercantile.ul(mercantile.quadkey_to_tile(x).x, mercantile.quadkey_to_tile(x).y, mercantile.quadkey_to_tile(x).z).lng)
                df['lat_grid'] = df['quadkey20'].apply(lambda x: mercantile.ul(mercantile.quadkey_to_tile(x).x, mercantile.quadkey_to_tile(x).y, mercantile.quadkey_to_tile(x).z).lat)

                precision = 6
                df['long_grid'] = df['long_grid'].apply(lambda x: round(x, precision))
                df['lat_grid'] = df['lat_grid'].apply(lambda x: round(x, precision))
                print("         ....done")
                '''

                df['traffic_dl_kb_mean'] = df['radio_vol_dl_byte']
                #df['traffic_dl_kb_max'] = df['radio_vol_dl_byte']
                #df['traffic_dl_kb_min'] = df['radio_vol_dl_byte']

                df['traffic_ul_kb_mean'] = df['radio_vol_ul_byte']
                #df['traffic_ul_kb_max'] = df['radio_vol_ul_byte']
                #df['traffic_ul_kb_min'] = df['radio_vol_ul_byte']

                df['throughput_dl_mean'] = df['ue_throughput_dl_drb_kbps']
                #df['throughput_dl_max'] = df['ue_throughput_dl_drb_kbps']
                #df['throughput_dl_min'] = df['ue_throughput_dl_drb_kbps']

                df['throughput_ul_mean'] = df['ue_throughput_ul_drb_kbps']
                #df['throughput_ul_max'] = df['ue_throughput_ul_drb_kbps']
                #df['throughput_ul_min'] = df['ue_throughput_ul_drb_kbps']


                df_grid_mdt = df.groupby(['date', 'site', 'enodebid', 'ci', 'quadkey20']).agg({
                                                    'mmes1apid': 'count',
                                                    'traffic_dl_kb_mean':'mean',  
                                                    'traffic_ul_kb_mean':'mean', 
                                                    'throughput_dl_mean':'mean', 									
                                                    'throughput_ul_mean':'mean', 
                                                    })

                df_grid_mdt = df_grid_mdt.rename(columns={'mmes1apid': 'sample'})
                #df_grid_mdt['grid_size'] = grid_size
                df_grid_mdt.to_csv('%s/grid_traffic3.csv'%path_daily)
                file = '%s/grid_traffic3.csv' %path_daily
                #del df

                print('Processing done, continue with pandas on %s' %datetime.now())
                df_grid_mdt = pd.read_csv(file, delimiter=',', index_col=None, header='infer')
                df_grid_mdt = df_grid_mdt[['date', 'site', 'enodebid','ci', 'quadkey20', 'sample', 'traffic_dl_kb_mean', 'traffic_ul_kb_mean', 'throughput_dl_mean', 'throughput_ul_mean']]
                file_cell = '%s/grid_mdt_traffic_cell_%s.csv'%(path_daily, s_date)

                df_grid_mdt = df_grid_mdt.sort_values(['quadkey20', 'traffic_dl_kb_mean'], ascending=[True, False])
                df_grid_mdt['rank'] = df_grid_mdt.groupby(['quadkey20'], as_index=False).cumcount()+1
                df_grid_mdt.to_csv(file_cell, index= None)

                '''
                df_grid_mdt_bestserver = df_grid_mdt.loc[df_grid_mdt['rank'] == 1]
                
                df_grid_mdt_bestserver[['enodebid','ci']] = df_grid_mdt_bestserver[['enodebid','ci']].apply(pd.to_numeric, errors='coerce')
                
                df_grid_mdt_bestserver = df_grid_mdt_bestserver[['date', 'site', 'enodebid','ci', 'long_grid', 'lat_grid', 'sample', 'traffic_dl_kb_mean', 'traffic_dl_kb_max', 'traffic_dl_kb_min', 'traffic_ul_kb_mean', 'traffic_ul_kb_max', 'traffic_ul_kb_min', 'throughput_dl_mean', 'throughput_dl_max', 'throughput_dl_min', 'throughput_ul_mean', 'throughput_ul_max', 'throughput_ul_min', 'grid_size']]

                file_data = '%s/grid_mdt_traffic_%s.csv'%(path_daily, s_date)
                df_grid_mdt_bestserver.to_csv(file_data, index= None)
                
                zip_file = '%s/grid_mdt_traffic_%s.zip'%(path_daily, s_date)
                zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
                zf.write(file_data, os.path.basename(file_data))
                zf.close()
                os.remove(file_data)
                '''

                print('grid_ue_traffic complete on %s' %datetime.now())
                print('zipping grid_ue_traffic.....')

                zip_file = '%s/grid_mdt_traffic_cell_%s.zip'%(path_daily, s_date)
                zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
                zf.write(file_cell, os.path.basename(file_cell))
                zf.close()
                os.remove(file)
                os.remove(file_cell)
                '''
                #======================================
                #  Binning 10x10m Throughput
                #======================================
                print('   Binning 10mx10m Throughput...........')
                size = 10
                grid_size = float(size)
                grid_const = 111.319491666667
                grid_size_deg_long = (grid_size/1000)/grid_const
                grid_size_deg_lat = (grid_size/1000)/grid_const

                df['long_grid'] =(((df['longitude']/grid_size_deg_long).astype(int) + 0.5)*grid_size_deg_long).astype(float)
                df['lat_grid'] =(((df['latitude']/grid_size_deg_lat).astype(int) + 0.5)*grid_size_deg_lat).astype(float)

                precision = 6
                df['long_grid'] = df['long_grid'].apply(lambda x: round(x, precision))
                df['lat_grid'] = df['lat_grid'].apply(lambda x: round(x, precision))


                df['traffic_dl_kb_mean'] = df['radio_vol_dl_byte']
                df['traffic_dl_kb_max'] = df['radio_vol_dl_byte']
                df['traffic_dl_kb_min'] = df['radio_vol_dl_byte']

                df['traffic_ul_kb_mean'] = df['radio_vol_ul_byte']
                df['traffic_ul_kb_max'] = df['radio_vol_ul_byte']
                df['traffic_ul_kb_min'] = df['radio_vol_ul_byte']

                df['throughput_dl_mean'] = df['ue_throughput_dl_drb_kbps']
                df['throughput_dl_max'] = df['ue_throughput_dl_drb_kbps']
                df['throughput_dl_min'] = df['ue_throughput_dl_drb_kbps']

                df['throughput_ul_mean'] = df['ue_throughput_ul_drb_kbps']
                df['throughput_ul_max'] = df['ue_throughput_ul_drb_kbps']
                df['throughput_ul_min'] = df['ue_throughput_ul_drb_kbps']


                df_grid_mdt = df.groupby(['date', 'site', 'enodebid','ci','long_grid', 'lat_grid']).agg({
                                                    'mmes1apid': 'count',
                                                    'traffic_dl_kb_mean':'mean', 
                                                    'traffic_dl_kb_max':'max', 
                                                    'traffic_dl_kb_min':'min', 
                                                    'traffic_ul_kb_mean':'mean', 
                                                    'traffic_ul_kb_max':'max', 
                                                    'traffic_ul_kb_min':'min', 
                                                    'throughput_dl_mean':'mean', 
                                                    'throughput_dl_max':'max', 
                                                    'throughput_dl_min':'min', 										
                                                    'throughput_ul_mean':'mean', 
                                                    'throughput_ul_max':'max', 
                                                    'throughput_ul_min':'min',
                                                    })


                df_grid_mdt = df_grid_mdt.rename(columns={'mmes1apid': 'sample'})
                df_grid_mdt['grid_size'] = grid_size
                df_grid_mdt.to_csv('%s/grid_traffic3.csv'%path_daily)
                file = '%s/grid_traffic3.csv' %path_daily
                #del df

                print('Processing done, continue with pandas on %s' %datetime.now())
                df_grid_mdt = pd.read_csv(file, delimiter=',', index_col=None, header='infer')
                df_grid_mdt = df_grid_mdt[['date', 'site', 'enodebid','ci', 'long_grid', 'lat_grid', 'sample', 'traffic_dl_kb_mean', 'traffic_dl_kb_max', 'traffic_dl_kb_min', 'traffic_ul_kb_mean', 'traffic_ul_kb_max', 'traffic_ul_kb_min', 'throughput_dl_mean', 'throughput_dl_max', 'throughput_dl_min', 'throughput_ul_mean', 'throughput_ul_max', 'throughput_ul_min', 'grid_size']]
                file_cell = '%s/grid10_mdt_traffic_cell_%s.csv'%(path_daily, s_date)

                df_grid_mdt = df_grid_mdt.sort_values(['long_grid', 'lat_grid', 'traffic_dl_kb_mean'], ascending=[True, True, False])
                df_grid_mdt['rank'] = df_grid_mdt.groupby(['long_grid', 'lat_grid'], as_index=False).cumcount()+1
                df_grid_mdt.to_csv(file_cell, index= None)
                '''
                '''
                df_grid_mdt_bestserver = df_grid_mdt.loc[df_grid_mdt['rank'] == 1]
                
                df_grid_mdt_bestserver[['enodebid','ci']] = df_grid_mdt_bestserver[['enodebid','ci']].apply(pd.to_numeric, errors='coerce')
                
                df_grid_mdt_bestserver = df_grid_mdt_bestserver[['date', 'site', 'enodebid','ci', 'long_grid', 'lat_grid', 'sample', 'traffic_dl_kb_mean', 'traffic_dl_kb_max', 'traffic_dl_kb_min', 'traffic_ul_kb_mean', 'traffic_ul_kb_max', 'traffic_ul_kb_min', 'throughput_dl_mean', 'throughput_dl_max', 'throughput_dl_min', 'throughput_ul_mean', 'throughput_ul_max', 'throughput_ul_min', 'grid_size']]

                file_data = '%s/grid10_mdt_traffic_%s.csv'%(path_daily, s_date)
                df_grid_mdt_bestserver.to_csv(file_data, index= None)
                
                zip_file = '%s/grid10_mdt_traffic_%s.zip'%(path_daily, s_date)
                zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
                zf.write(file_data, os.path.basename(file_data))
                zf.close()
                os.remove(file_data)
                '''
                '''
                print('grid10_ue_traffic complete on %s' %datetime.now())
                print('zipping grid10_ue_traffic.....')
                zip_file = '%s/grid10_mdt_traffic_cell_%s.zip'%(path_daily, s_date)
                zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
                zf.write(file_cell, os.path.basename(file_cell))
                zf.close()
                os.remove(file)
                os.remove(file_cell)
                '''

            elif file_type == 'ue_meas':
                '''
                size = 80
                grid_size = float(size)
                grid_const = 111.319491666667
                grid_size_deg_long = (grid_size/1000)/grid_const
                grid_size_deg_lat = (grid_size/1000)/grid_const
                '''
                print('%s initial raw: %s' %(file_type, len(df)))
                df['date'] = s_date
                df = df.loc[df['ci'] < 257]
                print('%s valid raw: %s' %(file_type, len(df)))
                df = df.loc[pd.notnull(df['longitude'])]
                df = df.loc[pd.notnull(df['latitude'])]
                df['longitude'] = pd.to_numeric(df["longitude"], errors='coerce')
                df['latitude'] = pd.to_numeric(df["latitude"], errors='coerce')
                
                # quadkey binning
                print("     transform coordinate to quadkey20")
                df["coordinate"] = df[["latitude", "longitude"]].apply(tuple, axis=1)
                df['quadkey20'] = df['coordinate'].apply(lambda x: quadkey.from_geo(x, 20))
                df['quadkey20'] = df['quadkey20'].astype('str')
                df.drop(['coordinate'], axis=1, inplace=True)
                print("         ....done")

                '''
                print("     transform quadkey20 to coordinate")
                df['long_grid'] = df['quadkey20'].apply(lambda x: mercantile.ul(mercantile.quadkey_to_tile(x).x, mercantile.quadkey_to_tile(x).y, mercantile.quadkey_to_tile(x).z).lng)
                df['lat_grid'] = df['quadkey20'].apply(lambda x: mercantile.ul(mercantile.quadkey_to_tile(x).x, mercantile.quadkey_to_tile(x).y, mercantile.quadkey_to_tile(x).z).lat)
                print("         ....done")

                precision = 6
                df['long_grid'] = df['long_grid'].apply(lambda x: round(x, precision))
                df['lat_grid'] = df['lat_grid'].apply(lambda x: round(x, precision))
                '''

                #df['cqi_90_percl'] = df['cqi_avg']
                #df['cqi_75_percl'] = df['cqi_avg']
                df['cqi_mean'] = df['cqi_avg']
                #df['cqi_max'] = df['cqi_avg']
                #df['cqi_min'] = df['cqi_avg']

                #df['sinr_90_percl'] = df['pusch_sinr']
                #df['sinr_75_percl'] = df['pusch_sinr']
                df['sinr_mean'] = df['pusch_sinr']
                #df['sinr_max'] = df['pusch_sinr']
                #df['sinr_min'] = df['pusch_sinr']

                #df['rank2_90_percl'] = df['rank2_perc']
                #df['rank2_75_percl'] = df['rank2_perc']
                df['rank2_mean'] = df['rank2_perc']
                #df['rank2_max'] = df['rank2_perc']
                #df['rank2_min'] = df['rank2_perc']

                df_grid_mdt = df.groupby(['date', 'site', 'enodebid','ci', 'quadkey20']).agg({'mmes1apid': 'count','cqi_mean':'mean', 'sinr_mean':'mean', 'rank2_mean':'mean'})
                df_grid_mdt = df_grid_mdt.rename(columns={'mmes1apid': 'sample'})
                #df_grid_mdt['grid_size'] = grid_size
                df_grid_mdt.to_csv('%s/grid_meas3.csv'%path_daily)
                file = '%s/grid_meas3.csv' %path_daily
                #del df

                #convert to pandas using df_grid_mdt = df_grid_mdt.compute()
                print('Processing done, continue with pandas on %s' %datetime.now())
                df_grid_mdt = pd.read_csv(file, delimiter=',', index_col=None, header='infer')
                df_grid_mdt = df_grid_mdt[['date', 'site', 'enodebid','ci', 'quadkey20', 'sample', 'cqi_mean', 'sinr_mean', 'rank2_mean']]
                file_cell = '%s/grid_mdt_meas_cell_%s.csv'%(path_daily, s_date)

                df_grid_mdt = df_grid_mdt.sort_values(['quadkey20', 'cqi_mean'], ascending=[True, False])
                df_grid_mdt['rank'] = df_grid_mdt.groupby(['quadkey20'], as_index=False).cumcount()+1
                df_grid_mdt.to_csv(file_cell, index= None)

                '''
                df_grid_mdt_bestserver = df_grid_mdt.loc[df_grid_mdt['rank'] == 1]
                #df_grid_mdt_bestserver.to_csv('grid_meas2.csv', index=None)

                df_grid_mdt_bestserver[['enodebid','ci']] = df_grid_mdt_bestserver[['enodebid','ci']].apply(pd.to_numeric, errors='coerce')
                #df_grid_mdt_bestserver['cellname'] = (df_grid_mdt_bestserver['enodebid'] * 256) + df_grid_mdt_bestserver['ci']
                #df_grid_mdt_bestserver['cellname'] = df_grid_mdt_bestserver['cellname'].map(dict_cellname)

                df_grid_mdt_bestserver = df_grid_mdt_bestserver[['date', 'site', 'enodebid','ci', 'long_grid', 'lat_grid', 'sample', 'cqi_mean', 'cqi_max', 'cqi_min', 'sinr_mean', 'sinr_max', 'sinr_min', 'rank2_mean', 'rank2_max', 'rank2_min', 'grid_size']]

                file_data = '%s/grid_mdt_meas_%s.csv'%(path_daily, s_date)
                df_grid_mdt_bestserver.to_csv(file_data, index= None)
                zip_file = '%s/grid_mdt_meas_%s.zip'%(path_daily, s_date)
                zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
                zf.write(file_data, os.path.basename(file_data))
                zf.close()
                os.remove(file_data)
                '''

                print('grid_mdt_meas complete on %s' %datetime.now())
                print('zipping grid mdt meas.....')

                zip_file = '%s/grid_mdt_meas_cell_%s.zip'%(path_daily, s_date)
                zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
                zf.write(file_cell, os.path.basename(file_cell))
                zf.close()
                os.remove(file)
                os.remove(file_cell)
                '''
                #======================================
                #  Binning 10x10m CQI
                #======================================
                print('   Binning 10mx10m CQI...........')
                size = 10
                grid_size = float(size)
                grid_const = 111.319491666667
                #grid_const = 111.32
                grid_size_deg_long = (grid_size/1000)/grid_const
                grid_size_deg_lat = (grid_size/1000)/grid_const

                #((floor((minx + 180)/(grid_size/111.319491666667))+ 0.5) * (grid_size/111.319491666667)) - 180
                df['long_grid'] =(((df['longitude']/grid_size_deg_long).astype(int) + 0.5)*grid_size_deg_long).astype(float)
                df['lat_grid'] =(((df['latitude']/grid_size_deg_lat).astype(int) + 0.5)*grid_size_deg_lat).astype(float)

                precision = 6
                df['long_grid'] = df['long_grid'].apply(lambda x: round(x, precision))
                df['lat_grid'] = df['lat_grid'].apply(lambda x: round(x, precision))


                df['cqi_90_percl'] = df['cqi_avg']
                df['cqi_75_percl'] = df['cqi_avg']
                df['cqi_mean'] = df['cqi_avg']
                df['cqi_max'] = df['cqi_avg']
                df['cqi_min'] = df['cqi_avg']

                df['sinr_90_percl'] = df['pusch_sinr']
                df['sinr_75_percl'] = df['pusch_sinr']
                df['sinr_mean'] = df['pusch_sinr']
                df['sinr_max'] = df['pusch_sinr']
                df['sinr_min'] = df['pusch_sinr']

                df['rank2_90_percl'] = df['rank2_perc']
                df['rank2_75_percl'] = df['rank2_perc']
                df['rank2_mean'] = df['rank2_perc']
                df['rank2_max'] = df['rank2_perc']
                df['rank2_min'] = df['rank2_perc']

                df_grid_mdt = df.groupby(['date', 'site', 'enodebid','ci','long_grid', 'lat_grid']).agg({'mmes1apid': 'count','cqi_mean':'mean', 'cqi_max':'max', 'cqi_min':'min', 'sinr_mean':'mean', 'sinr_max':'max', 'sinr_min':'min','rank2_mean':'mean', 'rank2_max':'max', 'rank2_min':'min'})
                df_grid_mdt = df_grid_mdt.rename(columns={'mmes1apid': 'sample'})
                df_grid_mdt['grid_size'] = grid_size
                df_grid_mdt.to_csv('%s/grid_meas3.csv'%path_daily)
                file = '%s/grid_meas3.csv' %path_daily
                #del df

                #convert to pandas using df_grid_mdt = df_grid_mdt.compute()
                print('Processing done, continue with pandas on %s' %datetime.now())
                df_grid_mdt = pd.read_csv(file, delimiter=',', index_col=None, header='infer')
                df_grid_mdt = df_grid_mdt[['date', 'site', 'enodebid','ci', 'long_grid', 'lat_grid', 'sample', 'cqi_mean', 'cqi_max', 'cqi_min', 'sinr_mean', 'sinr_max', 'sinr_min', 'rank2_mean', 'rank2_max', 'rank2_min', 'grid_size']]
                file_cell = '%s/grid10_mdt_meas_cell_%s.csv'%(path_daily, s_date)
                
                df_grid_mdt = df_grid_mdt.sort_values(['long_grid', 'lat_grid', 'cqi_mean'], ascending=[True, True, False])
                df_grid_mdt['rank'] = df_grid_mdt.groupby(['long_grid', 'lat_grid'], as_index=False).cumcount()+1
                df_grid_mdt.to_csv(file_cell, index= None)
                '''
                '''
                df_grid_mdt_bestserver = df_grid_mdt.loc[df_grid_mdt['rank'] == 1]
                #df_grid_mdt_bestserver.to_csv('grid_meas2.csv', index=None)

                df_grid_mdt_bestserver[['enodebid','ci']] = df_grid_mdt_bestserver[['enodebid','ci']].apply(pd.to_numeric, errors='coerce')
                #df_grid_mdt_bestserver['cellname'] = (df_grid_mdt_bestserver['enodebid'] * 256) + df_grid_mdt_bestserver['ci']
                #df_grid_mdt_bestserver['cellname'] = df_grid_mdt_bestserver['cellname'].map(dict_cellname)

                df_grid_mdt_bestserver = df_grid_mdt_bestserver[['date', 'site', 'enodebid','ci', 'long_grid', 'lat_grid', 'sample', 'cqi_mean', 'cqi_max', 'cqi_min', 'sinr_mean', 'sinr_max', 'sinr_min', 'rank2_mean', 'rank2_max', 'rank2_min', 'grid_size']]

                file_data = '%s/grid10_mdt_meas_%s.csv'%(path_daily, s_date)
                df_grid_mdt_bestserver.to_csv(file_data, index= None)
                
                zip_file = '%s/grid10_mdt_meas_%s.zip'%(path_daily, s_date)
                zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
                zf.write(file_data, os.path.basename(file_data))
                zf.close()
                os.remove(file_data)
                '''
                '''
                print('grid10_mdt_meas complete on %s' %datetime.now())
                print('zipping grid10_mdt_meas.....')

                zip_file = '%s/grid10_mdt_meas_cell_%s.zip'%(path_daily, s_date)
                zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
                zf.write(file_cell, os.path.basename(file_cell))
                zf.close()
                os.remove(file)
                os.remove(file_cell)
                '''
            elif file_type == 'result_ue_cap':
                print("processing ue cap")
                df_ue = df.groupby(['tanggal','sitename','enodebid','ci'], as_index=False).sum()
                file_ue_cap = '%s/ue_capability_%s.csv'%(path_daily, s_date)
                df_ue.to_csv(file_ue_cap, sep=",", header=True, index=None)

                zip_file = '%s/ue_capability_%s.zip'%(path_daily, s_date)
                zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
                zf.write(file_ue_cap, 'ue_capability_%s.csv'%s_date)
                zf.close()
                os.remove(file_ue_cap)

#    list_file_final = ['grid_mdt_meas_cell', 'grid_mdt_meas', 'grid_mdt_traffic_cell', 'grid_mdt_traffic', 'grid_mdt_cell', 'grid_mdt', 
#                        'ue_capability']
#
#    path_mdt = '/home/supreme/data/mdt/%s' %s_date
#    for file_type in list_file_final:
#        print("Combining %s on %s" %(file_type, datetime.now()))
#        list_df = []
#        for enm in list_enm:
#            path_daily = '/var/opt/decoded_ctr/%s/%s'%(enm, s_date)
#            file = '%s/%s_%s.zip'%(path_daily, file_type, s_date)
#            #print("    Reading %s on %s"%(file, datetime.now()))
#            if os.path.exists(file):
#                df_new = pd.read_csv(file, delimiter=",", index_col=None, header='infer')
#                if len(df_new) > 0:
#                    list_df.append(df_new)
#        if len(list_df) > 0:
#            df = pd.concat(list_df, axis=0)
#            file = '%s/%s_%s.csv'%(path_daily, file_type, s_date)
#            df.to_csv(file, index=None)
#            zip_file = '%s/%s_%s.zip'%(path_daily, file_type, s_date)
#            zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
#            zf.write(file, '%s_%s.csv'%(file_type, s_date))
#            zf.close()
#            os.remove(file)
#            if file_type == 'ue_capability':
#                shutil.copyfile(zip_file, "%s/%s_%s.zip" %(path_uecap, file_type, s_date))
#            else:
#                shutil.copyfile(zip_file, "%s/%s_%s.zip" %(path_mdt, file_type, s_date))

if __name__ == '__main__':
    warnings.filterwarnings('ignore')
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
    for dt in rrule.rrule(rrule.DAILY, dtstart=start_datetime, until=stop_datetime):
        sekarang = datetime.now()
        s_date = dt.strftime("%Y%m%d")
        print("combining MDT %s" %(s_date))

        combine_mdt(s_date)



    
    

    
    

