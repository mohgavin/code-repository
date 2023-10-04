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

	
	list_file = ['mdt_result', 'ue_traffic']
	dict_columns = {}
	dict_columns['mdt_result'] = ['date', 'hour', 'enodebid', 'ci', 'mmes1apid','longitude', 'latitude', 'altitude', 'rsrp_serving']
	dict_columns['mdt_result'] = ['hour', 'mmes1apid','longitude', 'latitude', 'rsrp_serving']
	dict_columns['ue_traffic'] = ['date', 'hour', 'enodebid', 'ci', 'mmes1apid','longitude', 'latitude', 'ue_throughput_dl_drb_kbps']
	dict_columns['ue_traffic'] = ['hour', 'mmes1apid','longitude', 'latitude', 'ue_throughput_dl_drb_kbps']
	dict_columns['ue_meas'] = []
	dict_columns['result_ue_cap'] = []

	for file_type in list_file:
		list_df = []
		busy_hour = [23,0,1,2,3,10,11,12,13]
		for hour in range(24):
			print("-----Getting hour:%s data------" %hour)
			hour = '%02d'%hour
			ta_path = '%s/%s'%(path_daily, hour)
			#print("   Checking %s"%ta_path)
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
						if len(dict_columns[file_type]) == 0:
							df_new = pd.read_csv('%s/%s'%(ta_path,file), delimiter=",", index_col=None, header='infer')
							print("     Reading %s/%s" %(ta_path,file))
							
							if file_type != 'mdt_result':
								df_new['hour'] = df_new['hour'].astype('str')
								df_new['hour'] = df_new['hour'].str.split('.').str[0]
								df_new['hour'] = df_new['hour'].astype('int')
								print('			All Hour %s' %df_new['hour'].unique())
								df_new = df_new.loc[df_new['hour'].isin(busy_hour)]
								print('			Available BH %s' %df_new['hour'].unique())
							elif file_type == 'mdt_result':
								print('			All Hour %s' %df_new['hour'].unique())
								df_new = df_new.loc[df_new['hour'].isin(busy_hour)]
								print('			Available BH %s' %df_new['hour'].unique())

							if len(df_new) > 0:
								print('   Found %s data for MDT'%len(df_new))
								list_df.append(df_new)
						else:
							df_new = pd.read_csv('%s/%s'%(ta_path,file), delimiter=",", index_col=None, header='infer', usecols=dict_columns[file_type])
							print("     Reading %s/%s" %(ta_path,file))

							if file_type != 'mdt_result':
								df_new['hour'] = df_new['hour'].astype('str')
								df_new['hour'] = df_new['hour'].str.split('.').str[0]
								df_new['hour'] = df_new['hour'].astype('int')
								print('			All Hour %s' %df_new['hour'].unique())
								df_new = df_new.loc[df_new['hour'].isin(busy_hour)]
								print('			Available BH %s' %df_new['hour'].unique())
							elif file_type == 'mdt_result':
								print('			All Hour %s' %df_new['hour'].unique())
								df_new = df_new.loc[df_new['hour'].isin(busy_hour)]
								print('			Available BH %s' %df_new['hour'].unique())

							if len(df_new) > 0:
								print('   Found %s data for MDT'%len(df_new))
								list_df.append(df_new)
		if len(list_df) > 0:
			print('Combining BH data')
			df = pd.concat(list_df, axis=0)
			
			if file_type == 'mdt_result':
				print('%s initial raw: %s' %(file_type, len(df)))
				#df = df.loc[df['ci'] < 257]
				print('%s valid raw: %s' %(file_type, len(df)))
				print('Available BH for %s: %s' %(file_type, df['hour'].unique()))

				df = df.loc[pd.notnull(df['longitude'])]
				df = df.loc[pd.notnull(df['latitude'])]
				df['longitude'] = pd.to_numeric(df["longitude"], errors='coerce')
				df['latitude'] = pd.to_numeric(df["latitude"], errors='coerce')

				# quadkey binning
				print("     transform coordinate to quadkey17")
				df["coordinate"] = df[["latitude", "longitude"]].apply(tuple, axis=1)
				df['quadkey17'] = df['coordinate'].apply(lambda x: quadkey.from_geo(x, 17))
				df['quadkey17'] = df['quadkey17'].astype('str')
				df.drop(['coordinate'], axis=1, inplace=True)
				print("         ....done")
				
				df['rsrp_mean'] = df['rsrp_serving']
				df_grid_mdt = df.groupby(['quadkey17']).agg({'mmes1apid': 'count','rsrp_mean':'mean'})
				df_grid_mdt = df_grid_mdt.rename(columns={'mmes1apid': 'sample'})
				df_grid_mdt.to_csv('%s/grid_mdt_meas3.csv' %path_daily)
				file = '%s/grid_mdt_meas3.csv' %path_daily

				print('Processing done, continue with pandas on %s' %datetime.now())
				df_grid_mdt = pd.read_csv(file, delimiter=',', index_col=None, header='infer')
				file_cell = '%s/scanning_mdt_%s.csv'%(path_daily, s_date)

				df_grid_mdt = df_grid_mdt.sort_values(['quadkey17', 'rsrp_mean'], ascending=[True, False])
				df_grid_mdt['rank'] = df_grid_mdt.groupby(['quadkey17'], as_index=False).cumcount()+1
				df_grid_mdt =  df_grid_mdt.loc[df_grid_mdt['rank'] == 1]
				df_grid_mdt.drop(['rank'], axis=1, inplace=True)

				df_grid_mdt.to_csv(file_cell, index= None)

				print('scanning_mdt complete on %s' %datetime.now())
				print('zipping grid_mdt.....')
				zip_file = '%s/scanning_mdt_%s.zip'%(path_daily, s_date)
				zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
				zf.write(file_cell, os.path.basename(file_cell))
				zf.close()
				os.remove(file)
				os.remove(file_cell)

			elif file_type == 'ue_traffic':
				print('%s initial raw: %s' %(file_type, len(df)))
				#df = df.loc[df['ci'] < 257]
				print('%s valid raw: %s' %(file_type, len(df)))
				print('Available BH for %s: %s' %(file_type, df['hour'].unique()))

				df = df.loc[pd.notnull(df['longitude'])]
				df = df.loc[pd.notnull(df['latitude'])]
				df['longitude'] = pd.to_numeric(df["longitude"], errors='coerce')
				df['latitude'] = pd.to_numeric(df["latitude"], errors='coerce')

				# quadkey binning
				print("     transform coordinate to quadkey17")
				df["coordinate"] = df[["latitude", "longitude"]].apply(tuple, axis=1)
				df['quadkey17'] = df['coordinate'].apply(lambda x: quadkey.from_geo(x, 17))
				df['quadkey17'] = df['quadkey17'].astype('str')
				df.drop(['coordinate'], axis=1, inplace=True)
				print("         ....done")
				
				df['throughput_dl_mean'] = df['ue_throughput_dl_drb_kbps']
				df_grid_mdt = df.groupby(['quadkey17']).agg({'mmes1apid': 'count','throughput_dl_mean':'mean'})
				df_grid_mdt = df_grid_mdt.rename(columns={'mmes1apid': 'sample'})
				df_grid_mdt.to_csv('%s/grid_mdt_meas3.csv' %path_daily)
				file = '%s/grid_mdt_meas3.csv' %path_daily

				print('Processing done, continue with pandas on %s' %datetime.now())
				df_grid_mdt = pd.read_csv(file, delimiter=',', index_col=None, header='infer')
				file_cell = '%s/scanning_traffic_%s.csv'%(path_daily, s_date)

				df_grid_mdt = df_grid_mdt.sort_values(['quadkey17', 'throughput_dl_mean'], ascending=[True, False])
				df_grid_mdt['rank'] = df_grid_mdt.groupby(['quadkey17'], as_index=False).cumcount()+1
				df_grid_mdt =  df_grid_mdt.loc[df_grid_mdt['rank'] == 1]
				df_grid_mdt.drop(['rank'], axis=1, inplace=True)

				df_grid_mdt.to_csv(file_cell, index= None)

				print('scanning_traffic complete on %s' %datetime.now())
				print('zipping grid_mdt.....')
				zip_file = '%s/scanning_traffic_%s.zip'%(path_daily, s_date)
				zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
				zf.write(file_cell, os.path.basename(file_cell))
				zf.close()
				os.remove(file)
				os.remove(file_cell)

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

