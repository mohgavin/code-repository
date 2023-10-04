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

def combine_mdt(s_date):
	path = '/var/opt/common5/mdt'
	dict_cellname = {}


	path_daily = '/var/opt/common5/mdt/%s' %s_date
	if not os.path.exists(path_daily):
		os.mkdir(path_daily)

	
	list_file = ['mdt_result', 'ue_traffic', 'ue_meas']
	#list_file = ['mdt_result']
	dict_columns = {}
	dict_columns['mdt_result'] = ['date', 'site', 'enodebid', 'ci', 'longitude', 'latitude', 'mmes1apid', 'rsrp_serving', 'rsrq_serving', 'CA_capable', 'CA_3CC_capable', 'MIMO4x4', 'ENDC', 'NR_SA']
	dict_columns['mdt_result'] = ['date', 'hour', 'site', 'enodebid', 'ci', 'longitude', 'latitude', 'mmes1apid', 'altitude', 'rsrp_serving', 'rsrq_serving']
	dict_columns['ue_traffic'] = []
	dict_columns['ue_meas'] = []
	dict_columns['result_ue_cap'] = []
	list_site = [410259,411060]
	

	for file_type in list_file:
		list_df = []
		for hour in range(24):
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
							df_new = df_new.loc[df_new['enodebid'].isin(list_site)]
							#df_new = df_new.loc[df_new['site'].isin(list_site)]
							if len(df_new) > 0:
								print('   Found %s data for MDT'%len(df_new))
								list_df.append(df_new)
						else:
							df_new = pd.read_csv('%s/%s'%(ta_path,file), delimiter=",", index_col=None, header='infer', usecols=dict_columns[file_type])
							df_new = df_new.loc[df_new['enodebid'].isin(list_site)]
							#df_new = df_new.loc[df_new['site'].isin(list_site)]
							if len(df_new) > 0:
								print('   Found %s data for MDT'%len(df_new))
								list_df.append(df_new)
		if len(list_df) > 0:
			df = pd.concat(list_df, axis=0)
			df.to_csv('%s/filter_%s.csv'%(path_daily, file_type), index=None)
			zip_file = '%s/filter_%s.zip'%(path_daily, file_type)
			zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
			zf.write('%s/filter_%s.csv'%(path_daily, file_type), 'filter_%s.csv'%(file_type))
			zf.close()
			os.remove('%s/filter_%s.csv'%(path_daily, file_type))
			print('Result on %s/filter_%s.csv'%(path_daily, file_type))
			list_site_collected = df['enodebid'].unique().tolist()
			print('total %s sites found'%len(list_site_collected))

if __name__ == '__main__':
    warnings.filterwarnings('ignore')
    tanggal = datetime.now()
    delta_day = 94
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



    
    

    
    

