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
	list_site = [431015,470120]
	#list_site = ['4424974E', '4424739E', '441PX427E_CO', '441PL390E_CO', '441PL065E_CO', 'MC4431290E', 'SH3421104G_CO', '5018G_CO', '4424949E_CO', '441PC957E', '441PC430E_CO', '4410786E_CO', '441PX077E', '441PX087E', '441PL026E_CO', '4424902E', '442PL658E', '4424933E', '4425013E', '441PC256E_CO', '4425498E_BB', '4425007E_CO', 'V061G_CO', '441PL503E_CO', '441PC164E', 'SH4431545E', '341PC995G_CO', '442PC019E', '441PL074E_CO', '441PC902E', '441PL609E_CO', '341PX400G_CO', '5022G_CO', '442PX4803E', '441PX371E', '441PX910E_CO', '442PC087E', '441PL060E_CO', '441PL461E', '441PC199E_CO', '441PL384E_CO', '4424974E_BB', '441PX078E', '441PX094E_CO', 'MC4424718E', '4433055E', '341SC996G_CO', '441PX323E', '441PC960E_CO', '442PC009E_CO', '5016G_CO', '441PX364E_CO', '442PX349E_CO', '441PC439E', '442PC032E', '5015G_CO', '442PC472E', '4433058E', '5022G_C2', '441PC498E_CO', '442PC225E', '441PC173E', '3412390G_CO', '441PC402E', '5265G_CO', '441PC083E', 'SH4412317E_CO', '442PC179E', '4435480E', '3412296G_CO', '441A088E_CO', '441PC039E', '341PC251G_CO', '441PC124E', 'MC4415798E', '441PC805E', '442PC079E', '441PX029E']


	for file_type in list_file:
		list_df = []
		for  hour in range(24):
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
    delta_day = 6
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



    
    

    
    

