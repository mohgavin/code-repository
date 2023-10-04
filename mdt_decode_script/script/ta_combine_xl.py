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

def combine_ta(s_date):
	path = '/var/opt/common5/mdt'
	df_ta = pd.DataFrame()
	list_ta = []

	ta_path_day = '%s/%s' %(path, s_date)
	list_df = []
	for hour in range(0,24):
		ta_path = '%s/%02d'%(ta_path_day, hour)
		
		if os.path.exists(ta_path):
			ta_file = '%s/ta_histogram.csv' %(ta_path)
			ta_zip = '%s/ta_histogram.zip' %(ta_path)
			traffic_file = '%s/ue_traffic_result.csv' %(ta_path)
			meas_file = '%s/ue_meas_result.csv' %(ta_path)

			for file in os.listdir(ta_path):
				if 'ta_histogram' in file:
					try:
						file_histogram = '%s/%s'%(ta_path, file)
						print('   Reading TA Histogram %02d on %s'%(hour, datetime.now()))
						df_new = pd.read_csv(file_histogram, delimiter=",", index_col=None, header='infer')
						df_new['tanggal'] = s_date
						df_ta = pd.concat([df_ta, df_new])
						df_ta = df_ta.groupby(['tanggal', 'sitename','enodebid', 'ci', 'ta'], as_index=False).agg({'sample': 'sum'})
					except:
						pass
					
			if os.path.isfile(traffic_file):
				os.remove(traffic_file)                    
			if os.path.isfile(meas_file):
				os.remove(meas_file)
	print("Combining TA Completed.....%s" %datetime.now())
	#target_folder = '/home/tselftp/timing_advance'
	target_folder = '/var/opt/common5/ta'
	file = '%s/ta_histogram_%s.csv'%(target_folder,s_date)
	df_ta.to_csv(file, index=False, header=True)

	calculate_perc_ta(file, s_date)

	df_ta.to_csv(file, index=False, header=True)




	zip_file = "%s/ta_histogram_%s.zip" %(target_folder, s_date)
	zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
	zf.write(file, os.path.basename(file))
	zf.close()
	os.remove(file)



	
	


def calculate_perc_ta(file, s_date):
	try:
		df_ta_2 = pd.read_csv(file, delimiter=",", index_col=None, header='infer')
				
		#Calculating Avg TA
		df_ta_2['ta_distance'] = (df_ta_2['ta'] + 8) * 97.65/20000
		df_ta_2['ta_distance_2'] = (df_ta_2['ta'] + 16) * 97.65/20000
		df_ta_2['sum_ta_distance'] = df_ta_2['ta_distance'] * df_ta_2['sample'] 
		df_ta_2['tanggal'] = s_date
		df_ta_2['id'] =  df_ta_2['tanggal'].astype(str) + '_' + df_ta_2['enodebid'].astype(str) + '_' + df_ta_2['ci'].astype(str)

		df_ta_3 = df_ta_2.groupby(['tanggal','sitename','enodebid','ci'], as_index=False).agg({'sample': 'sum', 'sum_ta_distance': 'sum'})
		df_ta_3['avg_ta_km'] = df_ta_3['sum_ta_distance']/ df_ta_3['sample']
		df_ta_3['id'] =  df_ta_3['tanggal'].astype(str) + '_' + df_ta_3['enodebid'].astype(str) + '_' + df_ta_3['ci'].astype(str)
		dict_sample = df_ta_3.set_index('id').to_dict()['sample']
		dict_mean = df_ta_3.set_index('id').to_dict()['avg_ta_km']

		#Calculating Standard Deviation
		df_ta_2['avg_ta_km'] = df_ta_2['id']
		df_ta_2['avg_ta_km'] = df_ta_2['avg_ta_km'].map(dict_mean)
		df_ta_2['delta_x'] = df_ta_2['ta_distance'] - df_ta_2['avg_ta_km']
		df_ta_2['delta_x'] = df_ta_2['delta_x'] * df_ta_2['delta_x']
		df_ta_2['sum_delta_x'] = df_ta_2['delta_x'] * df_ta_2['sample'] 

		df_ta_6 = df_ta_2.groupby(['tanggal','sitename','enodebid','ci','id'], as_index=False).agg({'sample': 'sum', 'sum_delta_x': 'sum'})
		df_ta_6['std'] = df_ta_6['sum_delta_x']/ df_ta_6['sample']
		df_ta_6['std'] = df_ta_6['std'] ** 0.5
		dict_std = df_ta_6.set_index('id').to_dict()['std']
		
		#create pdf calculation
		df_ta_2['total_sample'] = df_ta_2['id']
		df_ta_2['total_sample'] = df_ta_2['total_sample'].map(dict_sample)
		df_ta_2['ta_perc'] = 100.0 * df_ta_2['sample'] / df_ta_2['total_sample']
		df_ta_2['pdf'] = df_ta_2.groupby(['id'])['ta_perc'].cumsum()
		
		#percentile 50
		df_ta_4 = df_ta_2.loc[df_ta_2['pdf']>= 50]
		df_ta_4['sort'] = df_ta_4.groupby('id').cumcount()+1
		df_ta_5 = df_ta_4.loc[df_ta_4['sort']== 1]
		dict_perc_50 = df_ta_5.set_index('id').to_dict()['ta_distance']

		df_ta_3['perc50_ta_distance_km'] = df_ta_3['id']
		df_ta_3['perc50_ta_distance_km'] = df_ta_3['perc50_ta_distance_km'].map(dict_perc_50)

		#percentile 80
		df_ta_4 = df_ta_2.loc[df_ta_2['pdf']>= 80]
		df_ta_4['sort'] = df_ta_4.groupby('id').cumcount()+1
		df_ta_5 = df_ta_4.loc[df_ta_4['sort']== 1]
		dict_perc_80 = df_ta_5.set_index('id').to_dict()['ta_distance']

		df_ta_3['perc80_ta_distance_km'] = df_ta_3['id']
		df_ta_3['perc80_ta_distance_km'] = df_ta_3['perc80_ta_distance_km'].map(dict_perc_80)

		#percentile 90
		df_ta_4 = df_ta_2.loc[df_ta_2['pdf']>= 90]
		df_ta_4['sort'] = df_ta_4.groupby('id').cumcount()+1
		df_ta_5 = df_ta_4.loc[df_ta_4['sort']== 1]
		dict_perc_90 = df_ta_5.set_index('id').to_dict()['ta_distance']

		df_ta_3['perc90_ta_distance_km'] = df_ta_3['id']
		df_ta_3['perc90_ta_distance_km'] = df_ta_3['perc90_ta_distance_km'].map(dict_perc_90)

		#percentile 95
		df_ta_4 = df_ta_2.loc[df_ta_2['pdf']>= 95]
		df_ta_4['sort'] = df_ta_4.groupby('id').cumcount()+1
		df_ta_5 = df_ta_4.loc[df_ta_4['sort']== 1]
		dict_perc_95 = df_ta_5.set_index('id').to_dict()['ta_distance']

		df_ta_3['perc95_ta_distance_km'] = df_ta_3['id']
		df_ta_3['perc95_ta_distance_km'] = df_ta_3['perc95_ta_distance_km'].map(dict_perc_95)

		#percentile 98
		df_ta_4 = df_ta_2.loc[df_ta_2['pdf']>= 98]
		df_ta_4['sort'] = df_ta_4.groupby('id').cumcount()+1
		df_ta_5 = df_ta_4.loc[df_ta_4['sort']== 1]
		dict_perc_98 = df_ta_5.set_index('id').to_dict()['ta_distance']

		df_ta_3['perc98_ta_distance_km'] = df_ta_3['id']
		df_ta_3['perc98_ta_distance_km'] = df_ta_3['perc98_ta_distance_km'].map(dict_perc_98)

		#percentile 99
		df_ta_4 = df_ta_2.loc[df_ta_2['pdf']>= 99]
		df_ta_4['sort'] = df_ta_4.groupby('id').cumcount()+1
		df_ta_5 = df_ta_4.loc[df_ta_4['sort']== 1]
		dict_perc_99 = df_ta_5.set_index('id').to_dict()['ta_distance']

		df_ta_3['perc99_ta_distance_km'] = df_ta_3['id']
		df_ta_3['perc99_ta_distance_km'] = df_ta_3['perc99_ta_distance_km'].map(dict_perc_99)

		#percentile 99.5
		df_ta_4 = df_ta_2.loc[df_ta_2['pdf']>= 99.5]
		df_ta_4['sort'] = df_ta_4.groupby('id').cumcount()+1
		df_ta_5 = df_ta_4.loc[df_ta_4['sort']== 1]
		dict_perc_995 = df_ta_5.set_index('id').to_dict()['ta_distance']

		df_ta_3['perc99_5_ta_distance_km'] = df_ta_3['id']
		df_ta_3['perc99_5_ta_distance_km'] = df_ta_3['perc99_5_ta_distance_km'].map(dict_perc_995)

		#percentile 85
		df_ta_4 = df_ta_2.loc[df_ta_2['pdf']>= 85]
		df_ta_4['sort'] = df_ta_4.groupby('id').cumcount()+1
		df_ta_5 = df_ta_4.loc[df_ta_4['sort']== 1]
		dict_perc_85 = df_ta_5.set_index('id').to_dict()['ta_distance']

		df_ta_3['perc85_ta_distance_km'] = df_ta_3['id']
		df_ta_3['perc85_ta_distance_km'] = df_ta_3['perc85_ta_distance_km'].map(dict_perc_85)

		#percentile 97.5
		df_ta_4 = df_ta_2.loc[df_ta_2['pdf']>= 97.5]
		df_ta_4['sort'] = df_ta_4.groupby('id').cumcount()+1
		df_ta_5 = df_ta_4.loc[df_ta_4['sort']== 1]
		dict_perc_975 = df_ta_5.set_index('id').to_dict()['ta_distance']

		df_ta_3['perc97_5_ta_distance_km'] = df_ta_3['id']
		df_ta_3['perc97_5_ta_distance_km'] = df_ta_3['perc97_5_ta_distance_km'].map(dict_perc_975)

		#percentile 98.6
		df_ta_4 = df_ta_2.loc[df_ta_2['pdf']>= 98.6]
		df_ta_4['sort'] = df_ta_4.groupby('id').cumcount()+1
		df_ta_5 = df_ta_4.loc[df_ta_4['sort']== 1]
		dict_perc_986 = df_ta_5.set_index('id').to_dict()['ta_distance']
		
		df_ta_3['perc98_6_ta_distance_km'] = df_ta_3['id']
		df_ta_3['perc98_6_ta_distance_km'] = df_ta_3['perc98_6_ta_distance_km'].map(dict_perc_986)

		#percentile 99.9
		df_ta_4 = df_ta_2.loc[df_ta_2['pdf']>= 99.9]
		df_ta_4['sort'] = df_ta_4.groupby('id').cumcount()+1
		df_ta_5 = df_ta_4.loc[df_ta_4['sort']== 1]
		dict_perc_999 = df_ta_5.set_index('id').to_dict()['ta_distance']

		df_ta_3['perc99_9_ta_distance_km'] = df_ta_3['id']
		df_ta_3['perc99_9_ta_distance_km'] = df_ta_3['perc99_9_ta_distance_km'].map(dict_perc_999)

		print("calculating distribution ta...")
		#percentage sample less than 300 m
		df_ta_4 = df_ta_2.loc[df_ta_2['ta_distance_2']<= 0.3]
		df_ta_5 = df_ta_4.groupby('id', as_index=False).agg({'ta_perc': 'sum'})
		dict_sample_300m = df_ta_5.set_index('id').to_dict()['ta_perc']
		df_ta_3['perc_300'] = df_ta_3['id']
		df_ta_3['perc_300'] = df_ta_3['perc_300'].map(dict_sample_300m)
		df_ta_3['perc_300'] = df_ta_3['perc_300'].fillna(0)

		#percentage sample 300m - 500m
		df_ta_4 = df_ta_2.loc[df_ta_2['ta_distance_2']<= 0.5]
		df_ta_5 = df_ta_4.groupby('id', as_index=False).agg({'ta_perc': 'sum'})
		dict_sample_500m = df_ta_5.set_index('id').to_dict()['ta_perc']		
		df_ta_3['perc_500'] = df_ta_3['id']
		df_ta_3['perc_500'] = df_ta_3['perc_500'].map(dict_sample_500m)
		df_ta_3['perc_500'] = df_ta_3['perc_500'].fillna(0)
		df_ta_3['perc_500'] = df_ta_3['perc_500'] - df_ta_3['perc_300']

		#percentage sample 500m - 700m
		df_ta_4 = df_ta_2.loc[df_ta_2['ta_distance_2']<= 0.7]
		df_ta_5 = df_ta_4.groupby('id', as_index=False).agg({'ta_perc': 'sum'})
		dict_sample_700m = df_ta_5.set_index('id').to_dict()['ta_perc']		
		df_ta_3['perc_700'] = df_ta_3['id']
		df_ta_3['perc_700'] = df_ta_3['perc_700'].map(dict_sample_700m)
		df_ta_3['perc_700'] = df_ta_3['perc_700'].fillna(0)
		df_ta_3['perc_700'] = df_ta_3['perc_700'] - df_ta_3['perc_500'] - df_ta_3['perc_300']

		#percentage sample 700m - 1000m
		df_ta_4 = df_ta_2.loc[df_ta_2['ta_distance_2']<= 1.0]
		df_ta_5 = df_ta_4.groupby('id', as_index=False).agg({'ta_perc': 'sum'})
		dict_sample_1km = df_ta_5.set_index('id').to_dict()['ta_perc']
		df_ta_3['perc_1000'] = df_ta_3['id']
		df_ta_3['perc_1000'] = df_ta_3['perc_1000'].map(dict_sample_1km)
		df_ta_3['perc_1000'] = df_ta_3['perc_1000'].fillna(0)
		df_ta_3['perc_1000'] = df_ta_3['perc_1000'] - df_ta_3['perc_700'] - df_ta_3['perc_500'] - df_ta_3['perc_300']

		df_ta_4 = df_ta_2.loc[df_ta_2['ta_distance_2']<= 1.5]
		df_ta_5 = df_ta_4.groupby('id', as_index=False).agg({'ta_perc': 'sum'})
		dict_sample_1500m = df_ta_5.set_index('id').to_dict()['ta_perc']		
		df_ta_3['perc_1500'] = df_ta_3['id']
		df_ta_3['perc_1500'] = df_ta_3['perc_1500'].map(dict_sample_1500m)
		df_ta_3['perc_1500'] = df_ta_3['perc_1500'].fillna(0)
		df_ta_3['perc_1500'] = df_ta_3['perc_1500'] - df_ta_3['perc_1000'] - df_ta_3['perc_700'] - df_ta_3['perc_500'] - df_ta_3['perc_300']

		df_ta_4 = df_ta_2.loc[df_ta_2['ta_distance_2']<= 2.0]
		df_ta_5 = df_ta_4.groupby('id', as_index=False).agg({'ta_perc': 'sum'})
		dict_sample_2000m = df_ta_5.set_index('id').to_dict()['ta_perc']		
		df_ta_3['perc_2000'] = df_ta_3['id']
		df_ta_3['perc_2000'] = df_ta_3['perc_2000'].map(dict_sample_2000m)
		df_ta_3['perc_2000'] = df_ta_3['perc_2000'] - df_ta_3['perc_1500'] - df_ta_3['perc_1000'] - df_ta_3['perc_700'] - df_ta_3['perc_500'] - df_ta_3['perc_300']

		df_ta_4 = df_ta_2.loc[df_ta_2['ta_distance_2']<= 3.0]
		df_ta_5 = df_ta_4.groupby('id', as_index=False).agg({'ta_perc': 'sum'})
		dict_sample_3000m = df_ta_5.set_index('id').to_dict()['ta_perc']		
		df_ta_3['perc_3000'] = df_ta_3['id']
		df_ta_3['perc_3000'] = df_ta_3['perc_3000'].map(dict_sample_3000m)
		df_ta_3['perc_3000'] = df_ta_3['perc_3000'] - df_ta_3['perc_2000']-df_ta_3['perc_1500'] - df_ta_3['perc_1000'] - df_ta_3['perc_700'] - df_ta_3['perc_500'] - df_ta_3['perc_300']

		df_ta_4 = df_ta_2.loc[df_ta_2['ta_distance_2']<= 5.0]
		df_ta_5 = df_ta_4.groupby('id', as_index=False).agg({'ta_perc': 'sum'})
		dict_sample_5000m = df_ta_5.set_index('id').to_dict()['ta_perc']		
		df_ta_3['perc_5000'] = df_ta_3['id']
		df_ta_3['perc_5000'] = df_ta_3['perc_5000'].map(dict_sample_5000m)
		df_ta_3['perc_5000'] = df_ta_3['perc_5000'] - df_ta_3['perc_3000'] - df_ta_3['perc_2000']-df_ta_3['perc_1500'] - df_ta_3['perc_1000'] - df_ta_3['perc_700'] - df_ta_3['perc_500'] - df_ta_3['perc_300']

		df_ta_4 = df_ta_2.loc[df_ta_2['ta_distance_2']<= 10.0]
		df_ta_5 = df_ta_4.groupby('id', as_index=False).agg({'ta_perc': 'sum'})
		dict_sample_10000m = df_ta_5.set_index('id').to_dict()['ta_perc']		
		df_ta_3['perc_10000'] = df_ta_3['id']
		df_ta_3['perc_10000'] = df_ta_3['perc_10000'].map(dict_sample_10000m)
		df_ta_3['perc_10000'] = df_ta_3['perc_10000'] - df_ta_3['perc_5000'] - df_ta_3['perc_3000'] - df_ta_3['perc_2000']-df_ta_3['perc_1500'] - df_ta_3['perc_1000'] - df_ta_3['perc_700'] - df_ta_3['perc_500'] - df_ta_3['perc_300']

		df_ta_4 = df_ta_2.loc[df_ta_2['ta_distance_2']<= 15.0]
		df_ta_5 = df_ta_4.groupby('id', as_index=False).agg({'ta_perc': 'sum'})
		dict_sample_15000m = df_ta_5.set_index('id').to_dict()['ta_perc']		
		df_ta_3['perc_15000'] = df_ta_3['id']
		df_ta_3['perc_15000'] = df_ta_3['perc_15000'].map(dict_sample_15000m)
		df_ta_3['perc_15000'] = df_ta_3['perc_15000'] - df_ta_3['perc_10000']  - df_ta_3['perc_5000'] - df_ta_3['perc_3000'] - df_ta_3['perc_2000']-df_ta_3['perc_1500'] - df_ta_3['perc_1000'] - df_ta_3['perc_700'] - df_ta_3['perc_500'] - df_ta_3['perc_300']

		df_ta_4 = df_ta_2.loc[df_ta_2['ta_distance_2']<= 30.0]
		df_ta_5 = df_ta_4.groupby('id', as_index=False).agg({'ta_perc': 'sum'})
		dict_sample_30000m = df_ta_5.set_index('id').to_dict()['ta_perc']		
		df_ta_3['perc_30000'] = df_ta_3['id']
		df_ta_3['perc_30000'] = df_ta_3['perc_30000'].map(dict_sample_30000m)
		df_ta_3['perc_30000'] = df_ta_3['perc_30000'] - df_ta_3['perc_15000'] - df_ta_3['perc_10000']  - df_ta_3['perc_5000'] - df_ta_3['perc_3000'] - df_ta_3['perc_2000']-df_ta_3['perc_1500'] - df_ta_3['perc_1000'] - df_ta_3['perc_700'] - df_ta_3['perc_500'] - df_ta_3['perc_300']
		df_ta_3 = df_ta_3.round({"perc_300":2, "perc_500":2, "perc_700":2, "perc_1000":2, "perc_1500":2, "perc_2000":2, "perc_3000":2, "perc_5000":2, "perc_10000":2, "perc_15000":2, "perc_30000":2, "avg_ta_km":2, "perc95_ta_distance_km":2, "perc98_ta_distance_km":2, "perc99_ta_distance_km":2, "perc99_5_ta_distance_km":2}) 
		#df_ta_3 = df_ta_3.round({"perc_300":2, "perc_700":2, "perc_1000":2, "perc_1500":2, "avg_ta_km":2, "perc95_ta_distance_km":2, "perc98_ta_distance_km":2}) 

		df_ta_3['std'] = df_ta_3['id']
		df_ta_3['std'] = df_ta_3['std'].map(dict_std)
		df_ta_3['enodebid'] = pd.to_numeric(df_ta_3['enodebid'], errors='coerce', downcast='integer')
		df_ta_3['ci'] = pd.to_numeric(df_ta_3['ci'], errors='coerce', downcast='integer')
		df_ta_3['eci'] = (df_ta_3['enodebid'] * 256) + (df_ta_3['ci'])
		print("write ta_stat.csv")
		df_ta_1 = df_ta_3[['tanggal','sitename','enodebid','ci','avg_ta_km','std','perc50_ta_distance_km','perc80_ta_distance_km','perc85_ta_distance_km','perc90_ta_distance_km','perc95_ta_distance_km','perc97_5_ta_distance_km','perc98_ta_distance_km','perc98_6_ta_distance_km', 'perc99_ta_distance_km', 'perc99_5_ta_distance_km','perc99_9_ta_distance_km','perc_300','perc_500','perc_700','perc_1000','perc_1500','perc_2000','perc_3000','perc_5000','perc_10000','perc_15000','perc_30000','sample']]
		df_ta_1.to_csv("/var/opt/common5/ta/ta_stat_%s.csv" % s_date,sep=',',index=False)

		del df_ta_3
		del df_ta_4
		del df_ta_5
		del df_ta_2
		del df_ta_1
	except Exception as e:
		print("error: %s" %(str(e)))
		pass

if __name__ == '__main__':
    warnings.filterwarnings("ignore")
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
        print("combining TA %s" %(s_date))
        combine_ta(s_date)



    
    

    
    

