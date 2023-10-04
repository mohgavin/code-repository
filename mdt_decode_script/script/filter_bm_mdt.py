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

def get_bm_mdt(s_date):
	path = '/var/opt/common5/mdt'
	list_enm = ['enm7', 'enm8', 'enm9', 'osskal']
	list_hour = ['00', '01', '02', '03', '04', '05',  '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21','22', '23']

	df_ta = pd.DataFrame()
	list_ta = []

	ta_path_day = '%s/%s' %(path, s_date)
	list_df = []
	for hour in range(0,24):
		ta_path = '%s/%02d'%(ta_path_day, hour)
		
		if os.path.exists(ta_path):
			mdt_file = '%s/mdt_result.zip' %(ta_path)

			if os.path.isfile(mdt_file):
				try:
					df_new = pd.read_csv(mdt_file, delimiter=",", index_col=None, header='infer')
					#print('   Reading file %s'%mdt_file)
					#print('    %s'%(df_new['earfcn_neigh'].unique()))
					
					#indosat
					#df2 = df_new.loc[df_new['earfcn_neigh'].isin([1625, 525, 3652])]
					
					#telkomsel
					df2 = df_new.loc[df_new['earfcn_neigh'].isin([1850, 225, 3500, 38750, 38948, 1625, 525, 3652])]
					
					if len(df2) > 0:
						df2 = df2[['date', 'hour', 'site', 'enodebid', 'ci', 'mmes1apid', 'longitude', 'latitude', 'altitude', 'rsrp_serving', 'rsrq_serving', 'earfcn_neigh', 'pci_1', 'rsrp_1', 'rsrq_1']]
						list_df.append(df2)
						print('    mdt found on %s'%ta_path)
				except Exception as e:
					print("Error: %s"%e)
					#pass
	if len(list_df) > 0:
		df = pd.concat(list_df, sort=False)
		result = '%s/mdt_bm_%s.csv'%(ta_path_day, s_date)
		df.to_csv(result, index=None)

		zip_file = '%s/mdt_bm_%s.zip'%(ta_path_day, s_date)
		zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
		zf.write(result, os.path.basename(result))
		zf.close()
		os.remove(result)

		#df.to_csv('mdt_bm_%s.csv'%(s_date), index=None)
		print('result on %s'%zip_file)





	
	


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
        print("get MDT BM %s" %(s_date))
        get_bm_mdt(s_date)



    
    

    
    

