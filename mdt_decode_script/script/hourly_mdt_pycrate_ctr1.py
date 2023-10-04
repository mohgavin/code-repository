import gzip
import zipfile	
from zipfile import ZipFile
import shutil
import time
from datetime import datetime, timedelta
from subprocess import call
import os
import sys
import multiprocessing
import pandas as pd
import numpy as np
from pycrate_asn1dir import RRCLTE
from dateutil import rrule
from binascii import unhexlify, hexlify
import warnings


def get_coordinate(spoint):
	if spoint != '':
		lat = spoint[1:4]
		long = spoint[4:7]
		nlat = int.from_bytes(lat, byteorder='big', signed=False)
		#Threshold for first bit is x7FFFFF (8388607), if > 8388607 then bit 0 = 1 --> South (negative latitude)
		if nlat > 8388607:
			ilat = -1 * (nlat - 8388608) / (2. ** 23) * 90
		else:
			ilat = nlat / (2. ** 23 -1) * 90
		
		nlong = int.from_bytes(long, byteorder='big', signed=False)
		if nlong <= 8388607: #east or west (-1)
			ilong = -1.0 * nlong / (2. ** 23) * 180
		else:
			ilong = (nlong- 8388608) / (2. ** 23 - 1)*180
		
	else:
		ilong = None
		ilat = None
	return ilong, ilat

def get_altitude(spoint):
	if spoint != '':
		altitude = spoint[7:9]
		n_altitude = int.from_bytes(altitude, byteorder='big', signed=False)
		#Threshold for first bit is x7FFF (32767), if > 32767 then bit 0 = 1 --> nol = Altitude expresses height, 1 = Altitude expresses depth
		if n_altitude > 32767:
			i_altitude = -1 * (n_altitude - 32768)
		else:
			i_altitude = n_altitude		
	else:
		i_altitude = None
	return i_altitude

def get_file_infolder_ue_cap(path):
	filenames = []
	for file in os.listdir(path):
		if file.endswith("csv"):
			if file.startswith("ue_cap"):
				filenames.append("%s/%s"%(path,file))
	return filenames

def get_rsrp(srsrp):
	if srsrp < 128:
		irsrp = srsrp - 140
	else:
		irsrp = ''
	return irsrp

def get_rsrq(srsrq):
	if srsrq < 128:
		irsrq = 0.5 * srsrq - 19.5
	else:
		irsrq = ''
	return irsrq
def get_pci(pci):
	if pci < 32768:
		ipci = pci
	else:
		ipci = ''
	return ipci

def Distance(lat1,lon1,lat2,lon2):
	from pyproj import Geod

	wgs84_geod = Geod(ellps='WGS84')
	az12,az21,dist = wgs84_geod.inv(lon1,lat1,lon2,lat2) #Yes, this order is correct
	return dist

def calculateIsd(file_gcell, file_isd):
	from pyproj import Geod
	from shapely.geometry import Point, Polygon
	import geopandas
	import shapely
	from shapely import wkt
	import shapely.speedups as sp

	selected_columns = ['cellname','enodeb_id', 'ci', 'siteid', 'type','longitude','latitude','azimuth']
	df_gcell = pd.read_csv(file_gcell, delimiter=",", index_col=None, header='infer', usecols=selected_columns)
	df_gcell = df_gcell.loc[pd.notnull(df_gcell['longitude'])]
	df_gcell = df_gcell.loc[pd.notnull(df_gcell['latitude'])]
	if 'type' in df_gcell.columns:
		df_gcell['type'] = df_gcell['type'].str.lower()
		df_gcell = df_gcell.loc[df_gcell['type'] != 'indoor']
	df_gcell = df_gcell.loc[df_gcell['type'] != 'indoor']
	df_site = df_gcell.groupby('siteid', as_index=False).agg({'longitude': 'mean', 'latitude':'mean'})

	#Create Point
	gdf_site = geopandas.GeoDataFrame(
					df_site, geometry=geopandas.points_from_xy(df_site.longitude, df_site.latitude))
	print("site point created...")
	#Create Beam Search
	radius = 6.0 / 111.32
	beam = 130
	half_beam = beam/2
	df_gcell['coordinate0'] = list(zip(df_gcell.longitude, df_gcell.latitude))
	df_gcell['coordinate1'] = list(zip(df_gcell.longitude + radius * np.sin(np.deg2rad(df_gcell.azimuth - half_beam)), df_gcell.latitude + radius * np.cos(np.deg2rad(df_gcell.azimuth - half_beam))))
	df_gcell['coordinate2'] = list(zip(df_gcell.longitude + radius * np.sin(np.deg2rad(df_gcell.azimuth - (0.8 * half_beam))), df_gcell.latitude + radius * np.cos(np.deg2rad(df_gcell.azimuth - (0.8 * half_beam)))))
	df_gcell['coordinate3'] = list(zip(df_gcell.longitude + radius * np.sin(np.deg2rad(df_gcell.azimuth - (0.6 * half_beam))), df_gcell.latitude + radius * np.cos(np.deg2rad(df_gcell.azimuth - (0.6 * half_beam)))))
	df_gcell['coordinate4'] = list(zip(df_gcell.longitude + radius * np.sin(np.deg2rad(df_gcell.azimuth - (0.4 * half_beam))), df_gcell.latitude + radius * np.cos(np.deg2rad(df_gcell.azimuth - (0.4 * half_beam)))))
	df_gcell['coordinate5'] = list(zip(df_gcell.longitude + radius * np.sin(np.deg2rad(df_gcell.azimuth - (0.2 * half_beam))), df_gcell.latitude + radius * np.cos(np.deg2rad(df_gcell.azimuth - (0.2 * half_beam)))))
	df_gcell['coordinate6'] = list(zip(df_gcell.longitude + radius * np.sin(np.deg2rad(df_gcell.azimuth)), df_gcell.latitude + radius * np.cos(np.deg2rad(df_gcell.azimuth))))
	df_gcell['coordinate7'] = list(zip(df_gcell.longitude + radius * np.sin(np.deg2rad(df_gcell.azimuth + (0.2 * half_beam))), df_gcell.latitude + radius * np.cos(np.deg2rad(df_gcell.azimuth + (0.2 * half_beam)))))
	df_gcell['coordinate8'] = list(zip(df_gcell.longitude + radius * np.sin(np.deg2rad(df_gcell.azimuth + (0.4 * half_beam))), df_gcell.latitude + radius * np.cos(np.deg2rad(df_gcell.azimuth + (0.4 * half_beam)))))
	df_gcell['coordinate9'] = list(zip(df_gcell.longitude + radius * np.sin(np.deg2rad(df_gcell.azimuth + (0.6 * half_beam))), df_gcell.latitude + radius * np.cos(np.deg2rad(df_gcell.azimuth + (0.6 * half_beam)))))
	df_gcell['coordinate10'] = list(zip(df_gcell.longitude + radius * np.sin(np.deg2rad(df_gcell.azimuth + (0.8 * half_beam))), df_gcell.latitude + radius * np.cos(np.deg2rad(df_gcell.azimuth + (0.8 * half_beam)))))
	df_gcell['coordinate11'] = list(zip(df_gcell.longitude + radius * np.sin(np.deg2rad(df_gcell.azimuth + half_beam)), df_gcell.latitude + radius * np.cos(np.deg2rad(df_gcell.azimuth + half_beam))))


	df_gcell['list_coordinate'] = df_gcell[['coordinate0', 'coordinate1', 'coordinate2','coordinate3', 'coordinate4', 'coordinate5','coordinate6', 'coordinate7', 'coordinate8','coordinate9', 'coordinate10', 'coordinate11']].values.tolist()
	df_gcell['geometry']= df_gcell['list_coordinate'].apply(lambda x: Polygon(x))
	df_gcell.drop(['list_coordinate','coordinate0', 'coordinate1', 'coordinate2','coordinate3', 'coordinate4', 'coordinate5','coordinate6', 'coordinate7', 'coordinate8','coordinate9', 'coordinate10', 'coordinate11'], axis=1, inplace=True)

	gdf_cell = geopandas.GeoDataFrame(df_gcell, geometry= 'geometry')


	chunkSize = 5000
	numberChunks = len(gdf_cell) // chunkSize + 1

	df_isd = pd.DataFrame()

	for i in range(numberChunks):
		#print("Calculating isd for chunk %s of %s" %(i, numberChunks))
		gdf_chunk = gdf_cell[i*chunkSize:(i+1)*chunkSize]

		df_merge = geopandas.sjoin(gdf_chunk, gdf_site, how="left", op='contains')
		df_merge = df_merge.loc[df_merge['siteid_left'] != df_merge['siteid_right']]
		df_merge['distance'] = Distance(df_merge['latitude_left'].tolist(),df_merge['longitude_left'].tolist(),df_merge['latitude_right'].tolist(),df_merge['longitude_right'].tolist())
		df_merge['id'] = df_merge['siteid_left'].astype('str') + '_' + df_merge['cellname'].astype('str')
		df_merge = df_merge.sort_values(['id', 'distance'], ascending=[True, True])
		df_merge['rank'] = df_merge.groupby('id').cumcount()+1
		df_merge = df_merge.loc[df_merge['rank'] < 4]

		df_merge = df_merge.reset_index(drop=True)
		df_merge = pd.DataFrame(df_merge.drop(columns='geometry'))
		df_isd = pd.concat([df_isd,df_merge])



	df1 = df_isd.loc[df_isd['rank'] == 1]
	df2 = df_isd.loc[df_isd['rank'] == 2]
	df3 = df_isd.loc[df_isd['rank'] == 3]

	dict_site2 = df2.set_index('id').to_dict()['siteid_right']
	dict_distance2 = df2.set_index('id').to_dict()['distance']
	dict_site3 =df3.set_index('id').to_dict()['siteid_right']
	dict_distance3 = df3.set_index('id').to_dict()['distance']

	df1['site_2'] = df1['id'].map(dict_site2 )
	df1['distance_2'] = df1['id'].map(dict_distance2 )
	df1['site_3'] = df1['id'].map(dict_site3 )
	df1['distance_3'] = df1['id'].map(dict_distance3 )

	df1 = df1.rename(columns={'distance': 'distance_1'})
	df1 = df1.rename(columns={'siteid_right': 'site_1'})
	df1 = df1.rename(columns={'siteid_left': 'siteid'})
	df1 = df1.rename(columns={'longitude_left': 'longitude'})
	df1 = df1.rename(columns={'latitude_left': 'latitude'})
	df1.drop(['id', 'index_right', 'longitude_right', 'latitude_right', 'rank'], axis=1, inplace=True)

	df1.loc[df1['distance_2']> (2*df1['distance_1']), ['distance_2', 'distance_3']] = None
	df1.loc[df1['distance_3']> (2*df1['distance_1']), 'distance_3'] = None
	#df1['isd'] = df1[['distance_1', 'distance_2', 'distance_3']].mean(axis=1)
	df1['isd'] = df1['distance_1']

	df1.loc[pd.isnull(df1['isd']), 'isd'] = 6000


	#df_gcell.to_csv("test_gcell.csv", index=None)
	#gdf_site.to_csv("test_gsite.csv", index=None)
	df1.to_csv(file_isd, index=None)

def gzip_file(file):
	if file.endswith("gz"):
		try:
			with gzip.open('%s'%(file), 'rb') as f_in:
				with open('%s'%(file[:-3]), 'wb') as f_out:
					shutil.copyfileobj(f_in, f_out)
			os.remove(file)
		except:
			pass
			
def gzip_folder(filenames):
	with multiprocessing.Pool(4) as pool:
		pool.map(gzip_file, filenames)


def unzip_one_file(file):
	try:
		#zipdata.extract(file, path=path2)
		zipdata.extract(file)
		print(file)
	except:
		pass
		print("gagal")
	
	
def unzip_all_files(filenames):
	with multiprocessing.Pool(4) as pool:
		pool.map(unzip_one_file, filenames)

def get_path():
	path = os.getcwd()
	path = path.replace("\\", "/", -1)
	return path


def get_file_infolder_ue_band(path):
	filenames = []
	for file in os.listdir(path):
		if file.endswith("csv"):
			if file.startswith("ue_band"):
				filenames.append("%s/%s"%(path,file))
	return filenames


def select_event(file):
	file2 = os.path.basename(file).lower()
	path = os.path.dirname(file)
	if not file2.startswith('sel_event'):
		try:
			if file2.endswith('.gz'):
				with gzip.open(file, 'rb') as f_in:
					with open(file[:-3], 'wb') as f_out:
						shutil.copyfileobj(f_in, f_out)
				os.remove(file)
				file = file[:-3]
				file2 = os.path.basename(file).lower()
				read_file = open(file, "rb")
			else:
				read_file = open(file, "rb")
			file_length_in_bytes = os.path.getsize(file)

			i_position = 0
			remaining = file_length_in_bytes - i_position
		
			if len(file2) >200:
				f_out = "%s/sel_event_%s.bin" % (path, file2[:-20])
			else:
				f_out = "%s/sel_event_%s" % (path,file2)

			try:
				out_file = open(f_out, "wb")
			except:
				print("cant write file: %s" %file)
				pass

			list_event = [19,3075,3108,3112,3092, 3076]

			#3092 (x0c14) - INTERNAL_PER_UETR_RADIO_UE_MEASUREMENT (TxRank, SINR, CQI) --> including Angle of Arrival and TA
			#3108 (x0c24) - INTERNAL_PER_RADIO_UE_MEASUREMENT_TA
			#3112 (x0c28) - INTERNAL_PER_UE_MDT_M1_REPORT
			#3075 (x0c03) - INTERNAL_PER_RADIO_UE_MEASUREMENT
			#3076 (x0c04) - INTERNAL_PER_UE_TRAFFIC_REP
			#19   (x0013) - RRC_UE_CAPABILITY_INFORMATION
			i_len_event = 0

			#print("List Event: %s" %list_event)

			while i_position < file_length_in_bytes:
				if remaining < 15:
					break
				else:
					len_event = read_file.read(2)
					rec_type = read_file.read(2)
					event_id = read_file.read(3)
					
					i_len_event = int.from_bytes(len_event, byteorder='big', signed=True)
					i_rec_type = int.from_bytes(rec_type, byteorder='big', signed=True)
					i_event_id = int.from_bytes(event_id, byteorder='big', signed=True)
					
					content = read_file.read(i_len_event - 7)
					if i_position < 433:
						out_file.write(len_event)
						out_file.write(rec_type)
						out_file.write(event_id)
						out_file.write(content)
						
					if i_event_id in list_event:
						#print("lenght = %s, record_type = %s, event_id = %s" %(i_len_event, i_rec_type, i_event_id))
						# if i_event_id == 3092:
							# print("Found UETR %s event" %i_event_id)
						# elif i_event_id == 3075:
							# print("Found Ue Measurement %s event" %i_event_id)
						out_file.write(len_event)
						out_file.write(rec_type)
						out_file.write(event_id)
						out_file.write(content)

						
						
					i_position = i_position + i_len_event
					remaining = file_length_in_bytes - i_position

			if i_position < file_length_in_bytes:
				content = read_file.read(file_length_in_bytes - i_position + 1)
				out_file.write(content)
				
			
			out_file.close()
			read_file.close()
			os.remove(file)
		except Exception as e:
			print("Error: %s" % e)
			pass
	else:
		f_out = file
	return(f_out)


def selected_event_folder(filenames):
	with multiprocessing.Pool(12) as pool:
		pool.map(select_event, filenames)

def select_event2(file):
	file2 = os.path.basename(file).lower()
	try:
		read_file = open(file, "rb")		
		
		file_length_in_bytes = os.path.getsize(file)

		i_position = 0
		remaining = file_length_in_bytes - i_position
		
		if len(file2) >200:
			f_out = "sel_event_%s.bin" % file2[:-20]
		else:
			f_out = "sel_event_%s" % file2

		try:
			out_file = open(f_out, "wb")
		except:
			print("cant write file: %s" %file)
			pass

		list_event = [19,3075, 3076, 3108, 3112, 3092]

		if os.path.isfile('list_event.txt'):
			with open('list_event.txt') as f:
				list_event = f.readlines()
			list_event = [int(x.strip()) for x in list_event]

		#3092 (x0c14) - INTERNAL_PER_UETR_RADIO_UE_MEASUREMENT (TxRank, SINR, CQI) --> including Angle of Arrival and TA
		#3108 (x0c24) - INTERNAL_PER_RADIO_UE_MEASUREMENT_TA
		#3112 (x0c28) - INTERNAL_PER_UE_MDT_M1_REPORT
		#3075 (x0c03) - INTERNAL_PER_RADIO_UE_MEASUREMENT
		#3076 (x0c04) - INTERNAL_PER_UE_TRAFFIC_REP
		#19   (x0013) - RRC_UE_CAPABILITY_INFORMATION
		i_len_event = 0

		#print("List Event: %s" %list_event)

		while i_position < file_length_in_bytes:
			if remaining < 15:
				break
			else:
				len_event = read_file.read(2)
				rec_type = read_file.read(2)
				event_id = read_file.read(3)
				
				i_len_event = int.from_bytes(len_event, byteorder='big', signed=True)
				i_rec_type = int.from_bytes(rec_type, byteorder='big', signed=True)
				i_event_id = int.from_bytes(event_id, byteorder='big', signed=True)
				
				content = read_file.read(i_len_event - 7)
				if i_position < 433:
					out_file.write(len_event)
					out_file.write(rec_type)
					out_file.write(event_id)
					out_file.write(content)
					
				if i_event_id in list_event:
					#print("lenght = %s, record_type = %s, event_id = %s" %(i_len_event, i_rec_type, i_event_id))
					# if i_event_id == 3092:
						# print("Found UETR %s event" %i_event_id)
					# elif i_event_id == 3075:
						# print("Found Ue Measurement %s event" %i_event_id)
					out_file.write(len_event)
					out_file.write(rec_type)
					out_file.write(event_id)
					out_file.write(content)

				
					
				i_position = i_position + i_len_event
				remaining = file_length_in_bytes - i_position

		if i_position < file_length_in_bytes:
			content = read_file.read(file_length_in_bytes - i_position + 1)
			out_file.write(content)
			
		read_file.close()
		out_file.close()
	except:
		print("Failed parsing file: %s" % file2)
		pass
		
	os.remove(file)

def selected_event_folder2(filenames):
	#This function delete source file, while select_event not delete source file
	with multiprocessing.Pool(4) as pool:
		pool.map(select_event2, filenames)

def unzip_file(file):
	target_folder = 'unzip'

	with ZipFile(file, 'r') as zipObj:
		listOfiles = zipObj.namelist()
		for elem in listOfiles:
			if elem.endswith('.gz'):                
				file2 = os.path.basename(elem)
				new_file = "%s/%s"%(target_folder, file2)
				try:
					with zipObj.open(elem) as zf, open('%s/%s'%(target_folder,file2), 'wb') as f:
						shutil.copyfileobj(zf, f)
				
					if new_file.endswith(".gz"):
						with gzip.open(new_file, 'rb') as f_in:
							with open('%s/%s'%(target_folder,file2[:-3]), 'wb') as f_out:
								shutil.copyfileobj(f_in, f_out)
						os.remove(new_file)
				except:
					print("    Error unzipping %s" %file)
					pass
			elif elem.endswith('.bin'):                
				file2 = os.path.basename(elem)
				new_file = "%s/%s"%(target_folder, file2)
				try:
					with zipObj.open(elem) as zf, open('%s/%s'%(target_folder,file2), 'wb') as f:
						shutil.copyfileobj(zf, f)
				except:
					print("    Error unzipping %s" %file)
					pass


def unzip_files(filenames):
	with multiprocessing.Pool(4) as pool:
		pool.map(unzip_file, filenames)
		
def old_decode_event(file):
	read_file = open(file, "rb")
	file2 = os.path.basename(file).lower()
	file_length_in_bytes = os.path.getsize(file)

	i_count_mdt = 0
	i_count_ta = 0
	i_count_ue_meas = 0
	i_count_ue_cap = 0
	i_count_ue_nr = 0
	i_count_ue_traffic = 0
	
	i_position = 0
	remaining = file_length_in_bytes - i_position

	file_ue_cap =  "ue_cap_%s.csv" % (file2[11:-4])
	file_mdt_m1 = "mdt_m1_%s.csv" % (file2[11:-4])
	file_ta_dist = "ta_dist_%s.csv" % (file2[11:-4])
	file_ue_meas = "ue_meas_%s.csv" % (file2[11:-4])
	file_ue_traffic = "ue_traffic_%s.csv" % (file2[11:-4])
	dist_ta = open(file_ta_dist, "w") # open for [w]riting as [b]inary
	f_mdt_m1 = open(file_mdt_m1, "w")
	f_ue_meas = open(file_ue_meas, "w")
	f_ue_traffic = open(file_ue_traffic, "w")
	f_ue_cap = open(file_ue_cap, "w")
	test_number = 1

	dict_meas_type = {0: 'A1',1: 'A2',2: 'A3',3: 'A4',4: 'A5',5: 'B1',6: 'B2',7: 'PERIODIC',8: 'REPORT_CGI',9: 'PERIODIC_LOC',10: 'A6',11: 'A2_PERIODIC'}

	list_event = [19,3108,3112,3075,3092, 3076]
	i_len_event = 0

	#counter initiazion for ue_capability
	col_uecap=['tanggal','hour','sitename','enodebid','ci','rel8','rel9','rel10','rel11', 'rel12','rel13','rel14','rel15','band_1','band_2','band_3','band_4','band_5','band_6','band_7','band_8','band_9','band_10','band_11','band_12','band_13','band_14','band_15','band_16','band_17','band_18','band_19','band_20','band_21','band_22','band_23','band_24','band_25','band_26','band_27','band_28','band_29','band_30','band_31','band_32','band_33','band_34','band_35','band_36','band_37','band_38','band_39','band_40','band_41','band_42','band_43','band_44','band_45','band_46']
	dict_row = {}
	for col in col_uecap:
		dict_row[col] = ''
	list_data = []
	i_row = 0
	i_max_row = 0
	list_ue_cap = []

	i_cursor = read_file.tell()

	while i_position < file_length_in_bytes:
		#print("i_position: %s, i_cursor: %s remaining: %s"%(i_position, i_cursor, remaining))
		if remaining < 15:
			break
		else:
			len_event = read_file.read(2)
			rec_type = read_file.read(2)
			event_id = read_file.read(3)

			i_len_event = int.from_bytes(len_event, byteorder='big', signed=False)
			i_rec_type = int.from_bytes(rec_type, byteorder='big', signed=False)
			i_event_id = int.from_bytes(event_id, byteorder='big', signed=False)
			
			i_cursor = read_file.tell()
			if i_event_id in list_event:
				icount = 7
				next_stop = i_len_event
				dict_ta = {}
				dict_ta[0]=dict_ta[16]=dict_ta[32]=dict_ta[48]=dict_ta[64]=dict_ta[80]=dict_ta[96]=dict_ta[112]=dict_ta[128]=dict_ta[144]=dict_ta[160]=dict_ta[176]=dict_ta[192]=dict_ta[208]=dict_ta[224]=dict_ta[240]=dict_ta[256]=dict_ta[272]=dict_ta[288]=dict_ta[304]=dict_ta[320]=dict_ta[336]=dict_ta[352]=dict_ta[368]=dict_ta[384]=dict_ta[400]=dict_ta[416]=dict_ta[432]=dict_ta[448]=dict_ta[464]=dict_ta[480]=dict_ta[496]=dict_ta[512]=dict_ta[528]=dict_ta[544]=dict_ta[560]=dict_ta[576]=dict_ta[592]=dict_ta[608]=dict_ta[624]=dict_ta[640]=dict_ta[656]=dict_ta[672]=dict_ta[688]=dict_ta[704]=dict_ta[720]=dict_ta[736]=dict_ta[752]=dict_ta[768]=dict_ta[784]=dict_ta[800]=dict_ta[816]=dict_ta[832]=dict_ta[848]=dict_ta[864]=dict_ta[880]=dict_ta[896]=dict_ta[912]=dict_ta[928]=dict_ta[944]=dict_ta[960]=dict_ta[976]=dict_ta[992]=dict_ta[1008]=dict_ta[1024]=dict_ta[1040]=dict_ta[1056]=dict_ta[1072]=dict_ta[1088]=dict_ta[1104]=dict_ta[1120]=dict_ta[1136]=dict_ta[1152]=dict_ta[1168]=dict_ta[1184]=dict_ta[1200]=dict_ta[1216]=dict_ta[1232]=dict_ta[1248]=dict_ta[1264]=dict_ta[1280]=dict_ta[1296]=dict_ta[1312]=dict_ta[1328]=dict_ta[1344]=dict_ta[1360]=dict_ta[1376]=dict_ta[1392]=dict_ta[1408]=dict_ta[1424]=dict_ta[1440]=dict_ta[1456]=dict_ta[1472]=dict_ta[1488]=dict_ta[1504]=dict_ta[1520]=dict_ta[1536]=dict_ta[1552]=dict_ta[1568]=dict_ta[1584]=dict_ta[1600]=dict_ta[1616]=dict_ta[1632]=dict_ta[1648]=dict_ta[1664]=dict_ta[1680]=dict_ta[1696]=dict_ta[1712]=dict_ta[1728]=dict_ta[1744]=dict_ta[1760]=dict_ta[1776]=dict_ta[1792]=dict_ta[1808]=dict_ta[1824]=dict_ta[1840]=dict_ta[1856]=dict_ta[1872]=dict_ta[1888]=dict_ta[1904]=dict_ta[1920]=dict_ta[1936]=dict_ta[1952]=dict_ta[1968]=dict_ta[1984]=dict_ta[2000]=dict_ta[2016]=dict_ta[2032]=dict_ta[2048]=dict_ta[2064]=dict_ta[2080]=dict_ta[2096]=dict_ta[2112]=dict_ta[2128]=dict_ta[2144]=dict_ta[2160]=dict_ta[2176]=dict_ta[2192]=dict_ta[2208]=dict_ta[2224]=dict_ta[2240]=dict_ta[2256]=dict_ta[2272]=dict_ta[2288]=dict_ta[2304]=dict_ta[2320]=dict_ta[2336]=dict_ta[2352]=dict_ta[2368]=dict_ta[2384]=dict_ta[2400]=dict_ta[2416]=dict_ta[2432]=dict_ta[2448]=dict_ta[2464]=dict_ta[2480]=dict_ta[2496]=dict_ta[2512]=dict_ta[2528]=dict_ta[2544]=dict_ta[2560]=dict_ta[2576]=dict_ta[2592]=dict_ta[2608]=dict_ta[2624]=dict_ta[2640]=dict_ta[2656]=dict_ta[2672]=dict_ta[2688]=dict_ta[2704]=dict_ta[2720]=dict_ta[2736]=dict_ta[2752]=dict_ta[2768]=dict_ta[2784]=dict_ta[2800]=dict_ta[2816]=dict_ta[2832]=dict_ta[2848]=dict_ta[2864]=dict_ta[2880]=dict_ta[2896]=dict_ta[2912]=dict_ta[2928]=dict_ta[2944]=dict_ta[2960]=dict_ta[2976]=dict_ta[2992]=dict_ta[3008]=dict_ta[3024]=dict_ta[3040]=dict_ta[3056]=dict_ta[3072]=dict_ta[3088]=dict_ta[3104]=dict_ta[3120]=dict_ta[3136]=dict_ta[3152]=dict_ta[3168]=dict_ta[3184]=dict_ta[3200]=dict_ta[3216]=dict_ta[3232]=dict_ta[3248]=dict_ta[3264]=dict_ta[3280]=dict_ta[3296]=dict_ta[3312]=dict_ta[3328]=dict_ta[3344]=dict_ta[3360]=dict_ta[4800]=dict_ta[6400]=dict_ta[8000]=dict_ta[9600]=dict_ta[11200]=dict_ta[12800]=dict_ta[14400]=dict_ta[16000]=dict_ta[19200]=0
				list_ta=[0,16,32,48,64,80,96,112,128,144,160,176,192,208,224,240,256,272,288,304,320,336,352,368,384,400,416,432,448,464,480,496,512,528,544,560,576,592,608,624,640,656,672,688,704,720,736,752,768,784,800,816,832,848,864,880,896,912,928,944,960,976,992,1008,1024,1040,1056,1072,1088,1104,1120,1136,1152,1168,1184,1200,1216,1232,1248,1264,1280,1296,1312,1328,1344,1360,1376,1392,1408,1424,1440,1456,1472,1488,1504,1520,1536,1552,1568,1584,1600,1616,1632,1648,1664,1680,1696,1712,1728,1744,1760,1776,1792,1808,1824,1840,1856,1872,1888,1904,1920,1936,1952,1968,1984,2000,2016,2032,2048,2064,2080,2096,2112,2128,2144,2160,2176,2192,2208,2224,2240,2256,2272,2288,2304,2320,2336,2352,2368,2384,2400,2416,2432,2448,2464,2480,2496,2512,2528,2544,2560,2576,2592,2608,2624,2640,2656,2672,2688,2704,2720,2736,2752,2768,2784,2800,2816,2832,2848,2864,2880,2896,2912,2928,2944,2960,2976,2992,3008,3024,3040,3056,3072,3088,3104,3120,3136,3152,3168,3184,3200,3216,3232,3248,3264,3280,3296,3312,3328,3344,3360,4800,6400,8000,9600,11200,12800,14400,16000,19200]
				list_access_stratum_release = ['rel8','rel9','rel10','rel11', 'rel12','rel13','rel14','rel15']

				if i_event_id == 3108:
					i_count_ta = i_count_ta + 1
					#-----INTERNAL_PER_RADIO_UE_MEASUREMENT_TA-----------
					i_hour= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					i_minute= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					i_second= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					i_msecond= int.from_bytes(read_file.read(2), byteorder='big', signed=False)
					s_hour = "%s.%s.%s.%s" %(i_hour, i_minute, i_second, i_msecond)
					skip = read_file.read(4)
					i_ecgi= int.from_bytes(read_file.read(4), byteorder='big', signed=False)
					i_enodebid = int(i_ecgi/256)
					i_ci = i_ecgi % 256
					skip = read_file.read(3)
					i_mmes1apid= int.from_bytes(read_file.read(4), byteorder='big', signed=False)
					skip = read_file.read(21)
					
					sTemp2 = sTemp = "%s,%s,%s,%s" %(s_tanggal,s_sitename,i_enodebid,i_ci)
					#i_ta = [0]*60
					icount = icount + 41
					dict_ta = {}
					list_ta = []
					for ta_id in range(60):
						ta_value = int.from_bytes(read_file.read(2), byteorder='big', signed=False)
						if ta_value < 32768:
							if ta_value in list_ta:
								dict_ta[ta_value] = dict_ta[ta_value] + 1
							else:
								dict_ta[ta_value] = 1
								list_ta.append(ta_value)

					for ta_value in list_ta:
						sTemp2 = "%s,%s,%s\n" % (sTemp, dict_ta[ta_value],ta_value)
						dist_ta.write(sTemp2)

					icount = icount + 120

						
				elif i_event_id == 19:					
					#-----RRC_UE_CAPABILITY_INFORMATION-----------
					i_hour= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					i_minute= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					i_second= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					i_msecond= int.from_bytes(read_file.read(2), byteorder='big', signed=False)
					#s_hour = "%s.%s.%s.%s" %(i_hour, i_minute, i_second, i_msecond)					
					s_hour = "%s.%s" %(i_hour, (int(i_minute/15)*15))
					skip = read_file.read(4)
					i_ecgi= int.from_bytes(read_file.read(4), byteorder='big', signed=False)
					i_enodebid = int(i_ecgi/256)
					i_ci = i_ecgi % 256
					skip = read_file.read(3)
					i_mmes1apid= int.from_bytes(read_file.read(4), byteorder='big', signed=False)
					key_10s = "%s_%s_%s_%s_%s_%s_%s" %(i_mmes1apid, i_year, i_month, i_date, i_hour, i_minute, int(i_second/10)*10)
					key_mnt = "%s_%s_%s_%s_%s_%s" %(i_mmes1apid, i_year, i_month, i_date, i_hour, i_minute)
					key_3mnt = "%s_%s_%s_%s_%s_%s" %(i_mmes1apid, i_year, i_month, i_date, i_hour, int(i_minute/3)*3)
					skip = read_file.read(14)
					i_message_direction= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					i_l3message_lenght= int.from_bytes(read_file.read(2), byteorder='big', signed=False)
					s_l3message_content = read_file.read(i_l3message_lenght)
					
					#Check tanggal, enodebid, ci whether already in row or not
					cell_identity = "%s_%s_%s" %(s_tanggal,i_enodebid,i_ci)
					if cell_identity in list_data:
						i_row = list_data.index(cell_identity)
					else:
						list_data.append(cell_identity)
						dict_row = {}
						for col in col_uecap:
							dict_row[col] = 0
						dict_row['tanggal'] = s_tanggal
						dict_row['sitename'] = s_sitename
						dict_row['hour'] = s_hour
						dict_row['enodebid'] = i_enodebid
						dict_row['ci'] = i_ci
						list_ue_cap.append(dict_row)
						i_row = i_max_row
						i_max_row = i_max_row + 1
						
						
						



					#------------Decoding L3 message content------------------
					# s_l3message_content = 38 03 08 16 7D
					if len(s_l3message_content) > 2:
						#get next 4 bit of the third byte : xF0 = 240
						if (s_l3message_content[2] & 240) >> 4 == 0 :
							i_count_ue_cap = i_count_ue_cap + 1
							band = [0]*64
							band_check = [0]*64

							#Find start of container "xxxx 11xx" --> 0000 1100 = 12
							if (s_l3message_content[4] & 12) == 12:
								#AccessStratum . AND operation with x0F = 15
								#print(((bytes3[0] & 3)<<2) + ((bytes3[1] & 192)>>6))
								i_release =   (((s_l3message_content[4] & 3) <<2) + ((s_l3message_content[5] & 192) >> 6))
								i_start_lte = 4
								if i_release < 8 :
									s_release =   list_access_stratum_release[i_release]
									list_ue_cap[i_row][s_release] = list_ue_cap[i_row][s_release] + 1
								else:
									s_release = ""
							else:
								i_release =   (((s_l3message_content[3] & 3) <<2) + ((s_l3message_content[4] & 192) >> 6))
								i_start_lte = 3
								if i_release < 8 :
									s_release =   list_access_stratum_release[i_release]
									list_ue_cap[i_row][s_release] = list_ue_cap[i_row][s_release] + 1
								else:
									s_release = ""

							#Check Ue Category byte (i_start_lte + 1): 3 bit xx11 1xxx --> 0011 1000 = 56 >>3
							i_ue_category = ((s_l3message_content[i_start_lte + 1] & 56) >> 3) + 1

							#Check if default ROHC ContextSessions used or not 
							#---> byte (i_start_lte + 1): 2 bit xxxx x11x --> 0000 0110  = 6 >> 1
							i_rohc_contextsession_check = ((s_l3message_content[i_start_lte + 1] & 6) >> 1)
							if i_rohc_contextsession_check == 0:
								#position of size frequency is on byte i_start_lte + 3
								#--> byte (i_start_lte + 3): 6 bit xx11 1111 --> 0011 1111  = 63 
								i_num_freq = (s_l3message_content[i_start_lte + 3] & 63) + 1

								#band_1  --> byte +4 : 6 bit --> 1111 1100 = 252 >> 2
								#i_band_1 = ((s_l3message_content[i_start_lte + 4] & 252) >> 2) + 1

								#band_2 6 bit --> byte +4 : 1 bit --> 0000 0001 = 1 
								#                 byte +5 : 5 bit --> 1111 1000 = 248 >> 3
								#i_band_2 = (((s_l3message_content[i_start_lte + 4] & 1) << 5) + ((s_l3message_content[i_start_lte + 5] & 248) >> 3)  )+ 1

								i_start_byte = i_start_lte + 4
								i_start_bit_1 = 0
								
							else:
								#position of size frequency is on byte i_start_lte + 3 and + 4
								#--> byte (i_start_lte + 3): 6 bit xxxx xx11 --> 0000 0011  = 3 << 4
								#--> byte (i_start_lte + 4): 6 bit 1111 xxxx --> 1111 0000  = 240 >> 4
								i_num_freq =   (((s_l3message_content[i_start_lte + 3] & 3) << 4) + ((s_l3message_content[i_start_lte + 4] & 240) >> 4)) + 1

								#band_1 6bit --> byte +4 : 4 bit --> 0000 1111 = 15 << 2 
								#			     byte +5 : 2 bit --> 1100 0000 = 192 >> 6
								#i_band_1 = (((s_l3message_content[i_start_lte + 4] & 15) << 2) + ((s_l3message_content[i_start_lte + 5] & 192) >> 6)  )+ 1

								#band_2 6bit --> byte +5 : 5 bit --> 0001 1111 = 31 << 1 
								#			     byte +6 : 1 bit --> 1000 0000 = 64 >> 7
								#i_band_2 = (((s_l3message_content[i_start_lte + 5] & 31) << 1) + ((s_l3message_content[i_start_lte + 6] & 64) >> 7)  )+ 1

								i_start_byte = i_start_lte + 4
								i_start_bit_1 = 4

							for num_freq in range(0, i_num_freq):
								if i_start_bit_1 < 3:
									i_num_bit_1 = 6
									i_num_bit_2 = 0
									i_shift_right = 1
									i_shift_1 = 2 - i_start_bit_1
									i_shift_2 = 0
								else:
									i_num_bit_1 = 8 - i_start_bit_1
									i_num_bit_2 = i_start_bit_1 - 2		#6 - (8 - i_start_bit_1) = i_start_bit_1 -2
									i_shift_right = 0
									i_shift_1 = i_num_bit_2
									i_shift_2 = 8 - i_num_bit_2
								
								i_mask_1 = 0
								for i_num in range(0, i_num_bit_1):
									i_mask_1 = i_mask_1 + 2**(7 - i_num - i_start_bit_1)
								
								i_mask_2 = 0
								if i_num_bit_2 > 0:
									for i_num in range(0, i_num_bit_2):
										i_mask_2 = i_mask_2 + 2**(7 - i_num)
								
								# Read band 6 bit 
								if i_num_bit_2 == 0:
									band[num_freq] = ((s_l3message_content[i_start_byte] & i_mask_1) >> i_shift_1) + 1
								else:
									band[num_freq] = (((s_l3message_content[i_start_byte] & i_mask_1) << i_shift_1) +  ((s_l3message_content[i_start_byte + 1] & i_mask_2) >> i_shift_2)) + 1
								
								if band[num_freq] < 64:
									band_check[band[num_freq]-1] = 1
								#else:
								#	print("Freq outlier %s: %s" % (num_freq,band[num_freq]))

								#Calculating next i_start_byte and i_start_bit -- considering 1 bit Duplex Flag
								if i_start_bit_1 > 0:
									i_start_byte = i_start_byte + 1
									i_start_bit_1 = (i_start_bit_1 + 7) - 8
								else:
									i_start_bit_1 = 7

							i_crnti = int.from_bytes(read_file.read(5), byteorder='big', signed=False)

							for num_freq in range(0, 46):
								if band_check[num_freq] == 1:
									list_ue_cap[i_row]['band_%s'%(num_freq+1)] = list_ue_cap[i_row]['band_%s'%(num_freq+1)] + 1
							
							
							#f_ue_cap.write(sTemp2)
							icount = icount + 42 + i_l3message_lenght + 5
				elif i_event_id == 3075:
					i_count_ue_meas = i_count_ue_meas + 1
					#-----INTERNAL_PER_RADIO_UE_MEASUREMENT-----------
					i_hour= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					i_minute= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					i_second= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					i_msecond= int.from_bytes(read_file.read(2), byteorder='big', signed=False)
					s_hour = "%s.%s.%s.%s" %(i_hour, i_minute, i_second, i_msecond)
					skip = read_file.read(4)
					i_ecgi= int.from_bytes(read_file.read(4), byteorder='big', signed=False)
					i_enodebid = int(i_ecgi/256)
					i_ci = i_ecgi % 256
					skip = read_file.read(3)
					i_mmes1apid= int.from_bytes(read_file.read(4), byteorder='big', signed=False)
					key_10s = "%s_%s_%s_%s_%s_%s_%s" %(i_mmes1apid, i_year, i_month, i_date, i_hour, i_minute, int(i_second/10)*10)
					key_mnt = "%s_%s_%s_%s_%s_%s" %(i_mmes1apid, i_year, i_month, i_date, i_hour, i_minute)
					key_3mnt = "%s_%s_%s_%s_%s_%s" %(i_mmes1apid, i_year, i_month, i_date, i_hour, int(i_minute/3)*3)
					skip = read_file.read(14)
					
					sTemp2 = sTemp = "%s,%s,%s,%s,%s,%s" %(s_tanggal,s_hour,s_sitename,i_enodebid,i_ci,i_mmes1apid)
					i_rank_tx_0 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_rank_tx_1 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_rank_tx_2 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_rank_tx_3 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_rank_tx_4 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_rank_tx_5 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_rank_tx_6 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_rank_tx_7 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi_reported_0 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi_reported_1 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi_reported_2 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi_reported_3 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi_reported_4 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi_reported_5 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi_reported_6 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi_reported_7 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi_reported_8 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi_reported_9 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi_reported_10 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi_reported_11 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi_reported_12 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi_reported_13 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi_reported_14 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi_reported_15 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_rank_reported_0 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_rank_reported_1 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_tbs_power_restricted = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_tbs_power_unrestricted = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi2_reported_0 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi2_reported_1 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi2_reported_2 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi2_reported_3 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi2_reported_4 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi2_reported_5 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi2_reported_6 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi2_reported_7 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi2_reported_8 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi2_reported_9 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi2_reported_10 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi2_reported_11 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi2_reported_12 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi2_reported_13 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi2_reported_14 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi2_reported_15 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_out_loop_adj_dl_0 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_out_loop_adj_dl_1 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_out_loop_adj_dl_2 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_out_loop_adj_dl_3 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_out_loop_adj_dl_4 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_out_loop_adj_dl_5 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_out_loop_adj_dl_6 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_out_loop_adj_dl_7 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_sinr_pusch_0 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_sinr_pusch_1 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_sinr_pusch_2 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_sinr_pusch_3 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_sinr_pusch_4 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_sinr_pusch_5 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_sinr_pusch_6 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_sinr_pusch_7 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_sinr_pucch_0 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_sinr_pucch_1 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_sinr_pucch_2 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_sinr_pucch_3 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_sinr_pucch_4 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_sinr_pucch_5 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_sinr_pucch_6 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_sinr_pucch_7 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					#i_ta = int.from_bytes(read_file.read(2), byteorder='big', signed=False)  -- this available on UETR
					i_delta_sinr_pusch_0 = int.from_bytes(read_file.read(4), byteorder='big', signed=False)
					i_delta_sinr_pusch_1 = int.from_bytes(read_file.read(4), byteorder='big', signed=False)
					i_delta_sinr_pusch_2 = int.from_bytes(read_file.read(4), byteorder='big', signed=False)
					i_delta_sinr_pusch_3 = int.from_bytes(read_file.read(4), byteorder='big', signed=False)
					i_delta_sinr_pusch_4 = int.from_bytes(read_file.read(4), byteorder='big', signed=False)
					i_delta_sinr_pusch_5 = int.from_bytes(read_file.read(4), byteorder='big', signed=False)
					i_delta_sinr_pusch_6 = int.from_bytes(read_file.read(4), byteorder='big', signed=False)
					i_delta_sinr_pusch_7 = int.from_bytes(read_file.read(4), byteorder='big', signed=False)
					i_aoa = int.from_bytes(read_file.read(2), byteorder='big', signed=False)
					if i_aoa > 0:
						print("AOA value: %s" % i_aoa)
					i_power_headroom = int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					i_rank_tx_8 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_rank_tx_9 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_rank_tx_10 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_rank_tx_11 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_power_headroom_prb_used = int.from_bytes(read_file.read(2), byteorder='big', signed=False)
					i_rank_reported_2 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_rank_reported_3 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_last_ri_reported = int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					i_last_cqi_1_reported = int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					i_last_cqi_2_reported = int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					i_last_pusch_num_prb_reported = int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					i_last_pusch_sinr_reported = int.from_bytes(read_file.read(2), byteorder='big', signed=False)
					i_last_power_per_prb_reported = int.from_bytes(read_file.read(2), byteorder='big', signed=False)
					i_sinr_meas_pusch_8 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_rank_tx_12 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_rank_tx_13 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi_reported_0 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi_reported_1 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi_reported_2 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi_reported_3 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi_reported_4 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi_reported_5 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi_reported_6 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi_reported_7 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi_reported_8 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi_reported_9 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi_reported_10 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi_reported_11 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi_reported_12 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi_reported_13 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi_reported_14 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi_reported_15 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi2_reported_0 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi2_reported_1 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi2_reported_2 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi2_reported_3 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi2_reported_4 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi2_reported_5 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi2_reported_6 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi2_reported_7 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi2_reported_8 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi2_reported_9 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi2_reported_10 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi2_reported_11 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi2_reported_12 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi2_reported_13 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi2_reported_14 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi2_reported_15 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_rank_reported_0 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_rank_reported_1 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_rank_reported_2 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_rank_reported_3 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi3_reported_0 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi3_reported_1 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi3_reported_2 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi3_reported_3 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi3_reported_4 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi3_reported_5 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi3_reported_6 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi3_reported_7 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi3_reported_8 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi3_reported_9 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi3_reported_10 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi3_reported_11 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi3_reported_12 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi3_reported_13 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi3_reported_14 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi3_reported_15 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi4_reported_0 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi4_reported_1 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi4_reported_2 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi4_reported_3 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi4_reported_4 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi4_reported_5 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi4_reported_6 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi4_reported_7 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi4_reported_8 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi4_reported_9 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi4_reported_10 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi4_reported_11 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi4_reported_12 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi4_reported_13 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi4_reported_14 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_cqi4_reported_15 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi3_reported_0 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi3_reported_1 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi3_reported_2 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi3_reported_3 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi3_reported_4 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi3_reported_5 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi3_reported_6 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi3_reported_7 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi3_reported_8 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi3_reported_9 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi3_reported_10 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi3_reported_11 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi3_reported_12 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi3_reported_13 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi3_reported_14 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi3_reported_15 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi4_reported_0 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi4_reported_1 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi4_reported_2 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi4_reported_3 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi4_reported_4 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi4_reported_5 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi4_reported_6 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi4_reported_7 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi4_reported_8 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi4_reported_9 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi4_reported_10 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi4_reported_11 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi4_reported_12 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi4_reported_13 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi4_reported_14 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_hom_cqi4_reported_15 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_plmn_index = int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					i_ue_category_flex = int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					i_spid_value = int.from_bytes(read_file.read(2), byteorder='big', signed=False)
					i_br_ce_level = int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					i_ue_power_class = int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					i_subs_group_id = int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					
					
										
					icount = icount + 624
					sTemp2 = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n"%(sTemp2,i_aoa,i_ue_category_flex,i_ue_power_class,i_cqi_reported_0,i_cqi_reported_1,i_cqi_reported_2,i_cqi_reported_3,i_cqi_reported_4,i_cqi_reported_5,i_cqi_reported_6,i_cqi_reported_7,i_cqi_reported_8,i_cqi_reported_9,i_cqi_reported_10,i_cqi_reported_11,i_cqi_reported_12,i_cqi_reported_13,i_cqi_reported_14,i_cqi_reported_15,i_rank_reported_0,i_rank_reported_1,i_tbs_power_restricted,i_sinr_pusch_0,i_sinr_pusch_1,i_sinr_pusch_2,i_sinr_pusch_3,i_sinr_pusch_4,i_sinr_pusch_5,i_sinr_pusch_6,i_sinr_pusch_7,i_sinr_meas_pusch_8,i_rank_reported_2,i_rank_reported_3,i_hom_rank_reported_2,i_hom_rank_reported_3,key_10s,key_mnt,key_3mnt)
					
					f_ue_meas.write(sTemp2)
					#print("Ue Meas : ")
					
				elif i_event_id == 3076:
					i_count_ue_traffic = i_count_ue_traffic + 1
					#-----INTERNAL_PER_UE_TRAFFIC_REP-----------
					i_hour= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					i_minute= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					i_second= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					i_msecond= int.from_bytes(read_file.read(2), byteorder='big', signed=False)
					s_hour = "%s.%s.%s.%s" %(i_hour, i_minute, i_second, i_msecond)
					skip = read_file.read(4)
					i_ecgi= int.from_bytes(read_file.read(4), byteorder='big', signed=False)
					i_enodebid = int(i_ecgi/256)
					i_ci = i_ecgi % 256
					skip = read_file.read(3)
					i_mmes1apid= int.from_bytes(read_file.read(4), byteorder='big', signed=False)
					key_10s = "%s_%s_%s_%s_%s_%s_%s" %(i_mmes1apid, i_year, i_month, i_date, i_hour, i_minute, int(i_second/10)*10)
					key_mnt = "%s_%s_%s_%s_%s_%s" %(i_mmes1apid, i_year, i_month, i_date, i_hour, i_minute)
					key_3mnt = "%s_%s_%s_%s_%s_%s" %(i_mmes1apid, i_year, i_month, i_date, i_hour, int(i_minute/3)*3)
					skip = read_file.read(14)
					
					sTemp2 = sTemp = "%s,%s,%s,%s,%s,%s" %(s_tanggal,s_hour,s_sitename,i_enodebid,i_ci,i_mmes1apid)
					skip = read_file.read(143)	#parameter not decoded
					i_RADIOTHP_VOL_DL = int.from_bytes(read_file.read(5), byteorder='big', signed=False)
					i_RADIOTHP_RES_DL = int.from_bytes(read_file.read(5), byteorder='big', signed=False)
					i_RADIOTHP_VOL_UL = int.from_bytes(read_file.read(5), byteorder='big', signed=False)
					i_RADIOTHP_RES_UL = int.from_bytes(read_file.read(5), byteorder='big', signed=False)
					skip = read_file.read(5)
					i_UE_THP_DL_DRB = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					if i_UE_THP_DL_DRB > 8388607:
						i_UE_THP_DL_DRB = ""
					i_UE_THP_UL_DRB = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					if i_UE_THP_UL_DRB > 8388607:
						i_UE_THP_UL_DRB = ""
					
					
										
					#icount = icount + 100
					sTemp2 = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n"%(sTemp2,i_RADIOTHP_VOL_DL,i_RADIOTHP_RES_DL,i_RADIOTHP_VOL_UL,i_RADIOTHP_RES_UL,i_UE_THP_DL_DRB,i_UE_THP_UL_DRB,key_10s,key_mnt,key_3mnt)
					
					f_ue_traffic.write(sTemp2)
					#print("Ue Meas : ")	
				
				elif i_event_id == 3112:
					#-----MDT Event-----------
					i_hour= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					i_minute= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					i_second= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					i_msecond= int.from_bytes(read_file.read(2), byteorder='big', signed=False)
					s_hour = "%s.%s.%s.%s" %(i_hour, i_minute, i_second, i_msecond)
					skip = read_file.read(4)
					i_ecgi= int.from_bytes(read_file.read(4), byteorder='big', signed=False)
					i_enodebid = int(i_ecgi/256)
					i_ci = i_ecgi % 256
					skip = read_file.read(3)
					i_mmes1apid= int.from_bytes(read_file.read(4), byteorder='big', signed=False)
					key_10s = "%s_%s_%s_%s_%s_%s_%s" %(i_mmes1apid, i_year, i_month, i_date, i_hour, i_minute, int(i_second/10)*10)
					key_mnt = "%s_%s_%s_%s_%s_%s" %(i_mmes1apid, i_year, i_month, i_date, i_hour, i_minute)
					key_3mnt = "%s_%s_%s_%s_%s_%s" %(i_mmes1apid, i_year, i_month, i_date, i_hour, int(i_minute/3)*3)
					skip = read_file.read(11)
					i_unique_ueid= int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					i_meas_type= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					s_meas_type = dict_meas_type[i_meas_type]
					i_rsrp_serving = get_rsrp(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
					i_rsrq_serving = get_rsrq(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
					
					i_pci_1 = get_pci(int.from_bytes(read_file.read(2), byteorder='big', signed=False))
					i_rsrp_1 = get_rsrp(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
					i_rsrq_1 = get_rsrq(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
					i_pci_2 = get_pci(int.from_bytes(read_file.read(2), byteorder='big', signed=False))
					i_rsrp_2 = get_rsrp(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
					i_rsrq_2 = get_rsrq(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
					i_pci_3 = get_pci(int.from_bytes(read_file.read(2), byteorder='big', signed=False))
					i_rsrp_3 = get_rsrp(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
					i_rsrq_3 = get_rsrq(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
					i_pci_4 = get_pci(int.from_bytes(read_file.read(2), byteorder='big', signed=False))
					i_rsrp_4 = get_rsrp(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
					i_rsrq_4 = get_rsrq(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
					i_pci_5 = get_pci(int.from_bytes(read_file.read(2), byteorder='big', signed=False))
					i_rsrp_5 = get_rsrp(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
					i_rsrq_5 = get_rsrq(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
					i_pci_6 = get_pci(int.from_bytes(read_file.read(2), byteorder='big', signed=False))
					i_rsrp_6 = get_rsrp(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
					i_rsrq_6 = get_rsrq(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
					i_pci_7 = get_pci(int.from_bytes(read_file.read(2), byteorder='big', signed=False))
					i_rsrp_7 = get_rsrp(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
					i_rsrq_7 = get_rsrq(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
					i_pci_8 = get_pci(int.from_bytes(read_file.read(2), byteorder='big', signed=False))
					i_rsrp_8 = get_rsrp(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
					i_rsrq_8 = get_rsrq(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
					
					i_coordinate = int.from_bytes(read_file.read(1), byteorder='big', signed=False)
					h_point = read_file.read(8)
					h_point_w_alti = read_file.read(11)
					h_point_w_uncer_circle = read_file.read(9)
					h_point_w_uncer_ellipse = read_file.read(12)
					h_point_w_alti_n_uncer_ellipse = read_file.read(16)
					h_arc = read_file.read(14)
					h_polygon_1 = read_file.read(17)
					h_polygon_2 = read_file.read(17)
					h_polygon_3 = read_file.read(17)
					h_polygon_4 = read_file.read(17)
					h_polygon_5 = read_file.read(17)
					h_polygon_6 = read_file.read(17)
					h_polygon_7 = read_file.read(17)
					h_horizontal_velocity = read_file.read(5)
					h_gnss_tod_msec = read_file.read(4)
					i_earfcn = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
					
					i_altitude = None
					if i_coordinate == 0:
						s_location = h_point
					elif i_coordinate == 1:
						s_location = h_point_w_alti
						i_altitude = get_altitude(s_location)
					elif i_coordinate == 2:
						s_location = h_point_w_uncer_circle
					elif i_coordinate == 3:
						s_location = h_point_w_uncer_ellipse
					elif i_coordinate == 4:
						s_location = h_point_w_alti_n_uncer_ellipse
						i_altitude = get_altitude(s_location)
					elif i_coordinate == 5:
						s_location = h_arc
					elif i_coordinate == 6:
						s_location = h_polygon_1
					else:
						s_location = ''
					
					if s_location == '':
						ilong = ilat = ''
					else:
						ilong,ilat = get_coordinate(s_location)
						
						i_count_mdt = i_count_mdt + 1
						sTemp = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" %(s_tanggal,s_hour,s_sitename,i_enodebid,i_ci,i_mmes1apid,i_unique_ueid,i_earfcn,s_meas_type,ilong, ilat,i_altitude,i_rsrp_serving,i_rsrq_serving,i_pci_1,i_rsrp_1,i_rsrq_1,i_pci_2,i_rsrp_2,i_rsrq_2,i_pci_3,i_rsrp_3,i_rsrq_3,i_pci_4,i_rsrp_4,i_rsrq_4,i_pci_5,i_rsrp_5,i_rsrq_5,i_pci_6,i_rsrp_6,i_rsrq_6,i_pci_7,i_rsrp_7,i_rsrq_7,i_pci_8,i_rsrp_8,i_rsrq_8,key_10s,key_mnt,key_3mnt)
						f_mdt_m1.write(sTemp)
					
			elif i_cursor < 10:
				#read site name and date
				skip = read_file.read(20)
				i_year= int.from_bytes(read_file.read(2), byteorder='big', signed=False)
				i_month= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
				i_date= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
				skip = read_file.read(3)
				temp = read_file.read(128)                
				#s_sitename = (temp.rstrip(b'\x00')).decode('utf-8')
				temp = read_file.read(248)                
				s_sitename = (temp.rstrip(b'\x00')).decode('utf-8')
				s_tanggal = "%s%02d%02d"%(i_year,i_month,i_date)
				
					
			else:
				if i_position > 10000:
					print("event not in the list....")
					i_position = file_length_in_bytes + 10
				if i_len_event - 7 >= 0:
					content = read_file.read(i_len_event - 7)
					
				
			i_position = i_position + i_len_event
			read_file.seek(i_position,0)
			i_cursor = read_file.tell()
			if i_cursor > i_position :
				i_position = file_length_in_bytes + 10
				print("error on reading %s on eventid: %s"%(file,i_event_id))
			remaining = file_length_in_bytes - i_position
			
	read_file.close()
	dist_ta.close()
	f_mdt_m1.close()
	f_ue_meas.close()
	f_ue_traffic.close()

	if i_max_row > 0:
		for i_row in range(i_max_row):
			sTemp = ''
			i = 0
			for col in col_uecap:
				if i == 0:
					sTemp = "%s" % list_ue_cap[i_row][col]
				else:
					sTemp = "%s,%s" %(sTemp, list_ue_cap[i_row][col])
				i = i + 1
			sTemp = "%s\n" %(sTemp)
			f_ue_cap.write(sTemp)
	
	f_ue_cap.close()
	if i_count_ue_cap == 0:
		os.remove(file_ue_cap)
	#if i_count_ue_nr == 0:
	#	os.remove(file_ue_nr)
	if i_count_mdt == 0:
		os.remove(file_mdt_m1)
		os.remove(file_ue_meas)
		os.remove(file_ue_traffic)
	elif i_count_ue_meas == 0:
		#print("i_count_ue_meas: %s" % i_count_ue_meas)
		os.remove(file_ue_meas)
	else:
		print("   mdt data found.........")
	if i_count_ue_traffic == 0:
		if i_count_mdt > 0:
			os.remove(file_ue_traffic)
	
	if i_count_ta == 0:
		os.remove(file_ta_dist)
	os.remove(file)

def decode_event_folder(filenames):
	with multiprocessing.Pool(12) as pool:
		pool.map(decode_event, filenames)

def unzip_ctr(path, path2):
	for file in os.listdir(path):
		if file.startswith("CTR"):
			zipdata = zipfile.ZipFile(file)
			zipinfos = zipdata.infolist()
			for zip in zipinfos:
				zipdata.extract(zip, path2)
			
def get_file_infolder(path, numbers):
	blockfiles = []
	filenames = []
	i = 0
	for file in os.listdir(path):
		if file.endswith("bin"):
			if not file.startswith("sel_event"):
				filenames.append("%s/%s"%(path,file))
				if i%numbers == numbers - 1:
					blockfiles.append(filenames)
					filenames = []
				i = i + 1
	blockfiles.append(filenames)
	return blockfiles

def get_zip_infolder(path, numbers):
	blockfiles = []
	filenames = []
	i = 0
	for file in os.listdir(path):
		if file.endswith("zip"):
			if not os.path.isdir("%s/%s"%(path,file)):
				filenames.append("%s/%s"%(path,file))
				if i%numbers == numbers - 1:
					blockfiles.append(filenames)
					filenames = []
				i = i + 1
	blockfiles.append(filenames)
	return blockfiles

def get_unzip_infolder(path, numbers):
	blockfiles = []
	filenames = []
	i = 0
	for file in os.listdir(path):
		if file.endswith("bin"):
			filenames.append("%s/%s"%(path,file))
			if i%numbers == numbers - 1:
				blockfiles.append(filenames)
				filenames = []
			i = i + 1
	blockfiles.append(filenames)
	return blockfiles

def get_file_infolder_sel(path, numbers):
	blockfiles = []
	filenames = []
	i = 0
	for file in os.listdir(path):
		if file.endswith("bin"):
			if file.startswith("sel_event"):
				filenames.append("%s/%s"%(path,file))
				if i%numbers == numbers - 1:
					blockfiles.append(filenames)
					filenames = []
				i = i + 1
	blockfiles.append(filenames)
	return blockfiles
	
def insert_mdt_file(file):
	global df_mdt
	df_mdt_1 = pd.read_csv(file, delimiter=",", index_col=None, header='infer')
	df_mdt_2 = df_mdt_1[df_mdt_1.longitude.notnull()]
	df_mdt_2 = df_mdt_2.replace(np.nan, '', regex=True)
	df_mdt = df_mdt.append(df_mdt_2)
	del df_mdt_1
	del df_mdt_2
	os.remove(file)
	
def insert_ta_file(file):
	try :
		df_ta_1 = pd.read_csv(file, delimiter=",", index_col=None, header='infer')
		df_ta_1.columns = ['date','hour','site','enodebid', 'ci','mmes1apid','ta_0','ta_78','ta_156','ta_234','ta_312','ta_390','ta_468','ta_546','ta_624','ta_702','ta_780','ta_858','ta_936','ta_1015','ta_1093','ta_1171','ta_1249','ta_1327','ta_1405','ta_1483','ta_1561','ta_1639','ta_1717','ta_1795','ta_1873','ta_1952','ta_2030','ta_2108','ta_2186','ta_2264','ta_2342','ta_2420','ta_2498','ta_2576','ta_2654','ta_2732','ta_2810','ta_2888','ta_2967','ta_3045','ta_3123','ta_3201','ta_3279','ta_3357','ta_3435','ta_3513','ta_3591','ta_3669','ta_3747','ta_3825','ta_3904','ta_3982','ta_4060','ta_4138','ta_4216','ta_4294','ta_4372','ta_4450','ta_4528','ta_4606','ta_4684','ta_4762','ta_4840','ta_4919','ta_4997','ta_5075','ta_5153','ta_5231','ta_5309','ta_5387','ta_5465','ta_5543','ta_5621','ta_5699','ta_5777','ta_5856','ta_5934','ta_6012','ta_6090','ta_6168','ta_6246','ta_6324','ta_6402','ta_6480','ta_6558','ta_6636','ta_6714','ta_6792','ta_6871','ta_6949','ta_7027','ta_7105','ta_7183','ta_7261','ta_7339','ta_7417','ta_7495','ta_7573','ta_7651','ta_7729','ta_7808','ta_7886','ta_7964','ta_8042','ta_8120','ta_8198','ta_8276','ta_8354','ta_8432','ta_8510','ta_8588','ta_8666','ta_8744','ta_8823','ta_8901','ta_8979','ta_9057','ta_9135','ta_9213','ta_9291','ta_9369','ta_9447','ta_9525','ta_9603','ta_9681','ta_9760','ta_9838','ta_9916','ta_9994','ta_10072','ta_10150','ta_10228','ta_10306','ta_10384','ta_10462','ta_10540','ta_10618','ta_10696','ta_10775','ta_10853','ta_10931','ta_11009','ta_11087','ta_11165','ta_11243','ta_11321','ta_11399','ta_11477','ta_11555','ta_11633','ta_11712','ta_11790','ta_11868','ta_11946','ta_12024','ta_12102','ta_12180','ta_12258','ta_12336','ta_12414','ta_12492','ta_12570','ta_12648','ta_12727','ta_12805','ta_12883','ta_12961','ta_13039','ta_13117','ta_13195','ta_13273','ta_13351','ta_13429','ta_13507','ta_13585','ta_13664','ta_13742','ta_13820','ta_13898','ta_13976','ta_14054','ta_14132','ta_14210','ta_14288','ta_14366','ta_14444','ta_14522','ta_14600','ta_14679','ta_14757','ta_14835','ta_14913','ta_14991','ta_15069','ta_15147','ta_15225','ta_15303','ta_15381','ta_15459','ta_15537','ta_15616','ta_15694','ta_15772','ta_15850','ta_15928','ta_16006','ta_16084','ta_16162','ta_16240','ta_16318','ta_16396','ta_23424','ta_31232','ta_39040','ta_46848','ta_54656','ta_62464','ta_70272','ta_78080','ta_93696']
		df_ta_2 = df_ta_1.groupby(['date','site','enodebid','ci'], as_index=False).sum()
		df_ta_2 = df_ta_2.drop(['mmes1apid'], 1)
		df_ta_2.to_csv("agg_%s" %file, index=False, header=False)
		del df_ta_1
		del df_ta_2
		os.remove(file)
		
	except:
		pass

def calculate_perc_ta(file):
	try:
		df_ta_1 = pd.read_csv(file, delimiter=",", index_col=None, header='infer')
		#summarize histogram cell daily
		df_ta_1['sample'] = pd.to_numeric(df_ta_1["sample"], errors='ignore')
		
		#df_ta_2 is dataframe from group by ta level --> used later for pdf calculation
		df_ta_2 = df_ta_1.groupby(['tanggal','sitename','enodebid','ci','ta'], as_index=False).agg({'sample': 'sum'})
		del df_ta_1
		os.remove(file)

		dict_range = {}
		for i in range (2048):
			j = i * 16
			dict_range[j] = "[%s - %s]" %(round(j * 97.65/20000 , 3), round((j+16) * 97.65/20000 , 3))

		df_ta_2['range_of_distance_km'] = df_ta_2['ta']
		df_ta_2['range_of_distance_km'] = df_ta_2['range_of_distance_km'].map(dict_range)
		df_ta_1 = df_ta_2[['tanggal','sitename','enodebid','ci', 'ta','range_of_distance_km','sample']]
		df_ta_1.to_csv(file, index=False, header=True)
		
		#Calculating Avg TA
		df_ta_2['ta_distance'] = (df_ta_2['ta'] + 8) * 97.65/20000
		df_ta_2['ta_distance_2'] = (df_ta_2['ta'] + 16) * 97.65/20000
		df_ta_2['sum_ta_distance'] = df_ta_2['ta_distance'] * df_ta_2['sample'] 
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
		df_ta_3 = df_ta_3.round({"perc_300":2, "perc_500":2, "perc_700":2, "perc_1000":2, "perc_1500":2, "perc_2000":2, "perc_3000":2, "perc_5000":2, "perc_10000":2, "perc_15000":2, "perc_30000":2, "avg_ta_km":2, "perc95_ta_distance_km":2, "perc98_ta_distance_km":2}) 
		#df_ta_3 = df_ta_3.round({"perc_300":2, "perc_700":2, "perc_1000":2, "perc_1500":2, "avg_ta_km":2, "perc95_ta_distance_km":2, "perc98_ta_distance_km":2}) 

		df_ta_3['std'] = df_ta_3['id']
		df_ta_3['std'] = df_ta_3['std'].map(dict_std)
		print("write ta_stat.csv")
		df_ta_1 = df_ta_3[['tanggal','sitename','enodebid','ci','avg_ta_km','std','perc80_ta_distance_km','perc90_ta_distance_km','perc95_ta_distance_km','perc98_ta_distance_km','perc_300','perc_500','perc_700','perc_1000','perc_1500','perc_2000','perc_3000','perc_5000','perc_10000','perc_15000','perc_30000','sample']]
		df_ta_1.to_csv("%s/ta_stat.csv" % decoded_ctr_path,sep=',',index=False)

		del df_ta_3
		del df_ta_4
		del df_ta_5
		del df_ta_2
		del df_ta_1
	except:
		pass
	


		
def combine_mdt_meas(file_mdt, file_meas, output):
	df_mdt = pd.read_csv(file_mdt, delimiter=",", index_col=None, header='infer')
	
	key10s_long_dict = df_mdt.set_index('key_10s').to_dict()['longitude']
	key10s_lat_dict = df_mdt.set_index('key_10s').to_dict()['latitude']
	key_mnt_long_dict = df_mdt.set_index('key_mnt').to_dict()['longitude']
	key_mnt_lat_dict = df_mdt.set_index('key_mnt').to_dict()['latitude']
	key_3mnt_long_dict = df_mdt.set_index('key_3mnt').to_dict()['longitude']
	key_3mnt_lat_dict = df_mdt.set_index('key_3mnt').to_dict()['latitude']

	df_meas = pd.read_csv(file_meas, delimiter=",", index_col=None, header='infer')
	if len(df_meas) > 0:
		df_meas['long1'] = df_meas['key_10s'].map(key10s_long_dict)
		df_meas['lat1'] = df_meas['key_10s'].map(key10s_lat_dict)

		df_meas['long2'] = df_meas['key_mnt'].map(key_mnt_long_dict)
		df_meas['lat2'] = df_meas['key_mnt'].map(key_mnt_lat_dict)

		df_meas['longitude'] = np.where(df_meas.long1.notnull(), df_meas.long1, df_meas.long2)
		df_meas['latitude'] = np.where(df_meas.lat1.notnull(), df_meas.lat1, df_meas.lat2)

		df_meas['long3'] = df_meas['key_3mnt'].map(key_3mnt_long_dict)
		df_meas['lat3'] = df_meas['key_3mnt'].map(key_3mnt_lat_dict)

		df_meas['longitude'] = np.where(df_meas.longitude.notnull(), df_meas.longitude, df_meas.long3)
		df_meas['latitude'] = np.where(df_meas.latitude.notnull(), df_meas.latitude, df_meas.lat3)

		df_meas = df_meas.drop(['long1','lat1','long2','lat2','long3','lat3','key_10s','key_mnt','key_3mnt'], axis=1)
		df_meas =df_meas.loc[df_meas.longitude.notnull()]
		#print("calculating avg cqi, avg sinr and avg rank2")
		df_meas['cqi_avg'] = ((df_meas['cqi_reported_0']*0)+(df_meas['cqi_reported_1']*1)+(df_meas['cqi_reported_2']*2)+(df_meas['cqi_reported_3']*3)+(df_meas['cqi_reported_4']*4)+(df_meas['cqi_reported_5']*5)+(df_meas['cqi_reported_6']*6)+(df_meas['cqi_reported_7']*7)+(df_meas['cqi_reported_8']*8)+(df_meas['cqi_reported_9']*9)+(df_meas['cqi_reported_10']*10)+(df_meas['cqi_reported_11']*11)+(df_meas['cqi_reported_12']*12)+(df_meas['cqi_reported_13']*13)+(df_meas['cqi_reported_14']*14)+(df_meas['cqi_reported_15']*15))/(df_meas['cqi_reported_0']+df_meas['cqi_reported_1']+df_meas['cqi_reported_2']+df_meas['cqi_reported_3']+df_meas['cqi_reported_4']+df_meas['cqi_reported_5']+df_meas['cqi_reported_6']+df_meas['cqi_reported_7']+df_meas['cqi_reported_8']+df_meas['cqi_reported_9']+df_meas['cqi_reported_10']+df_meas['cqi_reported_11']+df_meas['cqi_reported_12']+df_meas['cqi_reported_13']+df_meas['cqi_reported_14']+df_meas['cqi_reported_15'])
		df_meas['rank2_perc'] = 100.0 * df_meas['rank_reported_1']/(df_meas['rank_reported_0']+df_meas['rank_reported_1'])
		df_meas['pusch_sinr'] = ((df_meas['sinr_pusch_0']*-5)+(df_meas['sinr_pusch_1']*-2)+(df_meas['sinr_pusch_2']*2)+(df_meas['sinr_pusch_3']*6)+(df_meas['sinr_pusch_4']*10)+(df_meas['sinr_pusch_5']*14)+(df_meas['sinr_pusch_6']*17)+(df_meas['sinr_pusch_7']*20)+(df_meas['sinr_pusch_8']*30))/(df_meas['sinr_pusch_0']+df_meas['sinr_pusch_1']+df_meas['sinr_pusch_2']+df_meas['sinr_pusch_3']+df_meas['sinr_pusch_4']+df_meas['sinr_pusch_5']+df_meas['sinr_pusch_6']+df_meas['sinr_pusch_7']+df_meas['sinr_pusch_8'])
		df_meas.to_csv(output, header=True, index=False)
	os.remove(file_meas)

	del key10s_long_dict
	del key10s_lat_dict
	del key_mnt_long_dict
	del key_mnt_lat_dict
	del key_3mnt_long_dict
	del key_3mnt_lat_dict
	del df_meas

def combine_mdt_traffic(file_mdt, file_traffic, output):
	try :
		df_mdt = pd.read_csv(file_mdt, delimiter=",", index_col=None, header='infer')
		
		key10s_long_dict = df_mdt.set_index('key_10s').to_dict()['longitude']
		key10s_lat_dict = df_mdt.set_index('key_10s').to_dict()['latitude']
		key_mnt_long_dict = df_mdt.set_index('key_mnt').to_dict()['longitude']
		key_mnt_lat_dict = df_mdt.set_index('key_mnt').to_dict()['latitude']
		key_3mnt_long_dict = df_mdt.set_index('key_3mnt').to_dict()['longitude']
		key_3mnt_lat_dict = df_mdt.set_index('key_3mnt').to_dict()['latitude']

		df_traffic = pd.read_csv(file_traffic, delimiter=",", index_col=None, header='infer')
		
		df_traffic['long1'] = df_traffic['key_10s']
		df_traffic['long1'] = df_traffic['long1'].map(key10s_long_dict)
		df_traffic['lat1'] = df_traffic['key_10s']
		df_traffic['lat1'] = df_traffic['lat1'].map(key10s_lat_dict)

		df_traffic['long2'] = df_traffic['key_mnt']
		df_traffic['long2'] = df_traffic['long2'].map(key_mnt_long_dict)
		df_traffic['lat2'] = df_traffic['key_mnt']
		df_traffic['lat2'] = df_traffic['lat2'].map(key_mnt_lat_dict)

		df_traffic['longitude'] = np.where(df_traffic.long1.notnull(), df_traffic.long1, df_traffic.long2)
		df_traffic['latitude'] = np.where(df_traffic.lat1.notnull(), df_traffic.lat1, df_traffic.lat2)

		df_traffic['long3'] = df_traffic['key_3mnt']
		df_traffic['long3'] = df_traffic['long3'].map(key_3mnt_long_dict)
		df_traffic['lat3'] = df_traffic['key_3mnt']
		df_traffic['lat3'] = df_traffic['lat3'].map(key_3mnt_lat_dict)

		df_traffic['longitude'] = np.where(df_traffic.longitude.notnull(), df_traffic.longitude, df_traffic.long3)
		df_traffic['latitude'] = np.where(df_traffic.latitude.notnull(), df_traffic.latitude, df_traffic.lat3)

		df_traffic = df_traffic.drop(['long1','lat1','long2','lat2','long3','lat3','key_10s','key_mnt','key_3mnt'], axis=1)
		
		df_traffic = df_traffic.loc[df_traffic.longitude.notnull()]
		df_traffic.to_csv(output, header=True, index=False)
		os.remove(file_traffic)

		del key10s_long_dict
		del key10s_lat_dict
		del key_mnt_long_dict
		del key_mnt_lat_dict
		del key_3mnt_long_dict
		del key_3mnt_lat_dict
		del df_traffic


		
	except:
		pass	

def insert_mdt_folder(filenames):
	with multiprocessing.Pool(num_worker) as pool:
		pool.map(insert_mdt_file, filenames)

def insert_ta_folder(filenames):
	with multiprocessing.Pool(num_worker) as pool:
		pool.map(insert_ta_file, filenames)			

def combine_ue_cap(filenames):
	try :
		if len(filenames)> 0:
			file_ue_cap =  "%s/result_ue_cap.csv" % decoded_ctr_path
			f_ue_cap = open(file_ue_cap, "w")
			col_uecap=['tanggal','hour','sitename','enodebid','ci','rel8','rel9','rel10','rel11', 'rel12','rel13','rel14','rel15','band_1','band_2','band_3','band_4','band_5','band_6','band_7','band_8','band_9','band_10','band_11','band_12','band_13','band_14','band_15','band_16','band_17','band_18','band_19','band_20','band_21','band_22','band_23','band_24','band_25','band_26','band_27','band_28','band_29','band_30','band_31','band_32','band_33','band_34','band_35','band_36','band_37','band_38','band_39','band_40','band_41','band_42','band_43','band_44','band_45','band_46']
			sTemp = ""
			i = 0
			for col in col_uecap:
				if i == 0:
					sTemp = "%s" % col
				else:
					sTemp = "%s,%s" % (sTemp, col)
				i = i + 1
			sTemp = "%s\n" %sTemp
			f_ue_cap.write(sTemp)

			for file in filenames:
				with open(file, "r") as infile:
					f_ue_cap.write(infile.read())
				os.remove(file)
			f_ue_cap.close()

			#aggregate ue capability daily
			df_ue = pd.read_csv(file_ue_cap, delimiter=",", index_col=None, header='infer')
			del df_ue['hour']
			df_ue = df_ue.groupby(['tanggal','sitename','enodebid','ci'], as_index=False).sum()
			df_ue.to_csv(file_ue_cap, sep=",", header=True, index=False)
			del df_ue
	except:
		pass

def decode_ctr_folder(list_site):
	with multiprocessing.Pool(48) as pool:
		pool.map(decode_event, list_site)


def decode_event(site):
	csv_path = '%s/csv' %site
	if not os.path.exists(csv_path):
		os.mkdir(csv_path)
	#set_mmes1apid = set()
	#dict_nr_ue = {}
	#file_ue_nr = "%s/csv/ue_nr.csv" % (site)
	#f_ue_nr = open(file_ue_nr, "w")
	#f_ue_nr.write('mmes1apid,endc_capable\n')

	duration = datetime.now() - start_time
	if duration.seconds < 4500:
		for ff in os.listdir(site):
			#try:
			isFileOk = False
			if ff.endswith('.gz'):
				isFileOk = True
			elif ff.endswith('.bin'):
				isFileOk = True
			if isFileOk:
				file = '%s/%s'%(site,ff)
				file = select_event(file)


				
				read_file = open(file, "rb")
				file2 = os.path.basename(file).lower()
				file_length_in_bytes = os.path.getsize(file)

				i_count_mdt = 0
				i_count_ta = 0
				i_count_ue_meas = 0
				i_count_ue_cap = 0
				i_count_ue_traffic = 0
				
				i_position = 0
				remaining = file_length_in_bytes - i_position

				file_ue_cap =  "%s/csv/ue_cap_%s.csv" % (site, file2[11:-4])
				file_ue_band =  "%s/csv/ue_band_%s.csv" % (site, file2[11:-4])
				file_mdt_m1 = "%s/csv/mdt_m1_%s.csv" % (site, file2[11:-4])
				file_ta_dist = "%s/csv/ta_dist_%s.csv" % (site, file2[11:-4])
				file_ue_meas = "%s/csv/ue_meas_%s.csv" % (site, file2[11:-4])
				
				file_ue_traffic = "%s/csv/ue_traffic_%s.csv" % (site, file2[11:-4])
				dist_ta = open(file_ta_dist, "w") # open for [w]riting as [b]inary
				f_mdt_m1 = open(file_mdt_m1, "w")
				f_ue_meas = open(file_ue_meas, "w")
				f_ue_traffic = open(file_ue_traffic, "w")
				f_ue_cap = open(file_ue_cap, "w")
				f_ue_band = open(file_ue_band, "w")

				#dict_meas_type = {0: 'A1',1: 'A2',2: 'A3',3: 'A4',4: 'A5',5: 'B1',6: 'B2',7: 'PERIODIC',8: 'REPORT_CGI',9: 'PERIODIC_LOC',10: 'A6',11: 'A2_PERIODIC'}

				list_event = [19,3108,3112,3075,3092, 3076]
				i_len_event = 0

				#counter initiazion for ue_capability
				col_uecap=['tanggal','hour','sitename','enodebid','ci','ue_category_1','ue_category_2','ue_category_3','ue_category_4','ue_category_5','ue_category_6','ue_category_7','ue_category_8','ue_category_9','ue_category_10','ue_category_11','ue_category_12','ue_category_dl_13','ue_category_dl_14','ue_category_ul_13','rel8','rel9','rel10','rel11', 'rel12','rel13','rel14','rel15','band_1','band_2','band_3','band_4','band_5','band_6','band_7','band_8','band_9','band_10','band_11','band_12','band_13','band_14','band_15','band_16','band_17','band_18','band_19','band_20','band_21','band_22','band_23','band_24','band_25','band_26','band_27','band_28','band_29','band_30','band_31','band_32','band_33','band_34','band_35','band_36','band_37','band_38','band_39','band_40','band_41','band_42','band_43','band_44','band_45','band_46','standaloneGNSS','MIMO4x4','256QAM','UL64QAM','fgi_bit7_rlc_um','fgi_bit28_tti_bundling','fgi_bit30_ho_fdd_tdd','ca_2cc', 'ca_3cc','ca_3cc_fgi111_true','ca_3cc_fgi111_112_true', 'ENDC', 'NR_SA', 'ENDC_N1', 'ENDC_N3', 'ENDC_N8', 'ENDC_N40', 'ENDC_N41', 'ENDC_N77', 'ENDC_N78', 'NRSA_N1', 'NRSA_N3', 'NRSA_N8', 'NRSA_N40', 'NRSA_N41', 'NRSA_N77', 'NRSA_N78']

				dict_row = {}
				for col in col_uecap:
					dict_row[col] = ''
				list_data = []
				i_row = 0
				i_max_row = 0
				list_ue_cap = []

				i_cursor = read_file.tell()

				while i_position < file_length_in_bytes:
					#print("i_position: %s, i_cursor: %s remaining: %s"%(i_position, i_cursor, remaining))
					if remaining < 15:
						break
					else:
						len_event = read_file.read(2)
						rec_type = read_file.read(2)
						event_id = read_file.read(3)

						i_len_event = int.from_bytes(len_event, byteorder='big', signed=False)
						i_rec_type = int.from_bytes(rec_type, byteorder='big', signed=False)
						i_event_id = int.from_bytes(event_id, byteorder='big', signed=False)
						
						i_cursor = read_file.tell()
						if i_cursor < 10:
							#read site name and date
							skip = read_file.read(20)
							i_year= int.from_bytes(read_file.read(2), byteorder='big', signed=False)
							i_month= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
							i_date= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
							skip = read_file.read(3)
							temp = read_file.read(128)                
							#s_sitename = (temp.rstrip(b'\x00')).decode('utf-8')
							temp = read_file.read(248)                
							s_sitename = (temp.rstrip(b'\x00')).decode('utf-8')
							s_tanggal = "%s%02d%02d"%(i_year,i_month,i_date)
						elif i_event_id in list_event:
							icount = 7
							next_stop = i_len_event
							dict_ta = {}
							dict_ta[0]=dict_ta[16]=dict_ta[32]=dict_ta[48]=dict_ta[64]=dict_ta[80]=dict_ta[96]=dict_ta[112]=dict_ta[128]=dict_ta[144]=dict_ta[160]=dict_ta[176]=dict_ta[192]=dict_ta[208]=dict_ta[224]=dict_ta[240]=dict_ta[256]=dict_ta[272]=dict_ta[288]=dict_ta[304]=dict_ta[320]=dict_ta[336]=dict_ta[352]=dict_ta[368]=dict_ta[384]=dict_ta[400]=dict_ta[416]=dict_ta[432]=dict_ta[448]=dict_ta[464]=dict_ta[480]=dict_ta[496]=dict_ta[512]=dict_ta[528]=dict_ta[544]=dict_ta[560]=dict_ta[576]=dict_ta[592]=dict_ta[608]=dict_ta[624]=dict_ta[640]=dict_ta[656]=dict_ta[672]=dict_ta[688]=dict_ta[704]=dict_ta[720]=dict_ta[736]=dict_ta[752]=dict_ta[768]=dict_ta[784]=dict_ta[800]=dict_ta[816]=dict_ta[832]=dict_ta[848]=dict_ta[864]=dict_ta[880]=dict_ta[896]=dict_ta[912]=dict_ta[928]=dict_ta[944]=dict_ta[960]=dict_ta[976]=dict_ta[992]=dict_ta[1008]=dict_ta[1024]=dict_ta[1040]=dict_ta[1056]=dict_ta[1072]=dict_ta[1088]=dict_ta[1104]=dict_ta[1120]=dict_ta[1136]=dict_ta[1152]=dict_ta[1168]=dict_ta[1184]=dict_ta[1200]=dict_ta[1216]=dict_ta[1232]=dict_ta[1248]=dict_ta[1264]=dict_ta[1280]=dict_ta[1296]=dict_ta[1312]=dict_ta[1328]=dict_ta[1344]=dict_ta[1360]=dict_ta[1376]=dict_ta[1392]=dict_ta[1408]=dict_ta[1424]=dict_ta[1440]=dict_ta[1456]=dict_ta[1472]=dict_ta[1488]=dict_ta[1504]=dict_ta[1520]=dict_ta[1536]=dict_ta[1552]=dict_ta[1568]=dict_ta[1584]=dict_ta[1600]=dict_ta[1616]=dict_ta[1632]=dict_ta[1648]=dict_ta[1664]=dict_ta[1680]=dict_ta[1696]=dict_ta[1712]=dict_ta[1728]=dict_ta[1744]=dict_ta[1760]=dict_ta[1776]=dict_ta[1792]=dict_ta[1808]=dict_ta[1824]=dict_ta[1840]=dict_ta[1856]=dict_ta[1872]=dict_ta[1888]=dict_ta[1904]=dict_ta[1920]=dict_ta[1936]=dict_ta[1952]=dict_ta[1968]=dict_ta[1984]=dict_ta[2000]=dict_ta[2016]=dict_ta[2032]=dict_ta[2048]=dict_ta[2064]=dict_ta[2080]=dict_ta[2096]=dict_ta[2112]=dict_ta[2128]=dict_ta[2144]=dict_ta[2160]=dict_ta[2176]=dict_ta[2192]=dict_ta[2208]=dict_ta[2224]=dict_ta[2240]=dict_ta[2256]=dict_ta[2272]=dict_ta[2288]=dict_ta[2304]=dict_ta[2320]=dict_ta[2336]=dict_ta[2352]=dict_ta[2368]=dict_ta[2384]=dict_ta[2400]=dict_ta[2416]=dict_ta[2432]=dict_ta[2448]=dict_ta[2464]=dict_ta[2480]=dict_ta[2496]=dict_ta[2512]=dict_ta[2528]=dict_ta[2544]=dict_ta[2560]=dict_ta[2576]=dict_ta[2592]=dict_ta[2608]=dict_ta[2624]=dict_ta[2640]=dict_ta[2656]=dict_ta[2672]=dict_ta[2688]=dict_ta[2704]=dict_ta[2720]=dict_ta[2736]=dict_ta[2752]=dict_ta[2768]=dict_ta[2784]=dict_ta[2800]=dict_ta[2816]=dict_ta[2832]=dict_ta[2848]=dict_ta[2864]=dict_ta[2880]=dict_ta[2896]=dict_ta[2912]=dict_ta[2928]=dict_ta[2944]=dict_ta[2960]=dict_ta[2976]=dict_ta[2992]=dict_ta[3008]=dict_ta[3024]=dict_ta[3040]=dict_ta[3056]=dict_ta[3072]=dict_ta[3088]=dict_ta[3104]=dict_ta[3120]=dict_ta[3136]=dict_ta[3152]=dict_ta[3168]=dict_ta[3184]=dict_ta[3200]=dict_ta[3216]=dict_ta[3232]=dict_ta[3248]=dict_ta[3264]=dict_ta[3280]=dict_ta[3296]=dict_ta[3312]=dict_ta[3328]=dict_ta[3344]=dict_ta[3360]=dict_ta[4800]=dict_ta[6400]=dict_ta[8000]=dict_ta[9600]=dict_ta[11200]=dict_ta[12800]=dict_ta[14400]=dict_ta[16000]=dict_ta[19200]=0
							list_ta=[0,16,32,48,64,80,96,112,128,144,160,176,192,208,224,240,256,272,288,304,320,336,352,368,384,400,416,432,448,464,480,496,512,528,544,560,576,592,608,624,640,656,672,688,704,720,736,752,768,784,800,816,832,848,864,880,896,912,928,944,960,976,992,1008,1024,1040,1056,1072,1088,1104,1120,1136,1152,1168,1184,1200,1216,1232,1248,1264,1280,1296,1312,1328,1344,1360,1376,1392,1408,1424,1440,1456,1472,1488,1504,1520,1536,1552,1568,1584,1600,1616,1632,1648,1664,1680,1696,1712,1728,1744,1760,1776,1792,1808,1824,1840,1856,1872,1888,1904,1920,1936,1952,1968,1984,2000,2016,2032,2048,2064,2080,2096,2112,2128,2144,2160,2176,2192,2208,2224,2240,2256,2272,2288,2304,2320,2336,2352,2368,2384,2400,2416,2432,2448,2464,2480,2496,2512,2528,2544,2560,2576,2592,2608,2624,2640,2656,2672,2688,2704,2720,2736,2752,2768,2784,2800,2816,2832,2848,2864,2880,2896,2912,2928,2944,2960,2976,2992,3008,3024,3040,3056,3072,3088,3104,3120,3136,3152,3168,3184,3200,3216,3232,3248,3264,3280,3296,3312,3328,3344,3360,4800,6400,8000,9600,11200,12800,14400,16000,19200]
							list_access_stratum_release = ['rel8','rel9','rel10','rel11', 'rel12','rel13','rel14','rel15']

							if i_event_id == 3108:
								i_count_ta = i_count_ta + 1
								#-----INTERNAL_PER_RADIO_UE_MEASUREMENT_TA-----------
								i_hour= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								i_minute= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								i_second= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								i_msecond= int.from_bytes(read_file.read(2), byteorder='big', signed=False)
								s_hour = "%s" %(i_hour)
								skip = read_file.read(4)
								i_ecgi= int.from_bytes(read_file.read(4), byteorder='big', signed=False)
								i_enodebid = int(i_ecgi/256)
								i_ci = i_ecgi % 256
								skip = read_file.read(3)
								i_mmes1apid= int.from_bytes(read_file.read(4), byteorder='big', signed=False)
								skip = read_file.read(21)
								
								sTemp2 = sTemp = "%s,%s,%s,%s,%s" %(s_tanggal,i_hour,s_sitename,i_enodebid,i_ci)
								#i_ta = [0]*60
								icount = icount + 41
								dict_ta = {}
								list_ta = []
								for ta_id in range(60):
									ta_value = int.from_bytes(read_file.read(2), byteorder='big', signed=False)
									if ta_value < 32768:
										if ta_value in list_ta:
											dict_ta[ta_value] = dict_ta[ta_value] + 1
										else:
											dict_ta[ta_value] = 1
											list_ta.append(ta_value)

								for ta_value in list_ta:
									sTemp2 = "%s,%s,%s\n" % (sTemp, dict_ta[ta_value],ta_value)
									dist_ta.write(sTemp2)

								icount = icount + 120

									
							elif i_event_id == 19:					
								#-----RRC_UE_CAPABILITY_INFORMATION-----------
								i_hour= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								i_minute= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								i_second= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								i_msecond= int.from_bytes(read_file.read(2), byteorder='big', signed=False)
								#s_hour = "%s.%s.%s.%s" %(i_hour, i_minute, i_second, i_msecond)					
								s_hour = "%s.%s" %(i_hour, (int(i_minute/15)*15))
								skip = read_file.read(4)
								i_ecgi= int.from_bytes(read_file.read(4), byteorder='big', signed=False)
								i_enodebid = int(i_ecgi/256)
								i_ci = i_ecgi % 256
								skip = read_file.read(3)
								i_mmes1apid= int.from_bytes(read_file.read(4), byteorder='big', signed=False)
								#if not i_mmes1apid in set_mmes1apid:
								#	set_mmes1apid.add(i_mmes1apid)
								#	dict_nr_ue[i_mmes1apid] = 0

								key_10s = "%s_%s_%s_%s_%s_%s_%s" %(i_mmes1apid, i_year, i_month, i_date, i_hour, i_minute, int(i_second/10)*10)
								key_mnt = "%s_%s_%s_%s_%s_%s" %(i_mmes1apid, i_year, i_month, i_date, i_hour, i_minute)
								key_3mnt = "%s_%s_%s_%s_%s_%s" %(i_mmes1apid, i_year, i_month, i_date, i_hour, int(i_minute/3)*3)
								skip = read_file.read(14)
								i_message_direction= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								i_l3message_lenght= int.from_bytes(read_file.read(2), byteorder='big', signed=False)
								s_l3message_content = read_file.read(i_l3message_lenght)
								
								sTemp = '%s,%s,%s,%s,%s'%(s_tanggal,s_sitename,i_enodebid,i_ci,i_mmes1apid)
								#Check tanggal, enodebid, ci whether already in row or not
								cell_identity = "%s_%s_%s" %(s_tanggal,i_enodebid,i_ci)
								if cell_identity in list_data:
									i_row = list_data.index(cell_identity)
								else:
									list_data.append(cell_identity)
									dict_row = {}
									for col in col_uecap:
										dict_row[col] = 0
									dict_row['tanggal'] = s_tanggal
									dict_row['sitename'] = s_sitename
									dict_row['hour'] = s_hour
									dict_row['enodebid'] = i_enodebid
									dict_row['ci'] = i_ci
									list_ue_cap.append(dict_row)
									i_row = i_max_row
									i_max_row = i_max_row + 1
									
									
									



								#------------Decoding L3 message content------------------
								# s_l3message_content = 38 03 08 16 7D
								if len(s_l3message_content) > 2:
									# RAT-Type ::=	ENUMERATED {eutra, utra, geran-cs, geran-ps, cdma2000-1XRTT,nr, eutra-nr, spare1, ...}  --> 0 = eutra, 5 = nr, 6 = endc, 1 = utra
									#get next 4 bit of the third byte : xF0 = 240
									if (s_l3message_content[2] & 240) >> 4 == 0 :
										i_count_ue_cap = i_count_ue_cap + 1
										band = [0]*64
										band_check = [0]*64
										num_freq = 0
										isCA = False
										isCA3CC = False
										isENDC = False
										isNRSA = False
										isMIMO4x4 = False

										#check 4  less significant bits of third bytes to check if length of container list is 3 hex or 2 hex
										# x0F = 15  
										s_eutran_container = hexlify(s_l3message_content)
										s_eutran_container_hex = ''
										if (s_l3message_content[2] & 15) == 8 :
											# lenght is 3 hex
											i_len =  (s_l3message_content[3]<<4) + ((s_l3message_content[4] & 240)>>4)
											if i_len < i_l3message_lenght:
												s_eutran_container = s_eutran_container[9:((i_len * 2) + 9)]
												s_eutran_container_hex = unhexlify(s_eutran_container)
										else:
											i_len =  ((s_l3message_content[2] & 15)<<4) + ((s_l3message_content[3] & 240)>>4)
											if i_len < i_l3message_lenght:
												s_eutran_container = s_eutran_container[7:((i_len * 2) + 7)]
												s_eutran_container_hex = unhexlify(s_eutran_container)
										
										#Use PyCrate to decode UE-Eutra capability
										if len(s_eutran_container_hex) > 2:
											uecap = RRCLTE.EUTRA_RRC_Definitions.UE_EUTRA_Capability
											try:
												uecap.from_uper(s_eutran_container_hex)

												#AccessStratum Release
												s_release = uecap._val['accessStratumRelease']
												list_ue_cap[i_row][s_release] = list_ue_cap[i_row][s_release] + 1
												if not s_release in list_access_stratum_release:
													print("s_release %s not found........." % s_release)

												#Ue-Category
												i_ue_category = uecap._val['ue-Category']
												s_param = "ue_category_%s" % i_ue_category
												list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
												#i_ue_category_v1020, s_standaloneGNSS, s_supportedMIMO_CapabilityDL_r10, i_ue_category_v1170, i_ue_categoryDL_r12, i_ue_categoryUL_r12, s_dl_256QAM_r12, s_ul_256QAM_r12

												s_fgi = '{0:32b}'.format(uecap._val['featureGroupIndicators'][0])
												if s_fgi[6] == '1': #FGI start from 1 to 32 --> index 6 means FGI bit 7
													s_param = 'fgi_bit7_rlc_um'
													list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
												if s_fgi[28] == '1': 
													s_param = 'fgi_bit28_tti_bundling'
													list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
												if s_fgi[30] == '1': 
													s_param = 'fgi_bit30_ho_fdd_tdd'
													list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1

												
												try:
													#Release 10
													if 'nonCriticalExtension' in uecap._val['nonCriticalExtension']['nonCriticalExtension']:                                                
														#UE Category b1020
														try:
															i_ue_category_v1020 = uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['ue-Category-v1020']
															s_param = "ue_category_%s" % i_ue_category_v1020
															list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
														except:
															pass

														#standaloneGNSS-Location-r10
														try:
															if uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['ue-BasedNetwPerfMeasParameters-r10']['standaloneGNSS-Location-r10']:
																s_param = "standaloneGNSS" 
																list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
														except:
															pass

														#supported MIMO 4x4
														try:
															s_supportedMIMO_CapabilityDL_r10 = uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['rf-Parameters-v1020']['supportedBandCombination-r10'][0][0]['bandParametersDL-r10'][0]['supportedMIMO-CapabilityDL-r10']
															if s_supportedMIMO_CapabilityDL_r10 == 'fourLayers':
																s_param = 'MIMO4x4'
																isMIMO4x4 = True
																list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
														except:
															pass

														#'supportedBandCombination-r10'
														try:
															num_3cc = False
															num_2cc = False
															for band_combi in uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['rf-Parameters-v1020']['supportedBandCombination-r10']:
																if len(band_combi) >= 3:
																	num_3cc = True
																	isCA3CC = True
																if len(band_combi) >= 2:
																	num_2cc = True
																	isCA = True
															if num_2cc:
																s_param = 'ca_2cc'
																list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1    
															
															if num_3cc:
																s_param = 'ca_3cc'
																list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
																#print("Ue %s supported 3cc. Data: %s"%(i_mmes1apid,hexlify(s_eutran_container_hex)))

																#Fgi bit 111 and 112 for 3cc only
																try:
																	fgi_r10 = '{0:32b}'.format(uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['featureGroupIndRel10-r10'][0])
																	
																	if fgi_r10[10] == '1': #FGI start from 101 to 132 --> index 10 means FGI bit 111
																		s_param = 'ca_3cc_fgi111_true'
																		list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
																		if fgi_r10[11] == '1': 
																			s_param = 'ca_3cc_fgi111_112_true'
																			list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
																except:
																	pass
																

														except:
															pass

														#Release 11 Ue
														try:
															if uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']:
																#i_ue_category_v1170
																try:
																	i_ue_category_v1170 = uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['ue-Category-v1170']
																	s_param = "ue_category_%s" % i_ue_category_v1170
																	list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
																except:
																	pass

																#Release 12
																try:
																	if uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']:
																		#Ue Category DL r12
																		try:
																			i_ue_categoryDL_r12 = uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['ue-CategoryDL-r12']
																			s_param = "ue_category_dl_%s" % i_ue_categoryDL_r12
																			if s_param in col_uecap:
																				list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
																		except:
																			pass

																		#Ue Category UL r12
																		try:
																			i_ue_categoryUL_r12 = uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['ue-CategoryUL-r12']
																			s_param = "ue_category_ul_%s" % i_ue_categoryUL_r12
																			if s_param in col_uecap:
																				list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
																		except:
																			pass

																		#DL 256 QAM
																		try:
																			if uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['rf-Parameters-v1250']['supportedBandListEUTRA-v1250'][0]['dl-256QAM-r12']:
																				s_param = '256QAM'
																				list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
																		except:
																			pass

																		#UL 64 QAM
																		try:
																			if uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['rf-Parameters-v1250']['supportedBandListEUTRA-v1250'][0]['ul-64QAM-r12']:
																				s_param = 'UL64QAM'
																				list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
																		except:
																			pass

																		#ENDC Capable UE
																		try:
																			if uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['irat-ParametersNR-r15']['en-DC-r15']:
																				s_param = 'ENDC'
																				list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
																				isENDC = True
																				
																				dict_nr_ue[i_mmes1apid] = 1

																			#ENDC Supported Band 1, 3, 8, 40, 41, 77, 78
																			try:
																				for band_number in range(len(uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['irat-ParametersNR-r15']['supportedBandListEN-DC-r15'])):
																					if uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['irat-ParametersNR-r15']['supportedBandListEN-DC-r15'][band_number]['bandNR-r15'] == 40:
																						s_param = 'ENDC_N40'
																						list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
																					elif uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['irat-ParametersNR-r15']['supportedBandListEN-DC-r15'][band_number]['bandNR-r15'] == 1:
																						s_param = 'ENDC_N1'
																						list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
																					elif uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['irat-ParametersNR-r15']['supportedBandListEN-DC-r15'][band_number]['bandNR-r15'] == 3:
																						s_param = 'ENDC_N3'
																						list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
																					elif uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['irat-ParametersNR-r15']['supportedBandListEN-DC-r15'][band_number]['bandNR-r15'] == 8:
																						s_param = 'ENDC_N8'
																						list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
																					elif uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['irat-ParametersNR-r15']['supportedBandListEN-DC-r15'][band_number]['bandNR-r15'] == 41:
																						s_param = 'ENDC_N41'
																						list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
																					elif uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['irat-ParametersNR-r15']['supportedBandListEN-DC-r15'][band_number]['bandNR-r15'] == 77:
																						s_param = 'ENDC_N77'
																						list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
																					elif uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['irat-ParametersNR-r15']['supportedBandListEN-DC-r15'][band_number]['bandNR-r15'] == 78:
																						s_param = 'ENDC_N78'
																						list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
																			except:
																				pass

																			
																			#NR-SA Capable UE
																			try:
																				if uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['irat-ParametersNR-v1540']['sa-NR-r15']:
																					s_param = 'NR_SA'
																					list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
																					isNRSA = True
																				
																				#NR-SA Supported Band 3, 8, 40, 41, 77, 78
																				try:
																					for band_number in range(len(uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['irat-ParametersNR-v1540']['supportedBandListNR-SA-r15'])):
																						if uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['irat-ParametersNR-v1540']['supportedBandListNR-SA-r15'][band_number]['bandNR-r15'] == 3:
																							s_param = 'NRSA_N3'
																							list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
																						elif uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['irat-ParametersNR-v1540']['supportedBandListNR-SA-r15'][band_number]['bandNR-r15'] == 1:
																							s_param = 'NRSA_N1'
																							list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
																						elif uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['irat-ParametersNR-v1540']['supportedBandListNR-SA-r15'][band_number]['bandNR-r15'] == 8:
																							s_param = 'NRSA_N8'
																							list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
																						elif uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['irat-ParametersNR-v1540']['supportedBandListNR-SA-r15'][band_number]['bandNR-r15'] == 40:
																							s_param = 'NRSA_N40'
																							list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
																						elif uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['irat-ParametersNR-v1540']['supportedBandListNR-SA-r15'][band_number]['bandNR-r15'] == 41:
																							s_param = 'NRSA_N41'
																							list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
																						elif uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['irat-ParametersNR-v1540']['supportedBandListNR-SA-r15'][band_number]['bandNR-r15'] == 77:
																							s_param = 'NRSA_N77'
																							list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
																						elif uecap._val['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['nonCriticalExtension']['irat-ParametersNR-v1540']['supportedBandListNR-SA-r15'][band_number]['bandNR-r15'] == 78:
																							s_param = 'NRSA_N78'
																							list_ue_cap[i_row][s_param] = list_ue_cap[i_row][s_param] + 1
																						
																				
																				except:
																					pass
																			except:
																				pass
																		except:
																			pass

																		
																except:
																	pass
														except:
															pass
												except:
													pass




												#supportedBandListEUTRA
												i_num_freq = len(uecap._val['rf-Parameters']['supportedBandListEUTRA'])
												for num_freq in range(0, i_num_freq):
													band[num_freq] = uecap._val['rf-Parameters']['supportedBandListEUTRA'][num_freq]['bandEUTRA']
													if band[num_freq] < 64:
														band_check[band[num_freq]-1] = 1


												i_crnti = int.from_bytes(read_file.read(5), byteorder='big', signed=False)

												for num_freq in range(0, 46):
													if band_check[num_freq] == 1:
														list_ue_cap[i_row]['band_%s'%(num_freq+1)] = list_ue_cap[i_row]['band_%s'%(num_freq+1)] + 1
												
												sTemp2 = "%s"%i_mmes1apid
												isFound = False
												if band_check[2] == 1:
													sTemp2 = "%s,1"%sTemp2
													isFound = True
												else:
													sTemp2 = "%s,0"%sTemp2
												if band_check[7] == 1:
													sTemp2 = "%s,1"%sTemp2
													isFound = True
												else:
													sTemp2 = "%s,0"%sTemp2
												if band_check[40] == 1:
													sTemp2 = "%s,1"%sTemp2
													isFound = True
												else:
													sTemp2 = "%s,0"%sTemp2
												
												if isCA:
													sTemp2 = "%s,1"%sTemp2
												else:
													sTemp2 = "%s,0"%sTemp2
												
												if isCA3CC:
													sTemp2 = "%s,1"%sTemp2
												else:
													sTemp2 = "%s,0"%sTemp2
												if isMIMO4x4:
													sTemp2 = "%s,1"%sTemp2
												else:
													sTemp2 = "%s,0"%sTemp2
												if isENDC:
													sTemp2 = "%s,1"%sTemp2
												else:
													sTemp2 = "%s,0"%sTemp2
												if isNRSA:
													sTemp2 = "%s,1"%sTemp2
												else:
													sTemp2 = "%s,0"%sTemp2

												
												if isFound:
													f_ue_band.write("%s\n"%sTemp2)
										
											except:
												pass
										
										#f_ue_cap.write(sTemp2)
										icount = icount + 42 + i_l3message_lenght

							elif i_event_id == 3075:
								i_count_ue_meas = i_count_ue_meas + 1
								#-----INTERNAL_PER_RADIO_UE_MEASUREMENT-----------
								i_hour= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								i_minute= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								i_second= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								i_msecond= int.from_bytes(read_file.read(2), byteorder='big', signed=False)
								s_hour = "%s.%s.%s.%s" %(i_hour, i_minute, i_second, i_msecond)
								skip = read_file.read(4)
								i_ecgi= int.from_bytes(read_file.read(4), byteorder='big', signed=False)
								i_enodebid = int(i_ecgi/256)
								i_ci = i_ecgi % 256
								skip = read_file.read(3)
								i_mmes1apid= int.from_bytes(read_file.read(4), byteorder='big', signed=False)
								key_10s = "%s_%s_%s_%s_%s_%s_%s" %(i_mmes1apid, i_year, i_month, i_date, i_hour, i_minute, int(i_second/10)*10)
								key_mnt = "%s_%s_%s_%s_%s_%s" %(i_mmes1apid, i_year, i_month, i_date, i_hour, i_minute)
								key_3mnt = "%s_%s_%s_%s_%s_%s" %(i_mmes1apid, i_year, i_month, i_date, i_hour, int(i_minute/3)*3)
								skip = read_file.read(14)
								
								sTemp2 = sTemp = "%s,%s,%s,%s,%s,%s" %(s_tanggal,s_hour,s_sitename,i_enodebid,i_ci,i_mmes1apid)
								i_rank_tx_0 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_rank_tx_1 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_rank_tx_2 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_rank_tx_3 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_rank_tx_4 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_rank_tx_5 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_rank_tx_6 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_rank_tx_7 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi_reported_0 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi_reported_1 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi_reported_2 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi_reported_3 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi_reported_4 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi_reported_5 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi_reported_6 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi_reported_7 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi_reported_8 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi_reported_9 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi_reported_10 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi_reported_11 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi_reported_12 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi_reported_13 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi_reported_14 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi_reported_15 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_rank_reported_0 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_rank_reported_1 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_tbs_power_restricted = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_tbs_power_unrestricted = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi2_reported_0 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi2_reported_1 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi2_reported_2 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi2_reported_3 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi2_reported_4 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi2_reported_5 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi2_reported_6 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi2_reported_7 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi2_reported_8 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi2_reported_9 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi2_reported_10 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi2_reported_11 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi2_reported_12 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi2_reported_13 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi2_reported_14 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_cqi2_reported_15 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_out_loop_adj_dl_0 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_out_loop_adj_dl_1 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_out_loop_adj_dl_2 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_out_loop_adj_dl_3 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_out_loop_adj_dl_4 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_out_loop_adj_dl_5 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_out_loop_adj_dl_6 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_out_loop_adj_dl_7 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_sinr_pusch_0 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_sinr_pusch_1 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_sinr_pusch_2 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_sinr_pusch_3 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_sinr_pusch_4 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_sinr_pusch_5 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_sinr_pusch_6 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_sinr_pusch_7 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_sinr_pucch_0 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_sinr_pucch_1 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_sinr_pucch_2 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_sinr_pucch_3 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_sinr_pucch_4 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_sinr_pucch_5 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_sinr_pucch_6 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_sinr_pucch_7 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								#i_ta = int.from_bytes(read_file.read(2), byteorder='big', signed=False)  -- this available on UETR
								i_delta_sinr_pusch_0 = int.from_bytes(read_file.read(4), byteorder='big', signed=False)
								i_delta_sinr_pusch_1 = int.from_bytes(read_file.read(4), byteorder='big', signed=False)
								i_delta_sinr_pusch_2 = int.from_bytes(read_file.read(4), byteorder='big', signed=False)
								i_delta_sinr_pusch_3 = int.from_bytes(read_file.read(4), byteorder='big', signed=False)
								i_delta_sinr_pusch_4 = int.from_bytes(read_file.read(4), byteorder='big', signed=False)
								i_delta_sinr_pusch_5 = int.from_bytes(read_file.read(4), byteorder='big', signed=False)
								i_delta_sinr_pusch_6 = int.from_bytes(read_file.read(4), byteorder='big', signed=False)
								i_delta_sinr_pusch_7 = int.from_bytes(read_file.read(4), byteorder='big', signed=False)
								i_aoa = int.from_bytes(read_file.read(2), byteorder='big', signed=False)
								if i_aoa > 0:
									print("AOA value: %s" % i_aoa)
								i_power_headroom = int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								i_rank_tx_8 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_rank_tx_9 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_rank_tx_10 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_rank_tx_11 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_power_headroom_prb_used = int.from_bytes(read_file.read(2), byteorder='big', signed=False)
								i_rank_reported_2 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_rank_reported_3 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_last_ri_reported = int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								i_last_cqi_1_reported = int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								i_last_cqi_2_reported = int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								i_last_pusch_num_prb_reported = int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								i_last_pusch_sinr_reported = int.from_bytes(read_file.read(2), byteorder='big', signed=False)
								i_last_power_per_prb_reported = int.from_bytes(read_file.read(2), byteorder='big', signed=False)
								i_sinr_meas_pusch_8 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_rank_tx_12 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								i_rank_tx_13 = int.from_bytes(read_file.read(3), byteorder='big', signed=False)

								skip = read_file.read(96)
								skip = read_file.read(12)
								skip = read_file.read(96)
								skip = read_file.read(96)
								
								i_plmn_index = int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								i_ue_category_flex = int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								i_spid_value = int.from_bytes(read_file.read(2), byteorder='big', signed=False)
								i_br_ce_level = int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								i_ue_power_class = int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								i_subs_group_id = int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								
								
													
								icount = icount + 624
								sTemp2 = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n"%(sTemp2,i_aoa,i_ue_category_flex,i_ue_power_class,i_cqi_reported_0,i_cqi_reported_1,i_cqi_reported_2,i_cqi_reported_3,i_cqi_reported_4,i_cqi_reported_5,i_cqi_reported_6,i_cqi_reported_7,i_cqi_reported_8,i_cqi_reported_9,i_cqi_reported_10,i_cqi_reported_11,i_cqi_reported_12,i_cqi_reported_13,i_cqi_reported_14,i_cqi_reported_15,i_rank_reported_0,i_rank_reported_1,i_tbs_power_restricted,i_sinr_pusch_0,i_sinr_pusch_1,i_sinr_pusch_2,i_sinr_pusch_3,i_sinr_pusch_4,i_sinr_pusch_5,i_sinr_pusch_6,i_sinr_pusch_7,i_sinr_meas_pusch_8,i_rank_reported_2,i_rank_reported_3,key_10s,key_mnt,key_3mnt)
								
								f_ue_meas.write(sTemp2)
								#print("Ue Meas : ")
								
							elif i_event_id == 3076:
								i_count_ue_traffic = i_count_ue_traffic + 1
								#-----INTERNAL_PER_UE_TRAFFIC_REP-----------
								i_hour= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								i_minute= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								i_second= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								i_msecond= int.from_bytes(read_file.read(2), byteorder='big', signed=False)
								s_hour = "%s.%s.%s.%s" %(i_hour, i_minute, i_second, i_msecond)
								skip = read_file.read(4)
								i_ecgi= int.from_bytes(read_file.read(4), byteorder='big', signed=False)
								i_enodebid = int(i_ecgi/256)
								i_ci = i_ecgi % 256
								skip = read_file.read(3)
								i_mmes1apid= int.from_bytes(read_file.read(4), byteorder='big', signed=False)
								key_10s = "%s_%s_%s_%s_%s_%s_%s" %(i_mmes1apid, i_year, i_month, i_date, i_hour, i_minute, int(i_second/10)*10)
								key_mnt = "%s_%s_%s_%s_%s_%s" %(i_mmes1apid, i_year, i_month, i_date, i_hour, i_minute)
								key_3mnt = "%s_%s_%s_%s_%s_%s" %(i_mmes1apid, i_year, i_month, i_date, i_hour, int(i_minute/3)*3)
								skip = read_file.read(14)
								
								sTemp2 = sTemp = "%s,%s,%s,%s,%s,%s" %(s_tanggal,s_hour,s_sitename,i_enodebid,i_ci,i_mmes1apid)
								skip = read_file.read(143)	#parameter not decoded
								i_RADIOTHP_VOL_DL = int.from_bytes(read_file.read(5), byteorder='big', signed=False)
								i_RADIOTHP_RES_DL = int.from_bytes(read_file.read(5), byteorder='big', signed=False)
								i_RADIOTHP_VOL_UL = int.from_bytes(read_file.read(5), byteorder='big', signed=False)
								i_RADIOTHP_RES_UL = int.from_bytes(read_file.read(5), byteorder='big', signed=False)
								skip = read_file.read(5)
								i_UE_THP_DL_DRB = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								if i_UE_THP_DL_DRB > 8388607:
									i_UE_THP_DL_DRB = ""
								i_UE_THP_UL_DRB = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								if i_UE_THP_UL_DRB > 8388607:
									i_UE_THP_UL_DRB = ""
								
								
													
								#icount = icount + 100
								sTemp2 = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n"%(sTemp2,i_RADIOTHP_VOL_DL,i_RADIOTHP_RES_DL,i_RADIOTHP_VOL_UL,i_RADIOTHP_RES_UL,i_UE_THP_DL_DRB,i_UE_THP_UL_DRB,key_10s,key_mnt,key_3mnt)
								
								f_ue_traffic.write(sTemp2)
								#print("Ue Meas : ")	
							
							elif i_event_id == 3112:
								#-----MDT Event-----------
								
								i_hour= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								i_minute= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								i_second= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								i_msecond= int.from_bytes(read_file.read(2), byteorder='big', signed=False)
								s_hour = "%s" %(i_hour)
								skip = read_file.read(4)
								i_ecgi= int.from_bytes(read_file.read(4), byteorder='big', signed=False)
								i_enodebid = int(i_ecgi/256)
								i_ci = i_ecgi % 256
								skip = read_file.read(3)
								i_mmes1apid= int.from_bytes(read_file.read(4), byteorder='big', signed=False)
								key_10s = "%s_%s_%s_%s_%s_%s_%s" %(i_mmes1apid, i_year, i_month, i_date, i_hour, i_minute, int(i_second/10)*10)
								key_mnt = "%s_%s_%s_%s_%s_%s" %(i_mmes1apid, i_year, i_month, i_date, i_hour, i_minute)
								key_3mnt = "%s_%s_%s_%s_%s_%s" %(i_mmes1apid, i_year, i_month, i_date, i_hour, int(i_minute/3)*3)
								skip = read_file.read(15)   #prev 11 + 3 (unique ueid) + 1 meastype
								#i_unique_ueid= int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								#i_meas_type= int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								#s_meas_type = dict_meas_type[i_meas_type]
								i_rsrp_serving = get_rsrp(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
								i_rsrq_serving = get_rsrq(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
								
								i_pci_1 = get_pci(int.from_bytes(read_file.read(2), byteorder='big', signed=False))
								i_rsrp_1 = get_rsrp(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
								i_rsrq_1 = get_rsrq(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
								i_pci_2 = get_pci(int.from_bytes(read_file.read(2), byteorder='big', signed=False))
								i_rsrp_2 = get_rsrp(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
								i_rsrq_2 = get_rsrq(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
								i_pci_3 = get_pci(int.from_bytes(read_file.read(2), byteorder='big', signed=False))
								i_rsrp_3 = get_rsrp(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
								i_rsrq_3 = get_rsrq(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
								i_pci_4 = get_pci(int.from_bytes(read_file.read(2), byteorder='big', signed=False))
								i_rsrp_4 = get_rsrp(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
								i_rsrq_4 = get_rsrq(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
								skip = read_file.read(16)

								#i_pci_5 = get_pci(int.from_bytes(read_file.read(2), byteorder='big', signed=False))
								#i_rsrp_5 = get_rsrp(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
								#i_rsrq_5 = get_rsrq(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
								#i_pci_6 = get_pci(int.from_bytes(read_file.read(2), byteorder='big', signed=False))
								#i_rsrp_6 = get_rsrp(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
								#i_rsrq_6 = get_rsrq(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
								#i_pci_7 = get_pci(int.from_bytes(read_file.read(2), byteorder='big', signed=False))
								#i_rsrp_7 = get_rsrp(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
								#i_rsrq_7 = get_rsrq(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
								#i_pci_8 = get_pci(int.from_bytes(read_file.read(2), byteorder='big', signed=False))
								#i_rsrp_8 = get_rsrp(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
								#i_rsrq_8 = get_rsrq(int.from_bytes(read_file.read(1), byteorder='big', signed=False))
								
								i_coordinate = int.from_bytes(read_file.read(1), byteorder='big', signed=False)
								h_point = read_file.read(8)
								h_point_w_alti = read_file.read(11)
								h_point_w_uncer_circle = read_file.read(9)
								h_point_w_uncer_ellipse = read_file.read(12)
								h_point_w_alti_n_uncer_ellipse = read_file.read(16)
								h_arc = read_file.read(14)
								h_polygon_1 = read_file.read(17)
								h_polygon_2 = read_file.read(17)
								h_polygon_3 = read_file.read(17)
								h_polygon_4 = read_file.read(17)
								h_polygon_5 = read_file.read(17)
								h_polygon_6 = read_file.read(17)
								h_polygon_7 = read_file.read(17)
								h_horizontal_velocity = read_file.read(5)
								h_gnss_tod_msec = read_file.read(4)
								i_earfcn = int.from_bytes(read_file.read(3), byteorder='big', signed=False)
								
								i_altitude = None
								if i_coordinate == 0:
									s_location = h_point
								elif i_coordinate == 1:
									s_location = h_point_w_alti
									i_altitude = get_altitude(s_location)
								elif i_coordinate == 2:
									s_location = h_point_w_uncer_circle
								elif i_coordinate == 3:
									s_location = h_point_w_uncer_ellipse
								elif i_coordinate == 4:
									s_location = h_point_w_alti_n_uncer_ellipse
									i_altitude = get_altitude(s_location)
								elif i_coordinate == 5:
									s_location = h_arc
								elif i_coordinate == 6:
									s_location = h_polygon_1
								else:
									s_location = ''
								
								if s_location == '':
									ilong = ilat = ''
								else:
									ilong,ilat = get_coordinate(s_location)
									
									i_count_mdt = i_count_mdt + 1
									sTemp = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" %(s_tanggal,s_hour,s_sitename,i_enodebid,i_ci,i_mmes1apid,ilong, ilat,i_altitude,i_rsrp_serving,i_rsrq_serving,i_earfcn,i_pci_1,i_rsrp_1,i_rsrq_1,i_pci_2,i_rsrp_2,i_rsrq_2,i_pci_3,i_rsrp_3,i_rsrq_3,i_pci_4,i_rsrp_4,i_rsrq_4,key_10s,key_mnt,key_3mnt)
									f_mdt_m1.write(sTemp)
								
						else:
							if i_position > 10000:
								#print("event not in the list....")
								i_position = file_length_in_bytes + 10
							if i_len_event - 7 >= 0:
								content = read_file.read(i_len_event - 7)
								
							
						i_position = i_position + i_len_event
						read_file.seek(i_position,0)
						i_cursor = read_file.tell()
						if i_cursor > i_position :
							i_position = file_length_in_bytes + 10
							print("error on reading %s on eventid: %s"%(file,i_event_id))
						remaining = file_length_in_bytes - i_position
						
				read_file.close()
				dist_ta.close()
				f_mdt_m1.close()
				f_ue_meas.close()
				f_ue_traffic.close()
				f_ue_band.close()

				if i_max_row > 0:
					for i_row in range(i_max_row):
						sTemp = ''
						i = 0
						for col in col_uecap:
							if i == 0:
								sTemp = "%s" % list_ue_cap[i_row][col]
							else:
								sTemp = "%s,%s" %(sTemp, list_ue_cap[i_row][col])
							i = i + 1
						sTemp = "%s\n" %(sTemp)
						f_ue_cap.write(sTemp)

				f_ue_cap.close()
				if i_count_ue_cap == 0:
					os.remove(file_ue_cap)
					os.remove(file_ue_band)
				if i_count_mdt == 0:
					os.remove(file_mdt_m1)
					os.remove(file_ue_meas)
					os.remove(file_ue_traffic)
					try:
						os.remove(file_ue_band)
					except:
						pass
				elif i_count_ue_meas == 0:
					#print("i_count_ue_meas: %s" % i_count_ue_meas)
					os.remove(file_ue_meas)
				#else:
				#	print("   mdt data found.........")
				if i_count_ue_traffic == 0:
					if i_count_mdt > 0:
						os.remove(file_ue_traffic)

				if i_count_ta == 0:
					os.remove(file_ta_dist)
				os.remove(file)

			#except Exception as e:
			#	print("Eror %s" %e)

		#if len(set_mmes1apid) > 0:
		#	for i_mmes1apid in set_mmes1apid:
		#		f_ue_nr.write('%s,%s\n'%(i_mmes1apid,dict_nr_ue[i_mmes1apid]))
		#f_ue_nr.close()


		#print("combining TA: %s" % datetime.now())
		try :
			i = 1
			sTemp = "tanggal,hour,sitename,enodebid,ci,sample,ta\n"
			with open("%s/ta_histogram.csv" % csv_path, "w") as outfile:
				outfile.write(sTemp)
				for file in os.listdir(csv_path):
					if file.startswith("ta_dist_"):
						i = i + 1
						with open('%s/%s'%(csv_path,file), "r") as infile:
							outfile.write(infile.read())
						os.remove('%s/%s'%(csv_path,file))
			
			#calculate percentage TA  --- > not used now
			if i == 1:
				os.remove("%s/ta_histogram.csv" % csv_path)
			else:
				calculate_perc_ta("%s/ta_histogram.csv" % csv_path, csv_path)
		except:
			pass

		#Combine Ue Measurement
		try :
			i = 1
			temp = "date,hour,site,enodebid,ci,mmes1apid,aoa,ue_category_flex,ue_power_class,cqi_reported_0,cqi_reported_1,cqi_reported_2,cqi_reported_3,cqi_reported_4,cqi_reported_5,cqi_reported_6,cqi_reported_7,cqi_reported_8,cqi_reported_9,cqi_reported_10,cqi_reported_11,cqi_reported_12,cqi_reported_13,cqi_reported_14,cqi_reported_15,rank_reported_0,rank_reported_1,tbs_power_restricted,sinr_pusch_0,sinr_pusch_1,sinr_pusch_2,sinr_pusch_3,sinr_pusch_4,sinr_pusch_5,sinr_pusch_6,sinr_pusch_7,sinr_pusch_8,rank_reported_2,rank_reported_3,key_10s,key_mnt,key_3mnt\n"        
			with open("%s/result_ue_meas.csv"%csv_path, "w") as outfile:
				outfile.write(temp)
				for file in os.listdir(csv_path):
					if file.startswith("ue_meas"):
						i = i + 1
						with open('%s/%s'%(csv_path,file), "r") as infile:
							outfile.write(infile.read())
						os.remove('%s/%s'%(csv_path,file))
			if i == 1 :
				os.remove("%s/result_ue_meas.csv"%csv_path)
			
			#print("complete ue meas summary : %s" % datetime.now())
		except:
			pass

		#Combine Ue Traffic
		try :
			i = 1
			temp = "date,hour,site,enodebid,ci,mmes1apid,radio_vol_dl_byte,radio_resource_dl_rb,radio_vol_ul_byte,radio_resource_ul_rb,ue_throughput_dl_drb_kbps,ue_throughput_ul_drb_kbps,key_10s,key_mnt,key_3mnt\n"        
			with open("%s/result_ue_traffic.csv"%csv_path, "w") as outfile:
				outfile.write(temp)
				for file in os.listdir(csv_path):
					if file.startswith("ue_traffic"):
						i = i + 1
						with open('%s/%s'%(csv_path,file), "r") as infile:
							outfile.write(infile.read())
						os.remove('%s/%s'%(csv_path,file))
			if i == 1 :
				os.remove("%s/result_ue_traffic.csv"%csv_path)
		except:
			pass

		#print("combining MDT: %s" % datetime.now())
		try :
			i = 1
			temp = "date,hour,site,enodebid,ci,mmes1apid,longitude,latitude,altitude,rsrp_serving,rsrq_serving,earfcn_neigh,pci_1,rsrp_1,rsrq_1,pci_2,rsrp_2,rsrq_2,pci_3,rsrp_3,rsrq_3,pci_4,rsrp_4,rsrq_4,key_10s,key_mnt,key_3mnt\n"
			file_mdt = "%s/mdt_result.csv"%csv_path
			with open(file_mdt, "w") as outfile:
				outfile.write(temp)
				for file in os.listdir(csv_path):
					if file.startswith("mdt_m1"):
						i = i + 1
						with open('%s/%s'%(csv_path,file), "r") as infile:
							outfile.write(infile.read())
						os.remove('%s/%s'%(csv_path,file))
			if i == 1 :
				os.remove(file_mdt)
			else:
				file_meas = "%s/result_ue_meas.csv"%csv_path
				file_output = "%s/res_ue_meas.csv"%csv_path
				combine_mdt_meas(file_mdt, file_meas, file_output)
				
				file_traffic = "%s/result_ue_traffic.csv"%csv_path
				file_output = "%s/res_ue_traffic.csv"%csv_path
				combine_mdt_traffic(file_mdt, file_traffic, file_output)
		except:
			pass

		filenames = get_file_infolder_ue_cap(csv_path)
		filenames2 = get_file_infolder_ue_band(csv_path)
		combine_ue_cap_site(filenames, csv_path)


		if len(filenames2) > 0:
			combine_ue_band(filenames2, csv_path)


def combine_ue_cap_site(filenames, decoded_ctr_path):
	try :
		if len(filenames)> 0:
			file_ue_cap =  "%s/result_ue_cap.csv" % decoded_ctr_path
			f_ue_cap = open(file_ue_cap, "w")
			col_uecap=['tanggal','hour','sitename','enodebid','ci','ue_category_1','ue_category_2','ue_category_3','ue_category_4','ue_category_5','ue_category_6','ue_category_7','ue_category_8','ue_category_9','ue_category_10','ue_category_11','ue_category_12','ue_category_dl_13','ue_category_dl_14','ue_category_ul_13','rel8','rel9','rel10','rel11', 'rel12','rel13','rel14','rel15','band_1','band_2','band_3','band_4','band_5','band_6','band_7','band_8','band_9','band_10','band_11','band_12','band_13','band_14','band_15','band_16','band_17','band_18','band_19','band_20','band_21','band_22','band_23','band_24','band_25','band_26','band_27','band_28','band_29','band_30','band_31','band_32','band_33','band_34','band_35','band_36','band_37','band_38','band_39','band_40','band_41','band_42','band_43','band_44','band_45','band_46','standaloneGNSS','MIMO4x4','256QAM','UL64QAM','fgi_bit7_rlc_um','fgi_bit28_tti_bundling','fgi_bit30_ho_fdd_tdd','ca_2cc', 'ca_3cc','ca_3cc_fgi111_true','ca_3cc_fgi111_112_true', 'ENDC', 'NR_SA', 'ENDC_N1', 'ENDC_N3', 'ENDC_N8', 'ENDC_N40', 'ENDC_N41', 'ENDC_N77', 'ENDC_N78', 'NRSA_N1', 'NRSA_N3', 'NRSA_N8', 'NRSA_N40', 'NRSA_N41', 'NRSA_N77', 'NRSA_N78']
			sTemp = ""
			i = 0
			for col in col_uecap:
				if i == 0:
					sTemp = "%s" % col
				else:
					sTemp = "%s,%s" % (sTemp, col)
				i = i + 1
			sTemp = "%s\n" %sTemp
			f_ue_cap.write(sTemp)

			for file in filenames:
				with open(file, "r") as infile:
					f_ue_cap.write(infile.read())
				os.remove(file)
			f_ue_cap.close()

			#aggregate ue capability daily
			df_ue = pd.read_csv(file_ue_cap, delimiter=",", index_col=None, header='infer')
			del df_ue['hour']
			df_ue = df_ue.groupby(['tanggal','sitename','enodebid','ci'], as_index=False).sum()
			df_ue.to_csv(file_ue_cap, sep=",", header=True, index=False)
			del df_ue
	except:
		pass



def combine_ue_band(filenames, decoded_ctr_path):
	#try :
	if len(filenames)> 0:
		file_ue_cap =  "%s/result_ue_band.csv" % decoded_ctr_path
		f_ue_cap = open(file_ue_cap, "w")
		col_uecap=['mmes1apid','supported_L1800_FDD', 'supported_L900_FDD','supporterd_L2600_TDD', 'CA_capable', 'CA_3CC_capable', 'MIMO4x4', 'ENDC', 'NR_SA']
		sTemp = ""
		i = 0
		for col in col_uecap:
			if i == 0:
				sTemp = "%s" % col
			else:
				sTemp = "%s,%s" % (sTemp, col)
			i = i + 1
		sTemp = "%s\n" %sTemp
		f_ue_cap.write(sTemp)

		for file in filenames:
			with open(file, "r") as infile:
				f_ue_cap.write(infile.read())
			os.remove(file)
		f_ue_cap.close()

		#aggregate ue capability daily
		df_ue = pd.read_csv(file_ue_cap, delimiter=",", index_col=None, header='infer')
		df_ue = df_ue.groupby(['mmes1apid'], as_index=False).max()
		df_ue.to_csv(file_ue_cap, sep=",", header=True, index=False)
		del df_ue

		file_mdt =  "%s/mdt_result.csv" % decoded_ctr_path
		if os.path.isfile(file_mdt):
			df_mdt = pd.read_csv(file_mdt, delimiter=",", index_col=None, header='infer')
			df_mdt['support_L900_FDD'] = df_mdt['mmes1apid']
			df_mdt['support_L1800_FDD'] = df_mdt['mmes1apid']
			df_mdt['support_L2600_TDD'] = df_mdt['mmes1apid']
			df_mdt['CA_capable'] = df_mdt['mmes1apid']
			df_mdt['CA_3CC_capable'] = df_mdt['mmes1apid']
			df_mdt['MIMO4x4'] = df_mdt['mmes1apid']
			df_mdt['ENDC'] = df_mdt['mmes1apid']
			df_mdt['NR_SA'] = df_mdt['mmes1apid']

			df_ue = pd.read_csv(file_ue_cap, delimiter=",", index_col=None, header='infer')
			dict_900 = df_ue.set_index('mmes1apid').to_dict()['supported_L900_FDD']
			dict_1800 = df_ue.set_index('mmes1apid').to_dict()['supported_L1800_FDD']
			dict_2600 = df_ue.set_index('mmes1apid').to_dict()['supporterd_L2600_TDD']
			dict_ca = df_ue.set_index('mmes1apid').to_dict()['CA_capable']
			dict_ca_3cc = df_ue.set_index('mmes1apid').to_dict()['CA_3CC_capable']
			dict_mimo4x4 = df_ue.set_index('mmes1apid').to_dict()['MIMO4x4']
			dict_endc = df_ue.set_index('mmes1apid').to_dict()['ENDC']
			dict_nr_sa = df_ue.set_index('mmes1apid').to_dict()['NR_SA']
			
			df_mdt['support_L900_FDD'] = df_mdt['support_L900_FDD'].map(dict_900)
			df_mdt['support_L1800_FDD'] = df_mdt['support_L1800_FDD'].map(dict_1800)
			df_mdt['support_L2600_TDD'] = df_mdt['support_L2600_TDD'].map(dict_2600)
			df_mdt['CA_capable'] = df_mdt['CA_capable'].map(dict_ca)
			df_mdt['CA_3CC_capable'] = df_mdt['CA_3CC_capable'].map(dict_ca_3cc)
			df_mdt['MIMO4x4'] = df_mdt['MIMO4x4'].map(dict_mimo4x4)
			df_mdt['ENDC'] = df_mdt['ENDC'].map(dict_endc)
			df_mdt['NR_SA'] = df_mdt['NR_SA'].map(dict_nr_sa)
			df_mdt.to_csv(file_mdt, sep=",", header=True, index=False)
		else:
			os.remove(file_ue_cap)

	#except:
	#	pass


def decode_ctr(s_date, s_hour):
	now = datetime.now()

	print("Decoding ctr start on %s" %datetime.now())


	python_file_path = os.path.dirname(os.path.realpath(__file__))


	ctr_path = "/var/opt/common5/ctr_mdt/%s" % (s_date)
	if not os.path.exists(ctr_path):
		os.mkdir(ctr_path)
	ctr_path = "/var/opt/common5/ctr_mdt/%s/%02d" % (s_date, s_hour)
	if not os.path.exists(ctr_path):
		os.mkdir(ctr_path)
	decoded_ctr_path = "/var/opt/common5/mdt/%s" % (s_date)
	if not os.path.exists(decoded_ctr_path):
		os.mkdir(decoded_ctr_path)
	decoded_ctr_path = "/var/opt/common5/mdt/%s/%02d" % (s_date, s_hour)
	if not os.path.exists(decoded_ctr_path):
		os.mkdir(decoded_ctr_path)

	os.chdir(ctr_path)
	print(ctr_path)

	filenames = []
	list_folder = []
	for file in os.listdir(ctr_path):
		path = "%s/%s" %(ctr_path, file)
		if os.path.isdir(path):
			list_folder.append(path)
	
	print('\nList Folder found: %s' %len(list_folder))

	print('start decoding ctr, on %s' %datetime.now())

	for i in range(0,len(list_folder),200):
		list_temp = list_folder[i:i+200]
		duration = datetime.now() - now
		if duration.seconds < 4500:
			try:
				decode_ctr_folder(list_temp)
			except Exception as e:
				print("Error: %s")
		else:
			print('timeout...............')
		print("complete decoding %s sites on %s" %(i + 200, datetime.now()))
	#combine all site level csv into decoded_ctr
	#1. Combine TA histogram

	list_output = ['result_ue_cap', 'res_ue_traffic', 'res_ue_meas', 'mdt_result', 'ta_histogram']
	for output_file in list_output:
		print("combining %s" %output_file)
		super_x = []
		for site in list_folder:
			file = '%s/csv/%s.csv' %(site, output_file)
			if os.path.isfile(file):
				df_new = pd.read_csv(file, delimiter=",", index_col=None, header='infer')
				if output_file == 'mdt_result':
					df_new = df_new.drop(['key_10s','key_mnt','key_3mnt'], 1)
				super_x.append(df_new)
		if len(super_x) > 0:
			if output_file.startswith('res_'):
				output_file = output_file.replace('res_', '')
			super_x = pd.concat(super_x, axis=0)
			#if output_file == 'ue_nr':
			#	#Get unique mmes1apid
			#	super_x = super_x.groupby('mmes1apid', as_index=False).agg({'endc_capable': 'max'})
			super_x.to_csv('%s/%s.csv'%(decoded_ctr_path, output_file), index=None)
			try:
				zip_file = '%s/%s.zip'%(decoded_ctr_path, output_file)
				zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)
				zf.write('%s/%s.csv'%(decoded_ctr_path, output_file), '%s.csv'%output_file)
				zf.close()
				os.remove('%s/%s.csv'%(decoded_ctr_path, output_file))
			except:
				pass
		del super_x



if __name__ == "__main__":
	start_time = datetime.now()
	warnings.filterwarnings("ignore")
	num_worker = 12
	now = datetime.now()
	delta_hours = 4
	s_hour_2 = s_hour = 0
	start_datetime = now - timedelta(hours=delta_hours) #60*3 +30
	stop_datetime = start_datetime.replace(minute=59)

	if len(sys.argv) > 1:
		s_date = sys.argv[1]
		if len(sys.argv) >2:
			s_hour = int(sys.argv[2])
			if len(sys.argv) > 3:
				s_date_2 = sys.argv[3]
				if len(sys.argv) >4:
					s_hour_2 = int(sys.argv[4])
					if len(sys.argv) > 5:
						option = int(sys.argv[5])
						if option == 0:
							collect_data = True
							calculate_kpi = False
						elif option == 1:
							collect_data = True
							calculate_kpi = True
						elif option == 2:
							collect_data = False
							calculate_kpi = True
				else:
					s_hour_2 = s_hour
			else:
				s_date_2 = s_date
				s_hour_2 = s_hour
		else:
			s_hour = s_hour_2 = 0
			s_date_2 = s_date
		
		start_datetime = datetime.strptime(s_date, '%Y%m%d')
		start_datetime = start_datetime.replace(hour=s_hour)
		stop_datetime = datetime.strptime(s_date_2, '%Y%m%d')
		stop_datetime = stop_datetime.replace(hour=s_hour_2, minute=59)


	list_pmic = ['pmic1', 'pmic2']
	for dt in rrule.rrule(rrule.HOURLY, dtstart=start_datetime, until=stop_datetime):
		sekarang = datetime.now()
		menit_sekarang = sekarang.minute
		s_hour = dt.hour
		s_date = dt.strftime("%Y%m%d")
		s_date2 = dt.strftime("%Y-%m-%d")

		print("Collecting data: %s %s" %(s_date, s_hour))
		print("   start on %s" %datetime.now())

		decode_ctr(s_date, s_hour)


