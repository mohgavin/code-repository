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
	list_site = ['410053','410056','410059','410079','410080','410081','410084','410090','410092','410093','410094','410098','410099','410100','410101','410106','410108','410114','410120','410121','410122','410123','410125','410126','410127','410128','410130','410131','410134','410135','410136','410139','410145','410167','410168','410174','410194','410196','410198','410202','410203','410207','410210','410250','410253','410254','410256','410273','410290','410297','410304','410306','410308','410311','410314','410319','410331','410344','410346','410352','410356','410359','410366','410367','410378','410382','410383','410385','410394','410395','410402','410403','410406','410416','410420','410528','410572','410578','410579','410581','410583','410592','410599','410600','410610','410652','410666','410668','410676','410699','410700','410717','410719','410724','410760','410791','410800','410812','410814','410815','410825','410833','410835','410836','410862','410875','410889','410893','410903','410904','410908','410911','410912','410917','410949','410953','410971','410983','410987','410994','410996','411008','411018','411024','411026','411032','411034','411038','411040','411049','411087','411121','411153','411172','411173','411180','411203','411215','411222','411232','411271','420007','420008','420009','420011','420012','420015','420018','420028','420033','420034','420035','420036','420037','420038','420039','420040','420041','420042','420043','420044','420045','420049','420050','420051','420052','420053','420055','420057','420073','420076','420078','420079','420080','420089','420093','420096','420120','420135','420138','420144','420192','420193','420216','420236','420257','420263','420299','420310','420311','420327','420329','420330','420331','420332','420356','420358','420378','420432','420436','420486','420491','420492','420494','420495','420496','420498','420499','420501','420502','420504','420508','420509','420510','420511','420513','420514','420515','420516','420517','420518','420519','420520','420521','420522','420524','420525','420526','420527','420529','420533','420535','420536','420537','420538','420540','420541','420543','420546','420547','420553','420554','420555','420556','420557','420558','420559','420560','420561','420562','420564','420565','420566','420568','420570','420572','420576','420577','420578','420582','420583','420626','420640','420663','420664','420674','420692','420746','420752','420753','420760','420763','420791','420802','420804','420808','420856','420862','420865','420869','420885','420887','420893','420901','420912','420956','420960','420968','420975','420976','420978','420983','420986','420993','420998','421005','421006','421011','421015','421018','421020','421021','421026','421041','421042','421047','421071','421091','421092','421094','421100','421102','421120','421145','421150','421160','421161','421162','421177','421178','421181','421186','421240','421263','421264','421266','421276','421280','421302','421330','421355','430003','430008','430011','430016','430018','430019','430021','430022','430026','430027','430028','430031','430032','430033','430034','430035','430037','430038','430041','430042','430045','430047','430050','430051','430052','430057','430060','430061','430062','430065','430066','430067','430071','430072','430074','430075','430078','430080','430087','430090','430091','430092','430093','430094','430097','430098','430102','430103','430104','430107','430108','430114','430115','430117','430120','430126','430127','430128','430129','430130','430131','430132','430133','430134','430135','430137','430139','430142','430144','430145','430146','430147','430149','430150','430151','430152','430153','430155','430156','430157','430158','430160','430161','430162','430163','430166','430167','430168','430173','430181','430182','430184','430185','430187','430188','430189','430190','430192','430193','430194','430195','430197','430198','430199','430200','430201','430208','430209','430210','430212','430214','430215','430216','430217','430218','430220','430222','430223','430231','430232','430233','430235','430241','430242','430244','430250','430255','430262','430263','430264','430265','430266','430267','430270','430272','430273','430274','430275','430278','430283','430286','430288','430289','430293','430296','430299','430302','430306','430309','430312','430314','430317','430318','430319','430320','430323','430328','430329','430330','430333','430334','430335','430341','430344','430345','430347','430348','430350','430353','430354','430358','430359','430360','430367','430368','430369','430370','430373','430375','430376','430377','430379','430381','430384','430385','430386','430389','430390','430392','430394','430397','430399','430400','430405','430408','430409','430410','430414','430415','430416','430420','430422','430425','430427','430430','430433','430437','430438','430440','430444','430446','430448','430450','430453','430454','430456','430458','430464','430465','430467','430476','430479','430483','430484','430491','430492','430494','430496','430498','430501','430504','430505','430506','430508','430509','430511','430512','430513','430514','430515','430516','430517','430520','430522','430525','430526','430528','430534','430536','430537','430538','430539','430553','430562','430565','430570','430574','430576','430588','430590','430591','430598','430603','430604','430611','430619','430623','430625','430626','430627','430629','430634','430638','430646','430665','430667','430680','430682','430683','430693','430704','430708','430713','430714','430717','430718','430719','430724','430727','430730','430733','430734','430735','430737','430744','430745','430746','430764','430768','430771','430775','430776','430777','430782','430784','430789','430790','430794','430796','430799','430800','430807','430814','430822','430823','430825','430826','430827','430832','430833','430837','430838','430839','430840','430841','430843','430844','430848','430849','430850','430851','430853','430855','430856','430857','430858','430860','430862','430863','430865','430866','430867','430868','430871','430874','430878','430892','430893','430894','430895','430896','430902','430905','430906','430907','430908','430909','430910','430911','430913','430914','430915','430923','430924','430925','430927','430928','430931','430932','430936','430937','430938','430939','430942','430943','430945','430946','430947','430949','430953','430955','430960','430961','430963','430965','430966','430968','430969','430970','430971','430972','430976','430978','430979','430980','430981','430983','430984','430988','430989','430991','430995','430996','430998','431000','431003','431005','431006','431007','431009','431014','431016','431017','431021','431022','431026','431030','431031','431032','431033','431036','431037','431040','431041','431045','431048','431053','431061','431062','431066','431075','431078','431082','431083','440002','440004','440005','440006','440007','440008','440009','440010','440012','440013','440024','440027','440028','440033','440034','440036','440040','440041','440044','440046','440047','440049','440052','440070','440075','440077','440089','440091','440092','440094','440095','440096','440097','440098','440099','440100','440101','440105','440106','440107','440117','440118','440122','440130','440139','440147','440149','440150','440154','440157','440158','440162','440167','440168','440169','440177','440179','440193','440196','440202','440206','440207','440208','440209','440213','440216','440218','440221','440222','440225','440227','440228','440229','440245','440248','440249','440262','440266','440271','440286','440289','440292','440293','440298','440299','440300','440304','440305','440310','440313','440322','440323','440327','440328','440329','440332','440375','440380','440383','440385','440387','440391','440393','440395','440399','440406','440407','440409','440412','440455','440458','440476','440487','440492','440496','440502','440507','440508','440509','440516','440517','440518','440519','440526','440529','440546','440558','440565','440566','440577','440582','440587','440604','440612','440613','440620','440621','440623','440628','440630','440631','440638','440640','440644','440648','440656','440657','440658','440661','440671','440677','440678','440687','440691','440696','440702','440703','440705','440709','440712','440713','440715','440721','440726','440731','440737','440742','440748','440749','440750','440751','440752','440754','440755','440759','440762','440763','440765','440767','440778','440791','440797','440798','440811','440812','440813','440833','440838','440845','440849','440850','440852','440857','440869','440885','440893','440899','440936','451458','460050','460088','460157','460294','470447','470460','470497']
	#list_site = [421279]
	#list_site = ['4424974E', '4424739E', '441PX427E_CO', '441PL390E_CO', '441PL065E_CO', 'MC4431290E', 'SH3421104G_CO', '5018G_CO', '4424949E_CO', '441PC957E', '441PC430E_CO', '4410786E_CO', '441PX077E', '441PX087E', '441PL026E_CO', '4424902E', '442PL658E', '4424933E', '4425013E', '441PC256E_CO', '4425498E_BB', '4425007E_CO', 'V061G_CO', '441PL503E_CO', '441PC164E', 'SH4431545E', '341PC995G_CO', '442PC019E', '441PL074E_CO', '441PC902E', '441PL609E_CO', '341PX400G_CO', '5022G_CO', '442PX4803E', '441PX371E', '441PX910E_CO', '442PC087E', '441PL060E_CO', '441PL461E', '441PC199E_CO', '441PL384E_CO', '4424974E_BB', '441PX078E', '441PX094E_CO', 'MC4424718E', '4433055E', '341SC996G_CO', '441PX323E', '441PC960E_CO', '442PC009E_CO', '5016G_CO', '441PX364E_CO', '442PX349E_CO', '441PC439E', '442PC032E', '5015G_CO', '442PC472E', '4433058E', '5022G_C2', '441PC498E_CO', '442PC225E', '441PC173E', '3412390G_CO', '441PC402E', '5265G_CO', '441PC083E', 'SH4412317E_CO', '442PC179E', '4435480E', '3412296G_CO', '441A088E_CO', '441PC039E', '341PC251G_CO', '441PC124E', 'MC4415798E', '441PC805E', '442PC079E', '441PX029E']


	for file_type in list_file:
		list_df = []
		busy_hours = [12,13,14,15,16,17,23,0,1,2,3,4]
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
    delta_day = 5
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



    
    

    
    
