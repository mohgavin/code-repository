import pandas as pd
import os
import numpy as np
import re
from warnings import warn
from datetime import datetime

ABSOLUTE_PATH = os.path.dirname(__file__) + '/source_data/'

class Hardware:

    def __init__(self, filepath):
        
        # ...Initiate of reading file from pathfile and reading Mapping Layer to Tower...
        self.title = filepath
        self.filepath = os.path.join(ABSOLUTE_PATH, filepath)
        self.date = datetime.strptime(re.findall(r"\d{8}|$", filepath)[0], '%Y%m%d').date()  
        self.files = pd.read_csv(self.filepath)
        self.files['REAL NAME'] = self.files['REAL NAME'].astype(str)
        self.files['NODE NAME'] = self.files['NODE NAME'].astype(str)

        self.files_tower = pd.read_excel(os.path.join(ABSOLUTE_PATH, 'Mapping Layer to Tower_20230301.xlsx'))        
        self.files_tower.rename(columns={'Node ID':'NODE NAME'}, inplace = True)

        # ...Delete Duplication in mapping tower to prevent adding new tower id to base line because of merge/lookup behaviour...
        self.files_tower.drop_duplicates(subset=['NODE NAME'], keep='first', inplace=True)
        self.files_tower['NODE NAME'] = self.files_tower['NODE NAME'].astype(str)

        # ...Lookup mapping tower layer with tower id as no existing tower ID...
        self.files = pd.merge(self.files, self.files_tower[['NODE NAME', 'Special Tower ID mapping for Tower ID with more than 1 site ID']], on='NODE NAME', how='left')

        self.files = self.files[self.files['REAL NAME'].str.contains('TRS5024EV-SB01|TRF5736AMLB309|TRF5736AVLB345')==False]
        self.files = self.files[self.files['HW TYPE'].str.contains('Cabinet not_standart')==False]

        # ...Drop Duplication as the duplication creates false information...
        self.files.drop_duplicates(subset=['SERIAL NUMBER'], keep='first', inplace=True)

        # ...Drop columns as it is not required for summary...
        cols1 = [0, 2, 4, 5, 6, 7, 9, 10]
        self.files.drop(self.files.columns[cols1], axis=1, inplace=True)
        # self.files['REAL NAME'] = self.files['REAL NAME'].astype(str)
        self.files.insert(len(self.files.columns), 'Qty', 1)
        self.files['REAL NAME'] = self.files['REAL NAME'].str.upper()

    def tech_hardware(self, column):#This function has been deprecated.. Please use new method below
       
        self.column = column

        def func1(pivtab):  
            ### Function of logic to return of specific string in the DataFrame `

            if pivtab['Contain_B1B3']:
                return 'RU_B1B3'
            elif pivtab['Contain_B1']:
                return 'RU_B1'
            elif pivtab['Contain_B0']:
                return 'RU_B0'
            elif pivtab['Contain_B3']:
                return 'RU_B3'
            elif pivtab['Contain_B8']:
                return 'RU_B8'
            elif pivtab['Contain_Baseband']:
                return 'Baseband'
            elif pivtab['Contain_6630']:
                return 'Baseband'
            elif pivtab['Contain_RBS']:
                return 'RBS'
            elif pivtab['Contain_Enclosure']:
                return 'RBS'
            elif pivtab['Contain_6140']:
                return 'RBS'
            elif pivtab['Contain_6150']:
                return 'RBS'
            elif pivtab['Contain_6601']:
                return 'RBS'
            elif pivtab['Contain_DUG']:
                return 'Baseband'
            elif pivtab['Contain_DRU9P']:
                return 'RU_B0'
            elif pivtab['Contain_DRU18P']:
                return 'RU_B3'
            elif pivtab['Contain_DXU']:
                return 'Baseband'
            elif pivtab['Contain_DUW']:
                return 'Baseband'
            elif pivtab['Contain_B42']:
                return 'RU_B42'
            elif pivtab['Contain_AIR 1641 B1a B3a']:
                return 'RU_B1B3'       
            elif pivtab['Contain_RRUW']:
                return 'RU_B1'
            elif pivtab['Contain_IRU']:
                return 'IRU'
            elif pivtab['Contain_DUS']:
                return 'Baseband'
            else:
                return '0'

        self.files_tech = pd.merge(self.files, self.files_tower[['NODE NAME', 'Special Tower ID mapping for Tower ID with more than 1 site ID']], on='NODE NAME', how='left')

        match self.column:
            case "all":
                pass
            case _:
                self.files_tech = self.files_tech.loc[self.files_tech['TECH'].str.contains(self.column)]

        #      Delete specific column/Rows as it is not required for pivot table
        
        self.files_tech = self.files_tech[self.files_tech['REAL NAME'].str.contains('TRS5024EV-SB01|TRF5736AMLB309|TRF5736AVLB345')==False]
        self.files_tech = self.files_tech[self.files_tech['HW TYPE'].str.contains('Cabinet not_standart')==False]
        self.files_tech.drop_duplicates(subset=['SERIAL NUMBER'], keep='first', inplace=True)
        
        self.cols1 = [0, 2, 4, 5, 6, 7, 9, 10]
        self.files_tech.drop(self.files_tech.columns[self.cols1], axis=1, inplace=True)

        #      Create Pivot Table to count rows of vertical hardware 
        
        self.table_tech = pd.pivot_table(self.files_tech, index=['Special Tower ID mapping for Tower ID with more than 1 site ID','NODE NAME', 'TECH', 'REAL NAME'], aggfunc=len).stack().reset_index()
        self.table_tech.rename(columns={self.table_tech.columns[5]:'Qty'}, inplace = True)
        self.table_tech.drop(self.table_tech.columns[4], axis=1, inplace=True)

        #      Applying function to contain if there is specific string in the columns 

        self.equipment = ['B1B3', 'B3', 'B0', 'B8', 'Baseband', 'RBS', 'Enclosure', '6140', '6150', '6601', 'DUG', 'DRU9P', 'DRU18P', 'DXU', '6630', 'DUW', 'B1', 'B42', 'AIR 1641 B1a B3a', 'RRUW', 'IRU', 'DUS']

        for a in self.equipment:
            self.table_tech['Contain_'+a] = self.table_tech['REAL NAME'].str.contains(a)
        
        self.table_tech['Category'] = self.table_tech.apply(func1, axis = 1)
        self.table_tech = self.table_tech.loc[self.table_tech['Category'].str.contains('[a-zA-Z]')]
        self.table_tech.drop(self.table_tech.columns[5:(5+len(self.equipment))], axis=1, inplace=True)
        self.table_tech = pd.pivot_table(self.table_tech, index=['Special Tower ID mapping for Tower ID with more than 1 site ID'], columns=['Category', 'REAL NAME'], aggfunc=sum)
        self.table_tech.fillna(0, inplace=True)
        
        match self.column:
            case "2G":
                self.list_hardware = ['RU_B0', 'RU_B1B3', 'RU_B3', 'RU_B8', 'Baseband', 'RBS']

            case "3G":
                self.list_hardware = ['RU_B0', 'RU_B1B3', 'RU_B8', 'Baseband', 'RBS']

            case "4G":
                self.list_hardware = ['RU_B0', 'RU_B1B3', 'RU_B3', 'RU_B8', 'Baseband', 'RBS']

            case "all":
                self.list_hardware = ['RBS', 'Baseband', 'RU_B0', 'RU_B1', 'RU_B3', 'RU_B8', 'RU_B1B3', 'RU_B42', 'IRU']

        self.list_item = []

        #     Make a list of columns from multi-index Columns / Create list of column from spesific parent column or node column / Flatten the list
        
        for a in self.list_hardware:
            self.list_type = self.table_tech.loc[:, (['Qty'], [a])].columns.get_level_values(2)
            self.list_item.append(self.list_type)

        print(self.list_item[0])

        self.table_tech = self.table_tech.droplevel(level=[0,1], axis=1)

        #    Create a function of logic to apply [quantity]Hardware & join them in the same element of series/list.
        def material_tolist (column) : 
    
            self.list1 = [''] * len(self.table_tech.index)
            self.list_flat = []
    
            for a in column:
    
                self.table_tech[a] = self.table_tech[a].apply(np.int64)   
                self.coba = self.table_tech[a].apply(lambda val: f'[{val}]{a}' if val > 0 else '')
                self.list1 = list(zip(*zip(*self.list1), self.coba))

            for a in range(len(self.list1)):
    
                self.joined = "|".join(filter(None, self.list1[a]))
                self.list_flat.append(self.joined)
    
            return(self.list_flat)

        #    Create Dataframe of table_tech Index as basic tower ID
        self.df = pd.DataFrame(self.table_tech.index)
        self.b = 0

        for a in self.list_item:

            self.materialdf = pd.DataFrame(material_tolist(a))
            self.materialdf.rename(columns= {0 : self.list_hardware[self.b]+"_"+self.column}, inplace = True)
            self.b +=1
            
            self.df = pd.merge(self.df, self.materialdf, left_index=True, right_index=True)
        
        self.df.rename(columns={self.df.columns[0]:'Special_Tower_ID_mapping_for_Tower_ID_with_more_than_1_site_ID'}, inplace = True)
        self.df.set_index('Special_Tower_ID_mapping_for_Tower_ID_with_more_than_1_site_ID', inplace=True)
        self.df.columns = pd.MultiIndex.from_product([1 * [''], self.df.columns])
        self.df = self.df.transpose().reset_index()
        self.df = self.df.transpose()
        self.df = self.df.add_suffix(self.filepath)
        return(self.df, pd.DataFrame(data={'File Name': self.title, 'Date': [datetime.strftime(self.date, '%d-%B-%Y')]}))

    def newmethod_tech_hardware(self):

        # ...Create Dictionary to classify equipment based on their category... 
        self.df = pd.DataFrame(self.files['Special Tower ID mapping for Tower ID with more than 1 site ID'].unique())

        rbs_dict    = dict.fromkeys(['6601', 'RBS', 'ENC', '6140', '6150', 'ENCLOSURE'], 'RBS')
        b1b3_dict   = dict.fromkeys(['B1B3', 'AIR 1641 B1a B3a'], 'RU_B1B3')
        bb_dict     = dict.fromkeys(['DUG', '6651', 'DXU', 'BASEBAND', 'DUW','6630', 'DUS', 'IXU'], 'BASEBAND')
        b1_dict     = dict.fromkeys(['RRUW', 'B1'], 'RU_B1')
        b3_dict     = dict.fromkeys(['DRU18P', 'B3','RRU_T'], 'RU_B3')
        b0_dict     = dict.fromkeys(['DRU9P', 'B0'], 'RU_B0')
        b42_dict    = dict.fromkeys(['B42'], 'RU_B42')
        b8_dict     = dict.fromkeys(['B8'], 'RU_B8')
        iru_dict    = dict.fromkeys(['IRU'], 'IRU')
        b258_dict   = dict.fromkeys(['B258'], 'RU_B258')

        self.equipment_dict = rbs_dict | b1b3_dict | bb_dict | b1_dict | b3_dict | b0_dict | b42_dict | b8_dict | iru_dict | b258_dict

        def matcher(x):

            for a in self.equipment_dict:
                if a.upper() in x.upper():
                    return f'{self.equipment_dict[a]}'

        self.files['Categories'] = self.files['REAL NAME'].apply(matcher)
        self.files['Categories'].fillna(value=np.nan, inplace=True)
        self.files = self.files.loc[self.files['Categories'].str.contains(r'.', regex=True)==True].copy(deep=True)
        self.files['CONCATENATE-1'] = self.files['Special Tower ID mapping for Tower ID with more than 1 site ID'] + "_" + self.files['Categories']

        self.files = self.files.groupby(['CONCATENATE-1', 'REAL NAME'])['Qty'].sum().reset_index()
        self.files['COMBINE-1'] = "[" + self.files['Qty'].astype(str) + "]" + self.files['REAL NAME']
        self.files = self.files.groupby('CONCATENATE-1')['COMBINE-1'].apply(lambda x: "%s" % '+'.join(x)).reset_index()

        self.list_item = ['RBS', 'BASEBAND', 'RU_B8', 'RU_B0', 'RU_B3', 'RU_B1', 'RU_B1B3', 'RU_B42', 'RU_B258', 'IRU']

        for a in self.list_item :
            self.df[f'Special Tower ID mapping for Tower ID with more than 1 site ID_{a}'] = self.df[0] + "_" + a

        self.list_call = list(self.df.columns)
        self.list_call.pop(0)

        for a, b in zip(self.list_call, self.list_item):
            self.df = pd.merge(self.df, self.files[['COMBINE-1']], left_on = a, right_on = self.files['CONCATENATE-1'], how='left')
            self.df.rename(columns={"COMBINE-1": b }, inplace = True)

        self.df.drop(self.list_call, axis=1, inplace=True)
        self.df.rename(columns={0:'Special_Tower_ID_mapping_for_Tower_ID_with_more_than_1_site_ID'}, inplace=True)
        self.df.set_index('Special_Tower_ID_mapping_for_Tower_ID_with_more_than_1_site_ID', inplace=True)

        # ...Create Multiindex to add some blank row and reserved the column for another information...
        self.df.columns = pd.MultiIndex.from_product([1 * [''], self.df.columns])
        self.df = self.df.transpose().reset_index()
        self.df = self.df.transpose()
        self.df = self.df.add_suffix(self.filepath)
        
        return(self.df, pd.DataFrame(data={'File Name': self.title, 'Date': [datetime.strftime(self.date, '%d-%B-%Y')]}))

class Radio_Sector:

    def __init__(self, filepath):

        self.column_list = [0,12,17,41,42,46,51,54,56,85,89,90,95]

        self.title = filepath
        self.filepath = os.path.join(ABSOLUTE_PATH, filepath)
        self.date = datetime.strptime(re.findall(r"\d{8}|$", filepath)[0], '%Y%m%d').date()

        # self.files   = pd.read_csv(self.filepath, usecols=self.column_list, low_memory=False)
        self.files = pd.read_excel(self.filepath, sheet_name='4G', usecols=self.column_list, dtype=str)

        self.files = self.files[self.files['dlChannelBandwidth'].astype(str).str.contains('dlChannelBandwidth')==False].copy(deep=True)
        # self.files = self.files[self.files['REMARK'].astype(str).str.contains('FALSE')==True].copy(deep=True) #If using CSV, Boolean dont change
        self.files = self.files[self.files['REMARK'].astype(str).str.contains('False')==True] #If using XLSX, string is changed.

        self.files['BW'] = self.files['dlChannelBandwidth'].astype(int).div(1000).astype(str)
        self.files['freqBand-NewName'] = self.files['freqBand'].replace({'8': 'Band-900', '3': 'Band-1800', '1':'Band-2100'}, regex=True)
        self.files['freqBand'] = self.files['freqBand'].replace({'8': 'Band-900', '3': 'Band-1800', '1':'Band-2100'}, regex=True)

        self.files['TYPE2'].replace({'0':'NO_INFO'}, inplace=True)
        self.files['SECTOR_FINAL'].replace({'x':0}, inplace=True)
        self.files['SECTOR_FINAL'] = self.files['SECTOR_FINAL'].astype(int).replace({5:4, 6:4}).astype(str)
        self.files['SECTOR_FINAL'] = "SECTOR_FINAL_" + self.files['SECTOR_FINAL'].astype(str)
        self.files['TYPE2']   = self.files['TYPE2'].str.upper()

        self.files['freqBand-NewName'] = self.files['freqBand-NewName'].astype(str).str.strip()
        self.files['productName'].fillna('0', inplace=True)
        self.files['productName'].replace({' ': ''}, inplace=True, regex=True)
        self.files['DATE'] =  pd.to_datetime(self.files['DATE'])

        # ...Sorting the date column as it requires get the most updates rows...
        self.files = self.files.sort_values(by='DATE', ascending=False)

        # ...Create a concatenation of Tower DNK and Column Name to get lookup index...
        self.files['CONCATENATE-1'] = self.files[['TOWER_DNK', 'freqBand-NewName', 'SECTOR_FINAL']].astype(str).agg('_'.join, axis=1)
        self.files['CONCATENATE-2'] = self.files[['TOWER_DNK', 'freqBand-NewName', 'serialNumber', 'SECTOR_FINAL']].astype(str).agg('_'.join, axis=1)
        self.files['CONCATENATE-3'] = self.files[['TOWER_DNK', 'serialNumber', 'SECTOR_FINAL']].astype(str).agg('_'.join, axis=1)
        self.files['CONCATENATE-6'] = self.files[['TOWER_DNK', 'freqBand', 'sectorFunctionRef']].astype(str).agg('_'.join, axis=1)

        self.files.insert(len(self.files.columns), column='Qty_productName', value=1)
        self.files['MIMO'] = self.files['noOfTxAntennas'].astype(str) + "x" + self.files['noOfRxAntennas'].astype(str)

        #...Create 2 files variable : 
        
        # files1 (Files 1 is used for calculate Qty of Radio Per Sector)
        # files2 (Files 2 is used for calculate Cell Qty of MIMO and BW)
        
        self.files1 = self.files.drop_duplicates(subset=['CONCATENATE-2'], keep='first').copy(deep=True)
        self.files1['CONCATENATE-3_LOGIC'] = self.files1['CONCATENATE-3'].duplicated(keep=False)
        self.files1.loc[self.files1['CONCATENATE-3_LOGIC'] == True, 'freqBand-NewName'] = 'Band-1800&2100'
        self.files1['CONCATENATE-1'] = self.files1[['TOWER_DNK', 'freqBand-NewName', 'SECTOR_FINAL']].astype(str).agg('_'.join, axis=1)
        self.files1['CONCATENATE-1'] = self.files1[['TYPE2', 'CONCATENATE-1']].astype(str).agg('|'.join, axis=1)
        self.files1['CONCATENATE-2'] = self.files1[['TOWER_DNK', 'freqBand-NewName', 'serialNumber', 'SECTOR_FINAL']].astype(str).agg('_'.join, axis=1)

        #...Drop Duplication as the duplication is a false information ...
        self.files1.drop_duplicates(subset=['CONCATENATE-2'], keep='first', inplace=True)
        
        self.extract_rf = self.files1['Rfport'].str.extract(r'(\b)(RfPort=.)')
        self.files1 = self.files1.join(self.extract_rf)
        self.files1['CONCATENATE-4'] = self.files1[['TOWER_DNK', 'serialNumber']].astype(str).agg('_'.join, axis=1)

        self.files2 = self.files.drop_duplicates(subset=['CONCATENATE-6'], keep='first')

        self.df = pd.DataFrame(self.files['TOWER_DNK'].unique())
        self.df.rename(columns={self.df.columns[0]:'TOWER_DNK'}, inplace = True)
        self.df.set_index('TOWER_DNK', inplace=True)

    def oldmethod_radiosector(self): #This function has been deprecated.. Slow method... Please use new method below...

        warn("This function has been deprecated...Please use new method below as this method is slow")
        self.towerid_hardware = []

        for a in np.unique(self.files['CONCATENATE-1']):

            self.series_hardware = self.files[self.files['CONCATENATE-1'] == a]['productName'].value_counts().astype(str)
            self.list_string = list(zip(self.series_hardware, self.series_hardware.index))
            self.combined = [a] + ['|'.join(['x'.join(tups) for tups in self.list_string])]
            self.towerid_hardware.append(self.combined)

        self.df_hardware = pd.DataFrame(self.towerid_hardware, columns=['CONCATENATE-1', 'productName'])

        for a in self.list_call:
            self.df = pd.merge(self.df, self.df_hardware[['productName']], left_on = a, right_on = self.df_hardware['CONCATENATE-1'], how='left')
            self.df.rename(columns={"productName": "Radio_" + a }, inplace = True)

        self.df.set_index('TOWER_DNK', inplace=True)
        self.df.drop(self.list_call, axis=1, inplace=True)
        self.df.columns = self.df.columns.str.replace('TOWER_DNK_','')

        for a in self.df.columns:
            if len(self.df[a].value_counts()) == 0:
                self.df.drop(columns=[a], inplace=True)
        
        self.df.index.rename('Special_Tower_ID_mapping_for_Tower_ID_with_more_than_1_site_ID', inplace = True)
        self.df.columns = pd.MultiIndex.from_product([self.date, self.df.columns])
        self.df = self.df.transpose().reset_index()
        self.df = self.df.transpose()
        return(self.df)

    def newmethod_radiosector(self): #This function need to be code refactory. Requires a lot of RAM Memory. Please use new method below...
        
        self.list_concatenate = ['productName', 'dlChannelBandwidth', 'Tx_Rx']
        self.list_index = ['CONCATENATE-1', 'CONCATENATE-4', 'CONCATENATE-5']
        self.list_df = [self.files1, self.files2, self.files2]

        self.df_hardware = pd.DataFrame()

        #...Create Recurssion Grouping consist of column product, BW, and TXRX and merge them in one df_hardware...
        for a, b, c in list(zip(self.list_concatenate, self.list_index, self.list_df)):
            
            self.df_qty = c.groupby([b, a])['Qty_productName'].sum().reset_index()
            self.df_qty['COMBINE - 1'] = "[" + self.df_qty['Qty_productName'].astype(str) + "]" + self.df_qty[a]
            self.df_qty = self.df_qty.groupby([b])['COMBINE - 1'].apply(lambda x: "%s" % '+'.join(x)).reset_index()
            self.df_qty.rename(columns={b: 'CONCATENATE-1'}, inplace = True)
            self.df_hardware = pd.concat([self.df_hardware, self.df_qty], ignore_index=False).astype(str)  
            
        # self.list_call = list(self.df.columns)
        # self.list_call.pop(0)

        #...Merging df and df_hardware based on tagging...
        for a in list(np.unique(self.files2['freqBand'])) :
            for b in ['MIMO', 'BW'] :
                
                self.df = pd.merge(self.df, self.df_hardware[['COMBINE - 1']], left_on = [self.df['TOWER_DNK'] + "_" + a + "_" + b], right_on = self.df_hardware['CONCATENATE-1'], how='left')
                self.df.rename(columns={"COMBINE - 1": "LTE_Radio_" + a + "_" + b}, inplace = True)
                self.df.drop('key_0', inplace=True, axis=1, errors='ignore')

        #...Need Code Refactory for methods below as slow methods and inefficients...
        for a in list(np.unique(self.files1['freqBand-NewName'])) :
            for b in list(np.unique(self.files1['SECTOR_FINAL'])):
                for c in ['MACRO', 'PICO TO MACRO', 'PICO']:
            
                    self.df = pd.merge(self.df, self.df_hardware[['COMBINE - 1']], left_on =  [c + "|" + self.df['TOWER_DNK'] + "_" + a + "_" + b], right_on = self.df_hardware['CONCATENATE-1'], how='left')
                    self.df.rename(columns={"COMBINE - 1": f'{c}|LTE_Radio_{a}_{b}'}, inplace = True)
                    self.df.drop('key_0', inplace=True, axis=1, errors='ignore')

        self.df.rename(columns={"TOWER_DNK":'Special_Tower_ID_mapping_for_Tower_ID_with_more_than_1_site_ID'}, inplace=True)
        self.df.set_index('Special_Tower_ID_mapping_for_Tower_ID_with_more_than_1_site_ID', inplace=True)     
        self.df.columns = self.df.columns.str.split('|', expand=True)

        self.df.rename(columns={np.nan : 'value'}, level=1, inplace=True)


        for a in self.df.columns:
            if len(self.df[a].value_counts()) == 0:
                self.df.drop(columns=[a], inplace=True)
        
        self.df.sort_index(axis=1, level=0, inplace=True)

        # self.df.columns = pd.MultiIndex.from_product([1 * [''], self.df.columns])
        self.df = self.df.transpose().reset_index()
        self.df = self.df.transpose()
        self.df = self.df.add_suffix(self.filepath)
        
        return(self.df, pd.DataFrame(data={'File Name': self.title, 'Date': [datetime.strftime(self.date, '%d-%B-%Y')]}))

    def newmethod1_radiosector(self):

        #...Flagging which radio has duplication which meaning one radio has been used in other sector... 
        self.files1['DUPLICATED'] = self.files1['CONCATENATE-4'].duplicated(keep=False)
        self.files1.reset_index(inplace=True)
        self.files1.drop(columns='index', inplace=True)

        #...The method is getting the index of duplicated and concatenate them through iloc function...
        self.list1 = list(self.files1[self.files1['DUPLICATED'] == True].index)
        self.files1.iloc[self.list1, 7] = self.files1.iloc[self.list1, 7] + "_" + self.files1.iloc[self.list1, 23]

        self.files1['productName'] = self.files1[['productName', 'MIMO']].astype(str).agg('_'.join, axis=1)

        for a in list(self.files2['freqBand-NewName'].unique()):
            for b in ['MIMO', 'BW']:    

                self.df_qty = self.files2.query(f'["{a}"] in `freqBand-NewName`') 
                self.df_qty = self.df_qty.groupby(['TOWER_DNK', b], group_keys=False)['Qty_productName'].sum().reset_index()
                self.df_qty['COMBINE - 1'] = "[" + self.df_qty['Qty_productName'].astype(str) + "]" + self.df_qty[b]

                self.df_qty = self.df_qty.groupby(['TOWER_DNK'], group_keys=False)['COMBINE - 1'].apply(lambda x: "%s" % '+'.join(x)).reset_index()
                self.df_qty.rename(columns={'COMBINE - 1':f'1. VALUE|LTE_RADIO_{a}_{b}'}, inplace=True)
                
                self.df_qty.set_index('TOWER_DNK', inplace=True)
                self.df = self.df.join(self.df_qty, how='left')
                
        for a in list(self.files1['freqBand-NewName'].unique()):
            for b in list(self.files1['SECTOR_FINAL'].unique()):
                for c in list(self.files1['TYPE2'].unique()):

                    self.df_qty = self.files1.query(f'["{b}"] in SECTOR_FINAL and ["{a}"] in `freqBand-NewName` and ["{c}"] in `TYPE2`')
                    self.df_qty = self.df_qty.groupby(['TOWER_DNK','CONCATENATE-1', 'productName'], group_keys=False)['Qty_productName'].sum().reset_index()
                    self.df_qty['COMBINE - 1'] = "[" + self.df_qty['Qty_productName'].astype(str) + "]" + self.df_qty['productName']
                    
                    self.df_qty = self.df_qty.groupby(['TOWER_DNK'], group_keys=False)['COMBINE - 1'].apply(lambda x: "%s" % '+'.join(x)).reset_index()
                    self.df_qty.rename(columns={'COMBINE - 1':f'{c}|LTE_RADIO_{a}_{b}'}, inplace=True)
                    self.df_qty.set_index('TOWER_DNK', inplace=True)
                    self.df = self.df.join(self.df_qty, how='left')

        self.df = self.df.reset_index()
        self.df.rename(columns={"TOWER_DNK":'Special_Tower_ID_mapping_for_Tower_ID_with_more_than_1_site_ID'}, inplace=True)
        self.df.set_index('Special_Tower_ID_mapping_for_Tower_ID_with_more_than_1_site_ID', inplace=True)     
        self.df.columns = self.df.columns.str.split('|', expand=True)
        self.df.rename(columns={np.nan : 'value'}, level=1, inplace=True)

        for a in self.df.columns:
            if len(self.df[a].value_counts()) == 0:
                self.df.drop(columns=[a], inplace=True)
        
        self.df.sort_index(axis=1, level=0, inplace=True)

        # self.df.columns = pd.MultiIndex.from_product([1 * [''], self.df.columns])
        self.df = self.df.transpose().reset_index()
        self.df = self.df.transpose()
        self.df = self.df.add_suffix(self.filepath)
        
        return(self.df, pd.DataFrame(data={'File Name': self.title, 'Date': [datetime.strftime(self.date, '%d-%B-%Y')]}))