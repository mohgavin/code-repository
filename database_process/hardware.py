import pandas as pd
import os
import numpy as np
import re

ABSOLUTE_PATH = os.path.dirname(__file__)

class Radio_Sector:

    def __init__(self, filepath):

        self.column_list = [0,2,12,17,19,41,42,46,51,52,54,56,85,89,90,95]
        print('Loading...')
        self.title = filepath
        self.filepath = os.path.join(ABSOLUTE_PATH, filepath)

        # self.files   = pd.read_csv(self.filepath, usecols=self.column_list, low_memory=False)
        self.files = pd.read_excel(self.filepath, sheet_name='4G', usecols=self.column_list, dtype=str)

        self.files = self.files[self.files['dlChannelBandwidth'].astype(str).str.contains('dlChannelBandwidth')==False].copy(deep=True)
        # self.files = self.files[self.files['REMARK'].astype(str).str.contains('FALSE')==True].copy(deep=True) #If using CSV, Boolean dont change
        self.files = self.files[self.files['REMARK'].astype(str).str.contains('False')==True] #If using XLSX, string is changed.
        self.files = self.files[self.files['operationalState'].astype(str).str.contains('ENABLED')==True]
        self.files = self.files[self.files['faultIndicator'].astype(str).str.contains('OFF')==True]

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

    def newmethod1_radiosector(self):

        #...Flagging which radio has duplication which meaning one radio has been used in other sector... 
        self.files1['DUPLICATED'] = self.files1['CONCATENATE-4'].duplicated(keep=False)
        self.files1.reset_index(inplace=True)
        self.files1.drop(columns='index', inplace=True)

        #...The method is getting the index of duplicated and concatenate them through iloc function...
        self.list1 = list(self.files1[self.files1['DUPLICATED'] == True].index)
        # self.files1.iloc[self.list1, 7] = self.files1.iloc[self.list1, 7] + "_" + self.files1.iloc[self.list1, 23]
        self.files1.iloc[self.list1, 8] = self.files1.iloc[self.list1, 9] + "_" + self.files1.iloc[self.list1, 25]

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
        # self.df = self.df.add_suffix(self.filepath)
        
        self.title = os.path.split(self.title)[1]
        self.title = self.title.split('.')[0]

        self.files = self.files[['TOWER_DNK', 'FDN']].drop_duplicates(subset='FDN', ignore_index=True)
        self.files['FDN'] = self.files['FDN'].str.replace(('(') , '').str.replace((')') , '')

        self.files['FDN'] = self.files['FDN'].str.split(pat='MeContext=', expand=True)[1]
        self.files['FDN'] = self.files['FDN'].str.split(pat=',', expand=True)[0]

        print('Saving...')

        with pd.ExcelWriter(f'{self.title}-RadioSector.xlsx', engine='xlsxwriter') as writer: 
            self.df.to_excel(writer, header=True, sheet_name='Radio Per Sector')
            self.files.to_excel(writer, header=True, sheet_name='Mapping-TowerID', index=False)