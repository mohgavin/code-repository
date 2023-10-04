import pandas as pd
import re
import os
import numpy as np
from datetime import datetime

ABSOLUTE_PATH = os.path.dirname(__file__) + '/Site Collo_Folder'
ABSOLUTE_PATH_1 = os.path.dirname(__file__) + '/BTSWeb_Folder'

class Tower:
    def __init__(self, filepath):

        self.title = filepath
        self.filepath = os.path.join(ABSOLUTE_PATH, filepath)

        self.date = datetime.strptime(re.findall(r"\d{8}|$", filepath)[0], '%Y%m%d').date()        
        self.df = pd.read_excel(self.filepath)
        self.df.set_index('Special_Tower_ID_mapping_for_Tower_ID_with_more_than_1_site_ID', inplace=True)        
        self.df.columns = pd.MultiIndex.from_product([1 * [''], self.df.columns])
        self.df = self.df.transpose().reset_index()
        self.df = self.df.transpose()
        self.df = self.df.add_suffix(filepath)

    def return_result(self):
    
        return(self.df, pd.DataFrame(data={'File Name': self.title, 'Date': [datetime.strftime(self.date, '%d-%B-%Y')]}))

class BTSWeb:
    def __init__(self, filepath):

        self.title = filepath
        self.filepath = os.path.join(ABSOLUTE_PATH_1, filepath)
        self.date = datetime.strptime(re.findall(r"\d{8}|$", self.filepath)[0], '%Y%m%d').date() 
        
        self.files = pd.read_csv(self.filepath, sep = '^')       
        
        self.files['CONCATENATE-1'] = self.files['Tower ID'] + '_' + self.files['Site Layer']
        self.files['Cell Status - Layer'] = self.files['Cell Status'] + '_' + self.files['Site Layer']
        
        self.list1 = ['Active', 'Assets', 'MBC', 'Plan', 'Plan SSR']

        self.files = self.files[self.files['Cell Status'].astype(str).str.contains('|'.join(self.list1))==True]
        self.files.sort_values(by=['Updated Date (Tower)', 'Site Layer'], ascending=True, inplace=True)
        self.files.drop_duplicates(subset=['CONCATENATE-1'], keep='first', inplace=True)
        self.files.insert(len(self.files.columns), 'Tower ID-Site Layer', self.files['Tower ID'] + "_" + self.files['Site Layer'])

    def return_result(self):

        self.df = pd.DataFrame(self.files[['SiteID Colo', 'Tower ID', 'Province', 'Kabupaten', 'Kecamatan', 'Desa', 'Old Tower ID', 'Tower Classification', 'Region']].drop_duplicates(subset=['Tower ID'], keep='first'))

        for a in ['2G', '3G', '4G', '5G']:
            
            self.df1 = self.files.groupby(by = self.files[self.files['BTS Type'] == a] ['Tower ID'])['Site Layer'].apply(lambda x: "%s" % '+'.join(x)).str.strip('++')
            self.df1 = pd.DataFrame(self.df1)
            self.df2 = self.files.groupby(by = self.files[self.files['BTS Type'] == a] ['Tower ID'])['Cell Status - Layer'].apply(lambda x: "%s" % '+'.join(x)).str.strip('++')
            self.df2 = pd.DataFrame(self.df2)

            self.df = pd.merge(self.df, self.df1, on='Tower ID', how='left')
            self.df = pd.merge(self.df, self.df2, on='Tower ID', how='left')

            self.df.rename(columns={'Site Layer' : f'BTSWebLayer_{a}', 'Cell Status - Layer' : f'Cell Status BTSWeb_{a}'}, inplace=True)

        self.df = pd.merge(self.df, self.files[['Tower ID', 'TP', 'TP ID']].drop_duplicates(subset=['Tower ID'], keep='first'), on='Tower ID', how='left')
        self.df = self.df.set_index('Tower ID')
        self.df['BTSWebLayer_Combined'] = self.df[['BTSWebLayer_2G', 'BTSWebLayer_3G', 'BTSWebLayer_4G', 'BTSWebLayer_5G']].apply(lambda x: x.str.cat(sep='_'), axis=1 )
        self.df.index.rename('Special_Tower_ID_mapping_for_Tower_ID_with_more_than_1_site_ID', inplace = True)

        self.df.columns = pd.MultiIndex.from_product([1 * [''], self.df.columns])
        self.df = self.df.transpose().reset_index()
        self.df = self.df.transpose()
        self.df = self.df.add_suffix(self.filepath)
        
        return(self.df, pd.DataFrame(data={'File Name': self.title, 'Date': [datetime.strftime(self.date, '%d-%B-%Y')]}))