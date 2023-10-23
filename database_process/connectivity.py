import pandas as pd
import numpy as np
import os
import re
from datetime import datetime

ABSOLUTE_PATH = os.path.dirname(__file__) + '/source_data/'

class RET_Status:

    def __init__(self, filepath):

        self.title = filepath
        self.filepath = os.path.join(ABSOLUTE_PATH, filepath)
        self.date = datetime.strptime(re.findall(r"\d{8}|$", filepath)[0], '%Y%m%d').date()
        self.files_tower = pd.read_excel(os.path.join(ABSOLUTE_PATH, 'Mapping Layer to Tower_20230301.xlsx'))
        self.files = pd.read_csv(self.filepath, low_memory=False)
        
        self.files_tower.rename(columns={'Node ID':'NODE NAME'}, inplace = True)
        self.files_tower.drop_duplicates(subset=['NODE NAME'], keep='first', inplace=True)
        self.files = pd.merge(self.files, self.files_tower[['NODE NAME', 'Special Tower ID mapping for Tower ID with more than 1 site ID']], on='NODE NAME', how='left')

        self.files.drop_duplicates(subset=['iuantAntennaSerialNumber'], keep='first', inplace=True)
        self.files.insert(len(self.files.columns), 'Qty', 1)
        self.files['iuantAntennaModelNumber'] = self.files['iuantAntennaModelNumber'].replace(np.nan, 0).astype(str)

        self.regex1 = ['_Y*', '-Y*']
        self.files['iuantAntennaModelNumber'].replace(self.regex1, '', inplace=True, regex=True)

    def return_result(self):

        self.df = self.files.groupby(['Special Tower ID mapping for Tower ID with more than 1 site ID', 'iuantAntennaModelNumber'])['Qty'].sum().reset_index()
        self.df['COMBINE-1'] = "[" + self.df['Qty'].astype(str) + "]" + self.df['iuantAntennaModelNumber']

        self.df = self.df.groupby('Special Tower ID mapping for Tower ID with more than 1 site ID')['COMBINE-1'].apply(lambda x: "%s" % '+'.join(x)).reset_index()
        self.df.rename(columns={self.df.columns[0]:'Special_Tower_ID_mapping_for_Tower_ID_with_more_than_1_site_ID', self.df.columns[1]:'RET_Antenna'}, inplace = True)
        self.df.set_index('Special_Tower_ID_mapping_for_Tower_ID_with_more_than_1_site_ID', inplace=True)
        
        self.df.columns = pd.MultiIndex.from_product([1 * [''], self.df.columns])
        self.df = self.df.transpose().reset_index()
        self.df = self.df.transpose()
        self.df = self.df.add_suffix(self.filepath)
        
        return(self.df, pd.DataFrame(data={'File Name': self.title,  'Date': [datetime.strftime(self.date, '%d-%B-%Y')]}))

class BB_Port:

    def __init__(self, filepath):

        self.title = filepath
        self.filepath = os.path.join(ABSOLUTE_PATH, filepath)        
        self.date = datetime.strptime(re.findall(r"\d{8}|$", filepath)[0], '%Y%m%d').date()  
        self.files_tower = pd.read_excel(os.path.join(ABSOLUTE_PATH, 'Mapping Layer to Tower_20230301.xlsx'))
        self.files = pd.read_csv(self.filepath)

        self.files_tower.rename(columns={'Node ID':'NODE NAME'}, inplace = True)
        self.files_tower.drop_duplicates(subset=['NODE NAME'], keep='first', inplace=True)
        self.files = pd.merge(self.files, self.files_tower[['NODE NAME', 'Special Tower ID mapping for Tower ID with more than 1 site ID']], on='NODE NAME', how='left')
        
        self.files['Board2'] = self.files[self.files['Remark']== 'OK']['Board2'].replace('NO BOARD TYPE', '')
        self.files['Layer in board 2'] = self.files[self.files['Remark']== 'OK']['Layer in board 2'].replace('NO DATA', '')
        self.files['Util in board 2'] = self.files[self.files['Remark']== 'OK']['Util in board 2'].replace('NO DATA', '')
        self.files.insert(0, column='Special Tower ID mapping for Tower ID with more than 1 site ID', value=self.files.pop('Special Tower ID mapping for Tower ID with more than 1 site ID'))

    def return_result(self):

        # Combine Board 1 and Board 2 Column into 1 Column together to ease all baseband in one unique tower ID
        self.board1 = self.files.loc[:, list(self.files.columns[0:9])].rename(columns={x:y for x,y in zip(self.files.columns, range(0, 9))})
        self.board2 = self.files.loc[:, list(self.files.columns[0:5]) + list(self.files.columns[9:])]
        self.board2 = self.board2.rename(columns={x:y for x,y in zip(self.board2.columns, range(0, 9))})

        self.df = pd.concat([self.board1, self.board2], ignore_index=False).astype(str)
        self.df.reset_index(inplace=True)
        self.df.rename(columns={x:y for x,y in zip(range(0, 9), self.files.columns)}, inplace=True)

        self.df['Count Port Util in board 1'].replace('0', '', inplace=True)
        self.df['Tower_Layer'] = (self.df['Board1'] + "," + self.df['Layer in board 1']).str.strip(', ')
        self.df['Tower_Port_Util'] = self.df.loc[:, list(self.df.columns[6:7]) + list(self.df.columns[8:10])].fillna('').agg(','.join, axis=1).str.strip(', ')

        # Grouping of towerlayer and Towerportutil
        self.df_towerlayer = self.df.groupby('Special Tower ID mapping for Tower ID with more than 1 site ID')['Tower_Layer'].apply(lambda x: "%s" % '+'.join(x)).str.strip('++')
        self.df_towerportutil = self.df.groupby('Special Tower ID mapping for Tower ID with more than 1 site ID')['Tower_Port_Util'].apply(lambda x: "%s" % '+'.join(x)).str.strip('++')
        self.df = pd.concat([self.df_towerlayer, self.df_towerportutil], axis=1)

        self.df.index.rename('Special_Tower_ID_mapping_for_Tower_ID_with_more_than_1_site_ID', inplace = True)
        
        self.df.columns = pd.MultiIndex.from_product([1 * [''], self.df.columns])
        self.df = self.df.transpose().reset_index()
        self.df = self.df.transpose()
        self.df = self.df.add_suffix(self.filepath)
        
        return(self.df, pd.DataFrame(data={'File Name': self.title,  'Date': [datetime.strftime(self.date, '%d-%B-%Y')]}))