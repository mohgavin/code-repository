#!/home/nivag/database_consolidate/.env/bin/python

from hardware import Hardware, Radio_Sector
from connectivity import RET_Status, BB_Port
from tower import Tower, BTSWeb
from datetime import date
from functools import reduce

import time
import pandas as pd

def main():
   
    s = time.perf_counter()

    towerid     = Tower('Tower ID - Site ID Collo_20230301.xlsx').return_result()
    btsw        = BTSWeb('BtsNewReport-20230301.zip').return_result()
    hardware    = Hardware('HW_inventory_All_OSS-ENM_20230320.csv.gz').newmethod_tech_hardware()
    radiosector = Radio_Sector('2G_3G_4G_ESS_NBIOT_20230305_FINAL.xlsb').newmethod1_radiosector()
    ret         = RET_Status('HW_RET_inventory_All_ENM_20230301.csv.gz').return_result()
    bbport      = BB_Port('Port_Utilization_Report_All_ENM_20230301.csv.gz').return_result()

    list_zip_df = list(zip(towerid, btsw, hardware, bbport, ret, radiosector)) 

    df1 = reduce(lambda  left,right: pd.merge(left,right,on='Special_Tower_ID_mapping_for_Tower_ID_with_more_than_1_site_ID', how='outer'), list_zip_df[0])

    df2 = pd.concat(list_zip_df[1], axis=0, ignore_index =True)

    df1.rename(index={'level_1': 'Special_Tower_ID_mapping_for_Tower_ID_with_more_than_1_site_ID'}, inplace=True)

    with pd.ExcelWriter(f'~/database_consolidate/result/result-merge_{date.today().strftime("%Y%m%d")}.xlsx', engine='xlsxwriter') as writer:  
        df1.to_excel(writer, header=False, sheet_name='Consolidate DB')
        df2.to_excel(writer, header=True, sheet_name='Log-Date')

    elapsed = time.perf_counter() - s
    print(f"{__file__} executed in {elapsed:0.2f} seconds.")
    
if __name__ == '__main__':
    
    main()