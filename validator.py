#
# Validator of solutions (sequential vs distributed)
#
# Author: Herpo Nahuel
# Feb. 2023
#

import pandas as pd
from datetime import datetime
import sys

# Take index of directory
directory_index = sys.argv[1]
print("Checking the datos", directory_index,"results...")

# Load the results from csv's files
df_seq_result = pd.read_csv('datos' + directory_index + '/sequential.csv')
df_dtb_result = pd.read_csv('datos' + directory_index + '/distributed.csv')
df_dtb_result['FECHA_NAC'] = df_dtb_result['FECHA_NAC'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").strftime('%m/%d/%Y'))
df_seq_result = df_seq_result.sort_values(by=['ID', 'ID_MADRE', 'ORDEN', 'sisters_count', 'NRO_PROD', 'mother_NRO_PROD'])
df_dtb_result = df_dtb_result.sort_values(by=['ID', 'ID_MADRE', 'ORDEN', 'sisters_count', 'NRO_PROD', 'mother_NRO_PROD'])

if df_dtb_result.equals(df_dtb_result):
    print("OK: The results are the same")
else:
    print("ERROR: The results are not the same")