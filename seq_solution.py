#
# Sequential solution to the query using Pandas
#
# Author: Herpo Nahuel
#
# Date: Feb, 2023
#

import pandas as pd
import numpy as np
from datetime import datetime
import sys
import os

# Take the number of trees from the arguments
directory_index = sys.argv[1]
number_of_trees = int(sys.argv[2])
print("Execution number:", directory_index)
print("Trees number:", number_of_trees)

# Load dataframes individuos and producciones
df_list_ind = []
df_list_prod = []
for i in range(number_of_trees):
    df_list_ind.append(
        pd.read_csv('datos' + directory_index + '/individuos/individuos' + str(i+1) + '.csv').replace(np.nan, -1))
    df_list_prod.append(
        pd.read_csv('datos' + directory_index + '/producciones/produccion' + str(i+1) + '.csv').filter(items=['ID_VAC', 'NRO_PROD']))
df_ind = pd.concat(df_list_ind, ignore_index=True)
df_prod = pd.concat(df_list_prod, ignore_index=True)

df_ind['ID_MADRE'] = df_ind['ID_MADRE'].astype('int64')


# Set the first timestamp
t0 = datetime.timestamp(datetime.now())

""" Query: Cows that have produced more than 40L (liters)
of milk, their mother produced 40L of milk and they have
at least 2 sisters. """

#print("Query: Cows that have produced more than 40L (liters) \
#of milk, their mother produced 40L of milk and they have \
#at least 2 sisters.")


#1. Filter only cows that have more than one sister
# >>> tc1 = tc.filtrar(lambda ind: ind.__sistersCount__() > 1)
print("\n1. First query step: Filter cows with at least 2 sisters")
child_count = dict() #This dict contains for each mother its childen count
temp_df = df_ind.copy(deep=True) #Make a copy of the original dataframe
temp_df['children_count'] = 1 #Add column with ones
# Filter temp dataframe with only two columns, then make a reduce by key
#to indicate the occurrences of ID_MADRE (to know children count)
temp_df = temp_df.filter(items=['ID_MADRE', 'children_count']) \
    .groupby(['ID_MADRE']).sum()
df_ind = df_ind.merge(right=temp_df, left_on='ID_MADRE', right_on='ID_MADRE')
df_ind['children_count'] = df_ind['children_count'] - 1
df_ind = df_ind.rename(columns={'children_count':'sisters_count'})
df_ind = df_ind[df_ind['sisters_count'] > 1]


#2. Cows that have more than 40L of production in any record
# >>> tc2 = tc1.filtrar(lambda ind: ind["NRO_PROD"].value() > 40)
print("\n2. Second query step: Filter cows that have more than 40L of production in some record")
#Make a copy and filter per production amount
temp_df = df_prod.copy(deep=True)
temp_df = temp_df[temp_df['NRO_PROD'] > 40] #Take records with more than 40L

#Inner join between ind and prod dataframes...
#I used merge instead of join, cause join only works with indexes
df_ind = df_ind.merge(right=temp_df, how='inner', left_on='ID', right_on='ID_VAC') \
    .drop(columns=['ID_VAC'])
#print(df_ind.to_string())


#3. Cows that have a mother with more than 40L in some record
# >>> tc3 = tc2.filtrar(lambda ind: ind.mother["NRO_PROD"].value() > 40)
print("\n3. Finally query step: Filter cows that have a mother with more than 40L in some record")
temp_df = temp_df.rename(columns={'ID_VAC':'ID_MADRE', 'NRO_PROD':'mother_NRO_PROD'}) #pre-condition: if I have production, I'm a mother
#print('temp_df: ',temp_df.to_string())
#Inner join between the same dataframes, with keys: ID_MADRE - ID
df_ind = df_ind.merge(right=temp_df, left_on='ID_MADRE', right_on='ID_MADRE')
del temp_df

#Calculate and print the time
time = datetime.timestamp(datetime.now()) - t0
print(time, " sec")
os.system("echo " + str(time) + " >> datos" + directory_index + "/seq_times.txt")
#print(df_ind.to_string())

df_ind.to_csv('datos' + directory_index + '/sequential.csv', index=False)