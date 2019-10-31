#to be run each day: collects problems from task two and aggregates them into daily issue areas.

import sqlalchemy
import pandas as pd
import sqlite3
import numpy as np
import csv

PATH = 'my_file'

engine = sqlalchemy.create_engine('sqlite:///' + PATH)
cold_data = pd.read_sql_table("ColdDatabase", engine)
warm_data = pd.read_sql_table("WarmDatabase", engine)
carbon_data = pd.read_sql_table("CarbonDatabase", engine)

print(cold_data)
print(warm_data)
print(carbon_data)

warm_data['Intervals Too Warm'] = None
cold_data['Intervals Too Cold'] = None
carbon_data['Intervals Too Much CO2'] = None

warm_data = warm_data.groupby("Room #").agg({'Intervals Too Warm' : np.size})
cold_data = cold_data.groupby("Room #").agg({'Intervals Too Cold' : np.size})
carbon_data = carbon_data.groupby("Room #").agg({'Intervals Too Much CO2' : np.size})

temp_vals = pd.merge(warm_data, cold_data, how='outer', on=['Room #'])

all_data = pd.merge(temp_vals, carbon_data, how='outer', on=['Room #']).fillna(0)

'''
with open('basic_weekly.csv', 'w') as weekly_df:
    csv_writer = csv.writer(weekly_df, delimiter=";")
    for index, row in all_data.iterrows():
        my_index = index
        temp_warm = row['Intervals Too Warm']
        temp_cold = row['Intervals Too Cold']
        temp_carbon = row['Intervals Too Much CO2']
        csv_writer.writerow(["{0}, {1}, {2}, {3}".format(my_index, temp_warm, temp_cold, temp_carbon)])

'''
