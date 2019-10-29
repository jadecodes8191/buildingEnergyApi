import pandas as pd
import numpy as np
import csv

warm_data = pd.read_csv("ahs_warm_data.csv", names=['Timestamp', 'Room #', 'Temperature', 'Temp Units', 'CO2', 'CO2 Units'])
cold_data = pd.read_csv("ahs_cold_data.csv", names=['Timestamp', 'Room #', 'Temperature', 'Temp Units', 'CO2', 'CO2 Units'])
carbon_data = pd.read_csv("ahs_carbon_data.csv", names=['Timestamp', 'Room #', 'Temperature', 'Temp Units', 'CO2', 'CO2 Units'])

'''

my_test_room = pd.Series(["Sun Oct 20 12:00:00 1985", "000", -15, "deg F", np.NaN, "ppm"], index=['Timestamp', 'Room #', 'Temperature', 'Temp Units', 'CO2', 'CO2 Units'])
my_test_room2 = pd.Series(["Sun Oct 20 14:00:00 1985", "000", 250, "deg F", np.NaN, "ppm"], index=['Timestamp', 'Room #', 'Temperature', 'Temp Units', 'CO2', 'CO2 Units'])
warm_data = warm_data.append(my_test_room, ignore_index=True)
cold_data = cold_data.append(my_test_room2, ignore_index=True)

Testing for a hypothetical room that is both too warm and too cold at different times during the week.
It merges successfully!

'''

warm_data['Intervals Too Warm'] = None
cold_data['Intervals Too Cold'] = None
carbon_data['Intervals Too Much CO2'] = None

warm_data = warm_data.groupby("Room #").agg({'Intervals Too Warm' : np.size})
cold_data = cold_data.groupby("Room #").agg({'Intervals Too Cold' : np.size})
carbon_data = carbon_data.groupby("Room #").agg({'Intervals Too Much CO2' : np.size})

temp_vals = pd.merge(warm_data, cold_data, how='outer', on=['Room #'])

all_data = pd.merge(temp_vals, carbon_data, how='outer', on=['Room #']).fillna(0)

print(all_data)

with open('basic_weekly.csv', 'w') as weekly_df:
    csv_writer = csv.writer(weekly_df, delimiter=";")
    for index, row in all_data.iterrows():
        my_index = index
        temp_warm = row['Intervals Too Warm']
        temp_cold = row['Intervals Too Cold']
        temp_carbon = row['Intervals Too Much CO2']
        csv_writer.writerow(["{0}, {1}, {2}, {3}".format(my_index, temp_warm, temp_cold, temp_carbon)])

'''
with open('basic_weekly.csv', 'r') as weekly_reader:
    csv_reader = csv.reader(weekly_reader)
    for index, row in csv_reader:
        print("{0}, {1}".format(index, row))
'''
