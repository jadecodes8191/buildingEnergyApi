import pandas as pd
import numpy as np

temp_max = 75
temp_min = 65
co2_max = 1200

warm_data = pd.read_csv("ahs_warm_data.csv", names=['Timestamp', 'Room #', 'Temperature', 'Temp Units', 'CO2', 'CO2 Units'])
cold_data = pd.read_csv("ahs_cold_data.csv", names=['Timestamp', 'Room #', 'Temperature', 'Temp Units', 'CO2', 'CO2 Units'])
carbon_data = pd.read_csv("ahs_carbon_data.csv", names=['Timestamp', 'Room #', 'Temperature', 'Temp Units', 'CO2', 'CO2 Units'])

warm_data['Temp. Difference'] = warm_data['Temperature'] - temp_max
cold_data['Temp. Difference'] = cold_data['Temperature'] - temp_min
carbon_data['CO2 Difference'] = carbon_data['CO2'] - co2_max
# tracks difference from normal temperature or co2 levels

warm_data['Sum'] = warm_data['Temp. Difference']
warm_data['Count'] = None
cold_data['Sum'] = cold_data['Temp. Difference']
cold_data['Count'] = None
carbon_data['Sum'] = carbon_data['CO2 Difference']
carbon_data['Count'] = None

# this is just setting up for the groupby -- current values are not actually going to be used

warm_data = warm_data.groupby("Room #").agg({'Temp. Difference' : np.mean, 'Sum' : np.sum, 'Count' : np.size})
cold_data = cold_data.groupby("Room #").agg({'Temp. Difference' : np.mean, 'Sum' : np.sum, 'Count' : np.size})
carbon_data = carbon_data.groupby("Room #").agg({'CO2 Difference' : np.mean, 'Sum' : np.sum, 'Count' : np.size})

# calculates mean temp difference for each room number

warm_data['Check Column'] = warm_data['Sum']/warm_data['Count']
cold_data['Check Column'] = cold_data['Sum']/cold_data['Count']
carbon_data['Check Column'] = carbon_data['Sum']/carbon_data['Count']
# just checks that the mean is being calculated correctly -- it is as of 10/24


temp_vals = pd.merge(warm_data, cold_data, how='outer', on=['Room #', 'Temp. Difference', 'Sum', 'Count', 'Check Column'])
all_data = pd.merge(temp_vals, carbon_data, how='outer', on=['Room #', 'Sum', 'Count', 'Check Column'])
temp_vs_carbon = all_data.groupby(np.isnan(all_data['Temp. Difference'])).agg({'Count' : np.size})
print(all_data)

#all_data = pd.merge(warm_data, cold_data, how='outer', on='Room #')
#all_data = pd.merge(all_data, carbon_data, how='outer', on='Room #')
#all_data = all_data.fillna(0)

#all_data['Temp. Difference'] = (all_data['Temp. Difference_x'] + all_data['Temp. Difference_y'])/2

#print(all_data['Temp. Difference_x'])
#print(all_data)
#print(warm_data)
#print(cold_data)
#print(carbon_data)
