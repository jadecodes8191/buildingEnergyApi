import pandas as pd
import numpy as np
import csv

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

'''
warm_data['Sum'] = warm_data['Temp. Difference']
warm_data['Count'] = None
cold_data['Sum'] = cold_data['Temp. Difference']
cold_data['Count'] = None
carbon_data['Sum'] = carbon_data['CO2 Difference']
carbon_data['Count'] = None

The above was just testing for the mean.
'''
# this is just setting up for the groupby testing -- current values are not actually going to be used

'''
warm_data = warm_data.groupby("Room #").agg({'Temp. Difference' : np.mean, 'Sum' : np.sum, 'Count' : np.size})
cold_data = cold_data.groupby("Room #").agg({'Temp. Difference' : np.mean, 'Sum' : np.sum, 'Count' : np.size})
carbon_data = carbon_data.groupby("Room #").agg({'CO2 Difference' : np.mean, 'Sum' : np.sum, 'Count' : np.size})
'''

warm_data = warm_data.groupby("Room #").agg({'Temp. Difference' : np.mean})
cold_data = cold_data.groupby("Room #").agg({'Temp. Difference' : np.mean})
carbon_data = carbon_data.groupby("Room #").agg({'CO2 Difference' : np.mean})

# calculates mean temp difference for each room number

'''
warm_data['Check Column'] = warm_data['Sum']/warm_data['Count']
cold_data['Check Column'] = cold_data['Sum']/cold_data['Count']
carbon_data['Check Column'] = carbon_data['Sum']/carbon_data['Count']
'''

# The above was more testing -- just checks that the mean is being calculated correctly. It is as of 10/24


def diff_type(x):
    if np.isnan(x):
        return "CO2"
    return "Temperature"


def second_diff_type(x):
    if np.isnan(x):
        return "Temperature"
    return "CO2"


temp_vals = pd.merge(warm_data, cold_data, how='outer', on=['Room #', 'Temp. Difference'])
all_data = pd.merge(temp_vals, carbon_data, how='left', on=['Room #'])
#inner_data = pd.merge(temp_vals, carbon_data, how='inner', on=['Room #'])#the problem is that it's not merging successfully!

with open ('weekly.csv', 'w') as merge_tester:
    csv_writer = csv.writer(merge_tester)
    for index, row in all_data.iterrows():
        csv_writer.writerow("{0}, {1}".format(index, row))

print(all_data)

# The above successfully writes a "weekly report" file, with a couple caveats:
#   1. Not sure what it does in the event that a room registers as having temperatures both above and below the normal range.
#       Ideally, it would calculate the mean including the negative values, but...
#   2. The averages are only on the days which were logged as being outside of the normal, not on all 7 days.
