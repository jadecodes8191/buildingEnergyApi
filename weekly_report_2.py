import pandas as pd
import numpy as np
import csv

temp_max = 75
temp_min = 65
co2_max = 1200

warm_data = pd.read_csv("ahs_warm_data.csv", names=['Timestamp', 'Room #', 'Temperature', 'Temp Units', 'CO2', 'CO2 Units'])
cold_data = pd.read_csv("ahs_cold_data.csv", names=['Timestamp', 'Room #', 'Temperature', 'Temp Units', 'CO2', 'CO2 Units'])
carbon_data = pd.read_csv("ahs_carbon_data.csv", names=['Timestamp', 'Room #', 'Temperature', 'Temp Units', 'CO2', 'CO2 Units'])
test= warm_data.reset_index()

# THE BELOW DATA (my_test_room and my_test_room_2) IS NOT REAL DATA! There is no room 000 at Andover High School.
# This is just a test for the case in which a room has temperatures both below and above the norm within the same week.
my_test_room = pd.Series(["Sun Oct 20 12:00:00 1985", "000", -15, "deg F", np.NaN, "ppm"], index=['Timestamp', 'Room #', 'Temperature', 'Temp Units', 'CO2', 'CO2 Units'])
my_test_room2 = pd.Series(["Sun Oct 20 14:00:00 1985", "000", 250, "deg F", np.NaN, "ppm"], index=['Timestamp', 'Room #', 'Temperature', 'Temp Units', 'CO2', 'CO2 Units'])
warm_data = warm_data.append(my_test_room, ignore_index=True)
cold_data = cold_data.append(my_test_room2, ignore_index=True)

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

'''
def diff_type(x):
    if np.isnan(x):
        return "CO2"
    return "Temperature"


def second_diff_type(x):
    if np.isnan(x):
        return "Temperature"
    return "CO2"
'''

temp_vals = pd.merge(warm_data, cold_data, how='outer', on=['Room #'])#removed "temp difference" from the "on" param -- this gives us a _x and _y... but maybe I can deal with that.
#temp_vals['Temp. Difference'] = (temp_vals['Temp. Difference_y'] + temp_vals['Temp. Difference_x'])/2 #the mean of the _x and _Y columns is ideal... but adding NaN to something could cause more issues.


temp_vals['Temp. Difference'] = None
temp_vals = temp_vals.reset_index()
temp_vals = temp_vals.T
print(temp_vals)

for i in range(1, 50):
    print(" ")

for room in temp_vals:
    print(room)
    if (not np.isnan(temp_vals[room]['Temp. Difference_x'])) and (not np.isnan(temp_vals[room]['Temp. Difference_y'])):
        print(temp_vals.iloc[room])
        temp_vals.loc[:, (room,'Temp. Difference')] = (temp_vals[room]['Temp. Difference_y'] + temp_vals[room]['Temp. Difference_x'])/2
        #print((temp_vals.loc[room])['Temp. Difference_y'])
    elif not np.isnan(temp_vals[room]['Temp. Difference_y']):
        temp_vals[room]['Temp. Difference'] = temp_vals[room]['Temp. Difference_y']
    else:
        temp_vals[room]['Temp. Difference'] = temp_vals[room]['Temp. Difference_x']


temp_vals = temp_vals.T
print(temp_vals)

temp_vals['Temp. Difference'] = 0 #the mean of the _x and _Y columns is ideal... but adding NaN to something could cause more issues.
all_data = pd.merge(temp_vals, carbon_data, how='left', on=['Room #'])
#inner_data = pd.merge(temp_vals, carbon_data, how='inner', on=['Room #'])#the problem is that it's not merging successfully!

with open ('weekly.csv', 'w') as merge_tester:
    csv_writer = csv.writer(merge_tester)
    for index, row in all_data.iterrows():
        csv_writer.writerow("{0}, {1}".format(index, row))

print(all_data)

# The above successfully writes a "weekly report" file, with a couple caveats:
#   1. In the event of a room (demonstrated by test room 000) that registers as being too cold and too warm at different points in the week:
#       The room registers as TWO different rooms.
#   2. The averages are only on the days which were logged as being outside of the normal, not on all 7 days.
