# to be run each day: collects problems from task two and aggregates them into daily issue areas.
# this version doesn't work currently -- making a branch for large scale edits

import datetime
import sqlalchemy
import pandas as pd
import sqlite3
import numpy as np
import csv

PATH = 'my_file'

# Reads in databases from tasks 1 and 2
engine = sqlalchemy.create_engine('sqlite:///' + PATH)
cold_data = pd.read_sql_table("ColdDatabase", engine)
warm_data = pd.read_sql_table("WarmDatabase", engine)
carbon_data = pd.read_sql_table("CarbonDatabase", engine)
low_carbon_data = pd.read_sql_table("LowCarbonDatabase", engine)

# Convert times to integers so that they compare accurately
# Not necessary, actually... right?
for x in range(0, len(cold_data['Timestamp'])):
    cold_data['Timestamp'].loc[x] = (pd.to_datetime(cold_data['Timestamp'].loc[x]) - datetime.timedelta(0))
for x in range(0, len(warm_data['Timestamp'])):
    warm_data['Timestamp'].loc[x] = (pd.to_datetime(warm_data['Timestamp'].loc[x]) - datetime.timedelta(0))
for x in range(0, len(carbon_data['Timestamp'])):
    carbon_data['Timestamp'].loc[x] = (pd.to_datetime(carbon_data['Timestamp'].loc[x]) - datetime.timedelta(0))
for x in range(0, len(low_carbon_data['Timestamp'])):
    low_carbon_data['Timestamp'].loc[x] = (pd.to_datetime(low_carbon_data['Timestamp'].loc[x]) - datetime.timedelta(0))


#print(cold_data)
#print(warm_data)
time_cold = cold_data.copy().set_index("Temperature")
time_warm = warm_data.copy().set_index("Temperature")
time_carbon = carbon_data.copy().set_index("CO2")
time_lowcarbon = low_carbon_data.copy().set_index('CO2')

#This version tries to work without the copies, instead streamlining the process on only the main four dataframes.
#cold_data_copy = cold_data.copy()
#warm_data_copy = warm_data.copy()
#carbon_data_copy = carbon_data.copy()
#low_carbon_data_copy = low_carbon_data.copy()

'''
# Sets up timestamp piece -- first/last time too cold/warm
cold_data['First Time Too Cold'] = cold_data['Timestamp']
cold_data['Last Time Too Cold'] = cold_data['Timestamp']
warm_data['First Time Too Warm'] = warm_data['Timestamp']
warm_data['Last Time Too Warm'] = warm_data['Timestamp']
carbon_data['First Time Too Much CO2'] = carbon_data['Timestamp']
carbon_data['Last Time Too Much CO2'] = carbon_data['Timestamp']
low_carbon_data['First Time Too Little CO2'] = low_carbon_data['Timestamp']
low_carbon_data['Last Time Too Little CO2'] = low_carbon_data['Timestamp']

'''

# Time Testing
# print(cold_data_copy['First Time Too Cold'])
# print(np.min(cold_data_copy['First Time Too Cold']))  # prints incorrectly across days - nov 9 is min
# print(np.max(cold_data_copy['First Time Too Cold']))  # prints incorrectly across days - nov 5 is max on same trial
# print(np.min(["Sun Nov 10 17:00:00 2019", "Wed Nov 6 16:00:00 2019"]))
# but this only happens when i put it in the pandas format -- in other formats it was calculating correctly

warm_data['Intervals Too Warm'] = None
cold_data['Intervals Too Cold'] = None
carbon_data['Intervals Too Much CO2'] = None
low_carbon_data['Intervals Too Little CO2'] = None

warm_data['Highest Temperature'] = warm_data['Temperature']
warm_data['Lowest Temperature'] = warm_data['Temperature']
cold_data['Lowest Temperature'] = cold_data['Temperature']
cold_data['Highest Temperature'] = cold_data['Temperature']
carbon_data['Highest CO2'] = carbon_data['CO2']
carbon_data['Lowest CO2'] = carbon_data['CO2']
low_carbon_data['Lowest CO2'] = carbon_data['CO2']
low_carbon_data['Highest CO2'] = carbon_data['CO2']

# CENTRAL GROUPBY STATEMENT

# All grouping & analysis should actually be done HERE


cold_data = cold_data.groupby("Room #").agg({'Lowest Temperature': np.min,
                                             'Highest Temperature': np.max,
                                             'Intervals Too Cold': np.size})
warm_data = warm_data.groupby("Room #").agg({'Highest Temperature': np.max,
                                             "Lowest Temperature": np.min,
                                             'Intervals Too Warm': np.size})
carbon_data = carbon_data.groupby('Room #').agg({'Highest CO2': np.max,
                                                 'Lowest CO2': np.min,
                                                 'Intervals Too Much CO2': np.size})
low_carbon_data = low_carbon_data.groupby('Room #').agg({'Lowest CO2': np.min,
                                                         'Highest CO2': np.max,
                                                         'Intervals Too Little CO2': np.size})


#unless they're done AFTER the merge...

#CENTRAL MERGE STATEMENTS
all_temps = pd.merge(warm_data, cold_data, how='outer', on=['Room #'])
all_temps = all_temps.groupby(level=0, axis=1).max()
all_carbon = pd.merge(carbon_data, low_carbon_data, how='outer', on=['Room #'])
all_carbon = all_carbon.groupby(level=0, axis=1).max()


print(all_temps)
print(all_carbon)

all_data = pd.merge(all_temps, all_carbon, how='outer', on=['Room #'])

# go back into time database (copied from original database) and locate timestamps
all_data['Lowest Temperature Time'] = None
all_data['Highest Temperature Time'] = None
all_data['Highest CO2 Time'] = None
all_data['Lowest CO2 Time'] = None

for room in all_data.index:
    temp_temp = all_data['Lowest Temperature'][room]
    print("Original Value: " + str(temp_temp) + "\n")
    if not (np.isnan(temp_temp)):
        temp_temp = int(temp_temp)
        temp_time = time_cold.loc[temp_temp].groupby("Room #").agg({"Timestamp":np.min})
        (all_data.loc[room])['Timestamp'] = temp_time.loc[room]['Timestamp']
    else:
        (all_data.loc[room])['Lowest Temperature Time'] = np.nan
    temp_temp = all_data['Highest Temperature'][room]
    print("Original Value: " + str(temp_temp) + "\n")
    if not (np.isnan(temp_temp)):
        temp_temp = int(temp_temp)
        temp_time = time_warm.loc[temp_temp].groupby("Room #").agg({"Timestamp":np.min})
        (all_data.loc[room])['Timestamp'] = temp_time.loc[room]['Timestamp']
    else:
        (all_data.loc[room])['Highest Temperature Time'] = np.nan
    temp_co2 = all_data['Highest CO2'][room]
    print("Original Value: " + str(temp_co2) + "\n")
    if not (np.isnan(temp_co2)):
        #temp_co2 = int(temp_co2)
        temp_time = time_carbon.loc[temp_co2].groupby("Room #").agg({"Timestamp":np.min})
        (all_data.loc[room])['Timestamp'] = temp_time.loc[room]['Timestamp']
    else:
        (all_data.loc[room])['Highest CO2 Time'] = np.nan
    temp_co2 = all_data['Lowest CO2'][room]
    print("Original Value: " + str(temp_co2) + "\n")
    if not (np.isnan(temp_co2)):
        #temp_co2 = int(temp_co2)
        print(time_lowcarbon.index)
        temp_time = time_lowcarbon.loc[temp_co2].groupby("Room #").agg({"Timestamp":np.min})
        (all_data.loc[room])['Timestamp'] = temp_time.loc[room]['Timestamp']
    else:
        (all_data.loc[room])['Lowest CO2 Time'] = np.nan

#merge later
#time_temp_vals = pd.merge(cold_with_times, warm_with_times, how='outer', on=['Room #']).fillna("N/A")
#time_carbon_vals = pd.merge(carbon_with_times, low_carbon_with_times, how='outer', on=['Room #']).fillna("N/A")
# all_times = pd.merge(time_temp_vals, time_carbon_vals, how='outer', on=['Room #']).fillna("N/A")

'''
coldest = cold_data.sort_values(by='Temperature', ascending=True).reset_index().iloc[0]
warmest = warm_data.sort_values(by='Temperature', ascending=False).reset_index().iloc[0]
carbonest = carbon_data.sort_values(by='CO2', ascending=False).reset_index().iloc[0]
print(coldest)
print(warmest)
print(carbonest)

nice data to have, but NOT part of the required weekly report.

'''
'''
# ORIGINAL VERSION (used for interval counting):


# COPY VERSION (used for high/low temps):

#cold_data_copy = cold_data_copy.drop('index', axis=1)
#warm_data_copy = warm_data_copy.drop('index', axis=1)

temp_vals_copy = pd.merge(warm_data_copy, cold_data_copy, how='outer', on=['Room #', 'Temperature', 'CO2'])
temp_vals_copy['Highest Temperature'] = temp_vals_copy['Temperature']
temp_vals_copy['Lowest Temperature'] = temp_vals_copy['Temperature']
all_temps = temp_vals_copy.set_index("Room #")['Temperature']

carbon_vals_copy = pd.merge(carbon_data_copy, low_carbon_data_copy, how='outer', on=['Room #', 'Temperature', 'CO2'])
carbon_vals_copy['Highest CO2'] = carbon_vals_copy['CO2']
carbon_vals_copy['Lowest CO2'] = carbon_vals_copy['CO2']
all_carbon = carbon_vals_copy.set_index("Room #")['CO2']

temp_vals_copy = temp_vals_copy.groupby("Room #").agg({'Highest Temperature': np.max, 'Lowest Temperature': np.min})
carbon_vals_copy = carbon_vals_copy.groupby("Room #").agg({'Highest CO2': np.max, "Lowest CO2": np.min})

most_temp_data = pd.merge(temp_vals, temp_vals_copy, on=['Room #']).fillna(0)
# print(most_temp_data)

most_carbon_data = pd.merge(carbon_vals, carbon_vals_copy, on=['Room #']).fillna(0)
print(most_carbon_data)

all_temp_data = pd.merge(most_temp_data, time_temp_vals, on=['Room #']).fillna(0)
# print(all_temp_data)

all_carbon_data = pd.merge(most_carbon_data, time_carbon_vals, on=['Room #']).fillna(0)
print(all_carbon_data)

all_data = pd.merge(all_temp_data, all_carbon_data, how='outer', on=['Room #']).fillna(0)
# print(all_data.T.index)  # range of timestamps is 4-7


def convert_datetime(z):
    if type(z) == str:
        return z
    elif type(z) == pd.Timestamp:
        print(datetime.datetime.strftime(z.to_pydatetime(), '%Y-%m-%d %H:%M:%S'))
        return datetime.datetime.strftime(z.to_pydatetime(), '%Y-%m-%d %H:%M:%S')


for x in range(0, len(all_data['First Time Too Cold'])):
    all_data['First Time Too Cold'].iloc[x] = convert_datetime(all_data['First Time Too Cold'].iloc[x])
    all_data['Last Time Too Cold'].iloc[x] = convert_datetime(all_data['Last Time Too Cold'].iloc[x])
    all_data['First Time Too Warm'].iloc[x] = convert_datetime(all_data['First Time Too Warm'].iloc[x])
    all_data['Last Time Too Warm'].iloc[x] = convert_datetime(all_data['Last Time Too Warm'].iloc[x])
    all_data['First Time Too Much CO2'].iloc[x] = convert_datetime(all_data['First Time Too Much CO2'].iloc[x])
    all_data['Last Time Too Much CO2'].iloc[x] = convert_datetime(all_data['Last Time Too Much CO2'].iloc[x])
    all_data['First Time Too Little CO2'].iloc[x] = convert_datetime(all_data['First Time Too Little CO2'].iloc[x])
    all_data['Last Time Too Little CO2'].iloc[x] = convert_datetime(all_data['Last Time Too Little CO2'].iloc[x])

    print(all_data['First Time Too Cold'].iloc[x])

'''

conn = sqlite3.connect(PATH)
all_data.to_sql("DailyDatabase", conn, if_exists='append')
all_temps.to_sql("DailyTempDatabase", conn, if_exists='append')
all_carbon.to_sql("DailyCarbonDatabase", conn, if_exists='append')

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

# Daily Clear -- commented out for testing
'''


cursor = conn.cursor()                       
                                          
drop = "DROP TABLE ColdDatabase"             
drop2 = "DROP TABLE WarmDatabase"            
drop3 = "DROP TABLE CarbonDatabase"          
drop4 = "DROP TABLE LowCarbonDatabase"       
                                             
cursor.execute(drop)                         
cursor.execute(drop2)                        
cursor.execute(drop3)                        
cursor.execute(drop4)                        
                                             
conn.close()
'''
