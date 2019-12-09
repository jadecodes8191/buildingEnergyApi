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
temp_data = pd.read_sql_table("TemperatureProblemsDatabase", engine)
co2_data = pd.read_sql_table("CarbonDioxideProblemsDatabase", engine)

# Convert times to integers so that they compare accurately
# Not necessary, actually... right?
for x in range(0, len(temp_data['Timestamp'])):
    temp_data['Timestamp'].loc[x] = (pd.to_datetime(temp_data['Timestamp'].loc[x]) - datetime.timedelta(0))
for x in range(0, len(co2_data['Timestamp'])):
    co2_data['Timestamp'].loc[x] = (pd.to_datetime(co2_data['Timestamp'].loc[x]) - datetime.timedelta(0))

time_temp = temp_data.copy().set_index(["Room #", "Temperature"])
time_co2 = co2_data.copy().set_index(["Room #", "CO2"])
print("Time CO2")
print((time_co2.loc['000'].loc[-50])['Timestamp'])
print("Time Temp")
print(time_temp)
# Multi-index should identify uniquely

intervals_temp = temp_data.copy()
intervals_temp['Intervals Too Warm'] = None
intervals_temp['Intervals Too Cold'] = None
intervals_temp = intervals_temp.groupby("High Temp?").agg({"Intervals Too Warm": np.size, "Intervals Too Cold": np.size})
print(intervals_temp)

intervals_co2 = co2_data.copy()
intervals_co2['Intervals Too Much CO2'] = None
intervals_co2['Intervals Too Little CO2'] = None
intervals_co2 = intervals_co2.groupby("High Carbon?").agg({"Intervals Too Much CO2": np.size, "Intervals Too Little CO2": np.size})
print(intervals_co2)

temp_data['Intervals Too Warm'] = None
temp_data['Intervals Too Cold'] = None
co2_data['Intervals Too Much CO2'] = None
co2_data['Intervals Too Little CO2'] = None

temp_data['Highest Temperature'] = temp_data['Temperature']
temp_data['Lowest Temperature'] = temp_data['Temperature']
co2_data['Highest CO2'] = co2_data['CO2']
co2_data['Lowest CO2'] = co2_data['CO2']

# CENTRAL GROUPBY STATEMENT

# All grouping & analysis should actually be done HERE

temp_data = temp_data.groupby("Room #").agg({'Lowest Temperature': np.min,
                                             'Highest Temperature': np.max})

co2_data = co2_data.groupby('Room #').agg({'Highest CO2': np.max,
                                           'Lowest CO2': np.min})

all_data = pd.merge(temp_data, co2_data, how='outer', on=['Room #'])

# go back into time database (copied from original database) and locate timestamps
all_data['Lowest Temperature Time'] = None
all_data['Highest Temperature Time'] = None
all_data['Highest CO2 Time'] = None
all_data['Lowest CO2 Time'] = None

for room in time_temp.index:
    low_temps = time_temp.loc[room[0]].loc[all_data['Lowest Temperature'][room[0]]]['Timestamp']
    high_temps = time_temp.loc[room[0]].loc[all_data['Highest Temperature'][room[0]]]['Timestamp']
    if type(low_temps) == pd.Series:
        all_data['Lowest Temperature Time'][room[0]] = low_temps.iloc[0]
    else:
        all_data['Lowest Temperature Time'][room[0]] = low_temps
    if type(high_temps) == pd.Series:
        all_data['Highest Temperature Time'][room[0]] = high_temps.iloc[0]
    else:
        all_data['Highest Temperature Time'][room[0]] = high_temps

for room in time_co2.index:
    low_co2 = time_co2.loc[room[0]].loc[all_data['Lowest CO2'][room[0]]]['Timestamp']
    high_co2 = time_co2.loc[room[0]].loc[all_data['Highest CO2'][room[0]]]['Timestamp']
    if type(low_co2) == pd.Series:
        all_data['Lowest CO2 Time'][room[0]] = low_co2.iloc[0]
    else:
        all_data['Lowest CO2 Time'][room[0]] = low_co2
    if type(high_co2) == pd.Series:
        all_data['Highest CO2 Time'][room[0]] = high_co2.iloc[0]
    else:
        all_data['Highest CO2 Time'][room[0]] = high_co2


print(all_data['Lowest Temperature Time'])
print(all_data['Highest Temperature Time'])
print(all_data['Lowest CO2 Time'])
print(all_data['Highest CO2 Time'])

'''
for room in all_data.index:
    temp_temp = all_data['Lowest Temperature'][room]
    print("Original Value: " + str(temp_temp) + "\n")
    if not (np.isnan(temp_temp)):
        temp_temp = int(temp_temp)
        temp_time = time_temp.loc[temp_temp].groupby("Room #").agg({"Timestamp":np.min})
        (all_data.loc[room])['Timestamp'] = temp_time.loc[room]['Timestamp']
    else:
        (all_data.loc[room])['Lowest Temperature Time'] = np.nan
    temp_temp = all_data['Highest Temperature'][room]
    print("Original Value: " + str(temp_temp) + "\n")
    if not (np.isnan(temp_temp)):
        temp_temp = int(temp_temp)
        temp_time = time_temp.loc[temp_temp].groupby("Room #").agg({"Timestamp":np.min})
        (all_data.loc[room])['Timestamp'] = temp_time.loc[room]['Timestamp']
    else:
        (all_data.loc[room])['Highest Temperature Time'] = np.nan
    temp_co2 = all_data['Highest CO2'][room]
    print("Original Value: " + str(temp_co2) + "\n")
    if not (np.isnan(temp_co2)):
        #temp_co2 = int(temp_co2)
        temp_time = time_co2.loc[temp_co2].groupby("Room #").agg({"Timestamp": np.min})
        print("Before: ")
        print((all_data.loc[room])['Timestamp'])
        (all_data.loc[room])['Timestamp'] = temp_time.loc[room]['Timestamp']
        print("After:")
        print((all_data.loc[room])['Timestamp'])
    else:
        (all_data.loc[room])['Highest CO2 Time'] = np.nan
    temp_co2 = all_data['Lowest CO2'][room]
    print("Original Value: " + str(temp_co2) + "\n")
    if not (np.isnan(temp_co2)):
        #temp_co2 = int(temp_co2)
        print(time_co2.index)
        temp_time = time_co2.loc[temp_co2].groupby("Room #").agg(dict(Timestamp=np.min))
        (all_data.loc[room])['Timestamp'] = temp_time.loc[room]['Timestamp']
    else:
        (all_data.loc[room])['Lowest CO2 Time'] = np.nan
'''


'''
After all functionality added -- remember to add in convert datetime function

conn = sqlite3.connect(PATH)
all_data.to_sql("DailyDatabase", conn, if_exists='append')
temp_data.to_sql("DailyTempDatabase", conn, if_exists='append')
co2_data.to_sql("DailyCarbonDatabase", conn, if_exists='append')
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
