# to be run each day: collects problems from task two and aggregates them into daily issue areas.
# this version doesn't work currently -- making a branch for large scale edits

import datetime
import sqlalchemy
import pandas as pd
import sqlite3
import numpy as np

PATH = 'my_file'

# Reads in databases from tasks 1 and 2
engine = sqlalchemy.create_engine('sqlite:///' + PATH)
temp_data = pd.read_sql_table("TemperatureProblemsDatabase", engine)
temp_data = temp_data.sort_values("Room #")
temp_data.to_csv('tester.csv')
co2_data = pd.read_sql_table("CarbonDioxideProblemsDatabase", engine)

# Convert times to integers so that they compare accurately

for x in range(0, len(temp_data['Timestamp'])):
    temp_data['Timestamp'].loc[x] = (pd.to_datetime(temp_data['Timestamp'].loc[x]) - datetime.timedelta(0))
for x in range(0, len(co2_data['Timestamp'])):
    co2_data['Timestamp'].loc[x] = (pd.to_datetime(co2_data['Timestamp'].loc[x]) - datetime.timedelta(0))

time_temp = temp_data.copy().set_index(["Room #", "Temperature"])
time_co2 = co2_data.copy().set_index(["Room #", "CO2"])
# Multi-index should identify a room and temp or co2 value uniquely for when we look for the times of h/l values

td_copy = temp_data.set_index("Room #").T
cd_copy = co2_data.set_index("Room #").T

temp_data['Highest Temperature'] = temp_data['Temperature']
temp_data['Lowest Temperature'] = temp_data['Temperature']
co2_data['Highest CO2'] = co2_data['CO2']
co2_data['Lowest CO2'] = co2_data['CO2']

# Groups low/high #s

temp_data = temp_data.groupby("Room #").agg({'Lowest Temperature': np.min,
                                             'Highest Temperature': np.max})

co2_data = co2_data.groupby('Room #').agg({'Highest CO2': np.max,
                                           'Lowest CO2': np.min})

all_data = pd.merge(temp_data, co2_data, how='outer', on=['Room #'])

# Finds number of intervals with a given problem for each room

all_data['Intervals Too Cold'] = None
all_data['Intervals Too Warm'] = None
all_data['Intervals Too Much CO2'] = None
all_data['Intervals Too Little CO2'] = None

for room in td_copy:
    print("ROOM: ")
    print(room)
    intervals_temp = td_copy[room].T
    intervals_temp['Intervals'] = None
    intervals_temp = intervals_temp.groupby("High Temp?").agg({"Intervals": np.size})
    print("Temp Intervals: ")
    print(intervals_temp)
    if len(intervals_temp) == 1:
        if intervals_temp.index[0] == 0:
            all_data['Intervals Too Cold'][room] = (intervals_temp.iloc[0])[0]
        else:
            all_data['Intervals Too Warm'][room] = (intervals_temp.iloc[0])[0]
    elif len(intervals_temp) == 2:
        all_data['Intervals Too Cold'][room] = (intervals_temp.iloc[0])[0]
        all_data['Intervals Too Warm'][room] = (intervals_temp.iloc[1])[0]

for room in cd_copy:
    print("ROOM: ")
    print(room)
    intervals_co2 = cd_copy[room].T
    intervals_co2['Intervals'] = None
    intervals_co2 = intervals_co2.groupby("High Carbon?").agg({"Intervals": np.size})
    print("CO2 Intervals: ")
    print(intervals_co2)
    if len(intervals_co2) == 1:
        if intervals_co2.index[0] == 0:
            all_data['Intervals Too Little CO2'][room] = (intervals_co2.iloc[0])[0]
        else:
            all_data['Intervals Too Much CO2'][room] = (intervals_co2.iloc[0])[0]
    elif len(intervals_co2) == 2:
        all_data['Intervals Too Little CO2'][room] = (intervals_co2.iloc[0])[0]
        all_data['Intervals Too Much CO2'][room] = (intervals_co2.iloc[1])[0]

# go back into time database (copied from original database) and locate timestamps

all_data['First Time Too Cold'] = None
all_data['First Time Too Warm'] = None
all_data['Last Time Too Cold'] = None
all_data['Last Time Too Warm'] = None

for room in time_temp.index:
    room_number = room[0]
    temp_df = time_temp.loc[room_number]
    temp_df['First Time'] = temp_df['Timestamp']
    temp_df['Last Time'] = temp_df['Timestamp']
    temp_df = temp_df.groupby("High Temp?").agg({"First Time": np.min, "Last Time": np.max})
    early_times = temp_df['First Time']
    if len(early_times) == 1:
        if early_times.index[0] == 0:
            all_data['First Time Too Cold'][room_number] = early_times.iloc[0]
        else:
            all_data['First Time Too Warm'][room_number] = early_times.iloc[0]
    elif len(early_times) == 2:
        print(early_times)
        all_data['First Time Too Cold'][room_number] = early_times.iloc[0]
        all_data['First Time Too Warm'][room_number] = early_times.iloc[1]
        # make sure data is sorted before this happens!!! I think it is sorted because of the groupby
    late_times = temp_df['Last Time']
    if len(late_times) == 1:
        if late_times.index[0] == 0:
            all_data['Last Time Too Cold'][room_number] = late_times.iloc[0]
        else:
            all_data['Last Time Too Warm'][room_number] = late_times.iloc[0]
    elif len(late_times) == 2:
        print(late_times)
        all_data['Last Time Too Cold'][room_number] = late_times[0]
        all_data['Last Time Too Warm'][room_number] = late_times[1]
        # make sure data is sorted before this happens!!! I think it is sorted because of the groupby

all_data['Lowest Temperature Time'] = None
all_data['Highest Temperature Time'] = None
all_data['Highest CO2 Time'] = None
all_data['Lowest CO2 Time'] = None
temp_data['Lowest Temperature Time'] = None
temp_data['Highest Temperature Time'] = None
co2_data['Lowest CO2 Time'] = None
co2_data['Highest CO2 Time'] = None


def convert_datetime(z):
    if type(z) == str:
        return z
    elif type(z) == pd.Timestamp:
        print(type(datetime.datetime.strftime(z.to_pydatetime(), '%Y-%m-%d %H:%M:%S')))
        return datetime.datetime.strftime(z.to_pydatetime(), '%Y-%m-%d %H:%M:%S')


# finds times of high/low temps on a daily basis... this isn't actually used in the final report but it might be good information to have

for room in time_temp.index:
    low_temps = time_temp.loc[room[0]].loc[all_data['Lowest Temperature'][room[0]]]['Timestamp']
    high_temps = time_temp.loc[room[0]].loc[all_data['Highest Temperature'][room[0]]]['Timestamp']
    if type(low_temps) == pd.Series:
        all_data['Lowest Temperature Time'][room[0]] = convert_datetime(low_temps.iloc[0])
    else:
        all_data['Lowest Temperature Time'][room[0]] = convert_datetime(low_temps)
    if type(high_temps) == pd.Series:
        all_data['Highest Temperature Time'][room[0]] = convert_datetime(high_temps.iloc[0])
    else:
        all_data['Highest Temperature Time'][room[0]] = convert_datetime(high_temps)
    temp_data['Lowest Temperature Time'][room[0]] = all_data['Lowest Temperature Time'][room[0]]
    temp_data['Highest Temperature Time'][room[0]] = all_data['Highest Temperature Time'][room[0]]

for room in time_co2.index:
    low_co2 = time_co2.loc[room[0]].loc[all_data['Lowest CO2'][room[0]]]['Timestamp']
    high_co2 = time_co2.loc[room[0]].loc[all_data['Highest CO2'][room[0]]]['Timestamp']
    if type(low_co2) == pd.Series:
        all_data['Lowest CO2 Time'][room[0]] = convert_datetime(low_co2.iloc[0])
    else:
        all_data['Lowest CO2 Time'][room[0]] = convert_datetime(low_co2)
    if type(high_co2) == pd.Series:
        all_data['Highest CO2 Time'][room[0]] = convert_datetime(high_co2.iloc[0])
    else:
        all_data['Highest CO2 Time'][room[0]] = convert_datetime(high_co2)
    co2_data['Lowest CO2 Time'][room[0]] = all_data['Lowest CO2 Time'][room[0]]
    co2_data['Highest CO2 Time'][room[0]] = all_data['Highest CO2 Time'][room[0]]

# Converts to string so SQL can handle it
for x in range(0, len(all_data['First Time Too Cold'])):
    all_data['First Time Too Cold'].iloc[x] = convert_datetime(all_data['First Time Too Cold'].iloc[x])
    all_data['Last Time Too Cold'].iloc[x] = convert_datetime(all_data['Last Time Too Cold'].iloc[x])
    all_data['First Time Too Warm'].iloc[x] = convert_datetime(all_data['First Time Too Warm'].iloc[x])
    all_data['Last Time Too Warm'].iloc[x] = convert_datetime(all_data['Last Time Too Warm'].iloc[x])

td_copy = td_copy.T.sort_values('Room #')
cd_copy = cd_copy.T.sort_values('Room #')
all_data.to_csv("tester.csv")

# Connect to databases

conn = sqlite3.connect(PATH)
all_data.to_sql("DailyDatabase", conn, if_exists='replace')
td_copy.to_sql("DailyTempDatabase", conn, if_exists='replace')
cd_copy.to_sql("DailyCarbonDatabase", conn, if_exists='replace')

# Clear all daily files so they're not repeated the next day

cursor = conn.cursor()                       
                                          
drop = "DROP TABLE TemperatureProblemsDatabase"
drop2 = "DROP TABLE CarbonDioxideProblemsDatabase"
                                             
cursor.execute(drop)                         
cursor.execute(drop2)
                                             
conn.close()

