import sqlalchemy
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime
import datetime as dt
import sys

# Copyright 2018 Building Energy Gateway.  All rights reserved.

import time
from building_data_requests import get_bulk
import numbers
import csv

temp_min = 65
temp_units = "deg F"
co2_units = "ppm"
co2_max = 1200
temp_max = 75

SERVER_PATH = ''  # '/media/ea/Data/Students/jade/buildingEnergyApi/'
PATH = 'my_file'

engine = sqlalchemy.create_engine('sqlite:///' + SERVER_PATH + PATH)
conn = sqlite3.connect(SERVER_PATH + PATH)

start_time = time.time()

# TASK TWO BEGINS HERE: analysis of problem rooms at each interval

df = pd.read_sql("TempAndCO2LogFiltered", engine)

# version with input -- could evolve into an interactive front end. Automation will come
# This is now deprecated: the week start is chosen at task_zero.
# week_start_month = input("Month: (number 1-12)")
# week_start_day = input("Day: (number 1-31)")
# week_start_year = input("Year: ")

# week_start = datetime.strptime(week_start_month + " " + week_start_day + " " + week_start_year + " 10:30:02", "%m %d %Y %H:%M:%S")
# #print(week_start)

# #print(df)
df_test_copy = df.set_index("Timestamp")
df_test_copy["Timestamp"] = pd.to_datetime(df_test_copy.index)
# for i in range(0, len(df_test_copy.index)):
# df_test_copy["New Column"][i] = datetime.strptime(df_test_copy.index[i], "%a %b %d %H:%M:%S %Y")
# #print("Still working...")
df_test_copy = df_test_copy.set_index(["Timestamp", "Room #"])
# #print(week_start)
# #print(datetime(2020, 2, 14, 7, 0, 3))
# #print(df_test_copy.index)
# #print(df_test_copy)
# #print(df_test_copy.loc[str(week_start)])
# the above line works if you add in the desired room # or not - use .loc to get a row

#mi_test = pd.DataFrame(np.array([[3, 2, 1], [4, 5, 5], [7, 48, 9]]), columns=[1, 3, 5])
##print(mi_test)
##print(mi_test.loc[0])
#mi_test = mi_test.set_index([1, 3])
##print(mi_test)

# #print(mi_test[mi_test.index[0]])
# #print(mi_test.loc[(1732, 222)])# produces a key error


# Gets interval data about a certain datetime, and the optional room parameter is passed in
# Not needed yet...
def get_interval_data(date_time, room=None):
    if room is None:
        print(df_test_copy.loc[str(date_time)])  # this works
    else:
        print(df_test_copy.loc[(str(date_time), str(room))])  # this also works -- the room data type is a STRING


# get_interval_data(datetime(2020, 2, 14, 7, 0, 3))  # test function call 3/11 -- works perfectly!

# Function defs from task II
def check_temp(x):
    ##print("Start of x:")
    ##print(x)
    if x['Temperature'] > temp_max:
        return True
    return False


def check_carbon(x):
    if x['CO2'] > co2_max:
        return True
    return False
# End of function defs from Task II


new_data = df_test_copy.copy().reset_index()
new_data_copy = new_data.copy()

new_data_copy["Weekday"] = new_data_copy["Timestamp"].apply(lambda x: x.weekday())
new_data_copy.to_csv("basic_weekly.csv")
co2_min = 350
# TODO: Fix this placeholder value

# These drops AREN'T necessary! We are replacing the tables at the start of the loop anyways
# conn.cursor().execute("DROP TABLE TemperatureProblemsDatabase")
# conn.cursor().execute("DROP TABLE CarbonDioxideProblemsDatabase")

filtered_log = pd.read_sql("TempAndCO2LogFiltered", engine)
filtered_log.to_csv("weekly.csv")

# temporarily (0,1) --> should be (0, 5) or (0, # of days)
for i in range(0, 5):
    # TODO: for each day, filter (in task 2 style) & create daily problem report. Append this to a database,
    #  which will serve as a task 3 equivalent

    new_data = new_data_copy[new_data_copy["Weekday"] == i]

    # Beginning of Section Modified from Task II

    # #print("\nToo Cold: \n")
    temp_data = new_data[(new_data['Temperature'] < temp_min) | (new_data['Temperature'] > temp_max)]
    temp_data = temp_data[['Timestamp', 'Room #', 'Temperature', 'CO2']].sort_values(by="Temperature", ascending=True)
    temp_data['High Temp?'] = temp_data.T.apply(check_temp)
    ##print("Temp Data")
    ##print(i)
    ##print(temp_data)
    # temp_data.to_csv("tester.csv")
    # random_testing_copy = temp_data.copy().reset_index()
    # for i in range(temp_data.size):
    #   #print(random_testing_copy.loc[i])
    temp_data.to_sql("TemperatureProblemsDatabase", conn, if_exists='replace')  # should replace, because task three will run on one day of data at a time.

    # #print("\nToo Much CO2: \n")
    carbon_data = new_data[(new_data.CO2 > co2_max) | (new_data.CO2 < co2_min)][['Timestamp', 'Room #', 'Temperature', 'CO2']].sort_values(by='CO2')
    carbon_data['High Carbon?'] = carbon_data.T.apply(check_carbon)
    carbon_data.to_sql("CarbonDioxideProblemsDatabase", conn, if_exists='replace')  # should replace, because task three will run on one day of data at a time.
    # carbon_data.to_csv("weekly.csv")

    # End of Section Modified from Task II

    # TODO: make a task 3 aggregation here.

    temp_data = pd.read_sql_table("TemperatureProblemsDatabase", engine)  # might need this into the other sql table directly... probably easiest
    temp_data = temp_data.sort_values("Room #")
    temp_data.to_csv(SERVER_PATH + 'tester.csv')
    co2_data = pd.read_sql_table("CarbonDioxideProblemsDatabase", engine)

    weekly_log = new_data.copy().reset_index().drop("level_0", axis=1)

    # Convert times to integers so that they compare accurately

    for x in range(0, len(temp_data['Timestamp'])):
        temp_data['Timestamp'].loc[x] = (pd.to_datetime(temp_data['Timestamp'].loc[x]) - dt.timedelta(0))
    for x in range(0, len(co2_data['Timestamp'])):
        co2_data['Timestamp'].loc[x] = (pd.to_datetime(co2_data['Timestamp'].loc[x]) - dt.timedelta(0))
    for x in range(0, len(weekly_log['Timestamp'])):
        #print(weekly_log["Timestamp"])
        weekly_log['Timestamp'].loc[x] = (pd.to_datetime(weekly_log['Timestamp'].loc[x]) - dt.timedelta(0))

    time_temp = temp_data.copy().set_index(["Room #", "Temperature"])
    time_co2 = co2_data.copy().set_index(["Room #", "CO2"])
    time_wkly_temp = weekly_log.copy().set_index(["Room #", "Temperature"])
    time_wkly_co2 = weekly_log.copy().set_index(["Room #", "CO2"])

    # Multi-index should identify a room and temp or co2 value uniquely for when we look for the times of h/l values

    td_copy = temp_data.set_index("Room #").T
    cd_copy = co2_data.set_index("Room #").T

    weekly_log['Highest Temperature'] = weekly_log['Temperature']
    weekly_log['Lowest Temperature'] = weekly_log['Temperature']
    weekly_log['Highest CO2'] = weekly_log['CO2']
    weekly_log['Lowest CO2'] = weekly_log['CO2']

    # Groups low/high #s

    weekly_log = weekly_log.groupby("Room #").agg({'Lowest Temperature': np.min,
                                                   'Highest Temperature': np.max,
                                                   'Highest CO2': np.max,
                                                   'Lowest CO2': np.min})

    # weekly_log.to_csv("tester.csv")

    all_data = pd.merge(temp_data, co2_data, how='outer', on=['Room #', "Timestamp", "Temperature", "CO2"]).drop("index_x", axis=1).drop("index_y", axis=1)
    # all_data.to_csv("tester.csv")

    # Finds number of intervals with a given problem for each room

    weekly_log['Intervals Too Cold'] = None
    weekly_log['Intervals Too Warm'] = None
    weekly_log['Intervals Too Much CO2'] = None
    weekly_log['Intervals Too Little CO2'] = None

    for room in td_copy:
        #print("ROOM: ")
        #print(room)
        intervals_temp = td_copy[room].T
        intervals_temp['Intervals'] = None
        if type(intervals_temp) == pd.Series:
            intervals_temp = pd.DataFrame(intervals_temp).T
        intervals_temp = intervals_temp.groupby("High Temp?").agg({"Intervals": np.size})
        #print("Temp Intervals: ")
        #print(intervals_temp)
        if len(intervals_temp) == 1:
            if intervals_temp.index[0] == 0:
                weekly_log['Intervals Too Cold'][room] = (intervals_temp.iloc[0])[0]
            else:
                weekly_log['Intervals Too Warm'][room] = (intervals_temp.iloc[0])[0]
        elif len(intervals_temp) == 2:
            weekly_log['Intervals Too Cold'][room] = (intervals_temp.iloc[0])[0]
            weekly_log['Intervals Too Warm'][room] = (intervals_temp.iloc[1])[0]

    for room in cd_copy:
        #print("ROOM: ")
        #print(room)
        intervals_co2 = cd_copy[room].T
        intervals_co2['Intervals'] = None
        if type(intervals_co2) == pd.Series:
            intervals_co2 = pd.DataFrame(intervals_co2).T
        intervals_co2 = intervals_co2.groupby("High Carbon?").agg({"Intervals": np.size})
        #print("CO2 Intervals: ")
        #print(intervals_co2)
        if len(intervals_co2) == 1:
            if intervals_co2.index[0] == 0:
                weekly_log['Intervals Too Little CO2'][room] = (intervals_co2.iloc[0])[0]
            else:
                weekly_log['Intervals Too Much CO2'][room] = (intervals_co2.iloc[0])[0]
        elif len(intervals_co2) == 2:
            weekly_log['Intervals Too Little CO2'][room] = (intervals_co2.iloc[0])[0]
            weekly_log['Intervals Too Much CO2'][room] = (intervals_co2.iloc[1])[0]

    # go back into time database (copied from original database) and locate timestamps

    weekly_log['First Time Too Cold'] = None
    weekly_log['First Time Too Warm'] = None
    weekly_log['Last Time Too Cold'] = None
    weekly_log['Last Time Too Warm'] = None

    for room in time_temp.index:
        room_number = room[0]
        temp_df = time_temp.loc[room_number]
        temp_df['First Time'] = temp_df['Timestamp']
        temp_df['Last Time'] = temp_df['Timestamp']
        temp_df = temp_df.groupby("High Temp?").agg({"First Time": np.min, "Last Time": np.max})
        early_times = temp_df['First Time']
        if len(early_times) == 1:
            if early_times.index[0] == 0:
                weekly_log['First Time Too Cold'][room_number] = early_times.iloc[0]
            else:
                weekly_log['First Time Too Warm'][room_number] = early_times.iloc[0]
        elif len(early_times) == 2:
            #print(early_times)
            weekly_log['First Time Too Cold'][room_number] = early_times.iloc[0]
            weekly_log['First Time Too Warm'][room_number] = early_times.iloc[1]
            # make sure data is sorted before this happens!!! I think it is sorted because of the groupby
        late_times = temp_df['Last Time']
        if len(late_times) == 1:
            if late_times.index[0] == 0:
                weekly_log['Last Time Too Cold'][room_number] = late_times.iloc[0]
            else:
                weekly_log['Last Time Too Warm'][room_number] = late_times.iloc[0]
        elif len(late_times) == 2:
            #print(late_times)
            weekly_log['Last Time Too Cold'][room_number] = late_times[0]
            weekly_log['Last Time Too Warm'][room_number] = late_times[1]
            # make sure data is sorted before this happens!!! I think it is sorted because of the groupby

    weekly_log['Time of Lowest Temperature'] = None
    weekly_log['Time of Highest Temperature'] = None
    weekly_log['Time of Highest CO2'] = None
    weekly_log['Time of Lowest CO2'] = None
    temp_data['Time of Lowest Temperature'] = None
    temp_data['Time of Highest Temperature'] = None
    co2_data['Time of Lowest CO2'] = None
    co2_data['Time of Highest CO2'] = None


    def convert_datetime(z):
        if type(z) == str:
            return z
        elif type(z) == pd.Timestamp:
            #print(type(datetime.strftime(z.to_pydatetime(), '%Y-%m-%d %H:%M:%S')))
            return datetime.strftime(z.to_pydatetime(), '%Y-%m-%d %H:%M:%S')


    # finds times of high/low temps on a daily basis... this isn't actually used in the final report but it might be good information to have

    for room in time_wkly_temp.index:
        low_temps = time_wkly_temp.loc[room[0]].loc[weekly_log['Lowest Temperature'][room[0]]]['Timestamp']
        high_temps = time_wkly_temp.loc[room[0]].loc[weekly_log['Highest Temperature'][room[0]]]['Timestamp']
        if type(low_temps) == pd.Series:
            weekly_log['Time of Lowest Temperature'][room[0]] = convert_datetime(low_temps.iloc[0])
        else:
            weekly_log['Time of Lowest Temperature'][room[0]] = convert_datetime(low_temps)
        if type(high_temps) == pd.Series:
            weekly_log['Time of Highest Temperature'][room[0]] = convert_datetime(high_temps.iloc[0])
        else:
            weekly_log['Time of Highest Temperature'][room[0]] = convert_datetime(high_temps)
        temp_data['Time of Lowest Temperature'][room[0]] = weekly_log['Time of Lowest Temperature'][room[0]]
        temp_data['Time of Highest Temperature'][room[0]] = weekly_log['Time of Highest Temperature'][room[0]]

    for room in time_wkly_co2.index:
        low_co2 = time_wkly_co2.loc[room[0]].loc[weekly_log['Lowest CO2'][room[0]]]['Timestamp']
        high_co2 = time_wkly_co2.loc[room[0]].loc[weekly_log['Highest CO2'][room[0]]]['Timestamp']
        if type(low_co2) == pd.Series:
            weekly_log['Time of Lowest CO2'][room[0]] = convert_datetime(low_co2.iloc[0])
        else:
            weekly_log['Time of Lowest CO2'][room[0]] = convert_datetime(low_co2)
        if type(high_co2) == pd.Series:
            weekly_log['Time of Highest CO2'][room[0]] = convert_datetime(high_co2.iloc[0])
        else:
            weekly_log['Time of Highest CO2'][room[0]] = convert_datetime(high_co2)
        co2_data['Time of Lowest CO2'][room[0]] = weekly_log['Time of Lowest CO2'][room[0]]
        co2_data['Time of Highest CO2'][room[0]] = weekly_log['Time of Highest CO2'][room[0]]

    #weekly_log = pd.merge(all_data, weekly_log, how='outer', on=['Room #'])

    # Converts to string so SQL can handle it
    for x in range(0, len(weekly_log['First Time Too Cold'])):
        weekly_log['First Time Too Cold'].iloc[x] = convert_datetime(weekly_log['First Time Too Cold'].iloc[x])
        weekly_log['Last Time Too Cold'].iloc[x] = convert_datetime(weekly_log['Last Time Too Cold'].iloc[x])
        weekly_log['First Time Too Warm'].iloc[x] = convert_datetime(weekly_log['First Time Too Warm'].iloc[x])
        weekly_log['Last Time Too Warm'].iloc[x] = convert_datetime(weekly_log['Last Time Too Warm'].iloc[x])

    for x in range(0, len(time_wkly_temp['Timestamp'])):
        time_wkly_temp['Timestamp'].iloc[x] = convert_datetime(time_wkly_temp['Timestamp'].iloc[x])

    for x in range(0, len(time_wkly_co2['Timestamp'])):
        time_wkly_co2['Timestamp'].iloc[x] = convert_datetime(time_wkly_co2['Timestamp'].iloc[x])

    time_wkly_temp = time_wkly_temp.reset_index()
    time_wkly_co2 = time_wkly_co2.reset_index()
    time_wkly_temp = time_wkly_temp.sort_values('Room #')
    time_wkly_co2 = time_wkly_co2.sort_values('Room #')
    # all_data.to_csv("tester.csv")

    #time_wkly_temp.to_csv("tester.csv")

    # Connect to databases

    conn = sqlite3.connect(SERVER_PATH + PATH)
    all_data.to_sql("FilteredT3Database", conn, if_exists='append')
    time_wkly_temp.to_sql("DailyTempDatabase", conn, if_exists='append')
    #print(time_wkly_temp)
    time_wkly_co2.to_sql("DailyCarbonDatabase", conn, if_exists='append')
    weekly_log.to_sql("DailyDatabase", conn, if_exists='append')

    # Drops aren't necessary:
    # TemperatureProblems and CarbonDioxideProblems DBs are "replaced" at the start of the loop
    # Daily Log is reset to a copy of "new_data" at the start of the loop
    #print("Daily Problems")

# sql_temp_test = pd.read_sql("TemperatureProblemsDatabase", engine)
# sql_co2_test = pd.read_sql("CarbonDioxideProblemsDatabase", engine)
# sql_co2_test.to_csv("weekly.csv")
