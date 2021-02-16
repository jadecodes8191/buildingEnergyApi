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

#TODO: run task 4 on aggregation of daily problem reports

daily_data = pd.read_sql_table("DailyDatabase", engine)
daily_data['Days With Problems'] = None
all_temps = pd.read_sql_table("DailyTempDatabase", engine)
all_carbon = pd.read_sql_table("DailyCarbonDatabase", engine)
days_with_problems = pd.read_sql_table("FilteredT3Database", engine)
days_with_problems = days_with_problems.drop("index", axis=1)
days_with_problems['Day'] = days_with_problems['Timestamp'].apply(lambda z: datetime.strftime(z, "%Y-%m-%d"))#kept as a string for now just to avoid automatic time assignment
days_with_problems = days_with_problems.set_index(["Room #", "Day"])
days_with_problems["Days With Problems"] = None
days_with_problems = days_with_problems.groupby(level=[0, 1]).agg({"Days With Problems": np.size})
days_with_problems = days_with_problems.groupby(level=0).agg({"Days With Problems": np.size})
days_with_problems.to_csv("ahs_cold_data.csv")

all_temps_copy = all_temps.set_index(['Room #', 'Temperature'])
all_carbon_copy = all_carbon.set_index(['Room #', 'CO2'])


def convert_back(z):
    if z == "N/A":
        return np.NaN
    elif z is not None:
        return datetime.strptime(z, "%Y-%m-%d %H:%M:%S").timestamp()
    else:
        return None


for x in range(0, len(daily_data['First Time Too Cold'])):
    daily_data['First Time Too Cold'].loc[x] = convert_back(daily_data['First Time Too Cold'].loc[x])
    daily_data['Last Time Too Cold'].loc[x] = convert_back(daily_data['Last Time Too Cold'].loc[x])
    daily_data['First Time Too Warm'].loc[x] = convert_back(daily_data['First Time Too Warm'].loc[x])
    daily_data['Last Time Too Warm'].loc[x] = convert_back(daily_data['Last Time Too Warm'].loc[x])

#print(daily_data['Last Time Too Cold'])


def none_to_nan(x):
    if x is None:
        return np.NaN
    return x


all_temps['Temperature'] = all_temps['Temperature'].apply(none_to_nan)
all_carbon['CO2'] = all_carbon['CO2'].apply(none_to_nan)

all_temps['Median Temperature'] = all_temps['Temperature']
all_temps['Mean Temperature'] = all_temps['Temperature']
temp_analysis = all_temps.groupby("Room #").agg({"Mean Temperature": np.nanmean,
                                                 "Median Temperature": np.nanmedian})

all_carbon['Median CO2'] = all_carbon['CO2']
all_carbon['Mean CO2'] = all_carbon['CO2']
co2_analysis = all_carbon.groupby("Room #").agg({"Mean CO2": np.mean,
                                                "Median CO2": np.median})

# for some reason, sql was automatically converting all the interval values to bytes... but this reverses it


def convert_to_int(x):
    if x is not None:
        return int.from_bytes(x, sys.byteorder)
    return None


daily_data['Intervals Too Warm'] = daily_data['Intervals Too Warm'].apply(convert_to_int)
daily_data['Intervals Too Cold'] = daily_data['Intervals Too Cold'].apply(convert_to_int)
daily_data['Intervals Too Much CO2'] = daily_data['Intervals Too Much CO2'].apply(convert_to_int)
daily_data['Intervals Too Little CO2'] = daily_data['Intervals Too Little CO2'].apply(convert_to_int)

#print(daily_data['First Time Too Cold'])
#print(daily_data['Last Time Too Cold'])

#print(daily_data.where(daily_data["Room #"] == "Mars"))

#days_with_problems = days_with_problems.groupby("")

daily_data = daily_data.groupby("Room #")

daily_data = daily_data.agg({"Intervals Too Warm": np.sum,
                                               "Intervals Too Cold": np.sum,
                                               "Intervals Too Much CO2": np.sum,
                                               "Intervals Too Little CO2": np.sum,
                                               "Highest Temperature": np.max,
                                               "Lowest Temperature": np.min,
                                               'Highest CO2': np.max,
                                               'Lowest CO2': np.min,
                                               "First Time Too Warm": np.min,
                                               "Last Time Too Warm": np.max,
                                               "First Time Too Cold": np.min,
                                               "Last Time Too Cold": np.max})
daily_data = pd.merge(daily_data, days_with_problems, how='outer', on=['Room #'])

daily_data['Time of Highest Temperature'] = None
daily_data['Time of Lowest Temperature'] = None
daily_data['Time of Highest CO2'] = None
daily_data['Time of Lowest CO2'] = None

daily_data.to_csv("tester.csv")

# For each room, goes back into the copies to find the times of the most extreme values
for room in daily_data.index:
    if not np.isnan(daily_data['Highest Temperature'][room]):
        # match highest temp to time at which it occurred
        index_tuple = (room, daily_data['Highest Temperature'][room]) # removed cast to int...
        if type(all_temps_copy.loc[index_tuple]) == pd.Series:
            temp_df =(pd.DataFrame(all_temps_copy.loc[index_tuple]).T.sort_values('Timestamp')).T
            daily_data['Time of Highest Temperature'][room] = temp_df.loc['Timestamp'][0]
        else:
            daily_data['Time of Highest Temperature'][room] = all_temps_copy.loc[index_tuple].sort_values('Timestamp').reset_index().iloc[0]['Timestamp']
    if not np.isnan(daily_data['Lowest Temperature'][room]):
        # match lowest temp to time at which it occurred
        index_tuple = (room, int(daily_data['Lowest Temperature'][room]))
        if type(all_temps_copy.loc[index_tuple]) == pd.Series:
            temp_df =(pd.DataFrame(all_temps_copy.loc[index_tuple]).T.sort_values('Timestamp')).T
            daily_data['Time of Lowest Temperature'][room] = temp_df.loc['Timestamp'][0]
        else:
            daily_data['Time of Lowest Temperature'][room] = all_temps_copy.loc[index_tuple].sort_values('Timestamp').reset_index().iloc[0]['Timestamp']
    if not np.isnan(daily_data['Highest CO2'][room]):
        # match highest co2 to time at which it occurred
        index_tuple = (room, int(daily_data['Highest CO2'][room]))
        if type(all_carbon_copy.loc[index_tuple]) == pd.Series:
            temp_df =(pd.DataFrame(all_carbon_copy.loc[index_tuple]).T.sort_values('Timestamp')).T
            daily_data['Time of Highest CO2'][room] = temp_df.loc['Timestamp'][0]
        else:
            daily_data['Time of Highest CO2'][room] = all_carbon_copy.loc[index_tuple].sort_values('Timestamp').reset_index().iloc[0]['Timestamp']
    if not np.isnan(daily_data['Lowest CO2'][room]):
        # match lowest co2 to time at which it occurred
        index_tuple = (room, int(daily_data['Lowest CO2'][room]))
        if type(all_carbon_copy.loc[index_tuple]) == pd.Series:
            temp_df =(pd.DataFrame(all_carbon_copy.loc[index_tuple]).T.sort_values('Timestamp')).T
            daily_data['Time of Lowest CO2'][room] = temp_df.loc['Timestamp'][0]
        else:
            daily_data['Time of Lowest CO2'][room] = all_carbon_copy.loc[index_tuple].sort_values('Timestamp').reset_index().iloc[0]['Timestamp']


def make_time_readable(x):
    if (x is not None) and (not np.isnan(x)):
        return datetime.fromtimestamp(x)
    return None


daily_data["First Time Too Warm"] = daily_data["First Time Too Warm"].apply(make_time_readable)
daily_data["Last Time Too Warm"] = daily_data["Last Time Too Warm"].apply(make_time_readable)
daily_data["First Time Too Cold"] = daily_data["First Time Too Cold"].apply(make_time_readable)
daily_data["Last Time Too Cold"] = daily_data["Last Time Too Cold"].apply(make_time_readable)

daily_data = pd.merge(daily_data, temp_analysis, how='outer', on=['Room #'])
daily_data = pd.merge(daily_data, co2_analysis, how='outer', on=['Room #'])

daily_data.to_excel("output.xlsx")
# daily_data.to_csv('tester.csv')

# NOTE: The old data that was in the Weekly Log table is saved in a table called OldWeeklyLog, fittingly.
# Clearing weekly files

cursor = conn.cursor()
drop1 = "DROP TABLE DailyTempDatabase"
drop2 = "DROP TABLE DailyCarbonDatabase"
drop3 = "DROP TABLE DailyDatabase"
drop4 = "DROP TABLE FilteredT3Database"
cursor.execute(drop1)
cursor.execute(drop2)
cursor.execute(drop3)
cursor.execute(drop4)


# Task 4.5 -- creating the 4 more consolidated sheets
# UPDATE: in the new branch, this task will also separate rooms with sensor issues into their own spreadsheets

original_file = pd.read_excel("output.xlsx")
#original_file.to_csv("tester.csv")
original_file['Likely Sensor Issue?'] = None
original_file["CO2 Sensor Issue?"] = None
original_file["Temperature Sensor Issue?"] = None
# Too little CO2 should probably be combined with this...
for x in range(0, len(original_file["Days With Problems"])):
    original_file["Likely Sensor Issue?"].iloc[x] = original_file["Intervals Too Cold"].iloc[x] > 160 or original_file['Intervals Too Warm'].iloc[x] > 160 or original_file["Intervals Too Much CO2"].iloc[x] > 160 or original_file["Lowest Temperature"].iloc[x] == original_file["Highest Temperature"].iloc[x] or original_file["Lowest CO2"].iloc[x] == original_file["Highest CO2"].iloc[x] or original_file["Intervals Too Little CO2"].iloc[x] > 0
    original_file["CO2 Sensor Issue?"].iloc[x] = original_file["Intervals Too Much CO2"].iloc[x] > 160 or original_file["Lowest CO2"].iloc[x] == original_file["Highest CO2"].iloc[x] or original_file["Intervals Too Little CO2"].iloc[x] > 0
    original_file["Temperature Sensor Issue?"].iloc[x] = original_file["Intervals Too Cold"].iloc[x] > 160 or original_file['Intervals Too Warm'].iloc[x] > 160 or original_file["Lowest Temperature"].iloc[x] == original_file["Highest Temperature"].iloc[x]


# Cold Values
cold_values = original_file[["Room #", "Days With Problems", "Intervals Too Cold", "Lowest Temperature", "Highest Temperature", "Mean Temperature", "Median Temperature", "First Time Too Cold", "Last Time Too Cold", "Time of Highest Temperature", "Time of Lowest Temperature", "Likely Sensor Issue?", "Temperature Sensor Issue?"]]
cold_values = cold_values[cold_values['Intervals Too Cold'] > 0]
cold_values = cold_values[cold_values["Temperature Sensor Issue?"] == False]
cold_values = cold_values.sort_values(by="Intervals Too Cold", ascending=False)
for x in range(0, len(cold_values['Median Temperature'])):
    cold_values['Median Temperature'].iloc[x] = int(cold_values['Median Temperature'].iloc[x])
    cold_values['Mean Temperature'].iloc[x] = int(cold_values['Mean Temperature'].iloc[x])
    for category in ['Time of Highest Temperature', 'Time of Lowest Temperature', "First Time Too Cold", "Last Time Too Cold"]:
        if type(cold_values[category].iloc[x]) == str:
            temp_time = datetime.strptime(cold_values[category].iloc[x], "%Y-%m-%d %H:%M:%S")
        elif type(cold_values[category].iloc[x] == pd.Timestamp):
            temp_time = cold_values[category].iloc[x]
        cold_values[category].iloc[x] = datetime.strftime(temp_time, "%a %d %b %Y %H:%M")
#cold_values.to_csv("tester.csv")
cold_values.to_excel("cold.xlsx")

# Warm Values
warm_values = original_file[["Room #", "Days With Problems", "Intervals Too Warm", "Lowest Temperature", "Highest Temperature", "Mean Temperature", "Median Temperature", "First Time Too Warm", "Last Time Too Warm", "Time of Highest Temperature", "Time of Lowest Temperature", "Likely Sensor Issue?", "Temperature Sensor Issue?"]]
warm_values = warm_values[warm_values['Intervals Too Warm'] > 0]
warm_values = warm_values[warm_values["Temperature Sensor Issue?"] == False]
warm_values = warm_values.sort_values(by="Intervals Too Warm", ascending=False)
for x in range(0, len(warm_values['Median Temperature'])):
    warm_values['Median Temperature'].iloc[x] = int(warm_values['Median Temperature'].iloc[x])
    warm_values['Mean Temperature'].iloc[x] = int(warm_values['Mean Temperature'].iloc[x])
    for category in ['Time of Highest Temperature', 'Time of Lowest Temperature', "First Time Too Warm", "Last Time Too Warm"]:
        if type(warm_values[category].iloc[x]) == str:
            temp_time = datetime.strptime(warm_values[category].iloc[x], "%Y-%m-%d %H:%M:%S")
        elif type(warm_values[category].iloc[x] == pd.Timestamp):
            temp_time = warm_values[category].iloc[x]
        warm_values[category].iloc[x] = datetime.strftime(temp_time, "%a %d %b %Y %H:%M")
warm_values.to_csv("weekly.csv")
warm_values.to_excel("warm.xlsx")

# High CO2 Values
high_co2 = original_file[["Room #", "Days With Problems", "Intervals Too Much CO2", "Lowest CO2", "Highest CO2", "Mean CO2", "Median CO2", "Time of Highest CO2", "Time of Lowest CO2", "Likely Sensor Issue?", "CO2 Sensor Issue?"]]
high_co2 = high_co2[high_co2['Intervals Too Much CO2'] > 0]
high_co2 = high_co2[high_co2["CO2 Sensor Issue?"] == False]
high_co2 = high_co2.sort_values(by="Intervals Too Much CO2", ascending=False)
for x in range(0, len(high_co2['Median CO2'])):
    high_co2['Median CO2'].iloc[x] = int(high_co2['Median CO2'].iloc[x])
    high_co2['Mean CO2'].iloc[x] = int(high_co2['Mean CO2'].iloc[x])
    for category in ['Time of Highest CO2', 'Time of Lowest CO2']:
        if type(high_co2[category].iloc[x]) == str:
            temp_time = datetime.strptime(high_co2[category].iloc[x], "%Y-%m-%d %H:%M:%S")
        elif type(high_co2[category].iloc[x] == pd.Timestamp):
            temp_time = high_co2[category].iloc[x]
        high_co2[category].iloc[x] = datetime.strftime(temp_time, "%a %d %b %Y %H:%M")
#high_co2.to_csv("basic_weekly.csv")
high_co2.to_excel("high_co2.xlsx")

# SENSOR ISSUE (incl. low co2)
low_co2 = original_file[["Room #", "Intervals Too Warm", "Intervals Too Cold", "Intervals Too Much CO2", "Intervals Too Little CO2", "Lowest CO2", "Highest CO2", "Lowest Temperature", "Highest Temperature", "Likely Sensor Issue?", "CO2 Sensor Issue?", "Temperature Sensor Issue?"]]
low_co2 = low_co2[low_co2["Likely Sensor Issue?"] == True]
low_co2 = low_co2.sort_values(by="Intervals Too Little CO2", ascending=False)
#for x in range(0, len(low_co2['Intervals Too Warm'])):
    # low_co2['Median CO2'].iloc[x] = int(low_co2['Median CO2'].iloc[x])
    # low_co2['Mean CO2'].iloc[x] = int(low_co2['Mean CO2'].iloc[x])
    # low_co2['Median Temperature'].iloc[x] = int(low_co2['Median Temperature'].iloc[x])
    # low_co2['Mean Temperature'].iloc[x] = int(low_co2['Mean Temperature'].iloc[x])
    #for category in ['Time of Highest CO2', 'Time of Lowest CO2', 'Time of Highest Temperature', 'Time of Lowest Temperature', 'First Time Too Cold', 'Last Time Too Cold', 'First Time Too Warm', 'Last Time Too Warm']:
        #if type(low_co2[category].iloc[x]) == str:
            #temp_time = datetime.strptime(low_co2[category].iloc[x], "%Y-%m-%d %H:%M:%S")
        #elif type(low_co2[category].iloc[x] == pd.Timestamp):
            #temp_time = low_co2[category].iloc[x]
        #try:
            #low_co2[category].iloc[x] = datetime.strftime(temp_time, "%a %d %b %Y %H:%M")
        #except ValueError:
            #low_co2[category].iloc[x] = None
low_co2.to_csv("ahs_carbon_data.csv")
low_co2.to_excel("low_co2.xlsx")

