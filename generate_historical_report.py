import sqlalchemy
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

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

SERVER_PATH = ''#'/media/ea/Data/Students/jade/buildingEnergyApi/'
PATH = 'my_file'

engine = sqlalchemy.create_engine('sqlite:///' + SERVER_PATH + PATH)
conn = sqlite3.connect(SERVER_PATH + PATH)

start_time = time.time()

# TASK TWO BEGINS HERE: analysis of problem rooms at each interval

df = pd.read_sql("TempAndCO2LogFiltered", engine)

# version with input -- could evolve into an interactive front end. Automation will come
week_start_month = input("Month: (number 1-12)")
week_start_day = input("Day: (number 1-31)")
week_start_year = input("Year: ")

# TODO: select a week of data from the database

week_start = datetime.strptime(week_start_month + " " + week_start_day + " " + week_start_year + " 10:30:02", "%m %d %Y %H:%M:%S")
print(week_start)

print(df)
df_test_copy = df.set_index("Timestamp")
df_test_copy["New Column"] = pd.to_datetime(df_test_copy.index)
# for i in range(0, len(df_test_copy.index)):
    # df_test_copy["New Column"][i] = datetime.strptime(df_test_copy.index[i], "%a %b %d %H:%M:%S %Y")
    # print("Still working...")
df_test_copy = df_test_copy.set_index(["New Column", "Room #"])
print(week_start)
print(datetime(2020, 2, 14, 7, 0, 3))
print(df_test_copy.index)
print(df_test_copy)
print(df_test_copy.loc[str(week_start)])
# the above line works if you add in the desired room # or not - use .loc to get a row

mi_test = pd.DataFrame(np.array([[3, 2, 1], [4, 5, 5], [7, 48, 9]]), columns=[1, 3, 5])
print(mi_test)
print(mi_test.loc[0])
mi_test = mi_test.set_index([1, 3])
print(mi_test)

# print(mi_test[mi_test.index[0]])
# print(mi_test.loc[(1732, 222)])# produces a key error


# Gets interval data about a certain datetime, and the optional room parameter is passed in
def get_interval_data(date_time, room=None):
    if room is None:
        print(df_test_copy.loc[str(date_time)])  # this works
    else:
        print(df_test_copy.loc[(str(date_time), str(room))])  # this also works -- the room data type is a STRING


get_interval_data(datetime(2020, 2, 14, 7, 0, 3), 225)  # test function call 3/11 -- works perfectly!


# Function defs from task II
def check_temp(x):
    print("Start of x:")
    print(x)
    if x['Temperature'] > temp_max:
        return True
    return False


def check_carbon(x):
    if x['CO2'] > co2_max:
        return True
    return False
# End of function defs from Task II


new_data = df_test_copy.copy().reset_index()

co2_min = 200
# TODO: Fix this placeholder value

for i in range(0, 7):
    # TODO: for each day, filter (in task 2 style) & create daily problem report. Append this to a database,
    #  which will serve as a task 3 equivalent

    # Beginning of Section Modified from Task II

    # print("\nToo Cold: \n")
    temp_data = new_data[(new_data['Temperature'] < temp_min) | (new_data['Temperature'] > temp_max)]
    temp_data = temp_data[['New Column', 'Room #', 'Temperature', 'CO2']].sort_values(by="Temperature", ascending=True)
    temp_data['High Temp?'] = temp_data.T.apply(check_temp)
    print(temp_data)
    temp_data.to_sql("TemperatureProblemsDatabase", conn, if_exists='replace')
    # temp_data.to_csv('tester.csv')

    # print("\nToo Much CO2: \n")
    carbon_data = new_data[(new_data.CO2 > co2_max) | (new_data.CO2 < co2_min)][['New Column', 'Room #', 'Temperature', 'CO2']].sort_values(by='CO2')
    carbon_data['High Carbon?'] = carbon_data.T.apply(check_carbon)
    carbon_data.to_sql("CarbonDioxideProblemsDatabase", conn, if_exists='replace')

    # End of Section Modified from Task II
    print("Daily Problems")

# TODO: run task 4 on aggregation of daily problem reports
# TODO: rename from "New Column" to "Timestamp" or something equivalent
