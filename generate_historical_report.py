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

df = pd.read_sql("TempAndCO2Log", engine)

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
print(df_test_copy[str(week_start), 'Outside Air'])

for i in range(0, 7):
    # TODO: for each day, filter & create daily problem report. Append this to a database
    print("Daily Problems")

# TODO: run task 4 on aggregation of daily problem reports
