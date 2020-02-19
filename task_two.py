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

df = pd.read_sql("TempAndCO2LogWeekly", engine)

# version with input -- could evolve into an interactive front end. Automation will come
week_start_month = input("Month: (number 1-12)")
week_start_day = input("Day: (number 1-31)")
week_start_year = input("Year: ")

week_start = datetime.strptime(week_start_month + " " + week_start_day + " " + week_start_year, "%m %d %Y")
print(week_start)

#TODO: filter based on weekly condition

# creates a "sort" of copy for analysis/Task 2
new_data = df.sort_values(by='Room #').reset_index()
print("Full CSV: ")
print(new_data)

# now we can connect the dataframe to 3 databases

conn = sqlite3.connect(SERVER_PATH + PATH)

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


# print("\nToo Cold: \n")
temp_data = new_data[(new_data['Temperature'] < temp_min) | (new_data['Temperature'] > temp_max)]
temp_data = temp_data[['Timestamp', 'Room #', 'Temperature', 'CO2']].sort_values(by="Temperature", ascending=True)
temp_data['High Temp?'] = temp_data.T.apply(check_temp)
print(temp_data)
temp_data.to_sql("TemperatureProblemsDatabase", conn, if_exists='append')
# temp_data.to_csv('tester.csv')


# print("\nToo Much CO2: \n")
carbon_data = new_data[(new_data.CO2 > co2_max) | (new_data.CO2 < co2_min)][['Timestamp', 'Room #', 'Temperature', 'CO2']].sort_values(by='CO2')
carbon_data['High Carbon?'] = carbon_data.T.apply(check_carbon)
carbon_data.to_sql("CarbonDioxideProblemsDatabase", conn, if_exists='append')

# Report elapsed time
elapsed_time = round( ( time.time() - start_time ) * 1000 ) / 1000
print( '\nElapsed time: {0} seconds'.format( elapsed_time ) )

