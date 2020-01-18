# to be run every 15 minutes: collect data & add to a database

import sqlalchemy
import pandas as pd
import sqlite3
import numpy as np

# Copyright 2018 Building Energy Gateway.  All rights reserved.

import time
from building_data_requests import get_bulk
import numbers
import csv

# sets thresholds
temp_min = 65
temp_units = "deg F"
co2_units = "ppm"
co2_max = 1200
temp_max = 75

start_time = time.time()

# Read spreadsheet into a dataframe.
# Each row contains the following:
#   - Label
#   - Facility
#   - Instance ID of CO2 sensor
#   - Instance ID of temperature sensor

#

SERVER_PATH = '' #'/media/ea/Data/Students/jade/buildingEnergyApi/'

df = pd.read_csv(SERVER_PATH + 'ahs_air.csv', na_filter=False, comment='#' )
# This file location doesn't work on the server (see above file path) --> be careful

# Initialize empty bulk request
bulk_rq = []

# Iterate over the rows of the dataframe, adding elements to the bulk request
for index, row in df.iterrows():

    # Append facility/instance pairs to bulk request
    if row['Temperature']:
        bulk_rq.append( { 'facility': row['Facility'], 'instance': row['Temperature'] } )
    if row['CO2']:
        bulk_rq.append( { 'facility': row['Facility'], 'instance': row['CO2'] } )

# Issue get-bulk request
bulk_rsp = get_bulk( bulk_rq )

# Extract map from get-bulk response
map = bulk_rsp['rsp_map']

# Output column headings
# print( 'Location,Temperature,Temperature Units,CO2,CO2 Units' )

# writes to a new csv file, so we can use pandas on the real-time data

# this file location ALSO DOESN'T WORK ON THE SERVER ---> add in that path /media/ea/Data/Students/jade/buildingEnergyApi/
with open(SERVER_PATH + 'ahs_air_data.csv', mode='w') as ahs_air_data:
    temp_writer = csv.writer(ahs_air_data, delimiter=";")
    # Iterate over the rows of the dataframe, displaying temperature and CO2 values extracted from map
    for index, row in df.iterrows():

        # Initialize empty display values
        temp_value = ''
        temp_units = ''
        co2_value = ''
        co2_units = ''

        # Get facility of current row
        facility = row['Facility']

        # Try to extract current row's temperature and CO2 values from map
        if facility in map:

            instance = str( row['Temperature'] )
            if instance and ( instance in map[facility] ):
                rsp = map[facility][instance]
                property = rsp['property']
                temp_value = int( rsp[property] ) if isinstance( rsp[property], numbers.Number ) else ''
                temp_units = rsp['units']

            instance = str( row['CO2'] )
            if instance and ( instance in map[facility] ):
                rsp = map[facility][instance]
                property = rsp['property']
                co2_value = int( rsp[property] ) if isinstance( rsp[property], numbers.Number ) else ''
                co2_units = rsp['units']

        # Output CSV format
        # print(time.asctime())
        current_time = time.asctime()
        temp_writer.writerow( ['{0},{1},{2},{3},{4},{5}'.format( current_time, row['Label'], temp_value, temp_units, co2_value, co2_units ) ])
        #instead of printing, uses writerow method to enter into the csv file

ahs_air_data.close()

PATH = 'my_file'
# ^this works on the server also

# Log raw data into permanent database
df = pd.read_csv(SERVER_PATH + 'ahs_air_data.csv', names=['Timestamp', 'Room #', 'Temperature', 'Temp. Units', 'CO2', 'CO2 Units'])

# Time Testing Cont'd. (not real rooms, unrealistic values should indicate if these get into any permanent databases, but they shouldn't
'''
my_test_room = pd.Series(['Sun Nov 10 17:00:00 2019', '000', -890, 'deg C', -30, 'ppm'], index=df.T.index)
my_second_test_room = pd.Series(['Wed Nov 6 16:00:00 2019', '543', -800, 'deg C', -8080, 'ppm'], index=df.T.index)
my_third_test_room = pd.Series(['Mon Nov 11 16:00:00 2019', '000', -990, 'deg C', -50, 'ppm'], index=df.T.index)
my_fourth_test_room = pd.Series(['Wed Nov 6 19:00:00 2019', '543', 4000, 'deg C', 8, 'ppm'], index=df.T.index)
my_fifth_test_room = pd.Series(['Mon Nov 11 21:00:00 2019', '543', -909090, 'deg C', 8, 'ppm'], index=df.T.index)
my_sixth_test_room = pd.Series(['Thu Oct 31 17:00:00 2019', '000', -890, 'deg C', -30, 'ppm'], index=df.T.index)


df = df.append(my_test_room, ignore_index=True)
df = df.append(my_second_test_room, ignore_index=True)
df = df.append(my_third_test_room, ignore_index=True)
df = df.append(my_fourth_test_room, ignore_index=True)
df = df.append(my_fifth_test_room, ignore_index=True)
df = df.append(my_sixth_test_room, ignore_index=True)
'''

'''
my_seventh_test_room = pd.Series(['Sun Dec 1 17:00:00 2019', 'Mars', -10000, 'deg C', -3, 'ppm'], index=df.T.index)
my_eighth_test_room = pd.Series(['Mon Dec 2 12:00:00 2019', 'Mars', 42938, 'deg C', 98, 'ppm'], index=df.T.index)
my_ninth_test_room = pd.Series(['Mon Dec 2 13:00:00 2019', 'Mars', 5, 'deg C', 1500, 'ppm'], index=df.T.index)
my_tenth_test_room = pd.Series(['Wed Dec 4 01:00:00 2019', 'Europa', 5983, 'deg C', 2985, 'ppm'], index=df.T.index)
my_eleventh_test_room = pd.Series(['Fri Nov 22 09:00:00 2019', 'Europa', -9, 'deg C', 9900, 'ppm'], index=df.T.index)
my_twelfth_test_room = pd.Series(['Thu Oct 31 18:00:00 2019', 'Europa', 0, 'deg C', -3092, 'ppm'], index=df.T.index)


df = df.append(my_seventh_test_room, ignore_index=True)
df = df.append(my_eighth_test_room, ignore_index=True)
df = df.append(my_ninth_test_room, ignore_index=True)
df = df.append(my_tenth_test_room, ignore_index=True)
df = df.append(my_eleventh_test_room, ignore_index=True)
df = df.append(my_twelfth_test_room, ignore_index=True)
'''

'''

#Test Set 1 -- Interval 1, Day 1

my_thirteenth_test_room = pd.Series(['Thu Nov 22 01:00:00 2017', 'Mars', 70, 'deg F', 1199, 'ppm'], index=df.T.index)
my_fourteenth_test_room = pd.Series(['Thu Nov 22 10:00:00 2017', 'Mars', 70, 'deg F', 98, 'ppm'], index=df.T.index)
my_fifteenth_test_room = pd.Series(['Thu Nov 22 09:00:00 2017', 'Mars', 5, 'deg F', 1500, 'ppm'], index=df.T.index)
my_sixteenth_test_room = pd.Series(['Thu Nov 22 13:00:00 2017', 'Europa', 1000, 'deg F', 1100, 'ppm'], index=df.T.index)
my_seventeenth_test_room = pd.Series(['Thu Nov 22 14:00:00 2017', 'Europa', 1960, 'deg F', 1929, 'ppm'], index=df.T.index)
my_eighteenth_test_room = pd.Series(['Thu Nov 22 04:00:00 2017', 'Europa', 70, 'deg F', 1000, 'ppm'], index=df.T.index)


df = df.append(my_thirteenth_test_room, ignore_index=True)
df = df.append(my_fourteenth_test_room, ignore_index=True)
df = df.append(my_fifteenth_test_room, ignore_index=True)
df = df.append(my_sixteenth_test_room, ignore_index=True)
df = df.append(my_seventeenth_test_room, ignore_index=True)
df = df.append(my_eighteenth_test_room, ignore_index=True)

#'''

'''

#Test Set 2 -- Interval 2, Day 2

my_seventh_test_room = pd.Series(['Fri Nov 23 17:00:00 2017', 'Mars', 70, 'deg C', 1199, 'ppm'], index=df.T.index)
my_eighth_test_room = pd.Series(['Fri Nov 23 12:00:00 2017', 'Mars', 70, 'deg C', 1199, 'ppm'], index=df.T.index)
my_ninth_test_room = pd.Series(['Fri Nov 23 13:00:00 2017', 'Mars', 70, 'deg C', 1199, 'ppm'], index=df.T.index)
my_tenth_test_room = pd.Series(['Fri Nov 23 01:00:00 2017', 'Europa', 5983, 'deg C', 2985, 'ppm'], index=df.T.index)
my_eleventh_test_room = pd.Series(['Fri Nov 23 09:00:00 2017', 'Europa', -9, 'deg C', 9900, 'ppm'], index=df.T.index)
my_twelfth_test_room = pd.Series(['Fri Nov 23 18:00:00 2017', 'Europa', 0, 'deg C', -3092, 'ppm'], index=df.T.index)


df = df.append(my_seventh_test_room, ignore_index=True)
df = df.append(my_eighth_test_room, ignore_index=True)
df = df.append(my_ninth_test_room, ignore_index=True)
df = df.append(my_tenth_test_room, ignore_index=True)
df = df.append(my_eleventh_test_room, ignore_index=True)
df = df.append(my_twelfth_test_room, ignore_index=True)

#'''

'''

#Test Set 3: Interval 3, Day 3

my_seventh_test_room = pd.Series(['Sat Nov 24 17:00:00 2017', 'Mars', 80, 'deg C', 1201, 'ppm'], index=df.T.index)
my_eighth_test_room = pd.Series(['Sat Nov 24 12:00:00 2017', 'Mars', 70, 'deg C', 1199, 'ppm'], index=df.T.index)
my_ninth_test_room = pd.Series(['Sat Nov 24 13:00:00 2017', 'Mars', 70, 'deg C', 1199, 'ppm'], index=df.T.index)
my_tenth_test_room = pd.Series(['Sat Nov 24 01:00:00 2017', 'Europa', 59, 'deg C', 2985, 'ppm'], index=df.T.index)
my_eleventh_test_room = pd.Series(['Sat Nov 24 09:00:00 2017', 'Europa', 90, 'deg C', 9900, 'ppm'], index=df.T.index)
my_twelfth_test_room = pd.Series(['Sat Nov 24 18:00:00 2017', 'Europa', 0, 'deg C', -3093, 'ppm'], index=df.T.index)


df = df.append(my_seventh_test_room, ignore_index=True)
df = df.append(my_eighth_test_room, ignore_index=True)
df = df.append(my_ninth_test_room, ignore_index=True)
df = df.append(my_tenth_test_room, ignore_index=True)
df = df.append(my_eleventh_test_room, ignore_index=True)
df = df.append(my_twelfth_test_room, ignore_index=True)


#'''

df = df.set_index('Room #')

# I had commented out the permanent database for testing, but it's back now
engine = sqlalchemy.create_engine('sqlite:///' + PATH)
conn = sqlite3.connect(PATH)
df.to_sql("TempAndCO2Log", conn, if_exists='append') # actual permanent database
df.to_sql("TempAndCO2LogWeekly", conn, if_exists='append') # copy used for tasks 3 and 4 in this branch, must be cleared out every week

# Carbon Dioxide minimum is calculated by subtracting 20 from the outside levels
outside = df.loc['Outside Air']
co2_min = outside['CO2'] - 20
print("hi")
print(co2_min)

# TASK TWO BEGINS HERE: analysis of problem rooms at each interval

# creates a "sort" of copy for analysis/Task 2
new_data = df.sort_values(by='Room #').reset_index()
print("Full CSV: ")
print(new_data)

# now we can connect the dataframe to 3 databases

conn = sqlite3.connect(PATH)


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
temp_data.to_csv('tester.csv')


# print("\nToo Much CO2: \n")
carbon_data = new_data[(new_data.CO2 > co2_max) | (new_data.CO2 < co2_min)][['Timestamp', 'Room #', 'Temperature', 'CO2']].sort_values(by='CO2')
carbon_data['High Carbon?'] = carbon_data.T.apply(check_carbon)
carbon_data.to_sql("CarbonDioxideProblemsDatabase", conn, if_exists='append')

# Report elapsed time
elapsed_time = round( ( time.time() - start_time ) * 1000 ) / 1000
print( '\nElapsed time: {0} seconds'.format( elapsed_time ) )

