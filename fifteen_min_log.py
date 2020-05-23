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

SERVER_PATH = ''  # '/media/ea/Data/Students/jade/buildingEnergyApi/'

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

df = pd.read_csv(SERVER_PATH + 'ahs_air_data.csv', names=['Timestamp', 'Room #', 'Temperature', 'Temp. Units', 'CO2', 'CO2 Units'])
# df = pd.DataFrame()

# Time Testing Cont'd. (not real rooms, unrealistic values should indicate if these get into any permanent databases, but they shouldn't
'''
my_test_room = pd.Series(['Mon Mar 23 17:00:00 2020', '000', -890, 'deg C', -30, 'ppm'], index=df.T.index)
my_second_test_room = pd.Series(['Mon Mar 23 16:00:00 2020', '543', -800, 'deg C', -8080, 'ppm'], index=df.T.index)
my_third_test_room = pd.Series(['Tue Mar 24 16:00:00 2020', '000', -990, 'deg C', -50, 'ppm'], index=df.T.index)
my_fourth_test_room = pd.Series(['Tue Mar 24 19:00:00 2020', '543', 4000, 'deg C', 8, 'ppm'], index=df.T.index)
my_fifth_test_room = pd.Series(['Wed Mar 25 21:00:00 2020', '543', -909090, 'deg C', 8, 'ppm'], index=df.T.index)
my_sixth_test_room = pd.Series(['Wed Mar 25 17:00:00 2020', '000', -890, 'deg C', -30, 'ppm'], index=df.T.index)


df = df.append(my_test_room, ignore_index=True)
df = df.append(my_second_test_room, ignore_index=True)
df = df.append(my_third_test_room, ignore_index=True)
df = df.append(my_fourth_test_room, ignore_index=True)
df = df.append(my_fifth_test_room, ignore_index=True)
df = df.append(my_sixth_test_room, ignore_index=True)

my_seventh_test_room = pd.Series(['Wed Mar 25 17:00:00 2020', 'Mars', -10000, 'deg C', -3, 'ppm'], index=df.T.index)
my_eighth_test_room = pd.Series(['Wed Mar 25 12:00:00 2020', 'Mars', 42938, 'deg C', 98, 'ppm'], index=df.T.index)
my_ninth_test_room = pd.Series(['Thu Mar 26 13:00:00 2020', 'Mars', 5, 'deg C', 1500, 'ppm'], index=df.T.index)
my_tenth_test_room = pd.Series(['Thu Mar 26 01:00:00 2020', 'Europa', 5983, 'deg C', 2985, 'ppm'], index=df.T.index)
my_eleventh_test_room = pd.Series(['Fri Mar 27 09:00:00 2020', 'Europa', -9, 'deg C', 9900, 'ppm'], index=df.T.index)
my_twelfth_test_room = pd.Series(['Fri Mar 27 18:00:00 2020', 'Europa', 0, 'deg C', -3092, 'ppm'], index=df.T.index)


df = df.append(my_seventh_test_room, ignore_index=True)
df = df.append(my_eighth_test_room, ignore_index=True)
df = df.append(my_ninth_test_room, ignore_index=True)
df = df.append(my_tenth_test_room, ignore_index=True)
df = df.append(my_eleventh_test_room, ignore_index=True)
df = df.append(my_twelfth_test_room, ignore_index=True)




#Test Set 1 -- Interval 1, Day 1

my_thirteenth_test_room = pd.Series(['Thu Mar 26 01:00:00 2020', 'Mars', 70, 'deg F', 1199, 'ppm'], index=df.T.index)
my_fourteenth_test_room = pd.Series(['Thu Mar 26 10:00:00 2020', 'Mars', 70, 'deg F', 98, 'ppm'], index=df.T.index)
my_fifteenth_test_room = pd.Series(['Thu Mar 26 09:00:00 2020', 'Mars', 5, 'deg F', 1500, 'ppm'], index=df.T.index)
my_sixteenth_test_room = pd.Series(['Thu Mar 26 13:00:00 2020', 'Europa', 1000, 'deg F', 1100, 'ppm'], index=df.T.index)
my_seventeenth_test_room = pd.Series(['Thu Mar 26 14:00:00 2020', 'Europa', 1960, 'deg F', 1929, 'ppm'], index=df.T.index)
my_eighteenth_test_room = pd.Series(['Thu Mar 26 04:00:00 2020', 'Europa', 70, 'deg F', 1000, 'ppm'], index=df.T.index)


df = df.append(my_thirteenth_test_room, ignore_index=True)
df = df.append(my_fourteenth_test_room, ignore_index=True)
df = df.append(my_fifteenth_test_room, ignore_index=True)
df = df.append(my_sixteenth_test_room, ignore_index=True)
df = df.append(my_seventeenth_test_room, ignore_index=True)
df = df.append(my_eighteenth_test_room, ignore_index=True)





#Test Set 2 -- Interval 2, Day 2

my_seventh_test_room = pd.Series(['Fri Mar 27 17:00:00 2020', 'Mars', 70, 'deg C', 1199, 'ppm'], index=df.T.index)
my_eighth_test_room = pd.Series(['Fri Mar 27 12:00:00 2020', 'Mars', 70, 'deg C', 1199, 'ppm'], index=df.T.index)
my_ninth_test_room = pd.Series(['Fri Mar 27 13:00:00 2020', 'Mars', 70, 'deg C', 1199, 'ppm'], index=df.T.index)
my_tenth_test_room = pd.Series(['Fri Mar 27 01:00:00 2020', 'Europa', 5983, 'deg C', 2985, 'ppm'], index=df.T.index)
my_eleventh_test_room = pd.Series(['Fri Mar 27 09:00:00 2020', 'Europa', -9, 'deg C', 9900, 'ppm'], index=df.T.index)
my_twelfth_test_room = pd.Series(['Fri Mar 27 18:00:00 2020', 'Europa', 0, 'deg C', -3092, 'ppm'], index=df.T.index)


df = df.append(my_seventh_test_room, ignore_index=True)
df = df.append(my_eighth_test_room, ignore_index=True)
df = df.append(my_ninth_test_room, ignore_index=True)
df = df.append(my_tenth_test_room, ignore_index=True)
df = df.append(my_eleventh_test_room, ignore_index=True)
df = df.append(my_twelfth_test_room, ignore_index=True)





# Test Set 3: Interval 3, Day 3

my_seventh_test_room = pd.Series(['Tue Mar 24 17:00:00 2020', 'Mars', 80, 'deg C', 1201, 'ppm'], index=df.T.index)
my_eighth_test_room = pd.Series(['Tue Mar 24 12:00:00 2020', 'Mars', 70, 'deg C', 1199, 'ppm'], index=df.T.index)
my_ninth_test_room = pd.Series(['Tue Mar 24 13:00:00 2020', 'Mars', 70, 'deg C', 1199, 'ppm'], index=df.T.index)
my_tenth_test_room = pd.Series(['Tue Mar 24 01:00:00 2020', 'Europa', 59, 'deg C', 2985, 'ppm'], index=df.T.index)
my_eleventh_test_room = pd.Series(['Tue Mar 24 09:00:00 2020', 'Europa', 90, 'deg C', 9900, 'ppm'], index=df.T.index)
my_twelfth_test_room = pd.Series(['Tue Mar 24 18:00:00 2020', 'Europa', 0, 'deg C', -3093, 'ppm'], index=df.T.index)


df = df.append(my_seventh_test_room, ignore_index=True)
df = df.append(my_eighth_test_room, ignore_index=True)
df = df.append(my_ninth_test_room, ignore_index=True)
df = df.append(my_tenth_test_room, ignore_index=True)
df = df.append(my_eleventh_test_room, ignore_index=True)
df = df.append(my_twelfth_test_room, ignore_index=True)


#'''

df = df.set_index('Room #')

# I had commented out the permanent database for testing, but it's back now
engine = sqlalchemy.create_engine('sqlite:///' + SERVER_PATH + PATH)
conn = sqlite3.connect(SERVER_PATH + PATH)
df.to_sql("TempAndCO2Log", conn, if_exists='append')  # actual permanent database
df.to_sql("TempAndCO2LogDaily", conn, if_exists='append')  # copy used for tasks 3 and 4 in this branch, must be cleared out every week
new_df = pd.read_sql("TempAndCO2LogDaily", engine)
new_df.to_csv(SERVER_PATH + "tester.csv")
