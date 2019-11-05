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
from crontab import CronTab

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

#/media/ea/Data/Students/jade/buildingEnergyApi/
df = pd.read_csv('ahs_air.csv', na_filter=False, comment='#' )
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
with open('ahs_air_data.csv', mode='w') as ahs_air_data:
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

PATH = 'my_file'#change for the server

df = pd.read_csv('ahs_air_data.csv', names=['Timestamp', 'Room #', 'Temperature', 'Temp. Units', 'CO2', 'CO2 Units']).set_index('Room #')
outside = df.loc['Outside Air']
co2_min = outside['CO2'] - 20
print(co2_min)
#my_test_room = pd.DataFrame(pd.Series(["Sun Oct 20 12:00:00 1985", "000", 15, "deg F", np.NaN, "ppm"], index=['Timestamp', 'Room #', 'Temperature', 'Temp Units', 'CO2', 'CO2 Units']))
#my_test_room = my_test_room.T.set_index('Room #')
#print(my_test_room)
#df = df.append(my_test_room, sort=False)
#print(df)

engine = sqlalchemy.create_engine('sqlite:///' + PATH)

conn = sqlite3.connect(PATH)
df.to_sql("ProblemAreasDatabase", conn, if_exists='append')
#new_data = pd.read_sql_table("ProblemAreasDatabase", engine)

#new_data = pd.read_csv('', delimiter=",", names=['Time Stamp', 'Room Number', 'Temperature', 'Temperature Units', 'CO2', 'CO2 Units'])
new_data = df.sort_values(by='Room #').reset_index()
print("Full CSV: ")
print(new_data)
# print(new_data[new_data.Location == '270.01'][['Room Number', 'Temperature', 'CO2']])

# now we can connect the 3 dataframes to a database

conn = sqlite3.connect(PATH)

# print("\nToo Cold: \n")
cold_data = new_data[new_data.Temperature < temp_min][['Room #', 'Temperature', 'CO2']].sort_values(by="Temperature", ascending=True)
print(cold_data)
cold_data.to_sql("ColdDatabase", conn, if_exists='append')

# print("\nToo Much CO2: \n")
carbon_data = new_data[new_data.CO2 > co2_max][['Room #', 'Temperature', 'CO2']].sort_values(by='CO2')
carbon_data.to_sql("CarbonDatabase", conn, if_exists='append')

# print("\nToo Little CO2: \n")
less_carbon_data = new_data[new_data.CO2 < co2_min][['Room #', 'Temperature', 'CO2']].sort_values(by='CO2', ascending=True)
less_carbon_data.to_sql("LowCarbonDatabase", conn, if_exists='append')

# print("\nToo Hot: \n")
warm_data = new_data[new_data.Temperature > temp_max][['Room #', 'Temperature', 'CO2']].sort_values(by='Temperature')
warm_data.to_sql("WarmDatabase", conn, if_exists='append')

# Report elapsed time
elapsed_time = round( ( time.time() - start_time ) * 1000 ) / 1000
print( '\nElapsed time: {0} seconds'.format( elapsed_time ) )
