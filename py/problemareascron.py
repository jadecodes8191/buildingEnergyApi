# Copyright 2018 Building Energy Gateway.  All rights reserved.

import time
import pandas as pd
from building_data_requests import get_bulk
import numbers
import csv
from crontab import CronTab

start_time = time.time()

temp_min = 65
temp_max = 75
co2_max = 1200
# Sets values for reference later -- these are the boundaries to identify whether a room is a "problem area"

# Read spreadsheet into a dataframe.
# Each row contains the following:
#   - Label
#   - Facility
#   - Instance ID of CO2 sensor
#   - Instance ID of temperature sensor
df = pd.read_csv( 'ahs_air.csv', na_filter=False, comment='#' )

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

new_data = pd.read_csv('ahs_air_data.csv', delimiter=",", names=['Time Stamp', 'Room Number', 'Temperature', 'Temperature Units', 'CO2', 'CO2 Units'])
new_data = new_data.sort_values(by='Room Number')
print("Full CSV: ")
print(new_data)
# print(new_data[new_data.Location == '270.01'][['Room Number', 'Temperature', 'CO2']])

# now we can print out the values in 3 respective data frames
# print("\nToo Cold: \n")
cold_data = new_data[new_data.Temperature < temp_min][['Room Number', 'Temperature', 'CO2']].sort_values(by="Temperature", ascending=True)

# print("\nToo Much CO2: \n")
carbon_data = new_data[new_data.CO2 > co2_max][['Room Number', 'Temperature', 'CO2']].sort_values(by='CO2')

# print("\nToo Hot: \n")
warm_data = new_data[new_data.Temperature > temp_max][['Room Number', 'Temperature', 'CO2']].sort_values(by='Temperature')


with open('ahs_cold_data.csv', 'a') as permanent_cold_data:
    temp_writer = csv.writer(permanent_cold_data, delimiter=";")
    current_time = time.asctime()
    for index, row in cold_data.iterrows():
        temp_rn = row['Room Number']
        temp_squared = row['Temperature']
        temp_carbon = row['CO2']
        temp_writer.writerow( ['{0},{1},{2},{3},{4},{5}'.format( current_time, temp_rn, temp_squared, temp_units, temp_carbon, co2_units) ])

with open('ahs_carbon_data.csv', 'a') as permanent_carbon_data:
    temp_writer = csv.writer(permanent_carbon_data, delimiter=";")
    current_time = time.asctime()
    for index, row in carbon_data.iterrows():
        temp_rn = row['Room Number']
        temp_squared = row['Temperature']
        temp_carbon = row['CO2']
        temp_writer.writerow( ['{0},{1},{2},{3},{4},{5}'.format( current_time, temp_rn, temp_squared, temp_units, temp_carbon, co2_units) ])

with open('ahs_warm_data.csv', 'a') as permanent_warm_data:
    temp_writer = csv.writer(permanent_warm_data, delimiter=';')
    current_time = time.asctime()
    for index, row in warm_data.iterrows():
        temp_rn = row['Room Number']
        temp_squared = row['Temperature']
        temp_carbon = row['CO2']
        temp_writer.writerow( ['{0},{1},{2},{3},{4},{5}'.format( current_time, temp_rn, temp_squared, temp_units, temp_carbon, co2_units) ])

# Report elapsed time
elapsed_time = round( ( time.time() - start_time ) * 1000 ) / 1000
print( '\nElapsed time: {0} seconds'.format( elapsed_time ) )
