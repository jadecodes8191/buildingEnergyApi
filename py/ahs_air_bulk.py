# Copyright 2018 Building Energy Gateway.  All rights reserved.

import time
import pandas as pd
from building_data_requests import get_bulk
import numbers
import csv

start_time = time.time()

temp_min = 65
temp_max = 75
co2_max = 1200

# figure out which rooms have consistently high temperatures or high co2 vals CHECK
# define what "hi temp" means CHECK
# > 75 --> don't hard code --> have a constant up front so keeps track/easily adaptable CHECK
# co2 vals is 1200 for a high value CHECK
# minimum temperature should be 65 --> it shouldn't be too cold CHECK
# make a list of all rooms < 65 CHECK
# set up a weebly blog
# start coursera course
# databases eventually -->

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
print( 'Location,Temperature,Temperature Units,CO2,CO2 Units' )

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
        temp_writer.writerow( ['{0},{1},{2},{3},{4}'.format( row['Label'], temp_value, temp_units, co2_value, co2_units ) ])


new_data = pd.read_csv('ahs_air_data.csv', delimiter=",", names=['Location', 'Temperature', 'Temperature Units', 'CO2', 'CO2 Units'])
new_data = new_data.sort_values(by='Temperature')
print(new_data[['Location', 'Temperature', 'CO2']])

print("\nToo Cold: \n")
print(new_data[new_data.Temperature < temp_min][['Location', 'Temperature', 'CO2']])

print("\nToo Much CO2: \n")
print(new_data[new_data.CO2 > co2_max][['Location', 'Temperature', 'CO2']])

print("\nToo Hot: \n")
print(new_data[new_data.Temperature > temp_max][['Location', 'Temperature', 'CO2']])

# Report elapsed time
elapsed_time = round( ( time.time() - start_time ) * 1000 ) / 1000
print( '\nElapsed time: {0} seconds'.format( elapsed_time ) )
