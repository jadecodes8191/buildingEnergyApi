# Copyright 2018 Building Energy Gateway.  All rights reserved.

# New version: Sorts the facilities in ascending order of power usage :)
# (the old stuff is commented out)

import time
import pandas as pd
from building_data_requests import get_value
import numbers

start_time = time.time()

# Read spreadsheet into a dataframe.
# Each row contains the following:
#   - Label
#   - Facility
#   - Instance ID of electric meter
df = pd.read_csv('ahs_power.csv')
valueLabelsList = [[], [], []]

# Output column headings
# print( 'Feeder,Meter,Units' )

maximum = 0

# Iterate over the rows of the dataframe, getting meter readings for each feeder
for index, row in df.iterrows():
    # Retrieve data
    value, units = get_value( row['Facility'], row['Meter'] )

    # Prepare to print
    value = int( value ) if isinstance( value, numbers.Number ) else ''
    units = units if units else ''

    # if value > maximum:
    #   maximum = value
    valueLabelsList[0].append(value)
    valueLabelsList[1].append(row['Label'])
    valueLabelsList[2].append(value)


#df = df.sort_values(by='Label', ascending=False)
valueLabelsList[0].sort()
# print(valueLabelsList[0])
# print(valueLabelsList[1])
# print(valueLabelsList[2])

print("Sorted: ")

for val in valueLabelsList[0]:
    tempIdx = valueLabelsList[2].index(val)
    temp = str(valueLabelsList[1][tempIdx])
    print(temp)
    print(val)


# for index, row in df.iterrows():
    # Output CSV format
    # print( '{0},{1},{2}'.format( row['Label'], value, units ) )

# Report elapsed time
elapsed_time = round( ( time.time() - start_time ) * 1000 ) / 1000
print( '\nElapsed time: {0} seconds'.format( elapsed_time ) )
