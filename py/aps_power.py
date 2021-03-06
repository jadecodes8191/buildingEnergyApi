# Copyright 2018 Building Energy Gateway.  All rights reserved.

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
df = pd.read_csv( '../csv/aps_power.csv' )
df = df.sort_values(by='Label', ascending=True)

# Iterate over dataframe, getting values for each row
for index, row in df.iterrows():

    # Retrieve data
    value, units = get_value( row['Facility'], row['Meter'] )

    # Prepare to print
    value = int( value ) if isinstance( value, numbers.Number ) else ''
    units = units if units else ''

    # Output result
    print( '{0}: {1} {2}'.format( row['Label'], value, units ) )

# Report elapsed time
elapsed_time = round( ( time.time() - start_time ) * 1000 ) / 1000
print( '\nElapsed time: {0} seconds'.format( elapsed_time ) )
