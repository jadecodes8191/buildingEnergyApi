import pandas as pd
import time
import csv

start_time = time.time()

temp_max = 75

new_data = pd.read_csv('ahs_air_data.csv', delimiter=",", names=['Time Stamp', 'Room Number', 'Temperature', 'Temperature Units', 'CO2', 'CO2 Units'])
new_data = new_data.sort_values(by='Room Number')
print("Full CSV: ")
print(new_data)
# print(new_data[new_data.Location == '270.01'][['Room Number', 'Temperature', 'CO2']])

# print("\nToo Hot: \n")
warm_data = new_data[new_data.Temperature > temp_max][['Room Number', 'Temperature', 'CO2']].sort_values(by='Temperature')

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
