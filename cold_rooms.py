import pandas as pd
import time
import csv

start_time = time.time()

temp_min = 65
temp_units = "deg F"
co2_units = "ppm"

new_data = pd.read_csv('ahs_air_data.csv', delimiter=",", names=['Time Stamp', 'Room Number', 'Temperature', 'Temperature Units', 'CO2', 'CO2 Units'])
new_data = new_data.sort_values(by='Room Number')
print("Full CSV: ")
print(new_data)
# print(new_data[new_data.Location == '270.01'][['Room Number', 'Temperature', 'CO2']])

# now we can print out the values in 3 respective data frames
# print("\nToo Cold: \n")
cold_data = new_data[new_data.Temperature < temp_min][['Room Number', 'Temperature', 'CO2']].sort_values(by="Temperature", ascending=True)


with open('ahs_cold_data.csv', 'a') as permanent_cold_data:
    temp_writer = csv.writer(permanent_cold_data, delimiter=";")
    current_time = time.asctime()
    for index, row in cold_data.iterrows():
        temp_rn = row['Room Number']
        temp_squared = row['Temperature']
        temp_carbon = row['CO2']
        temp_writer.writerow( ['{0},{1},{2},{3},{4},{5}'.format( current_time, temp_rn, temp_squared, temp_units, temp_carbon, co2_units) ])

# Report elapsed time
elapsed_time = round( ( time.time() - start_time ) * 1000 ) / 1000
print( '\nElapsed time: {0} seconds'.format( elapsed_time ) )
