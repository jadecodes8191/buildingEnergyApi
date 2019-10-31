#to be run whenever task 1 is run: identifies problem areas for one 15-minute span of data

import pandas as pd
import time
import csv
import sqlalchemy
import sqlite3

PATH = "my_file"

start_time = time.time()

temp_min = 65
temp_units = "deg F"
co2_units = "ppm"
co2_max = 1200
temp_max = 75

engine = sqlalchemy.create_engine('sqlite:///' + PATH)
new_data = pd.read_sql_table("ProblemAreasDatabase", engine)


#new_data = pd.read_csv('', delimiter=",", names=['Time Stamp', 'Room Number', 'Temperature', 'Temperature Units', 'CO2', 'CO2 Units'])
new_data = new_data.sort_values(by='Room #')
print("Full CSV: ")
print(new_data)
# print(new_data[new_data.Location == '270.01'][['Room Number', 'Temperature', 'CO2']])

# now we can print out the values in 3 respective data frames
# print("\nToo Cold: \n")
cold_data = new_data[new_data.Temperature < temp_min][['Room #', 'Temperature', 'CO2']].sort_values(by="Temperature", ascending=True)

conn = sqlite3.connect(PATH)
cold_data.to_sql("ColdDatabase", conn, if_exists='append')

# print("\nToo Much CO2: \n")
carbon_data = new_data[new_data.CO2 > co2_max][['Room #', 'Temperature', 'CO2']].sort_values(by='CO2')

conn = sqlite3.connect(PATH)
carbon_data.to_sql("CarbonDatabase", conn, if_exists='append')

# print("\nToo Hot: \n")
warm_data = new_data[new_data.Temperature > temp_max][['Room #', 'Temperature', 'CO2']].sort_values(by='Temperature')

conn = sqlite3.connect(PATH)
warm_data.to_sql("WarmDatabase", conn, if_exists='append')

# Report elapsed time
elapsed_time = round( ( time.time() - start_time ) * 1000 ) / 1000
print( '\nElapsed time: {0} seconds'.format( elapsed_time ) )
