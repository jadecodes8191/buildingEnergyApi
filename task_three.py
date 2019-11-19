#to be run each day: collects problems from task two and aggregates them into daily issue areas.

import datetime
import sqlalchemy
import pandas as pd
import sqlite3
import numpy as np
import csv

PATH = 'my_file'

# Reads in databases from tasks 1 and 2
engine = sqlalchemy.create_engine('sqlite:///' + PATH)
cold_data = pd.read_sql_table("ColdDatabase", engine)
warm_data = pd.read_sql_table("WarmDatabase", engine)
carbon_data = pd.read_sql_table("CarbonDatabase", engine)
low_carbon_data = pd.read_sql_table("LowCarbonDatabase", engine)

# Fixing Times hopefully
#print(cold_data.T.index)
for x in range(0, len(cold_data['Timestamp'])):
    cold_data['Timestamp'].loc[x] = (pd.to_datetime(cold_data['Timestamp'].loc[x]) - datetime.timedelta(0))
for x in range(0, len(warm_data['Timestamp'])):
    warm_data['Timestamp'].loc[x] = (pd.to_datetime(warm_data['Timestamp'].loc[x]) - datetime.timedelta(0))

# Creates copies such that we can do different kinds of analysis on different copies
cold_data_copy = cold_data.copy()
warm_data_copy = warm_data.copy()
carbon_data_copy = carbon_data.copy()
low_carbon_data_copy = low_carbon_data.copy()

#Sets up timestamp piece -- first/last time too cold/warm
cold_data_copy['First Time Too Cold'] = cold_data_copy['Timestamp']
cold_data_copy['Last Time Too Cold'] = cold_data_copy['Timestamp']
warm_data_copy['First Time Too Warm'] = warm_data_copy['Timestamp']
warm_data_copy['Last Time Too Warm'] = warm_data_copy['Timestamp']

# Time Testing
#print(cold_data_copy['First Time Too Cold'])
#print(np.min(cold_data_copy['First Time Too Cold']))  # prints incorrectly across days - nov 9 is min
#print(np.max(cold_data_copy['First Time Too Cold']))  # prints incorrectly across days - nov 5 is max on same trial
#print(np.min(["Sun Nov 10 17:00:00 2019", "Wed Nov 6 16:00:00 2019"]))
# but this only happens when i put it in the pandas format -- in other formats it was calculating correctly

cold_with_times = cold_data_copy.groupby("Room #").agg({'First Time Too Cold' : np.min, 'Last Time Too Cold' : np.max})
warm_with_times = warm_data_copy.groupby("Room #").agg({'First Time Too Warm' : np.min, 'Last Time Too Warm' : np.max})

time_temp_vals = pd.merge(cold_with_times, warm_with_times, how='outer', on=['Room #']).fillna("N/A")

'''
coldest = cold_data.sort_values(by='Temperature', ascending=True).reset_index().iloc[0]
warmest = warm_data.sort_values(by='Temperature', ascending=False).reset_index().iloc[0]
carbonest = carbon_data.sort_values(by='CO2', ascending=False).reset_index().iloc[0]
print(coldest)
print(warmest)
print(carbonest)

nice data to have, but NOT part of the required weekly report.

'''
# ORIGINAL VERSION (used for interval counting):

warm_data['Intervals Too Warm'] = None
cold_data['Intervals Too Cold'] = None
carbon_data['Intervals Too Much CO2'] = None
low_carbon_data['Intervals Too Little CO2'] = None

warm_data = warm_data.groupby("Room #").agg({'Intervals Too Warm' : np.size})
cold_data = cold_data.groupby("Room #").agg({'Intervals Too Cold' : np.size})
carbon_data = carbon_data.groupby("Room #").agg({'Intervals Too Much CO2' : np.size})
low_carbon_data = low_carbon_data.groupby("Room #").agg({'Intervals Too Little CO2' : np.size})

temp_vals = pd.merge(warm_data, cold_data, how='outer', on=['Room #'])
#print(temp_vals)
carbon_vals = pd.merge(carbon_data, low_carbon_data, how='outer', on=['Room #'])

# COPY VERSION (used for high/low temps):

cold_data_copy = cold_data_copy.drop('index', axis=1)
warm_data_copy = warm_data_copy.drop('index', axis=1)

temp_vals_copy = pd.merge(warm_data_copy, cold_data_copy, how='outer', on=['Room #', 'Temperature', 'CO2'])
temp_vals_copy['Highest Temperature'] = temp_vals_copy['Temperature']
temp_vals_copy['Lowest Temperature'] = temp_vals_copy['Temperature']
all_temps = temp_vals_copy.set_index("Room #")['Temperature']

carbon_vals_copy = pd.merge(carbon_data_copy, low_carbon_data_copy, how='outer', on=['Room #', 'Temperature', 'CO2'])
all_carbon = carbon_vals_copy.set_index("Room #")['CO2']

temp_vals_copy = temp_vals_copy.groupby("Room #").agg({'Highest Temperature': np.max, 'Lowest Temperature': np.min})
carbon_vals_copy = carbon_vals_copy.groupby("Room #").agg({'CO2' : np.mean})# This is now simply a placeholder for the groupby -- the mean value per day is never used

most_temp_data = pd.merge(temp_vals, temp_vals_copy, on=['Room #']).fillna(0)
#print(most_temp_data)

all_carbon_data = pd.merge(carbon_vals, carbon_vals_copy, on=['Room #']).fillna(0)
#print(all_carbon_data)

all_temp_data = pd.merge(most_temp_data, time_temp_vals, on=['Room #']).fillna(0)
#print(all_temp_data)

all_data = pd.merge(all_temp_data, all_carbon_data, how='outer', on=['Room #']).fillna(0)
#print(all_data.T.index)  # range of timestamps is 4-7

def convert_datetime(z):
    if type(z) == str:
        return z
    elif type(z) == pd.Timestamp:
        print(datetime.datetime.strftime(z.to_pydatetime(), '%Y-%m-%d %H:%M:%S'))
        return datetime.datetime.strftime(z.to_pydatetime(), '%Y-%m-%d %H:%M:%S')

for x in range(0, len(all_data['First Time Too Cold'])):
    all_data['First Time Too Cold'].iloc[x] = convert_datetime(all_data['First Time Too Cold'].iloc[x])
    all_data['Last Time Too Cold'].iloc[x] = convert_datetime(all_data['Last Time Too Cold'].iloc[x])
    all_data['First Time Too Warm'].iloc[x] = convert_datetime(all_data['First Time Too Warm'].iloc[x])
    all_data['Last Time Too Warm'].iloc[x] = convert_datetime(all_data['Last Time Too Warm'].iloc[x])
    print(all_data['First Time Too Cold'].iloc[x])

conn = sqlite3.connect(PATH)
all_data.to_sql("DailyDatabase", conn, if_exists='append')
all_temps.to_sql("DailyTempDatabase", conn, if_exists='append')
all_carbon.to_sql("DailyCarbonDatabase", conn, if_exists='append')

'''
with open('basic_weekly.csv', 'w') as weekly_df:
    csv_writer = csv.writer(weekly_df, delimiter=";")
    for index, row in all_data.iterrows():
        my_index = index
        temp_warm = row['Intervals Too Warm']
        temp_cold = row['Intervals Too Cold']
        temp_carbon = row['Intervals Too Much CO2']
        csv_writer.writerow(["{0}, {1}, {2}, {3}".format(my_index, temp_warm, temp_cold, temp_carbon)])

'''

# Daily Clear



cursor = conn.cursor()                       
                                          
drop = "DROP TABLE ColdDatabase"             
drop2 = "DROP TABLE WarmDatabase"            
drop3 = "DROP TABLE CarbonDatabase"          
drop4 = "DROP TABLE LowCarbonDatabase"       
                                             
cursor.execute(drop)                         
cursor.execute(drop2)                        
cursor.execute(drop3)                        
cursor.execute(drop4)                        
                                             
conn.close()                                 
                     







