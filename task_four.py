# to be run every week: takes in data from task three and further aggregates it to get a cohesive weekly report.

import sqlite3

import sqlalchemy
import pandas as pd
import numpy as np
import datetime

PATH = 'my_file'

engine = sqlalchemy.create_engine('sqlite:///' + PATH)
daily_data = pd.read_sql_table("DailyDatabase", engine)
daily_data['Days With Problems'] = None
all_temps = pd.read_sql_table("DailyTempDatabase", engine)
all_temps.to_csv("weekly.csv")
all_carbon = pd.read_sql_table("DailyCarbonDatabase", engine)

all_temps_copy = all_temps.set_index(['Room #', 'Temperature'])
all_carbon_copy = all_carbon.set_index(['Room #', 'CO2'])


def convert_back(z):
    if z == "N/A":
        return np.NaN
    elif z is not None:
        return datetime.datetime.strptime(z, "%Y-%m-%d %H:%M:%S")
    else:
        return None


for x in range(0, len(daily_data['First Time Too Cold'])):
    daily_data['First Time Too Cold'].loc[x] = convert_back(daily_data['First Time Too Cold'].loc[x])
    daily_data['Last Time Too Cold'].loc[x] = convert_back(daily_data['Last Time Too Cold'].loc[x])
    daily_data['First Time Too Warm'].loc[x] = convert_back(daily_data['First Time Too Warm'].loc[x])
    daily_data['Last Time Too Warm'].loc[x] = convert_back(daily_data['Last Time Too Warm'].loc[x])

all_temps['Median Temperature'] = all_temps['Temperature']
all_temps['Mean Temperature'] = all_temps['Temperature']
temp_analysis = all_temps.groupby("Room #").agg({"Mean Temperature": np.mean,
                                                 "Median Temperature": np.median})

all_carbon['Median CO2'] = all_carbon['CO2']
all_carbon['Mean CO2'] = all_carbon['CO2']
co2_analysis = all_carbon.groupby("Room #").agg({"Mean CO2": np.mean,
                                                 "Median CO2": np.median})

'''
# Time Testing
my_timestamps = ['2019-11-14 00:00:00', '2019-11-14 00:01:02', '2019-11-13 00:00:05', '2019-11-12 10:23:07']
for x in range(0, len(my_timestamps)):
    my_timestamps[x] = (pd.to_datetime(my_timestamps[x]) - datetime.timedelta(0))
print(np.min(my_timestamps))
print(np.max(my_timestamps))
# End of Time Testing - produces accurate result with extra functionality, won't work without some conversions

'''

daily_data = daily_data.groupby("Room #").agg({"Days With Problems": np.size,
                                               "Intervals Too Warm" : np.sum,
                                               "Intervals Too Cold" : np.sum,
                                               "Intervals Too Much CO2": np.sum,
                                               "Intervals Too Little CO2": np.sum,
                                               "Highest Temperature" : np.max,
                                               "Lowest Temperature" : np.min,
                                               'Highest CO2':np.max,
                                               'Lowest CO2':np.min,
                                               "First Time Too Warm" : np.min,
                                               "Last Time Too Warm" : np.max,
                                               "First Time Too Cold" : np.min,
                                               "Last Time Too Cold" : np.max})

daily_data['Time of Highest Temp'] = None
daily_data['Time of Lowest Temp'] = None
daily_data['Time of Highest CO2'] = None
daily_data['Time of Lowest CO2'] = None

for room in daily_data.index:
    if not np.isnan(daily_data['Highest Temperature'][room]):
        # match highest temp to time at which it occured
        index_tuple = (room, int(daily_data['Highest Temperature'][room]))
        daily_data['Time of Highest Temp'][room] = all_temps_copy.loc[index_tuple].sort_values('Timestamp').reset_index().iloc[0]['Timestamp']
    if not np.isnan(daily_data['Lowest Temperature'][room]):
        # match lowest temp to time at which it occured
        index_tuple = (room, int(daily_data['Lowest Temperature'][room]))
        daily_data['Time of Lowest Temp'][room] = all_temps_copy.loc[index_tuple].sort_values('Timestamp').reset_index().iloc[0]['Timestamp']
    if not np.isnan(daily_data['Highest CO2'][room]):
        # match highest co2 to time at which it occured
        index_tuple = (room, int(daily_data['Highest CO2'][room]))
        daily_data['Time of Highest CO2'][room] = all_carbon_copy.loc[index_tuple].sort_values('Timestamp').reset_index().iloc[0]['Timestamp']
    if not np.isnan(daily_data['Lowest CO2'][room]):
        # match lowest co2 to time at which it occured
        index_tuple = (room, int(daily_data['Lowest CO2'][room]))
        daily_data['Time of Lowest CO2'][room] = all_carbon_copy.loc[index_tuple].sort_values('Timestamp').reset_index().iloc[0]['Timestamp']

daily_data = pd.merge(daily_data, temp_analysis, how='outer', on=['Room #'])
daily_data = pd.merge(daily_data, co2_analysis, how='outer', on=['Room #'])

daily_data.to_excel("output.xlsx")
daily_data.to_csv("tester.csv")

"""
should be run once a week: clear out daily database and others, or maybe save them to a more permanent database, just to
clear for the next weekly report.

Commented out for testing: Should add back in after testing

PATH = 'my_file'

conn = sqlite3.connect(PATH)
cursor = conn.cursor()

drop = "DROP TABLE DailyDatabase"
drop2 = "DROP TABLE DailyTempDatabase"
drop3 = "DROP TABLE DailyCarbonDatabase"

cursor.execute(drop)
cursor.execute(drop2)
cursor.execute(drop3)

conn.close()
"""


