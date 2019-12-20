# to be run every week: takes in data from task three and further aggregates it to get a cohesive weekly report.

import sqlite3
import sys

import sqlalchemy
import pandas as pd
import numpy as np
import datetime

PATH = 'my_file'

engine = sqlalchemy.create_engine('sqlite:///' + PATH)
daily_data = pd.read_sql_table("DailyDatabase", engine)
daily_data['Days With Problems'] = None
all_temps = pd.read_sql_table("DailyTempDatabase", engine)
all_carbon = pd.read_sql_table("DailyCarbonDatabase", engine)

all_temps_copy = all_temps.set_index(['Room #', 'Temperature'])
all_carbon_copy = all_carbon.set_index(['Room #', 'CO2'])


def convert_back(z):
    if z == "N/A":
        return np.NaN
    elif z is not None:
        return datetime.datetime.strptime(z, "%Y-%m-%d %H:%M:%S").timestamp()
    else:
        return None


for x in range(0, len(daily_data['First Time Too Cold'])):
    daily_data['First Time Too Cold'].loc[x] = convert_back(daily_data['First Time Too Cold'].loc[x])
    daily_data['Last Time Too Cold'].loc[x] = convert_back(daily_data['Last Time Too Cold'].loc[x])
    daily_data['First Time Too Warm'].loc[x] = convert_back(daily_data['First Time Too Warm'].loc[x])
    daily_data['Last Time Too Warm'].loc[x] = convert_back(daily_data['Last Time Too Warm'].loc[x])

print(daily_data['Last Time Too Cold'])


def none_to_nan(x):
    if x is None:
        return np.NaN
    return x


all_temps['Temperature'] = all_temps['Temperature'].apply(none_to_nan)
all_carbon['CO2'] = all_carbon['CO2'].apply(none_to_nan)

all_temps['Median Problematic Temperature'] = all_temps['Temperature']
all_temps['Mean Problematic Temperature'] = all_temps['Temperature']
temp_analysis = all_temps.groupby("Room #").agg({"Mean Problematic Temperature": np.nanmean,
                                                 "Median Problematic Temperature": np.nanmedian})

all_carbon['Median Problematic CO2'] = all_carbon['CO2']
all_carbon['Mean Problematic CO2'] = all_carbon['CO2']
co2_analysis = all_carbon.groupby("Room #").agg({"Mean Problematic CO2": np.mean,
                                                 "Median Problematic CO2": np.median})

# for some reason, sql was automatically converting all the interval values to bytes... but this reverses it


def convert_to_int(x):
    if x is not None:
        return int.from_bytes(x, sys.byteorder)
    return None


daily_data['Intervals Too Warm'] = daily_data['Intervals Too Warm'].apply(convert_to_int)
daily_data['Intervals Too Cold'] = daily_data['Intervals Too Cold'].apply(convert_to_int)
daily_data['Intervals Too Much CO2'] = daily_data['Intervals Too Much CO2'].apply(convert_to_int)
daily_data['Intervals Too Little CO2'] = daily_data['Intervals Too Little CO2'].apply(convert_to_int)

print(daily_data['First Time Too Cold'])
print(daily_data['Last Time Too Cold'])

daily_data = daily_data.groupby("Room #").agg({"Days With Problems": np.size,
                                               "Intervals Too Warm": np.sum,
                                               "Intervals Too Cold": np.sum,
                                               "Intervals Too Much CO2": np.sum,
                                               "Intervals Too Little CO2": np.sum,
                                               "Highest Problematic Temperature": np.max,
                                               "Lowest Problematic Temperature": np.min,
                                               'Highest Problematic CO2': np.max,
                                               'Lowest Problematic CO2': np.min,
                                               "First Time Too Warm": np.min,
                                               "Last Time Too Warm": np.max,
                                               "First Time Too Cold": np.min,
                                               "Last Time Too Cold": np.max})

daily_data['Time of Highest Problematic Temperature'] = None
daily_data['Time of Lowest Problematic Temperature'] = None
daily_data['Time of Highest Problematic CO2'] = None
daily_data['Time of Lowest Problematic CO2'] = None

# For each room, goes back into the copies to find the times of the most extreme values
for room in daily_data.index:
    if not np.isnan(daily_data['Highest Problematic Temperature'][room]):
        # match highest temp to time at which it occured
        index_tuple = (room, int(daily_data['Highest Problematic Temperature'][room]))
        if type(all_temps_copy.loc[index_tuple]) == pd.Series:
            temp_df =(pd.DataFrame(all_temps_copy.loc[index_tuple]).T.sort_values('Timestamp')).T
            daily_data['Time of Highest Problematic Temperature'][room] = temp_df.loc['Timestamp'][0]
        else:
            daily_data['Time of Highest Problematic Temperature'][room] = all_temps_copy.loc[index_tuple].sort_values('Timestamp').reset_index().iloc[0]['Timestamp']
    if not np.isnan(daily_data['Lowest Problematic Temperature'][room]):
        # match lowest temp to time at which it occured
        index_tuple = (room, int(daily_data['Lowest Problematic Temperature'][room]))
        if type(all_temps_copy.loc[index_tuple]) == pd.Series:
            temp_df =(pd.DataFrame(all_temps_copy.loc[index_tuple]).T.sort_values('Timestamp')).T
            daily_data['Time of Lowest Problematic Temperature'][room] = temp_df.loc['Timestamp'][0]
        else:
            daily_data['Time of Lowest Problematic Temperature'][room] = all_temps_copy.loc[index_tuple].sort_values('Timestamp').reset_index().iloc[0]['Timestamp']
    if not np.isnan(daily_data['Highest Problematic CO2'][room]):
        # match highest co2 to time at which it occured
        index_tuple = (room, int(daily_data['Highest Problematic CO2'][room]))
        if type(all_carbon_copy.loc[index_tuple]) == pd.Series:
            temp_df =(pd.DataFrame(all_carbon_copy.loc[index_tuple]).T.sort_values('Timestamp')).T
            daily_data['Time of Highest Problematic CO2'][room] = temp_df.loc['Timestamp'][0]
        else:
            daily_data['Time of Highest Problematic CO2'][room] = all_carbon_copy.loc[index_tuple].sort_values('Timestamp').reset_index().iloc[0]['Timestamp']
    if not np.isnan(daily_data['Lowest Problematic CO2'][room]):
        # match lowest co2 to time at which it occured
        index_tuple = (room, int(daily_data['Lowest Problematic CO2'][room]))
        if type(all_carbon_copy.loc[index_tuple]) == pd.Series:
            temp_df =(pd.DataFrame(all_carbon_copy.loc[index_tuple]).T.sort_values('Timestamp')).T
            daily_data['Time of Lowest Problematic CO2'][room] = temp_df.loc['Timestamp'][0]
        else:
            daily_data['Time of Lowest Problematic CO2'][room] = all_carbon_copy.loc[index_tuple].sort_values('Timestamp').reset_index().iloc[0]['Timestamp']


def make_time_readable(x):
    if (x is not None) and (not np.isnan(x)):
        return datetime.datetime.fromtimestamp(x)
    return None


daily_data["First Time Too Warm"] = daily_data["First Time Too Warm"].apply(make_time_readable)
daily_data["Last Time Too Warm"] = daily_data["Last Time Too Warm"].apply(make_time_readable)
daily_data["First Time Too Cold"] = daily_data["First Time Too Cold"].apply(make_time_readable)
daily_data["Last Time Too Cold"] = daily_data["Last Time Too Cold"].apply(make_time_readable)

daily_data = pd.merge(daily_data, temp_analysis, how='outer', on=['Room #'])
daily_data = pd.merge(daily_data, co2_analysis, how='outer', on=['Room #'])

daily_data.to_excel("output.xlsx")
daily_data.to_csv('tester.csv')

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



