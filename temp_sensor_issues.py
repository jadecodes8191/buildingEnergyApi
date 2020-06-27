from datetime import datetime
import pandas as pd
import numpy as np

# Task 4.5 -- creating the 4 more consolidated sheets
# UPDATE: in the new branch, this task will also separate rooms with sensor issues into their own spreadsheets

original_file = pd.read_excel("output.xlsx")
#original_file.to_csv("tester.csv")
original_file['Likely Sensor Issue?'] = None
# Too little CO2 should probably be combined with this...
for x in range(0, len(original_file["Days With Problems"])):
    original_file["Likely Sensor Issue?"].iloc[x] = original_file["Intervals Too Cold"].iloc[x] > 160 or original_file['Intervals Too Warm'].iloc[x] > 160 or original_file["Intervals Too Much CO2"].iloc[x] > 160 or original_file["Lowest Temperature"].iloc[x] == original_file["Highest Temperature"].iloc[x] or original_file["Lowest CO2"].iloc[x] == original_file["Highest CO2"].iloc[x] or original_file["Intervals Too Little CO2"].iloc[x] > 0

# Cold Values
cold_values = original_file[["Room #", "Days With Problems", "Intervals Too Cold", "Lowest Temperature", "Highest Temperature", "Mean Temperature", "Median Temperature", "First Time Too Cold", "Last Time Too Cold", "Time of Highest Temperature", "Time of Lowest Temperature", "Likely Sensor Issue?"]]
cold_values = cold_values[cold_values['Intervals Too Cold'] > 0]
cold_values = cold_values[cold_values["Likely Sensor Issue?"] == False]
cold_values = cold_values.sort_values(by="Intervals Too Cold", ascending=False)
for x in range(0, len(cold_values['Median Temperature'])):
    cold_values['Median Temperature'].iloc[x] = int(cold_values['Median Temperature'].iloc[x])
    cold_values['Mean Temperature'].iloc[x] = int(cold_values['Mean Temperature'].iloc[x])
    for category in ['Time of Highest Temperature', 'Time of Lowest Temperature', "First Time Too Cold", "Last Time Too Cold"]:
        if type(cold_values[category].iloc[x]) == str:
            temp_time = datetime.strptime(cold_values[category].iloc[x], "%Y-%m-%d %H:%M:%S")
        elif type(cold_values[category].iloc[x] == pd.Timestamp):
            temp_time = cold_values[category].iloc[x]
        cold_values[category].iloc[x] = datetime.strftime(temp_time, "%a %d %b %Y %H:%M")
#cold_values.to_csv("tester.csv")
cold_values.to_excel("cold.xlsx")

# Warm Values
warm_values = original_file[["Room #", "Days With Problems", "Intervals Too Warm", "Lowest Temperature", "Highest Temperature", "Mean Temperature", "Median Temperature", "First Time Too Warm", "Last Time Too Warm", "Time of Highest Temperature", "Time of Lowest Temperature", "Likely Sensor Issue?"]]
warm_values = warm_values[warm_values['Intervals Too Warm'] > 0]
warm_values = warm_values[warm_values["Likely Sensor Issue?"] == False]
warm_values = warm_values.sort_values(by="Intervals Too Warm", ascending=False)
for x in range(0, len(warm_values['Median Temperature'])):
    warm_values['Median Temperature'].iloc[x] = int(warm_values['Median Temperature'].iloc[x])
    warm_values['Mean Temperature'].iloc[x] = int(warm_values['Mean Temperature'].iloc[x])
    for category in ['Time of Highest Temperature', 'Time of Lowest Temperature', "First Time Too Warm", "Last Time Too Warm"]:
        if type(warm_values[category].iloc[x]) == str:
            temp_time = datetime.strptime(warm_values[category].iloc[x], "%Y-%m-%d %H:%M:%S")
        elif type(warm_values[category].iloc[x] == pd.Timestamp):
            temp_time = warm_values[category].iloc[x]
        warm_values[category].iloc[x] = datetime.strftime(temp_time, "%a %d %b %Y %H:%M")
warm_values.to_csv("weekly.csv")
warm_values.to_excel("warm.xlsx")

# High CO2 Values
high_co2 = original_file[["Room #", "Days With Problems", "Intervals Too Much CO2", "Lowest CO2", "Highest CO2", "Mean CO2", "Median CO2", "Time of Highest CO2", "Time of Lowest CO2", "Likely Sensor Issue?"]]
high_co2 = high_co2[high_co2['Intervals Too Much CO2'] > 0]
high_co2 = high_co2[high_co2["Likely Sensor Issue?"] == False]
high_co2 = high_co2.sort_values(by="Intervals Too Much CO2", ascending=False)
for x in range(0, len(high_co2['Median CO2'])):
    high_co2['Median CO2'].iloc[x] = int(high_co2['Median CO2'].iloc[x])
    high_co2['Mean CO2'].iloc[x] = int(high_co2['Mean CO2'].iloc[x])
    for category in ['Time of Highest CO2', 'Time of Lowest CO2']:
        if type(high_co2[category].iloc[x]) == str:
            temp_time = datetime.strptime(high_co2[category].iloc[x], "%Y-%m-%d %H:%M:%S")
        elif type(high_co2[category].iloc[x] == pd.Timestamp):
            temp_time = high_co2[category].iloc[x]
        high_co2[category].iloc[x] = datetime.strftime(temp_time, "%a %d %b %Y %H:%M")
#high_co2.to_csv("basic_weekly.csv")
high_co2.to_excel("high_co2.xlsx")

# Low CO2 Values OR SENSOR ISSUE
low_co2 = original_file.copy()
low_co2 = low_co2[low_co2["Likely Sensor Issue?"] == True]
low_co2 = low_co2.sort_values(by="Intervals Too Little CO2", ascending=False)
for x in range(0, len(low_co2['Median CO2'])):
    low_co2['Median CO2'].iloc[x] = int(low_co2['Median CO2'].iloc[x])
    low_co2['Mean CO2'].iloc[x] = int(low_co2['Mean CO2'].iloc[x])
    low_co2['Median Temperature'].iloc[x] = int(low_co2['Median Temperature'].iloc[x])
    low_co2['Mean Temperature'].iloc[x] = int(low_co2['Mean Temperature'].iloc[x])
    for category in ['Time of Highest CO2', 'Time of Lowest CO2', 'Time of Highest Temperature', 'Time of Lowest Temperature', 'First Time Too Cold', 'Last Time Too Cold', 'First Time Too Warm', 'Last Time Too Warm']:
        if type(low_co2[category].iloc[x]) == str:
            temp_time = datetime.strptime(low_co2[category].iloc[x], "%Y-%m-%d %H:%M:%S")
        elif type(low_co2[category].iloc[x] == pd.Timestamp):
            temp_time = low_co2[category].iloc[x]
        try:
            low_co2[category].iloc[x] = datetime.strftime(temp_time, "%a %d %b %Y %H:%M")
        except ValueError:
            low_co2[category].iloc[x] = None
low_co2.to_csv("ahs_carbon_data.csv")
low_co2.to_excel("low_co2.xlsx")

