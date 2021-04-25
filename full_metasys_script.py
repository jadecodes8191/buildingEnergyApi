import sqlite3

import pandas as pd
import numpy as np

import datetime
import sqlalchemy
import pandas as pd
import sqlite3
import time

# metasys data format:
# each row is a timestamp and each column is a sensor (each sensor either measures temperature or co2 for one room)

# goal data format:
# each row is a timestamp x room number, columns are temp and co2 values for that timestamp and room

# 1. parse room numbers from column headers & maybe match room numbers to each other --> it's always RM something except if it's the caf, so we'd remove the "RM"
# 2. we should be able to tell whether the column in question is temperature or co2 -- looks like it's marked with a -T or a CO2 (no dash)
# 3. move the room number factor from a row to a column/reorganize the data into the goal format

# basically, reading column headers to group data
import sqlalchemy


def read_room(x):
    if "RM" in x:
        rm = x.split(' ')[0]
        return rm[2:]
    else:
        return x


def is_co2_sensor(x):
    # if "CO2" not in x:
        # print("hello")
        # print(x)
    return "CO2" in x or "-Q" in x  # not sure this works 100% of the time


df = pd.read_csv("2021-Q1-IAQ-AHS-Temp-CO2.csv", error_bad_lines=False, low_memory=False)  # low_memory=False added b/c of potential data type issues
df = df.drop(df.tail(2).index) # removes informational lines at the bottom of the file
df.to_csv("tester.csv")


rooms = pd.Series(df.T.index)[1:].reset_index(drop=True)
room_nums = rooms.apply(read_room)  # for some reason this adds an extra row at the start so I'm just getting rid of it
# print(room_nums[100:110])
# print("Temp Sensors: ")
is_co2 = rooms.apply(is_co2_sensor)

# this goes into the multiindex now
rooms_plus_sensors = pd.concat([room_nums, is_co2], axis=1)
print("rooms plus sensors")
print(rooms_plus_sensors)
#rooms_plus_sensors.to_csv("tester.csv")

# save a transposed copy of df so that we can index by rooms
# print("End of temp sensors")
# print(df.columns)
transposed = df.set_index("Unnamed: 0").T
transposed = transposed.reset_index()
transposed.insert(1, "Room Number", room_nums, True)
transposed.insert(2, "CO2 Sensor?", is_co2, True)
# transposed.to_csv("new_tester.csv")

# print("CO2" in "Cafe UV01 ZN08 Q CO2")

# my_fake_df = pd.DataFrame()
# my_fake_df.insert(0, "Room Number", room_nums, True)
# my_fake_df.to_csv("new_tester.csv")

transposed = transposed.sort_values("CO2 Sensor?")
transposed = transposed.sort_values("Room Number")
transposed = transposed.reset_index().drop("index", axis=1).drop("level_0", axis=1)

# Final stage of modifying data
pivot = transposed.melt(id_vars=["Room Number", "CO2 Sensor?"], var_name="Timestamp", value_name="Value")
pivot = pivot.set_index(["Room Number", "Timestamp"])
pivot = pd.pivot_table(pivot, index=["Room Number", "Timestamp"], values="Value", columns=["CO2 Sensor?"], aggfunc='first')
pivot.columns = ["Temperature", "CO2"]
temp_units = ["deg F"]*len(pivot.axes[0])
co2_units = ["ppm"]*len(pivot.axes[0])
pivot["Temp Units"] = temp_units
pivot["CO2 Units"] = co2_units
pivot = pivot.reset_index()
pivot = pivot.rename(columns={"Room Number": "Room #", "Temp Units": "Temp. Units"})
pivot = pivot.set_index("Room #")


def custom_conv(x):
    if type(x) != float or not np.isnan(x):
        return int(round(float(x)))
    return x


pivot["Temperature"] = pivot["Temperature"].apply(custom_conv)
pivot["CO2"] = pivot["CO2"].apply(custom_conv) # FOUND IT !!

SERVER_PATH = ''  # '/media/ea/Data/Students/jade/buildingEnergyApi/'
PATH = 'my_file'

engine = sqlalchemy.create_engine('sqlite:///' + SERVER_PATH + PATH)
conn = sqlite3.connect(SERVER_PATH + PATH)
# new_df = pd.read_sql("MetasysLog", engine)
pivot.to_csv(SERVER_PATH + "tester.csv")

pivot.to_sql("MetasysLog", conn, if_exists='append')  # actual permanent database
pivot.to_sql("TempAndCO2LogDaily", conn, if_exists='append')  # copy used for tasks 3 and 4 in this branch, must be cleared out every week

test2 = pd.read_sql("TempAndCO2Log", engine)
test2.to_csv(SERVER_PATH + "tester.csv")

# GOAL: Filter the database to match a "calendar" of days when school is in session
# NOTE: This may not stay task_zero -- it may become integrated into the historical data so as to
#   avoid doing the same 47-second task (changing all the pd.datetimes to strings because SQL
#   doesn't like datetimes) twice


start_time = time.time()

SERVER_PATH = ''#'/media/ea/Data/Students/jade/buildingEnergyApi/'
PATH = 'my_file'

engine = sqlalchemy.create_engine('sqlite:///' + SERVER_PATH + PATH)
conn = sqlite3.connect(SERVER_PATH + PATH)

# create a dictionary for the school calendar ({timestamp date: boolean})

df = pd.read_sql("MetasysLog", engine)
df["School Day?"] = None
df["Timestamp"] = df["Timestamp"].apply(pd.to_datetime)

print(df["Timestamp"][0].date)

school_calendar = {}

'''print(datetime.datetime.now())
print(type(datetime.datetime.now()))
#print(datetime.datetime.now().year())
d = datetime.datetime.now()
print(d.year)'''

# start_date = datetime.date(datetime.datetime.now().year, 1, 1)
# Above is a simplified version -- starting on January 1st of the current year.
# Obviously, this will not allow for examining of data from past years.

# Below is a more complicated version that starts from the date of the first entry in the permanent database,
# then looping through each day from the start of logging to the last day logged.

week_start_month = input("Month: (number 1-12)")
week_start_day = input("Day: (number 1-31)")
week_start_year = input("Year: ")
start_date = datetime.datetime.strptime(week_start_month + " " + week_start_day + " " + week_start_year, "%m %d %Y")
start_date = datetime.date(start_date.year, start_date.month, start_date.day)
# print(week_start)

# start_date = datetime.date(df["Timestamp"][0].year, df["Timestamp"][0].month, df["Timestamp"][0].day) - datetime.timedelta(days=1)
print("START DATE: ")
print(start_date)
last_idx = df["Timestamp"].size - 1
print(df["Timestamp"][last_idx])
# end_date = datetime.date(df["Timestamp"][last_idx].year, df["Timestamp"][last_idx].month, df["Timestamp"][last_idx].day)
end_date = start_date + datetime.timedelta(days=7)
# df["School Day?"] = df["Timestamp"].apply(lambda x: x.weekday() < 5)
# This is a basic version -- need to add in actual school calendar including days off and then
# do some sort of lambda x : some map.get(x) == true

# A slightly more complex version which actually uses a dictionary

# Basic version of the loop used a for loop w/ 365, restricting the calendar to the current year.
# The more complex version of the loop is a while loop that waits for the start date to catch up to the
# last date data was logged.
# the end date is reduced by 1 because it's immediately incremented and because there's no guarantee
# that the day before the last day logged will have data, so you could miss out on the last day if you
# just used <
while start_date < end_date:
    print("START DATE: ")
    print(start_date)
    print("END DATE: ")
    print(end_date)
    school_calendar[start_date] = (start_date.weekday() < 5)
    start_date += datetime.timedelta(days=1)
# This can and should be modified to include weekdays that are
# days off from school -- for now this is just a placeholder

# filter each day in the database with condition ~ map.get(this.day) == true
print(datetime.date(df["Timestamp"][0].year, df["Timestamp"][0].month, df["Timestamp"][0].day))

df["School Day?"] = df["Timestamp"].apply(lambda x: school_calendar.get(datetime.date(x.year, x.month, x.day)))
print(df["School Day?"])

# clean up database post-filter
df_filtered = df.where(df["School Day?"] == True).dropna()
df_filtered.to_sql("TempAndCO2LogFiltered", conn, if_exists="replace")
df_filtered.to_csv("weekly.csv")

# Track Elapsed Time
elapsed_time = round((time.time() - start_time) * 1000)/1000
print('\nElapsed time: {0} seconds'.format(elapsed_time))

