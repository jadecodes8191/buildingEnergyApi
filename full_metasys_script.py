import numpy as np
from datetime import datetime
import datetime as dt
import sys
import time
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.dates as dts
import pandas as pd
import datetime
import sqlite3


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
    if x == "Outside Air CO2" or x == "Outside Air RTU1":
        return "Outside Air"
    else:
        return x


def is_co2_sensor(x):
    # if "CO2" not in x:
        # print("hello")
        # print(x)
    return "CO2" in x or "-Q" in x  # not sure this works 100% of the time


filename = input("Enter file name")

df = pd.read_csv(filename, error_bad_lines=False, low_memory=False)  # low_memory=False added b/c of potential data type issues
df = df.drop(df.tail(2).index)  # removes informational lines at the bottom of the file
df.to_csv("tester.csv")


rooms = pd.Series(df.T.index)[1:].reset_index(drop=True)
room_nums = rooms.apply(read_room)  # for some reason this adds an extra row at the start so I'm just getting rid of it
# print(room_nums[100:110])
# print("Temp Sensors: ")
is_co2 = rooms.apply(is_co2_sensor)

# this goes into the multiindex now
rooms_plus_sensors = pd.concat([room_nums, is_co2], axis=1)
#print("rooms plus sensors")
#print(rooms_plus_sensors)
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
pivot["CO2"] = pivot["CO2"].apply(custom_conv)  # FOUND IT !!

SERVER_PATH = ''  # '/media/ea/Data/Students/jade/buildingEnergyApi/'
PATH = 'my_file'

engine = sqlalchemy.create_engine('sqlite:///' + SERVER_PATH + PATH)
conn = sqlite3.connect(SERVER_PATH + PATH)
# new_df = pd.read_sql("MetasysLog", engine)
pivot.to_csv(SERVER_PATH + "tester.csv")
pivot.to_sql("MetasysLog", conn, if_exists='append')  # actual permanent database
pivot.to_sql("TempAndCO2LogDaily", conn, if_exists='append')  # copy used for tasks 3 and 4 in this branch, must be cleared out every week

#test2 = pd.read_sql("TempAndCO2Log", engine)
#test2.to_csv(SERVER_PATH + "tester.csv")

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

df = pivot
# Outside Air still exists here
df["School Day?"] = None
df["Timestamp"] = df["Timestamp"].apply(pd.to_datetime)

#print(df["Timestamp"][0].date)

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
#print("START DATE: ")
#print(start_date)
last_idx = df["Timestamp"].size - 1
#print(df["Timestamp"][last_idx])
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
    #print("START DATE: ")
    #print(start_date)
    #print("END DATE: ")
    #print(end_date)
    school_calendar[start_date] = (start_date.weekday() < 5)
    start_date += datetime.timedelta(days=1)
# This can and should be modified to include weekdays that are
# days off from school -- for now this is just a placeholder

# filter each day in the database with condition ~ map.get(this.day) == true
#print(datetime.date(df["Timestamp"][0].year, df["Timestamp"][0].month, df["Timestamp"][0].day))

df["School Day?"] = df["Timestamp"].apply(lambda x: school_calendar.get(datetime.date(x.year, x.month, x.day)))

# clean up database post-filter
df_filtered = df.where(df["School Day?"] == True).dropna()
#print(df_filtered.set_index("Room #").loc["Outside Air AHU2 ZN-T"])
df_filtered.to_sql("TempAndCO2LogFiltered", conn, if_exists="replace")
df_filtered.to_csv("weekly.csv")

# Track Elapsed Time
elapsed_time = round((time.time() - start_time) * 1000)/1000
print('\nElapsed time: {0} seconds'.format(elapsed_time))

temp_min = 65
temp_units = "deg F"
co2_units = "ppm"
co2_max = 1200
temp_max = 75

SERVER_PATH = ''  # '/media/ea/Data/Students/jade/buildingEnergyApi/'
PATH = 'my_file'

engine = sqlalchemy.create_engine('sqlite:///' + SERVER_PATH + PATH)
conn = sqlite3.connect(SERVER_PATH + PATH)

start_time = time.time()

# TASK TWO BEGINS HERE: analysis of problem rooms at each interval

df = pd.read_sql("TempAndCO2LogFiltered", engine)
# Outside Air is gone by this point...
#print(df.set_index("Room #").loc["Outside Air AHU2 ZN-T"])

# version with input -- could evolve into an interactive front end. Automation will come
# This is now deprecated: the week start is chosen at task_zero.
# week_start_month = input("Month: (number 1-12)")
# week_start_day = input("Day: (number 1-31)")
# week_start_year = input("Year: ")

# week_start = datetime.strptime(week_start_month + " " + week_start_day + " " + week_start_year + " 10:30:02", "%m %d %Y %H:%M:%S")
# #print(week_start)

# #print(df)
df_test_copy = df.set_index("Timestamp")
df_test_copy["Timestamp"] = pd.to_datetime(df_test_copy.index)
# for i in range(0, len(df_test_copy.index)):
# df_test_copy["New Column"][i] = datetime.strptime(df_test_copy.index[i], "%a %b %d %H:%M:%S %Y")
# #print("Still working...")
df_test_copy = df_test_copy.set_index(["Timestamp", "Room #"])
# #print(week_start)
# #print(datetime(2020, 2, 14, 7, 0, 3))
# #print(df_test_copy.index)
# #print(df_test_copy)
# #print(df_test_copy.loc[str(week_start)])
# the above line works if you add in the desired room # or not - use .loc to get a row

#mi_test = pd.DataFrame(np.array([[3, 2, 1], [4, 5, 5], [7, 48, 9]]), columns=[1, 3, 5])
##print(mi_test)
##print(mi_test.loc[0])
#mi_test = mi_test.set_index([1, 3])
##print(mi_test)

# #print(mi_test[mi_test.index[0]])
# #print(mi_test.loc[(1732, 222)])# produces a key error


# Gets interval data about a certain datetime, and the optional room parameter is passed in
# Not needed yet...
#def get_interval_data(date_time, room=None):
    #if room is None:
        #print(df_test_copy.loc[str(date_time)])  # this works
    #else:
        #print(df_test_copy.loc[(str(date_time), str(room))])  # this also works -- the room data type is a STRING


# get_interval_data(datetime(2020, 2, 14, 7, 0, 3))  # test function call 3/11 -- works perfectly!

# Function defs from task II
def check_temp(x):
    ##print("Start of x:")
    ##print(x)
    if x['Temperature'] > temp_max:
        return True
    return False


def check_carbon(x):
    if x['CO2'] > co2_max:
        return True
    return False
# End of function defs from Task II


new_data = df_test_copy.copy().reset_index()
new_data_copy = new_data.copy()

new_data_copy["Weekday"] = new_data_copy["Timestamp"].apply(lambda x: x.weekday())
new_data_copy.to_csv("basic_weekly.csv")
#co2_min = 350
#fixed this placeholder value

# These drops AREN'T necessary! We are replacing the tables at the start of the loop anyways
# conn.cursor().execute("DROP TABLE TemperatureProblemsDatabase")
# conn.cursor().execute("DROP TABLE CarbonDioxideProblemsDatabase")

filtered_log = pd.read_sql("TempAndCO2LogFiltered", engine)
filtered_log.to_csv("weekly.csv")

# temporarily (0,1) --> should be (0, 5) or (0, # of days)
for i in range(0, 5):

    new_data = new_data_copy[new_data_copy["Weekday"] == i]

    # Beginning of Section Modified from Task II

    # #print("\nToo Cold: \n")
    temp_data = new_data[(new_data['Temperature'] < temp_min) | (new_data['Temperature'] > temp_max)]
    temp_data = temp_data[['Timestamp', 'Room #', 'Temperature', 'CO2']].sort_values(by="Temperature", ascending=True)
    temp_data['High Temp?'] = temp_data.T.apply(check_temp)
    ##print("Temp Data")
    ##print(i)
    ##print(temp_data)
    # temp_data.to_csv("tester.csv")
    # random_testing_copy = temp_data.copy().reset_index()
    # for i in range(temp_data.size):
    #   #print(random_testing_copy.loc[i])
    temp_data.to_sql("TemperatureProblemsDatabase", conn, if_exists='replace')  # should replace, because task three will run on one day of data at a time.

    # #print("\nToo Much CO2: \n")
    #print(new_data[["CO2", "Room #"]])
    tmp = new_data[["CO2", "Room #"]].set_index("Room #")
    #print(tmp.loc["Outside Air AHU2 ZN-T"])
    new_data["Min_CO2"] = None

    def find_min_co2(row):
        tmstmp = row["Timestamp"]
        df1 = new_data.where(new_data["Room #"] == "Outside Air").dropna(how='all')
        df1 = df1.where(df1["Timestamp"] == tmstmp).dropna(how='all')
        # print(df1)
        return df1["CO2"].iloc[0]

    new_data["Min_CO2"] = new_data.apply(find_min_co2, axis=1)
    #print(new_data["Min_CO2"])
    carbon_data = new_data[(new_data.CO2 > co2_max) | (new_data.CO2 < new_data.Min_CO2)][['Timestamp', 'Room #', 'Temperature', 'CO2']].sort_values(by='CO2')
    carbon_data['High Carbon?'] = carbon_data.T.apply(check_carbon)
    carbon_data.to_sql("CarbonDioxideProblemsDatabase", conn, if_exists='replace')  # should replace, because task three will run on one day of data at a time.
    # carbon_data.to_csv("weekly.csv")

    # End of Section Modified from Task II

    # TODO: make a task 3 aggregation here.

    temp_data = pd.read_sql_table("TemperatureProblemsDatabase", engine)  # might need this into the other sql table directly... probably easiest
    temp_data = temp_data.sort_values("Room #")
    temp_data.to_csv(SERVER_PATH + 'tester.csv')
    co2_data = pd.read_sql_table("CarbonDioxideProblemsDatabase", engine)

    weekly_log = new_data.copy().reset_index().drop("level_0", axis=1, errors='ignore')

    # Convert times to integers so that they compare accurately

    for x in range(0, len(temp_data['Timestamp'])):
        temp_data['Timestamp'].loc[x] = (pd.to_datetime(temp_data['Timestamp'].loc[x]) - dt.timedelta(0))
    for x in range(0, len(co2_data['Timestamp'])):
        co2_data['Timestamp'].loc[x] = (pd.to_datetime(co2_data['Timestamp'].loc[x]) - dt.timedelta(0))
    for x in range(0, len(weekly_log['Timestamp'])):
        #print(weekly_log["Timestamp"])
        weekly_log['Timestamp'].loc[x] = (pd.to_datetime(weekly_log['Timestamp'].loc[x]) - dt.timedelta(0))

    time_temp = temp_data.copy().set_index(["Room #", "Temperature"])
    time_co2 = co2_data.copy().set_index(["Room #", "CO2"])
    time_wkly_temp = weekly_log.copy().set_index(["Room #", "Temperature"])
    time_wkly_co2 = weekly_log.copy().set_index(["Room #", "CO2"])

    # Multi-index should identify a room and temp or co2 value uniquely for when we look for the times of h/l values

    td_copy = temp_data.set_index("Room #").T
    cd_copy = co2_data.set_index("Room #").T

    weekly_log['Highest Temperature'] = weekly_log['Temperature']
    weekly_log['Lowest Temperature'] = weekly_log['Temperature']
    weekly_log['Highest CO2'] = weekly_log['CO2']
    weekly_log['Lowest CO2'] = weekly_log['CO2']

    # Groups low/high #s

    weekly_log = weekly_log.groupby("Room #").agg({'Lowest Temperature': np.min,
                                                   'Highest Temperature': np.max,
                                                   'Highest CO2': np.max,
                                                   'Lowest CO2': np.min})

    # weekly_log.to_csv("tester.csv")

    all_data = pd.merge(temp_data, co2_data, how='outer', on=['Room #', "Timestamp", "Temperature", "CO2"]).drop("index_x", axis=1).drop("index_y", axis=1)
    # all_data.to_csv("tester.csv")

    # Finds number of intervals with a given problem for each room

    weekly_log['Intervals Too Cold'] = None
    weekly_log['Intervals Too Warm'] = None
    weekly_log['Intervals Too Much CO2'] = None
    weekly_log['Intervals Too Little CO2'] = None

    for room in td_copy:
        #print("ROOM: ")
        #print(room)
        intervals_temp = td_copy[room].T
        intervals_temp['Intervals'] = None
        if type(intervals_temp) == pd.Series:
            intervals_temp = pd.DataFrame(intervals_temp).T
        intervals_temp = intervals_temp.groupby("High Temp?").agg({"Intervals": np.size})
        #print("Temp Intervals: ")
        #print(intervals_temp)
        if len(intervals_temp) == 1:
            if intervals_temp.index[0] == 0:
                weekly_log['Intervals Too Cold'][room] = (intervals_temp.iloc[0])[0]
            else:
                weekly_log['Intervals Too Warm'][room] = (intervals_temp.iloc[0])[0]
        elif len(intervals_temp) == 2:
            weekly_log['Intervals Too Cold'][room] = (intervals_temp.iloc[0])[0]
            weekly_log['Intervals Too Warm'][room] = (intervals_temp.iloc[1])[0]

    for room in cd_copy:
        #print("ROOM: ")
        #print(room)
        intervals_co2 = cd_copy[room].T
        intervals_co2['Intervals'] = None
        if type(intervals_co2) == pd.Series:
            intervals_co2 = pd.DataFrame(intervals_co2).T
        intervals_co2 = intervals_co2.groupby("High Carbon?").agg({"Intervals": np.size})
        #print("CO2 Intervals: ")
        #print(intervals_co2)
        if len(intervals_co2) == 1:
            if intervals_co2.index[0] == 0:
                weekly_log['Intervals Too Little CO2'][room] = (intervals_co2.iloc[0])[0]
            else:
                weekly_log['Intervals Too Much CO2'][room] = (intervals_co2.iloc[0])[0]
        elif len(intervals_co2) == 2:
            weekly_log['Intervals Too Little CO2'][room] = (intervals_co2.iloc[0])[0]
            weekly_log['Intervals Too Much CO2'][room] = (intervals_co2.iloc[1])[0]

    # go back into time database (copied from original database) and locate timestamps

    weekly_log['First Time Too Cold'] = None
    weekly_log['First Time Too Warm'] = None
    weekly_log['Last Time Too Cold'] = None
    weekly_log['Last Time Too Warm'] = None

    for room in time_temp.index:
        room_number = room[0]
        temp_df = time_temp.loc[room_number]
        temp_df['First Time'] = temp_df['Timestamp']
        temp_df['Last Time'] = temp_df['Timestamp']
        temp_df = temp_df.groupby("High Temp?").agg({"First Time": np.min, "Last Time": np.max})
        early_times = temp_df['First Time']
        if len(early_times) == 1:
            if early_times.index[0] == 0:
                weekly_log['First Time Too Cold'][room_number] = early_times.iloc[0]
            else:
                weekly_log['First Time Too Warm'][room_number] = early_times.iloc[0]
        elif len(early_times) == 2:
            #print(early_times)
            weekly_log['First Time Too Cold'][room_number] = early_times.iloc[0]
            weekly_log['First Time Too Warm'][room_number] = early_times.iloc[1]
            # make sure data is sorted before this happens!!! I think it is sorted because of the groupby
        late_times = temp_df['Last Time']
        if len(late_times) == 1:
            if late_times.index[0] == 0:
                weekly_log['Last Time Too Cold'][room_number] = late_times.iloc[0]
            else:
                weekly_log['Last Time Too Warm'][room_number] = late_times.iloc[0]
        elif len(late_times) == 2:
            #print(late_times)
            weekly_log['Last Time Too Cold'][room_number] = late_times[0]
            weekly_log['Last Time Too Warm'][room_number] = late_times[1]
            # make sure data is sorted before this happens!!! I think it is sorted because of the groupby

    weekly_log['Time of Lowest Temperature'] = None
    weekly_log['Time of Highest Temperature'] = None
    weekly_log['Time of Highest CO2'] = None
    weekly_log['Time of Lowest CO2'] = None
    temp_data['Time of Lowest Temperature'] = None
    temp_data['Time of Highest Temperature'] = None
    co2_data['Time of Lowest CO2'] = None
    co2_data['Time of Highest CO2'] = None


    def convert_datetime(z):
        if type(z) == str:
            return z
        elif type(z) == pd.Timestamp:
            #print(type(datetime.strftime(z.to_pydatetime(), '%Y-%m-%d %H:%M:%S')))
            return datetime.datetime.strftime(z.to_pydatetime(), '%Y-%m-%d %H:%M:%S')


    # finds times of high/low temps on a daily basis... this isn't actually used in the final report but it might be good information to have

    for room in time_wkly_temp.index:
        low_temps = time_wkly_temp.loc[room[0]].loc[weekly_log['Lowest Temperature'][room[0]]]['Timestamp']
        high_temps = time_wkly_temp.loc[room[0]].loc[weekly_log['Highest Temperature'][room[0]]]['Timestamp']
        if type(low_temps) == pd.Series:
            weekly_log['Time of Lowest Temperature'][room[0]] = convert_datetime(low_temps.iloc[0])
        else:
            weekly_log['Time of Lowest Temperature'][room[0]] = convert_datetime(low_temps)
        if type(high_temps) == pd.Series:
            weekly_log['Time of Highest Temperature'][room[0]] = convert_datetime(high_temps.iloc[0])
        else:
            weekly_log['Time of Highest Temperature'][room[0]] = convert_datetime(high_temps)
        temp_data['Time of Lowest Temperature'][room[0]] = weekly_log['Time of Lowest Temperature'][room[0]]
        temp_data['Time of Highest Temperature'][room[0]] = weekly_log['Time of Highest Temperature'][room[0]]

    for room in time_wkly_co2.index:
        low_co2 = time_wkly_co2.loc[room[0]].loc[weekly_log['Lowest CO2'][room[0]]]['Timestamp']
        high_co2 = time_wkly_co2.loc[room[0]].loc[weekly_log['Highest CO2'][room[0]]]['Timestamp']
        if type(low_co2) == pd.Series:
            weekly_log['Time of Lowest CO2'][room[0]] = convert_datetime(low_co2.iloc[0])
        else:
            weekly_log['Time of Lowest CO2'][room[0]] = convert_datetime(low_co2)
        if type(high_co2) == pd.Series:
            weekly_log['Time of Highest CO2'][room[0]] = convert_datetime(high_co2.iloc[0])
        else:
            weekly_log['Time of Highest CO2'][room[0]] = convert_datetime(high_co2)
        co2_data['Time of Lowest CO2'][room[0]] = weekly_log['Time of Lowest CO2'][room[0]]
        co2_data['Time of Highest CO2'][room[0]] = weekly_log['Time of Highest CO2'][room[0]]

    #weekly_log = pd.merge(all_data, weekly_log, how='outer', on=['Room #'])

    # Converts to string so SQL can handle it
    for x in range(0, len(weekly_log['First Time Too Cold'])):
        weekly_log['First Time Too Cold'].iloc[x] = convert_datetime(weekly_log['First Time Too Cold'].iloc[x])
        weekly_log['Last Time Too Cold'].iloc[x] = convert_datetime(weekly_log['Last Time Too Cold'].iloc[x])
        weekly_log['First Time Too Warm'].iloc[x] = convert_datetime(weekly_log['First Time Too Warm'].iloc[x])
        weekly_log['Last Time Too Warm'].iloc[x] = convert_datetime(weekly_log['Last Time Too Warm'].iloc[x])

    for x in range(0, len(time_wkly_temp['Timestamp'])):
        time_wkly_temp['Timestamp'].iloc[x] = convert_datetime(time_wkly_temp['Timestamp'].iloc[x])

    for x in range(0, len(time_wkly_co2['Timestamp'])):
        time_wkly_co2['Timestamp'].iloc[x] = convert_datetime(time_wkly_co2['Timestamp'].iloc[x])

    time_wkly_temp = time_wkly_temp.reset_index()
    time_wkly_co2 = time_wkly_co2.reset_index()
    time_wkly_temp = time_wkly_temp.sort_values('Room #')
    time_wkly_co2 = time_wkly_co2.sort_values('Room #')
    # all_data.to_csv("tester.csv")

    #time_wkly_temp.to_csv("tester.csv")

    # Connect to databases

    conn = sqlite3.connect(SERVER_PATH + PATH)
    all_data.to_sql("FilteredT3Database", conn, if_exists='append')
    time_wkly_temp.to_sql("DailyTempDatabase", conn, if_exists='append')
    #print(time_wkly_temp)
    time_wkly_co2.to_sql("DailyCarbonDatabase", conn, if_exists='append')
    weekly_log.to_sql("DailyDatabase", conn, if_exists='append')

    # Drops aren't necessary:
    # TemperatureProblems and CarbonDioxideProblems DBs are "replaced" at the start of the loop
    # Daily Log is reset to a copy of "new_data" at the start of the loop
    #print("Daily Problems")

# sql_temp_test = pd.read_sql("TemperatureProblemsDatabase", engine)
# sql_co2_test = pd.read_sql("CarbonDioxideProblemsDatabase", engine)
# sql_co2_test.to_csv("weekly.csv")

# TODO: run task 4 on aggregation of daily problem reports

daily_data = pd.read_sql_table("DailyDatabase", engine)
daily_data['Days With Problems'] = None
all_temps = pd.read_sql_table("DailyTempDatabase", engine)
all_carbon = pd.read_sql_table("DailyCarbonDatabase", engine)
days_with_problems = pd.read_sql_table("FilteredT3Database", engine)
days_with_problems = days_with_problems.drop("index", axis=1)
days_with_problems['Day'] = days_with_problems['Timestamp'].apply(lambda z: datetime.datetime.strftime(z, "%Y-%m-%d"))#kept as a string for now just to avoid automatic time assignment
days_with_problems = days_with_problems.set_index(["Room #", "Day"])
days_with_problems["Days With Problems"] = None
days_with_problems = days_with_problems.groupby(level=[0, 1]).agg({"Days With Problems": np.size})
days_with_problems = days_with_problems.groupby(level=0).agg({"Days With Problems": np.size})
days_with_problems.to_csv("ahs_cold_data.csv")

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

#print(daily_data['Last Time Too Cold'])


def none_to_nan(x):
    if x is None:
        return np.NaN
    return x


all_temps['Temperature'] = all_temps['Temperature'].apply(none_to_nan)
all_carbon['CO2'] = all_carbon['CO2'].apply(none_to_nan)

all_temps['Median Temperature'] = all_temps['Temperature']
all_temps['Mean Temperature'] = all_temps['Temperature']
temp_analysis = all_temps.groupby("Room #").agg({"Mean Temperature": np.nanmean,
                                                 "Median Temperature": np.nanmedian})

all_carbon['Median CO2'] = all_carbon['CO2']
all_carbon['Mean CO2'] = all_carbon['CO2']
co2_analysis = all_carbon.groupby("Room #").agg({"Mean CO2": np.mean,
                                                "Median CO2": np.median})

# for some reason, sql was automatically converting all the interval values to bytes... but this reverses it


def convert_to_int(x):
    if x is not None:
        return int.from_bytes(x, sys.byteorder)
    return None


daily_data['Intervals Too Warm'] = daily_data['Intervals Too Warm'].apply(convert_to_int)
daily_data['Intervals Too Cold'] = daily_data['Intervals Too Cold'].apply(convert_to_int)
daily_data['Intervals Too Much CO2'] = daily_data['Intervals Too Much CO2'].apply(convert_to_int)
daily_data['Intervals Too Little CO2'] = daily_data['Intervals Too Little CO2'].apply(convert_to_int)

#print(daily_data['First Time Too Cold'])
#print(daily_data['Last Time Too Cold'])

#print(daily_data.where(daily_data["Room #"] == "Mars"))

#days_with_problems = days_with_problems.groupby("")

daily_data = daily_data.groupby("Room #")

daily_data = daily_data.agg({"Intervals Too Warm": np.sum,
                                               "Intervals Too Cold": np.sum,
                                               "Intervals Too Much CO2": np.sum,
                                               "Intervals Too Little CO2": np.sum,
                                               "Highest Temperature": np.max,
                                               "Lowest Temperature": np.min,
                                               'Highest CO2': np.max,
                                               'Lowest CO2': np.min,
                                               "First Time Too Warm": np.min,
                                               "Last Time Too Warm": np.max,
                                               "First Time Too Cold": np.min,
                                               "Last Time Too Cold": np.max})
daily_data = pd.merge(daily_data, days_with_problems, how='outer', on=['Room #'])

daily_data['Time of Highest Temperature'] = None
daily_data['Time of Lowest Temperature'] = None
daily_data['Time of Highest CO2'] = None
daily_data['Time of Lowest CO2'] = None

# For each room, goes back into the copies to find the times of the most extreme values
for room in daily_data.index:
    if not np.isnan(daily_data['Highest Temperature'][room]):
        # match highest temp to time at which it occurred
        index_tuple = (room, daily_data['Highest Temperature'][room]) # removed cast to int...
        if type(all_temps_copy.loc[index_tuple]) == pd.Series:
            temp_df =(pd.DataFrame(all_temps_copy.loc[index_tuple]).T.sort_values('Timestamp')).T
            daily_data['Time of Highest Temperature'][room] = temp_df.loc['Timestamp'][0]
        else:
            daily_data['Time of Highest Temperature'][room] = all_temps_copy.loc[index_tuple].sort_values('Timestamp').reset_index().iloc[0]['Timestamp']
    if not np.isnan(daily_data['Lowest Temperature'][room]):
        # match lowest temp to time at which it occurred
        index_tuple = (room, int(daily_data['Lowest Temperature'][room]))
        if type(all_temps_copy.loc[index_tuple]) == pd.Series:
            temp_df =(pd.DataFrame(all_temps_copy.loc[index_tuple]).T.sort_values('Timestamp')).T
            daily_data['Time of Lowest Temperature'][room] = temp_df.loc['Timestamp'][0]
        else:
            daily_data['Time of Lowest Temperature'][room] = all_temps_copy.loc[index_tuple].sort_values('Timestamp').reset_index().iloc[0]['Timestamp']
    if not np.isnan(daily_data['Highest CO2'][room]):
        # match highest co2 to time at which it occurred
        index_tuple = (room, int(daily_data['Highest CO2'][room]))
        if type(all_carbon_copy.loc[index_tuple]) == pd.Series:
            temp_df =(pd.DataFrame(all_carbon_copy.loc[index_tuple]).T.sort_values('Timestamp')).T
            daily_data['Time of Highest CO2'][room] = temp_df.loc['Timestamp'][0]
        else:
            daily_data['Time of Highest CO2'][room] = all_carbon_copy.loc[index_tuple].sort_values('Timestamp').reset_index().iloc[0]['Timestamp']
    if not np.isnan(daily_data['Lowest CO2'][room]):
        # match lowest co2 to time at which it occurred
        index_tuple = (room, int(daily_data['Lowest CO2'][room]))
        if type(all_carbon_copy.loc[index_tuple]) == pd.Series:
            temp_df =(pd.DataFrame(all_carbon_copy.loc[index_tuple]).T.sort_values('Timestamp')).T
            daily_data['Time of Lowest CO2'][room] = temp_df.loc['Timestamp'][0]
        else:
            daily_data['Time of Lowest CO2'][room] = all_carbon_copy.loc[index_tuple].sort_values('Timestamp').reset_index().iloc[0]['Timestamp']


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
# daily_data.to_csv('tester.csv')

# NOTE: The old data that was in the Weekly Log table is saved in a table called OldWeeklyLog, fittingly.
# Clearing weekly files

cursor = conn.cursor()
drop1 = "DROP TABLE DailyTempDatabase"
drop2 = "DROP TABLE DailyCarbonDatabase"
drop3 = "DROP TABLE DailyDatabase"
drop4 = "DROP TABLE FilteredT3Database"
cursor.execute(drop1)
cursor.execute(drop2)
cursor.execute(drop3)
cursor.execute(drop4)

SERVER_PATH = ''  # '/media/ea/Data/Students/jade/buildingEnergyApi/'
PATH = 'MetasysWeeklyDatabase'

conn = sqlite3.connect(SERVER_PATH + PATH)
# To add into database for easier searching
start_date = week_start_month + "/" + week_start_day + "/" + week_start_year


# Task 4.5 -- creating the 4 more consolidated sheets
# UPDATE: in the new branch, this task will also separate rooms with sensor issues into their own spreadsheets

original_file = pd.read_excel("output.xlsx")
#original_file.to_csv("tester.csv")
original_file['Likely Sensor Issue?'] = None
original_file["CO2 Sensor Issue?"] = None
original_file["Temperature Sensor Issue?"] = None
# Too little CO2 should probably be combined with this...
for x in range(0, len(original_file["Days With Problems"])):
    original_file["Likely Sensor Issue?"].iloc[x] = original_file["Intervals Too Cold"].iloc[x] > 160 or original_file['Intervals Too Warm'].iloc[x] > 160 or original_file["Intervals Too Much CO2"].iloc[x] > 160 or original_file["Lowest Temperature"].iloc[x] == original_file["Highest Temperature"].iloc[x] or original_file["Lowest CO2"].iloc[x] == original_file["Highest CO2"].iloc[x] or original_file["Intervals Too Little CO2"].iloc[x] > 0
    original_file["CO2 Sensor Issue?"].iloc[x] = original_file["Lowest CO2"].iloc[x] == original_file["Highest CO2"].iloc[x] or original_file["Intervals Too Little CO2"].iloc[x] > 0
    # original_file["Intervals Too Much CO2"].iloc[x] > 160 or
    original_file["Temperature Sensor Issue?"].iloc[x] = original_file["Intervals Too Cold"].iloc[x] > 160 or original_file['Intervals Too Warm'].iloc[x] > 160 or original_file["Lowest Temperature"].iloc[x] == original_file["Highest Temperature"].iloc[x]


# Cold Values
cold_values = original_file[["Room #", "Days With Problems", "Intervals Too Cold", "Lowest Temperature", "Highest Temperature", "Mean Temperature", "Median Temperature", "First Time Too Cold", "Last Time Too Cold", "Time of Highest Temperature", "Time of Lowest Temperature", "Likely Sensor Issue?", "Temperature Sensor Issue?"]]
cold_values = cold_values[cold_values['Intervals Too Cold'] > 0]
cold_values = cold_values[cold_values["Temperature Sensor Issue?"] == False]
cold_values = cold_values.sort_values(by="Intervals Too Cold", ascending=False)
for x in range(0, len(cold_values['Median Temperature'])):
    cold_values['Median Temperature'].iloc[x] = int(cold_values['Median Temperature'].iloc[x])
    cold_values['Mean Temperature'].iloc[x] = int(cold_values['Mean Temperature'].iloc[x])
    for category in ['Time of Highest Temperature', 'Time of Lowest Temperature', "First Time Too Cold", "Last Time Too Cold"]:
        if type(cold_values[category].iloc[x]) == str:
            temp_time = datetime.datetime.strptime(cold_values[category].iloc[x], "%Y-%m-%d %H:%M:%S")
        elif type(cold_values[category].iloc[x] == pd.Timestamp):
            temp_time = cold_values[category].iloc[x]
        cold_values[category].iloc[x] = datetime.datetime.strftime(temp_time, "%a %d %b %Y %H:%M")
#cold_values.to_csv("tester.csv")
cold_values["Week"] = start_date
cold_values.to_excel("cold.xlsx")
cold_values.to_sql("MetasysColdValues", conn, if_exists="append")

# Warm Values
warm_values = original_file[["Room #", "Days With Problems", "Intervals Too Warm", "Lowest Temperature", "Highest Temperature", "Mean Temperature", "Median Temperature", "First Time Too Warm", "Last Time Too Warm", "Time of Highest Temperature", "Time of Lowest Temperature", "Likely Sensor Issue?", "Temperature Sensor Issue?"]]
warm_values = warm_values[warm_values['Intervals Too Warm'] > 0]
warm_values = warm_values[warm_values["Temperature Sensor Issue?"] == False]
warm_values = warm_values.sort_values(by="Intervals Too Warm", ascending=False)
for x in range(0, len(warm_values['Median Temperature'])):
    warm_values['Median Temperature'].iloc[x] = int(warm_values['Median Temperature'].iloc[x])
    warm_values['Mean Temperature'].iloc[x] = int(warm_values['Mean Temperature'].iloc[x])
    for category in ['Time of Highest Temperature', 'Time of Lowest Temperature', "First Time Too Warm", "Last Time Too Warm"]:
        if type(warm_values[category].iloc[x]) == str:
            temp_time = datetime.datetime.strptime(warm_values[category].iloc[x], "%Y-%m-%d %H:%M:%S")
        elif type(warm_values[category].iloc[x] == pd.Timestamp):
            temp_time = warm_values[category].iloc[x]
        warm_values[category].iloc[x] = datetime.datetime.strftime(temp_time, "%a %d %b %Y %H:%M")
warm_values["Week"] = start_date
warm_values.to_csv("weekly.csv")
warm_values.to_excel("warm.xlsx")
warm_values.to_sql("MetasysWarmValues", conn, if_exists="append")

# High CO2 Values
high_co2 = original_file[["Room #", "Days With Problems", "Intervals Too Much CO2", "Lowest CO2", "Highest CO2", "Mean CO2", "Median CO2", "Time of Highest CO2", "Time of Lowest CO2", "Likely Sensor Issue?", "CO2 Sensor Issue?"]]
high_co2 = high_co2[high_co2['Intervals Too Much CO2'] > 0]
high_co2 = high_co2[high_co2["CO2 Sensor Issue?"] == False]
high_co2 = high_co2.sort_values(by="Intervals Too Much CO2", ascending=False)
for x in range(0, len(high_co2['Median CO2'])):
    high_co2['Median CO2'].iloc[x] = int(high_co2['Median CO2'].iloc[x])
    high_co2['Mean CO2'].iloc[x] = int(high_co2['Mean CO2'].iloc[x])
    for category in ['Time of Highest CO2', 'Time of Lowest CO2']:
        if type(high_co2[category].iloc[x]) == str:
            temp_time = datetime.datetime.strptime(high_co2[category].iloc[x], "%Y-%m-%d %H:%M:%S")
        elif type(high_co2[category].iloc[x] == pd.Timestamp):
            temp_time = high_co2[category].iloc[x]
        high_co2[category].iloc[x] = datetime.datetime.strftime(temp_time, "%a %d %b %Y %H:%M")
#high_co2.to_csv("basic_weekly.csv")
high_co2["Week"] = start_date
high_co2.to_excel("high_co2.xlsx")
high_co2.to_sql("MetasysHighCO2Values", conn, if_exists="append")

# SENSOR ISSUE (incl. low co2)
low_co2 = original_file[["Room #", "Intervals Too Warm", "Intervals Too Cold", "Intervals Too Much CO2", "Intervals Too Little CO2", "Lowest CO2", "Highest CO2", "Lowest Temperature", "Highest Temperature", "Likely Sensor Issue?", "CO2 Sensor Issue?", "Temperature Sensor Issue?"]]
low_co2 = low_co2[low_co2["Likely Sensor Issue?"] == True]
low_co2 = low_co2.sort_values(by="Intervals Too Little CO2", ascending=False)
#for x in range(0, len(low_co2['Intervals Too Warm'])):
    # low_co2['Median CO2'].iloc[x] = int(low_co2['Median CO2'].iloc[x])
    # low_co2['Mean CO2'].iloc[x] = int(low_co2['Mean CO2'].iloc[x])
    # low_co2['Median Temperature'].iloc[x] = int(low_co2['Median Temperature'].iloc[x])
    # low_co2['Mean Temperature'].iloc[x] = int(low_co2['Mean Temperature'].iloc[x])
    #for category in ['Time of Highest CO2', 'Time of Lowest CO2', 'Time of Highest Temperature', 'Time of Lowest Temperature', 'First Time Too Cold', 'Last Time Too Cold', 'First Time Too Warm', 'Last Time Too Warm']:
        #if type(low_co2[category].iloc[x]) == str:
            #temp_time = datetime.strptime(low_co2[category].iloc[x], "%Y-%m-%d %H:%M:%S")
        #elif type(low_co2[category].iloc[x] == pd.Timestamp):
            #temp_time = low_co2[category].iloc[x]
        #try:
            #low_co2[category].iloc[x] = datetime.strftime(temp_time, "%a %d %b %Y %H:%M")
        #except ValueError:
            #low_co2[category].iloc[x] = None
low_co2.to_csv("ahs_carbon_data.csv")
low_co2.to_excel("low_co2.xlsx")

# Generates graphs based on user input of which room and issue they would like to see.
# Maybe this program can be run on each item in the "leaderboard" the Facility requested...

df = pd.read_csv("graph_tester.csv")

cold = pd.read_excel("cold.xlsx")
warm = pd.read_excel("warm.xlsx")
high_co2 = pd.read_excel("high_co2.xlsx")
sensor_issues = pd.read_excel("low_co2.xlsx")
real_orig_db = pd.read_csv("basic_weekly.csv")
#print(real_orig_db)

co2_temp_list = [high_co2, cold, warm]
heading_list = ["CO2", "Temperature", "Temperature"]
parenthetical_list = ["(High CO2)", "(Cold)", '(Warm)']

with PdfPages(r'C:\Users\jadaf\Desktop\buildingEnergyApi\graphs.pdf') as export_pdf:

    long_text = "Box Plots: Box plots (also known as box-and-whisker plots) are a way of showing data so that you can see both the range and where the middle part of the data lies. The “box” represents the 25th-75th percentile of data, and the orange line in the middle is the median. The “whiskers” extending from the box lead to the minimum and maximum (excluding outliers), and the outliers are represented by dots outside of the structure. For each room in one of the top 5 categories, a box plot is presented showing either its temperature or its carbon dioxide over the time data was collected. \n\nData Collection Methods: The data shown was exported from temperature and CO2 data from Metasys. It was then filtered to include values from only when school was in session (7am to 3pm Monday through Friday). The visualizations do not include rooms with likely sensor issues (those are in a separate Excel file attached with the report).\n\n Thresholds: High CO2 is 1200 ppm, low CO2 (resulting in a sensor issue flag) is the outside air level at the time. High temperature is 75F, low is 65. CO2 sensors are flagged as having issues only if the value is either below the outside air level or always the same. \n\nDates: This data was logged for the week of "
    first_time = datetime.datetime.strftime(datetime.datetime.strptime(real_orig_db["Timestamp"][0], "%Y-%m-%d %H:%M:%S"), "%B %d, %Y")
    first_time_copy = first_time
    long_text += first_time
    long_text += " to "
    last_time = datetime.datetime.strftime(datetime.datetime.strptime(real_orig_db["Timestamp"][0], "%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=5), "%B %d, %Y")
    long_text += last_time
    long_text += "."
    #print(long_text)
    page1 = plt.figure()
    page1.clf()
    page1.text(0.7, 0.03, "Visualizations by Jade Nair w/ guidance from Kate Connolly", size=4, wrap=True)
    page1.text(0.1, 0.03, "Data logged for week of " + first_time_copy + " to " + last_time, size=4, wrap=True)
    page1.text(0.15, 0.8, "Welcome to the weekly report!", size=20)
    page1.text(0.15, 0.2, long_text, size=8, wrap=True)

    export_pdf.savefig()

    # Box plots should probably come first

    for j in range(3):
        temp_df = co2_temp_list[j].copy()
        orig_db = real_orig_db.copy()
        temp_df = temp_df.set_index("Room #").drop("Outside Air", errors='ignore')
        for insignificant_room in ['Field House NW', "Field House NE", "Field House SW", "Field House SE", "CC Band & Choral ZN1", "CC Entry Hall & Common", "CC Multizone ZN1", "CC Multizone ZN2", "CC Multizone ZN3", "CC Multizone ZN4", "CC Scene Shop", "CC Seating", "CC Stage"]:
            temp_df = temp_df.drop(insignificant_room, errors='ignore')

        temp_factor = temp_df.head(10)
        #print(temp_factor)
        if temp_factor.empty:
            continue
        temp_factor = temp_factor.T
        i_df_list = []
        room_num_list = []
        for i in temp_factor:
            i = str(i)
            tst = orig_db.set_index("Room #")
            ixd = tst.index
            i_df = orig_db.set_index("Room #").T[i]
            if j > 0:
                i_df_list.append(i_df.T['Temperature'])
            else:
                i_df_list.append(i_df.T['CO2'])
            room_num_list.append(i)

        #print(i_df_list)
        page1 = plt.figure()
        fig, ax = plt.subplots()
        ax.set_title(heading_list[j] + " in top " + str(len(i_df_list)) + " rooms w/ issue " + parenthetical_list[j])
        #room_num_list.reverse()
        #i_df_list.reverse()
        # Reverse both lists...
        #print(room_num_list)
        ax.set_xticklabels(room_num_list)
        ax.set_xlabel("Room #")
        if j > 0:
            ax.set_ylabel("Temperature (deg F)")
        else:
            ax.set_ylabel("CO2 (ppm)")

        fig.text(0.7, 0.03, "Visualization by Jade Nair w/ guidance from Kate Connolly", size=4, wrap=True)
        fig.text(0.1, 0.03, "Data logged for week of " + first_time_copy + " to " + last_time, size=4, wrap=True)
        plt.boxplot(i_df_list, vert=True)
        plt.margins(0.2)
        fig.tight_layout()
        export_pdf.savefig()

    for j in range(3):
        orig_db = real_orig_db.copy()

        co2_or_temp = 0
        if j == 0:
            co2_or_temp = 1
        another_df = co2_temp_list[j].set_index("Room #")
        for insignificant_room in ['Field House NW', "Field House NE", "Field House SW", "Field House SE", "CC Band & Choral ZN1", "CC Entry Hall & Common", "CC Multizone ZN1", "CC Multizone ZN2", "CC Multizone ZN3", "CC Multizone ZN4", "CC Scene Shop", "CC Seating", "CC Stage"]:
            another_df = another_df.drop(insignificant_room, errors='ignore')
        another_df = another_df.reset_index()
        room_num_list = another_df.head(10)["Room #"]
        # Top 5 rooms

        for room_number in room_num_list:
            room_number = str(room_number)
            orig_db = real_orig_db.copy().set_index(["Room #"])
            for insignificant_room in ['Field House NW', "Field House NE", "Field House SW", "Field House SE", "CC Band & Choral ZN1", "CC Entry Hall & Common", "CC Multizone ZN1", "CC Multizone ZN2", "CC Multizone ZN3", "CC Multizone ZN4", "CC Scene Shop", "CC Seating", "CC Stage"]:
                orig_db = orig_db.drop(insignificant_room, errors='ignore')
            orig_db = orig_db.reset_index()
            orig_db = orig_db[orig_db["Room #"] == room_number]
            orig_db = orig_db[orig_db["Weekday"] < 5] # TODO: figure this out so incase the user inputs a day that's not Monday, we're not counting weekends!
            #print(orig_db)
            #print(orig_db["Timestamp"])
            orig_db = orig_db.reset_index()
            first_time = orig_db["Timestamp"][0]
            orig_db["Edited Timestamp"] = orig_db["Timestamp"].apply(lambda x: int((datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S")).timestamp()))
            orig_db["Day"] = orig_db["Edited Timestamp"].apply(lambda x: (datetime.datetime.fromtimestamp(x)).date())
            orig_db["Time"] = orig_db["Edited Timestamp"].apply(lambda x: (datetime.datetime.fromtimestamp(x)).time())
            orig_db = orig_db[orig_db["Time"] < datetime.datetime.strptime("15:15", "%H:%M").time()]
           # print(first_time)

            #print("This is running")
            page1 = plt.figure()

            db_list = []
            new_list = []
            int_list = []
            for i in range(10):
                temp = datetime.datetime.strptime(first_time, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(i)
                db_list.append(orig_db[orig_db["Day"] == temp.date()])
                new_list.append(datetime.datetime.strftime(temp, "%Y-%m-%d"))
                int_list.append(int(temp.timestamp()))

            #print("DB LIST")
            #print(db_list[1])
            #print(new_list)
            timestamp_list = ["7:00", "8:00", "9:00", "10:00", "11:00", "12:00", "1:00", "2:00", "3:00"]
            for i in range(len(timestamp_list)):
                timestamp_list[i] = datetime.datetime.strptime(timestamp_list[i], "%H:%M").time()

            fig, (ax1, ax2, ax3, ax4, ax5) = plt.subplots(nrows=1, ncols=5, sharey=True)
            ax_list = [ax1, ax2, ax3, ax4, ax5]
            title_list = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
            for i in range(5):
                ax = ax_list[i]
                temp_db = db_list[i]
                temp_db["Edited Timestamp"] = temp_db["Edited Timestamp"].apply(lambda x : dts.date2num(datetime.datetime.fromtimestamp(x)))

                temp_db = temp_db.sort_values("Edited Timestamp")
                if co2_or_temp == 1:
                    ax.plot_date(temp_db['Edited Timestamp'], temp_db["CO2"], fmt="-")  # line plot probably worked best but like...
                else:
                    ax.plot_date(temp_db["Edited Timestamp"], temp_db["Temperature"], fmt="-")
                #print("X Ticks")
                new_x_tick_list = []
                for x in ax.get_xticks():
                    #print(x)
                    new_x_tick_list.append(datetime.datetime.strftime(datetime.datetime.fromtimestamp(x), "%H:%M"))
                #print(new_x_tick_list)
                ax.set_xticklabels(["7:00", "", "", "", "11:00", "", "", "", "3:00"], fontsize=5)
                ax.set_title(title_list[i])
                if i == 0:
                    if co2_or_temp == 1:
                        ax.set_ylabel("CO2 (ppm)")
                    else:
                        ax.set_ylabel("Temperature (deg F)")
                if i == 2:
                    ax.set_xlabel("Time")

            fig.text(0.7, 0.03, "Visualization by Jade Nair w/ guidance from Kate Connolly", size=4, wrap=True)
            fig.text(0.1, 0.03, "Data logged for week of " + first_time_copy + " to " + last_time, size=4, wrap=True)
            fig.suptitle(heading_list[j] + " in room " + room_number + parenthetical_list[j])
            export_pdf.savefig()
            plt.close()

    # Sensor Issue Table
    temp_df = sensor_issues.copy().T.drop("Unnamed: 0", errors='ignore').T.set_index("Room #")
    temp_df = temp_df.drop("CC Multizone ZN1",errors='ignore')
    for insignificant_room in ['Field House NW', "Field House NE", "Field House SW", "Field House SE", "CC Band & Choral ZN1", "CC Entry Hall & Common", "CC Multizone ZN1", "CC Multizone ZN2", "CC Multizone ZN3", "CC Multizone ZN4", "CC Scene Shop", "CC Seating", "CC Stage", "Outside Air"]:
        temp_df = temp_df.drop(insignificant_room, errors='ignore')
        #print("got here")
    temp_df = temp_df.reset_index()

    def sensor_issue_type(row):
        if row["Temperature Sensor Issue?"] and row["CO2 Sensor Issue?"]:
            return "Both"
        elif row["Temperature Sensor Issue?"]:
            return "Temperature"
        elif row["CO2 Sensor Issue?"]:
            return "CO2"
        return "None"

    temp_df["Sensor Issue Type"] = temp_df.T.apply(sensor_issue_type).T
    temp_df = temp_df.T.drop("Likely Sensor Issue?", errors='ignore').T
    temp_df = temp_df.T.drop("Temperature Sensor Issue?", errors='ignore').T
    temp_df = temp_df.T.drop("CO2 Sensor Issue?", errors='ignore').T

    # TODO: possibly re-sort?

    temp_df["Week"] = start_date
    temp_df.to_excel("SensorIssues.xlsx")
    temp_df.to_sql("MetasysSensorIssues", conn, if_exists="append")

    temp_df = temp_df.set_index("Room #").drop("Outside Air", errors='ignore')
    for insignificant_room in ['Field House NW', "Field House NE", "Field House SW", "Field House SE", "CC Band & Choral ZN1", "CC Entry Hall & Common", "CC Multizone ZN1", "CC Multizone ZN2", "CC Multizone ZN3", "CC Multizone ZN4", "CC Scene Shop", "CC Seating", "CC Stage"]:
        temp_df = temp_df.drop(insignificant_room, errors='ignore')
    temp_df = temp_df.where(temp_df["Sensor Issue Type"] != "Temperature").dropna(how='all')

    for j in range(3):
        orig_db = real_orig_db.copy()
        temp_factor = temp_df.head(10)
        temp_df = temp_df.iloc[len(temp_factor):]
        #print(temp_factor)
        if temp_factor.empty:
            continue
        temp_factor = temp_factor.T
        i_df_list = []
        room_num_list = []
        for i in temp_factor:
            i = str(i)
            tst = orig_db.set_index("Room #")
            ixd = tst.index
            i_df = orig_db.set_index("Room #").T[i]
            i_df_list.append(i_df.T['CO2'])
            room_num_list.append(i)

        print(i_df_list)
        page1 = plt.figure()
        fig, ax = plt.subplots()
        ax.set_title("CO2 values in rooms w/ sensor issues: #" + str(j*10) + "-" + str(j*10 + len(temp_factor)))
        #room_num_list.reverse()
        #i_df_list.reverse()
        # Reverse both lists...
        #print(room_num_list)
        ax.set_xticklabels(room_num_list)
        ax.set_xlabel("Room #")
        ax.set_ylabel("CO2 (ppm)")

        fig.text(0.7, 0.03, "Visualization by Jade Nair w/ guidance from Kate Connolly", size=4, wrap=True)
        fig.text(0.1, 0.03, "Data logged for week of " + first_time_copy + " to " + last_time, size=4, wrap=True)
        plt.boxplot(i_df_list, vert=True)
        plt.margins(0.2)
        fig.tight_layout()
        export_pdf.savefig()

elapsed_time = round((time.time() - start_time) * 1000)/1000
print('\nElapsed time: {0} seconds'.format(elapsed_time))
