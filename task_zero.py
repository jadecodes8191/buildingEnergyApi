# GOAL: Filter the database to match a "calendar" of days when school is in session
# NOTE: This may not stay task_zero -- it may become integrated into the historical data so as to
#   avoid doing the same 47-second task (changing all the pd.datetimes to strings because SQL
#   doesn't like datetimes) twice

import datetime
import sqlalchemy
import pandas as pd
import sqlite3
import time

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
