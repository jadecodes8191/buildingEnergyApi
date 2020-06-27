# Generates graphs based on user input of which room and issue they would like to see.
# Maybe this program can be run on each item in the "leaderboard" the Facility requested...

import matplotlib.pyplot as plt
import pandas as pd
import datetime
import numpy as np

df = pd.read_csv("graph_tester.csv")

cold = pd.read_excel("cold.xlsx")
warm = pd.read_excel("warm.xlsx")
high_co2 = pd.read_excel("high_co2.xlsx")
low_co2 = pd.read_excel("low_co2.xlsx")
orig_db = pd.read_csv("basic_weekly.csv")
print(orig_db)
room_number = input("What room?")
co2_or_temp = int(input("1 for CO2, 2 for Temp"))

try:

    orig_db = orig_db[orig_db["Room #"] == room_number]
    print(orig_db)
    print(orig_db["Timestamp"])
    orig_db = orig_db.reset_index()
    first_time = orig_db["Timestamp"][0]
    orig_db["Edited Timestamp"] = orig_db["Timestamp"].apply(lambda x: int((datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S")).timestamp()))
    print(first_time)

    print("This is running")
    plt.figure()

    new_list = []
    int_list = []
    for i in range(6):
        temp = datetime.datetime.strptime(first_time, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(i)
        new_list.append(datetime.datetime.strftime(temp, "%Y-%m-%d"))
        int_list.append(int(temp.timestamp()))

    ax = plt.axes()
    ax.set_xticks(int_list)
    ax.set_xlabel("Time")
    ax.set_ylabel("CO2 (ppm)")
    if co2_or_temp == 1:
        ax.set_title("CO2 vs. Time in room " + room_number)
        ax.set_ylim(0, 2000)  # should there be y-limits here or no?
    else:
        ax.set_title("Temperature vs. Time in room " + room_number)
        ax.set_ylim(0, 90)  # should there be y-limits here or no?
    ax.set_xlim(left=int_list[0], right=int_list[5])
    ax.set_xticklabels(new_list)
    if co2_or_temp == 1:
        plt.scatter(orig_db['Edited Timestamp'], orig_db["CO2"])  # line plot probably worked best but like...
    else:
        plt.scatter(orig_db["Edited Timestamp"], orig_db["Temperature"])
    plt.show()

except IndexError:
    print("Your room is not in the list, sorry")

# Scatter plot over time is good to see just one room
# End goal/result = dashboard
# !!box-and-whisker plots by room for a 1-d kind of view!!
# sensor issues --> 1 category? same numbers over time/165 intervals with problems/low co2 probably
# actually low/high co2/temperature
# bar charts for different groups
# 5 subplots with each day
# add into Word/PDF


# TODO: categorize sensor issues based on tells such as 165 problem intervals, the same numbers over time, and low co2

# TODO: turn weekly plots above into 5 daily subplots

# TODO: figure out how to add into a word doc/pdf

# TODO: update co2 minimum based on outside air values

# TODO: update task 0 to specify parameters
