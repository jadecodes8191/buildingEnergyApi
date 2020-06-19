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
ax.set_xticks(int_list)# the problem with this is the scale
ax.set_xlabel("Time")
ax.set_ylabel("CO2 (ppm)")
if co2_or_temp == 1:
    ax.set_title("CO2 vs. Time in room " + room_number)
else:
    ax.set_title("Temperature vs. Time in room " + room_number)
ax.set_xlim(left=int_list[0], right=int_list[5])
ax.set_xticklabels(new_list)
if co2_or_temp == 1:
    plt.plot(orig_db['Edited Timestamp'], orig_db["CO2"])
else:
    plt.plot(orig_db["Edited Timestamp"], orig_db["Temperature"])
plt.show()

