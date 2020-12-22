import sqlite3

import pandas as pd
import numpy as np

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


df = pd.read_csv("metasys_data.csv", error_bad_lines=False, low_memory=False)  # low_memory=False added b/c of potential data type issues
# print(df.head())

rooms = pd.Series(df.T.index)[1:].reset_index(drop=True)
room_nums = rooms.apply(read_room)  # for some reason this adds an extra row at the start so I'm just getting rid of it
# print(room_nums[100:110])
# print("Temp Sensors: ")
is_co2 = rooms.apply(is_co2_sensor)

# this goes into the multiindex now
rooms_plus_sensors = pd.concat([room_nums, is_co2], axis=1)
print("rooms plus sensors")
print(rooms_plus_sensors)
rooms_plus_sensors.to_csv("tester.csv")

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

SERVER_PATH = ''  # '/media/ea/Data/Students/jade/buildingEnergyApi/'
PATH = 'my_file'

engine = sqlalchemy.create_engine('sqlite:///' + SERVER_PATH + PATH)
conn = sqlite3.connect(SERVER_PATH + PATH)
new_df = pd.read_sql("TempAndCO2Log", engine)
new_df.to_csv(SERVER_PATH + "tester.csv")
print(new_df.index)
print(pivot.index)
pivot.to_sql("TempAndCO2Log", conn, if_exists='append')  # actual permanent database
pivot.to_sql("TempAndCO2LogDaily", conn, if_exists='append')  # copy used for tasks 3 and 4 in this branch, must be cleared out every week

