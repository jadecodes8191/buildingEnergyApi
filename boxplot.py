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
co2_or_temp = int(input("1 for High CO2, 2 for Warm, 3 for Low CO2, 4 for Cold"))

temp_df = pd.DataFrame()
if co2_or_temp == 1:
    temp_df = high_co2
elif co2_or_temp == 2:
    temp_df = warm
elif co2_or_temp == 3:
    temp_df = low_co2
else:
    temp_df = cold

plt.figure()
temp_df = temp_df.set_index("Room #").drop("Outside Air", errors='ignore')
temp_df = temp_df.drop("Field House NW", errors='ignore')
temp_df = temp_df.drop("Field House NE", errors='ignore')
temp_df = temp_df.drop("Field House SW", errors='ignore')
temp_df = temp_df.drop("Field House SE", errors='ignore')
# TODO: Should we drop Field House numbers?
temp_factor = temp_df.head()
print(temp_factor)
temp_factor = temp_factor.T
i_df_list = []
room_num_list = []
for i in temp_factor:
    i_df = orig_db.set_index("Room #").T[i]
    if co2_or_temp % 2 == 0:
        i_df_list.append(i_df.T['Temperature'])
    else:
        i_df_list.append(i_df.T['CO2'])
    room_num_list.append(i)

print(i_df_list)
ax = plt.axes()
if co2_or_temp == 1:
    ax.set_title("CO2 in 5 rooms w/ highest CO2")
elif co2_or_temp == 2:
    ax.set_title("Temperature in warmest 5 rooms")
elif co2_or_temp == 3:
    ax.set_title("CO2 in 5 rooms w/ lowest CO2")
else:
    ax.set_title("Temperature in coldest 5 rooms")
room_num_list.reverse()
i_df_list.reverse()
print(room_num_list)
ax.set_yticklabels(room_num_list)
plt.boxplot(i_df_list, vert=False)
plt.show()

#try:
    #orig_db = orig_db[orig_db["Room #"] == room_number]
    #print(orig_db)
    #print(orig_db["Timestamp"])
    #orig_db = orig_db.reset_index()
    #first_time = orig_db["Timestamp"][0]
    #orig_db["Edited Timestamp"] = orig_db["Timestamp"].apply(lambda x: int((datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S")).timestamp()))
    #print(first_time)

    #print("This is running")
    #plt.figure()
    #if co2_or_temp == 1:
        #plt.boxplot(orig_db["CO2"])

    #else:
        #plt.boxplot(orig_db["Temperature"])
    #plt.show()

#except IndexError:
    #print("Your room is not in the list, sorry")
