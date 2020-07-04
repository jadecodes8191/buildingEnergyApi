# Generates graphs based on user input of which room and issue they would like to see.
# Maybe this program can be run on each item in the "leaderboard" the Facility requested...

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.dates as dts
import pandas as pd
import datetime
import numpy as np

df = pd.read_csv("graph_tester.csv")

cold = pd.read_excel("cold.xlsx")
warm = pd.read_excel("warm.xlsx")
high_co2 = pd.read_excel("high_co2.xlsx")
low_co2 = pd.read_excel("low_co2.xlsx")
real_orig_db = pd.read_csv("basic_weekly.csv")
print(real_orig_db)

co2_temp_list = [cold, high_co2, warm]
heading_list = ["Temperature", "CO2", "Temperature"]
parenthetical_list = ["(Cold)", "(High CO2)", '(Warm)']

with PdfPages(r'C:\Users\jadaf\Desktop\buildingEnergyApi\graphs.pdf') as export_pdf:

    # Box plots should probably come first

    for j in range(3):
        temp_df = co2_temp_list[j].copy()
        orig_db = real_orig_db.copy()
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
            if j % 2 == 0:
                i_df_list.append(i_df.T['Temperature'])
            else:
                i_df_list.append(i_df.T['CO2'])
            room_num_list.append(i)

        print(i_df_list)
        ax = plt.axes()
        ax.set_title(heading_list[j] + " in top 5 rooms w/ issue " + parenthetical_list[j])
        room_num_list.reverse()
        i_df_list.reverse()
        print(room_num_list)
        ax.set_yticklabels(room_num_list)
        plt.boxplot(i_df_list, vert=False)
        export_pdf.savefig()

    for j in range(3):
        orig_db = real_orig_db.copy()
        co2_or_temp = (j % 2)

        room_num_list = co2_temp_list[j].head()["Room #"]
        # Top 5 rooms

        for room_number in room_num_list:
            orig_db = real_orig_db.copy()

            orig_db = orig_db[orig_db["Room #"] == room_number]
            orig_db = orig_db[orig_db["Weekday"] < 5] # TODO: figure this out so incase the user inputs a day that's not Monday, we're not counting weekends!
            print(orig_db)
            print(orig_db["Timestamp"])
            orig_db = orig_db.reset_index()
            first_time = orig_db["Timestamp"][0]
            orig_db["Edited Timestamp"] = orig_db["Timestamp"].apply(lambda x: int((datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S")).timestamp()))
            orig_db["Day"] = orig_db["Edited Timestamp"].apply(lambda x: (datetime.datetime.fromtimestamp(x)).date())
            orig_db["Time"] = orig_db["Edited Timestamp"].apply(lambda x: (datetime.datetime.fromtimestamp(x)).time())
            orig_db = orig_db[orig_db["Time"] < datetime.datetime.strptime("15:15", "%H:%M").time()]
            print(first_time)

            print("This is running")
            plt.figure()

            db_list = []
            new_list = []
            int_list = []
            for i in range(5):
                temp = datetime.datetime.strptime(first_time, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(i)
                db_list.append(orig_db[orig_db["Day"] == temp.date()])
                new_list.append(datetime.datetime.strftime(temp, "%Y-%m-%d"))
                int_list.append(int(temp.timestamp()))

            print("DB LIST")
            print(db_list[1])
            print(new_list)
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

                if co2_or_temp == 1:
                    ax.plot_date(temp_db['Edited Timestamp'], temp_db["CO2"], fmt="-")  # line plot probably worked best but like...
                else:
                    ax.plot_date(temp_db["Edited Timestamp"], temp_db["Temperature"], fmt="-")
                print("X Ticks")
                new_x_tick_list = []
                for x in ax.get_xticks():
                    print(x)
                    new_x_tick_list.append(datetime.datetime.strftime(datetime.datetime.fromtimestamp(x), "%H:%M"))
                print(new_x_tick_list)
                ax.set_xticklabels(["7:00", "", "", "", "11:00", "", "", "", "3:00"], fontsize=5)
                ax.set_title(title_list[i])

            fig.suptitle(heading_list[j] + " in room " + room_number + parenthetical_list[j])
            export_pdf.savefig()
            plt.close()


