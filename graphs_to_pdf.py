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
sensor_issues = pd.read_excel("low_co2.xlsx")
real_orig_db = pd.read_csv("basic_weekly.csv")
print(real_orig_db)

co2_temp_list = [cold, high_co2, warm]
heading_list = ["Temperature", "CO2", "Temperature"]
parenthetical_list = ["(Cold)", "(High CO2)", '(Warm)']

with PdfPages(r'C:\Users\jadaf\Desktop\buildingEnergyApi\graphs.pdf') as export_pdf:

    # Stuff here

    long_text = "Box Plots: Box plots (also known as box-and-whisker plots) are a way of showing data so that you can see both the range and where the middle part of the data lies. The “box” represents the 25th-75th percentile of data, and the orange line in the middle is the median. The “whiskers” extending from the box lead to the minimum and maximum (excluding outliers), and the outliers are represented by dots outside of the structure. For each room in one of the top 5 categories, a box plot is presented showing either its temperature or its carbon dioxide over the time data was collected. \n\nData Collection Methods: The data shown was exported from temperature and CO2 data from Metasys. It was then filtered to include values from only when school was in session (7am to 3pm Monday through Friday). The visualizations do not include rooms with likely sensor issues (those are in a separate Excel file attached with the report). \n\nDates: This data was logged for the week of "
    first_time = datetime.datetime.strftime(datetime.datetime.strptime(real_orig_db["Timestamp"][0], "%Y-%m-%d %H:%M:%S"), "%B %d, %Y")
    long_text += first_time
    long_text += " to "
    last_time = datetime.datetime.strftime(datetime.datetime.strptime(real_orig_db["Timestamp"][0], "%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=5), "%B %d, %Y")
    long_text += last_time
    long_text += "."
    print(long_text)
    page1 = plt.figure()
    page1.clf()
    page1.text(0.7, 0.03, "Visualizations by Jade Nair w/ guidance from Kate Connolly", size=4, wrap=True)
    page1.text(0.15, 0.8, "Welcome to the weekly report!", size=20)
    page1.text(0.15, 0.4, long_text, size=8, wrap=True)

    export_pdf.savefig()

    # Box plots should probably come first

    for j in range(3):
        temp_df = co2_temp_list[j].copy()
        orig_db = real_orig_db.copy()
        temp_df = temp_df.set_index("Room #").drop("Outside Air", errors='ignore')
        for insignificant_room in ['Field House NW', "Field House NE", "Field House SW", "Field House SE", "CC Band & Choral ZN1", "CC Entry Hall & Common", "CC Multizone ZN1", "CC Multizone ZN2", "CC Multizone ZN3", "CC Multizone ZN4", "CC Scene Shop", "CC Seating", "CC Stage"]:
            temp_df = temp_df.drop(insignificant_room, errors='ignore')

        temp_factor = temp_df.head(10)
        print(temp_factor)
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
            if j % 2 == 0:
                i_df_list.append(i_df.T['Temperature'])
            else:
                i_df_list.append(i_df.T['CO2'])
            room_num_list.append(i)

        print(i_df_list)
        page1 = plt.figure()
        fig, ax = plt.subplots()
        ax.set_title(heading_list[j] + " in top " + str(len(i_df_list)) + " rooms w/ issue " + parenthetical_list[j])
        #room_num_list.reverse()
        #i_df_list.reverse()
        # Reverse both lists...
        print(room_num_list)
        ax.set_xticklabels(room_num_list)
        ax.set_xlabel("Room #")
        if j % 2 == 0:
            ax.set_ylabel("Temperature (deg F)")
        else:
            ax.set_ylabel("CO2 (ppm)")

        fig.text(0.7, 0.03, "Visualization by Jade Nair w/ guidance from Kate Connolly", size=4, wrap=True)
        plt.boxplot(i_df_list, vert=True)
        plt.margins(0.2)
        fig.tight_layout()
        export_pdf.savefig()

    for j in range(3):
        orig_db = real_orig_db.copy()

        co2_or_temp = (j % 2)
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
            page1 = plt.figure()

            db_list = []
            new_list = []
            int_list = []
            for i in range(10):
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

                temp_db = temp_db.sort_values("Edited Timestamp")
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
                if i == 0:
                    if co2_or_temp == 1:
                        ax.set_ylabel("CO2 (ppm)")
                    else:
                        ax.set_ylabel("Temperature (deg F)")
                if i == 2:
                    ax.set_xlabel("Time")

            fig.text(0.7, 0.03, "Visualization by Jade Nair w/ guidance from Kate Connolly", size=4, wrap=True)
            fig.suptitle(heading_list[j] + " in room " + room_number + parenthetical_list[j])
            export_pdf.savefig()
            plt.close()

    # Sensor Issue Table
    temp_df = sensor_issues.copy().T.drop("Unnamed: 0", errors='ignore').T.set_index("Room #")
    try:
        temp_df = temp_df.drop("CC Multizone ZN1")
    except KeyError:
        print("No cc multizone zn1 ig")
    for insignificant_room in ['Field House NW', "Field House NE", "Field House SW", "Field House SE", "CC Band & Choral ZN1", "CC Entry Hall & Common", "CC Multizone ZN1", "CC Multizone ZN2", "CC Multizone ZN3", "CC Multizone ZN4", "CC Scene Shop", "CC Seating", "CC Stage", "Outside Air"]:
        temp_df = temp_df.drop(insignificant_room, errors='ignore')
        print("got here")
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

    temp_df.to_excel("SensorIssues.xlsx")


