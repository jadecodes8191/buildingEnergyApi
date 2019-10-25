# Building Energy API

This is the home of the code for my work with Energize Andover! (forked from the original Energize repo)

Main files to take a look at:

- problemareascron.py, cold_rooms.py, warm_rooms.py, carbon_rooms.py and all related CSV files -- 
  - This system uses schoolwide sensors to get temperature and CO2 data, which it then writes to a temporary CSV file (ahs_air_data).
  - Next, using three different programs, it reads in the file using Pandas, and based on set values, it determines which rooms have high temperatures, low temperatures, or high carbon dioxide. (each program measures a different factor)
  - Finally, each program writes these rooms and their values to more permanent CSV files (ahs_warm_data, ahs_carbon_data, ahs_cold_data).
  
- weekly_report.py and weekly.csv -- currently in progress.
  - This takes in the data logged to the three CSV files (ahs_cold_data, ahs_carbon_data, and ahs_warm_data) and creates a simple weekly report out of it. It states the average temperature difference and CO2 difference from the norm for each problematic room (but only on days when they were logged as being problematic). It should deal with all possible cases -- except when a room logs as being too warm at one time during the week and too cold at another.
  - In the future, it will deal with this last case, calculate averages for every day (including all the 0's for when a room did not register as having issues), as well as possibly deliver a more comprehensive report with more data, including factors like time of day.
