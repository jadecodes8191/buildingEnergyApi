# Building Energy API

This is the home of the code for my work with Energize Andover! (forked from the original Energize repo)

Main files to take a look at:

- problemareascron.py, cold_rooms.py, warm_rooms.py, carbon_rooms.py and all related CSV files
  - This system uses schoolwide sensors to get temperature and CO2 data, which it then writes to a temporary CSV file (ahs_air_data).
  - Next, using three different programs, it reads in the file using Pandas, and based on set values, it determines which rooms have high temperatures, low temperatures, or high carbon dioxide. (each program measures a different factor)
  - Finally, each program writes these rooms and their values to more permanent CSV files (ahs_warm_data, ahs_carbon_data, ahs_cold_data).
  
- basic_weekly_report.py and basic_weekly.csv
  - This is a simple version of a weekly report -- for each room that logs as having problems during the week, this produces the number of intervals at which the room was too warm, too cold, and had too much carbon dioxide throughout the week.
  
- task_one through task_four -- currently in progress
  - This is the more in-depth version of the weekly report, which both makes use of databases and aggregates the data more systematically (first per day, and then per week), while storing the raw data to allow for more flexibility and use cases.
