# Building Energy API + Classroom Climate Reporting Engine

This is the home of the code for my work with Energize Andover! (forked from the original Energize repo)

The file containing all of the code for the current version is full_metasys_script.py. This application can process any week's worth of CO2 and temperature data and create a report of rooms with air flow problems or sensor issues. The data is first pulled from Metasys and preprocessed into a raw spreadsheet format by Energize Andover's API. My code synthesizes the week's worth of data for each room, determines which rooms are problematic, and produces comprehensive visualizations and spreadsheets depicting those rooms in order to achieve a clearer understanding of climate issues in the school building.

Here are some other files to take a look at, which display the steps I took towards having a full reporting engine ready to go:

- problemareascron.py, cold_rooms.py, warm_rooms.py, carbon_rooms.py 
  - This system uses schoolwide sensors to get temperature and CO2 data, which it then writes to a temporary CSV file (ahs_air_data).
  - Next, using three different programs, it reads in the file using Pandas, and based on set values, it determines which rooms have high temperatures, low temperatures, or high carbon dioxide. (each program measures a different factor)
  - Finally, each program writes these rooms and their values to more permanent CSV files (ahs_warm_data, ahs_carbon_data, ahs_cold_data).
  
- basic_weekly_report.py 
  - This generates a very simple version of a weekly report -- for each room that logs as having problems during the week, this produces the number of intervals at which the room was too warm, too cold, and had too much carbon dioxide throughout the week.
  
- task_one_and_two, task_three, task_four, task_four_and_a_half, and task_five
  - This is the more in-depth version of the weekly report, which both makes use of databases and aggregates the data more systematically (first per day, and then per week), while storing the raw data to allow for more flexibility and use cases. It includes additional data, such as the mean and median temperature and carbon dioxide, as well as the first and last times certain issues were recorded for a room. These values are then consolidated into four separate spreadsheets for maximum readability. It should then send off the final weekly report (as an Excel file) to a mailing list.
  
- Next, switch to the historical-data-report branch to see the version I'm currently working on! Files of interest here are fifteen_min_log.py, task_zero.py, generate_historical_report.py, and graphs_to_pdf.py 
  - This version is a lot more streamlined towards the final product - it allows the user to generate a report from any week stored within the database immediately, rather than waiting 5 days for the next report.

- Finally, the file I've most recently been updating is convert_metasys_data.py, which basically will allow the software to formulate reports from Metasys data. This produces a report which checks out correctly with test data from Metasys.


