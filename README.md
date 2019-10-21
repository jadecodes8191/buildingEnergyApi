# Building Energy API

This is the home of the code for my work with Energize Andover! (forked from the original Energize repo)

Main files to take a look at:

- problemareascron.py, cold_rooms.py, warm_rooms.py, carbon_rooms.py and all related CSV files -- 
  - This system uses schoolwide sensors to get temperature and CO2 data, which it then writes to a temporary CSV file (ahs_air_data). 
  - Next, it reads in the file using Pandas, and based on set values, it determines which rooms have high/low temperatures or high carbon dioxide. 
  - Finally, it writes these rooms and their values to more permanent CSV files (ahs_warm_data, ahs_carbon_data, ahs_cold_data), which in this fork are empty.
