#to be run every week: takes in data from task three and further aggregates it to get a cohesive weekly report.

import sqlite3
import sqlalchemy
import pandas as pd
import numpy as np

PATH = 'my_file'

engine = sqlalchemy.create_engine('sqlite:///' + PATH)
daily_data = pd.read_sql_table("DailyDatabase", engine)
daily_data['Days With Problems'] = None

print(daily_data)

daily_data = daily_data.groupby("Room #").agg({"Days With Problems": np.size,
                                               "Intervals Too Warm" : np.sum,
                                               "Intervals Too Cold" : np.sum,
                                               "Intervals Too Much CO2": np.sum,
                                               "Highest Temperature" : np.max,
                                               "Temperature" : np.mean,
                                               "Median Temperature" : np.median,
                                               "Lowest Temperature" : np.min,
                                               "CO2" : np.mean,
                                               "First Time Too Warm" : np.min,
                                               "Last Time Too Warm" : np.max,
                                               "First Time Too Cold" : np.min,
                                               "Last Time Too Cold" : np.max})

"""
should be run once a week: clear out daily database and others, or maybe save them to a more permanent database, just to
clear for the next weekly report.
"""

'''
PATH = 'my_file'

conn = sqlite3.connect(PATH)
cursor = conn.cursor()

drop = "DROP TABLE DailyDatabase"

cursor.execute(drop)

conn.close()

'''

