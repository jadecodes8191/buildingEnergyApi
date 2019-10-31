#to be run every week: takes in data from task three and further aggregates it to get a cohesive weekly report.

import sqlite3
import sqlalchemy
import pandas as pd
import numpy as np

PATH = 'my_file'

engine = sqlalchemy.create_engine('sqlite:///' + PATH)
daily_data = pd.read_sql_table("DailyDatabase", engine)
daily_data['Days With Problems'] = None

daily_data = daily_data.groupby("Room #").agg({"Days With Problems": np.size, "Intervals Too Warm" : np.sum, "Intervals Too Cold" : np.sum, "Intervals Too Much CO2": np.sum})

print(daily_data)
