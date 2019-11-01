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

"""
should be run once a week: clear out daily database and others, or maybe save them to a more permanent database, just to
clear for the next weekly report.
"""
import pandas as pd
import sqlite3

PATH = 'my_file'

conn = sqlite3.connect(PATH)
cursor = conn.cursor()

drop = "DROP TABLE DailyDatabase"

cursor.execute(drop)

conn.close()

