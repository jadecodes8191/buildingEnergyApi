import datetime
import sqlalchemy
import pandas as pd
import sqlite3
import numpy as np

PATH = 'my_file'

# Reads in databases from tasks 1 and 2
engine = sqlalchemy.create_engine('sqlite:///' + PATH)
all_data = pd.read_sql_table("TempAndCO2Log", engine)
all_data.to_csv("tester.csv")
