"""
should be run once a week: clear out daily database and others, or maybe save them to a more permanent database, just to
clear for the next weekly report.
"""
import pandas as pd
import sqlite3

PATH = 'my_file'

conn = sqlite3.connect(PATH)
cursor = conn.cursor()

drop = "DROP TABLE ColdDatabase"
drop2 = "DROP TABLE WarmDatabase"
drop3 = "DROP TABLE CarbonDatabase"

cursor.execute(drop)
cursor.execute(drop2)
cursor.execute(drop3)

conn.close()
