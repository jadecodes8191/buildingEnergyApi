"""
should be run once a week: clear out daily database and others, or maybe save them to a more permanent database, just to
clear for the next weekly report.
"""
import sqlite3

PATH = 'my_file'

conn = sqlite3.connect(PATH)
cursor = conn.cursor()

drop = "DROP TABLE TemperatureProblemsDatabase"
drop2 = "DROP TABLE CarbonDioxideProblemsDatabase"

cursor.execute(drop)
cursor.execute(drop2)

conn.close()
