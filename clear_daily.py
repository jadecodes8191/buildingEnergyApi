"""
should be run once a week: clear out daily database and others, or maybe save them to a more permanent database, just to
clear for the next weekly report.
"""
import sqlite3

PATH = 'my_file'

conn = sqlite3.connect(PATH)
cursor = conn.cursor()

# Still doesn't work
# drop = "DELETE FROM TempAndCO2Log WHERE CO2='2020 Sept - AHS ZN-T OA-T & CO2.csv'"
drop = "DELETE FROM MetasysLog WHERE 1=1"

cursor.execute(drop)

conn.close()
