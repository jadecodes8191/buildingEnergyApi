import sqlalchemy
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime
import datetime as dt
import sys

# Copyright 2018 Building Energy Gateway.  All rights reserved.

import time
from building_data_requests import get_bulk
import numbers
import csv

temp_min = 65
temp_units = "deg F"
co2_units = "ppm"
co2_max = 1200
temp_max = 75

SERVER_PATH = ''  # '/media/ea/Data/Students/jade/buildingEnergyApi/'
PATH = 'my_file'

engine = sqlalchemy.create_engine('sqlite:///' + SERVER_PATH + PATH)
conn = sqlite3.connect(SERVER_PATH + PATH)

cursor = conn.cursor()
drop1 = "DROP TABLE DailyTempDatabase"
drop2 = "DROP TABLE DailyCarbonDatabase"
drop3 = "DROP TABLE DailyDatabase"
drop4 = "DROP TABLE FilteredT3Database"
cursor.execute(drop1)
cursor.execute(drop2)
cursor.execute(drop3)
cursor.execute(drop4)
