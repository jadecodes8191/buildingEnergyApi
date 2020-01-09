import pandas as pd
import xlrd

report_table = pd.read_csv("week_1_output.csv") #this would normally be reading an excel file, but this is just for testing purposes
fields = ['Room #', 'Highest Problematic Temperature', 'Lowest Problematic Temperature', 'Mean Problematic Temperature', 'Median Problematic Temperature']

for room in report_table.index:
    for i in range(0, len(fields)):
        print(fields[i] + ": " + str(report_table[fields[i]][room]))
