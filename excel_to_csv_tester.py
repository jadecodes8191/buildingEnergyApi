import pandas as pd
import xlrd

my_excel_file = pd.read_excel("output.xlsx")
my_excel_file.to_csv("graph_tester.csv")
