import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


df = pd.read_csv("graph_tester.csv")


print("This is running")
plt.figure()
plt.scatter(df['Intervals Too Warm'], df['Intervals Too Cold'])
plt.show()
# important!^^^


