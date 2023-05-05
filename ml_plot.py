import pydoop.hdfs as hd
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

with hd.open("/output_mapper/output1/part-00000") as f:
    data =  pd.read_csv(f) # Imported the data set "Data500.csv" file
print(data.shape) # Just showing the data shape(rows and columns)

column_names = data.head()
for col in column_names:  # showing the column names by using a for loop
    print(col)

df = pd.DataFrame(data)
md = df['MD [m]'].values
east = df['East [m]'].values
north = df['North [m]'].values

ax = plt.axes(projection='3d')
ax.scatter3D(east, north, md, c=md, cmap='Greens');