#-------------class1------------------#
import pandas as pd
c=pd.openfile("path")
c --->will print the sheet
c['column name'].max() ---> max of the value
c['first column'][c['colomn']=='compare string']
c['Current Salary'].max()[s['Birth Date']]
c['column name'].mean()
c.filna(0,inplace=True)---> replace null with 0

#------------class2-------------------#
import pandas as pd
c=pd.read_csv('C:\\Users\\abrahamv\\Downloads\\ER-5537\\Graph.csv')
df=c[1:30]
df.index()
df.columns()
df.fillna(0,inplace=True)
df.columns
df.head
df.tail
df.head(5)
df.columns
df.Minority
df[['Education','Job Category']]
df['Current Salary'].max()
df['Current Salary'].min()
df['Current Salary'].mean()
df['Current Salary'].describe()
df['Sal Begin'].describe()
df.describe()
df.index
df.set_index('Birth Date', inplace=True)
df.loc['2/26/1949']
df.reset_index(inplace=True)

#-----------------class5-------------------------#
#How to handle missing data.
df=pd.read_excel("path",Parse_dates=["col name"]) --#to change the date datatype.
df.filna(0)
df.fillna({'columnname':0})
df.fillna(method=ffill/bfill) --#carry forward the other column values vertically
df.fillna(method="ffill",axis='columns') --#fill horizontally.
df.fillna(method="ffill",limit=1) --#copy the values for missing ina limit
df.interpolate() --#gradual fill in missing
df.interpolate(method="time") --#gradual fill in missing consider with time.
df.dropna() drop all --#the missing values
df.dropna(how="all") --#remove only all columns having Nan.
df.dropna(thresh=2) --#to keep the column need 2 value.
