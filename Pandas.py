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
