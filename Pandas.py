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

#------------------Class6-----------------------#
#how to replace missing data
df.replace(11.0,np.NaN) --#to replace the value to another NaN
df.replace([8.011.0],np.NaN) --#lost for multiple NaN
#replace value based on specific column (provide a dict)
	df.replace({'columnname':'exiting value'},'newvalue' )
	df.replace({'existing value':'newvalue'})
#remove the unit fo measure (use regex to identify the pattern) 
	df.replace('[A-Za-z]','',regex=True)
	df.replace({'Country':'[A-Za-z]'},'',regex=True) --#on a particular column
#replace list of value witha nother list of value
	df.replace(['list of existing value'],['new value list'])

#-----------------Class7-------------------------#
#How to group by

g=df.groupby('league')
for league,league_df in g:
    print(" ")
    print(league)
    print(" ")
    print(league_df)
%matplotlib inline
g.plot()

#-----------------Class8-------------------------#
#concat dataframes
pd.concat(['1stdatframe','sec data frame'],ignore_index=True)
df1=pd.DataFrame({'city':['Mumbai','Delhi','Bangalore'],'Temperature':[25,36,21]})
df2=pd.DataFrame({'city':['california','Illionis','Boston'],'Temperature':[12,18,22]})
dfs=pd.concat([df1,df2])
dfs=pd.concat([df1,df2],keys=['India','USA']) --#create  seperate index
dfs=pd.concat([df1,df2],ignore_index=True) --#make the autoindex to continue
dfs=pd.concat([df1,df2],axis=1) --#join the table paralell
df1=pd.DataFrame({'city':['Mumbai','Delhi','Bangalore'],'Temperature':[25,36,21]}, index=[0,1,2])
s=pd.Series(['23,58,89'],name='Joice') --#adding another column and values
f=pd.concat([dfs,s],axis=1)

#-----------------Class9-------------------------#
#merge/join dataframes
df=pd.merge(df1,df2, on=('city'))
df=pd.merge(df1,df2, on=('city'),suffixes=('left','right'))
df=pd.merge(df1,df2, on=('city'), how='outer')
df=pd.merge(df1,df2, on=('city'), how='outer',indicator=False)

#-----------------Class10-------------------------#
#Pivot to reshape the data
#Piot table is used to summarize and aggregate data inside dataframe
df.pivot(index='columnname',columns='columnname', values='coulmnname')
#index-->row level
#columns-->column level
#values --> what you want your value to be

#usecase--> duplicate entries with different values(morning and evening temperature)
	df.pivot_table(index='',columns='')
#by using aggfunc you can apply aggregate functions also 
	df.pivot_table(index='',columns='',aggfunc='count')
#grouper function to aggregate based on date
	df.pivot_table(index=pd.grouper(freq='M',key='date'), columns='')

#-----------------Class11-------------------------#
#reshape dataframe by using melt
pd.melt(df,id_vars=['column name'])
--#1st argument is dataframe
--#2nd argument is id variable(what you want ton your x axis)

#change the column name of the dataframe
pd.melt(df,id_vars=['column name'],columnname_name='what to change')

#-----------------Class12-------------------------#
#Reshaping by using stack for multiple headers
df.stack() --#take the inner most header.
df.stack(level=0/1/2) --# based on the header level
df.stack(level=0)
df_stacked.unstack() --#to unstack the stacked values
#we can also usev dropna to drop the values.

#-----------------Class13-------------------------#
#crosstab func to many small referenec table

pd.crosstab(df['columnname'],df['columnname'])
pd.crosstab(df['league'],df['Team Name'], values=df['Goals and assists'], aggfunc=np.average)-->use the aggregate functions withit.
margins=True --> to knoe the sum
normalize='Index' --> The percentage of individual value.

#----------------Time Series Analysis ----------------#
#dataindex
df=pd.read_csv('C:/Users/abrahamv/Downloads/AAPL.csv',parse_dates=['Date'],index_col=['Date'])
Ap=df["2019-04"] --# to view the data only April 2019
df['Close'].resample('Q').mean().plot(kind='bar')
#resample is used to specify the range as daily/monthly/quarterly
#plot will draw the chart by usign matplotlib and kind we can change the type of it.

