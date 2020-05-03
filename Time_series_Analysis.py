#-----------when dataframe doesnot have dates---------#

rng=pd.date_range(start='2020-01-01',periods=49,freq='B')  #--creating dates for business days for known period
df.set_index(rng,inplace=True)  #--set generaeted date as a index in df
df.asfreq('D',method='pad')   #--regener df accoding to new freq and we can do hourly,weekly (assuming weekend as a working day)

#--------generate random number for frequency-------#

import numpy as np
np.random.randomint(1,10,len(index))
ts=pd.Series(np.random.randint(1,20,len(rng)), index=rng) #--set this to the frequency

#specify holiday calender for different country
from pandas.tseries.holiday import USfederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay
usb=CustomBusinessDay(Calendar=USFederalHolidayCalendar()) #--now we can pass usb as a frequency

#---------Q date and period change------------#

y=pd.Period('2020')  #--Period('2020', 'A-DEC')
y.start_time #--Timestamp('2020-01-01 00:00:00')
y.end_time #--Timestamp('2020-12-31 23:59:59.999999999')
y=pd.Period('2020-2',freq='M')
y+1  #--Period('2020-03', 'M')
y=pd.Period('2020Q1')  
y.start_time  #--Timestamp('2020-01-01 00:00:00')
y.end_time  #--Timestamp('2020-03-31 23:59:59.999999999')
y=pd.Period('2020Q1', 'Q-JAN') #--when the fiscal year ends from Jan
y.asfreq('M',how='start') #--cahange the frequency to monthly
y.asfreq('M',how='end') #--cahange the frequency to monthly
d=pd.period_range(2019,2021,freq='Q-JAN')  #--period of quarter


#-------------Shifting and Lagging----------------#
df.shift(1) #-- to shift the column one step back
df['Price difference']=df['Price'].shift(1) #-- to calculate the previous day
df.tshift(1)   #-- to shift the index when it has freq



#-------------TimeZone Handling___________________#

from pytz import all_timezones
df.index=pd.date_range(start='2020-03-05 00:00:00',periods=17,freq='H',tz='America/Bahia')
df.tz_localize(tz='US/Eastern')
