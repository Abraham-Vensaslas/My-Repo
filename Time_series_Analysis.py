###when dataframe doesnot have dates

rng=pd.date_range(start='2020-01-01',periods=49,freq='B')  #--creating dates for business days for known period
df.set_index(rng,inplace=True)  #--set generaeted date as a index in df
df.asfreq('D',method='pad')   #--regener df accoding to new freq and we can do hourly,weekly (assuming weekend as a working day)

#generate random number for frequency

import numpy as np
np.random.randomint(1,10,len(index))
ts=pd.Series(np.random.randint(1,20,len(rng)), index=rng) #--set this to the frequency

#specify holiday calender for different country
from pandas.tseries.holiday import USfederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay
usb=CustomBusinessDay(Calendar=USFederalHolidayCalendar()) #--now we can pass usb as a frequency
