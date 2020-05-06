Days=np.arange(0,5)
min_Temp=[25,33,36,25,12]
max_Temp=[62,59,84,64,72]
mean=[45,23,33,51,46]
plt.plot(Days,min_Temp,'--+',label='Min')
plt.plot(Days,max_Temp,'-*',label='Max')
plt.plot(Days,mean,'--D',label='Mean')
plt.xlabel("Days")
plt.ylabel("Temperature")
plt.title("My Sample")
plt.legend(loc='best',shadow=True)
plt.grid()


Company=['GOOGLE','AMAZON','FLIPKART','MICROSOFT']
xpos=np.arange(len(Company))
revenue=[65,58,95,79]
profit=[35,62,85,32]
loss=[32,25,37,46]
plt.xticks(xpos,Company)
plt.bar(xpos-0.2,revenue,label='revenue',color='#886288',width=0.2)
plt.bar(xpos,profit,label='profit',color='#006500',width=0.2)
plt.bar(xpos+0.2,loss,label='loss',color='#782528',width=0.2)
plt.xlabel('Company')
plt.ylabel('Revenue in Billlion ')
plt.title("Bar Sample")
plt.legend()
