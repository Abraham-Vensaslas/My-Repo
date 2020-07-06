# -*- coding: utf-8 -*-
import sys
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import logging

sys.path.append("/tech-ops/Public/OPS/Config/")
import Credential as c

sys.path.append("/tech-ops/Public/OPS/Utils/")
from MailUtils import *
from Email_table import *

date_format = '%m/%d/%Y %H:%M:%S %Z'
Today = str(datetime.today()).split()[0]
FileName = "Bucket_4_NSB_Report" + str(Today)

logging.basicConfig(filename='/tech-ops/Public/OPS/Logs/Bucket_4_NSB_Report/Bucket_4_NSB_Report.log',
                    level=logging.INFO, format="%(asctime)s: %(levelname)s -- %(message)s", datefmt="%Y-%m-%d %H:%M:%S")


class Bucket_4_NSB_Report:

	def __init__(self):
		try:
			self.sqlEngine = create_engine('mysql+pymysql://' + str(c.Reports_DB_Credential["username"]) + ':' + str(c.Reports_DB_Credential["password"]) + '@' + str(c.Reports_DB_Credential["server"]) + ':3306/' + str(c.Reports_DB_Credential["database"]))
			self.dbConnection = self.sqlEngine.connect()
			logging.info("DB connection has been made successfully")

		except Exception as e:
			logging.exception(e)


	def Report_Gen(self):

		try:
			global Final

			query1 = ("""select (case when Placed_to='NSB' then 'NSB' else 'Radius' end) as Placed,
(case
when p.Loan_Pool__c='A' then 'A'
when p.Loan_Pool__c='B' then 'B'
when p.Loan_Pool__c='C' then 'C'
when p.Loan_Pool__c='D' then 'D'
when p.Loan_Pool__c='POOLB' then 'B'
when p.Loan_Pool__c='POOLC' then 'C' end)  as APR_Bucket,
monthname(Picked_date) as Month_of_assignment,
group_concat(distinct monthname(case 
when p.Placed_to is null and lpt.loan__Reversed__c=0 and lpt.loan__Cleared__c=1 and lpt.loan__Rejected__c=0 and lpt.loan__Transaction_Date__c>p.Picked_date then loan__Transaction_Date__c 
when p.Placed_to ='NSB' and lpt.loan__Reversed__c=0 and lpt.loan__Cleared__c=1 and lpt.loan__Rejected__c=0 and lpt.loan__Transaction_Date__c>p.Picked_date and lpt.loan__Payment_Mode__c='a1D3Z000005mGp5UAE' then loan__Transaction_Date__c
end)) performance_month,
count( distinct Loanid) as Accounts_placed,
sum( distinct Placementamount) as Balance_placed, ###distinct won't work
round(sum(case 
when Placed_to is null and lpt.loan__Reversed__c=0 and lpt.loan__Cleared__c=1 and lpt.loan__Rejected__c=0 and lpt.loan__Transaction_Date__c>p.Picked_date then loan__Transaction_Amount__c 
when Placed_to='NSB' and lpt.loan__Reversed__c=0 and lpt.loan__Cleared__c=1 and lpt.loan__Rejected__c=0 and lpt.loan__Transaction_Date__c>p.Picked_date and lpt.loan__Payment_Mode__c='a1D3Z000005mGp5UAE'then loan__Transaction_Amount__c
end),2) Payment_Posted,

count(distinct case when llac.loan__Number_of_Days_Overdue__c>=90 then p.loanid end) Remains_in_Bucket4,
count(distinct case when loan__Number_of_Days_Overdue__c<90 then llac.Id end) Moved_to_Bucket3_or_lower,
count(distinct case when llac.loan__Closed_Date__c is not null then llac.id end) PIF_or_SIF,
count(distinct case when loan__Loan_Status__c='Closed- Written Off' then llac.Id end) Charged_Off,
round(sum(distinct case when loan__Number_of_Days_Overdue__c>=90 then p.Placementamount end),2) as balance_that_remain_in_bucket_4,
round(sum(distinct case when loan__Number_of_Days_Overdue__c<90 then p.Placementamount end),2) as balance_that_remain_in_bucket_3_or_low,
round(sum(distinct case when llac.loan__Closed_Date__c is not null then case when (llac.loan__Last_Payment_Amount__c<5 and llac.loan__Closed_Date__c = lpt.loan__Transaction_Date__c and lpt.loan__Payment_Mode__c != 'a1Df1000004SCWzEAO') then lpt.loan__Transaction_Amount__c  else loan__Last_Payment_Amount__c end end),2) as Balance_that_are_paid_off_PIF_or_SIF,
round(sum( distinct case when loan__Loan_Status__c='Closed- Written Off' then p.Placementamount end),2) Charged_Off_amount
from reports.Pre_Chargeoff_list p left join cl_import.loan__Loan_Account__c llac on (p.Loanid=llac.Name) left join cl_import.loan__Loan_Payment_Transaction__c lpt on (llac.id =lpt.loan__Loan_Account__c) group by 1,2""")
			df = pd.read_sql(query1, con=self.dbConnection)
                        logging.info("----query executed----")
      
			if df.empty:
                                logging.info("----Query result is empty----")
				mail_subject = "Bucket-4 NSB Report" +" - "+ Today
				final_content = "<p style=font-family:'verdana'>Hi All,<br>There is no data found in DB. Hence the report haven't generated for the day.<br></br>Regards,<br>Team TechOPS.</p>"
				email = ['ops@applieddatafinance.com']
				Send_Mail_OPS(email, [], mail_subject, final_content)
				exit()
			else:
                                logging.info("----Df has been created----")
				Final=df[['Placed','APR_Bucket','Month_of_assignment','performance_month','Accounts_placed','Balance_placed','Payment_Posted','Remains_in_Bucket4','Moved_to_Bucket3_or_lower','PIF_or_SIF','Charged_Off','balance_that_remain_in_bucket_4','balance_that_remain_in_bucket_3_or_low','Balance_that_are_paid_off_PIF_or_SIF','Charged_Off_amount']]
			

			 	Final.rename(columns={
						"Placed": "Group (Radius/NSB)", 
						"APR_Bucket": "APR bucket (A/B/C/D)",
						"Month_of_assignment": "Month of assignment",
						"Performance_month": "Performance month",
						"Accounts_placed": "# accounts placed",
						"Balance_placed": "$ balance placed",
						"Payment_Posted": "$ payments posted",
						"Remains_in_Bucket4": "# accounts that remain in bucket 4",
						"Moved_to_Bucket3_or_lower": "# accounts that move to bucket 3 or lower",
						"PIF_or_SIF": "# accounts that are paid off (PIF/SIF)", 
						"Charged_Off": "# # accounts that are charged off", 
						"balance_that_remain_in_bucket_4": "# $ balance that remain in bucket 4", 
						"balance_that_remain_in_bucket_3_or_low": "# $ balance that move to bucket 3 or lower", 
						"Balance_that_are_paid_off_PIF_or_SIF": "# $ balance that are paid off (BIF/SIF)", 
						"Charged_Off_amount": "$ balance charged off"},inplace=True)
                            

		except Exception as e:
			mail_subject = 'Error while running the second report df creation function in Bucket_4_NSB_Report!!!'	
			final_content = """<p>Hi Team,</p><p><br>Error getting while second report df creation function. Kindly look into 
this:<br><b>""" + str(e) + """</b><br><br>Regards,<br>Tech Ops Team </p>"""
			email = ['ops@applieddatafinance.com']
			Send_Mail_OPS(email, [], mail_subject, final_content)

	def create_excel(self):

		try:
			writer = pd.ExcelWriter('/tech-ops/Public/OPS/Files/Bucket_4_NSB_Report/' + 'Bucket_4_NSB_Report.xlsx', engine='xlsxwriter')
			Final.to_excel(writer, sheet_name='Sheet1', index=False)
			writer.save()
			mail_subject = "Bucket-4 NSB Report" +" - "+ Today
			final_content = """<p>Hi All,</p><p><br>Please find attached, Bucket-4 NSB Report for the day. <br></br>Regards,<br>Team TechOPS.</p> """
			email = ['bhargavijayanthi@applieddatafinance.com','santhoshkumar.a@applieddatafinance.com','mohan.raj@applieddatafinance.com','arunprasadr@applieddatafinance.com','ops@applieddatafinance.com']
			files = ['/tech-ops/Public/OPS/Files/Bucket_4_NSB_Report/' + 'Bucket_4_NSB_Report.xlsx']
			Send_Mail_OPS(email, [], mail_subject, final_content, files)
                        logging.info("Report has been created and triggered successfully")



		except Exception as e:
			mail_subject = 'Error while running the Excel sheet creation in Bucket-4 NSB Report!!!'
			final_content = """<p>Hi Team,</p><p><br>Error getting while running Excel sheet creation and send_mail  function. Kindly look into this:<br><b>""" + str(e) + """</b><br><br>Regards,<br>Tech Ops Team </p> """
			email = ['ops@applieddatafinance.com']
			Send_Mail_OPS(email, [], mail_subject, final_content)
      


if __name__ == "__main__":
    run = Bucket_4_NSB_Report()
    run.Report_Gen()
    run.create_excel()


