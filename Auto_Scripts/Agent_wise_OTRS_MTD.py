# -*- coding: utf-8 -*-
import sys
import traceback
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
FileName = "OTRS_Agent_wise_MTD_Report " + str(Today)+'.xlsx'

logging.basicConfig(filename='/tech-ops/Public/OPS/Logs/OTRS/OTRS_Agent_wise_MTD_Report.log',
                    level=logging.INFO, format="%(asctime)s: %(levelname)s -- %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

class OTRS_Agent_wise_MTD_Report:

	def __init__(self):
		try:
			self.sqlEngine = create_engine('mysql+pymysql://' + str(c.OTRS_DB_Credential["username"]) + ':' + str(c.OTRS_DB_Credential["password"]) + '@' + str(c.OTRS_DB_Credential["server"]) + ':3306/' + str(c.OTRS_DB_Credential["database"]))
			self.dbConnection = self.sqlEngine.connect()
			logging.info("DB connection has been made successfully")

		except Exception as e:
			logging.exception(e)

	def Report1_Gen(self):

		try:
			global Report1

			query1 = ("""select User,total_tickets_handled,Open,Closed,InProgress,Moved,Replied from(
select concat(b.first_name,' ',b.last_name) as User,	
count( distinct case when t.id is not null then a.ticket_id end ) as total_tickets_handled,
count(distinct case when ticket_state_id in (1,4) then t.id end ) as Open,
count(distinct case when ticket_state_id in (2) then t.id end ) as Closed,
count( distinct case when ticket_state_id in (11) then t.id end )as InProgress,
count( distinct case when history_type_id=16 then t.id end ) as Moved,
count( distinct case when history_type_id in(8,11) then t.id end ) as Replied
from ticket t left join
ticket_history a on t.id = a.ticket_id left join users b on a.change_by=b.id where month(a.create_time)= month(now()) and year(a.create_time)= year(now()) and
b.id not in (1,2,3,4,8,10,11,12,13,14,15,16,18,19,23,27,28,29,31,41,42,52,53,55,56,60,61,79,96,111,350,103,106,107,118,119,130,142,144,166,187,198,210,211,227,228,237,238,239,254,255,256,257,258,259,260,261,280,281,282,283,284,285,287,288,289,290,291,292,293,294,295,296,297,298,299,385,305,306,351,352,353,354,355,356,380,384) group by 1) tmp""")
			df1= pd.read_sql(query1, con=self.dbConnection)
      
			if df1.empty:
        
				mail_subject = "OTRS - Agent wise MTD Report" +" - "+ Today
				final_content = "<p style=font-family:'verdana'>Hi All,<br>There is no data found in DB. Hence the report haven't generated for the day.<br></br>Regards,<br>Team TechOPS.</p>"
				email = ['ops@applieddatafinance.com']
				Send_Mail_OPS(email, [], mail_subject, final_content)
				exit()
			else:
				Report1=df1[['User','total_tickets_handled','Open','Closed','InProgress','Moved','Replied']]

				
				
				
			 	Report1.rename(columns={
						"total_tickets_handled": "Total Tickets Handled#", 
						"Open": "Open#",
						"Closed": "Closed#",
						"InProgress": "InProgress#",
						"Moved": "Moved#",
						"Replied": "Replied#"},inplace=True)
				
				

		except Exception as e:
			logging.exception(str(e))
			mail_subject = 'Error while running the second Report1 df creation function in OTRS_Agent_wise_MTD_Report!!!'	
			final_content = """<p>Hi Team,</p><p><br>Error getting while second Report1 df creation function. Kindly look into 
this:<br><b>""" + str(e) + """</b><br><br>Regards,<br>Tech-Ops Team </p>"""
			email = ['ops@applieddatafinance.com']
			Send_Mail_OPS(email, [], mail_subject, final_content)
		        exit()

	def Report2_Gen(self):

		try:
			global Report2

			query2 = ("""select User,tn as "Ticket#",Date as "Date of Open",closedtime as "Date of Close",Status from(
select a.create_time as Date,
concat(b.first_name,' ',b.last_name) as User,tn,	
(case
when ticket_state_id in (1,4) then 'Open'
when ticket_state_id in (2) then 'Closed'
when ticket_state_id in (11) then 'InProgress'
when history_type_id=16 then 'Moved'
when history_type_id in(8,11) then 'Replied' end  )as 'Status',(case when ticket_state_id in (2) then t.change_time end) as 'closedtime'
from ticket t left join
ticket_history a on t.id = a.ticket_id left join users b on a.change_by=b.id where month(a.create_time)= month(now()) and year(a.create_time)= year(now()) and
b.id not in (1,2,3,4,8,10,11,12,13,14,15,16,18,19,23,27,28,29,31,41,42,52,53,55,56,60,61,79,96,111,350,103,106,107,118,119,130,142,144,166,187,198,210,211,227,228,237,238,239,254,255,256,257,258,259,260,261,280,281,282,283,284,285,287,288,289,290,291,292,293,294,295,296,297,298,299,385,305,306,351,352,353,354,355,356,380,384) group by 2,3 order by Date desc) tmp """)
			df2= pd.read_sql(query2, con=self.dbConnection)




			query3 = ("""select User,tn as "Ticket#",Date as "Date of Open",Status from(
select a.create_time as Date,
concat(b.first_name,' ',b.last_name) as User,tn,	
(case
when history_type_id=16 then 'Moved'
when history_type_id in(8,11) then 'Replied' end  )as 'Status'
from ticket t  join
ticket_history a on t.id = a.ticket_id left join users b on a.change_by=b.id where history_type_id in (16,8,11) and month(a.create_time)= month(now()) and year(a.create_time)= year(now()) and
b.id not in (1,2,3,4,8,10,11,12,13,14,15,16,18,19,23,27,28,29,31,41,42,52,53,55,56,60,61,79,96,111,350,103,106,107,118,119,130,142,144,166,187,198,210,211,227,228,237,238,239,254,255,256,257,258,259,260,261,280,281,282,283,284,285,287,288,289,290,291,292,293,294,295,296,297,298,299,385,305,306,351,352,353,354,355,356,380,384) group by 2,3 order by Date desc) tmp """)
			df3=pd.read_sql(query3,con=self.dbConnection)


			dfs=pd.concat([df2,df3])


      
			if dfs.empty:
        
				mail_subject = "OTRS - Agent wise MTD Report" +" - "+ Today
				final_content = "<p style=font-family:'verdana'>Hi All,<br>There is no data found in DB. Hence the report haven't generated for the day.<br></br>Regards,<br>Team TechOPS.</p>"
				email = ['gautamkumargupta@applieddatafinance.com','agilanmahalingam@applieddatafinance.com','santhanakrishnanr@applieddatafinance.com','ops@applieddatafinance.com']
				Send_Mail_OPS(email, [], mail_subject, final_content)
				exit()
			else:
				Report2=dfs[['User','Ticket#','Date of Open','Date of Close','Status']]

				
				

		except Exception as e:
			logging.exception(str(e))
			mail_subject = 'Error while running the second Report2 df creation function in OTRS_Agent_wise_MTD_Report!!!'	
			final_content = """<p>Hi Team,</p><p><br>Error getting while second Report2 df creation function. Kindly look into 
this:<br><b>""" + str(e) + """</b><br><br>Regards,<br>Tech-Ops Team </p>"""
			email = ['abrahamv@applieddatafinance.com']
			Send_Mail_OPS(email, [], mail_subject, final_content)
                        exit()


	def create_excel(self):

		try:
			writer = pd.ExcelWriter('/home/abrahamv/OTRS_Agent_wise_MTD_Report/' + FileName, engine='xlsxwriter')
			Report1.to_excel(writer, sheet_name='Sheet1', index=False)
			Report2.to_excel(writer, sheet_name='Sheet2', index=False)
			writer.save()
			mail_subject = "OTRS_Agent_wise_MTD_Report" +" - "+ Today
			final_content = """<p>Hi All,</p><p><br>Please find attached, OTRS - Agent wise MTD Report for the day. <br></br>Regards,<br>Team TechOPS.</p> """
			email = ['gautamkumargupta@applieddatafinance.com','agilanmahalingam@applieddatafinance.com','santhanakrishnanr@applieddatafinance.com','ops@applieddatafinance.com']
			files = ['/home/abrahamv/OTRS_Agent_wise_MTD_Report/' +FileName]
			Send_Mail_OPS(email, [], mail_subject, final_content, files)



		except Exception as e:
			mail_subject = 'Error while running the Excel sheet creation in OTRS_Agent_wise_MTD_Report!!!'
			final_content = """<p>Hi Team,</p><p><br>Error getting while running Excel sheet creation and send_mail function. Kindly look into this:<br><b>""" + str(e) + """</b><br><br>Regards,<br>Tech Ops Team </p> """
			email = ['ops@applieddatafinance.com']
			Send_Mail_OPS(email, [], mail_subject, final_content)
                        exit()


if __name__ == "__main__":
    run = OTRS_Agent_wise_MTD_Report()
    run.Report1_Gen()
    run.Report2_Gen()
    run.create_excel()


