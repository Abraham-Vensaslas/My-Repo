# -*- coding: utf-8 -*-
import os
import requests
import MySQLdb, logging
from datetime import datetime
import json
import csv
import logging
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
#FileName = "OTRS_Agent_wise_MTD_Report " + str(Today)+'.xlsx'

#logging.basicConfig(filename='/tech-ops/Public/OPS/Logs/OTRS/OTRS_Doc_upload.log',
#                    level=logging.INFO, format="%(asctime)s: %(levelname)s -- %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

class OTRS_Doc_upload:

	def __init__(self):
		try:
			self.sqlEngine = create_engine('mysql+pymysql://' + str(c.OTRS_DB_Credential["username"]) + ':' + str(c.OTRS_DB_Credential["password"]) + '@' + str(c.OTRS_DB_Credential["server"]) + ':3306/' + str(c.OTRS_DB_Credential["database"]))
			self.dbConnection = self.sqlEngine.connect()
			#logging.info("DB connection has been made successfully")
			print("DB connection has been made successfully")

		except Exception as e:
			print(e)
			#logging.exception(e)


	def initial_insert(self):
		
		try:
			query = ("""SELECT a.id as article_id,a.ticket_id as ticket_id,t.tn,t.customer_id as Email,a_subject as Title,content_path,a.create_time,tl.contact_id,tl.LoanID,1 as 'contact_update'FROM otrs.article a LEFT JOIN otrs.ticket t ON a.ticket_id = t.id join otrscase.Ticket_List tl on (t.id=tl.ticket_id)    WHERE a.id > (SELECT MAX(article_id)FROM otrscase.Doc_Upload) AND customer_id IS NOT NULL AND queue_id IN (5) AND t.create_time < DATE_SUB(NOW(), INTERVAL 3 HOUR) AND article_sender_type_id = 3 AND a_from NOT REGEXP 'MicrosoftExchange' AND a_from NOT REGEXP 'personifyfinancial' AND a_from NOT REGEXP 'woopra' AND a_from NOT REGEXP 'postmaster' AND a_from NOT REGEXP 'localhost' """) 
			df= pd.read_sql(query, con=self.dbConnection)

			if df.empty:
        			print("df is empty")
				mail_subject = "OTRS - Agent wise MTD Report" +" - "+ Today
				final_content = "<p style=font-family:'verdana'>Hi All,<br>There is no data found in DB. Hence the report haven't generated for the day.<br></br>Regards,<br>Team TechOPS.</p>"
				email = ['ops@applieddatafinance.com']
				#Send_Mail_OPS(email, [], mail_subject, final_content)
				exit()
			else:
				print (df)
#				df.to_sql('Doc_Upload', con = self.engine, if_exists = 'append', chunksize = 1000, index=False)

		except Exception as e:
			print(e)
			logging.exception(str(e))
			mail_subject = 'Error in OTRS Doc upload script!!!'	
			final_content = """<p>Hi Team,</p><p><br>Error getting while doing initial insert in otrs doc upload script. Kindly look into this:<br><b>""" + str(e) + """</b><br><br>Regards,<br>Tech-Ops Team </p>"""
			email = ['ops@applieddatafinance.com']
#			Send_Mail_OPS(email, [], mail_subject, final_content)



	def Attachment_path(self):

		try:
			query1 = ("""select article_id,Email,content_path from otrscase.Doc_Upload where Attachment_Path is null and doc_uploaded = 0 """)
			attachment_path = pd.read_sql(query1, con=self.dbConnection)

			ContentPath = attachment_path['content_path'].to_list()
			ArticleID = attachment_path['article_id'].to_list()
			
			for i,j in zip(ContentPath,ArticleID):
				Path = "/opt/otrs/var/article/"+str(i)+"/"+str(j)+""
				Update_AttachmentPath = """ update otrscase.Doc_Upload set Attachment_Path = '""" + str(Path) + """' where article_id = '""" + str(j) + """';"""
				result = self.dbConnection.execute(Update_AttachmentPath)
			logging.info("Attachment Path has been stored in table successfully...")

		except Exception as e:
                        print(e)
                        logging.exception(str(e))
                        mail_subject = 'Error in OTRS Doc upload script!!!'
                        final_content = """<p>Hi Team,</p><p><br>Error getting while updating attachment path in otrs doc upload script. Kindly look into this:<br><b>""" + str(e) + """</b><br><br>Regards,<br>Tech-Ops Team </p>"""
                        email = ['ops@applieddatafinance.com']
#                       Send_Mail_OPS(email, [], mail_subject, final_content)


	def Find_attachment(self):
	
		try:
			query2 = ("""select article_id,du.ticket_id as ticket_id,tn,Attachment_Path,contact_id from otrscase.Doc_Upload du join otrs.article art on(du.article_id=art.id) where Attachment_Path is not null and have_attachment != '1' and art.a_subject !='Ticket Merged' """)
			validate_path = pd.read_sql(query2, con=self.dbConnection)
			

			ArticleID = validate_path['article_id'].to_list()
			TicketID = validate_path['ticket_id'].to_list()
			TicketNmumber = validate_path['tn'].to_list()
			AttachmentPath = validate_path['Attachment_Path'].to_list()
			ContactID = validate_path['contact_id'].to_list()
			
			for i,j in zip(AttachmentPath,ArticleID):
				path=str(i)
				l=[]
				Er=[]
				valid_attachments = (".jpeg",".jpg",".gif",".png",".pdf")
				f = os.listdir(path)
				for file_name in f:
					try:
						if file_name.lower().endswith(valid_attachments):
							l.append(file_name)
					except Exception as e:
						print(e)

				if l != []:
					update = """ update otrscase.Doc_Upload set have_attachment = 1 where article_id = '""" + str(j) + """'; """
					self.dbConnection.execute(update)
					logging.info("table has been updated with attachment details for article"+str(j)+"")

				else:
					update = """ update otrscase.Doc_Upload set have_attachment = 0 where article_id = '""" + str(j) + """'; """
					self.dbConnection.execute(update)
					logging.info("table has been updated without attachment details for article"+str(j)+"")



		except Exception as e:
                        logging.exception(str(e))
                        mail_subject = 'Error in OTRS Doc upload script!!!'
                        final_content = """<p>Hi Team,</p><p><br>Error getting while updating have_attachment in otrs doc upload script. Kindly look into this:<br><b>""" + str(e) + """</b><br><br>Regards,<br>Tech-Ops Team </p>"""
                        email = ['ops@applieddatafinance.com']
#                       Send_Mail_OPS(email, [], mail_subject, final_content)

	def update_ticket(self):
		
		try:
			query4 = ("""select ticket_id,tn,article_id,have_attachment from otrscase.Doc_Upload where have_attachment = 1 and Uploaded_Attachments is null order by id limit 1000 """)
			Final_update = pd.read_sql(query4, con=self.dbConnection)
			
			TicketID_F = Final_update['ticket_id'].to_list()
			ArticleID_F = Final_update['article_id'].to_list()
			Attchment = Final_update['have_attachment'].to_list()

			for i,j,k in zip(TicketID_F,ArticleID_F,Attchment):

				if k==1:
					try:
						client=Client(str(c.OTRS_DB_Credential["url"]), str(c.OTRS_DB_Credential["username"]) + '@' + str(c.OTRS_DB_Credential["server"]), str(c.OTRS_DB_Credential["password"]))
						client.session_restore_or_create()
						client.ticket_get_by_id(str(i))
						
						logging.info("pyotrs connection established")
						update_queue = Ticket({"queue_id":"25"})
						client.ticket_update(str(i), ticket=update_queue)
	
					except Exception as e:
						print(e)
			                        logging.exception(str(e))
                        			mail_subject = 'Error in OTRS Doc upload script!!!'
                        			final_content = """<p>Hi Team,</p><p><br>Error while connecting pyotrs for DB update  in otrs doc upload script. Kindly look into this:<br><b>""" + str(e) + """</b><br><br>Regards,<br>Tech-Ops Team </p>"""
                        			email = ['ops@applieddatafinance.com']
#                       			Send_Mail_OPS(email, [], mail_subject, final_content)

						exit()

					update = """ update otrscase.Doc_Upload set Uploaded_Attachments = '1' where article_id = '""" + str(j) + """'; """
					self.dbConnection.execute(update)
					logging.info("table has been updated without attachment details for article"+str(i[0])+"")
		
		except Exception as e:
			print(e)
                        logging.exception(str(e))
                        mail_subject = 'Error in OTRS Doc upload script!!!'
                        final_content = """<p>Hi Team,</p><p><br>Error while running final update function in otrs doc upload script. Kindly look into this:<br><b>""" + str(e) + """</b><br><br>Regards,<br>Tech-Ops Team </p>"""
                        email = ['ops@applieddatafinance.com']
#                       Send_Mail_OPS(email, [], mail_subject, final_content)

				


if __name__ == "__main__":
	run=OTRS_Doc_upload()
	run.initial_insert()
#	run.Attachment_path()
#	run.Find_attachment()
#	run.update_ticket()
   


