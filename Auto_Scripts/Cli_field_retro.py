# -*- coding: utf-8 -*-
import os
import datetime
import logging
import sys
import re
import traceback
from datetime import datetime, timedelta, date
import pysftp
from salesforce_bulk import SalesforceBulk, CsvDictsAdapter
from salesforce_bulk_api import SalesforceBulkJob
from simple_salesforce import Salesforce
import pandas as pd
from sqlalchemy import create_engine
import math as m
import Config_retro as c
sys.path.append("/tech-ops/Public/OPS/Config/")
import Credential as cc
sys.path.append("/tech-ops/Public/OPS/Utils/")
from MailUtils import *



bulk_job_chunk_size = 50.0
logging.basicConfig(filename='Retro_techops_20.log',level=logging.DEBUG, format="%(asctime)s: %(levelname)s -- %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

class retro():

	def __init__(self):
		try:
			self.cnx = create_engine('mysql+pymysql://' + str(cc.Reports_DB_Credential["username"]) + ':' + str(cc.Reports_DB_Credential["password"]) + '@' + str(cc.Reports_DB_Credential["server"]) + ':3306/' + str(cc.Reports_DB_Credential["database"])).connect()
			logging.info('DB Connection established')

		except Exception as e:
			logging.exception(e)



	def SF_Backend_Update(self):


		try:
			data = list()
  
			query="""select Id,'Collections' as Team__c,'Email' as Request_Channel__c from cl_import.Task where Id='00T3Z00003u7m6GUAQ';"""


		#	query="""select Id, 'Collections' as Team__c,case when (Subject REGEXP 'call' or Description REGEXP 'call') then 'Call' when (Subject REGEXP 'chat' or Description REGEXP 'chat')then 'Chat' else 'Email' end as 'Request_Channel__c' from cl_import.Task where (Subject REGEXP 'covid' or Description REGEXP 'covid') and date(CreatedDate)>='2020-01-01' and Disposition__c is null and Request_Channel__c is null having Request_Channel__c='call' limit 2;"""

 
			result = self.cnx.execute(query).fetchall()
			print("Task")
			for i in result:
				data1=[]
				data1.append(i[0])
				data1.append(i[1])
				data1.append(i[2])
				data_dict = dict(zip(c.SALES_FORCE_FIELDS,data1))
				k="BAP â€“ Covid 19"
				data_dict['Disposition__c']=k
				data.append(data_dict)
				logging.info(data)
				sf = Salesforce(username=cc.SalesForce_Token["username"], password=cc.SalesForce_Token["password"],security_token=cc.SalesForce_Token["security_token"])

				bulk = SalesforceBulk(host=sf.sf_instance,sessionId=sf.session_id)
				logging.info("Chunk Size : {0}".format(int(m.ceil(len(data) / bulk_job_chunk_size))))
				k=0
          
				for data_chunk in range(0, len(data1), int(bulk_job_chunk_size)):
					chunk = data[data_chunk:data_chunk + int(bulk_job_chunk_size)]
					logging.info("Sending Chunk : {0}".format((k + 1)))
					logging.info(chunk)
					job = bulk.create_update_job('Task', contentType='CSV')
					csv_iterator = CsvDictsAdapter(iter(chunk))
					batch = bulk.post_batch(job, csv_iterator)
					bulk.wait_for_batch(job, batch)
					bulk.close_job(job)
					k=k+1



		except Exception as e:
			print(e)

			logging.error(str(e))
			mail_subject='Very Important : Error while running techops_20 retro!!!'
			final_content="<p>Hi Team,</p><p><br><br>Error getting while running techops_20 retro. Kindly look into this:<br><b>"+str(e)+"</b><br><br>Regards,<br>Tech Ops Team </p>"
			email = c.Exception_Email
			Send_Mail_OPS(email, [], mail_subject, final_content)

if __name__ == "__main__":
	ob=retro()
	ob.SF_Backend_Update()

