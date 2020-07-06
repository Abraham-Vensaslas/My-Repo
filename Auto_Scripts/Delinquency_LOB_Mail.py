import os
import re
import traceback
import sys
from datetime import datetime, timedelta, date
import logging
import requests
import json
from sqlalchemy import create_engine
import math as m
import pandas as pd
sys.path.append("/tech-ops/Public/OPS/Config/")
import Credential as cc

from salesforce_bulk import SalesforceBulk, CsvDictsAdapter
from simple_salesforce import Salesforce

sys.path.append("/tech-ops/Public/OPS/Utils/")
from LOBUtils import lob_letter

# TimeStamp Assignment
timestamp = datetime.now().strftime('%B %d, %Y')
timestamp1 = datetime.now().strftime('%Y-%m-%d')
timestamp2 = datetime.now().strftime('%m/%d/%Y')

# template location
template_path = '/tech-ops/Public/OPS/Files/LOB_Templates/Delinquency_Reminder/'

logging.basicConfig(filename='/tech-ops/Public/OPS/Logs/Delinquency/Delinquency_Reminder_LOB_Mails.log',
                    level=logging.DEBUG, format="%(asctime)s: %(levelname)s -- %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")

bulk_job_chunk_size = 50.0

class DelinquencyReminderLOB:

    def __init__(self):
        try:
            self.db = create_engine('mysql+pymysql://' + str(cc.Reports_DB_Credential["username"]) + ':' + str(
                cc.Reports_DB_Credential["password"]) + '@' + str(cc.Reports_DB_Credential["server"]) + ':3306/' + str(
                cc.Reports_DB_Credential["database"])).connect()

            logging.info("DB Connection Established")

        except Exception as exe:
            logging.exception("[DB Connection]: {0}".format(exe))

    def select_loans(self):

        master_query = """        
        SELECT t.Days_Past_Due,CONCAT(t.FirstName,' ',t.LastName) AS fullname,t.otherAddress AS address1,
        CONCAT(t.city,', ',t.state,' ',t.Zip) AS address2,t.LoanID AS loanid, t.Delinquent_Amount, t.Unpaid_Fees,
        t.Past_Due_Since, t.Amount_To_Current, (t.loandate) AS loandate, t.term, t.Amount, t.perdiem, t.city, t.state,
        t.Zip, t.ALoan, t.contact FROM (SELECT CONCAT(UCASE(LEFT(c.FirstName, 1)),
        SUBSTRING(c.FirstName, 2)) AS 'FirstName', CONCAT(UCASE(LEFT(c.LastName, 1)),
        SUBSTRING(c.LastName, 2)) AS 'LastName', lo.LoanID AS 'LoanID', c.Email AS 'Email',
        c.MailingStreet AS otherAddress, c.MailingCity AS city, c.MailingState AS state, c.MailingPostalCode AS Zip,
        DATE_FORMAT(DATE(lo.CreateDate), '%%m/%%d/%%Y') AS loandate, lo.Term AS term, FORMAT(lo.Amount,2) AS Amount,
        FORMAT(l.Per_diem_interest_amount__c, 2) AS perdiem,
        CONCAT('', FORMAT(l.loan__Delinquent_Amount__c, 2)) AS 'Delinquent_Amount',
        CONCAT('', FORMAT(l.loan__Fees_Remaining__c, 2)) AS 'Unpaid_Fees',
        DATE_FORMAT(DATE(loan__Oldest_Due_Date__c), '%%m/%%d/%%Y') AS 'Past_Due_Since',
        CONCAT('', FORMAT(loan__Amount_to_Current__c, 2)) AS 'Amount_To_Current',
        l.loan__Number_of_Days_Overdue__c AS 'Days_Past_Due',
        CASE WHEN (l.loan__Number_of_Days_Overdue__c IN (35)) THEN '35'
        WHEN (l.loan__Number_of_Days_Overdue__c IN(55)) THEN '55'
        WHEN (l.loan__Number_of_Days_Overdue__c IN (85)) THEN '85'
        WHEN (l.loan__Number_of_Days_Overdue__c IN (108)) THEN '108'
        ELSE 'ERROR - CHECK DAYS PAST DUE' END AS Email_to_Send, l.Name AS ALoan, c.Id AS contact,
        CASE WHEN (c.Mailing_Address_Status__c NOT IN ('Do not mail') OR c.Mailing_Address_Status__c IS NULL)
        THEN 0 ELSE 1 END Mailing_Status, l.Sub_Status__c, b.Bankruptcy_Status__c, f.Fraud_Status__c, d.Death_Status__c
        FROM cl_import.loan__Loan_Account__c l JOIN decision.loan lo ON l.Name = lo.sf_loan_number LEFT JOIN
        cl_import.Contact AS c ON l.loan__Contact__c = c.Id LEFT JOIN cl_import.Bankruptcy__c AS b ON 
        l.loan__Contact__c = b.Contact__c LEFT JOIN cl_import.Fraud__c AS f ON l.loan__Contact__c = f.Contact__c
        LEFT JOIN cl_import.Deceased__c AS d ON l.loan__Contact__c = d.Contact__c WHERE
        l.loan__Loan_Status__c IN ('Active - Bad Standing') AND c.DoNotCall = '0' AND c.Cease_Desist__c = '0'
        AND loan__Amount_to_Current__c >= 25 AND l.loan__Loan_Status__c NOT IN ('Canceled','Closed- Written Off')
        AND (l.Sub_Status__c NOT IN ('SCRA Eligible', 'Legal', 'Disputed', '3rd Party Recovery') OR
        l.Sub_Status__c IS NULL) AND (b.Bankruptcy_Status__c NOT IN ('Discharged','Dischraged','Active') OR
        b.Bankruptcy_Status__c IS NULL) AND (f.Fraud_Status__c NOT IN ('Confirmed Fraud- CLOSED') OR
        f.Fraud_Status__c IS NULL) AND (d.Death_Status__c NOT IN ('Confirmation by death certificate',
        'Confirmation by published obituary','Confirmation by TLO') OR d.Death_Status__c IS NULL) HAVING
        Mailing_Status = 0 ORDER BY Email_to_Send) t WHERE t.Email_to_Send != 'ERROR - CHECK DAYS PAST DUE' union
        SELECT t.Days_Past_Due, CONCAT(t.FirstName, ' ', t.LastName) AS fullname, t.otherAddress AS address1,
        CONCAT(t.city, ', ', t.state, ' ', t.Zip) AS address2, t.LoanID AS loanid, t.Delinquent_Amount, t.Unpaid_Fees,
        t.Past_Due_Since, t.Amount_To_Current, (t.loandate) AS loandate, t.term, t.Amount, t.perdiem, t.city, t.state,
        t.Zip, t.ALoan, t.contact FROM (SELECT CONCAT(UCASE(LEFT(c.FirstName, 1)),
        SUBSTRING(c.FirstName, 2)) AS 'FirstName', CONCAT(UCASE(LEFT(c.LastName, 1)),
        SUBSTRING(c.LastName, 2)) AS 'LastName', l.ADF_LOAN_ID__c AS 'LoanID', c.Email AS 'Email',
        c.MailingStreet AS otherAddress, c.MailingCity AS city, c.MailingState AS state, c.MailingPostalCode AS Zip,
        DATE_FORMAT(DATE(lo.created_dtm), '%%m/%%d/%%Y') AS loandate, lo.terms AS term, 
        FORMAT(lo.loan_amount,2) AS Amount, FORMAT(l.Per_diem_interest_amount__c, 2) AS perdiem,
        CONCAT('', FORMAT(l.loan__Delinquent_Amount__c, 2)) AS 'Delinquent_Amount',
        CONCAT('', FORMAT(l.loan__Fees_Remaining__c, 2)) AS 'Unpaid_Fees',
        DATE_FORMAT(DATE(loan__Oldest_Due_Date__c), '%%m/%%d/%%Y') AS 'Past_Due_Since',
        CONCAT('', FORMAT(loan__Amount_to_Current__c, 2)) AS 'Amount_To_Current',
        l.loan__Number_of_Days_Overdue__c AS 'Days_Past_Due', CASE WHEN (l.loan__Number_of_Days_Overdue__c IN (35))
        THEN '35' WHEN (l.loan__Number_of_Days_Overdue__c IN(55)) THEN '55'
        WHEN (l.loan__Number_of_Days_Overdue__c IN (85)) THEN '85'
        WHEN (l.loan__Number_of_Days_Overdue__c IN (108)) THEN '108'
        ELSE 'ERROR - CHECK DAYS PAST DUE' END AS Email_to_Send, l.Name AS ALoan, c.Id AS contact,
        CASE WHEN (c.Mailing_Address_Status__c NOT IN ('Do not mail') OR c.Mailing_Address_Status__c IS NULL)
        THEN 0 ELSE 1 END Mailing_Status, l.Sub_Status__c, b.Bankruptcy_Status__c, f.Fraud_Status__c, d.Death_Status__c
        FROM rpos.loan_details lo join rpos.lead_id_map z on(lo.lead_id=z.lead_id) join
        cl_import.loan__Loan_Account__c l on z.sf_loan_id=l.Name LEFT JOIN cl_import.Contact AS c ON
        l.loan__Contact__c = c.Id LEFT JOIN cl_import.Bankruptcy__c AS b ON l.loan__Contact__c = b.Contact__c
        LEFT JOIN cl_import.Fraud__c AS f ON l.loan__Contact__c = f.Contact__c LEFT JOIN cl_import.Deceased__c AS d ON 
        l.loan__Contact__c = d.Contact__c WHERE l.loan__Loan_Status__c IN ('Active - Bad Standing') AND 
        c.DoNotCall = '0' AND c.Cease_Desist__c = '0' AND loan__Amount_to_Current__c >= 25 AND 
        l.loan__Loan_Status__c NOT IN ('Canceled','Closed- Written Off') AND 
        (l.Sub_Status__c NOT IN ('SCRA Eligible', 'Legal', 'Disputed', '3rd Party Recovery') OR l.Sub_Status__c IS NULL)
        AND (b.Bankruptcy_Status__c NOT IN ('Discharged','Dischraged','Active') OR b.Bankruptcy_Status__c IS NULL)
        AND (f.Fraud_Status__c NOT IN ('Confirmed Fraud- CLOSED') OR f.Fraud_Status__c IS NULL)
        AND (d.Death_Status__c NOT IN ('Confirmation by death certificate','Confirmation by published obituary',
        'Confirmation by TLO') OR d.Death_Status__c IS NULL) HAVING Mailing_Status = 0 ORDER BY Email_to_Send) t
        WHERE t.Email_to_Send != 'ERROR - CHECK DAYS PAST DUE';
        """

        result = self.db.execute(master_query)

        if result.rowcount == 0:
            logging.info("No loans Eligible for Delinquency Reminder LOB Mail")

        else:
#            self.send_letter(result)

    def send_letter(self, result):
        try:
            letter = ''
            for res in result:
                if str(res[0]) == '85':
                    data = {
                        'CURDATE': timestamp,
                        'FULLNAME': str(res[1]),
                        'ADDRESS1': str(res[2]),
                        'ADDRESS2': str(res[3]),
                        'PASTDUE': str(res[5]),
                        'UNPAIDFEES': str(res[6]),
                        'PASTDUESINCE': str(res[7]),
                        'TOTALDUE': str(res[8]),
                        'LOANDATE': str(res[9]),
                        'LOANTERM': str(res[10]),
                        'LOANAMOUNT': str(res[11]),
                        'LOANID': str(res[4]),
                    }

                    to_address = {
                        'name': str(res[1]),
                        'address_line1': str(res[2]),
                        'address_city': str(res[13]),
                        'address_state': str(res[14]),
                        'address_zip': str(res[15]),
                        'address_country': 'US',
                    }

 #                   letter = lob_letter(template_path + 'Delinquency_Reminder_85.html',
 #                                       'Delinquency Letter to ' + str(res[1]),
 ##                                       {'campaign': 'Delinquency Reminder' + str(res[0])}, data, to_address)

                elif str(res[0]) == '108':
                    data = {
                        'CURDATE': timestamp,
                        'FULLNAME': str(res[1]),
                        'ADDRESS1': str(res[2]),
                        'ADDRESS2': str(res[3]),
                        'PASTDUE': str(res[5]),
                        'UNPAIDFEES': str(res[6]),
                        'PASTDUESINCE': str(res[7]),
                        'TOTALDUE': str(res[8]),
                        'LOANID': str(res[4]),
                        'LOANDATE': str(res[9]),
                    }

                    to_address = {
                        'name': str(res[1]),
                        'address_line1': str(res[2]),
                        'address_city': str(res[13]),
                        'address_state': str(res[14]),
                        'address_zip': str(res[15]),
                        'address_country': 'US',
                    }

 #                   letter = lob_letter(template_path + 'Delinquency_Reminder_108.html',
 #                                       'Delinquency Letter to ' + str(res[1]),
 #                                       {'campaign': 'Delinquency Reminder' + str(res[0])}, data, to_address)

                elif str(res[0]) == '55':
                    data = {
                        'CURDATE': timestamp,
                        'FULLNAME': str(res[1]),
                        'ADDRESS1': str(res[2]),
                        'ADDRESS2': str(res[3]),
                        'PASTDUE': str(res[5]),
                        'UNPAIDFEES': str(res[6]),
                        'PASTDUESINCE': str(res[7]),
                        'TOTALDUE': str(res[8]),
                        'LOANID': str(res[4]),
                        'PERDIEM': str(res[12]),
                    }

                    to_address = {
                        'name': str(res[1]),
                        'address_line1': str(res[2]),
                        'address_city': str(res[13]),
                        'address_state': str(res[14]),
                        'address_zip': str(res[15]),
                        'address_country': 'US',
                    }

#                    letter = lob_letter(template_path + 'Delinquency_Reminder_55.html',
#                                        'Delinquency Letter to ' + str(res[1]),
#                                        {'campaign': 'Delinquency Reminder' + str(res[0])}, data, to_address)

                elif str(res[0]) == '35':
                    data = {
                        'CURDATE': timestamp,
                        'FULLNAME': str(res[1]),
                        'ADDRESS1': str(res[2]),
                        'ADDRESS2': str(res[3]),
                        'PASTDUE': str(res[5]),
                        'UNPAIDFEES': str(res[6]),
                        'PASTDUESINCE': str(res[7]),
                        'TOTALDUE': str(res[8]),
                        'LOANID': str(res[4]),
                        'PERDIEM': str(res[12]),
                    }

                    to_address = {
                        'name': str(res[1]),
                        'address_line1': str(res[2]),
                        'address_city': str(res[13]),
                        'address_state': str(res[14]),
                        'address_zip': str(res[15]),
                        'address_country': 'US',
                    }

 #                   letter = lob_letter(template_path + 'Delinquency_Reminder_35.html',
 #                                       'Delinquency Letter to ' + str(res[1]),
 #                                       {'campaign': 'Delinquency Reminder' + str(res[0])}, data, to_address)

                logging.info(letter.url)
  #              self.insert_query(res[17], res[16], res[0], letter.url)
                self.sf_notes(res[0], res[2], res[13], res[14], res[15], res[5], res[6], res[8], res[17])

        except Exception as exe:
            logging.exception("[send_letter]: {0}".format(exe))

    def insert_query(self, contact_id, loan_id, dayPastDue, url):
        try:
            insert = """
            insert into reports.delinquencylobmailings(ContactID,LoanID,DaysPastDue,CreateDate,url) values 
            ('""" + str(contact_id) + """','""" + str(loan_id) + """','""" + str(dayPastDue) + """',
            '""" + timestamp1 + """', '""" + url + """'); 
            """
            self.db.execute(insert)

        except Exception as exe:
            logging.exception("[insert_query] {0}".format(exe))

    def sf_notes_creation(self, parentid, title, body):

	try:
		data = list()
		data1=[]
		data1.append(parentid)
		data1.append(title)
		data1.append(body)
		data_dict = dict(zip(['ParentID','Title','Body',],data1))
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
			job = bulk.create_insert_job('Note', contentType='CSV')
			csv_iterator = CsvDictsAdapter(iter(chunk))
			batch = bulk.post_bulk_batch(job, csv_iterator)
			bulk.wait_for_batch(job, batch)
			bulk.close_job(job)
			k=k+1

	except Exception as e:
        	logging.exception("parent_id: {0}, title: {1}, body: {2}".format(str(parentid), str(title), str(body)))
                logging.exception(e)

        return result_new

    def sf_notes(self, DaysPastDue, email, city, state, zipCode, amount, fee, total_due, contact_id):
        try:
            title = 'Day ' + str(DaysPastDue) + ' Delinquency Notice Mailed on ' + str(timestamp2)
            body = """
            Date of Letter: """ + str(timestamp2) + """.  """ + str(DaysPastDue) + """ days past due.  
            Mailed to: """ + str(email) + """, """ + str(city) + """, """ + str(state) + """ """ + str(zipCode) + """.  
            Delinquent Amount: $ """ + str(amount) + """.   
            Unpaid Fees:  $ """ + str(fee) + """.   
            Total Due: $ """ + str(total_due)

            self.sf_notes_creation(contact_id, title, body)

        except Exception as exe:
            logging.exception("[sf_notes]: {0}".format(exe))


if __name__ == "__main__":
    ob = DelinquencyReminderLOB()
    ob.select_loans()

