import sys
import time
import boto3
import botocore.exceptions
from boto3.s3.transfer import S3Transfer
import pysftp
import os
from salesforce_bulk import SalesforceBulk, CsvDictsAdapter
from simple_salesforce import Salesforce
from datetime import datetime, timedelta
from PyPDF2 import PdfFileWriter, PdfFileReader
import requests
import logging
import json
from sqlalchemy import create_engine
import codecs
codecs.BOM_UTF8
'\xef\xbb\xbf'

import PIF_SIF_Config as ps

sys.path.append("/tech-ops/Public/OPS/Config/")
import Credential as cc

sys.path.append("/tech-ops/Public/OPS/Utils/")
from MailUtils import Send_Mail_OPS

global tdate
tdate1 = datetime.today() - timedelta(days=0)
tdate = str(tdate1)[:10]

global File_list
File_list = []

logging.basicConfig(filename='/tech-ops/Public/OPS/Logs/PIF_SIF/PIF_SIF_Mail.log', level=logging.INFO,
                    format="%(asctime)s: %(levelname)s -- %(message)s", datefmt="%Y-%m-%d %H:%M:%S")


class PIF_SIF_Mail:

    def __init__(self):
        try:
            self.db = create_engine('mysql+pymysql://' + str(cc.Reports_DB_Credential["username"]) + ':' + str(
                cc.Reports_DB_Credential["password"]) + '@' + str(cc.Reports_DB_Credential["server"]) + ':3306/' + str(
                cc.Reports_DB_Credential["database"])).connect()

            logging.info('DB Connection Established')

        except Exception as exe:
            logging.exception('[DB Connection] : {0}'.format(exe))

    def select_loans(self):
        try:
            query1 = """
            select llac.name,c.Id,replace(firstName,'''',' '),replace(LastName,'''',' '),llac.loan__Loan_Status__c,
            llac.loan__Closed_Date__c,round(llac.loan__Excess__c,2),round(llac.Refund_Amount__c,2),llac.Refund_Date__c,
            llac.Refund_Type__c,llac.Closure_Email_Date__c,llac.Closure_Email__c,NULL PIF_SIF_Type,llac.Sub_Status__c 
            from cl_import.loan__Loan_Account__c llac join cl_import.Contact c on llac.loan__Contact__c=c.id left join 
            reports.PIF_SIF_Mail ps on llac.name=ps.LoanID where loan__Loan_Status__c = 'Closed - Obligations met' and 
            ps.LoanID is null and llac.Closure_Email_Date__c is null and llac.Closure_Email__c is null and 
            date(llac.loan__Closed_Date__c)<=date_sub(curdate(), interval 10 day) and loan__Closed_Date__c>'2018-05-25' 
            and llac.name != 'LAI-00089284';
            """

            result = self.db.execute(query1)

            if result.rowcount == 0:
                logging.info("No Eligible Loans")
            else:
                self.insert_loans(result)

        except Exception as exe:
            logging.exception("[select_loans]: {0}".format(exe))

    def insert_loans(self, result):
        try:
            for i in result:
                query2 = """
                insert into reports.PIF_SIF_Mail(LoanID,Contact_ID,First_Name,Last_Name,Loan_Status,Loan_closed_date,
                Excess_amount,Refund_Amount,Refund_Date,Refund_Type,Closure_Email_Date,Closure_Email_Type,PIF_SIF_Type,
                Sub_Status,Create_date,update_date,Sent,Email,Acc_No) values
                ('"""+str(i[0])+"""','"""+str(i[1])+"""','"""+str(i[2])+"""','"""+str(i[3])+"""',
                '"""+str(i[4])+"""','"""+str(i[5])+"""','"""+str(i[6])+"""','"""+str(i[7])+"""','"""+str(i[8])+"""',
                '"""+str(i[9])+"""','"""+str(i[10])+"""','"""+str(i[11])+"""','"""+str(i[12])+"""',
                '"""+str(i[13])+"""',CURDATE(),CURDATE(),0,'None','0000');
                """
                self.db.execute(query2)

        except Exception as exe:
            logging.exception("[insert_loans]: {0}".format(exe))
            mail_subject = 'Exception in PIF-SIF Script !!!! '
            final_content = """
            <p>Hi Team,</p><p><br>Error while running Insert to reports db function:<br><b>
            """ + str(exe) + """</b><br><br>Regards,<br>Tech Ops Team </p>
            """
            email = ['ops@applieddatafinance.com']
            Send_Mail_OPS(email, [], mail_subject, final_content)

    def pif_sif_query(self):
        try:
            global loan_id, location, template_id, update_field, contact_id

            logging.info('-------PIF SIF Query function---- ')

            query11 = """
            select LoanID,Contact_ID,First_Name,Last_Name,Excess_amount,Refund_Amount,Refund_Type,
            'WaterMark_file_PIF.pdf' as FileName,'Paid_In_Full_NoRefund' as TemplateID,
            'Paid in Full' as Update_field,llac.Closure_Email_Date__c,llac.Closure_Email__c from 
            reports.PIF_SIF_Mail ps join cl_import.loan__Loan_Account__c llac on ps.LoanID=llac.name where 
            Closure_Email_Date='None' and Closure_Email_Type='None' and llac.Closure_Email_Date__c is null and 
            llac.Closure_Email__c is null and Sub_Status !='Settlement' and Excess_amount < 1 union 
            select LoanID,Contact_ID,First_Name,Last_Name,Excess_amount,Refund_Amount,Refund_Type,
            'WaterMark_file_PIF.pdf' as FileName,'Paid_In_Full_01' as TemplateID,
            'Paid in Full' as Update_field,llac.Closure_Email_Date__c,llac.Closure_Email__c from 
            reports.PIF_SIF_Mail ps join cl_import.loan__Loan_Account__c llac on ps.LoanID=llac.name where 
            Closure_Email_Date='None' and Closure_Email_Type='None' and llac.Closure_Email_Date__c is null and 
            llac.Closure_Email__c is null and Refund_Type!='RCC' and Refund_Date!='None' and Refund_Date__c is not null 
            and Sub_Status !='Settlement' and llac.Refund_Type__c!='RCC' and Refund_Amount!='None' and 
            Excess_amount > 0.99 and Excess_amount=Refund_Amount union 
            select LoanID,Contact_ID,First_Name,Last_Name,Excess_amount,Refund_Amount,Refund_Type,
            'WaterMark_file_SIF.pdf' as FileName,'Settled_In_Full' as TemplateID,
            'Settled in Full' as Update_field,llac.Closure_Email_Date__c,llac.Closure_Email__c from 
            reports.PIF_SIF_Mail ps join cl_import.loan__Loan_Account__c llac on ps.LoanID=llac.name where 
            Closure_Email_Date='None' and Closure_Email_Type='None' and llac.Closure_Email_Date__c is null and 
            llac.Closure_Email__c is null and Sub_Status ='Settlement';
            """

            result = self.db.execute(query11)

            if result.rowcount != 0:
                for r in result:
                    loan_id = r[0]
                    contact_id = r[1]
                    First_Name = r[2]
                    Last_Name = r[3]
                    location = ps.file_path + r[7]
                    template_id = r[8]
                    update_field = r[9]

                    p1 = """
                    update reports.PIF_SIF_Mail set Pick_Date=current_timestamp() where LoanID = '"""+str(loan_id)+"""';
                    """
                    self.db.execute(p1)
                    self.get_loan_id(loan_id)

            else:
                mail_subject = 'PIF-SIF Automatic mail !!! '
                final_content = """
                <p>Hi Team,</p><p><br>PIF/SIF accounts is not available for the day. Kindly check once and confirm 
                by reply over it.<br><br>Regards,<br>Tech Ops Team </p>
                """
                email = ['ops@applieddatafinance.com']
                Send_Mail_OPS(email, [], mail_subject, final_content)

        except Exception as exe:
            logging.exception("[pif_sif_query]: {0}".format(exe))
            mail_subject = 'Exception in PIF-SIF Script !!!! '
            final_content = """
            <p>Hi Team,</p><p><br>Error while running pif sif query function: <br><b> """ + str(exe) + """
            </b><br><br>Regards,<br>Tech Ops Team </p>
            """
            email = ['ops@applieddatafinance.com']
            Send_Mail_OPS(email, [], mail_subject, final_content)

    def get_loan_id(self, loan_id):
        try:
            logging.info('-------S3 function Initializing---- ')

            query = """
            select '"""+str(loan_id)+"""',doclink from decision.loan l where LoanID = (select ADF_LOAN_ID__c from 
            cl_import.loan__Loan_Account__c where name='"""+str(loan_id)+"""') and doclink is not null union select 
            '"""+str(loan_id)+"""', concat(substring_index(loan_doc_name,'.',1),'.pdf') doclink from rpos.loan_details 
            join rpos.lead_id_map using(lead_id) where loan_id = (select ADF_LOAN_ID__c from 
            cl_import.loan__Loan_Account__c where name= '"""+str(loan_id)+"""') and loan_doc_name is not null having 
            doclink is not null limit 1;
            """

            Id = self.db.execute(query).fetchall()

            li = []

            for doc_name in Id:
                li.append(doc_name[1])
            self.conn_s3(li)

            for doc_name in Id:
                self.pif_sif_watermark(doc_name[0], doc_name[1])

        except Exception as exe:
            logging.exception("[get_loan_id]: {0}".format(exe))
            mail_subject = 'Exception in PIF-SIF Script !!!! '
            final_content = """
            <p>Hi Team,</p><p><br>Error while running S3 function initializing:<br><b>"""+str(exe)+"""
            </b><br><br>Regards,<br>Tech Ops Team </p>
            """
            email = ['ops@applieddatafinance.com']
            Send_Mail_OPS(email, [], mail_subject, final_content)

    def conn_s3(self, link=[]):
        try:
            logging.info('--------------Downloading Agreement in S3 bucket------------------')

            AWS_ACCESS_KEY_ID = 'AKIAJKRLZKIQSUIVFBPA'
            AWS_SECRET_ACCESS_KEY = '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'
            AWS_bucket = 'adfcontracts'
            s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

            for li in link:
                keyString = str(li)
                DownloadString = ps.file_path + str(keyString)
                logging.info(DownloadString)

                try:
                    s3.Bucket(AWS_bucket).download_file(keyString, DownloadString)

                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        logging.info("File does not exist: {0}".format(keyString))
                        mail_subject = 'Exception in PIF-SIF Script !!!! '
                        final_content = """
                        <p>Hi Team,</p><p>""" + str(keyString) + """ File was not found in S3 bucket. Kindly verify. 
                        <br><br><br>Regards,<br>Tech Ops Team </p>
                        """
                        email = ['ops@applieddatafinance.com']
                        Send_Mail_OPS(email, [], mail_subject, final_content)

                    else:
                        raise

        except Exception as exe:
            logging.exception("[conn_s3]: {0}".format(exe))

    def pif_sif_watermark(self, loan_id, doc_link):
        try:
            logging.info('---------Creation of watermark-------------')
            os.chdir(ps.file_path)
            loan_name = ps.file_path + str(doc_link)

            if os.path.exists(loan_name):
                img_name = location
                logging.info(img_name)

                watermark = PdfFileReader(open(img_name, "rb"))
                output_file = PdfFileWriter()
                input_file = PdfFileReader(open(loan_name, "rb"))
                page_count = input_file.getNumPages()

                for page_number in range(0, page_count):
                    input_page = input_file.getPage(page_number)
                    input_page.mergePage(watermark.getPage(0))
                    output_file.addPage(input_page)

                filename = os.path.join(ps.file_path, str(loan_id) + '.pdf')
                outputStream = open(filename, 'wb')
                output_file.write(outputStream)
                outputStream.close()
                filename = str(loan_id) + '.pdf'
                water_pdf = os.path.getsize(filename)
                agree_pdf = os.path.getsize(loan_name)

                if water_pdf > agree_pdf:
                    self.ftp_putfile(filename)
                    time.sleep(3)
                    self.mail_trigger(filename, loan_id, template_id)
                    self.update_sf(loan_id, update_field)
                    self.bulkapi(loan_id, template_id, contact_id)
                    File_list.append(filename)

                else:
                    mail_subject = 'Exception in PIF-SIF Script !!!! '
                    final_content = """
                    <p>Hi Team,</p><p><br>Water Mark PIF/SIF file  is not created correctly. Kindly check once 
                    and confirm by reply over it.<br>File Location: """ + str(ps.file_path) + """<br>Regards,<br>
                    Tech Ops Team </p>
                    """
                    email = ['ops@applieddatafinance.com']
                    Send_Mail_OPS(email, [], mail_subject, final_content)

        except Exception as exe:
            logging.exception("[pif_sif_watermark]: {0}".format(exe))
            mail_subject = 'Exception in PIF-SIF Script !!!! '
            final_content = """
            <p>Hi Team,</p><p><br>Error while running watermark creation function:<br><b>
            """ + str(exe) + """ </b><br><br>Regards,<br>Tech Ops Team </p>
            """
            email = ['ops@applieddatafinance.com']
            Send_Mail_OPS(email, [], mail_subject, final_content)

    def ftp_putfile(self, filename):
        try:
            logging.info('-----Upload file in SFTP----')
            with pysftp.Connection(host=cc.ET_SFTP["host"], username=cc.ET_SFTP["username"],
                                   password=cc.ET_SFTP["password"]) as sftp:
                sftp.put(ps.file_path + str(filename), '/Import/' + str(filename), preserve_mtime=True)
                logging.info('{0} file uploaded to ET'.format(filename))
                sftp.close()

        except Exception as exe:
            logging.exception("[ftp_putfile]: {0}".format(exe))
            mail_subject = 'Exception in PIF-SIF Script !!!! '
            final_content = """
            <p>Hi Team,</p><p><br>Error while running SFTP upload function:<br><b> """ + str(exe) + """
            </b><br><br>Regards,<br>Tech Ops Team </p>
            """
            email = ['ops@applieddatafinance.com']
            Send_Mail_OPS(email, [], mail_subject, final_content)
            self.ftp_putfile(filename)

    def mail_trigger(self, filename, loan_id, template_id):

        logging.info('------------Mailtrigger function intializing----------- ')

        query3 = """
        select * from 
        (select DATE_FORMAT(DATE(CONVERT_TZ(CURRENT_TIMESTAMP(), 'UTC', 'US/Pacific')), '%%M %%d, %%Y') curdate,
        FirstName,LastName,MailingStreet,MailingCity,MailingState,MailingPostalCode,ADF_LOAN_ID__c,Email,llac.name,
        c.Id,FORMAT(llac.Refund_Amount__c,2),right(loan__Bank_Account_Number__c,4) 'Accno' from 
        (select max(name) LPT_Name, loan__Loan_Account__c,loan__Payment_Mode__c,loan__Automated_Payment_Setup__c 
        from cl_import.loan__Loan_Payment_Transaction__c where loan__Cleared__c=1 and loan__Rejected__c=0 and 
        loan__Reversed__c=0 and loan__Payment_Mode__c in ('a1Dj0000002XbjREAS','a1Dj00000023VuBEAU',
        'a1Dj00000023VuGEAU') group by 2 order by loan__Transaction_Date__c desc ) lpt left join 
        cl_import.loan__Loan_Account__c llac  on lpt.loan__Loan_Account__c=llac.Id left join 
        cl_import.loan__Automated_Payment_Setup__c aps on aps.id=lpt.loan__Automated_Payment_Setup__c left join 
        cl_import.loan__Bank_Account__c bc on bc.id=aps.loan__Bank_Account__c left join cl_import.Contact c on 
        llac.loan__Contact__c=c.id where llac.name='""" + str(loan_id) + """' union 
        select DATE_FORMAT(DATE(CONVERT_TZ(CURRENT_TIMESTAMP(), 'UTC', 'US/Pacific')), '%%M %%d, %%Y') curdate, 
        FirstName,LastName,MailingStreet,MailingCity,MailingState,MailingPostalCode,ADF_LOAN_ID__c,Email,llac.name,
        c.Id,FORMAT(llac.Refund_Amount__c,2),right(loan__Bank_Account_Number__c,4) 'Accno' from (select 
        max(name) LPT_Name, loan__Loan_Account__c,loan__Payment_Mode__c,loan__Automated_Payment_Setup__c from 
        cl_import.loan__Loan_Payment_Transaction__c where loan__Cleared__c=1 and loan__Rejected__c=0 and 
        loan__Reversed__c=0 group by 2 order by loan__Transaction_Date__c desc ) lpt left join 
        cl_import.loan__Loan_Account__c llac  on lpt.loan__Loan_Account__c=llac.Id left join 
        cl_import.loan__Automated_Payment_Setup__c aps on aps.id=lpt.loan__Automated_Payment_Setup__c left join 
        cl_import.loan__Bank_Account__c bc on bc.id=aps.loan__Bank_Account__c left join cl_import.Contact c on 
        llac.loan__Contact__c=c.id where llac.name='""" + str(loan_id) + """' union select
        DATE_FORMAT(DATE(CONVERT_TZ(CURRENT_TIMESTAMP(), 'UTC', 'US/Pacific')), '%%M %%d, %%Y') curdate,FirstName,
        LastName,MailingStreet,MailingCity,MailingState,MailingPostalCode,ADF_LOAN_ID__c,Email,llac.name,c.Id,
        FORMAT(llac.Refund_Amount__c,2),right(loan__Bank_Account_Number__c,4) 'Accno' from 
        cl_import.loan__Loan_Account__c llac left join  cl_import.Contact c on llac.loan__Contact__c=c.id left join 
        cl_import.loan__Bank_Account__c bc using(loan__Contact__c) where llac.name='""" + str(loan_id) + """') a 
        having Accno is not null limit 1 ;
        """

        result = self.db.execute(query3)

        url = cc.API['url'] + cc.API['mail_trigger']

        if result.rowcount != 0:
            for b in result:
                try:
                    headers = {
                        'Authorization': 'Basic VEVTVEVSOlRFU1RFUg==',
                        'Content-Type': 'application/json'
                    }

                    payload = {
                        'toAddress': b[8],
                        'bccAddress': 'etobemail@personifyfinancial.com',
                        'templateId': template_id,
                        'contactId': b[10],
                        'templateAttributes': {
                            'CurrentDate': b[0],
                            'FirstName': b[1],
                            'LastName': b[2],
                            'StreetAdd': b[3],
                            'City': b[4],
                            'State': b[5],
                            'Zipcode': b[6],
                            'LoanID': b[7],
                            'FileName': filename,
                            'Amount': b[11],
                            'AccNo': b[12]
                        }
                    }

                    logging.info(payload)

                    req = requests.post(url, auth=('TESTER', 'TESTER'), data=json.dumps(payload), headers=headers)
                    logging.info(req)

                    if '200' in str(req):
                        response = """
                        update reports.PIF_SIF_Mail set Response='1' where LoanID='""" + str(loan_id) + """';
                        """
                    else:
                        response = """
                        update reports.PIF_SIF_Mail set Response='0' where LoanID='""" + str(loan_id) + """';
                        """

                    self.db.execute(response)

                    update = """
                    update reports.PIF_SIF_Mail set Sent=1, PIF_SIF_Type='""" + str(template_id) + """',
                    Acc_No='""" + str(b[12]) + """',Email='""" + str(b[8]) + """',update_date=current_timestamp() where 
                    LoanID='""" + str(loan_id) + """';
                    """
                    self.db.execute(update)

                except Exception as exe:
                    logging.exception("[mail_trigger]: {0}".format(exe))
                    mail_subject = 'Exception in PIF-SIF Script !!!! '
                    final_content = """
                    <p>Hi Team,</p><p><br>Error while running mail trigger function: <br><b>""" + str(
                        exe) + """</b><br><br>Regards,<br>Tech Ops Team </p>
                        """
                    email = ['ops@applieddatafinance.com']
                    Send_Mail_OPS(email, [], mail_subject, final_content)

    def update_sf(self, loan_id, email_type):
        try:
            logging.info('------------------- loan__Loan_Account__c field updating -------------- ')

            data = list()

            query = """
            select Id,'""" + str(email_type) + """',curdate() from cl_import.loan__Loan_Account__c where 
            name= '""" + str(loan_id) + """';
            """

            result = self.db.execute(query)

            if result.rowcount != 0:
                for i in result:
                    data1 = []
                    data1.append(i[0])
                    data1.append(i[1])
                    data1.append(i[2])
                    data_dict = dict(zip(ps.SALES_FORCE_FIELDS, data1))
                    data.append(data_dict)

                    sf = Salesforce(username=cc.SalesForce_Token['username'], password=cc.SalesForce_Token['password'],
                                    security_token=cc.SalesForce_Token['security_token'],
                                    sandbox=cc.SalesForce_Token['sandbox'])

                    bulk = SalesforceBulk(host=sf.sf_instance, sessionId=sf.session_id)
                    job = bulk.create_update_job('loan__Loan_Account__c')

                    csv_iterator = CsvDictsAdapter(iter(data))

                    batch = bulk.post_bulk_batch(job, csv_iterator)
                    bulk.wait_for_batch(job, batch)
                    bulk.close_job(job)
                    
                    logging.info(data)

                    update_query = """
                    update reports.PIF_SIF_Mail set Closure_Email_Date=curdate(),
                    Closure_Email_Type='""" + str(email_type) + """',update_date=current_timestamp(), Field_Response='1' 
                    where LoanID='"""+str(loan_id)+"""';
                    """
                    self.db.execute(update_query)

        except Exception as exe:
            logging.exception("[update_sf]: {0}".format(exe))
            mail_subject = 'Exception in PIF-SIF Script !!!! '
            final_content = """
            <p>Hi Team,</p><p><br>Error while running update sf function:<br><b> """ + str(exe) + """
            </b><br><br>Regards,<br>Tech Ops Team </p>
            """
            email = ['ops@applieddatafinance.com']
            Send_Mail_OPS(email, [], mail_subject, final_content)

    def bulkapi(self, loan_id, template_id, contact_id):
        try:
            logging.info('------- Task creation function initializing --------------')

            query = """ 
            select Id,curdate() from cl_import.loan__Loan_Account__c where name= '""" + str(loan_id) + """';
            """

            result2 = self.db.execute(query)

            data = []

            for i in result2:
                subject = "PIF/SIF : " + str(tdate)
                Status = 'Completed'
                Priority = 'Normal'
                Ownid = '005j000000DbwqnAAB'
                newline = "\n"
                Description = "PIF/SIF Mail sent dated on : "+str(tdate)+""+str("\n")+" Sent Template :"+str(template_id)

                data1=[]
                data1.append(str(contact_id))
                data1.append(subject)
                data1.append(i[1])
                data1.append(Status)
                data1.append(Priority)
                data1.append(Description)
                data1.append(Ownid)

                sfg1 = ['Whoid', 'Subject', 'ActivityDate', 'Status', 'Priority', 'Description', 'OwnerId']

                logging.info(data1)
                data_dict = dict(zip(ps.SALES_FORCE_FIELDS1, data1))
                data.append(data_dict)
                logging.info("merging SF fields and query result " + str(data))

                sf = Salesforce(username=cc.SalesForce_Token['username'], password=cc.SalesForce_Token['password'],
                                security_token=cc.SalesForce_Token['security_token'],
                                sandbox=cc.SalesForce_Token['sandbox'])
                bulk = SalesforceBulk(host=sf.sf_instance, sessionId=sf.session_id)

                Vsessionid = sf.session_id
                job = bulk.create_insert_job('Task')
                csv_iterator = CsvDictsAdapter(iter(data))

                batch = bulk.post_bulk_batch(job, csv_iterator)
                bulk.wait_for_batch(job, batch)
                bulk.close_job(job)

                update_query = """
                update reports.PIF_SIF_Mail set Task_Response='1' where LoanID= '""" + str(loan_id) + """';
                """
                self.db.execute(update_query)

        except Exception as exe:
            logging.exception("[bulkapi]: {0}".format(exe))
            mail_subject = 'Exception in PIF-SIF Script !!!! '
            final_content = """
            <p>Hi Team,</p><p><br><br>Error while running sf task creation function:<br><b>""" + str(exe) + """
            </b><br><br>Regards,<br>Tech Ops Team </p>
            """
            email = ['ops@applieddatafinance.com']
            Send_Mail_OPS(email, [], mail_subject, final_content)

    def S3_doc_upload(self):
        try:
            logging.info('--------------upload watermark in s3 bucket function initializing-----------')
            for doc in File_list:
                    client = boto3.client('s3', aws_access_key_id='AKIAJKRLZKIQSUIVFBPA',
                                          aws_secret_access_key='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')
                    transfer = S3Transfer(client)
                    bucket_name = 'sif.pif.contract'
                    transfer.upload_file(doc, bucket_name, doc)

        except Exception as exe:
            logging.exception("[S3_doc_upload]: {0}".format(exe))
            mail_subject = 'Error PIF SIF query function !!!! '
            final_content = """
            <p>Hi Team,</p><p><br>Error while running s3 doc upload function:<br><b> """ + str(exe) + """
            </b><br><br>Regards,<br>Tech Ops Team </p>
            """
            email = ['ops@applieddatafinance.com']
            Send_Mail_OPS(email, [], mail_subject, final_content)

    def App_Desk_Update(self):
        try:
            logging.info('--------------App Desk Update-----------')

            query = """ 
            select LoanID from reports.PIF_SIF_Mail where Closure_Email_Date=curdate();  
            """
            result = self.db.execute(query)

            if result.rowcount != 0:
                for i in result:
                    LOAN_NUMBER = i[0]
                    DocType = "PIF/SIF WaterMark Contract"
                    self.App_Desk_Mail_Sent(LOAN_NUMBER, DocType)
                    DocType = "PIF/SIF Email Cover Letter"
                    self.App_Desk_Mail_Sent(LOAN_NUMBER, DocType)

        except Exception as exe:
            logging.exception("[App_Desk_Update]: {0}".format(exe))
            mail_subject = 'Exception in PIF-SIF Script !!!! '
            final_content = """
            <p>Hi Team,</p><p><br>Error while running PIF-SIF appdesk function:<br><b>
            """ + str(exe) + """ </b><br><br>Regards,<br>Tech Ops Team </p>
            """
            email = ['ops@applieddatafinance.com']
            Send_Mail_OPS(email, [], mail_subject, final_content)

    def App_Desk_Mail_Sent(self, loan_number, doc_type):
        try:
            logging.info("Received API Call for the loan -->  " + str(loan_number))

            Notes = doc_type + ' was Successfully Sent'

            query = """
            select Application_ApplicationID,'Sent','""" + str(doc_type) + """','""" + str(Notes) + """' from 
            decision.loan where sf_loan_number='""" + str(loan_number) + """';
            """

            result = self.db.execute(query)

            logging.info("Fetching required data for the given loan -->  " + str(loan_number))

            url = "https://prod-appdesk-priv.adfdata.net/api/doc-sent"

            headers1 = {
                'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6InN1cHVzZXIiLCJpYXQiOjE0ODM1MjAzMDgsInJvbGUiOiJwY3BfdXBsb2FkZXIiLCJleHAiOjQ2MzcxMjAzMDh9.1OgMiRQlWtAbx2jkQcodEbf-ZZfkYZ58x6PGz_GMRhM',
                'Content-Type': 'application/json'
            }

            if result.rowcount != 0:
                for r in result:
                    payload = {
                        'ApplicationID': r[0],
                        'Status': r[1],
                        'DocType': r[2],
                        'Notes': r[3]
                    }

                    data1 = json.dumps(payload)
                    logging.info("Result of the api posted -->  " + str(data1))
                    req = requests.post(url, data=data1, headers=headers1)
                    logging.info("Result of the api posted -->  " + str(req))
            else:
                logging.info("We Received Empty result for the given loan -->  " + str(loan_number))

        except Exception as exe:
            logging.exception("[App_Desk_Mail_Sent]: {0}".format(exe))
            mail_subject = "Error While trying to update appdesk communication content"
            email = ['ops@applieddatafinance.com']
            final_content = """
            <p>Hi All,<br><br><b>Error occurred while trying to update appdesk communication content. Please find the 
            following error </b><br> """ + str(exe) + """ <br><br>Regards,<br>Ops Team.</p>
            """
            Send_Mail_OPS(email, [], mail_subject, final_content)


if __name__ == "__main__":
    ob = PIF_SIF_Mail()
    ob.select_loans()
    ob.pif_sif_query()
    ob.S3_doc_upload()
    ob.App_Desk_Update()

