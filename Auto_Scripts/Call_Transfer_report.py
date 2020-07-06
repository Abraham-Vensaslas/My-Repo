# -*- coding: utf-8 -*-
import sys
from datetime import datetime
import pysftp
import pandas as pd
from sqlalchemy import create_engine
import logging

import Call_Transfer_Report_Config as f

sys.path.append("/tech-ops/Public/OPS/Config/")
import Credential as cc

sys.path.append("/tech-ops/Public/OPS/Utils/")
from MailUtils import *
from Email_table import *

date_format = '%m/%d/%Y %H:%M:%S %Z'
Today = str(datetime.today()).split()[0]
FileName = "Call_Transfer_Report_" + str(Today) + ".csv"

logging.basicConfig(filename='/tech-ops/Public/OPS/Logs/Call_Transfer_Report/Call_Transfer_Report.log',
                    level=logging.INFO, format="%(asctime)s: %(levelname)s -- %(message)s", datefmt="%Y-%m-%d %H:%M:%S")


class Call_Transfer_Report:

    def __init__(self):
        try:
            self.dbConnection = create_engine(
                'mysql+pymysql://' + str(cc.Reports_DB_Credential["username"]) + ':' + str(
                    cc.Reports_DB_Credential["password"]) + '@' + str(
                    cc.Reports_DB_Credential["server"]) + ':3306/reports').connect()

            logging.info("DB connection has been made successfully")

        except Exception as e:
            logging.exception(e)

    def sftp_conn(self):
        try:
            logging.info("----SFTP Connection Initiated----")

            with pysftp.Connection(cc.Five9_SFTP['host'], username=cc.Five9_SFTP['username'],
                                   password=cc.Five9_SFTP['password']) as sftp:

                logging.info("Connection successfully established ... ")

                remoteFilePath = f.Source_sftp + 'Call_Transfer_Report.csv'
                localFilePath = f.Destination + FileName

                sftp.get(remoteFilePath, localFilePath)

                logging.info("{0} Downloaded".format(FileName))

        except Exception as e:
            logging.exception(str(e))
            mail_subject = 'Error while running the sftp function in call transfer report!!!'
            final_content = """
            <p>Hi Team,</p><p><br>Error getting while running sftp function. Kindly look into this:<br><b>""" + str(
                e) + """</b><br><br>Regards,<br>Tech Ops Team </p>
            """
            email = ['ops@applieddatafinance.com']

            Send_Mail_OPS(email, [], mail_subject, final_content)

    def vaild_data(self):
        try:
            data_from_sftp = pd.read_csv(f.Destination + FileName)
            if data_from_sftp.empty:
                logging.info("The data received from sftp is empty")
                exit()
            else:
                logging.info("It contains data and good to go")

        except Exception as e:
            logging.exception(str(e))
            mail_subject = "No_vaild_data: Call Transfer Report " + Today
            final_content = """
            <p>Hi Team,</p><p><br>The data we have received from sftp is empty. Kindly look into 
            this:<br><b>""" + str(e) + """</b><br><br>Regards,<br>Tech Ops Team </p>
            """
            email = ['ops@applieddatafinance.com']

            Send_Mail_OPS(email, [], mail_subject, final_content)

    def insert_query(self):

        try:
            reader = pd.read_csv(f.Destination + FileName)
            query = pd.read_sql("""
            select SESSION_ID from reports.Call_Transfer_Report where date(insertDateTime)<=curdate()
            """, con=self.dbConnection)

            result = reader[reader['SESSION ID'].isin(query['SESSION_ID'])]

            dframe = pd.DataFrame(result, columns=['SESSION ID'])

            if dframe.empty:
                logging.info("Table insertion started")
                if not reader.empty:
                    reader.rename(columns={"DATE": "CallDate", "TIME": "CallTime", "CALL ID": "CallID",
                                           "SESSION ID": "SESSION_ID", "DISPOSITION PATH": "DISPOSITION_PATH",
                                           "CALL SEGMENT ID": "CALL_SEGMENT_ID", "CALLED PARTY": "CALLED_PARTY",
                                           "CALLING PARTY": "CALLING_PARTY", "SEGMENT TIME": "SEGMENT_TIME",
                                           "SEGMENT TYPE": "SEGMENT_TYPE", "CAMPAIGN TYPE": "CAMPAIGN_TYPE"},
                                  inplace=True)

                    reader.to_sql('Call_Transfer_Report', con=self.dbConnection, if_exists='append', chunksize=1000,
                                  index=False)
                    logging.info("data has been successfully loaded in DB")

                else:
                    logging.info("reader is empty")
            else:
                logging.info("Data already present in table...")
                exit()

        except Exception as e:
            logging.exception(str(e))
            mail_subject = 'Error while running the insert query function in call transfer report!!!'
            final_content = """
            <p>Hi Team,</p><p><br>Error getting while running insert query function. Kindly look into 
            this:<br><b>""" + str(e) + """</b><br><br>Regards,<br>Tech Ops Team </p>
            """
            email = ['ops@applieddatafinance.com']

            Send_Mail_OPS(email, [], mail_subject, final_content)

    def create_df_for_report_one(self):

        try:
            global df_final

            query1 = ("""
            select ptp.id as 'PTP_ID',ptp.CL_Contract__c as LoanID,ctr.SESSION_ID as 'F9_SessionID',ctr.CAMPAIGN as
            CAMPAIGN,ctr.CALLED_PARTY 'Called Party',ctr.CALLING_PARTY 'Calling Party',t.Call_Recording__c as 
            'Link to Call recording' from reports.Call_Transfer_Report ctr left join cl_import.Task t ON 
            (ctr.SESSION_ID = t.Five9__Five9SessionId__c) left join cl_import.Promise_To_Pay__c ptp on 
            (t.Whatid=ptp.CL_Contract__c) where date(ctr.insertDateTime)=curdate() and date(ptp.CreatedDate)=
            date(t.CreatedDate) and ptp.Call_Result__c='Promise' and (ctr.SESSION_ID = t.Five9__Five9SessionId__c) and 
            t.whatid is not null and ctr.CALLING_PARTY rlike "@applieddatafinance.com" and t.Call_Recording__c is not 
            null group by ptp.CL_Contract__c,ptp.id 
            """)
            df1 = pd.read_sql(query1, con=self.dbConnection)
            List_of_loanID = df1['LoanID'].tolist()

            query2 = ("""
            select  ptp.id as 'PTP_ID',ptp.CreatedDate as 'PTP Date',ptp.CreatedById as PTP_CreatedById,ptp.Amount__c 
            as 'PTP Amount',ss.Name as 'Settlement ID',ss.CL_Contract__c,ss.Offer_Amount__c as 'Settlement Amount' from 
            cl_import.Settlement_Installments__c ss join cl_import.Promise_To_Pay__c ptp using(CL_Contract__c) where 
            CL_Contract__c in """ + str(tuple(List_of_loanID)).replace(',)', ')') + """ group by ptp.id 
            """)
            df2 = pd.read_sql(query2, con=self.dbConnection)

            if df2.empty:

                mail_subject = "Call Transfer Report " + Today
                final_content = "<p style=font-family:'verdana'>Hi All,<br>There is no settlement offer made by this week's (call_transfer_report.csv)file. Hence the reports were not generated as expected .<br></br>Regards,<br>Team TechOPS.</p>"
                email = ['jkacirek@applieddatafinance.com', 'cgarone@applieddatafinance.com',
                'santhanakrishnanr@applieddatafinance.com', 'gautamkumargupta@applieddatafinance.com',
                'ops@applieddatafinance.com']
                Send_Mail_OPS(email, [], mail_subject, final_content)
                exit()
            else:

                df_merge = pd.merge(df1, df2, on='PTP_ID', how='inner')

                df3 = df_merge[['LoanID', 'PTP_ID', 'PTP Date', 'PTP_CreatedById', 'PTP Amount', 'Settlement ID', 'Settlement Amount',
                 'F9_SessionID', 'Called Party', 'Called Party', 'Link to Call recording']]
                List_of_LoanIDs = df3['LoanID'].to_list()
                List_of_PtpID = df3['PTP_ID'].to_list()

                query4 = ("""
            select llac.id as 'LoanID',llac.Name as 'Loan Number',llac.ContactFirstName__c as 'Customer First Name',
            llac.Contact_LastName__c as 'Customer Last Name' from cl_import.loan__Loan_Account__c llac where llac.id 
            in """ + str(tuple(List_of_LoanIDs)).replace(',)', ')') + """ """)
                df4 = pd.read_sql(query4, con=self.dbConnection)

                query5 = ("""
            select ptp.CL_Contract__c as LoanID,ptp.id as PTP_ID,u.Name as 'Agent Name Who set PTP and Settlement' from 
            cl_import.Promise_To_Pay__c ptp join cl_import.User u on (ptp.CreatedById=u.Id) where ptp.id in
            """ + str(tuple(List_of_PtpID)).replace(',)', ')') + """ 
            """)
                df5 = pd.read_sql(query5, con=self.dbConnection)

                df_merge_1 = pd.merge(df4, df5, on='LoanID', how='inner')

                df6 = df_merge_1[['Loan Number', 'Customer First Name', 'Customer Last Name', 'PTP_ID',
                              'Agent Name Who set PTP and Settlement']]

                df_merge_2 = pd.merge(df3, df6, on='PTP_ID', how='inner')

                df_final = df_merge_2[['LoanID', 'Loan Number', 'Customer First Name', 'Customer Last Name', 'PTP_ID', 'PTP Date',
                 'PTP Amount', 'Settlement ID', 'Settlement Amount', 'Agent Name Who set PTP and Settlement',
                 'F9_SessionID', 'Called Party', 'Called Party', 'Link to Call recording']]

        except Exception as e:
            logging.exception(str(e))
            mail_subject = 'Error while running the first report df creation function in call transfer report!!!'
            final_content = """
            <p>Hi Team,</p><p><br>Error getting while first report df creation function. Kindly look into 
            this:<br><b>""" + str(e) + """</b><br><br>Regards,<br>Tech Ops Team </p>
            """
            email = ['ops@applieddatafinance.com']

            Send_Mail_OPS(email, [], mail_subject, final_content)

    def create_df_for_report_two(self):

        try:
            global final_report_2
            query = ("""
            select ptp.id 'PTP_ID',ptp.CL_Contract__c 'LoanID',ptp.CreatedDate 'PTP Date',ptp.Due_Date__c as 
            'PTP Due Date',ptp.Amount__c 'PTP Amount',aps.CreatedDate 'Recurring APS Created Date',
            u.Name 'Agent Name Who set PTP' from cl_import.loan__Automated_Payment_Setup__c aps join 
            cl_import.Promise_To_Pay__c ptp on (aps.loan__CL_Contract__c=ptp.CL_Contract__c) left join cl_import.User u 
            ON (ptp.CreatedById = u.Id) where Call_Result__c='Promise' and ptp.CreatedDate!=ptp.Due_Date__c and 
            aps.loan__Type__c='RECURRING' and ptp.CreatedDate > aps.CreatedDate and loan__Active__c =1 group by 
            ptp.CL_Contract__c,ptp.id
            """)
            df1 = pd.read_sql(query, con=self.dbConnection)
            List_of_PTP_ID = df1['PTP_ID'].tolist()

            query1 = ("""
            select ptp.id as 'PTP_ID',t.Five9__Five9Campaign__c 'Campaign',t.Five9__Five9SessionId__c as 'SessionID',
            t.Call_Recording__c 'Link to Call recording'from cl_import.Promise_To_Pay__c ptp  join cl_import.Task t on 
            (ptp.Contact__c=t.Whoid) where t.Call_Recording__c is not null and t.Five9__Five9Campaign__c is not null and
             t.Five9__Five9SessionId__c is not null and date(ptp.CreatedDate)=date(t.CreatedDate) and ptp.id in 
             """ + str(tuple(List_of_PTP_ID)).replace(',)', ')') + """ group by ptp.id
             """)
            df2 = pd.read_sql(query1, con=self.dbConnection)

            df_merge = pd.merge(df1, df2, on='PTP_ID', how='inner')

            df3 = df_merge[['LoanID', 'PTP_ID', 'PTP Date', 'PTP Due Date', 'PTP Amount', 'Recurring APS Created Date',
                            'Agent Name Who set PTP', 'Campaign', 'SessionID', 'Link to Call recording']]
            List_of_LoanIDs = df3['LoanID'].to_list()

            query3 = ("""
            select id as 'LoanID',Name as 'Loan Number',llac.ContactFirstName__c as 'Customer First Name',
            llac.Contact_LastName__c 'Customer Last Name' from cl_import.loan__Loan_Account__c llac where id in
            """ + str(tuple(List_of_LoanIDs)).replace(',)', ')') + """ 
            """)
            df4 = pd.read_sql(query3, con=self.dbConnection)

            df_merge1 = pd.merge(df3, df4, on='LoanID', how='inner')

            final_report_2 = df_merge1[
                ['Loan Number', 'Customer First Name', 'Customer Last Name', 'PTP_ID', 'PTP Date', 'PTP Due Date',
                 'PTP Amount', 'Recurring APS Created Date', 'Agent Name Who set PTP', 'Campaign', 'SessionID',
                 'Link to Call recording']]

        except Exception as e:
            logging.exception(str(e))
            mail_subject = 'Error while running the second report df creation function in call transfer report!!!'
            final_content = """
            <p>Hi Team,</p><p><br>Error getting while second report df creation function. Kindly look into 
            this:<br><b>""" + str(e) + """</b><br><br>Regards,<br>Tech Ops Team </p>
            """
            email = ['ops@applieddatafinance.com']

            Send_Mail_OPS(email, [], mail_subject, final_content)

    def create_excel(self):

        try:

            writer = pd.ExcelWriter(f.Destination + 'Call_Transfer_Report_1.xlsx', engine='xlsxwriter')
            df_final.to_excel(writer, sheet_name='Sheet1', index=False)
            writer.save()

            writer = pd.ExcelWriter(f.Destination + 'Call_Transfer_Report_2.xlsx', engine='xlsxwriter')
            final_report_2.to_excel(writer, sheet_name='Sheet1', index=False)
            writer.save()

            mail_subject = "Call Transfer Report " + Today
            final_content = """
            <p>Hi All,</p><p><br>Please find attached, Call transfer report for the day. 
            <br></br>Regards,<br>Team TechOPS.</p>
            """

            email = [
                'jkacirek@applieddatafinance.com', 'cgarone@applieddatafinance.com',
                'santhanakrishnanr@applieddatafinance.com', 'gautamkumargupta@applieddatafinance.com',
                'ops@applieddatafinance.com'
            ]

            files = [f.Destination + 'Call_Transfer_Report_1.xlsx', f.Destination + 'Call_Transfer_Report_2.xlsx']

            Send_Mail_OPS(email, [], mail_subject, final_content, files)
            logging.info("Report has been created and triggered successfully")

        except Exception as e:
            logging.exception(str(e))
            mail_subject = 'Error while running the Excel sheet creation in call transfer report!!!'
            final_content = """
            <p>Hi Team,</p><p><br>Error getting while running Excel sheet creation and send_mail  function. Kindly 
            look into this:<br><b>""" + str(e) + """</b><br><br>Regards,<br>Tech Ops Team </p> 
            """
            email = ['ops@applieddatafinance.com']

            Send_Mail_OPS(email, [], mail_subject, final_content)


if __name__ == "__main__":
    run = Call_Transfer_Report()
    run.sftp_conn()
    run.vaild_data()
    run.insert_query()
    run.create_df_for_report_one()
    run.create_df_for_report_two()
    run.create_excel()

