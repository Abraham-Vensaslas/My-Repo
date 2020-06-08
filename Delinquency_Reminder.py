import sys, os
import datetime
import MySQLdb
import re
sys.path.append("/data/public/OPS/Utils/")
sys.path.append("/data/public/OPS/Config/")
import CommonConfig
import Common

api_key     = CommonConfig.LOB_List['API_Prod']

### Initialise DB connection
db=MySQLdb.connect(host=CommonConfig.DB_Credential['server'], user=CommonConfig.DB_Credential['username'], passwd=CommonConfig.DB_Credential['password'], db=CommonConfig.DB_Credential['database'])
cursor=db.cursor()

## TimeStamp Assignment
timestamp=datetime.datetime.now().strftime('%B %d, %Y')
timestamp1=datetime.datetime.now().strftime('%Y-%m-%d')
timestamp2=datetime.datetime.now().strftime('%m/%d/%Y')
print timestamp


### Get data
DATA="""
SELECT
    t.Days_Past_Due,
    CONCAT(t.FirstName, ' ', t.LastName) AS fullname,
    t.otherAddress AS address1,
    CONCAT(t.city, ', ', t.state, ' ', t.Zip) AS address2,
    t.LoanID AS loanid,
    t.Delinquent_Amount,
    t.Unpaid_Fees,
    t.Past_Due_Since,
    t.Amount_To_Current,
    (t.loandate) AS loandate,
    t.term,
    t.Amount,
    t.perdiem,
    t.city,
    t.state,
    t.Zip,
    t.ALoan,
    t.contact
FROM (
SELECT CONCAT(UCASE(LEFT(c.FirstName, 1)), SUBSTRING(c.FirstName, 2)) AS 'FirstName',
       CONCAT(UCASE(LEFT(c.LastName, 1)), SUBSTRING(c.LastName, 2)) AS 'LastName',
       lo.LoanID AS 'LoanID',
       c.Email AS 'Email',
       c.MailingStreet AS otherAddress,
       c.MailingCity AS city,
       c.MailingState AS state,
       c.MailingPostalCode AS Zip,
       DATE_FORMAT(DATE(lo.CreateDate), '%m/%d/%Y') AS loandate,
       lo.Term AS term,
       FORMAT(lo.Amount,2) AS Amount,
       FORMAT(l.Per_diem_interest_amount__c, 2) AS perdiem,
       CONCAT('', FORMAT(l.loan__Delinquent_Amount__c, 2)) AS 'Delinquent_Amount',
       CONCAT('', FORMAT(l.loan__Fees_Remaining__c, 2)) AS 'Unpaid_Fees',
       DATE_FORMAT(DATE(loan__Oldest_Due_Date__c), '%m/%d/%Y') AS 'Past_Due_Since',
       CONCAT('', FORMAT(loan__Amount_to_Current__c, 2)) AS 'Amount_To_Current',
       l.loan__Number_of_Days_Overdue__c AS 'Days_Past_Due',
       CASE WHEN (l.loan__Number_of_Days_Overdue__c IN (35)) THEN '35'
                        WHEN (l.loan__Number_of_Days_Overdue__c IN(55)) THEN '55'
            WHEN (l.loan__Number_of_Days_Overdue__c IN (85)) THEN '85'
            WHEN (l.loan__Number_of_Days_Overdue__c IN (108)) THEN '108'
            ELSE 'ERROR - CHECK DAYS PAST DUE' END AS Email_to_Send,
       l.Name AS ALoan,
       c.Id AS contact,
       CASE WHEN (c.Mailing_Address_Status__c NOT IN ('Do not mail') OR c.Mailing_Address_Status__c IS NULL) THEN 0 ELSE 1 END Mailing_Status,
       l.Sub_Status__c,
       b.Bankruptcy_Status__c,
       f.Fraud_Status__c,
       d.Death_Status__c
FROM cl_import.loan__Loan_Account__c l
JOIN decision.loan lo ON l.Name = lo.sf_loan_number
LEFT JOIN cl_import.Contact AS c ON l.loan__Contact__c = c.Id
LEFT JOIN cl_import.Bankruptcy__c AS b ON l.loan__Contact__c = b.Contact__c
LEFT JOIN cl_import.Fraud__c AS f ON l.loan__Contact__c = f.Contact__c
LEFT JOIN cl_import.Deceased__c AS d ON l.loan__Contact__c = d.Contact__c
WHERE l.loan__Loan_Status__c IN ('Active - Bad Standing')
      AND c.DoNotCall = '0'
      AND c.Cease_Desist__c = '0'
      AND loan__Amount_to_Current__c >= 25
      AND l.loan__Loan_Status__c NOT IN ('Canceled','Closed- Written Off')
      AND (l.Sub_Status__c NOT IN ('SCRA Eligible', 'Legal', 'Disputed', '3rd Party Recovery') OR l.Sub_Status__c IS NULL)
      AND (b.Bankruptcy_Status__c NOT IN ('Discharged','Dischraged','Active') OR b.Bankruptcy_Status__c IS NULL)
      AND (f.Fraud_Status__c NOT IN ('Confirmed Fraud- CLOSED') OR f.Fraud_Status__c IS NULL)
      AND (d.Death_Status__c NOT IN ('Confirmation by death certificate','Confirmation by published obituary','Confirmation by TLO') OR d.Death_Status__c IS NULL)
HAVING Mailing_Status = 0
ORDER BY Email_to_Send) t
WHERE t.Email_to_Send != 'ERROR - CHECK DAYS PAST DUE'
union
SELECT
    t.Days_Past_Due,
    CONCAT(t.FirstName, ' ', t.LastName) AS fullname,
    t.otherAddress AS address1,
    CONCAT(t.city, ', ', t.state, ' ', t.Zip) AS address2,
    t.LoanID AS loanid,
    t.Delinquent_Amount,
    t.Unpaid_Fees,
    t.Past_Due_Since,
    t.Amount_To_Current,
    (t.loandate) AS loandate,
    t.term,
    t.Amount,
    t.perdiem,
    t.city,
    t.state,
    t.Zip,
    t.ALoan,
    t.contact
FROM (
SELECT CONCAT(UCASE(LEFT(c.FirstName, 1)), SUBSTRING(c.FirstName, 2)) AS 'FirstName',
       CONCAT(UCASE(LEFT(c.LastName, 1)), SUBSTRING(c.LastName, 2)) AS 'LastName',
       l.ADF_LOAN_ID__c AS 'LoanID',
       c.Email AS 'Email',
       c.MailingStreet AS otherAddress,
       c.MailingCity AS city,
       c.MailingState AS state,
       c.MailingPostalCode AS Zip,
       DATE_FORMAT(DATE(lo.created_dtm), '%m/%d/%Y') AS loandate,
       lo.terms AS term,
       FORMAT(lo.loan_amount,2) AS Amount,
       FORMAT(l.Per_diem_interest_amount__c, 2) AS perdiem,
       CONCAT('', FORMAT(l.loan__Delinquent_Amount__c, 2)) AS 'Delinquent_Amount',
       CONCAT('', FORMAT(l.loan__Fees_Remaining__c, 2)) AS 'Unpaid_Fees',
       DATE_FORMAT(DATE(loan__Oldest_Due_Date__c), '%m/%d/%Y') AS 'Past_Due_Since',
       CONCAT('', FORMAT(loan__Amount_to_Current__c, 2)) AS 'Amount_To_Current',
       l.loan__Number_of_Days_Overdue__c AS 'Days_Past_Due',
       CASE WHEN (l.loan__Number_of_Days_Overdue__c IN (35)) THEN '35'
                        WHEN (l.loan__Number_of_Days_Overdue__c IN(55)) THEN '55'
            WHEN (l.loan__Number_of_Days_Overdue__c IN (85)) THEN '85'
            WHEN (l.loan__Number_of_Days_Overdue__c IN (108)) THEN '108'
            ELSE 'ERROR - CHECK DAYS PAST DUE' END AS Email_to_Send,
       l.Name AS ALoan,
       c.Id AS contact,
       CASE WHEN (c.Mailing_Address_Status__c NOT IN ('Do not mail') OR c.Mailing_Address_Status__c IS NULL) THEN 0 ELSE 1 END Mailing_Status,
       l.Sub_Status__c,
       b.Bankruptcy_Status__c,
       f.Fraud_Status__c,
       d.Death_Status__c
FROM rpos.loan_details lo join rpos.lead_id_map z on(lo.lead_id=z.lead_id) join  cl_import.loan__Loan_Account__c l on 
z.sf_loan_id=l.Name
LEFT JOIN cl_import.Contact AS c ON l.loan__Contact__c = c.Id
LEFT JOIN cl_import.Bankruptcy__c AS b ON l.loan__Contact__c = b.Contact__c
LEFT JOIN cl_import.Fraud__c AS f ON l.loan__Contact__c = f.Contact__c
LEFT JOIN cl_import.Deceased__c AS d ON l.loan__Contact__c = d.Contact__c
WHERE l.loan__Loan_Status__c IN ('Active - Bad Standing')
      AND c.DoNotCall = '0'
      AND c.Cease_Desist__c = '0'
      AND loan__Amount_to_Current__c >= 25
      AND l.loan__Loan_Status__c NOT IN ('Canceled','Closed- Written Off')
      AND (l.Sub_Status__c NOT IN ('SCRA Eligible', 'Legal', 'Disputed', '3rd Party Recovery') OR l.Sub_Status__c IS NULL)
      AND (b.Bankruptcy_Status__c NOT IN ('Discharged','Dischraged','Active') OR b.Bankruptcy_Status__c IS NULL)
      AND (f.Fraud_Status__c NOT IN ('Confirmed Fraud- CLOSED') OR f.Fraud_Status__c IS NULL)
      AND (d.Death_Status__c NOT IN ('Confirmation by death certificate','Confirmation by published obituary','Confirmation by TLO') OR d.Death_Status__c IS NULL)
HAVING Mailing_Status = 0
ORDER BY Email_to_Send) t
WHERE t.Email_to_Send != 'ERROR - CHECK DAYS PAST DUE';

"""
cursor.execute(DATA);
result=cursor.fetchall()

#print result[0]

pattern=r'[a-zA-Z\_]'
for i in result:
        if re.search(pattern,i[15]) or i[15].endswith('-') or i[15].startswith('-') or i[15]=='':
                te=datetime.now() - timedelta(days=2)
                print 'tdate',te
                mail_subject = 'Alert::: Zip code is not proper in delinquency Physical letter ' + (datetime.now() ).strftime('%Y-%m-%d') + '.'
                email=['ravi.ranjan@applieddatafinance.com']
                email_cc=[]
                final_content = "<p>Hi Team,</p><p><br><br><b>Below customer's ZIP code is either Null or not in proper format:</b><br>"+str(i[17])+"<br><br><b>Kindly update the propert zip code for this contact</b><br>""<br><br><br>Regards,<br>OPS Team</p>"
                Send_Mail_OPS(email, email_cc, mail_subject, final_content )
                print "mail triggered"


### Create letters in LOB
for res in result:
	try:
		if str(res[0])=='85':
			DATA = {
                                        'CURDATE' : timestamp,
                                        'FULLNAME': str(res[1]),
                                        'ADDRESS1': str(res[2]),
                                        'ADDRESS2': str(res[3]),
                                        'PASTDUE' : str(res[5]),
                                        'UNPAIDFEES': str(res[6]),
                                        'PASTDUESINCE': str(res[7]),
                                        'TOTALDUE': str(res[8]),
                                        'LOANDATE': str(res[9]),
                                        'LOANTERM': str(res[10]),
                                        'LOANAMOUNT': str(res[11]),
                                        'LOANID' : str(res[4]),
                               }

			to_address = {
                                        'name': str(res[1]),
                                        'address_line1': str(res[2]),
                                        'address_city': str(res[13]),
                                        'address_state': str(res[14]),
                                        'address_zip': str(res[15]),
                                        'address_country': 'US',
				     }

			letter = Common.lob_letter(api_key,
                                           '/data/public/OPS/Automation/LOB/templates/Delinquency/Delin_85.html',
                                           'Delinquency Letter to '+str(res[1]),
                                           {'campaign': 'Delinquency Reminder'+str(res[0])},
                                           DATA,
                                           to_address
                                          )

			
		elif str(res[0])=='108':

			DATA = {
                                        'CURDATE' : timestamp,
                                        'FULLNAME': str(res[1]),
                                        'ADDRESS1': str(res[2]),
                                        'ADDRESS2': str(res[3]),
                                        'PASTDUE' : str(res[5]),
                                        'UNPAIDFEES': str(res[6]),
                                        'PASTDUESINCE': str(res[7]),
                                        'TOTALDUE': str(res[8]),
                                        'LOANID' : str(res[4]),
                                        'LOANDATE' : str(res[9]),
                               }

			to_address = {
                                        'name': str(res[1]),
                                        'address_line1': str(res[2]),
                                        'address_city': str(res[13]),
                                        'address_state': str(res[14]),
                                        'address_zip': str(res[15]),
                                        'address_country': 'US', }

                        letter = Common.lob_letter(api_key,
                                           '/data/public/OPS/Automation/LOB/templates/Delinquency/Delin_108.html',
                                           'Delinquency Letter to '+str(res[1]),
                                           {'campaign': 'Delinquency Reminder'+str(res[0])},
                                           DATA,
                                           to_address
                                          )

		elif str(res[0])=='55':

			DATA = {
                                        'CURDATE' : timestamp,
                                        'FULLNAME': str(res[1]),
                                        'ADDRESS1': str(res[2]),
                                        'ADDRESS2': str(res[3]),
                                        'PASTDUE' : str(res[5]),
                                        'UNPAIDFEES': str(res[6]),
                                        'PASTDUESINCE': str(res[7]),
                                        'TOTALDUE': str(res[8]),
                                        'LOANID' : str(res[4]),
                                        'PERDIEM' : str(res[12]),
                               }

			to_address = {
                                        'name': str(res[1]),
                                        'address_line1': str(res[2]),
                                        'address_city': str(res[13]),
                                        'address_state': str(res[14]),
                                        'address_zip': str(res[15]),
                                        'address_country': 'US', }

                        letter = Common.lob_letter(api_key,
                                           '/data/public/OPS/Automation/LOB/templates/Delinquency/Delin_55.html',
                                           'Delinquency Letter to '+str(res[1]),
                                           {'campaign': 'Delinquency Reminder'+str(res[0])},
                                           DATA,
                                           to_address
                                          )

		elif str(res[0])=='35':
                        DATA = {
                                        'CURDATE' : timestamp,
                                        'FULLNAME': str(res[1]),
                                        'ADDRESS1': str(res[2]),
                                        'ADDRESS2': str(res[3]),
                                        'PASTDUE' : str(res[5]),
                                        'UNPAIDFEES': str(res[6]),
                                        'PASTDUESINCE': str(res[7]),
                                        'TOTALDUE': str(res[8]),
                                        'LOANID' : str(res[4]),
                                        'PERDIEM' : str(res[12]),
                               }

                        to_address = {
                                        'name': str(res[1]),
                                        'address_line1': str(res[2]),
                                        'address_city': str(res[13]),
                                        'address_state': str(res[14]),
                                        'address_zip': str(res[15]),
                                        'address_country': 'US', }

                        letter = Common.lob_letter(api_key,
                                           '/data/public/OPS/Automation/LOB/templates/Delinquency/Delin_35.html',
                                           'Delinquency Letter to '+str(res[1]),
                                           {'campaign': 'Delinquency Reminder'+str(res[0])},
                                           DATA,
                                           to_address
                                          )

		print letter.url


                ### Save in DB
            	dbinsert="insert into reports.delinquencylobmailings (ContactID,LoanID,DaysPastDue,CreateDate,url) values ('"+str(res[17])+"','"+str(res[16])+"','"+str(res[0])+"','"+timestamp1+"','"+letter.url+"'); "
                cursor.execute(dbinsert);
                db.commit()

                ### Make note in sf record
                try:
                        title		= 'Day '+str(res[0])+' Delinquency Notice Mailed on '+str(timestamp2)
                        body 		= "Date of Letter:"+str(timestamp2)+".  "+str(res[0])+ " days past due.  Mailed to:"+str(res[2])+","+str(res[13])+","+str(res[14])+" "+str(res[15])+".  Delinquent Amount: $"+str(res[5])+".   Unpaid Fees:  $"+str(res[6])+".   Total Due: $"+str(res[8])
			parentid	= str(res[17])
                        result_New	= Common.sf_notes_creation(parentid, title, body)
                        print result_New
                except Exception as e:
                        print e
                        print "Problem with pushing "+str(res[17])+" task to Salesforce"

	except Exception, e:
		print e,'##########################'
		### Save error reposnse in DB
		dbinsert="insert into reports.delinquencylobmailings (ContactID,LoanID,DaysPastDue,CreateDate,error) values ('"+str(res[17])+"','"+str(res[16])+"','"+str(res[0])+"','"+timestamp1+"','"+str(e)+"'); "
                cursor.execute(dbinsert);
                db.commit()

db.close()
