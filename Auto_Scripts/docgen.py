import sys
import requests
import MySQLdb, logging
from datetime import date
import json
import csv
import os
template="CRD_SCORE.html"



class Main:


    def __init__(self):
        
        try:
            db = MySQLdb.connect('reports-rds.adfdata.net','reports','reports123','decision')
            cursor = db.cursor()
            cur = db.cursor(MySQLdb.cursors.DictCursor)
            return cursor, cur
            print("DB connection established")
        except Exception as e:
            print(e)

    def data_read_acc(self):
    
        try:
            temp=list()
            with open('aa.csv', 'r') as csvFile:
                reader = csv.reader(csvFile)
                for row in reader:
                    temp.append(row)
                csvFile.close()
            del temp[0]
            return temp
            print("Data retrived successfully")
         except Exception as e:
            print(e)
            

    def TriggerReact():
        try:
#           cursor, cur = dbconn()
#           query = """select * from reports.NOD_issue where lead_id ='2417685' ; """

#           cursor.execute(query)
#           result = cursor.fetchall()
            DE_API='http://prod-pydoc.adfdata.net:8085/docgen/api/v1/pdf'
            headers = {'Authorization' : 'Basic VEVTVEVSOlRFU1RFUg==', 'Content-Type' : 'application/json'}

#           for i in result:
#               print (i[0])

#               payload = {"outputfile": i[5],"template": "CRD_SCORE.html","docValues": {"scoredate01": i[3],"name01": i[4],"score01": i[2],"title01": i[8]}}
        

#               payload =  {"outputfile": "CRD_P14170307685397671.pdf", "template": "CRD_SCORE.html", "docValues": {"scoredate01": "2019-12-01", "score01": "+579", "title01": "Personify Financial on behalf of First Electronic Bank", "name01": "Stacey Rene"}}

    #           payload = {"outputfile":"SC1F2019050600004A_R.html","template":"SCScenario1_PRE_TSR.html","docValues":{"agreementdate01":"May-07-2019","esigndate01":"Dec-17-2019","paymentdate01":"May-24-2019","loannumber01":"2019050600004A","name01":"Bobby Lee Thompson","address01":"205 Hauser Street","city01":"Sumter","state01":"SC","zipcode01":"29150-5823","phone01":"803-968-3094","rate01":"24.90","rate02":"25.00","rate03":"","financechg01":"68.36","amountfin01":"500.00","totalpayments01":"568.36","numberofpay01":"25","numberofpay02":"1","numberofpay03":"","numberofpay04":"","numberofpay05":"","numberofpay06":"","numberofpay07":"","numberofpay08":"","numberofpay09":"","amountofpayments01":"21.86","amountofpayments02":"21.86","amountofpayments03":"","amountofpayments04":"","amountofpayments05":"","amountofpayments06":"","amountofpayments07":"","amountofpayments08":"","amountofpayments09":"","paymentduedate01":"May-24-2019","paymentduedate02":"May-08-2020","paymentduedate03":"","paymentduedate04":"","paymentduedate05":"","paymentduedate06":"","paymentduedate07":"","paymentduedate08":"","paymentduedate09":"","itemization01":"500.00","amountgiven01":"500.00","amountfin02":"500.00","frequency":"Every two weeks","ActiveDuty":"","NOT":"am NOT","disbursementdate01":"May-07-2019","originationfee01":"0.00","eligiblenumberofpayments":"","totalnumpayments":"","actualtotalpayments01":"","actualfinancechg01":"","reducedrate":"","successivereducedrate":"","successiveeligiblenumberofpayments":"","maxpossiblereduction":"","amountpaid01":"0.00","amountpaidothers":"0","totalamountfinanced01":"500.00","principalbalance01":"500.00"}}

            print('Before....')
            data = json.dumps(payload)
            print payload
            print "curl -H 'Content-Type: application/json' -X POST -d '"+data+"' "+DE_API+""
            req = requests.post(DE_API,auth=('TESTER', 'TESTER'),data=json.dumps(payload), headers=headers)
            r=os.popen("curl -H 'Content-Type: application/json' -X POST -d '"+data+"' "+DE_API+"").read()
            print r
            print('After....')
#           print (req.json())
        except Exception as exe :
            print exe
            
            
            
if __name__ == "__main__":
    #data_read_acc()
    TriggerReact()










