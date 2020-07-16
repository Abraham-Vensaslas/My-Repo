import sys
import requests
import MySQLdb
from datetime import datetime
import json
import datetime
from datetime import *
from pytz import timezone
from time import gmtime, strftime
sys.path.append("/public/OPS/Utils/")
sys.path.append("/public/OPS/Config/")
import TimeConfig as tc
from MailUtils import *
from Email_table_03  import *
date_format='%m/%d/%Y %H:%M:%S %Z'
date = datetime.now()

my_timezone=timezone('US/Pacific')

date = my_timezone.localize(date)
date = date.astimezone(my_timezone)

from collections import defaultdict


def dbconn():
    db = MySQLdb.connect('reports-rds.adfdata.net', 'reports', 'reports123', 'decision', port=3306)
    cursor = db.cursor()
    cur = db.cursor(MySQLdb.cursors.DictCursor)
    return cursor, cur


def func():
    try:
        cursor, cur = dbconn()
        query = """select a.Value tucl,b.Value implicit,s.OfferAPR,min(c.estimatedapr),an.leadID as APR,count(c.estimatedapr) ct from decision.lead l left join decision.datavalues a on (l.id=a.lead_id) left join decision.datavalues b using (lead_id) join decision.application an using (lead_id) join decision.contact o using (lead_id) join decision.offer c using (lead_id) join decision.PreScreen s on (left(b.leadID,12)=s.PromoCode) where a.Source='Model' and a.Field='tucl_rt_v6_0_1_ps_score'and b.Source='OFFERDROPMODEL' and b.Field='implicit_risk_model_v1_0_2_score'  and c.isSelected=1  and o.Military=0 and l.Campaign in ('2020C1') and a.leadID not in('635891209916@20C1','635889212890@20C1','637969128330@20C1') group by a.lead_id ;"""
        cursor.execute(query);
        result = cursor.fetchall()
        print result
	a=[]
        for j in result:
            Implicit =float(j[1])
            Tucl =float(j[0])
            offer_APR =float(j[2])
            rn = 82
            Implicit_risk_model = defaultdict(lambda: i, {Implicit > 834: '10',
                                                          Implicit > 781 and Implicit <= 834: '9',
                                                          Implicit > 736 and Implicit <= 781: '8',
                                                          Implicit > 696 and Implicit <= 736: '7',
                                                          Implicit > 655 and Implicit <= 696: '6',
                                                          Implicit > 613 and Implicit <= 655: '5',
                                                          Implicit > 567 and Implicit <= 613: '4',
                                                          Implicit > 513 and Implicit <= 567: '3',
                                                          Implicit > 434 and Implicit <= 513: '2',
                                                          Implicit <= 434: '1'})

            TUCL_RT = defaultdict(lambda: t, {Tucl > 880: '0.5',
                                              Tucl > 867 and Tucl <= 880: '1',
                                              Tucl > 847 and Tucl <= 867: '2',
                                              Tucl > 829 and Tucl <= 847: '3',
                                              Tucl > 812 and Tucl <= 829: '4',
                                              Tucl > 793 and Tucl <= 812: '5',
                                              Tucl > 772 and Tucl <= 793: '6',
                                              Tucl > 744 and Tucl <= 772: '7',
                                              Tucl > 703 and Tucl <= 744: '8',
                                              Tucl > 614 and Tucl <= 703: '9',
                                              Tucl <= 613: '10'})

            R = Implicit_risk_model[True]
            T = TUCL_RT[True]
            output = "R%sT%s" % (R, T)

            if (offer_APR < 36):

                if (Tucl >= 800):

                    APR = {"R1T0.5":35.5,"R2T0.5":35.5,"R3T0.5":35.5,"R4T0.5":35.5,"R5T0.5":35.5,"R6T0.5":35.5,"R7T0.5":35.5,"R8T0.5":35.5,"R9T0.5":35.5,"R10T0.5":35.5,"R1T1":35.5,"R2T1":35.5,"R3T1":35.5,"R4T1":35.5,"R5T1":35.5,"R6T1":35.5,"R7T1":35.5,"R8T1":35.5,"R9T1":35.5,"R10T1":35.5,"R1T2":35.5,"R2T2":35.5,"R3T2":35.5,"R4T2":35.5,"R5T2":35.5,"R6T2":35.5,"R7T2":35.5,"R8T2":35.5,"R9T2":35.5,"R10T2":35.5,"R1T3":35.5,"R2T3":35.5,"R3T3":35.5,"R4T3":35.5,"R5T3":35.5,"R6T3":35.5,"R7T3":35.5,"R8T3":35.5,"R9T3":35.5,"R10T3":35.5,"R1T4":35.5,"R2T4":35.5,"R3T4":35.5,"R4T4":35.5,"R5T4":35.5,"R6T4":35.5,"R7T4":35.5,"R8T4":35.5,"R9T4":35.5,"R10T4":35.5,"R1T5":35.5,"R2T5":35.5,"R3T5":35.5,"R4T5":35.5,"R5T5":35.5,"R6T5":35.5,"R7T5":35.5,"R8T5":35.5,"R9T5":35.5,"R10T5":35.5,"R1T6":35.5,"R2T6":35.5,"R3T6":35.5,"R4T6":35.5,"R5T6":35.5,"R6T6":35.5,"R7T6":35.5,"R8T6":35.5,"R9T6":35.5,"R10T6":35.5,"R1T7":35.5,"R2T7":35.5,"R3T7":35.5,"R4T7":35.5,"R5T7":35.5,"R6T7":35.5,"R7T7":35.5,"R8T7":35.5,"R9T7":35.5,"R10T7":35.5,"R1T8":35.5,"R2T8":35.5,"R3T8":35.5,"R4T8":35.5,"R5T8":35.5,"R6T8":35.5,"R7T8":35.5,"R8T8":35.5,"R9T8":35.5,"R10T8":35.5,"R1T9":35.5,"R2T9":35.5,"R3T9":35.5,"R4T9":35.5,"R5T9":35.5,"R6T9":35.5,"R7T9":35.5,"R8T9":35.5,"R9T9":35.5,"R10T9":35.5,"R1T10":35.5,"R2T10":35.5,"R3T10":35.5,"R4T10":35.5,"R5T10":35.5,"R6T10":35.5,"R7T10":35.5,"R8T10":35.5,"R9T10":35.5,"R10T10":35.5}

                    op1 =APR.get(output)


                if(float(str(j[3]))!= float(str(op1))):
                	result=str(j[4]),str(j[3]),str(op1)
                	print result
                	a.append(result)
                	print a


            elif (offer_APR > 36) and (offer_APR < 100):

                if (Tucl >= 613):
                    APR = {"R1T0.5":35.5,"R2T0.5":35.5,"R3T0.5":35.5,"R4T0.5":49,"R5T0.5":49,"R6T0.5":49,"R7T0.5":49,"R8T0.5":59.5,"R9T0.5":59.5,"R10T0.5":59.5,"R1T1":49,"R2T1":49,"R3T1":49,"R4T1":49,"R5T1":49,"R6T1":59.5,"R7T1":59.5,"R8T1":59.5,"R9T1":59.5,"R10T1":59.5,"R1T2":49,"R2T2":49,"R3T2":59.5,"R4T2":59.5,"R5T2":59.5,"R6T2":79,"R7T2":79,"R8T2":79,"R9T2":79,"R10T2":79,"R1T3":59.5,"R2T3":79,"R3T3":79,"R4T3":79,"R5T3":89,"R6T3":89,"R7T3":89,"R8T3":98.5,"R9T3":98.5,"R10T3":98.5,"R1T4":79,"R2T4":79,"R3T4":89,"R4T4":98.5,"R5T4":98.5,"R6T4":98.5,"R7T4":98.5,"R8T4":98.5,"R9T4":98.5,"R10T4":98.5,"R1T5":79,"R2T5":89,"R3T5":98.5,"R4T5":98.5,"R5T5":98.5,"R6T5":98.5,"R7T5":98.5,"R8T5":98.5,"R9T5":98.5,"R10T5":98.5,"R1T6":89,"R2T6":98.5,"R3T6":98.5,"R4T6":98.5,"R5T6":98.5,"R6T6":98.5,"R7T6":98.5,"R8T6":98.5,"R9T6":98.5,"R10T6":98.5,"R1T7":98.5,"R2T7":98.5,"R3T7":98.5,"R4T7":98.5,"R5T7":98.5,"R6T7":98.5,"R7T7":98.5,"R8T7":98.5,"R9T7":98.5,"R10T7":98.5,"R1T8":98.5,"R2T8":98.5,"R3T8":98.5,"R4T8":98.5,"R5T8":98.5,"R6T8":98.5,"R7T8":98.5,"R8T8":98.5,"R9T8":98.5,"R10T8":98.5,"R1T9":98.5,"R2T9":98.5,"R3T9":98.5,"R4T9":98.5,"R5T9":98.5,"R6T9":98.5,"R7T9":98.5,"R8T9":98.5,"R9T9":98.5,"R10T9":98.5,"R1T10":98.5,"R2T10":98.5,"R3T10":98.5,"R4T10":98.5,"R5T10":98.5,"R6T10":98.5,"R7T10":98.5,"R8T10":98.5,"R9T10":98.5,"R10T10":98.5}


                    op1 = APR.get(output)



                if(float(str(j[3]))!= float(str(op1))):
                        result=str(j[4]),str(j[3]),str(op1)
                        print result
                        a.append(result)
                        print a

            elif (offer_APR > 100):

                if (Tucl >= 450):
                    APR = {"R1T0.5":79.0,"R2T0.5":79.0,"R3T0.5":79.0,"R4T0.5":79.0,"R5T0.5":89.0,"R6T0.5":89.0,"R7T0.5":89.0,"R8T0.5":89.0,"R9T0.5":89.0,"R10T0.5":89.0,"R1T1":79.0,"R2T1":79.0,"R3T1":89.0,"R4T1":89.0,"R5T1":89.0,"R6T1":89.0,"R7T1":89.0,"R8T1":89.0,"R9T1":89.0,"R10T1":89.0,"R1T2":89.0,"R2T2":89.0,"R3T2":98.5,"R4T2":98.5,"R5T2":98.5,"R6T2":98.5,"R7T2":98.5,"R8T2":98.5,"R9T2":98.5,"R10T2":98.5,"R1T3":98.5,"R2T3":98.5,"R3T3":98.5,"R4T3":98.5,"R5T3":98.5,"R6T3":98.5,"R7T3":98.5,"R8T3":98.5,"R9T3":98.5,"R10T3":98.5,"R1T4":98.5,"R2T4":98.5,"R3T4":98.5,"R4T4":98.5,"R5T4":98.5,"R6T4":98.5,"R7T4":98.5,"R8T4":98.5,"R9T4":98.5,"R10T4":98.5,"R1T5":98.5,"R2T5":98.5,"R3T5":98.5,"R4T5":98.5,"R5T5":98.5,"R6T5":98.5,"R7T5":98.5,"R8T5":98.5,"R9T5":98.5,"R10T5":98.5,"R1T6":98.5,"R2T6":98.5,"R3T6":98.5,"R4T6":98.5,"R5T6":98.5,"R6T6":98.5,"R7T6":98.5,"R8T6":98.5,"R9T6":98.5,"R10T6":98.5,"R1T7":139.0,"R2T7":139.0,"R3T7":139.0,"R4T7":139.0,"R5T7":139.0,"R6T7":139.0,"R7T7":139.0,"R8T7":139.0,"R9T7":139.0,"R10T7":139.0,"R1T8":139.0,"R2T8":139.0,"R3T8":139.0,"R4T8":139.0,"R5T8":149.0,"R6T8":149.0,"R7T8":149.0,"R8T8":149.0,"R9T8":149.0,"R10T8":149.0,"R1T9":149.0,"R2T9":149.0,"R3T9":149.0,"R4T9":159.0,"R5T9":159.0,"R6T9":159.0,"R7T9":159.0,"R8T9":159.0,"R9T9":179.5,"R10T9":179.5,"R1T10":159.0,"R2T10":179.5,"R3T10":179.5,"R4T10":179.5,"R5T10":179.5,"R6T10":179.5,"R7T10":179.5,"R8T10":179.5,"R9T10":179.5,"R10T10":179.5}

                    op1 =APR.get(output)

                if(float(str(j[3]))!= float(str(op1))):
                        result=str(j[4]),str(j[3]),str(op1)
                        print result
                        a.append(result)
                        print a

        heads = ['leadID','APR generated','APR as per UW']
        table = gen_table_abandoned(heads,a)
        mail_subject =  ' Discrepancies in PS-FEB  APR '+date.strftime(date_format)
        email = ['abrahamv@applieddatafinance.com']
        final_content = "<p>Hi Team,<br><br> Please verify generated APR for the below leads. Kindly verfiy and escalate  <br><br>"+table+"<br><br>Regards,<br>OPS Team</p>"
        no_content = "<p>Hi Team,<br><br>******* We don't have any discrepancies in APR for the day *******<br><br>Regards<br>OPS Team</p>"
        if result == ():
                Send_Mail_OPS(email,[],mail_subject,no_content)
                print ('******* We are not having any discrepancies in APR for the day  *******')
        else:
                Send_Mail_OPS(email, [], mail_subject, final_content )



    except Exception as exe:
        print exe







if __name__ == "__main__":
    func()


