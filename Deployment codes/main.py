import threading
import time

from darksky import forecast
from datetime import datetime, timedelta

import requests

import mysql.connector

import http.client

#salem = 11.664325, 78.146011
basel = 47.559, 7.588
KEY = "b06175c069028439ca7190d207348624"
threshold = 4.94
delay = 3600
#wind_D = 187.1295413753127
#darksky api caller
def Fetch_Darksky():
    try:
        ft = forecast(KEY, *basel, units='si')
        #print(ft['currently'])
        #ft = [ft['currently']['temperature'],ft['currently']['humidity'],ft['currently']['pressure'],wind_D]
        for i in ft['hourly']['data']:
            nt = datetime.now()
            if(datetime.strptime(time.ctime(i['time']), '%a %b %d %H:%M:%S %Y').hour == nt.hour) and (datetime.strptime(time.ctime(i['time']), '%a %b %d %H:%M:%S %Y').day == nt.day):
                ft = [i['temperature'], i['humidity'] , i['pressure'], i['windBearing'] ]
                break
        #print(ft)
        return ft
    except:
        return "Error in Fetching Data from Darksky..."

#predict using model
def Predict_WS(inp_data):
    try:
        url = 'http://127.0.0.1:5050/api'
        r = requests.post(url,json={'val':inp_data,})
        return r.json()
    except:
        return None 

#insertion to db
def Insert_Predicted(curnt_time, tph_data, ws):
    try:
        mydb = mysql.connector.connect(host='localhost', user='root', passwd='', database='wind_database')
        mycursor = mydb.cursor()
        sql = "insert into thpwswd (datetime, temperature, humidity, pressure, windspeed, winddirection) values (%s,%s,%s,%s,%s,%s)"
        mycursor.execute(sql,[curnt_time]+tph_data[:-1]+[ws]+[tph_data[-1]])
        mydb.commit()
        mydb.close()
        return 1
    except:
        return 0
    
#sending alert sms
def Send_AlertSms(ws, nexttime):
    #reading contacts from database
    try:
        mydb = mysql.connector.connect(host='localhost', user='root', passwd='', database='wind_database')
        mycursor = mydb.cursor()
        #sql = "SELECT Mobile_No FROM register_users where First_Name="+"\"inr\""
        sql = "SELECT Mobile_No FROM register_users"
        mycursor.execute(sql)
        res = mycursor.fetchall()
        xml_mobile = ""
        for row in res:
            #print(str(row[0]))
            xml_mobile="<ADDRESS TO=\""+str(row[0])+"\"></ADDRESS>"+xml_mobile
        mydb.close()
        
        #send sms via api
        conn = http.client.HTTPSConnection("control.msg91.com")
        payload = "<MESSAGE> <AUTHKEY>263841AMUdKFba3j5c6cf557</AUTHKEY> <SENDER>WALERT</SENDER><ROUTE>4</ROUTE> <CAMPAIGN>XML API</CAMPAIGN> <COUNTRY>91</COUNTRY> <SMS TEXT=\"WindSpeed will be "+str(ws)[:4]+" km/h at "+nexttime+"\">"+xml_mobile+"</SMS></MESSAGE>"
        headers = { 'content-type': "application/xml" }
        conn.request("POST", "/api/postsms.php", payload, headers)
        res = conn.getresponse()
        data = res.read()
        return data.decode("utf-8")
    except:
        return None
          
#Main thread
class Perform_Predict (threading.Thread):
    def __init__(self, name, delay):
        threading.Thread.__init__(self)
        self.name = name
        self.delay = delay
    def run(self):
        print("Starting ")
        print("---------")
        Predict_Start(self.name, self.delay)
        print ("Exiting ")
        
def Predict_Start(name, delay):
    while 1:
        next_time = time.time() + delay
        print ("Module started... Please wait...")
        #current time
        curnt_time = datetime.now().strftime('%Y-%m-%d %H:00:00')
        
        #fetching realtime data
        tph_data = Fetch_Darksky()
        tph_cnt = 3
        while(type(tph_data)!=list and tph_cnt>0):
            #print("\n",tph_data);
            print("Trying again...")
            tph_data = Fetch_Darksky()
            tph_cnt-=1
        if(type(tph_data)!=list):
            print("Error In Darksky Api... Data Fetch failed")
        else:
            print("\nLive data fetched... ")
            #prediction of windspeed using fetched data
            ws = Predict_WS(tph_data)
            ws_cnt = 3
            while(ws==None and ws_cnt>0):
                print("\n Error in Ml Model");
                print("Trying again...")
                ws =Predict_WS(tph_data)
                ws_cnt-=1
            if(ws==None):
                print("Error In Ml Model... Prediction failed")
            else:
                print(curnt_time)
                print("\nPredicted Windspeed : ",ws,"km/h")
                 
                #insertion to database
                #print([curnt_time]+tph_data[:-1]+[ws]+[tph_data[-1]])
                db_ins = Insert_Predicted(curnt_time, tph_data, ws)
                db_ins_cnt = 3
                while(db_ins==0 and db_ins_cnt>0):
                    print("\n Error in inserting predicted data to database");
                    print("Trying again...")
                    db_ins = Insert_Predicted(curnt_time, tph_data, ws)
                    db_ins_cnt-=1
                if(db_ins==0):
                    print("Error in inserting predicted data to database failed")
                else:
                    print("Successfully inserted predicted data to database !\n")
                    if(ws>=threshold):
                        #print(next_time, type(next_time), time.ctime(next_time))
                        #datetime.strptime(time.ctime(next_time), '%a %b %d %H:%M:%S %Y')
                        sms = Send_AlertSms(ws, str(time.ctime(next_time)))
                        sms_cnt = 3
                        while(sms==None and sms_cnt>0):
                            print("\n Error in sending sms... Trying again...");
                            sms = Send_AlertSms(ws, str(time.ctime(next_time)))
                            sms_cnt-=1
                        if(ws==None):
                            print("Error In sending sms... Alert Failed !")
                        else:
                            try:
                                print("Alert sms [%s] sent successfully..."%(sms))
                                mydb = mysql.connector.connect(host='localhost', user='root', passwd='', database='wind_database')
                                mycursor = mydb.cursor()
                                sql = "insert into sms (req_id) values (%s)"
                                mycursor.execute(sql,[sms])
                                mydb.commit()
                                mydb.close()
                            except:
                                print("Error In inserting sms req_id[%s] to Database..."%(sms))
                        
        #next prediction time
        print("\nNext prediction starts at ", time.ctime(next_time))                
        
        #sleeps until next prediction
        time.sleep(max(0, next_time - time.time()))
        
# Create new threads
thread1 = Perform_Predict('T1', delay)

# Start new Threads
thread1.start()
thread1.join()
print ("Exiting Main Thread")