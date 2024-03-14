import mysql.connector
import time
from datetime import datetime, timedelta

while True:
    processeddb = mysql.connector.connect(
            host="121.242.232.151",
            user="emsrouser",
            password="emsrouser@151",
            database='bmsmgmt_olap_prod_v13',
            port=3306
            )
    
    emsdb = mysql.connector.connect(
                    host="43.205.196.66",
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                )
    
    emscur = emsdb.cursor()
    
    processcur = processeddb.cursor()

    processcur.execute("""select FLOOR(acmeterenergy),polledTime from bmsmgmt_olap_prod_v13.MVPPolling where mvpnum in ("MVP1","MVP2","MVP3","MVP4") and Date(polledTime) = date(curdate()-1);""")

    data = processcur.fetchall()
    minute_list =[]
    hour_dict = {}
    fin_hour = {}
    minWisedict = {}

    def minSeg(Energy,polledTime):
        polled = str(polledTime)[0:17]+"00"
        # print(Energy,polled)

        if Energy != None:
            if polled in minWisedict.keys():
                minWisedict[polled] += Energy
            else:
                minWisedict[polled] = Energy
    
    def HourSeg(polledTime,Energy):
        polled = polledTime[0:14]+"00:00"
        
        if polled in hour_dict.keys():
            hour_dict[polled].append(Energy)
        else:
            hour_dict[polled] = [Energy] 
        
    for i in data:
        # print("entry")
        minSeg(i[0],i[1])
    
    for i in minWisedict.keys():
        HourSeg(i,minWisedict[i])
    
    for j in hour_dict.keys():
        li = hour_dict[j]

        for i in range(1,len(li)):
            energy = (li[i]-li[i-1])/1000

            if energy > 0 and energy < 500:
                if j in fin_hour.keys():
                    fin_hour[j].append(energy)
                else:
                    fin_hour[j] = [energy]
    
    for i in fin_hour.keys():
        val = (i,sum(fin_hour[i]))

        sql = "INSERT INTO Gridhourly(polledTime,Energy) VALUES(%s,%s)"
        # val = (i,hour_dict[i])
        print(val)
        try:
            emscur.execute(sql,val)
            emsdb.commit()
            print("Grid hourly")
        except mysql.connector.errors.IntegrityError:
            sql = "UPDATE Gridhourly SET Energy = %s WHERE polledTime = %s"
            val = (sum(fin_hour[i]),i)
            print(val)
            emscur.execute(sql,val)
            emsdb.commit()
            print("Grid hourly")
    
    
    time.sleep(100)


