import mysql.connector
# from datetime import date
import time

while True:
    emsdb = mysql.connector.connect(
                    host="43.205.196.66",
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                )

    try: 
        unprocesseddb = mysql.connector.connect(
		host="121.242.232.151",
		user="bmsrouser6",
		password="bmsrouser6@151U",
		database='bmsmgmtprodv13',
		port=3306
	      )
    except Exception as ex:
        print(ex)
         
    if 1==1:
        
        unproscur = unprocesseddb.cursor()
        emscur = emsdb.cursor()
        
        unproscur.execute("select schneidertotalpowerfactor,schneiderpolledtimestamp,schneidersubsystemid from bmsmgmtprodv13.schneider7230readings where schneidersubsystemid=346 and date(schneiderpolledtimestamp) = date(curdate()-1);")

        res = unproscur.fetchall()
        print(res)
        power_dict ={}

        def SummarizeHour(powerfact,polledTime):
            hour = str(polledTime)[0:14] + "00:00"
            if hour in power_dict.keys():
                power_dict[hour].append(powerfact)
            else:
                power_dict[hour] = [powerfact]

        for i in res:
            SummarizeHour(i[0],i[1])
        
        for i in power_dict.keys():
            avgpw = (round(sum(power_dict[i])/len(power_dict[i]),3))
            minpw = (min(power_dict[i]))
            polledTime = i
            print(avgpw,polledTime)
        
        for i in power_dict.keys():
            avgpw = (round(sum(power_dict[i])/len(power_dict[i]),3))
            minpw = (min(power_dict[i]))
            polledTime = i
           
            sql = "INSERT INTO schneider7230processed(polledTime,avgpowerfactor,minpowerfactor) VALUES(%s,%s,%s)"
            val = (polledTime,avgpw,minpw)
            print(val)
            try:
                emscur.execute(sql,val)
                print("powerfactor hourly Processed data written")
                emsdb.commit()
                continue
            except mysql.connector.IntegrityError:
                sql = """UPDATE schneider7230processed SET avgpowerfactor = %s, minpowerfactor=%s WHERE polledTime = %s"""
                val = (avgpw,minpw,polledTime)
                print(val)
                emscur.execute(sql,val)
                print("powerfactor hourly Processed data updated")
                emsdb.commit()
                continue

    time.sleep(300)