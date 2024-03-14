import mysql.connector
import time

awsdb = mysql.connector.connect(
                host="43.205.196.66",
                user="emsroot",
                password="22@teneT",
                database='EMS',
                port=3307
            )

unprocesseddb = mysql.connector.connect(
            host="121.242.232.151",
            user="ltouser",
            password="ltouser@151",
            database='bmsmgmtprodv13',
            port=3306
        )

while True:
    awscur = awsdb.cursor()

    ltocur = unprocesseddb.cursor()

    awscur.execute("select polledTime from EMS.LTOMinWise where Energy IS NOT NULL and packSoc IS NOT NULL order by polledTime desc limit 1;")

    timres = awscur.fetchall()

    if len(timres) > 0:
        timer = timres[0][0]
        ltocur.execute(f"select batteryStatus,chargingEnergy,dischargingEnergy,packUsableSOC,recordTimestamp from ltoBatteryData where recordTimestamp >= '{timer}';")
    else:
        ltocur.execute("""select batteryStatus,chargingEnergy,dischargingEnergy,packUsableSOC,recordTimestamp from ltoBatteryData where date(recordTimestamp) >= curdate()""")

    results = ltocur.fetchall()

    chgMin = {}
    dchgMin = {}
    idleMin = {}
    packMin = {}

    def segPackMin(polledTime,packSoc):
        polledTime = str(polledTime)[0:17]+"00"

        if polledTime in packMin.keys():
            packMin[polledTime].append(packSoc)
        else:
            packMin[polledTime] = [packSoc]

    def segChargeMin(polledTime,Energy):
        polledTime = str(polledTime)[0:17]+"00"

        if polledTime in chgMin.keys():
            chgMin[polledTime] += Energy
        else:
            chgMin[polledTime] = Energy
    
    def segDischargeMin(polledTime,Energy):
        polledTime = str(polledTime)[0:17]+"00"

        if polledTime in dchgMin.keys():
            dchgMin[polledTime] += Energy
        else:
            dchgMin[polledTime] = Energy
        
    def segIdleMin(polledTime,Energy):
        # print(polledTime,Energy)
        polledTime = str(polledTime)[0:17]+"00"

        if polledTime in idleMin.keys():
            idleMin[polledTime] += Energy
        else:
            idleMin[polledTime] = Energy

    for i in range(1,len(results)):
        if results[i][1] != None and results[i-1][1] != None:
            if results[i][0] == "CHG":
                segChargeMin(results[i][4],results[i][1]-results[i-1][1])
        if results[i][2] != None and results[i-1][2] != None:
            if results[i][0] == "DCHG":
                segDischargeMin(results[i][4],results[i][2]-results[i-1][2])
        if results[i][0] == "IDLE":
            segIdleMin(results[i][4],0)
        if results[i][3] != None:
            if results[i][3] <= 100 and results[i][3] >= 0:
                segPackMin(results[i][4],results[i][3])

    for i in dchgMin.keys():
        val = (i,-abs(dchgMin[i]/1000),'DCHG')
        sql = "INSERT INTO EMS.LTOMinWise(polledTime,Energy,batterySts) VALUES(%s,%s,%s)"
        try:
            awscur.execute(sql,val)
            awsdb.commit()
            print(val)
            print("LTO min wise data inserted")
        except mysql.connector.errors.IntegrityError:
            sql = "UPDATE EMS.LTOMinWise SET Energy = %s , batterySts = %s WHERE polledTime = %s"
            val = (-abs(dchgMin[i]/1000),'DCHG',i)
            awscur.execute(sql,val)
            awsdb.commit()
            print(val)
            print("LTO min wise data inserted")

    for i in chgMin.keys():
        val = (i,chgMin[i]/1000,'CHG')
        sql = "INSERT INTO EMS.LTOMinWise(polledTime,Energy,batterySts) VALUES(%s,%s,%s)"
        try:
            awscur.execute(sql,val)
            awsdb.commit()
            print(val)
            print("LTO min wise data inserted")
        except mysql.connector.errors.IntegrityError:
            sql = "UPDATE EMS.LTOMinWise SET Energy = %s, batterySts = %s WHERE polledTime = %s"
            val = (chgMin[i]/1000,'CHG',i)
            awscur.execute(sql,val)
            awsdb.commit()
            print(val)
            print("LTO min wise data inserted")
    
    for i in idleMin.keys():
        val = (i,idleMin[i]/1000,'IDLE')
        sql = "INSERT INTO EMS.LTOMinWise(polledTime,Energy,batterySts) VALUES(%s,%s,%s)"
        try:
            awscur.execute(sql,val)
            awsdb.commit()
            print(val)
            print("LTO min wise data inserted")
        except mysql.connector.errors.IntegrityError:
            sql = "UPDATE EMS.LTOMinWise SET Energy = %s, batterySts = %s WHERE polledTime = %s"
            val = (idleMin[i]/1000,'IDLE',i)
            awscur.execute(sql,val)
            awsdb.commit()
            print(val)
            print("LTO min wise data inserted")

    for i in packMin.keys():
        val = (i,max(packMin[i]))
        sql = "INSERT INTO EMS.LTOMinWise(polledTime,packSoc) VALUES(%s,%s)"
        try:
            awscur.execute(sql,val)
            awsdb.commit()
            print(val)
            print("LTO min wise data inserted")
        except mysql.connector.errors.IntegrityError:
            sql = "UPDATE EMS.LTOMinWise SET packSoc = %s WHERE polledTime = %s"
            val = (max(packMin[i]),i)
            awscur.execute(sql,val)
            awsdb.commit()
            print(val)
            print("LTO min wise data inserted")
    
    
    time.sleep(180)