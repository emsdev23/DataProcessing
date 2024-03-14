import mysql.connector
import time

while True:

    try:
        emsdb = mysql.connector.connect(
            host="121.242.232.211",
            user="emsroot",
            password="22@teneT",
            database='EMS',
            port=3306
        )

        awsdb = mysql.connector.connect(
                host="43.205.196.66",
                user="emsroot",
                password="22@teneT",
                database='EMS',
                port=3307
            )

        emscur = emsdb.cursor()

        awscur = awsdb.cursor()
        
    except mysql.connector.Error as e:
        print("mysql connection error")
        continue

    emscur.execute("SELECT batteryStatus,chargingEnergy,dischargingEnergy,packUsableSoc,recordTimestamp FROM EMS.ioeSt2BatteryData where date(recordTimestamp) = curdate();")

    str1res = emscur.fetchall()

    chgMinst2 = {}
    dchgMinst2 = {}
    idleMinst2 = {}
    packMinst2 = {}

    def segIdleMin(Energy,polledTime):
        polledTime = str(polledTime)[0:17]+"00"

        if polledTime in idleMinst2.keys():
            idleMinst2[polledTime] += Energy
        else:
            idleMinst2[polledTime] = Energy

    def segPackMinst2(packSoc,polledTime):
        polledTime = str(polledTime)[0:17]+"00"

        if polledTime in packMinst2.keys():
            packMinst2[polledTime].append(packSoc)
        else:
            packMinst2[polledTime] = [packSoc]

    def segChargeMinst2(Energy,polledTime):
        polledTime = str(polledTime)[0:17]+"00"

        if polledTime in chgMinst2.keys():
            chgMinst2[polledTime] += Energy
        else:
            chgMinst2[polledTime] = Energy
    
    def segDischargeMinst2(Energy,polledTime):
        polledTime = str(polledTime)[0:17]+"00"

        if polledTime in dchgMinst2.keys():
            dchgMinst2[polledTime] += Energy
        else:
            dchgMinst2[polledTime] = Energy

    def segIdleMinst2(Energy,polledTime):
        # print(polledTime,Energy)
        polledTime = str(polledTime)[0:17]+"00"

        if polledTime in idleMinst2.keys():
            idleMinst2[polledTime] += Energy
        else:
            idleMinst2[polledTime] = Energy

    for i in range(1,len(str1res)):
        if str1res[i][0] == 'CHG':
            if str1res[i][1] != None and str1res[i-1][0] != None:
                segChargeMinst2(str1res[i][1]-str1res[i-1][1],str1res[i][4])
        if str1res[i][0] == 'DCHG':
            if str1res[i][1] != None and str1res[i-1][0] != None:
                segDischargeMinst2(str1res[i][1]-str1res[i-1][1],str1res[i][4])
        if str1res[i][0] == 'IDLE':
            segIdleMinst2(0,str1res[i][4])
        if str1res[i][3] != None:
            if str1res[i][3] <= 100 and str1res[i][3] >= 0:
                segPackMinst2(str1res[i][3],str1res[i][4])

    for i in chgMinst2.keys():
        val = (i,chgMinst2[i]/1000)
        # print(val)
        sql = "INSERT INTO EMS.ioeMinWise(polledTime,Energyst2) values(%s,%s)"
        try:
            awscur.execute(sql,val)
            print(val)
            awsdb.commit()
            print("String2 chg min wise inserted")
        except mysql.connector.IntegrityError:
            sql = "UPDATE EMS.ioeMinWise SET Energyst2 = %s WHERE polledTime = %s"
            val = (chgMinst2[i]/1000,i)
            awscur.execute(sql,val)
            awsdb.commit()
            print(val)
            print("String2 chg min wise updated")

    for i in dchgMinst2.keys():
        val = (i,-abs(dchgMinst2[i]/1000))
        print(val)
        sql = "INSERT INTO EMS.ioeMinWise(polledTime,Energyst2) values(%s,%s)"
        try:
            awscur.execute(sql,val)
            print(val)
            awsdb.commit()
            print("String2 min dchg wise inserted")
        except mysql.connector.IntegrityError:
            sql = "UPDATE EMS.ioeMinWise SET Energyst2 = %s WHERE polledTime = %s"
            val = (-abs(dchgMinst2[i]/1000),i)
            awscur.execute(sql,val)
            awsdb.commit()
            print(val)
            print("String2 min dchg wise updated")
    
    for i in idleMinst2.keys():
        val = (i,idleMinst2/1000)
        print(val)
        sql = "INSERT INTO EMS.ioeMinWise(polledTime,Energyst2) values(%s,%s)"
        try:
            awscur.execute(sql,val)
            print(val)
            awsdb.commit()
            print("String2 min idle wise inserted")
        except mysql.connector.IntegrityError:
            sql = "UPDATE EMS.ioeMinWise SET Energyst2 = %s WHERE polledTime = %s"
            val = (idleMinst2[i]/1000,i)
            awscur.execute(sql,val)
            awsdb.commit()
            print(val)
            print("String2 min idle wise updated")
        
    for i in packMinst2.keys():
        val = (i,max(packMinst2[i]))
        print(val)
        sql = "INSERT INTO EMS.ioeMinWise(polledTime,packSocst2) values(%s,%s)"
        try:
            awscur.execute(sql,val)
            awsdb.commit()
            print("String2 min wise inserted")
        except mysql.connector.IntegrityError:
            sql = "UPDATE EMS.ioeMinWise SET packSocst2 = %s WHERE polledTime = %s"
            val = (max(packMinst2[i]),i)
            awscur.execute(sql,val)
            awsdb.commit()
            print(val)
            print("String2 min wise updated")

    time.sleep(200)

