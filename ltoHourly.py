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

    ltocur.execute("""select batteryStatus,chargingEnergy,dischargingEnergy,packUsableSOC,recordTimestamp from ltoBatteryData where date(recordTimestamp) = date(curdate()-1);""")

    results = ltocur.fetchall()

    chgdict = {}
    dchgdict ={}
    chgmin = {}
    dchgmin = {}
    packdict = {}
    chglist =[]
    dchglist = []

    def summarizePacksoc(polledTime,packSoc):
        hour = str(polledTime)[0:14]+"00:00"
        if hour not in packdict.keys():
            packdict[hour] = [packSoc]
        else:
            packdict[hour].append(packSoc)

    def summarizeHourChg(polledTime,charging):
        hour = str(polledTime)[0:14]+"00:00"
        
        if hour not in chgdict.keys():
            chgdict[hour] = charging
        else:
            chgdict[hour] += charging
           
    def summarizeHourDchg(polledTime,discharging):
        hour = str(polledTime)[0:14]+"00:00"
        if hour not in dchgdict.keys():
            dchgdict[hour] = discharging
        else:
            dchgdict[hour] += discharging


    def cummlativeChg(polledTime,charging):
        chglist.append([polledTime,charging])


    def cummlativeDchg(polledTime,discharging):
        dchglist.append([polledTime,discharging])


    def summarizeMin(status,charging,discharging,polledTime):
        min = str(polledTime)[0:17]+"00"
        if status == "CHG" and charging!=None:
            if min not in chgmin.keys():
                chgmin[min] = [charging]
            else:
               chgmin[min].append(charging)
        elif status == "DCHG" and discharging!= None:
            if min not in dchgmin.keys():
                # print(discharging)
                dchgmin[min] = [discharging]
            else:
                dchgmin[min].append(discharging)


    for i in range(0,len(results)):
        summarizeMin(results[i][0],results[i][1],results[i][2],results[i][4])
        if results[i][3] != None:
            if results[i][3]<=100:
            #print(results[i][4],results[i][3])
                summarizePacksoc(results[i][4],results[i][3])



    # for i in range(1,len(datalist)):
    #     summarizeHour(datalist[i][0],datalist[i][1]-datalist[i-1][1],datalist[i][2]-datalist[i-1][2],datalist[i][3],datalist[i][4])

    for i in chgmin.keys():
        cummlativeChg(i,max(chgmin[i]))

    for i in dchgmin.keys():
        cummlativeDchg(i,max(dchgmin[i]))
       
    for i in range(1,len(chglist)):
        if abs(chglist[i][1]-chglist[i-1][1]) > 0:
            summarizeHourChg(chglist[i][0],(chglist[i][1]-chglist[i-1][1])/1000)
            # print(chglist[i][0],(chglist[i][1]-chglist[i-1][1])/1000)

    for i in range(0,len(dchglist)):
        if dchglist[i][1]-dchglist[i-1][1] > 0:
            # print(dchglist[i][0],dchglist[i][1]-dchglist[i-1][1])
            summarizeHourDchg(dchglist[i][0],(dchglist[i][1]-dchglist[i-1][1])/1000)
       
    # for i in dchgmin.keys():
    #     summarizeHourDchg(i,max(dchgmin[i]))
            
    print() 

    for i in chgdict.keys():
        print(i,round(abs(chgdict[i]),2))
        sql = "INSERT INTO LTObatteryHourly(polledTime,chargingEnergy) VALUES(%s,%s)"
        val = (i,abs(chgdict[i]))
        try:
            awscur.execute(sql,val)
            awsdb.commit()
            print(val)
            print("Battery chg hourly")
        except mysql.connector.errors.IntegrityError:
            sql = "UPDATE LTObatteryHourly SET chargingEnergy = %s WHERE polledTime = %s"
            val = (abs(chgdict[i]),i)
            awscur.execute(sql,val)
            awsdb.commit()
            print(val)
            print("Battery chg hourly")
           
    for i in packdict.keys():
        print(i,max(packdict[i]))
        sql = "INSERT INTO LTObatteryHourly(polledTime,packsoc) VALUES(%s,%s)"
        val = (i,max(packdict[i]))
        try:
            awscur.execute(sql,val)
            awsdb.commit()
            print("Battery insert packsoc hourly")
        except mysql.connector.errors.IntegrityError:
            sql = "UPDATE LTObatteryHourly SET packsoc = %s WHERE polledTime = %s"
            val = (max(packdict[i]),i)
            awscur.execute(sql,val)
            awsdb.commit()
            print("Battery update packsoc hourly")
   
    for i in packdict.keys():
        energyAvail = float(max(packdict[i]))*0.15
        print(i,energyAvail)
        sql = "INSERT INTO LTObatteryHourly(polledTime,energyAvailable) VALUES(%s,%s)"
        val = (i,energyAvail)
        try:
            awscur.execute(sql,val)
            awsdb.commit()
            print("Battery insert energy available hourly")
        except mysql.connector.errors.IntegrityError:
            sql = "UPDATE LTObatteryHourly SET energyAvailable = %s WHERE polledTime = %s"
            val = (energyAvail,i)
            awscur.execute(sql,val)
            awsdb.commit()
            print("Battery update energy available hourly")
        
    print()

    for i in dchgdict.keys():
        print(i,round(-abs(dchgdict[i]),2))
        sql = "INSERT INTO LTObatteryHourly(polledTime,dischargingEnergy) VALUES(%s,%s)"
        val = (i,round(-abs(dchgdict[i]),2))
        try:
            awscur.execute(sql,val)
            awsdb.commit()
            print(val)
            print("Battery dchg hourly")
        except mysql.connector.errors.IntegrityError:
            sql = "UPDATE LTObatteryHourly SET dischargingEnergy = %s WHERE polledTime = %s"
            val = (round(-abs(dchgdict[i]),2),i)
            awscur.execute(sql,val)
            awsdb.commit()
            print(val)
            print("Battery dchg hourly")
   
    time.sleep(300)


# 88 18 03 FF 11 1A 0E 1F 0F 21 24 3C 39 88 18 13 FF 11 00 00 08 00 00 00 00 00
# {'batteryVolt': 361.0, 'batteryCurent': 38.71, 'mainConsSts': '2', 'preConSts': '1', 'batterySts': 'DCHG', 'packSoc': 60, 'usableSoc': 57, 'rectime': '2023-11-06 15:20:30', 'chargingEnergy': 0, 'dischargingEnergy': 8, 'availableEnergy': 0}

