import mysql.connector
import time

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

while True:
    emscur = emsdb.cursor()

    awscur = awsdb.cursor()

    emscur.execute("""select upsbatterystatus,upschargingenergy,upsdischargingenergy,pack_usable_soc,received_time from EMS.EMSUPSBattery where date(received_time) = curdate()""")

    results = emscur.fetchall()
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
        if status == "CHG":
            if min not in chgmin.keys():
                chgmin[min] = [charging]
            else:
               chgmin[min].append(charging)
        elif status == "DCHG":
            if min not in dchgmin.keys():
                dchgmin[min] = [discharging]
            else:
                dchgmin[min].append(discharging)

    for i in range(0,len(results)):
        summarizeMin(results[i][0],results[i][1],results[i][2],results[i][4])
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
        if chglist[i][1]-chglist[i-1][1] > 0 and chglist[i][1]-chglist[i-1][1] <= 1:
            summarizeHourChg(chglist[i][0],chglist[i][1]-chglist[i-1][1])

    for i in range(1,len(dchglist)):
        if dchglist[i][1]-dchglist[i-1][1] > 0 and dchglist[i][1]-dchglist[i-1][1] <= 1:
            summarizeHourDchg(dchglist[i][0],dchglist[i][1]-dchglist[i-1][1])
       
    # for i in dchgmin.keys():
    #     summarizeHourDchg(i,max(dchgmin[i]))

    for i in chgdict.keys():
        print(i,abs(chgdict[i]))
        sql = "INSERT INTO UPSbatteryHourly(polledTime,chargingEnergy) VALUES(%s,%s)"
        val = (i,abs(chgdict[i]))
        try:
            awscur.execute(sql,val)
            awsdb.commit()
            print("Battery chg hourly")
        except mysql.connector.errors.IntegrityError:
            sql = "UPDATE UPSbatteryHourly SET chargingEnergy = %s WHERE polledTime = %s"
            val = (abs(chgdict[i]),i)
            awscur.execute(sql,val)
            awsdb.commit()
            print("Battery chg hourly")
           
    for i in packdict.keys():
        print(i,max(packdict[i]))
        sql = "INSERT INTO UPSbatteryHourly(polledTime,packsoc) VALUES(%s,%s)"
        val = (i,max(packdict[i]))
        try:
            awscur.execute(sql,val)
            awsdb.commit()
            print("Battery insert packsoc hourly")
        except mysql.connector.errors.IntegrityError:
            sql = "UPDATE UPSbatteryHourly SET packsoc = %s WHERE polledTime = %s"
            val = (max(packdict[i]),i)
            awscur.execute(sql,val)
            awsdb.commit()
            print("Battery update packsoc hourly")
   
    for i in packdict.keys():
        energyAvail = float(max(packdict[i]))*0.44
        print(i,energyAvail)
        sql = "INSERT INTO UPSbatteryHourly(polledTime,energyAvailable) VALUES(%s,%s)"
        val = (i,energyAvail)
        try:
            awscur.execute(sql,val)
            awsdb.commit()
            print("Battery insert energy available hourly")
        except mysql.connector.errors.IntegrityError:
            sql = "UPDATE UPSbatteryHourly SET energyAvailable = %s WHERE polledTime = %s"
            val = (energyAvail,i)
            awscur.execute(sql,val)
            awsdb.commit()
            print("Battery update energy available hourly")

    for i in dchgdict.keys():
        print(i,-abs(dchgdict[i]))
        sql = "INSERT INTO UPSbatteryHourly(polledTime,discharhingEnergy) VALUES(%s,%s)"
        val = (i,-abs(dchgdict[i]))
        try:
            awscur.execute(sql,val)
            awsdb.commit()
            print("Battery dchg hourly")
        except mysql.connector.errors.IntegrityError:
            sql = "UPDATE UPSbatteryHourly SET discharhingEnergy = %s WHERE polledTime = %s"
            val = (-abs(dchgdict[i]),i)
            awscur.execute(sql,val)
            awsdb.commit()
            print("Battery dchg hourly")
   
    time.sleep(900)
