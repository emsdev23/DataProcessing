from flask import Flask, jsonify, request
import mysql.connector
import time
import psycopg2
from datetime import datetime, timedelta,date
import json
from decimal import Decimal

app = Flask(__name__)

bmshost = '121.242.232.151'
awshost = '43.205.196.66'
emshost = '121.242.232.211'

def check_authentication(token):
    # Replace this with your own logic to validate the token
    valid_token = "VKOnNhH2SebMU6S"
    return token == valid_token



@app.route('/tsstoredwater', methods = ['GET'])
def tsStoredWater():
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        try:
            unprocesseddb = mysql.connector.connect(
                host=bmshost,
                user="emsrouser",
                password="emsrouser@151",
                database='bmsmgmtprodv13',
                port=3306
            )
            emsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                ) 
            source_cur = unprocesseddb.cursor()
            dest_cur = emsdb.cursor()
        except:
            return {"error":"mysql connection"}
        
        source_cur.execute("""SELECT tsStoredWaterTemperature, polledTime FROM thermalStorageMQTTReadings WHERE date(polledTime) = CURRENT_DATE();""")

        result = source_cur.fetchall()

        maxtemp = {}

        def Twentyminsummarize(temperature,polledTime):
            mint = int(str(polledTime)[14:16])
            if temperature != 0 and temperature != None:
                if mint >= 0 and mint < 20:
                    polledTime = str(polledTime)[0:14]+"00:00"
                        
                    if polledTime not in maxtemp.keys():
                        maxtemp[polledTime] = [temperature]
                    else:
                        maxtemp[polledTime].append(temperature)
                    
                if mint >= 20 and mint < 40:
                    polledTime = str(polledTime)[0:14]+"20:00"
                        
                    if polledTime not in maxtemp.keys():
                        maxtemp[polledTime] = [temperature]
                    else:
                        maxtemp[polledTime].append(temperature)
                    
                if mint >= 40 and mint < 60:
                    polledTime = str(polledTime)[0:14]+"40:00"
                        
                    if polledTime not in maxtemp.keys():
                        maxtemp[polledTime] = [temperature]
                    else:
                        maxtemp[polledTime].append(temperature)

        for row in result:
            tswatertemp = row[0]
            timestamp = row[1]

            Twentyminsummarize(tswatertemp, timestamp)
            
        for polledTime, temperatures in maxtemp.items():
            maximumtemp = max(temperatures) / 100  
            print(polledTime, maximumtemp)

            sql = """INSERT INTO  Tsstoredwatertemp(polledTime,Temperature) VALUES(%s,%s);"""
            val = (polledTime,maximumtemp)
            print("Ts temperature Inserted")
            try:
                dest_cur.execute(sql,val)
                emsdb.commit()
            except mysql.connector.IntegrityError:
                sql = """UPDATE Tsstoredwatertemp SET Temperature =  %s WHERE polledTime = %s;"""
                val = (maximumtemp,polledTime)
                dest_cur.execute(sql,val)
                emsdb.commit()
                print("Existing record updated")
        
        data = {"message":"TS stored Water temperature updated"}
        return jsonify(data), 200
    else:
        return jsonify({'error': 'Unauthorized'}), 401 



@app.route('/inverterhour', methods = ['GET'])
def inverterHour():
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        try:
            awsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                )

            awscur = awsdb.cursor()
        except:
            return {"error":"mysql connection"}

        inverthr1 = {}
        inverthr2 = {}
        inverthr3 = {}
        inverthr4 = {}
        inverthr5 = {}
        inverthr6 = {}
        inverthr7 = {}
        inverthr8 = {}


        def HourSummmarize(inverthr, inverter_id, active_power, timestamp):
            hour = str(timestamp)[0:13] + ":00:00"

            if hour in inverthr.keys():
                inverthr[hour].append(active_power)
            else:
                inverthr[hour] = [active_power]

        awscur.execute("""SELECT inverterdeviceid, invertertimestamp, inverteractivepower FROM EMS.EMSInverterData WHERE inverterdeviceid IN ('1', '2', '3', '4', '5', '6', '7', '8') AND DATE(invertertimestamp) = CURDATE();""")

        result = awscur.fetchall()


        for row in result:
            inverter_id = row[0]
            timestamp = row[1]
            active_pow = row[2]

            if inverter_id == 1:
                HourSummmarize(inverthr1, inverter_id, active_pow, timestamp)
            elif inverter_id == 2:
                HourSummmarize(inverthr2, inverter_id, active_pow, timestamp)
            elif inverter_id == 3:
                HourSummmarize(inverthr3, inverter_id, active_pow, timestamp)
            elif inverter_id == 4:
                HourSummmarize(inverthr4, inverter_id, active_pow, timestamp)
            elif inverter_id == 5:
                HourSummmarize(inverthr5, inverter_id, active_pow, timestamp)
            elif inverter_id == 6:
                HourSummmarize(inverthr6, inverter_id, active_pow, timestamp)
            elif inverter_id == 7:
                HourSummmarize(inverthr7, inverter_id, active_pow, timestamp)
            elif inverter_id == 8:
                HourSummmarize(inverthr8, inverter_id, active_pow, timestamp)


        for hour, values in inverthr1.items():
            total_power = round(sum(values), 2)

            sql = "INSERT INTO EMS.inverterPowerHourly(inverter1,polledTime) values(%s,%s)"
            val = (total_power,hour)
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter1 data inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE EMS.inverterPowerHourly SET inverter1 = %s WHERE polledTime = %s"
                val = (total_power,hour)
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter1 data updated") 


        for hour, values in inverthr2.items():
            total_power = round(sum(values), 2)
            
            sql = "INSERT INTO EMS.inverterPowerHourly(inverter2,polledTime) values(%s,%s)"
            val = (total_power,hour)
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter2 data inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE EMS.inverterPowerHourly SET inverter2 = %s WHERE polledTime = %s"
                val = (total_power,hour)
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter2 data updated")


        for hour, values in inverthr3.items():
            total_power = round(sum(values), 2)
            
            sql = "INSERT INTO EMS.inverterPowerHourly(inverter3,polledTime) values(%s,%s)"
            val = (total_power,hour)
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter3 data inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE EMS.inverterPowerHourly SET inverter3 = %s WHERE polledTime = %s"
                val = (total_power,hour)
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter3 data updated") 


        for hour, values in inverthr4.items():
            total_power = round(sum(values), 2)
            
            sql = "INSERT INTO EMS.inverterPowerHourly(inverter4,polledTime) values(%s,%s)"
            val = (total_power,hour)
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter4 data inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE EMS.inverterPowerHourly SET inverter4 = %s WHERE polledTime = %s"
                val = (total_power,hour)
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter4 data updated")


        for hour, values in inverthr5.items():
            total_power = round(sum(values), 2)
            
            sql = "INSERT INTO EMS.inverterPowerHourly(inverter5,polledTime) values(%s,%s)"
            val = (total_power,hour)
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter5 data inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE EMS.inverterPowerHourly SET inverter5 = %s WHERE polledTime = %s"
                val = (total_power,hour)
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter5 data updated")


        for hour, values in inverthr6.items():
            total_power = round(sum(values), 2)
            
            sql = "INSERT INTO EMS.inverterPowerHourly(inverter6,polledTime) values(%s,%s)"
            val = (total_power,hour)
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter6 data inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE EMS.inverterPowerHourly SET inverter6 = %s WHERE polledTime = %s"
                val = (total_power,hour)
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter6 data updated")


        for hour, values in inverthr7.items():
            total_power = round(sum(values), 2)
            
            sql = "INSERT INTO EMS.inverterPowerHourly(inverter7,polledTime) values(%s,%s)"
            val = (total_power,hour)
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter7 data inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE EMS.inverterPowerHourly SET inverter7 = %s WHERE polledTime = %s"
                val = (total_power,hour)
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter7 data updated")


        for hour, values in inverthr8.items():
            total_power = round(sum(values), 2)
            
            sql = "INSERT INTO EMS.inverterPowerHourly(inverter8,polledTime) values(%s,%s)"
            val = (total_power,hour)
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter8 data inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE EMS.inverterPowerHourly SET inverter8 = %s WHERE polledTime = %s"
                val = (total_power,hour)
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter8 data updated")
        
        awscur.close()
        awsdb.close()

        data = {"message":"Inverter hour updated"}
        return jsonify(data), 200
    else:
        return jsonify({'error': 'Unauthorized'}), 401 
    
    

@app.route('/electricDayWise', methods = ['GET'])
def electricDay():
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        try:
            db = mysql.connector.connect(
                host=bmshost,
                user="emsrouser",
                password="emsrouser@151",
                port=3306
            )

            bms_cur = db.cursor()

            awsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                )

            awscur = awsdb.cursor()
        except:
            return {"error":"mysql connection"}
        
        bms_cur.execute("""SELECT tenantClass,energy,date from bmsmgmt_olap_prod_v13.electricDaywiseEnergyUsage where date = date_sub(curdate(), interval 1 day)""")

        result = bms_cur.fetchall()
        # print(result)

        Chiller_dict = {}
        Client  = {}                 
        Utility = {}
        others   = {}

        classes = ["ComArea-RP", "Lifts-RP", "MLCP-RP", "Exhaust-RP", "APFC-Panel-RP", "NewAuditorium-RP", "Food_Court-RP", "FirePumps-RP"]

        def DateSummmarize(Class,Energy,Time):
            Time = str(Time)
            if Energy != None:
                if Class == "ChillerIncomer-RP":
                    if Time in Chiller_dict.keys():
                        Chiller_dict[Time] += Energy
                    else:
                        Chiller_dict[Time] = Energy

                elif Class =="Client": 
                    # print(Class,Energy,Time)
                    if Time in Client.keys():
                        Client[Time] += Energy
                    else:
                        Client[Time] = Energy


                elif Class == "UtilityPanel-RP":  
                        
                        # print(Time)
                    if Time in Utility.keys():
                        Utility[Time] += Energy
                    else:
                        Utility[Time] = Energy

                elif Class in classes:
                        
                        # print(Time)
                    if Time in others.keys():
                        others[Time] += Energy
                    else:
                        others[Time] = Energy


        for row in result:
            DateSummmarize(row[0], row[1], row[2])
        
        for i in Client.keys():
            total_client_energy = Client[i]
            polledDate = i
        
        for i in Chiller_dict.keys():
            polledDate = i
            total_Incomer_energy = Chiller_dict[i]
        
        for i in Utility.keys():
            polledDate = i
            total_utility_energy = Utility[i]
        
        for i in others.keys():
            polledDate = i
            total_energy = others[i]
        

        sql = """INSERT INTO EMS.electricDaywiseUsage(polledDate, Chillers, Client, Utilities, Others) VALUES(%s, %s, %s, %s, %s)"""
        val = (polledDate, total_Incomer_energy, total_client_energy, total_utility_energy, total_energy)
        try:
            awscur.execute(sql, val)
            awsdb.commit()
            print(val)
            print("Electric Day wise inserted")
        except mysql.connector.errors.IntegrityError:
            sql = "UPDATE EMS.electricDaywiseUsage SET Chillers = %s, Client = %s, Utilities = %s, Others = %s WHERE polledDate = %s"
            val = (total_Incomer_energy, total_client_energy, total_utility_energy, total_energy, polledDate)
            awscur.execute(sql,val)
            awsdb.commit()
            print(val)
            print("Electric Day wise updated")
        
        bms_cur.close()
        db.close()
        awscur.close()
        awsdb.close()

        data = {"message":"Electric Day Wise"}
        return jsonify(data), 200
    else:
        return jsonify({'error': 'Unauthorized'}), 401 


@app.route('/upsHourly', methods = ['GET'])
def UPSHourly():
    token = request.headers.get('Authorization')
    print(token)
    
    if token and check_authentication(token):
        try:
            emsdb = mysql.connector.connect(
                host=emshost,
                user="emsroot",
                password="22@teneT",
                database='EMS',
                port=3306
            )

            awsdb = mysql.connector.connect(
                host=awshost,
                user="emsroot",
                password="22@teneT",
                database='EMS',
                port=3307
            )
        except:
            return {"error":"mysql connection"}
        
        emscur = emsdb.cursor()
        awscur = awsdb.cursor()

        emscur.execute("""select upsbatterystatus,upschargingenergy,upsdischargingenergy,pack_usable_soc,received_time from EMS.EMSUPSBattery where date(received_time) = curdate() and date()""")

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
        
        data = {"message":"UPS HOURLY"}
        return jsonify(data), 200
    else:
        return jsonify({'error': 'Unauthorized'}), 401


@app.route('/dgquarterly', methods = ['GET'])
def dgQuarterly():
    token = request.headers.get('Authorization')
    print(token)
    
    if token and check_authentication(token):
        awsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                )

        awscur = awsdb.cursor()

        awscur.execute("select polledTime,DG1,DG2,DG3,DG4,DG5 from EMS.DieselMinWise where date(polledTime) = curdate();")

        dgres = awscur.fetchall()

        dg1 = {}
        dg2 = {}
        dg3 = {}
        dg4 = {}
        dg5 = {}

        def DgSeg(polledTime,DG1Energy,DG2Energy,DG3Energy,DG4Energy,DG5Energy):
            mint = int(str(polledTime)[14:16])

            if mint>= 0 and mint < 15:
                polledTime = str(polledTime)[0:14]+"00:00"
                if polledTime in dg1.keys():
                    dg1[polledTime].append(DG1Energy)
                else:
                    dg1[polledTime] = [DG1Energy]
                
                if polledTime in dg2.keys():
                    dg2[polledTime].append(DG2Energy)
                else:
                    dg2[polledTime] = [DG2Energy]
                
                if polledTime in dg3.keys():
                    dg3[polledTime].append(DG3Energy)
                else:
                    dg3[polledTime] = [DG3Energy]
                
                if polledTime in dg4.keys():
                    dg4[polledTime].append(DG4Energy)
                else:
                    dg4[polledTime] = [DG4Energy]
                
                if polledTime in dg5.keys():
                    dg5[polledTime].append(DG5Energy)
                else:
                    dg5[polledTime] = [DG5Energy]
            
            elif mint>= 15 and mint < 30:
                polledTime = str(polledTime)[0:14]+"15:00"
                if polledTime in dg1.keys():
                    dg1[polledTime].append(DG1Energy)
                else:
                    dg1[polledTime] = [DG1Energy]
                
                if polledTime in dg2.keys():
                    dg2[polledTime].append(DG2Energy)
                else:
                    dg2[polledTime] = [DG2Energy]
                
                if polledTime in dg3.keys():
                    dg3[polledTime].append(DG3Energy)
                else:
                    dg3[polledTime] = [DG3Energy]
                
                if polledTime in dg4.keys():
                    dg4[polledTime].append(DG4Energy)
                else:
                    dg4[polledTime] = [DG4Energy]
                
                if polledTime in dg5.keys():
                    dg5[polledTime].append(DG5Energy)
                else:
                    dg5[polledTime] = [DG5Energy]

            elif mint>= 30 and mint < 45:
                polledTime = str(polledTime)[0:14]+"30:00"
                if polledTime in dg1.keys():
                    dg1[polledTime].append(DG1Energy)
                else:
                    dg1[polledTime] = [DG1Energy]
                
                if polledTime in dg2.keys():
                    dg2[polledTime].append(DG2Energy)
                else:
                    dg2[polledTime] = [DG2Energy]
                
                if polledTime in dg3.keys():
                    dg3[polledTime].append(DG3Energy)
                else:
                    dg3[polledTime] = [DG3Energy]
                
                if polledTime in dg4.keys():
                    dg4[polledTime].append(DG4Energy)
                else:
                    dg4[polledTime] = [DG4Energy]
                
                if polledTime in dg5.keys():
                    dg5[polledTime].append(DG5Energy)
                else:
                    dg5[polledTime] = [DG5Energy]
            
            elif mint>= 45 and mint < 60:
                polledTime = str(polledTime)[0:14]+"45:00"
                if polledTime in dg1.keys():
                    dg1[polledTime].append(DG1Energy)
                else:
                    dg1[polledTime] = [DG1Energy]
                
                if polledTime in dg2.keys():
                    dg2[polledTime].append(DG2Energy)
                else:
                    dg2[polledTime] = [DG2Energy]
                
                if polledTime in dg3.keys():
                    dg3[polledTime].append(DG3Energy)
                else:
                    dg3[polledTime] = [DG3Energy]
                
                if polledTime in dg4.keys():
                    dg4[polledTime].append(DG4Energy)
                else:
                    dg4[polledTime] = [DG4Energy]
                
                if polledTime in dg5.keys():
                    dg5[polledTime].append(DG5Energy)
                else:
                    dg5[polledTime] = [DG5Energy]
        

        for i in dgres:
            DgSeg(i[0],i[1],i[2],i[3],i[4],i[5])

        
        for i in dg1.keys():
            val = (i,round(sum(dg1[i]),2))
            print(val)
            sql = "INSERT INTO EMS.dieselQuarterly(polledTime,dg1Energy) values(%s,%s)"
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print("DG1 Quarterly Inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE EMS.dieselQuarterly SET dg1Energy = %s where polledTime = %s"
                val = (round(sum(dg1[i]),2),i)
                awscur.execute(sql,val)
                awsdb.commit()
                print("DG1 Quarterly Updated")
            
        for i in dg2.keys():
            val = (i,round(sum(dg2[i]),2))
            print(val)
            sql = "INSERT INTO EMS.dieselQuarterly(polledTime,dg2Energy) values(%s,%s)"
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print("DG2 Quarterly Inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE EMS.dieselQuarterly SET dg2Energy = %s where polledTime = %s"
                val = (round(sum(dg2[i]),2),i)
                awscur.execute(sql,val)
                awsdb.commit()
                print("DG2 Quarterly Updated")
        
        for i in dg3.keys():
            val = (i,round(sum(dg3[i]),2))
            print(val)
            sql = "INSERT INTO EMS.dieselQuarterly(polledTime,dg3Energy) values(%s,%s)"
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print("DG3 Quarterly Inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE EMS.dieselQuarterly SET dg3Energy = %s where polledTime = %s"
                val = (round(sum(dg3[i]),2),i)
                awscur.execute(sql,val)
                awsdb.commit()
                print("DG3 Quarterly Updated")
        
        for i in dg4.keys():
            val = (i,round(sum(dg4[i]),2))
            print(val)
            sql = "INSERT INTO EMS.dieselQuarterly(polledTime,dg4Energy) values(%s,%s)"
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print("DG4 Quarterly Inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE EMS.dieselQuarterly SET dg4Energy = %s where polledTime = %s"
                val = (round(sum(dg4[i]),2),i)
                awscur.execute(sql,val)
                awsdb.commit()
                print("DG4 Quarterly Updated")
        
        for i in dg5.keys():
            val = (i,round(sum(dg5[i]),2))
            sql = "INSERT INTO EMS.dieselQuarterly(polledTime,dg5Energy) values(%s,%s)"
            print(val)
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print("DG5 Quarterly Inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE EMS.dieselQuarterly SET dg5Energy = %s where polledTime = %s"
                val = (round(sum(dg5[i]),2),i)
                awscur.execute(sql,val)
                awsdb.commit()
                print("DG5 Quarterly Updated")

        data = {"message":"DG Quarterly"}
        return jsonify(data), 200
    else:
        return jsonify({'error': 'Unauthorized'}), 401


@app.route('/dghourly', methods = ['GET'])
def dgHourly():
    token = request.headers.get('Authorization')
    print(token)
    
    if token and check_authentication(token):
        emsdb = mysql.connector.connect(
                        host=awshost,
                        user="emsroot",
                        password="22@teneT",
                        database='EMS',
                        port=3307
                    )
        
        emscur = emsdb.cursor()

        dg_hr = {}

        emscur.execute("SELECT polledTime,dg1Energy+dg2Energy+dg3Energy+dg4Energy+dg5Energy as dgEnergy FROM EMS.dieselQuarterly where date(polledTime) = curdate();")

        res = emscur.fetchall()

        def HourSeg(polledTime,Energy):
            polledTime = str(polledTime)[0:14]+"00:00"
            if polledTime in dg_hr.keys():
                dg_hr[polledTime].append(Energy)
            else:
                dg_hr[polledTime] = [Energy]

        for i in res:
            HourSeg(i[0],i[1])

        for i in dg_hr.keys():
            val = (i,sum(dg_hr[i]))
            print(val)
            sql = "INSERT INTO EMS.DGHourly(polledTime,Energy) values(%s,%s)"
            try:
                emscur.execute(sql,val)
                emsdb.commit()
                print("DG Hourly inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE EMS.DGHourly SET Energy = %s where polledTime = %s"
                val = (sum(dg_hr[i]),i)
                emscur.execute(sql,val)
                emsdb.commit()
                print("DG Hourly updated")
        
        emscur.close()
        data = {"message":"DG Hourly"}
        return jsonify(data), 200
    else:
        return jsonify({'error': 'Unauthorized'}), 401


@app.route('/wheeledhourly', methods = ['GET'])
def wheeledHourly():
    token = request.headers.get('Authorization')
    print(token)
    
    if token and check_authentication(token):
        rad = {}
        radhr = {}
        hourly_wheeled= {}

        emsdb = mysql.connector.connect(
                            host=awshost,
                            user="emsroot",
                            password="22@teneT",
                            database='EMS',
                            port=3307
                        )
        
        processeddb = mysql.connector.connect(
                    host=bmshost,
                    user="emsrouser",
                    password="emsrouser@151",
                    database='bmsmgmtprodv13',
                    port=3306
                    )

        emscur = emsdb.cursor()
        proscur = processeddb.cursor()

        def Hourlywheeled(polledTime,Energy):
            hour = str(polledTime)[0:13] + ":00:00"
            if hour in hourly_wheeled.keys():
                if Energy >=0 and Energy <=100:
                    hourly_wheeled[hour] += Energy
            else:
                if Energy>=0 and Energy <=100:
                    hourly_wheeled[hour] = Energy

        proscur.execute("select createdTime,ctsedogenergy from bmsmgmtprodv13.ctsedogreadings where date(createdTime) = curdate();")

        wheeled_res = proscur.fetchall()

        for i in range( 1,len(wheeled_res)):
            Hourlywheeled(wheeled_res[i][0],(wheeled_res[i][1]-wheeled_res[i-1][1])*1000)
            
        for i in hourly_wheeled.keys():
            sql = "INSERT INTO WheeledHourly(polledTime,Energy) VALUES(%s,%s)"
            val = (i,hourly_wheeled[i])
            print(val)
            try:
                emscur.execute(sql,val)
                emsdb.commit()
                print("Wheeled hourly inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE WheeledHourly SET Energy = %s WHERE polledTime = %s"
                val = (hourly_wheeled[i],i)
                emscur.execute(sql,val)
                emsdb.commit()
                print("Wheeled hourly update")

        def HourSummmarize(irrad,Time):
            hour=str(Time)[0:13]+":00:00"
            if hour in radhr.keys():
                radhr[hour].append(irrad)
            else:
                radhr[hour] = [irrad]

        emscur.execute("""select wmstimestamp,CAST(wmsirradiation AS DECIMAL(18, 2)) from EMS.EMSWMSData WHERE DATE(wmstimestamp) = curdate();""")
        result = emscur.fetchall()          

        for row in result:
            # print(row[0],row[1])
            HourSummmarize(row[1],row[0])

        expectedwmms = 0

        for i in radhr.keys():
            wmsirradiation_sum = round(sum(radhr[i])/60000,2)    
            print(i,wmsirradiation_sum)
            if wmsirradiation_sum is not None:
                wmsirradiationkwh = Decimal(str(wmsirradiation_sum))
                # print('irr',wmsirradiationkwh)

                constant1 = Decimal('0.755')
                constant2 = Decimal('9702')
                constant3 = Decimal('0.207')

                def calculate_x():
                    month_multiplier = {
                                        'january': 0.1,
                                        'february': 0.2,
                                        'march': 0.3,
                                        'april': 0.4,
                                        'may': 0.5,
                                        'june': 0.6,
                                        'july': 0.7,
                                        'august': 0.8,
                                        'september': 0.9,
                                        'october': 1.0,
                                        'november': 0.005,
                                        'december': 0.005,
                                        }

                                                            
                    current_month = datetime.now().strftime('%B').lower()

                                                                        
                    if current_month in month_multiplier:
                
                        return Decimal(str(month_multiplier[current_month]))
                    else:
                        return Decimal('0')  

                x_multiplier = calculate_x()
                # print('multi',x_multiplier)

                expectedwmms = (wmsirradiationkwh * constant1 * constant2 * constant3) - ((wmsirradiationkwh * constant1 * constant2 * constant3) * Decimal(x_multiplier))
                expectedwmms = round(expectedwmms, 2)
                # print('expwmms',expectedwmms)

                val = (wmsirradiationkwh,expectedwmms,i)
                sql = "INSERT INTO EMS.WheeledHourly(irradiation,expectedEnergy,polledTime) values(%s,%s,%s)"
                try:
                    emscur.execute(sql,val)
                    emsdb.commit()
                    print(val)
                    print("Wheeled in irradiation inserted")
                except mysql.connector.errors.IntegrityError:
                    sql = "update EMS.WheeledHourly set irradiation = %s, expectedEnergy = %s where polledTime = %s"
                    val = (wmsirradiationkwh,expectedwmms,i)
                    print(val)
                    emscur.execute(sql,val)
                    emsdb.commit()
                    print("Wheeled in irradiation updated")

        emscur.close()
        proscur.close()
        
        data = {"message":"WHEELED HOURLY"}
        return jsonify(data), 200
    else:
        return jsonify({'error': 'Unauthorized'}), 401


    
@app.route('/bmsgridhourly', methods = ['GET'])
def bmsGridHourly():
    token = request.headers.get('Authorization')
    print(token)
    
    try:
        processeddb = mysql.connector.connect(
            host=bmshost,
            user="emsrouser",
            password="emsrouser@151",
            database='bmsmgmt_olap_prod_v13',
            port=3306
            )
    except Exception as ex:
        print(ex)

    emsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                )


    if token and check_authentication(token):
        proscur = processeddb.cursor()
        emscur = emsdb.cursor()
        
        proscur.execute("""SELECT date,hour,energy FROM bmsmgmt_olap_prod_v13.IITMRPHourwiseEnergyUsage where date(createdTime) = curdate() and name = 'IITMRP' and energyType='electric';""")

        result = proscur.fetchall()

        hour_dict ={}
        def HourlySummarize(polledDate,hour,Energy):
            if len(hour) == 1:
                polledTime = polledDate+" "+"0"+hour+":00:00"
            elif len(hour) == 2:
                polledTime = polledDate+" "+hour+":00:00"
           
            if polledTime not in hour_dict:
                hour_dict[polledTime] = Energy

        for i in result:
            HourlySummarize(str(i[0]),str(i[1]),i[2])
           
        for i in hour_dict.keys():
            sql = "INSERT INTO BMSgridhourly(polledTime,Energy) values(%s,%s)"
            val = (i,hour_dict[i])
            try:
                emscur.execute(sql,val)
                emsdb.commit()
                print("BMS Grid hourly")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE BMSgridhourly SET Energy = %s WHERE polledTime = %s"
                val = (hour_dict[i],i)
                print(val)
                emscur.execute(sql,val)
                emsdb.commit()
                print("BMS Grid hourly")
        proscur.close()
        emscur.close()
        
        data = {"message":"BMS GRID HOURLY"}
        return jsonify(data), 200
    else:
        return jsonify({'error': 'Unauthorized'}), 401


@app.route('/ltohourly', methods = ['GET'])
def ltoHourly():
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        
        try:
            awsdb = mysql.connector.connect(
                host=awshost,
                user="emsroot",
                password="22@teneT",
                database='EMS',
                port=3307
            )

            unprocesseddb = mysql.connector.connect(
                        host=bmshost,
                        user="ltouser",
                        password="ltouser@151",
                        database='bmsmgmtprodv13',
                        port=3306
                    )

            awscur = awsdb.cursor()

            ltocur = unprocesseddb.cursor()
        except Exception as ex:
            print(ex)
            jsonify({'error': 'Mysql connection errro'}), 401

        ltocur.execute("""select batteryStatus,chargingEnergy,dischargingEnergy,packUsableSOC,recordTimestamp from ltoBatteryData where date(recordTimestamp) = curdate();""")

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
        ltocur.close()
        awscur.close()
        
        data = {"message":"LTO HOURLY"}
        return jsonify(data), 200
    else:
        return jsonify({'error': 'Unauthorized'}), 401
    


@app.route('/gridhourly', methods = ['GET'])
def gridHourlyl():
    token = request.headers.get('Authorization')
    print(token)

    emsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                )
    try:
        processeddb = mysql.connector.connect(
            host=bmshost,
            user="bmsrouser6",
            password="bmsrouser6@151U",
            database='bmsmgmt_olap_prod_v13',
           port=3306
            )
    except:
         return {"error":"mysql connection"}

    if token and check_authentication(token):
        emscur = emsdb.cursor()
    
        processcur = processeddb.cursor()

        processcur.execute("""select FLOOR(acmeterenergy),polledTime from bmsmgmt_olap_prod_v13.MVPPolling where mvpnum in ("MVP1","MVP2","MVP3","MVP4") and Date(polledTime) = date(curdate()-5);""")

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

        
        processcur.close()
        emscur.close()          
        data = {'message': 'GRID updated'}
        return jsonify(data), 200
    else:
        # Return an error response for unauthorized access
        return jsonify({'error': 'Unauthorized'}), 401

@app.route('/griddaily', methods = ['GET'])
def gridDaily():
    token = request.headers.get('Authorization')
    print(token)

    emsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                )
    
    
    if token and check_authentication(token):
        emscur = emsdb.cursor()
        emscur.execute("SELECT Energy,polledTime from EMS.Gridhourly where date(polledTime) = curdate()")
        data = emscur.fetchall()
    
        grid_dict = {}
        def SummarizeDay(Energy,polledTime):
            day = str(polledTime)[0:10]
            if day in grid_dict.keys():
                grid_dict[day] += Energy
            else:
                grid_dict[day] = Energy
        
        for i in data:
            SummarizeDay(i[0],i[1])
        
        for i in grid_dict.keys():
            sql = "INSERT INTO GridProcessed(polledDate,Energy) VALUES(%s,%s)"
            val = (i,grid_dict[i])
            print(val)
            try:
                emscur.execute(sql,val)
                print("Grid data added")
                emsdb.commit()
            except mysql.connector.IntegrityError:
                sql = """UPDATE GridProcessed SET Energy = %s WHERE polledDate= %s;"""
                values = (grid_dict[i],i)
                emscur.execute(sql,values)
                print("Grid data updated")
                emsdb.commit()
        emscur.close()          
        data = {'message': 'GRID Daily updated'}
        return jsonify(data), 200 
    else:
        return jsonify({'error': 'Unauthorized'}), 401
    
@app.route('/peakdemandhourly', methods = ['GET'])
def peakHourly():
    token = request.headers.get('Authorization')
    print(token)
    #processeddb = mysql.connector.connect(
    #        host="121.242.232.151",
    #        user="emsrouser",
    #        password="emsrouser@151",
    #        database='bmsmgmt_olap_prod_v13',
    #        port=3306
    #        )

    emsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                )

    if token and check_authentication(token):
        #proscur = processeddb.cursor()
        emscur = emsdb.cursor()
        
        emscur.execute("select totalApparentPower2,polledTime from bmsunprocessed_prodv13.hvacSchneider7230Polling WHERE date(polledTime) = curdate();")

        peakres = emscur.fetchall()

        peak_dict = {}
        def peakHourly(peak,polledTime):
            hour = str(polledTime)[0:13]+":00:00"
            if hour in peak_dict.keys():
                peak_dict[hour].append(peak)
            else:
                peak_dict[hour] = [peak]

        for i in peakres:
                peakHourly(i[0],i[1])

        for i in peak_dict.keys():
            res = [i for i in peak_dict[i] if i is not None]
            sql = "INSERT INTO peakdemandHourly(polledTime,peakdemand) VALUES(%s,%s)"
            try:
                val = (i,max(res))
            except ValueError:
                continue
            try:
                emscur.execute(sql,val)
                print("Peak demand hourly updated",max(res),i)
                emsdb.commit()
            except mysql.connector.IntegrityError:
                sql="UPDATE peakdemandHourly SET peakdemand = %s where polledTime = %s"
                val = (max(res),i)
                emscur.execute(sql,val)
                emsdb.commit()
                print("Peak demand hourly updated",max(res),i)
        
        #proscur.close()
        emscur.close()
        data = {'message': 'PEAK DEMAND HOURLY updated'}
        return jsonify(data), 200
    else:
        # Return an error response for unauthorized access
        return jsonify({'error': 'Unauthorized'}), 401


@app.route('/rooftopHourly', methods = ['GET'])
def rooftopHourly():
    token = request.headers.get('Authorization')
    print(token)
    

    if token and check_authentication(token):
        unprocesseddb = mysql.connector.connect(
                host=bmshost,
                user="emsrouser",
                password="emsrouser@151",
                database='bmsmgmtprodv13',
                port=3306
            )

        emsdb = mysql.connector.connect(
            host=awshost,
            user="emsroot",
            password="22@teneT",
            database='EMS',
            port=3307
        )
        
        unpdbcur = unprocesseddb.cursor()
        emscur = emsdb.cursor()

        unpdbcur.execute("""select FLOOR(acmeterenergy),acmeterpolledtimestamp from bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1035 and Date(acmeterpolledtimestamp) = curdate(); """)

        result = unpdbcur.fetchall()

        roofhr = {}
        ph1 = {}
        ph2 = {}

        def ph1Hour(Energy,Time):
            hour=str(Time)[0:13]+":00:00"
            if hour in ph1.keys():
                ph1[hour] += Energy  
            else:
                ph1[hour] = Energy
            
        def ph2Hour(Energy,Time):
            hour=str(Time)[0:13]+":00:00"
            # print(Time)
            if hour in ph2.keys():
              ph2[hour] += Energy  
            else:
              ph2[hour] = Energy

        def HourSummmarize(Energy,Time):
            hour=str(Time)[0:13]+":00:00"
            # print(Time)
            if hour in roofhr.keys():
                roofhr[hour] += Energy  
            else:
                roofhr[hour] = Energy

        for i in range(1,len(result)):
            ph2Hour(round(((result[i][0]-result[i-1][0])/1000),2),result[i][1])
            HourSummmarize(round(((result[i][0]-result[i-1][0])/1000),2),result[i][1])
        # print(roofhr)   
        unpdbcur.execute("""select FLOOR(acmeterenergy),acmeterpolledtimestamp from bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1147 and Date(acmeterpolledtimestamp) = curdate(); """)

        result2 = unpdbcur.fetchall()

        for i in range(1,len(result2)):
            ph1Hour(round(((result2[i][0]-result2[i-1][0])/1000),2),result2[i][1])
            HourSummmarize(round(((result2[i][0]-result2[i-1][0])/1000),2),result2[i][1])

        for i in roofhr.keys():
                # print(i,roofhr[i])
            sql = """INSERT INTO roofTopHour(polledTime,energy) VALUES(%s,%s)"""
            val = (i,roofhr[i])
            print(val)
            try:
                emscur.execute(sql,val)
                print("Rooftop hourly Processed data written")
                emsdb.commit()
            except mysql.connector.IntegrityError:
                sql = """UPDATE roofTopHour SET energy = %s WHERE polledTime = %s"""
                val = (roofhr[i],i)
                emscur.execute(sql,val)
                print("Rooftop hourly Processed data written")
                emsdb.commit()
        
        for i in ph1.keys():
                # print(i,roofhr[i])
            sql = """INSERT INTO roofTopHour(polledTime,ph1Actual) VALUES(%s,%s)"""
            val = (i,ph1[i])
            print(val)
            try:
                emscur.execute(sql,val)
                print("Rooftop hourly ph1 Processed data written")
                emsdb.commit()
            except mysql.connector.IntegrityError:
                sql = """UPDATE roofTopHour SET ph1Actual = %s WHERE polledTime = %s"""
                val = (ph1[i],i)
                emscur.execute(sql,val)
                print("Rooftop hourly ph1 Processed data written")
                emsdb.commit()
        
        for i in ph2.keys():
                # print(i,roofhr[i])
            sql = """INSERT INTO roofTopHour(polledTime,ph2Actual) VALUES(%s,%s)"""
            val = (i,ph2[i])
            print(val)
            try:
                emscur.execute(sql,val)
                print("Rooftop hourly ph2 Processed data written")
                emsdb.commit()
            except mysql.connector.IntegrityError:
                sql = """UPDATE roofTopHour SET ph2Actual = %s WHERE polledTime = %s"""
                val = (ph2[i],i)
                emscur.execute(sql,val)
                print("Rooftop hourly ph2 Processed data written")
                emsdb.commit()
        
        rad = {}
        radhr = {}

        def HourSummmarize(irrad,Time):
            hour=str(Time)[0:13]+":00:00"
            if hour in radhr.keys():
                radhr[hour].append(irrad)
            else:
                radhr[hour] = [irrad]


        unpdbcur.execute("""select sensorpolledtimestamp,sensorsolarradiation from sensorreadings WHERE DATE(sensorpolledtimestamp) = CURDATE();""")    

        result = unpdbcur.fetchall()

        total_radiation = 0
        irradiation = 0
        expected1147 = 0
        expected1035 = 0


        for row in result:
            HourSummmarize(row[1],row[0])
        

        for i in radhr.keys():
            normalized_solar_radiation = round(sum(radhr[i])/4000,2)
            expected1147 = (normalized_solar_radiation * 0.75 * 4877 * 0.156) - ((normalized_solar_radiation * 0.75 * 4877 * 0.156) * 0.0719)
            expected1035 = (normalized_solar_radiation * 0.75 * 1777 * 0.156) - ((normalized_solar_radiation * 0.75 * 1777 * 0.156) * 0.0871)
            print(i,normalized_solar_radiation,expected1147,expected1035)

            sql = """INSERT INTO roofTopHour(irradiation,expph1Energy,expph2Energy,polledTime) VALUES(%s,%s,%s,%s);"""
            val = (normalized_solar_radiation,expected1147,expected1035,i)
            try:
                emscur.execute(sql,val)
                emsdb.commit()
                print("Rooftop Irradiation and expected inserted") 

            except mysql.connector.IntegrityError:
                sql = """UPDATE roofTopHour SET  irradiation = %s, expph1Energy = %s, expph2Energy = %s WHERE polledTime = %s;"""
                val = (normalized_solar_radiation,expected1147,expected1035,i)
                emscur.execute(sql,val)
                emsdb.commit()
                print("Rooftop Irradiation and expected updated")  
        
        emscur.close()
        unpdbcur.close()
        data = {'message': 'Roof top HOURLY updated'}
        return jsonify(data), 200
    else:
        # Return an error response for unauthorized access
        return jsonify({'error': 'Unauthorized'}), 401


#Rooftop daily api
@app.route('/rooftopdaily', methods = ['GET'])
def rooftopDaily():
    token = request.headers.get('Authorization')
    print(token)

    emsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                )
    try:
        processeddb = mysql.connector.connect(
            host=bmshost,
            user="emsrouser",
            password="emsrouser@151",
            database='bmsmgmt_olap_prod_v13',
           port=3306
            )
    except:
         return {"error":"mysql connection"}
            
    if token and check_authentication(token):
        energy1 = 0
        energy2 = 0    
        polleddate = ""
        
        emscur = emsdb.cursor()
        unpros = processeddb.cursor()
        
        unpros.execute("""select FLOOR(acmeterenergy),Date(acmeterpolledtimestamp) from bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1035 and Date(acmeterpolledtimestamp) = curdate() """)
        result1 = unpros.fetchall()
        # print(result1)
        temp1 = []
        for i in range(1,len(result1)):
                temp1.append(abs(result1[i][0]-result1[i-1][0]))
                polleddate = result1[i][1]
                # print(result1[i][0])
       
        # print(temp1)

        energy1 = sum(temp1)
           
        unpros.execute("""select FLOOR(acmeterenergy),Date(acmeterpolledtimestamp) from bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1147 and Date(acmeterpolledtimestamp) = curdate()""")
        result2 = unpros.fetchall()
       
        # print(result2)

        temp2 =[]
        for i in range(1,len(result2)):
                temp2.append(abs(result2[i][0]-result2[i-1][0]))
                polleddate = result2[i][1]
                # print(result2[i][0])
               
        energy2 = sum(temp2)
       
        # print(energy1/1000)
        #print(energy2/1000)
       
        totalEnergy = (energy1+energy2)/1000
       
        # print(totalEnergy,polleddate)
       
        sql = """INSERT INTO RooftopProcessed (acEnergy,polledDate) VALUES(%s,%s)"""
        values = (totalEnergy,polleddate)
        try:
            emscur.execute(sql,values)
            print("Rofftop data inserted",totalEnergy)
            emsdb.commit()
        except mysql.connector.IntegrityError:
            sql = """UPDATE RooftopProcessed SET acEnergy = %s where polledDate = %s"""
            values = (totalEnergy,polleddate)
            emscur.execute(sql,values)
            print("Rooftop data updated",totalEnergy)
            emsdb.commit()
           
            try:
                emscur.execute(sql,values)
                print("Rofftop data inserted",totalEnergy)
                emsdb.commit()
            except mysql.connector.IntegrityError:
                sql = """UPDATE RooftopProcessed SET acEnergy = %s where polledDate = %s"""
                values = (totalEnergy,polleddate)
                emscur.execute(sql,values)
                print("Rooftop data updated",totalEnergy)
                emsdb.commit()
        
        
        emscur.close()
        unpros.close()
        data = {'message': 'ROOFTOP DAILY updated'}
        return jsonify(data), 200
    else:
        # Return an error response for unauthorized access
        return jsonify({'error': 'Unauthorized'}), 401  

@app.route('/thermalquarterl', methods = ['GET'])
def thermalQuarterly():
    token = request.headers.get('Authorization')
    print(token)
    try:
        unprocesseddb = mysql.connector.connect(
            host=bmshost,
            user="emsrouser",
            password="emsrouser@151",
            database='bmsmgmtprodv13',
           port=3306
            )
    except Exception as ex:
        print(ex)

    emsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                )

    if token and check_authentication(token):
        emscur = emsdb.cursor()

        unpdbcursor = unprocesseddb.cursor()
        try:
            unpdbcursor.execute("""select polledTime,coolingEnergyConsumption,tsOutletADPvalveStatus,tsOutletBDPvalveStatus,HValve from thermalStorageMQTTReadings where Date(polledTime) = curdate() order by polledTime asc ;""")
        except mysql.connector.errors.OperationalError as e:
            print("Lost connection to MySQL server: {}".format(e))

        res = unpdbcursor.fetchall()

        min_dict = {}

        def Segregation(polledTime,Energy,adpv,bdpv,hv):
            mins = str(polledTime)[0:17]+"00"
            # print(polledTime,Energy,adpv,bdpv,hv)
            if mins not in min_dict.keys():
                if adpv == 1 and bdpv == 1 and hv == 1:
                    min_dict[mins] = Energy/100
                else:
                    min_dict[mins] = 0

        for i in range(1,len(res)):
            Segregation(res[i][0],(res[i][1]-res[i-1][1]),res[i][2],res[i][3],res[i][4])

        qtr_dict = {}

        for i in min_dict.keys():
            minu = int(i[14:16])

            if minu>=0 and minu<15:
                qtr = i[0:14]+"00:00"
                if qtr in qtr_dict.keys():
                    qtr_dict[qtr] += min_dict[i]
                else:
                    qtr_dict[qtr] = min_dict[i]

            elif minu>=15 and minu<30:
                qtr = i[0:14]+"15:00"
                if qtr in qtr_dict.keys():
                    qtr_dict[qtr] += min_dict[i]
                else:
                    qtr_dict[qtr] = min_dict[i]

            elif minu>=30 and minu<45:
                qtr = i[0:14]+"30:00"
                if qtr in qtr_dict.keys():
                    qtr_dict[qtr] += min_dict[i]
                else:
                    qtr_dict[qtr] = min_dict[i]

            elif minu>=45 and minu<60:
                qtr = i[0:14]+"45:00"
                if qtr in qtr_dict.keys():
                    qtr_dict[qtr] += min_dict[i]
                else:
                    qtr_dict[qtr] = min_dict[i]

        for i in qtr_dict.keys():
            sql = """INSERT INTO ThermalQuarter (coolingEnergy,polledTime) VALUES (%s,%s)"""
            values = (qtr_dict[i],i)
            try:
                emscur.execute(sql,values)
                print("Thermal storage quarter data written")
                emsdb.commit()
            except mysql.connector.IntegrityError:
                sql = """ UPDATE ThermalQuarter SET coolingEnergy = %s where polledTime = %s """
                values = (qtr_dict[i],i)
                emscur.execute(sql,values)
                emsdb.commit()
                print("Thermal storage quarted data updated")
        
        emscur.close()
        unpdbcursor.close()
        data = {'message': 'Thermal Quarter updated'}
        return jsonify(data), 200
    else:
        # Return an error response for unauthorized access
        return jsonify({'error': 'Unauthorized'}), 401 



#Thermal data api
@app.route('/thermalhourly', methods = ['GET'])
def thermalHourly():
    token = request.headers.get('Authorization')
    print(token)

    emsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                )

    try:
        unprocesseddb = mysql.connector.connect(
		host=bmshost,
		user="emsrouser",
		password="emsrouser@151",
		database='bmsmgmtprodv13',
		port=3306
	      )
    except:
         return {"error":"mysql connection"}
         
    if token and check_authentication(token):
        
        unproscur = unprocesseddb.cursor()
        emscur = emsdb.cursor()

        min_dict = {}
        hr_dict = {}

        def Segregation(polledTime,Energy,adpv,bdpv,hv):
            mins = str(polledTime)[0:17]+"00"
            # print(polledTime,Energy,adpv,bdpv,hv)
            if mins not in min_dict.keys():
                if adpv == 1 and bdpv == 1 and hv == 1:
                    min_dict[mins] = Energy/100
                else:
                    min_dict[mins] = 0
        
        try:
            unproscur.execute("""select polledTime,coolingEnergyConsumption,tsOutletADPvalveStatus,tsOutletBDPvalveStatus,HValve from thermalStorageMQTTReadings where Date(polledTime) = curdate() order by polledTime asc ;""")
        except mysql.connector.errors.OperationalError as e:
            print("Lost connection to MySQL server: {}".format(e))

        res = unproscur.fetchall()

        for i in range(1,len(res)):
                Segregation(res[i][0],(res[i][1]-res[i-1][1]),res[i][2],res[i][3],res[i][4])


        for i in min_dict.keys():
            polledTime = i[0:14]+"00:00"
            
            if polledTime in hr_dict.keys():
                hr_dict[polledTime] += min_dict[i]
            else:
                hr_dict[polledTime] = min_dict[i]

        
        for i in hr_dict.keys():
            val = (hr_dict[i],i)
            sql = "INSERT INTO EMS.ThermalHourly(coolingEnergy,polledTime) values(%s,%s)"
            try:
                print(val)
                emscur.execute(sql,val)
                emsdb.commit()
                print("Thermal Hourly Inserted")
            except mysql.connector.IntegrityError:
                sql = """UPDATE EMS.ThermalHourly SET coolingEnergy = %s where polledTime = %s"""
                values = (hr_dict[i],i)
                emscur.execute(sql,values)
                emsdb.commit()
                print("Thermal storage quarted data updated")
        
        unproscur.close()
        emscur.close()
        
        data = {"message":"THERMAL HOURLY DATA updated"}
        return jsonify(data), 200
    else:
        # Return an error response for unauthorized access
        return jsonify({'error': 'Unauthorized'}), 401 
 
 
@app.route('/thermalstatus', methods=['GET'])
def thermalstatus():
    token = request.headers.get('Authorization')
    print(token)
    
    if token and check_authentication(token):
        try: 
            processeddb = mysql.connector.connect(
		host=bmshost,
		user="emsrouser",
		password="emsrouser@151",
		database='bmsmgmtprodv13',
		port=3306
	      )

        except:
            return {"error":"mysql connection"}

        emsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                )

        procur = processeddb.cursor()

        emscur = emsdb.cursor()

        procur.execute("SELECT polledTime,chargingPump1Power,chargingPump2Power,dischargingPump1Power,dischargingPump2Power FROM bmsmgmt_olap_prod_v13.hvacChillerElectricPolling where date(polledTime) = curdate();")

        res = procur.fetchall()

        dchg = {}
        chg = {}

        def hourSummarize(polledTime,chp1,chp2,dcp1,dcp2):
            curdate = str(polledTime)[0:14]+"00:00"
            if curdate in dchg.keys():
                dchg[curdate].append(dcp1)
                dchg[curdate].append(dcp2)
            else:
                dchg[curdate] = [dcp1]
   
            if curdate in chg.keys():
                chg[curdate].append(chp1)
                chg[curdate].append(chp2)
            else:
                chg[curdate] = [chp1]
         
        for i in res:
            if i[1] != None and i[2] != None and i[3] != None and i[4] != None:
                hourSummarize(i[0],i[1],i[2],i[3],i[4])

        for i in dchg.keys():
            sql = "INSERT INTO thermalStatus(polledTime,dchgStatus) VALUES(%s,%s)"
            sts = 0
            if max(dchg[i]) > 0:
                sts = 1
            val = (i,sts)
            try:
                emscur.execute(sql,val)
                emsdb.commit()
                print("Thermal status inserted")
            except mysql.connector.IntegrityError:
                sql = "UPDATE thermalStatus SET dchgStatus = %s WHERE polledTime = %s"
                val = (sts,i)
                emscur.execute(sql,val)
                emsdb.commit()
                print("Thermal status updated")
   
        for i in chg.keys():
            sql = "INSERT INTO thermalStatus(polledTime,chgStatus) VALUES(%s,%s)"
            sts = 0
            if max(chg[i]) > 0:
                sts = 1
            val = (i,sts)
            try:
                emscur.execute(sql,val)
                emsdb.commit()
                print("Thermal status inserted")
            except mysql.connector.IntegrityError:
                sql = "UPDATE thermalStatus SET chgStatus = %s WHERE polledTime = %s"
                val = (sts,i)
                emscur.execute(sql,val)
                emsdb.commit()
                print("Thermal status updated") 
        data = {"message":"THERMAL STATUS DATA updated"}
        return jsonify(data), 200
    else:
        return jsonify({'error': 'Unauthorized'}), 401 

  
@app.route('/peakdaily', methods=['GET'])
def peakDaily():
    token = request.headers.get('Authorization')
    print(token)

    emsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                )

    if token and check_authentication(token):
        emscur = emsdb.cursor()
        
        emscur.execute("select max(peakdemand),polledTime from EMS.peakdemandHourly where date(polledTime)  = curdate()")

        res = emscur.fetchall()

        for i in res:
            sql = "INSERT INTO peakdemanddaily(peakdemand,polledTime) VALUES(%s,%s)"
            val = (i[0],i[1])
            try:
                emscur.execute(sql,val)
                print("Peak demand daily updated")
                emsdb.commit()
            except mysql.connector.IntegrityError:
                sql = "UPDATE peakdemanddaily SET peakdemand = %s where polledTime = %s"
                val = (i[0],i[1])
                emscur.execute(sql,val)
                print("Peak demand daily updated")
                emsdb.commit()
        
        emscur.close()
        data = {"message":"PEAK DAILY DATA updated"}
        return jsonify(data), 200
    else:
        # Return an error response for unauthorized access
        return jsonify({'error': 'Unauthorized'}), 401


@app.route('/peakquarter', methods=['GET'])
def peakQuarter():
    token = request.headers.get('Authorization')
    print(token)
    #processeddb = mysql.connector.connect(
    #        host="121.242.232.151",
    #        user="emsrouser",
    #        password="emsrouser@151",
    #        database='bmsmgmt_olap_prod_v13',
    #        port=3306
    #        )

    emsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                )

    if token and check_authentication(token):
        #peakdem = processeddb.cursor()
        emscur = emsdb.cursor()

        emscur.execute("select totalApparentPower2,polledTime from bmsunprocessed_prodv13.hvacSchneider7230Polling WHERE date(polledTime) =curdate();")

        peakres = emscur.fetchall()

        peakqdict = {}

        def segPeak(peak,polledTime):
            poltime = str(polledTime)
            min = int(poltime[14:16])

            if min >=0 and min < 15:
                polledTime = poltime[0:14]+"00:00"
                if polledTime in peakqdict.keys():
                    peakqdict[polledTime].append(peak)
                else:
                    peakqdict[polledTime] = [peak]
            elif min >=15 and min < 30:
                polledTime = poltime[0:14]+"15:00"
                if polledTime in peakqdict.keys():
                    peakqdict[polledTime].append(peak)
                else:
                    peakqdict[polledTime] = [peak]
            elif min >=30 and min < 45:
                polledTime = poltime[0:14]+"30:00"
                if polledTime in peakqdict.keys():
                    peakqdict[polledTime].append(peak)
                else:
                    peakqdict[polledTime] = [peak]
            elif min >=45 and min <=59:
                polledTime = poltime[0:14]+"45:00"
                if polledTime in peakqdict.keys():
                    peakqdict[polledTime].append(peak)
                else:
                    peakqdict[polledTime] = [peak]
                
        for i in peakres:
            if i[0] !=None:
                segPeak(i[0],i[1])

        for i in peakqdict.keys():
            sql = "INSERT INTO peakdemandquarter(polledTime,peakDemand) VALUES(%s,%s)"
            val = (i,max(peakqdict[i]))
            try:
                emscur.execute(sql,val)
                print("Peak demand quarterly updated")
                emsdb.commit()
            except mysql.connector.IntegrityError:
                sql="UPDATE peakdemandquarter SET peakDemand = %s where polledTime = %s"
                val = (max(peakqdict[i]),1)
                emscur.execute(sql,val)
                print("Peak demand quarterly updated")
                emsdb.commit()
        
        #peakdem.close()
        emscur.close()
        
        data = {"message":"PEAK QUARTER UPDATED"}
        return jsonify(data), 200
    else:
        return jsonify({'error': 'Unauthorized'}), 401
   
@app.route('/powerfactorhourly',methods=['GET'])
def powerFactor():
    token = request.headers.get('Authorization')
    print(token)

    emsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                )

    try: 
        unprocesseddb = mysql.connector.connect(
		host=bmshost,
		user="bmsrouser6",
		password="bmsrouser6@151U",
		database='bmsmgmtprodv13',
		port=3306
	      )
    except:
         return {"error":"mysql connection"}
         
    if token and check_authentication(token):
        
        unproscur = unprocesseddb.cursor()
        emscur = emsdb.cursor()
        
        unproscur.execute("select schneidertotalpowerfactor,schneiderpolledtimestamp,schneidersubsystemid from bmsmgmtprodv13.schneider7230readings where schneidersubsystemid=346 and date(schneiderpolledtimestamp) = curdate();")

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
            
            unproscur.close()
            emscur.close()
        
            data = {"message":"POWER FACTOR updated"}
            return jsonify(data),200
        else:
            return jsonify({'error': 'Unauthorized'}), 401

@app.route('/demandvsgrid',methods=['GET'])
def demandVSGrid():
    token = request.headers.get('Authorization')
    print(token)

    try:
        processeddb = mysql.connector.connect(
		host=bmshost,
		user="emsrouser",
		password="emsrouser@151",
		database='bmsmgmtprodv13',
		port=3306
	      )
    except Exception as ex:
        print("BMS database not connected")
        print(ex)

    try:
        emsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                )
    except Exception as ex:
        print("EMS database not connected")
        print(ex)

    if token and check_authentication(token):
        try:
            procur = processeddb.cursor()
        except Exception as ex:
            print("BMS cursor error")
            print(ex)

        try:
            emscur = emsdb.cursor()
        except Exception as ex:
            print("EMS cursor error")
            print(ex)

        emscur.execute("select polledTime from EMS.peakDemandvs order by polledTime desc limit 1;")

        lasttim = emscur.fetchall()

        lasttime = str(lasttim[0][0])

        procur.execute(f"SELECT polledTime,totalApparentPower2 FROM bmsmgmt_olap_prod_v13.hvacSchneider7230Polling where polledTime > '{lasttime}';")

        peakres = procur.fetchall()

        peak_dict = {}

        def peakSeg(polledTime,demand):
            min = str(polledTime)[0:17]+"00"
            if min not in peak_dict.keys():
                peak_dict[min] = demand


        for i in peakres:
            peakSeg(i[0],i[1])

        procur.execute(f"select FLOOR(acmeterenergy),polledTime from bmsmgmt_olap_prod_v13.MVPPolling where mvpnum in ('MVP1','MVP2','MVP3','MVP4') and polledTime > '{lasttime}';")

        gridres = procur.fetchall()

        min_dict = {}

        def minSeg(polledTime,Energy):
            mins = str(polledTime)[0:17]+"00"
            if Energy != None:
                if mins in min_dict.keys():
                    min_dict[mins] += Energy
                else:
                    min_dict[mins] = Energy


        for i in gridres:
            minSeg(i[1],i[0])
            
        keys_order = list(min_dict.keys())

        previous_val = None

        min_grid = {}

        for key in keys_order:
            current_val = min_dict[key]

            if previous_val is not None:
                diff = (current_val - previous_val)/1000
                
                min_grid[key] = diff

            previous_val = current_val

        for i in peak_dict.keys():
            sql = "INSERT INTO EMS.peakDemandvs(polledTime,peakDemand) VALUES(%s,%s)"
            val = (i,peak_dict[i])
            try:
                emscur.execute(sql,val)
                emsdb.commit()
                print("peak demand minute inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE peakDemandvs SET peakDemand = %s WHERE polledTime = %s"
                val = (peak_dict[i],i)
                emscur.execute(sql,val)
                emsdb.commit()
                print("peak demand minute inserted")

        for i in min_grid.keys():
            sql = "INSERT INTO EMS.peakDemandvs(polledTime,gridEnergy) VALUES(%s,%s)"
            val = (i,min_grid[i])
            try:
                emscur.execute(sql,val)
                emsdb.commit()
                print("Grid minute inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE peakDemandvs SET gridEnergy = %s WHERE polledTime = %s"
                val = (min_grid[i],i)
                emscur.execute(sql,val)
                emsdb.commit()
                print("Grid minute inserted")
        
        data = {"message":"POWER FACTOR updated"}
        return jsonify(data),200
    else:
        return jsonify({'error': 'Unauthorized'}), 401

   
if __name__ == '__main__':
    app.run(host="localhost",port=8000)
