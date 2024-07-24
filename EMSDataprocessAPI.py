from flask import Flask, jsonify, request
import mysql.connector
from datetime import datetime
from decimal import Decimal
from collections import defaultdict
import pandas as pd
import json

app = Flask(__name__)

bmshost = '121.242.232.151'
awshost = '3.111.70.53'
emshost = '121.242.232.211'

def check_authentication(token):
    # Replace this with your own logic to validate the token
    valid_token = "VKOnNhH2SebMU6S"
    return token == valid_token

@app.route('/windHourly', methods = ['GET'])
def windHourly():
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
            
            bmsdb = mysql.connector.connect(
                        host=bmshost,
                        user="emsrouser",
                        password="emsrouser@151",
                        database='bmsmgmtprodv13',
                        port=3306
                        )
            bmscur = bmsdb.cursor()
            awscur = awsdb.cursor()

            windDict = {}

            bmscur.execute("""SELECT FROM_UNIXTIME(otpmgndetailspolledtimestamp) as polledTime,otpmgndetailsactivepower/60 as Energy  
                FROM bmsmgmtprodv13.otpmgndetails where date(FROM_UNIXTIME(otpmgndetailspolledtimestamp)) = curdate()
                and otpmgndetailsactivepower > 0;""")
            
            res = bmscur.fetchall()

            def HourSummarize(polledTime,Energy):
                polledTime = str(polledTime)[0:14]
                
                if polledTime in windDict.keys():
                    windDict[polledTime] += Energy
                else:
                    windDict[polledTime] = Energy

            for i in res:
                if i[1] != None:
                    Energy = i[1] * 0.60
                    HourSummarize(i[0],Energy)

            for i in windDict.keys():
                val = (round(windDict[i],2),i)
                sql = "INSERT INTO EMS.windHourly(Energy,polledTime) VALUES(%s,%s)"
                try:
                    awscur.execute(sql,val)
                    awsdb.commit()
                    print(val)
                    print("Wind Hourly inserted")
                except mysql.connector.errors.IntegrityError:
                    sql = "UPDATE EMS.windHourly set Energy = %s where polledTime = %s"
                    awscur.execute(sql,val)
                    awsdb.commit()
                    print(val)
                    print("Wind Hourly updated")
        except:
            return {"error":"mysql connection"}

        data = {"message":"Wind hourly Updated"}
        return jsonify(data), 200
    
    else:
        return jsonify({'error': 'Unauthorized'}), 401

@app.route('/ioeHourly', methods = ['GET'])
def ioeHourly():
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
            
            emsdb = mysql.connector.connect(
                        host=emshost,
                        user="emsroot",
                        password="22@teneT",
                        database='EMS',
                        port=3306
                        )
            emscur = emsdb.cursor()
            awscur = awsdb.cursor()
        except:
            return {"error":"mysql connection"}

        emscur.execute("""select batteryStatus,chargingEnergy,dischargingEnergy,packUsableSOC,recordTimestamp from ioeSt1BatteryData where date(recordTimestamp) = curdate();""")

        results1 = emscur.fetchall()

        chgdict1 = {}
        dchgdict1 ={}
        chgmin1 = {}
        dchgmin1 = {}
        packdict1 = {}
        chglist1 =[]
        dchglist1 = []

        def summarizePacksoc1(polledTime,packSoc):
            hour = str(polledTime)[0:14]+"00:00"
            if hour not in packdict1.keys():
                packdict1[hour] = [packSoc]
            else:
                packdict1[hour].append(packSoc)

        def summarizeHourChg1(polledTime,charging):
            hour = str(polledTime)[0:14]+"00:00"
            
            if hour not in chgdict1.keys():
                chgdict1[hour] = charging
            else:
                chgdict1[hour] += charging
                
        def summarizeHourDchg1(polledTime,discharging):
            hour = str(polledTime)[0:14]+"00:00"
            if hour not in dchgdict1.keys():
                dchgdict1[hour] = discharging
            else:
                dchgdict1[hour] += discharging


        def cummlativeChg1(polledTime,charging):
            chglist1.append([polledTime,charging])


        def cummlativeDchg1(polledTime,discharging):
            dchglist1.append([polledTime,discharging])


        def summarizeMin1(status,charging,discharging,polledTime):
            min = str(polledTime)[0:17]+"00"
            if status == "CHG" and charging!=None:
                if min not in chgmin1.keys():
                    chgmin1[min] = [charging]
                else:
                    chgmin1[min].append(charging)
            elif status == "DCHG" and discharging!= None:
                if min not in dchgmin1.keys():
                    # print(discharging)
                    dchgmin1[min] = [discharging]
                else:
                    dchgmin1[min].append(discharging)

        
        for i in range(0,len(results1)):
            summarizeMin1(results1[i][0],results1[i][1],results1[i][2],results1[i][4])
            if results1[i][3] != None:
                if results1[i][3]<=100:
                #print(results[i][4],results[i][3])
                    summarizePacksoc1(results1[i][4],results1[i][3])    

        for i in chgmin1.keys():
            cummlativeChg1(i,max(chgmin1[i]))

        for i in dchgmin1.keys():
            cummlativeDchg1(i,max(dchgmin1[i]))
            
        for i in range(1,len(chglist1)):
            if abs(chglist1[i][1]-chglist1[i-1][1]) > 0:
                summarizeHourChg1(chglist1[i][0],(chglist1[i][1]-chglist1[i-1][1])/1000)
                # print(chglist[i][0],(chglist[i][1]-chglist[i-1][1])/1000)

        for i in range(0,len(dchglist1)):
            if dchglist1[i][1]-dchglist1[i-1][1] > 0:
                # print(dchglist[i][0],dchglist[i][1]-dchglist[i-1][1])
                summarizeHourDchg1(dchglist1[i][0],(dchglist1[i][1]-dchglist1[i-1][1])/1000)
            
        # for i in dchgmin.keys():
        #     summarizeHourDchg(i,max(dchgmin[i]))
                
        for i in chgdict1.keys():
            print(i,round(abs(chgdict1[i]),2))
            sql = "INSERT INTO IOEbatteryHourly(polledTime,st1chargingEnergy) VALUES(%s,%s)"
            val = (i,abs(chgdict1[i]))
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Battery chg hourly")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE IOEbatteryHourly SET st1chargingEnergy = %s WHERE polledTime = %s"
                val = (abs(chgdict1[i]),i)
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Battery chg hourly")
                
        for i in packdict1.keys():
            print(i,max(packdict1[i]))
            sql = "INSERT INTO IOEbatteryHourly(polledTime,st1packsoc) VALUES(%s,%s)"
            val = (i,max(packdict1[i]))
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print("Battery insert packsoc hourly")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE IOEbatteryHourly SET st1packsoc = %s WHERE polledTime = %s"
                val = (max(packdict1[i]),i)
                awscur.execute(sql,val)
                awsdb.commit()
                print("Battery update packsoc hourly")
        

        for i in dchgdict1.keys():
            print(i,round(-abs(dchgdict1[i]),2))
            sql = "INSERT INTO IOEbatteryHourly(polledTime,st1dischargingEnergy) VALUES(%s,%s)"
            val = (i,round(-abs(dchgdict1[i]),2))
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Battery dchg hourly")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE IOEbatteryHourly SET st1dischargingEnergy = %s WHERE polledTime = %s"
                val = (round(-abs(dchgdict1[i]),2),i)
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Battery dchg hourly")

        emscur.execute("""select batteryStatus,chargingEnergy,dischargingEnergy,packUsableSOC,recordTimestamp from ioeSt2BatteryData where date(recordTimestamp) = curdate();""")

        results2 = emscur.fetchall()

        chgdict2 = {}
        dchgdict2 ={}
        chgmin2 = {}
        dchgmin2 = {}
        packdict2 = {}
        chglist2 =[]
        dchglist2 = []

        def summarizePacksoc2(polledTime,packSoc):
            hour = str(polledTime)[0:14]+"00:00"
            if hour not in packdict2.keys():
                packdict2[hour] = [packSoc]
            else:
                packdict2[hour].append(packSoc)

        def summarizeHourChg2(polledTime,charging):
            hour = str(polledTime)[0:14]+"00:00"
            
            if hour not in chgdict2.keys():
                chgdict2[hour] = charging
            else:
                chgdict2[hour] += charging
                
        def summarizeHourDchg2(polledTime,discharging):
            hour = str(polledTime)[0:14]+"00:00"
            if hour not in dchgdict2.keys():
                dchgdict2[hour] = discharging
            else:
                dchgdict2[hour] += discharging


        def cummlativeChg2(polledTime,charging):
            chglist2.append([polledTime,charging])


        def cummlativeDchg2(polledTime,discharging):
            dchglist2.append([polledTime,discharging])


        def summarizeMin2(status,charging,discharging,polledTime):
            min = str(polledTime)[0:17]+"00"
            if status == "CHG" and charging!=None:
                if min not in chgmin2.keys():
                    chgmin2[min] = [charging]
                else:
                    chgmin2[min].append(charging)
            elif status == "DCHG" and discharging!= None:
                if min not in dchgmin2.keys():
                    # print(discharging)
                    dchgmin2[min] = [discharging]
                else:
                    dchgmin2[min].append(discharging)

        
        for i in range(0,len(results2)):
            summarizeMin2(results2[i][0],results2[i][1],results2[i][2],results2[i][4])
            if results2[i][3] != None:
                if results2[i][3]<=100:
                #print(results[i][4],results[i][3])
                    summarizePacksoc2(results2[i][4],results2[i][3])

        for i in chgmin2.keys():
            cummlativeChg2(i,max(chgmin2[i]))

        for i in dchgmin2.keys():
            cummlativeDchg2(i,max(dchgmin2[i]))
            
        for i in range(1,len(chglist2)):
            if abs(chglist2[i][1]-chglist2[i-1][1]) > 0:
                summarizeHourChg2(chglist2[i][0],(chglist2[i][1]-chglist2[i-1][1])/1000)
                # print(chglist[i][0],(chglist[i][1]-chglist[i-1][1])/1000)

        for i in range(0,len(dchglist2)):
            if dchglist2[i][1]-dchglist2[i-1][1] > 0:
                # print(dchglist[i][0],dchglist[i][1]-dchglist[i-1][1])
                summarizeHourDchg2(dchglist2[i][0],(dchglist2[i][1]-dchglist2[i-1][1])/1000)
            
        # for i in dchgmin.keys():
        #     summarizeHourDchg(i,max(dchgmin[i]))

        for i in chgdict2.keys():
            print(i,round(abs(chgdict2[i]),2))
            sql = "INSERT INTO IOEbatteryHourly(polledTime,st2chargingEnergy) VALUES(%s,%s)"
            val = (i,abs(chgdict2[i]))
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Battery chg hourly")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE IOEbatteryHourly SET st2chargingEnergy = %s WHERE polledTime = %s"
                val = (abs(chgdict2[i]),i)
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Battery chg hourly")
                
        for i in packdict2.keys():
            print(i,max(packdict2[i]))
            sql = "INSERT INTO IOEbatteryHourly(polledTime,st2packsoc) VALUES(%s,%s)"
            val = (i,max(packdict2[i]))
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print("Battery insert packsoc hourly")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE IOEbatteryHourly SET st2packsoc = %s WHERE polledTime = %s"
                val = (max(packdict2[i]),i)
                awscur.execute(sql,val)
                awsdb.commit()
                print("Battery update packsoc hourly")

        for i in dchgdict2.keys():
            print(i,round(-abs(dchgdict2[i]),2))
            sql = "INSERT INTO IOEbatteryHourly(polledTime,st2dischargingEnergy) VALUES(%s,%s)"
            val = (i,round(-abs(dchgdict2[i]),2))
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Battery dchg hourly")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE IOEbatteryHourly SET st2dischargingEnergy = %s WHERE polledTime = %s"
                val = (round(-abs(dchgdict2[i]),2),i)
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                
        emscur.execute("""select batteryStatus,chargingEnergy,dischargingEnergy,packUsableSOC,recordTimestamp from ioeSt3BatteryData where date(recordTimestamp) = curdate();""")

        results3 = emscur.fetchall()

        chgdict3 = {}
        dchgdict3 ={}
        chgmin3 = {}
        dchgmin3 = {}
        packdict3 = {}
        chglist3 =[]
        dchglist3 = []

        def summarizePacksoc3(polledTime,packSoc):
            hour = str(polledTime)[0:14]+"00:00"
            if hour not in packdict3.keys():
                packdict3[hour] = [packSoc]
            else:
                packdict3[hour].append(packSoc)

        def summarizeHourChg3(polledTime,charging):
            hour = str(polledTime)[0:14]+"00:00"
            
            if hour not in chgdict3.keys():
                chgdict3[hour] = charging
            else:
                chgdict3[hour] += charging
                
        def summarizeHourDchg3(polledTime,discharging):
            hour = str(polledTime)[0:14]+"00:00"
            if hour not in dchgdict3.keys():
                dchgdict3[hour] = discharging
            else:
                dchgdict3[hour] += discharging


        def cummlativeChg3(polledTime,charging):
            chglist3.append([polledTime,charging])


        def cummlativeDchg3(polledTime,discharging):
            dchglist3.append([polledTime,discharging])


        def summarizeMin3(status,charging,discharging,polledTime):
            min = str(polledTime)[0:17]+"00"
            if status == "CHG" and charging!=None:
                if min not in chgmin3.keys():
                    chgmin3[min] = [charging]
                else:
                    chgmin3[min].append(charging)
            elif status == "DCHG" and discharging!= None:
                if min not in dchgmin3.keys():
                    # print(discharging)
                    dchgmin3[min] = [discharging]
                else:
                    dchgmin3[min].append(discharging)

        
        for i in range(0,len(results3)):
            summarizeMin3(results3[i][0],results3[i][1],results3[i][2],results3[i][4])
            if results3[i][3] != None:
                if results3[i][3]<=100:
                #print(results[i][4],results[i][3])
                    summarizePacksoc3(results3[i][4],results3[i][3])

        for i in chgmin3.keys():
            cummlativeChg3(i,max(chgmin3[i]))

        for i in dchgmin3.keys():
            cummlativeDchg3(i,max(dchgmin3[i]))
            
        for i in range(1,len(chglist3)):
            if abs(chglist3[i][1]-chglist3[i-1][1]) > 0:
                summarizeHourChg3(chglist3[i][0],(chglist3[i][1]-chglist3[i-1][1])/1000)
                # print(chglist[i][0],(chglist[i][1]-chglist[i-1][1])/1000)

        for i in range(0,len(dchglist3)):
            if dchglist3[i][1]-dchglist3[i-1][1] > 0:
                # print(dchglist[i][0],dchglist[i][1]-dchglist[i-1][1])
                summarizeHourDchg2(dchglist3[i][0],(dchglist3[i][1]-dchglist3[i-1][1])/1000)
            


        for i in chgdict3.keys():
            print(i,round(abs(chgdict3[i]),2))
            sql = "INSERT INTO IOEbatteryHourly(polledTime,st3chargingEnergy) VALUES(%s,%s)"
            val = (i,abs(chgdict3[i]))
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Battery chg hourly")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE IOEbatteryHourly SET st3chargingEnergy = %s WHERE polledTime = %s"
                val = (abs(chgdict3[i]),i)
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Battery chg hourly")
                
        for i in packdict3.keys():
            print(i,max(packdict3[i]))
            sql = "INSERT INTO IOEbatteryHourly(polledTime,st3packsoc) VALUES(%s,%s)"
            val = (i,max(packdict3[i]))
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print("Battery insert packsoc hourly")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE IOEbatteryHourly SET st3packsoc = %s WHERE polledTime = %s"
                val = (max(packdict3[i]),i)
                awscur.execute(sql,val)
                awsdb.commit()
                print("Battery update packsoc hourly")

        for i in dchgdict3.keys():
            print(i,round(-abs(dchgdict3[i]),2))
            sql = "INSERT INTO IOEbatteryHourly(polledTime,st3dischargingEnergy) VALUES(%s,%s)"
            val = (i,round(-abs(dchgdict3[i]),2))
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Battery dchg hourly")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE IOEbatteryHourly SET st3dischargingEnergy = %s WHERE polledTime = %s"
                val = (round(-abs(dchgdict3[i]),2),i)
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Battery dchg hourly")

        emscur.execute("""select batteryStatus,chargingEnergy,dischargingEnergy,packUsableSOC,recordTimestamp from ioeSt4BatteryData where date(recordTimestamp) = curdate();""")

        results4 = emscur.fetchall()

        chgdict4 = {}
        dchgdict4 ={}
        chgmin4 = {}
        dchgmin4 = {}
        packdict4 = {}
        chglist4 =[]
        dchglist4 = []

        def summarizePacksoc4(polledTime,packSoc):
            hour = str(polledTime)[0:14]+"00:00"
            if hour not in packdict4.keys():
                packdict4[hour] = [packSoc]
            else:
                packdict4[hour].append(packSoc)

        def summarizeHourChg4(polledTime,charging):
            hour = str(polledTime)[0:14]+"00:00"
            
            if hour not in chgdict4.keys():
                chgdict4[hour] = charging
            else:
                chgdict4[hour] += charging
                
        def summarizeHourDchg4(polledTime,discharging):
            hour = str(polledTime)[0:14]+"00:00"
            if hour not in dchgdict4.keys():
                dchgdict4[hour] = discharging
            else:
                dchgdict4[hour] += discharging


        def cummlativeChg4(polledTime,charging):
            chglist4.append([polledTime,charging])


        def cummlativeDchg4(polledTime,discharging):
            dchglist4.append([polledTime,discharging])


        def summarizeMin4(status,charging,discharging,polledTime):
            min = str(polledTime)[0:17]+"00"
            if status == "CHG" and charging!=None:
                if min not in chgmin4.keys():
                    chgmin4[min] = [charging]
                else:
                    chgmin4[min].append(charging)
            elif status == "DCHG" and discharging!= None:
                if min not in dchgmin4.keys():
                    # print(discharging)
                    dchgmin4[min] = [discharging]
                else:
                    dchgmin4[min].append(discharging)

        
        for i in range(0,len(results4)):
            summarizeMin4(results4[i][0],results4[i][1],results4[i][2],results4[i][4])
            if results4[i][3] != None:
                if results4[i][3]<=100:
                #print(results[i][4],results[i][3])
                    summarizePacksoc4(results4[i][4],results4[i][3])

        for i in chgmin4.keys():
            cummlativeChg4(i,max(chgmin4[i]))

        for i in dchgmin4.keys():
            cummlativeDchg4(i,max(dchgmin4[i]))
            
        for i in range(1,len(chglist4)):
            if abs(chglist4[i][1]-chglist4[i-1][1]) > 0:
                summarizeHourChg4(chglist4[i][0],(chglist4[i][1]-chglist4[i-1][1])/1000)
                # print(chglist[i][0],(chglist[i][1]-chglist[i-1][1])/1000)

        for i in range(0,len(dchglist4)):
            if dchglist4[i][1]-dchglist4[i-1][1] > 0:
                # print(dchglist[i][0],dchglist[i][1]-dchglist[i-1][1])
                summarizeHourDchg4(dchglist4[i][0],(dchglist4[i][1]-dchglist4[i-1][1])/1000)
            
        # for i in dchgmin.keys():
        #     summarizeHourDchg(i,max(dchgmin[i]))
                
        #    print() 

        for i in chgdict4.keys():
            print(i,round(abs(chgdict4[i]),2))
            sql = "INSERT INTO IOEbatteryHourly(polledTime,st4chargingEnergy) VALUES(%s,%s)"
            val = (i,abs(chgdict4[i]))
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Battery chg hourly")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE IOEbatteryHourly SET st4chargingEnergy = %s WHERE polledTime = %s"
                val = (abs(chgdict4[i]),i)
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Battery chg hourly")
                
        for i in packdict4.keys():
            print(i,max(packdict4[i]))
            sql = "INSERT INTO IOEbatteryHourly(polledTime,st4packsoc) VALUES(%s,%s)"
            val = (i,max(packdict4[i]))
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print("Battery insert packsoc hourly")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE IOEbatteryHourly SET st4packsoc = %s WHERE polledTime = %s"
                val = (max(packdict4[i]),i)
                awscur.execute(sql,val)
                awsdb.commit()
                print("Battery update packsoc hourly")

        for i in dchgdict4.keys():
            print(i,round(-abs(dchgdict4[i]),2))
            sql = "INSERT INTO IOEbatteryHourly(polledTime,st4dischargingEnergy) VALUES(%s,%s)"
            val = (i,round(-abs(dchgdict4[i]),2))
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Battery dchg hourly")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE IOEbatteryHourly SET st4dischargingEnergy = %s WHERE polledTime = %s"
                val = (round(-abs(dchgdict4[i]),2),i)
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Battery dchg hourly")  

        emscur.execute("""select batteryStatus,chargingEnergy,dischargingEnergy,packUsableSOC,recordTimestamp from ioeSt5BatteryData where date(recordTimestamp) = curdate();""")

        results5 = emscur.fetchall()

        chgdict5 = {}
        dchgdict5 ={}
        chgmin5 = {}
        dchgmin5 = {}
        packdict5 = {}
        chglist5 =[]
        dchglist5 = []

        def summarizePacksoc5(polledTime,packSoc):
            hour = str(polledTime)[0:14]+"00:00"
            if hour not in packdict5.keys():
                packdict5[hour] = [packSoc]
            else:
                packdict5[hour].append(packSoc)

        def summarizeHourChg5(polledTime,charging):
            hour = str(polledTime)[0:14]+"00:00"
            
            if hour not in chgdict5.keys():
                chgdict5[hour] = charging
            else:
                chgdict5[hour] += charging
                
        def summarizeHourDchg5(polledTime,discharging):
            hour = str(polledTime)[0:14]+"00:00"
            if hour not in dchgdict5.keys():
                dchgdict5[hour] = discharging
            else:
                dchgdict5[hour] += discharging


        def cummlativeChg5(polledTime,charging):
            chglist5.append([polledTime,charging])


        def cummlativeDchg5(polledTime,discharging):
            dchglist5.append([polledTime,discharging])


        def summarizeMin5(status,charging,discharging,polledTime):
            min = str(polledTime)[0:17]+"00"
            if status == "CHG" and charging!=None:
                if min not in chgmin5.keys():
                    chgmin5[min] = [charging]
                else:
                    chgmin5[min].append(charging)
            elif status == "DCHG" and discharging!= None:
                if min not in dchgmin5.keys():
                    # print(discharging)
                    dchgmin5[min] = [discharging]
                else:
                    dchgmin5[min].append(discharging)

        
        for i in range(0,len(results5)):
            summarizeMin5(results5[i][0],results5[i][1],results5[i][2],results5[i][4])
            if results5[i][3] != None:
                if results5[i][3]<=100:
                #print(results[i][4],results[i][3])
                    summarizePacksoc5(results5[i][4],results5[i][3])


        for i in chgmin5.keys():
            cummlativeChg5(i,max(chgmin5[i]))

        for i in dchgmin5.keys():
            cummlativeDchg5(i,max(dchgmin5[i]))
            
        for i in range(1,len(chglist5)):
            if abs(chglist5[i][1]-chglist5[i-1][1]) > 0:
                summarizeHourChg5(chglist5[i][0],(chglist5[i][1]-chglist5[i-1][1])/1000)
                # print(chglist[i][0],(chglist[i][1]-chglist[i-1][1])/1000)

        for i in range(0,len(dchglist5)):
            if dchglist5[i][1]-dchglist5[i-1][1] > 0:
                # print(dchglist[i][0],dchglist[i][1]-dchglist[i-1][1])
                summarizeHourDchg5(dchglist5[i][0],(dchglist5[i][1]-dchglist5[i-1][1])/1000)
            
        for i in chgdict5.keys():
            print(i,round(abs(chgdict5[i]),2))
            sql = "INSERT INTO IOEbatteryHourly(polledTime,st5chargingEnergy) VALUES(%s,%s)"
            val = (i,abs(chgdict5[i]))
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Battery chg hourly")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE IOEbatteryHourly SET st5chargingEnergy = %s WHERE polledTime = %s"
                val = (abs(chgdict5[i]),i)
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Battery chg hourly")
                
        for i in packdict5.keys():
            print(i,max(packdict5[i]))
            sql = "INSERT INTO IOEbatteryHourly(polledTime,st5packsoc) VALUES(%s,%s)"
            val = (i,max(packdict5[i]))
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print("Battery insert packsoc hourly")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE IOEbatteryHourly SET st5packsoc = %s WHERE polledTime = %s"
                val = (max(packdict5[i]),i)
                awscur.execute(sql,val)
                awsdb.commit()
                print("Battery update packsoc hourly")

        for i in dchgdict5.keys():
            print(i,round(-abs(dchgdict5[i]),2))
            sql = "INSERT INTO IOEbatteryHourly(polledTime,st5dischargingEnergy) VALUES(%s,%s)"
            val = (i,round(-abs(dchgdict5[i]),2))
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Battery dchg hourly")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE IOEbatteryHourly SET st5dischargingEnergy = %s WHERE polledTime = %s"
                val = (round(-abs(dchgdict5[i]),2),i)
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Battery dchg hourly")  
    
        data = {"message":"Ioe hourly Updated"}
        return jsonify(data), 200
    
    else:
        return jsonify({'error': 'Unauthorized'}), 401

@app.route('/clientpower', methods = ['GET'])
def clientPower():
    token = request.headers.get('Authorization')
    print(token)

    file_path = './tenant_info.json'

    with open(file_path, 'r') as json_file:
        tenant_info = json.load(json_file)

    if token and check_authentication(token):
        try:
            bmsdb =  mysql.connector.connect(
                host=bmshost,
                user="emsrouser",
                password="emsrouser@151",
                database='bmsmgmtprodv13',
                port=3306
                )

            awsdb = mysql.connector.connect(
                        host=awshost,
                        user="emsroot",
                        password="22@teneT",
                        database='EMS',
                        port=3307
                        )

            bmscur = bmsdb.cursor()
            awscur = awsdb.cursor()
        except Exception as ex:
            print(ex)
            return {"Error":"Mysql connector"}
        
        bmscur.execute("""SELECT 
                    acmetersubsystemid,
                    CONCAT(DATE_FORMAT(acmeterpolledtimestamp, '%Y-%m-%d %H:'), LPAD(FLOOR(MINUTE(acmeterpolledtimestamp) / 15) * 15, 2, '0')) AS fifteen_min_interval,
                    MAX(acmeterpower) AS max_power
                FROM 
                    bmsmgmtprodv13.acmeterreadings
                WHERE 
                    acmeterpolledtimestamp >= CURDATE() AND acmeterpolledtimestamp < CURDATE() + INTERVAL 1 DAY
                GROUP BY 
                    acmetersubsystemid, 
                    CONCAT(DATE_FORMAT(acmeterpolledtimestamp, '%Y-%m-%d %H:'), LPAD(FLOOR(MINUTE(acmeterpolledtimestamp) / 15) * 15, 2, '0'));""")

        res = bmscur.fetchall()

        df = pd.DataFrame(res, columns=['acmetersubsystemid', 'polledTime', 'max_power'])

        tenant_df = pd.DataFrame.from_dict(tenant_info, orient='index', columns=['TenantName', 'Block', 'Floor','MVPno'])

        tenant_df.reset_index(inplace=True)

        tenant_df.rename(columns={'index': 'acmetersubsystemid'}, inplace=True)

        tenant_df['acmetersubsystemid'] = tenant_df['acmetersubsystemid'].astype(int)

        result_df = pd.merge(df, tenant_df, on='acmetersubsystemid', how='left')

        filtered_df = result_df[result_df['TenantName'].notna()]

        sum_power_df = filtered_df.groupby(['polledTime','acmetersubsystemid', 'TenantName', 'Block', 'Floor', 'MVPno'])['max_power'].sum().reset_index()

        print(sum_power_df.tail(100))

        updated_df = sum_power_df.tail(500)

        for index,row in updated_df.iterrows():
            print(row['acmetersubsystemid'],row['polledTime'],row['TenantName'],row['Block'],row['Floor'],row['MVPno'],row['max_power'])
            sql = """INSERT INTO EMS.ClientPower (subsystemid, polledTime, TenantName, Block, Floor, MVPno, max_power)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                TenantName = VALUES(TenantName),
                                Block = VALUES(Block),
                                Floor = VALUES(Floor),
                                MVPno = VALUES(MVPno),
                                max_power = VALUES(max_power)"""
            val = (row['acmetersubsystemid'], row['polledTime'], row['TenantName'], row['Block'], row['Floor'], row['MVPno'], row['max_power'])

            awscur.execute(sql,val)
            awsdb.commit()

        data = {"message":"Client Power Updated"}
        return jsonify(data), 200
    
    else:
        return jsonify({'error': 'Unauthorized'}), 401

@app.route('/maxpeakjmp', methods = ['GET'])
def maxpeakjmp():
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        try:
            bmsdb =  mysql.connector.connect(
            host=bmshost,
            user="emsrouser",
            password="emsrouser@151",
            database='bmsmgmtprodv13',
            port=3306
            )
            
            awsdb = mysql.connector.connect(
                host=awshost,
                user="emsroot",
                password="22@teneT",
                database='EMS',
                port=3307
                )
    
            bmscur = bmsdb.cursor()
            awscur = awsdb.cursor()
        except Exception as ex:
            print(ex)
            return {"Error":"mysql connector"}

        bmscur.execute('select totalApparentPower2,date(polledTime),polledTime from bmsmgmt_olap_prod_v13.hvacSchneider7230Polling WHERE date(polledTime) = CURDATE()')

        res = bmscur.fetchall()

        max_jump = {}

        for i in range(1,len(res)):
            if res[i][0] != None and res[i-1][0] != None:
                max_jump[res[i][2]] = (abs(res[i][0]-res[i-1][0]))

        polledDate = res[0][1]

        def findTime(dictionary, value):
            for key, val in dictionary.items():
                if val == value:
                    return key
            return None

        jumpLi = list(max_jump.values())

        maxVal = max(jumpLi)

        
        polledTime = findTime(max_jump,maxVal)

        sql = "INSERT INTO EMS.PeakMaxJump(peakJump,peakTime,polledDate) VALUES(%s,%s,%s)"
        val = (maxVal,polledTime,polledDate)

        try:
            awscur.execute(sql,val)
            awsdb.commit()
            print("Max Peak Jump inserted")
        except mysql.connector.IntegrityError:
            sql = "UPDATE EMS.PeakMaxJump SET peakJump = %s, peakTime = %s WHERE polledDate = %s"
            awscur.execute(sql,val)
            awsdb.commit()
            print("Max Peak Jump updated")

        data = {"message":"Max Peak Jump"}
        return jsonify(data), 200
    else:
        return jsonify({'error': 'Unauthorized'}), 401

@app.route('/ablktf', methods = ['GET'])
def ablktf():
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        try:
            bmsdb = mysql.connector.connect(
                host=bmshost,
                user="emsrouser",
                password="emsrouser@151",
                database='bmsmgmtprodv13',
                port=3306
            )
            awsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                ) 
            bmscur = bmsdb.cursor()
            awscur = awsdb.cursor()
        except:
            return {"error":"mysql connection"}
        
        atf1140 = {}
        atf1149 = {}
        atf1150 = {}

        ablktf = defaultdict(float)


        def Ablktf(polledTime,Energy,id):
            polledTime = str(polledTime)[0:14]+"00:00"
            
            if id == 1140:
                if polledTime in atf1140.keys():
                    atf1140[polledTime].append(Energy)
                else:
                    atf1140[polledTime] = [Energy]
        
            
            if id == 1149:
                if polledTime in atf1149.keys():
                    atf1149[polledTime].append(Energy)
                else:
                    atf1149[polledTime] = [Energy]
        
            if id == 1150:
                if polledTime in atf1150.keys():
                    atf1150[polledTime].append(Energy)
                else:
                    atf1150[polledTime] = [Energy]

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1140 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                Ablktf(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])
            

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1149 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                Ablktf(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1150 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                Ablktf(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])


        for i in atf1140.keys():
            ablktf[i] += sum(atf1140[i])
        
        for i in atf1149.keys():
            ablktf[i] += sum(atf1149[i])
        
        for i in atf1150.keys():
            ablktf[i] += sum(atf1150[i])
        
        for i in ablktf.keys():
            val = (ablktf[i],i)
            sql = "INSERT INTO EMS.OthersConsumption(ablocktf,polledTime) VALUES(%s,%s)"
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("A-block terrace inserted")
            except mysql.connector.IntegrityError:
                sql = "UPDATE EMS.OthersConsumption SET ablocktf = %s where polledTime = %s"
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("A-block terrace updated") 
        
        data = {"message":"A-block terrace"}
        return jsonify(data), 200
    else:
        return jsonify({'error': 'Unauthorized'}), 401


@app.route('/bblktf', methods = ['GET'])
def bblktf():
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        try:
            bmsdb = mysql.connector.connect(
                host=bmshost,
                user="emsrouser",
                password="emsrouser@151",
                database='bmsmgmtprodv13',
                port=3306
            )
            awsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                ) 
            bmscur = bmsdb.cursor()
            awscur = awsdb.cursor()
        except:
            return {"error":"mysql connection"}
        
        btf1154 = {}
        btf1155 = {}
        btf1156 = {}

        bblktf = defaultdict(float)


        def  Bblktf(polledTime,Energy,id):
            polledTime = str(polledTime)[0:14]+"00:00"
            
            if id == 1154:
                if polledTime in btf1154.keys():
                    btf1154[polledTime].append(Energy)
                else:
                    btf1154[polledTime] = [Energy]
        
            
            if id == 1155:
                if polledTime in btf1155.keys():
                    btf1155[polledTime].append(Energy)
                else:
                    btf1155[polledTime] = [Energy]
        
            if id == 1156:
                if polledTime in btf1156.keys():
                    btf1156[polledTime].append(Energy)
                else:
                    btf1156[polledTime] = [Energy]

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1154 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                Bblktf(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])
            

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1155 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                Bblktf(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1156 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                Bblktf(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])


        for i in btf1154.keys():
            bblktf[i] += sum(btf1154[i])
        
        for i in btf1155.keys():
            bblktf[i] += sum(btf1155[i])
        
        for i in btf1156.keys():
            bblktf[i] += sum(btf1156[i])
        
        for i in bblktf.keys():
            val = (bblktf[i],i)
            sql = "INSERT INTO EMS.OthersConsumption(bblocktf,polledTime) VALUES(%s,%s)"
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("B-block terrace inserted")
            except mysql.connector.IntegrityError:
                sql = "UPDATE EMS.OthersConsumption SET bblocktf = %s where polledTime = %s"
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("B-block terrace updated") 
        
        data = {"message":"B-block terrace"}
        return jsonify(data), 200
    else:
        return jsonify({'error': 'Unauthorized'}), 401

@app.route('/cblktf', methods = ['GET'])
def cblktf():
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        try:
            bmsdb = mysql.connector.connect(
                host=bmshost,
                user="emsrouser",
                password="emsrouser@151",
                database='bmsmgmtprodv13',
                port=3306
            )
            awsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                ) 
            bmscur = bmsdb.cursor()
            awscur = awsdb.cursor()
        except:
            return {"error":"mysql connection"}
        
        ctf1217 = {}
        ctf1218 = {}

        cblktf = defaultdict(float)


        def  Cblktf(polledTime,Energy,id):
            polledTime = str(polledTime)[0:14]+"00:00"
            
            if id == 1217:
                if polledTime in ctf1217.keys():
                    ctf1217[polledTime].append(Energy)
                else:
                    ctf1217[polledTime] = [Energy]
        
            
            if id == 1218:
                if polledTime in ctf1218.keys():
                    ctf1218[polledTime].append(Energy)
                else:
                    ctf1218[polledTime] = [Energy]
        

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1218 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                Cblktf(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])
            

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1217 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                Cblktf(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])

        
        for i in ctf1218.keys():
            cblktf[i] += sum(ctf1218[i])
        
        for i in ctf1217.keys():
            cblktf[i] += sum(ctf1217[i])
        
        for i in cblktf.keys():
            val = (cblktf[i],i)
            sql = "INSERT INTO EMS.OthersConsumption(cblocktf,polledTime) VALUES(%s,%s)"
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("C-block terrace inserted")
            except mysql.connector.IntegrityError:
                sql = "UPDATE EMS.OthersConsumption SET cblocktf = %s where polledTime = %s"
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("C block terrace updated") 
        
        data = {"message":"C-block terrace"}
        return jsonify(data), 200
    else:
        return jsonify({'error': 'Unauthorized'}), 401

@app.route('/dblk3f', methods = ['GET'])
def dblk3f():
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        try:
            bmsdb = mysql.connector.connect(
                host=bmshost,
                user="emsrouser",
                password="emsrouser@151",
                database='bmsmgmtprodv13',
                port=3306
            )
            awsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                ) 
            bmscur = bmsdb.cursor()
            awscur = awsdb.cursor()
        except:
            return {"error":"mysql connection"}
        
        d3f1021 = {}

        dblk3f = defaultdict(float)


        def Dblk7f(polledTime,Energy,id):
            polledTime = str(polledTime)[0:14]+"00:00"
            
            if id == 1021:
                if polledTime in d3f1021.keys():
                    d3f1021[polledTime].append(Energy)
                else:
                    d3f1021[polledTime] = [Energy]
        

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1021 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                Dblk7f(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])

        
        for i in d3f1021.keys():
            dblk3f[i] += sum(d3f1021[i])
        
        
        for i in dblk3f.keys():
            val = (dblk3f[i],i)
            sql = "INSERT INTO EMS.OthersConsumption(dblock3f,polledTime) VALUES(%s,%s)"
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("D-block 3rd floor inserted")
            except mysql.connector.IntegrityError:
                sql = "UPDATE EMS.OthersConsumption SET dblock3f = %s where polledTime = %s"
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("D block 3rd floor updated") 
        
        data = {"message":"D-block 3rd floor"}
        return jsonify(data), 200
    else:
        return jsonify({'error': 'Unauthorized'}), 401


@app.route('/dblk7f', methods = ['GET'])
def dblk7f():
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        try:
            bmsdb = mysql.connector.connect(
                host=bmshost,
                user="emsrouser",
                password="emsrouser@151",
                database='bmsmgmtprodv13',
                port=3306
            )
            awsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                ) 
            bmscur = bmsdb.cursor()
            awscur = awsdb.cursor()
        except:
            return {"error":"mysql connection"}
        
        d7f1121 = {}
        d7f1122 = {}
        d7f1123 = {}
        d7f1124 = {}
        d7f1125 = {}
        d7f1126 = {}
        d7f1127 = {}
        d7f1128 = {}
        d7f1151 = {}
        d7f1152 = {}
        d7f1153 = {}

        dblk7f = defaultdict(float)


        def Dblk7f(polledTime,Energy,id):
            polledTime = str(polledTime)[0:14]+"00:00"
            
            if id == 1121:
                if polledTime in d7f1121.keys():
                    d7f1121[polledTime].append(Energy)
                else:
                    d7f1121[polledTime] = [Energy]
            
            if id == 1122:
                if polledTime in d7f1122.keys():
                    d7f1122[polledTime].append(Energy)
                else:
                    d7f1122[polledTime] = [Energy]
            
            if id == 1123:
                if polledTime in d7f1123.keys():
                    d7f1123[polledTime].append(Energy)
                else:
                    d7f1123[polledTime] = [Energy]
            
            if id == 1124:
                if polledTime in d7f1124.keys():
                    d7f1124[polledTime].append(Energy)
                else:
                    d7f1124[polledTime] = [Energy]
                
            if id == 1125:
                if polledTime in d7f1125.keys():
                    d7f1125[polledTime].append(Energy)
                else:
                    d7f1125[polledTime] = [Energy]
            
            if id == 1126:
                if polledTime in d7f1126.keys():
                    d7f1126[polledTime].append(Energy)
                else:
                    d7f1126[polledTime] = [Energy]
            
            if id == 1127:
                if polledTime in d7f1127.keys():
                    d7f1127[polledTime].append(Energy)
                else:
                    d7f1127[polledTime] = [Energy]

            if id == 1128:
                if polledTime in d7f1128.keys():
                    d7f1128[polledTime].append(Energy)
                else:
                    d7f1128[polledTime] = [Energy]
            
            if id == 1151:
                if polledTime in d7f1151.keys():
                    d7f1151[polledTime].append(Energy)
                else:
                    d7f1151[polledTime] = [Energy]
            
            if id == 1152:
                if polledTime in d7f1152.keys():
                    d7f1152[polledTime].append(Energy)
                else:
                    d7f1152[polledTime] = [Energy]
            
            if id == 1153:
                if polledTime in d7f1153.keys():
                    d7f1153[polledTime].append(Energy)
                else:
                    d7f1153[polledTime] = [Energy]        

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1121 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                Dblk7f(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1122 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                Dblk7f(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])
            
        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1123 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                Dblk7f(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])
        

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1124 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                Dblk7f(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1125 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                Dblk7f(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1126 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                Dblk7f(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1127 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                Dblk7f(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])
        

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1128 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                Dblk7f(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1151 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                Dblk7f(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1152 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                Dblk7f(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])
        

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1153 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                Dblk7f(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])

        for i in d7f1121.keys():
            dblk7f[i] += sum(d7f1121[i])
        
        for i in d7f1122.keys():
            dblk7f[i] += sum(d7f1122[i])

        for i in d7f1123.keys():
            dblk7f[i] += sum(d7f1123[i])

        for i in d7f1124.keys():
            dblk7f[i] += sum(d7f1124[i])
        
        for i in d7f1125.keys():
            dblk7f[i] += sum(d7f1125[i])
        
        for i in d7f1126.keys():
            dblk7f[i] += sum(d7f1126[i])
        
        for i in d7f1127.keys():
            dblk7f[i] += sum(d7f1127[i])
        
        for i in d7f1128.keys():
            dblk7f[i] += sum(d7f1128[i])
        
        for i in d7f1151.keys():
            dblk7f[i] += sum(d7f1151[i])
        
        for i in d7f1152.keys():
            dblk7f[i] += sum(d7f1152[i])
        
        for i in d7f1153.keys():
            dblk7f[i] += sum(d7f1153[i])

        for i in dblk7f.keys():
            val = (dblk7f[i],i)
            sql = "INSERT INTO EMS.OthersConsumption(dblock7f,polledTime) VALUES(%s,%s)"
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("D-block 7th floor inserted")
            except mysql.connector.IntegrityError:
                sql = "UPDATE EMS.OthersConsumption SET dblock7f = %s where polledTime = %s"
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("D block 7th floor updated") 
        
        data = {"message":"D-block 7th floor"}
        return jsonify(data), 200
    else:
        return jsonify({'error': 'Unauthorized'}), 401


@app.route('/mlcpgf', methods = ['GET'])
def mlcpgf():
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        try:
            bmsdb = mysql.connector.connect(
                host=bmshost,
                user="emsrouser",
                password="emsrouser@151",
                database='bmsmgmtprodv13',
                port=3306
            )
            awsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                ) 
            bmscur = bmsdb.cursor()
            awscur = awsdb.cursor()
        except:
            return {"error":"mysql connection"}
        
        mlcp348 = {}
        mlcp351 = {}
        mlcp353 = {}
        mlcp354 = {}
        mlcp356 = {}
        mlcp360 = {}
        mlcp1145 = {}
        mlcp1148 = {}

        mlcpgf = defaultdict(float)


        def MlcpGf(polledTime,Energy,id):
            polledTime = str(polledTime)[0:14]+"00:00"
            
            if id == 348:
                if polledTime in mlcp348.keys():
                    mlcp348[polledTime].append(Energy)
                else:
                    mlcp348[polledTime] = [Energy]
            
            if id == 351:
                if polledTime in mlcp351.keys():
                    mlcp351[polledTime].append(Energy)
                else:
                    mlcp351[polledTime] = [Energy]
            
            if id == 353:
                if polledTime in mlcp353.keys():
                    mlcp353[polledTime].append(Energy)
                else:
                    mlcp353[polledTime] = [Energy]
            
            if id == 354:
                if polledTime in mlcp354.keys():
                    mlcp354[polledTime].append(Energy)
                else:
                    mlcp354[polledTime] = [Energy]
            
            if id == 356:
                if polledTime in mlcp356.keys():
                    mlcp356[polledTime].append(Energy)
                else:
                    mlcp356[polledTime] = [Energy]
            
            if id == 360:
                if polledTime in mlcp360.keys():
                    mlcp360[polledTime].append(Energy)
                else:
                    mlcp360[polledTime] = [Energy]
            
            if id == 1145:
                if polledTime in mlcp1145.keys():
                    mlcp1145[polledTime].append(Energy)
                else:
                    mlcp1145[polledTime] = [Energy]
            
            if id == 1148:
                if polledTime in mlcp1148.keys():
                    mlcp1148[polledTime].append(Energy)
                else:
                    mlcp1148[polledTime] = [Energy]

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 348 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                MlcpGf(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 351 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                MlcpGf(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])
        
        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 353 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                MlcpGf(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])
        
        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 354 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                MlcpGf(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])
        
        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 356 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                MlcpGf(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])

        
        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 360 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                MlcpGf(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1145 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                MlcpGf(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1148 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                MlcpGf(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])
        

        for i in mlcp348.keys():
            mlcpgf[i] += sum(mlcp348[i])
        
        for i in mlcp351.keys():
            mlcpgf[i]= sum(mlcp351[i])
        
        for i in mlcp353.keys():
            mlcpgf[i] += sum(mlcp353[i])
        
        for i in mlcp354.keys():
            mlcpgf[i] += sum(mlcp354[i])
        
        for i in mlcp356.keys():
            mlcpgf[i] += sum(mlcp356[i])
        
        for i in mlcp360.keys():
            mlcpgf[i] += sum(mlcp360[i])
        
        for i in mlcp1145.keys():
            mlcpgf[i] += sum(mlcp1145[i])

        for i in mlcp1148.keys():
            mlcpgf[i] += sum(mlcp1148[i])

        for i in mlcpgf.keys():
            val = (mlcpgf[i],i)
            sql = "INSERT INTO EMS.OthersConsumption(mlcpgf,polledTime) VALUES(%s,%s)"
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("MLCP ground floor inserted")
            except mysql.connector.IntegrityError:
                sql = "UPDATE EMS.OthersConsumption SET mlcpgf = %s where polledTime = %s"
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("MLCP ground floor updated") 
        
        data = {"message":"Eblock 0th & 9th floor"}
        return jsonify(data), 200
    else:
        return jsonify({'error': 'Unauthorized'}), 401

@app.route('/eblockgf9f', methods = ['GET'])
def eblockgf9f():
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        try:
            bmsdb = mysql.connector.connect(
                host=bmshost,
                user="emsrouser",
                password="emsrouser@151",
                database='bmsmgmtprodv13',
                port=3306
            )
            awsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                ) 
            bmscur = bmsdb.cursor()
            awscur = awsdb.cursor()
        except:
            return {"error":"mysql connection"}

        eblk1180 = {}
        eblk1207 = {}

        elblock9f = defaultdict(float)
        elblockgf = defaultdict(float)

        def Eblock9f(polledTime,Energy,id):
            polledTime = str(polledTime)[0:14]+"00:00"
            
            if id == 1180:
                if polledTime in eblk1180.keys():
                    eblk1180[polledTime].append(Energy)
                else:
                    eblk1180[polledTime] = [Energy]
            
            if id == 1207:
                if polledTime in eblk1207.keys():
                    eblk1207[polledTime].append(Energy)
                else:
                    eblk1207[polledTime] = [Energy]

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1180 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                Eblock9f(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])
        

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1207 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                Eblock9f(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])

        for i in eblk1180.keys():
            elblock9f[i] += sum(eblk1180[i])
        
        for i in eblk1207.keys():
            elblockgf[i] += sum(eblk1207[i])
        

        for i in elblock9f.keys():
            val = (elblock9f[i],i)
            sql = "INSERT INTO EMS.OthersConsumption(eblockf9,polledTime) VALUES(%s,%s)"
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("E block 9th floor inserted")
            except mysql.connector.IntegrityError:
                sql = "UPDATE EMS.OthersConsumption SET eblockf9 = %s where polledTime = %s"
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("E block 9th floor updated") 

        for i in elblockgf.keys():
            val = (elblockgf[i],i)
            sql = "INSERT INTO EMS.OthersConsumption(eblockgf,polledTime) VALUES(%s,%s)"
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("E block 0th floor inserted")
            except mysql.connector.IntegrityError:
                sql = "UPDATE EMS.OthersConsumption SET eblockgf = %s where polledTime = %s"
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("E block 0th floor updated")

        data = {"message":"Eblock 0th & 9th floor"}
        return jsonify(data), 200
    else:
        return jsonify({'error': 'Unauthorized'}), 401


@app.route('/eblock1f', methods = ['GET'])
def eblock1f():
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        try:
            bmsdb = mysql.connector.connect(
                host=bmshost,
                user="emsrouser",
                password="emsrouser@151",
                database='bmsmgmtprodv13',
                port=3306
            )
            awsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                ) 
            bmscur = bmsdb.cursor()
            awscur = awsdb.cursor()
        except:
            return {"error":"mysql connection"}
        
        eblk1133 = {}
        eblk1136 = {}
        eblk1138 = {}
        eblk1142 = {}
        eblk1162 = {}
        eblk1164 = {}
        eblk1166 = {}
        eblk1170 = {}

        elblock = defaultdict(float)

        def Eblock1f(polledTime,Energy,id):
            polledTime = str(polledTime)[0:14]+"00:00"
            
            if id == 1133:
                if polledTime in eblk1133.keys():
                    eblk1133[polledTime].append(Energy)
                else:
                    eblk1133[polledTime] = [Energy]
            
            if id == 1136:
                if polledTime in eblk1136.keys():
                    eblk1136[polledTime].append(Energy)
                else:
                    eblk1136[polledTime] = [Energy]
            
            if id == 1138:
                if polledTime in eblk1138.keys():
                    eblk1138[polledTime].append(Energy)
                else:
                    eblk1138[polledTime] = [Energy]
            
            if id == 1142:
                if polledTime in eblk1142.keys():
                    eblk1142[polledTime].append(Energy)
                else:
                    eblk1142[polledTime] = [Energy]

            if id == 1162:
                if polledTime in eblk1162.keys():
                    eblk1162[polledTime].append(Energy)
                else:
                    eblk1162[polledTime] = [Energy]
            
            if id == 1164:
                if polledTime in eblk1164.keys():
                    eblk1164[polledTime].append(Energy)
                else:
                    eblk1164[polledTime] = [Energy]
            
            if id == 1166:
                if polledTime in eblk1166.keys():
                    eblk1166[polledTime].append(Energy)
                else:
                    eblk1166[polledTime] = [Energy]
            
            if id == 1170:
                if polledTime in eblk1170.keys():
                    eblk1170[polledTime].append(Energy)
                else:
                    eblk1170[polledTime] = [Energy]


        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1133 and date(acmeterpolledtimestamp) = curdate();")
        
        e36res = bmscur.fetchall()

        for i in range(1,len(e36res)):
            if e36res[i][1] != None and e36res[i-1][1] != None:
                Eblock1f(e36res[i][0],e36res[i][1]-e36res[i-1][1],e36res[i][2])

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1136 and date(acmeterpolledtimestamp) = curdate();")
        
        e66res = bmscur.fetchall()

        for i in range(1,len(e66res)):
            if e66res[i][1] != None and e66res[i-1][1] != None:
                Eblock1f(e66res[i][0],e66res[i][1]-e66res[i-1][1],e66res[i][2])
        
        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1138 and date(acmeterpolledtimestamp) = curdate();")
        
        e66res = bmscur.fetchall()

        for i in range(1,len(e66res)):
            if e66res[i][1] != None and e66res[i-1][1] != None:
                Eblock1f(e66res[i][0],e66res[i][1]-e66res[i-1][1],e66res[i][2])
        
        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1142 and date(acmeterpolledtimestamp) = curdate();")
        
        e66res = bmscur.fetchall()

        for i in range(1,len(e66res)):
            if e66res[i][1] != None and e66res[i-1][1] != None:
                Eblock1f(e66res[i][0],e66res[i][1]-e66res[i-1][1],e66res[i][2])

        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1162 and date(acmeterpolledtimestamp) = curdate();")
        
        e66res = bmscur.fetchall()

        for i in range(1,len(e66res)):
            if e66res[i][1] != None and e66res[i-1][1] != None:
                Eblock1f(e66res[i][0],e66res[i][1]-e66res[i-1][1],e66res[i][2])
        
        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1164 and date(acmeterpolledtimestamp) = curdate();")
        
        e66res = bmscur.fetchall()

        for i in range(1,len(e66res)):
            if e66res[i][1] != None and e66res[i-1][1] != None:
                Eblock1f(e66res[i][0],e66res[i][1]-e66res[i-1][1],e66res[i][2])
        
        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1166 and date(acmeterpolledtimestamp) = curdate();")
        
        e66res = bmscur.fetchall()

        for i in range(1,len(e66res)):
            if e66res[i][1] != None and e66res[i-1][1] != None:
                Eblock1f(e66res[i][0],e66res[i][1]-e66res[i-1][1],e66res[i][2])
        
        bmscur.execute("SELECT acmeterpolledtimestamp,acmeterenergy,acmetersubsystemid FROM bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1170 and date(acmeterpolledtimestamp) = curdate();")
        
        e66res = bmscur.fetchall()

        for i in range(1,len(e66res)):
            if e66res[i][1] != None and e66res[i-1][1] != None:
                Eblock1f(e66res[i][0],e66res[i][1]-e66res[i-1][1],e66res[i][2])


        for i in eblk1133.keys():
            elblock[i] += sum(eblk1133[i])
        
        for i in eblk1136.keys():
            elblock[i] += sum(eblk1136[i])
        
        for i in eblk1138.keys():
            elblock[i] += sum(eblk1138[i])
        
        for i in eblk1142.keys():
            elblock[i] += sum(eblk1142[i])
        
        for i in eblk1162.keys():
            elblock[i] += sum(eblk1162[i])
        
        for i in eblk1164.keys():
            elblock[i] += sum(eblk1164[i])
        
        for i in eblk1166.keys():
            elblock[i] += sum(eblk1166[i])

        for i in eblk1170.keys():
            elblock[i] += sum(eblk1170[i])



        for i in elblock.keys():
            val = (elblock[i],i)
            sql = "INSERT INTO EMS.OthersConsumption(eblock1f,polledTime) VALUES(%s,%s)"
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("E block 1st floor inserted")
            except mysql.connector.IntegrityError:
                sql = "UPDATE EMS.OthersConsumption SET eblock1f = %s where polledTime = %s"
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("E block 1st floor updated")
        
        data = {"message":"Eblock 1st floor"}
        return jsonify(data), 200
    else:
        return jsonify({'error': 'Unauthorized'}), 401 

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


@app.route('/inverterhourph2', methods = ['GET'])
def inverterHourph2():
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

            awsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                )
            
            bmscur = unprocesseddb.cursor()
            awscur = awsdb.cursor()
        except:
            return {"error":"mysql connection"}
        
        inverthr9 = {}
        inverthr10 = {}
        inverthr11 = {}
        inverthr12 = {}
        inverthr13 = {}
        inverthr14 = {}


        def HourSummmarize(inverthr, inverter_id, active_power, timestamp):
            hour = str(timestamp)[0:13] + ":00:00"

            if hour in inverthr.keys():
                inverthr[hour].append(active_power)
            else:
                inverthr[hour] = [active_power]

        bmscur.execute("""SELECT ctsedinveterdeviceid,createdTime,ctsedinveteractivepower FROM bmsmgmtprodv13.ctsedinveterreadings where ctsedinveterdeviceid IN ('9', '10', '11', '12', '13', '14') and  date(createdTime) = curdate();""")

        result = bmscur.fetchall()


        for row in result:
            inverter_id = row[0]
            timestamp = row[1]
            active_pow = row[2]

            if inverter_id == 9:
                HourSummmarize(inverthr9, inverter_id, active_pow, timestamp)
            elif inverter_id == 10:
                HourSummmarize(inverthr10, inverter_id, active_pow, timestamp)
            elif inverter_id == 11:
                HourSummmarize(inverthr11, inverter_id, active_pow, timestamp)
            elif inverter_id == 12:
                HourSummmarize(inverthr12, inverter_id, active_pow, timestamp)
            elif inverter_id == 13:
                HourSummmarize(inverthr13, inverter_id, active_pow, timestamp)
            elif inverter_id == 14:
                HourSummmarize(inverthr14, inverter_id, active_pow, timestamp)



        for hour, values in inverthr9.items():
            total_power = round(sum(values), 2)

            sql = "INSERT INTO EMS.inverterHourlyph2(inverter9,polledTime) values(%s,%s)"
            val = (total_power,hour)
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter9 data inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE EMS.inverterHourlyph2 SET inverter9 = %s WHERE polledTime = %s"
                val = (total_power,hour)
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter9 data updated") 


        for hour, values in inverthr10.items():
            total_power = round(sum(values), 2)
            
            sql = "INSERT INTO EMS.inverterHourlyph2(inverter10,polledTime) values(%s,%s)"
            val = (total_power,hour)
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter10 data inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE EMS.inverterHourlyph2 SET inverter10 = %s WHERE polledTime = %s"
                val = (total_power,hour)
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter10 data updated")


        for hour, values in inverthr11.items():
            total_power = round(sum(values), 2)
            
            sql = "INSERT INTO EMS.inverterHourlyph2(inverter11,polledTime) values(%s,%s)"
            val = (total_power,hour)
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter11 data inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE EMS.inverterHourlyph2 SET inverter11 = %s WHERE polledTime = %s"
                val = (total_power,hour)
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter11 data updated") 


        for hour, values in inverthr12.items():
            total_power = round(sum(values), 2)
            
            sql = "INSERT INTO EMS.inverterHourlyph2(inverter12,polledTime) values(%s,%s)"
            val = (total_power,hour)
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter12 data inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE EMS.inverterHourlyph2 SET inverter12 = %s WHERE polledTime = %s"
                val = (total_power,hour)
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter12 data updated")


        for hour, values in inverthr13.items():
            total_power = round(sum(values), 2)
            
            sql = "INSERT INTO EMS.inverterHourlyph2(inverter13,polledTime) values(%s,%s)"
            val = (total_power,hour)
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter13 data inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE EMS.inverterHourlyph2 SET inverter13 = %s WHERE polledTime = %s"
                val = (total_power,hour)
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter13 data updated")


        for hour, values in inverthr14.items():
            total_power = round(sum(values), 2)
            
            sql = "INSERT INTO EMS.inverterHourlyph2(inverter14,polledTime) values(%s,%s)"
            val = (total_power,hour)
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter14 data inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE EMS.inverterHourlyph2 SET inverter14 = %s WHERE polledTime = %s"
                val = (total_power,hour)
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Inverter14 data updated")

        awscur.close()
        awsdb.close()
        bmscur.close()
        unprocesseddb.close()

        data = {"message":"Inverter hour updated"}
        return jsonify(data), 200
    else:
        return jsonify({'error': 'Unauthorized'}), 401 


@app.route('/inverterhour', methods = ['GET'])
def inverterHour():
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

            awsdb = mysql.connector.connect(
                    host=awshost,
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                )

            awscur = awsdb.cursor()
            bmscur = unprocesseddb.cursor()
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

        bmscur.execute("""SELECT ctsedinveterdeviceid,createdTime,ctsedinveteractivepower FROM bmsmgmtprodv13.ctsedinveterreadings where ctsedinveterdeviceid IN ('1', '2', '3', '4', '5', '6', '7', '8')and  date(createdTime) = curdate();""")

        result = bmscur.fetchall()


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
        unprocesseddb.close()
        bmscur.close()

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
            if chglist[i][1]-chglist[i-1][1] > 0 and chglist[i][1]-chglist[i-1][1] <= 1000:
                summarizeHourChg(chglist[i][0],(chglist[i][1]-chglist[i-1][1])/1000)

        for i in range(1,len(dchglist)):
            if dchglist[i][1]-dchglist[i-1][1] > 0 and dchglist[i][1]-dchglist[i-1][1] <= 1000:
                summarizeHourDchg(dchglist[i][0],(dchglist[i][1]-dchglist[i-1][1])/1000)
        
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


@app.route('/wheeledhourlyph2', methods = ['GET'])
def wheeledHourlyph2():
    token = request.headers.get('Authorization')
    print(token)
    
    if token and check_authentication(token):

        bmsdb = mysql.connector.connect(
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

        emscur = emsdb.cursor()
        bmscur = bmsdb.cursor()

        radhr = {}
        hourly_wheeled= {}

        def Hourlywheeled(polledTime,Energy):
            hr = int(str(polledTime)[11:13])
            if hr >= 7 and hr <= 18:
                hour = str(polledTime)[0:13] + ":00:00"
                if hour in hourly_wheeled.keys():
                    if Energy >=0 and Energy <=100:
                        hourly_wheeled[hour] += Energy
                else:
                    if Energy>=0 and Energy <=100:
                        hourly_wheeled[hour] = Energy
            else:
                hour = str(polledTime)[0:13] + ":00:00"
                hourly_wheeled[hour] = 0
        
        emscur.execute("SELECT metertimestamp,meterenergy FROM EMS.EMSMeterData where date(metertimestamp) = curdate() and meterdeviceid = 2;")

        wheeled_res = emscur.fetchall()

        for i in range(1,len(wheeled_res)):
            Hourlywheeled(wheeled_res[i][0],(wheeled_res[i][1]-wheeled_res[i-1][1])*1000)

        for i in hourly_wheeled.keys():
            sql = "INSERT INTO WheeledHourlyph2(polledTime,Energy) VALUES(%s,%s)"
            val = (i,hourly_wheeled[i])
            print(val)
            try:
                emscur.execute(sql,val)
                emsdb.commit()
                print("Wheeled hourly phase 2 inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE WheeledHourlyph2 SET Energy = %s WHERE polledTime = %s"
                val = (hourly_wheeled[i],i)
                emscur.execute(sql,val)
                emsdb.commit()
                print("Wheeled hourly phase 2 update")

        def HourSummmarize(irrad,Time):
            hour=str(Time)[0:13]+":00:00"
            if hour in radhr.keys():
                radhr[hour].append(irrad)
            else:
                radhr[hour] = [irrad]

        bmscur.execute("""SELECT createdTime,CAST(ctsedwmsirradiation AS DECIMAL(18, 2)) FROM bmsmgmtprodv13.ctsedwmsreadings where date(createdTime) = curdate();""")
        result = bmscur.fetchall()          

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

                constant2 = Decimal('9447')
                constant3 = Decimal('0.209')

                def calculate_x():
                    month_multiplier = {
                                    'january': 0.851,
                                    'february': 0.836,
                                    'march': 0.816,
                                    'april': 0.818,
                                    'may': 0.831,
                                    'june': 0.795,
                                    'july': 0.836,
                                    'august': 0.808,
                                    'september': 0.816,
                                    'october': 0.805,
                                    'november': 0.851,
                                    'december': 0.861,
                                    }

                                                            
                    current_month = datetime.now().strftime('%B').lower()

                                                                        
                    if current_month in month_multiplier:
                
                        return Decimal(str(month_multiplier[current_month]))
                    else:
                        return Decimal('0')  

                x_multiplier = calculate_x()
                # print('multi',x_multiplier)

                expectedwmms = wmsirradiationkwh*constant2*constant3*Decimal(x_multiplier)
                expectedwmms = round(expectedwmms, 2)
                # print('expwmms',expectedwmms)

                val = (wmsirradiationkwh,expectedwmms,i)
                sql = "INSERT INTO EMS.WheeledHourlyph2(irradiation,expectedEnergy,polledTime) values(%s,%s,%s)"
                try:
                    emscur.execute(sql,val)
                    emsdb.commit()
                    print(val)
                    print("Wheeled in irradiation inserted")
                except mysql.connector.errors.IntegrityError:
                    sql = "update EMS.WheeledHourlyph2 set irradiation = %s, expectedEnergy = %s where polledTime = %s"
                    val = (wmsirradiationkwh,expectedwmms,i)
                    print(val)
                    emscur.execute(sql,val)
                    emsdb.commit()
                    print("Wheeled in irradiation updated")

        emscur.close()
        bmscur.close()
        
        data = {"message":"WHEELED HOURLY"}
        return jsonify(data), 200
    else:
        return jsonify({'error': 'Unauthorized'}), 401


@app.route('/wheeledhourlyph1', methods = ['GET'])
def wheeledHourlyph1():
    token = request.headers.get('Authorization')
    print(token)
    
    if token and check_authentication(token):
        radhr = {}
        hourly_wheeled= {}

        bmsdb = mysql.connector.connect(
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

        emscur = emsdb.cursor()
        bmscur = bmsdb.cursor()

        def HourSeg(polledTime,Energy):
            hr = int(str(polledTime)[11:13])
            if hr >= 7 and hr <= 18:
                hour = str(polledTime)[0:13] + ":00:00"
                if hour in hourly_wheeled.keys():
                    if Energy >=0 and Energy <=100:
                        hourly_wheeled[hour] += Energy
                else:
                    if Energy>=0 and Energy <=100:
                        hourly_wheeled[hour] = Energy
            else:
                hour = str(polledTime)[0:13] + ":00:00"
                hourly_wheeled[hour] = 0

        bmscur.execute("SELECT createdTime,ctsedogenergy FROM bmsmgmtprodv13.ctsedogreadings where date(createdTime) = curdate() and ctsedogdeviceid =1;")

        res = bmscur.fetchall()

        for i in range(1,len(res)):
            if res[i][1] != None and res[i-1][1] != None:
                HourSeg(res[i][0],(res[i][1]-res[i-1][1])*1000)
            
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

        bmscur.execute("""SELECT createdTime,CAST(ctsedwmsirradiation AS DECIMAL(18, 2)) FROM bmsmgmtprodv13.ctsedwmsreadings where date(createdTime) = curdate();""")
        result = bmscur.fetchall()          

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

                expectedwmms = wmsirradiationkwh*constant2*constant3*Decimal(x_multiplier)
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
        
        proscur.execute("""SELECT date,hour,energy FROM bmsmgmt_olap_prod_v13.IITMRPHourwiseEnergyUsage where date(createdTime) >= date(curdate()-3) and name = 'IITMRP' and energyType='electric';""")

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

        processcur.execute("""select FLOOR(acmeterenergy),polledTime from bmsmgmt_olap_prod_v13.MVPPolling where mvpnum in ("MVP1","MVP2","MVP3","MVP4") and Date(polledTime) = curdate();""")

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
