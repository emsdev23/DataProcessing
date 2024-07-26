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


@app.route('/st1CurVol', methods = ['GET'])
def st1CurVol():
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        current1 = {}
        voltage1 = {}
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
            emsdb = mysql.connector.connect(
                host=emshost,
                user="emsroot",
                password="22@teneT",
                database='EMS',
                port=3306
            )
            emscur = emsdb.cursor()
            bmscur = bmsdb.cursor()
            awscur = awsdb.cursor()

            awscur.execute("SELECT * FROM EMS.ioecurvol where date(polledTime) = curdate() and st1_current >= 0 order by polledTime desc limit 1;")

            dateres = awscur.fetchall()

            if len(dateres) > 0:
                dated = str(dateres[0][0])[0:17]+"00"
                print("date",dated)
                query = f"SELECT recordTimestamp, batteryVoltage, batteryCurrent FROM EMS.ioeSt1BatteryData WHERE recordTimestamp >= '{dated}';"
            else:
                query = "SELECT recordTimestamp, batteryVoltage, batteryCurrent FROM EMS.ioeSt1BatteryData WHERE DATE(recordTimestamp) = CURDATE();"
            emscur.execute(query)
            result = emscur.fetchall()

            def CurrentSummarize(polledTime, voltage, currentValue):
                minute = str(polledTime)[0:17]+"00"

                if voltage is not None:
                    if minute not in current1:
                        current1[minute] = currentValue
                    else:
                        current1[minute] = max(current1[minute], currentValue)

                    if minute not in voltage1:
                        voltage1[minute] = voltage
                    else:
                        voltage1[minute] = max(voltage1[minute], voltage)         

            for row in result:                     
                polledTime = row[0]
                voltageValue = row[1]
                currentValue = row[2]

                CurrentSummarize(polledTime, voltageValue, currentValue)

            for minute in current1.keys():           
                current = current1[minute]
                voltage = voltage1[minute]        
                sql = """INSERT INTO ioecurvol(polledtime,st1_current, st1_voltage) VALUES(%s, %s, %s)"""
                val = (minute,current,voltage)
                try:
                        awscur.execute(sql, val)
                        print(f"New data inserted for st1 on {minute}")
                        awsdb.commit()
                except mysql.connector.IntegrityError:
                        sql = """UPDATE ioecurvol SET st1_current = %s , st1_voltage = %s WHERE polledtime = %s"""
                        val = (current,voltage,minute)
                        awscur.execute(sql, val)
                        print(val)
                        print("Existing record updated for st1")
                        awsdb.commit()
        except:
            return {"error":"mysql connection"}
        
        emscur.close()
        emsdb.close()
        bmscur.close()
        bmsdb.close()
        awscur.close()
        awsdb.close()

        data = {"message":"IOE cur/vol st1 Updated"}
        return jsonify(data), 200
    
    else:
        return jsonify({'error': 'Unauthorized'}), 401


@app.route('/st2CurVol', methods = ['GET'])
def st2CurVol():
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        current1 = {}
        voltage1 = {}
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
            emsdb = mysql.connector.connect(
                host=emshost,
                user="emsroot",
                password="22@teneT",
                database='EMS',
                port=3306
            )
            emscur = emsdb.cursor()
            bmscur = bmsdb.cursor()
            awscur = awsdb.cursor()

            awscur.execute("SELECT * FROM EMS.ioecurvol where date(polledTime) = curdate() and st2_current >= 0 order by polledTime desc limit 1;")

            dateres = awscur.fetchall()

            if len(dateres) > 0:
                dated = str(dateres[0][0])[0:17]+"00"
                print("date",dated)
                query = f"SELECT recordTimestamp, batteryVoltage, batteryCurrent FROM EMS.ioeSt2BatteryData WHERE recordTimestamp >= '{dated}';"
            else:
                query = "SELECT recordTimestamp, batteryVoltage, batteryCurrent FROM EMS.ioeSt2BatteryData WHERE DATE(recordTimestamp) = CURDATE();"
            emscur.execute(query)
            result = emscur.fetchall()

            def CurrentSummarize(polledTime, voltage, currentValue):
                minute = str(polledTime)[0:17]+"00"

                if voltage is not None:
                    if minute not in current1:
                        current1[minute] = currentValue
                    else:
                        current1[minute] = max(current1[minute], currentValue)

                    if minute not in voltage1:
                        voltage1[minute] = voltage
                    else:
                        voltage1[minute] = max(voltage1[minute], voltage)         

            for row in result:                     
                polledTime = row[0]
                voltageValue = row[1]
                currentValue = row[2]

                CurrentSummarize(polledTime, voltageValue, currentValue)

            for minute in current1.keys():           
                current = current1[minute]
                voltage = voltage1[minute]        
                sql = """INSERT INTO ioecurvol(polledtime,st2_current, st2_voltage) VALUES(%s, %s, %s)"""
                val = (minute,current,voltage)
                try:
                        awscur.execute(sql, val)
                        print(f"New data inserted for st2 on {minute}")
                        awsdb.commit()
                except mysql.connector.IntegrityError:
                        sql = """UPDATE ioecurvol SET st2_current = %s , st2_voltage = %s WHERE polledtime = %s"""
                        val = (current,voltage,minute)
                        awscur.execute(sql, val)
                        print(val)
                        print("Existing record updated for st2")
                        awsdb.commit()
        except:
            return {"error":"mysql connection"}
        
        emscur.close()
        emsdb.close()
        bmscur.close()
        bmsdb.close()
        awscur.close()
        awsdb.close()

        data = {"message":"IOE cur/vol st2 Updated"}
        return jsonify(data), 200
    
    else:
        return jsonify({'error': 'Unauthorized'}), 401



@app.route('/st3CurVol', methods = ['GET'])
def st3CurVol():
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        current1 = {}
        voltage1 = {}
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
            emsdb = mysql.connector.connect(
                host=emshost,
                user="emsroot",
                password="22@teneT",
                database='EMS',
                port=3306
            )
            emscur = emsdb.cursor()
            bmscur = bmsdb.cursor()
            awscur = awsdb.cursor()

            awscur.execute("SELECT * FROM EMS.ioecurvol where date(polledTime) = curdate() and st3_current >= 0 order by polledTime desc limit 1;")

            dateres = awscur.fetchall()

            if len(dateres) > 0:
                dated = str(dateres[0][0])[0:17]+"00"
                print("date",dated)
                query = f"SELECT recordTimestamp, batteryVoltage, batteryCurrent FROM EMS.ioeSt3BatteryData WHERE recordTimestamp >= '{dated}';"
            else:
                query = "SELECT recordTimestamp, batteryVoltage, batteryCurrent FROM EMS.ioeSt3BatteryData WHERE DATE(recordTimestamp) = CURDATE();"
            emscur.execute(query)
            result = emscur.fetchall()

            def CurrentSummarize(polledTime, voltage, currentValue):
                minute = str(polledTime)[0:17]+"00"

                if voltage is not None:
                    if minute not in current1:
                        current1[minute] = currentValue
                    else:
                        current1[minute] = max(current1[minute], currentValue)

                    if minute not in voltage1:
                        voltage1[minute] = voltage
                    else:
                        voltage1[minute] = max(voltage1[minute], voltage)         

            for row in result:                     
                polledTime = row[0]
                voltageValue = row[1]
                currentValue = row[2]

                CurrentSummarize(polledTime, voltageValue, currentValue)

            for minute in current1.keys():           
                current = current1[minute]
                voltage = voltage1[minute]        
                sql = """INSERT INTO ioecurvol(polledtime,st3_current, st3_voltage) VALUES(%s, %s, %s)"""
                val = (minute,current,voltage)
                try:
                        awscur.execute(sql, val)
                        print(f"New data inserted for st3 on {minute}")
                        awsdb.commit()
                except mysql.connector.IntegrityError:
                        sql = """UPDATE ioecurvol SET st3_current = %s , st3_voltage = %s WHERE polledtime = %s"""
                        val = (current,voltage,minute)
                        awscur.execute(sql, val)
                        print(val)
                        print("Existing record updated for st3")
                        awsdb.commit()
        except:
            return {"error":"mysql connection"}
        emscur.close()
        emsdb.close()
        bmscur.close()
        bmsdb.close()
        awscur.close()
        awsdb.close()

        data = {"message":"IOE cur/vol st3 Updated"}
        return jsonify(data), 200
    
    else:
        return jsonify({'error': 'Unauthorized'}), 401


@app.route('/st4CurVol', methods = ['GET'])
def st4CurVol():
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        current1 = {}
        voltage1 = {}
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
            emsdb = mysql.connector.connect(
                host=emshost,
                user="emsroot",
                password="22@teneT",
                database='EMS',
                port=3306
            )
            emscur = emsdb.cursor()
            bmscur = bmsdb.cursor()
            awscur = awsdb.cursor()

            awscur.execute("SELECT * FROM EMS.ioecurvol where date(polledTime) = curdate() and st4_current >= 0 order by polledTime desc limit 1;")

            dateres = awscur.fetchall()

            if len(dateres) > 0:
                dated = str(dateres[0][0])[0:17]+"00"
                print("date",dated)
                query = f"SELECT recordTimestamp, batteryVoltage, batteryCurrent FROM EMS.ioeSt4BatteryData WHERE recordTimestamp >= '{dated}';"
            else:
                query = "SELECT recordTimestamp, batteryVoltage, batteryCurrent FROM EMS.ioeSt4BatteryData WHERE DATE(recordTimestamp) = CURDATE();"
            emscur.execute(query)
            result = emscur.fetchall()

            def CurrentSummarize(polledTime, voltage, currentValue):
                minute = str(polledTime)[0:17]+"00"

                if voltage is not None:
                    if minute not in current1:
                        current1[minute] = currentValue
                    else:
                        current1[minute] = max(current1[minute], currentValue)

                    if minute not in voltage1:
                        voltage1[minute] = voltage
                    else:
                        voltage1[minute] = max(voltage1[minute], voltage)         

            for row in result:                     
                polledTime = row[0]
                voltageValue = row[1]
                currentValue = row[2]

                CurrentSummarize(polledTime, voltageValue, currentValue)

            for minute in current1.keys():           
                current = current1[minute]
                voltage = voltage1[minute]        
                sql = """INSERT INTO ioecurvol(polledtime,st4_current, st4_voltage) VALUES(%s, %s, %s)"""
                val = (minute,current,voltage)
                try:
                        awscur.execute(sql, val)
                        print(f"New data inserted for st4 on {minute}")
                        awsdb.commit()
                except mysql.connector.IntegrityError:
                        sql = """UPDATE ioecurvol SET st4_current = %s , st4_voltage = %s WHERE polledtime = %s"""
                        val = (current,voltage,minute)
                        awscur.execute(sql, val)
                        print(val)
                        print("Existing record updated for st4")
                        awsdb.commit()
        except:
            return {"error":"mysql connection"}
        
        emscur.close()
        emsdb.close()
        bmscur.close()
        bmsdb.close()
        awscur.close()
        awsdb.close()

        data = {"message":"IOE cur/vol st4 Updated"}
        return jsonify(data), 200
    
    else:
        return jsonify({'error': 'Unauthorized'}), 401


@app.route('/st5CurVol', methods = ['GET'])
def st5CurVol():
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        current1 = {}
        voltage1 = {}
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
            emsdb = mysql.connector.connect(
                host=emshost,
                user="emsroot",
                password="22@teneT",
                database='EMS',
                port=3306
            )
            emscur = emsdb.cursor()
            bmscur = bmsdb.cursor()
            awscur = awsdb.cursor()

            awscur.execute("SELECT * FROM EMS.ioecurvol where date(polledTime) = curdate() and st5_current >= 0 order by polledTime desc limit 1;")

            dateres = awscur.fetchall()

            if len(dateres) > 0:
                dated = str(dateres[0][0])[0:17]+"00"
                print("date",dated)
                query = f"SELECT recordTimestamp, batteryVoltage, batteryCurrent FROM EMS.ioeSt5BatteryData WHERE recordTimestamp >= '{dated}';"
            else:
                query = "SELECT recordTimestamp, batteryVoltage, batteryCurrent FROM EMS.ioeSt5BatteryData WHERE DATE(recordTimestamp) = CURDATE();"
            emscur.execute(query)
            result = emscur.fetchall()

            def CurrentSummarize(polledTime, voltage, currentValue):
                minute = str(polledTime)[0:17]+"00"

                if voltage is not None:
                    if minute not in current1:
                        current1[minute] = currentValue
                    else:
                        current1[minute] = max(current1[minute], currentValue)

                    if minute not in voltage1:
                        voltage1[minute] = voltage
                    else:
                        voltage1[minute] = max(voltage1[minute], voltage)         

            for row in result:                     
                polledTime = row[0]
                voltageValue = row[1]
                currentValue = row[2]

                CurrentSummarize(polledTime, voltageValue, currentValue)

            for minute in current1.keys():           
                current = current1[minute]
                voltage = voltage1[minute]        
                sql = """INSERT INTO ioecurvol(polledtime,st5_current, st5_voltage) VALUES(%s, %s, %s)"""
                val = (minute,current,voltage)
                try:
                        awscur.execute(sql, val)
                        print(f"New data inserted for st5 on {minute}")
                        awsdb.commit()
                except mysql.connector.IntegrityError:
                        sql = """UPDATE ioecurvol SET st5_current = %s , st5_voltage = %s WHERE polledtime = %s"""
                        val = (current,voltage,minute)
                        awscur.execute(sql, val)
                        print(val)
                        print("Existing record updated for st5")
                        awsdb.commit()
        except:
            return {"error":"mysql connection"}
        
        emscur.close()
        emsdb.close()
        bmscur.close()
        bmsdb.close()
        awscur.close()
        awsdb.close()
        
        data = {"message":"IOE cur/vol st5 Updated"}
        return jsonify(data), 200
    
    else:
        return jsonify({'error': 'Unauthorized'}), 401


@app.route('/sumCurVol', methods = ['GET'])
def sumCurVol():
    token = request.headers.get('Authorization')
    print(token)
    stVolt = []

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

            awscur.execute("SELECT polledTime FROM EMS.ioecurvol where date(polledTime) = curdate() and st5_current >= 0 order by polledTime desc limit 1;")

            dateres = awscur.fetchall()

            if len(dateres) > 0:
                if dateres[0][0] != None:
                    # dated = str(dateres[0][0])[0:14]+"00:00"
                    dated = "2024-07-26 00:00:00"
                    query = f"""SELECT polledtime,st1_current,st2_current,st3_current,st4_current,st5_current,
                                st1_voltage,st2_voltage,st3_voltage,st4_voltage,st5_voltage FROM EMS.ioecurvol 
                                where polledTime >= '{dated}';"""
                else:
                    query = f"""SELECT polledtime,st1_current,st2_current,st3_current,st4_current,st5_current, 
                                st1_voltage,st2_voltage,st3_voltage,st4_voltage,st5_voltage FROM EMS.ioecurvol 
                                where polledTime = curdate();"""
            else:
                query = f"""SELECT polledtime,st1_current,st2_current,st3_current,st4_current,st5_current,
                                st1_voltage,st2_voltage,st3_voltage,st4_voltage,st5_voltage FROM EMS.ioecurvol 
                                where polledTime >= curdate();"""
            awscur.execute(query)
            res = awscur.fetchall()

            print(res)

            def SumCurVol(polledTime,Cur,Volt):
                sql = """INSERT INTO ioecurvol(polledtime, sum_of_current, avg_of_voltage) VALUES(%s, %s, %s)"""
                val = (polledTime,Cur,Volt)
                try:
                    awscur.execute(sql, val)
                    print(val)
                    print(f"New data inserted for {polledTime}")
                    awsdb.commit()
                except mysql.connector.IntegrityError:
                    sql = """UPDATE ioecurvol SET sum_of_current = %s ,avg_of_voltage = %s WHERE polledtime = %s"""
                    val = (Cur,Volt,polledTime)
                    awscur.execute(sql, val)
                    print(val)
                    print("Existing record updated")
                    awsdb.commit()

            for i in res:
                if i[1] != None:
                    st1C = i[1]
                else:
                    st1C = 0
                if i[2] != None:
                    st2C = i[2]
                else:
                    st2C = 0
                if i[3] != None:
                    st3C = i[3]
                else:
                    st3C = 0
                if i[4] != None:
                    st4C = i[4]
                else:
                    st4C = 0
                if i[5] != None:
                    st5C = i[5]
                else:
                    st5C = 0
                
                if i[6] != None:
                    st1V = i[6]
                    stVolt.append(st1V)
                else:
                    st1V = 0
                if i[7] != None:
                    st2V = i[7]
                    stVolt.append(st2V)
                else:
                    st2V = 0
                if i[8] != None:
                    st3V = i[8]
                    stVolt.append(st3V)
                else:
                    st3V = 0
                if i[9] != None:
                    st4V = i[9]
                    stVolt.append(st4V)
                else:
                    st4V = 0
                if i[10] != None:
                    st5V = i[10]
                    stVolt.append(st5V)
                else:
                    st5V = 0

                sumCur = st1C+st2C+st3C+st4C+st5C
                sumVolt = sum(stVolt)/len(stVolt)

                SumCurVol(i[0],sumCur,sumVolt)

        except Exception as ex:
            print(ex)
            return {"error":"mysql connection"}
        
        emscur.close()
        emsdb.close()
        awscur.close()
        awsdb.close()
        
        data = {"message":"IOE cur/vol sum Updated"}
        return jsonify(data), 200
    
    else:
        return jsonify({'error': 'Unauthorized'}), 401

if __name__ == '__main__':
    app.run(host="localhost",port=8005)