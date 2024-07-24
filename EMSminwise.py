from flask import Flask, jsonify, request
import mysql.connector
from collections import defaultdict
import pandas as pd

app = Flask(__name__)

def check_authentication(token):
    # Replace this with your own logic to validate the token
    valid_token = "VKOnNhH2SebMU6S"
    return token == valid_token


@app.route('/kvavskwh', methods = ['GET'])
def KVAvsKWH():
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        max_demand = 0
        powermvp1 = defaultdict(int)
        powermvp2 = defaultdict(int)
        powermvp3 = defaultdict(int)
        powermvp4 = defaultdict(int)
        power_by_minute = defaultdict(int)
        try:
            awsdb = mysql.connector.connect(
                        host="3.111.70.53",
                        user="emsroot",
                        password="22@teneT",
                        database='meterdata',
                        port=3307
                        )
            
            bmsdb = mysql.connector.connect(
                        host="121.242.232.151",
                        user="emsrouser",
                        password="emsrouser@151",
                        database='bmsmgmtprodv13',
                        port=3306
                        )
            bmscur = bmsdb.cursor()
            awscur = awsdb.cursor()

            awscur.execute("SELECT timestamp FROM meterdata.kv_vs_kwh where date(timestamp) = curdate() order by timestamp desc limit 1;")

            res = awscur.fetchall()

            if len(res) > 0:
                polledTime = str(res[0][0])[0:14]+"00:00"          
                if polledTime != None:
                    query = f"""
                    SELECT polledTime, totalApparentPower2 
                    FROM bmsmgmt_olap_prod_v13.hvacSchneider7230Polling 
                    WHERE polledTime >= '{polledTime}';
                    """
                    bmscur.execute(query)
                    data = bmscur.fetchall()

                    for timestamp, power in data:
                        if power is not None:
                            minute = str(timestamp)[0:17] + "00"
                            power_by_minute[minute] = max(power_by_minute[minute], power)

                    query = f"""
                    SELECT polledTime, mvpnum, acmeterpower 
                    FROM bmsmgmt_olap_prod_v13.MVPPolling 
                    WHERE polledTime >= '{polledTime}';
                    """
                    bmscur.execute(query)
                    result = bmscur.fetchall()

                    for timestamp, mvp, power in result:
                        if power is not None:
                            minute = str(timestamp)[0:17] + "00"
                            if mvp == 'MVP1':
                                powermvp1[minute] = max(powermvp1[minute], power)
                            elif mvp == 'MVP2':
                                powermvp2[minute] = max(powermvp2[minute], power)
                            elif mvp == 'MVP3':
                                powermvp3[minute] = max(powermvp3[minute], power)
                            elif mvp == 'MVP4':
                                powermvp4[minute] = max(powermvp4[minute], power)

                    for minute in power_by_minute.keys():
                        max_demand = power_by_minute[minute]

                        mvp1 = powermvp1[minute]
                        mvp2 = powermvp2[minute]
                        mvp3 = powermvp3[minute]
                        mvp4 = powermvp4[minute]

                        sql = """INSERT INTO kv_vs_kwh(timestamp, peakmax, mvp1, mvp2, mvp3, mvp4) VALUES(%s, %s, %s, %s, %s, %s)"""
                        val = (minute, max_demand, mvp1 / 1000, mvp2 / 1000, mvp3 / 1000, mvp4 / 1000)
                        try:
                            awscur.execute(sql, val)
                            print(f"New data inserted for MVPs on {minute}")
                            awsdb.commit()
                        except mysql.connector.IntegrityError:
                            sql = """UPDATE kv_vs_kwh SET peakmax = %s, mvp1 = %s, mvp2 = %s, mvp3 = %s, mvp4 = %s WHERE timestamp = %s"""
                            val = (max_demand, mvp1 / 1000, mvp2 / 1000, mvp3 / 1000, mvp4 / 1000, minute)
                            awscur.execute(sql, val)
                            print("Existing record updated for MVPs")
                            awsdb.commit()
            else:

                query = """
                SELECT polledTime, totalApparentPower2 
                FROM bmsmgmt_olap_prod_v13.hvacSchneider7230Polling 
                WHERE DATE(polledTime) = CURDATE();
                """
                bmscur.execute(query)
                data = bmscur.fetchall()

                for timestamp, power in data:
                    if power is not None:
                        minute = str(timestamp)[0:17] + "00"
                        power_by_minute[minute] = max(power_by_minute[minute], power)

                query = """
                SELECT polledTime, mvpnum, acmeterpower 
                FROM bmsmgmt_olap_prod_v13.MVPPolling 
                WHERE DATE(polledTime) = CURDATE();
                """
                bmscur.execute(query)
                result = bmscur.fetchall()

                for timestamp, mvp, power in result:
                    if power is not None:
                        minute = str(timestamp)[0:17] + "00"
                        if mvp == 'MVP1':
                            powermvp1[minute] = max(powermvp1[minute], power)
                        elif mvp == 'MVP2':
                            powermvp2[minute] = max(powermvp2[minute], power)
                        elif mvp == 'MVP3':
                            powermvp3[minute] = max(powermvp3[minute], power)
                        elif mvp == 'MVP4':
                            powermvp4[minute] = max(powermvp4[minute], power)

                for minute in power_by_minute.keys():
                    max_demand = power_by_minute[minute]

                    mvp1 = powermvp1[minute]
                    mvp2 = powermvp2[minute]
                    mvp3 = powermvp3[minute]
                    mvp4 = powermvp4[minute]

                    sql = """INSERT INTO kv_vs_kwh(timestamp, peakmax, mvp1, mvp2, mvp3, mvp4) VALUES(%s, %s, %s, %s, %s, %s)"""
                    val = (minute, max_demand, mvp1 / 1000, mvp2 / 1000, mvp3 / 1000, mvp4 / 1000)
                    try:
                        awscur.execute(sql, val)
                        print(f"New data inserted for MVPs on {minute}")
                        awsdb.commit()
                    except mysql.connector.IntegrityError:
                        sql = """UPDATE kv_vs_kwh SET peakmax = %s, mvp1 = %s, mvp2 = %s, mvp3 = %s, mvp4 = %s WHERE timestamp = %s"""
                        val = (max_demand, mvp1 / 1000, mvp2 / 1000, mvp3 / 1000, mvp4 / 1000, minute)
                        awscur.execute(sql, val)
                        print("Existing record updated for MVPs")
                        awsdb.commit()
            bmscur.close()
            awscur.close()
        
        except Exception as ex:
            print(ex)
            return {"error":"mysql connection"}

        data = {"message":"KVA vs KWh Updated"}
        return jsonify(data), 200
    
    else:
        return jsonify({'error': 'Unauthorized'}), 401


@app.route('/dgMinwise', methods = ['GET'])
def DGMin():
    token = request.headers.get('Authorization')
    print(token)
    
    if token and check_authentication(token):
        processeddb = mysql.connector.connect(
                        host="121.242.232.151",
                        user="emsrouser",
                        password="emsrouser@151",
                        database='bmsmgmt_olap_prod_v13',
                        port=3306
                        )

        awsdb = mysql.connector.connect(
                            host="3.111.70.53",
                            user="emsroot",
                            password="22@teneT",
                            database='EMS',
                            port=3307
                        )

        awscur = awsdb.cursor()

        awscur.execute("select polledTime from EMS.DieselMinWise where date(polledTime) = curdate() order by polledTime desc limit 1;")

        dateres = awscur.fetchall()

        if len(dateres) !=0:
        
            dated = str(dateres[0][0])

            query = f"""SELECT polledTime,dgrealenergy*1000 as Energy,DGNum FROM bmsmgmt_olap_prod_v13.DGPolling where polledTime >= '{dated}';"""
        
        else:
            query = f"""SELECT polledTime,dgrealenergy*1000 as Energy,DGNum FROM bmsmgmt_olap_prod_v13.DGPolling where date(polledTime) >=  curdate();"""
        
        df = pd.read_sql_query(query,processeddb)

        df['polledTime'] = pd.to_datetime(df['polledTime']).dt.strftime('%Y-%m-%d %H:%M:00')

        df['Diff'] = df.groupby('DGNum')['Energy'].diff()

        DG_df = df.pivot_table(index='polledTime', columns='DGNum', values='Diff', aggfunc='sum').reset_index()
        DG_df[1].fillna(0, inplace=True)
        DG_df[2].fillna(0, inplace=True)
        DG_df[3].fillna(0, inplace=True)
        DG_df[4].fillna(0, inplace=True)
        DG_df[5].fillna(0, inplace=True)

        for index, row in DG_df.iterrows():
            val = (row['polledTime'],row[1],row[2],row[3],row[4],row[5])
            sql = "INSERT INTO EMS.DieselMinWise(polledTime,DG1 ,DG2 ,DG3 ,DG4, DG5) values(%s,%s,%s,%s,%s,%s)"
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("Diesel Min Inserted")
            except mysql.connector.errors.IntegrityError:
                continue
        
        processeddb.close()
        awscur.close()

        data = {"message":"DG Min"}
        return jsonify(data), 200
    else:
        return jsonify({'error': 'Unauthorized'}), 401

@app.route('/WdMinwise', methods = ['GET'])
def WdMin():
    token = request.headers.get('Authorization')
    print(token)
    
    if token and check_authentication(token):
        processeddb = mysql.connector.connect(
                    host="121.242.232.151",
                    user="emsrouser",
                    password="emsrouser@151",
                    database='bmsmgmt_olap_prod_v13',
                    port=3306
                    )

        procur = processeddb.cursor()

        emsdb = mysql.connector.connect(
                        host="3.111.70.53",
                        user="emsroot",
                        password="22@teneT",
                        database='EMS',
                        port=3307
                    )
        
        emscur = emsdb.cursor()

        emscur.execute("SELECT polledTime FROM EMS.minWiseData where date(polledTime) = curdate() and wind >= 0 order by polledTime desc limit 1;")

        wind_time_res = emscur.fetchall()

        print(len(wind_time_res))
        
        if len(wind_time_res) !=0:
            wind_time = wind_time_res[0][0]
            print("Last Wind Time",wind_time)

            procur.execute(f"""SELECT FROM_UNIXTIME(otpmgndetailspolledtimestamp) as polledTime,otpmgndetailsactivepower/60 as Energy  
                    FROM bmsmgmtprodv13.otpmgndetails where FROM_UNIXTIME(otpmgndetailspolledtimestamp) >= '{wind_time}'
                    and otpmgndetailsactivepower > 0""")
            wind_res = procur.fetchall()
        
        else:
            procur.execute(f"""SELECT FROM_UNIXTIME(otpmgndetailspolledtimestamp) as polledTime,otpmgndetailsactivepower/60 as Energy  
                    FROM bmsmgmtprodv13.otpmgndetails where date(FROM_UNIXTIME(otpmgndetailspolledtimestamp)) = curdate()
                    and otpmgndetailsactivepower > 0""")
            wind_res = procur.fetchall()
        
        def WindMinSeg(polledTime,Energy):
            if Energy < 0:
                Energy = 0
            print(polledTime)
            polledTime = str(polledTime)[0:17]+"00"
            val = (polledTime,Energy)
            print(val)
            sql = "INSERT INTO EMS.minWiseData(polledTime,wind) values(%s,%s)"
            try:
                emscur.execute(sql,val)
                emsdb.commit()
                print("Wind Minute Inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE EMS.minWiseData SET wind = %s WHERE polledTime = %s"
                val = (Energy,polledTime)
                emscur.execute(sql,val)
                emsdb.commit()
                print("Wind min Updated")

        for i in wind_res:
            WindMinSeg(i[0],i[1])
        
        procur.close()
        emscur.close()

        data = {"message":"Wind Min"}
        return jsonify(data), 200
    else:
        return jsonify({'error': 'Unauthorized'}), 401


@app.route('/WhGdMinwise', methods = ['GET'])
def WhGdMin():
    token = request.headers.get('Authorization')
    print(token)
    
    if token and check_authentication(token):
        processeddb = mysql.connector.connect(
                    host="121.242.232.151",
                    user="emsrouser",
                    password="emsrouser@151",
                    database='bmsmgmt_olap_prod_v13',
                    port=3306
                    )

        procur = processeddb.cursor()

        emsdb = mysql.connector.connect(
                        host="3.111.70.53",
                        user="emsroot",
                        password="22@teneT",
                        database='EMS',
                        port=3307
                    )
        
        emscur = emsdb.cursor()

        grid_min = {}

        emscur.execute("SELECT polledTime FROM EMS.minWiseData where date(polledTime) = curdate() and wheeled >= 0 order by polledTime desc limit 1;")

        wheeled_time_res = emscur.fetchall()

        print(len(wheeled_time_res))
        
        if len(wheeled_time_res) !=0:
            wheeled_time = wheeled_time_res[0][0]

            print("Last Wheeled Time",wheeled_time)

            emscur.execute(f"SELECT metertimestamp,meterenergy FROM EMS.EMSMeterData where metertimestamp >= '{wheeled_time}'")

            wheeled_res = emscur.fetchall()
        
        else:
            emscur.execute(f"SELECT metertimestamp,meterenergy FROM EMS.EMSMeterData where date(metertimestamp) = curdate()")

            wheeled_res = emscur.fetchall()

        def WheelMinSeg(polledTime,Energy):
            if Energy >= 0 and Energy <= 100:
                print(polledTime)
                polledTime = str(polledTime)[0:17]+"00"
                val = (polledTime,Energy)
                print(val)
                sql = "INSERT INTO EMS.minWiseData(polledTime,wheeled) values(%s,%s)"
                try:
                    emscur.execute(sql,val)
                    emsdb.commit()
                    print("Wheeled Minute Inserted")
                except mysql.connector.errors.IntegrityError:
                    sql = "UPDATE EMS.minWiseData SET wheeled = %s WHERE polledTime = %s"
                    val = (Energy,polledTime)
                    emscur.execute(sql,val)
                    emsdb.commit()
                    print("Wheeled min Updated")

        for i in range( 1,len(wheeled_res)):
            WheelMinSeg(wheeled_res[i][0],(wheeled_res[i][1]-wheeled_res[i-1][1])*1000)
            #print(wheeled_res[i][0],(wheeled_res[i][1]-wheeled_res[i-1][1]))

        def GridMinSeg(polledTime,Energy):
            polledTime = str(polledTime)[0:17]+"00"
            if polledTime in grid_min.keys():
                grid_min[polledTime] += Energy
            else:
                grid_min[polledTime] = Energy

        emscur.execute("SELECT polledTime FROM EMS.minWiseData where date(polledTime) = curdate() and grid >= 0 order by polledTime desc limit 1;")

        grid_time_res = emscur.fetchall()

        if len(grid_time_res) !=0:
            grid_time = grid_time_res[0][0]

            print("Last Wheeled Time",grid_time)

            procur.execute(f"select polledTime,FLOOR(acmeterenergy) from bmsmgmt_olap_prod_v13.MVPPolling where mvpnum in ('MVP1','MVP2','MVP3','MVP4') and polledTime >= '{grid_time}'")

            data = procur.fetchall()

        else:
            procur.execute(f"select polledTime,FLOOR(acmeterenergy) from bmsmgmt_olap_prod_v13.MVPPolling where mvpnum in ('MVP1','MVP2','MVP3','MVP4') and date(polledTime) = curdate()")
        
            data = procur.fetchall()

        for i in data:
            if i[1] != None:
                GridMinSeg(i[0],i[1]/1000)
        
        # for i in grid_min.keys():
        #     print(i,grid_min[i])
        
        result_dict = {}

        for key in grid_min.keys():
            current_value = grid_min[key]
            if key != list(grid_min.keys())[0]:  # Skip the first key since there is no previous value
                previous_value = grid_min[prev_key]
                cummulative = current_value - previous_value
                if abs(cummulative) <= 100:
                    result_dict[key] = current_value - previous_value
            prev_key = key

        # print(result_dict)

        for i in result_dict.keys():
            val = (i,abs(result_dict[i]))
            print(val)
            sql = "INSERT INTO EMS.minWiseData(polledTime,grid) values(%s,%s)"
            try:
                emscur.execute(sql,val)
                emsdb.commit()
                print("Grid Minute Inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "UPDATE EMS.minWiseData SET grid = %s WHERE polledTime = %s"
                val = (abs(result_dict[i]),i)
                emscur.execute(sql,val)
                emsdb.commit()
                print("Grid min Updated")

        procur.close()
        emscur.close()

        data = {"message":"Wheeled/Grid Min"}
        return jsonify(data), 200
    else:
        return jsonify({'error': 'Unauthorized'}), 401
    

if __name__ == '__main__':
    app.run(host="localhost",port=8004)