from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
import mysql.connector
from fastapi.responses import JSONResponse
import json

app = FastAPI()

origins = [
    "*",
    # Add more origins as needed
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

file_path = "/home/tenet/ems/py_script/db_creds.json"

with open(file_path, 'r') as file:
    data = json.load(file)

def get_emsdb():
    db = mysql.connector.connect(
        host=data['awsDB']['host'],
        user=data['awsDB']['user'],
        password=data['awsDB']['password'],
        database='EMS',
        port=data['awsDB']['port']
    )
    return db


def get_meterdb():
    db = mysql.connector.connect(
        host=data['awsDB']['host'],
        user=data['awsDB']['user'],
        password=data['awsDB']['password'],
        database='meterdata',
        port=data['awsDB']['port']
    )
    return db

def get_RAWemsdb():
    db = mysql.connector.connect(
        host=data['emsDB']['host'],
        user=data['emsDB']['user'],
        password=data['emsDB']['password'],
        database='EMS',
        port=data['emsDB']['port']
    )
    return db

@app.get('/Dashboard/REtillDay')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    clients = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"}) 
    
    emscur = processed_db.cursor()

    emscur.execute("select sum(gridEnergy),sum(diesel),sum(wheeledinEnergy),sum(wheeledinEnergy2),sum(windenergy),sum(RooftopEnergy) from EMS.buildingConsumptionMonthWise")

    res = emscur.fetchall()

    for i in res:
        Renewable = (i[2]+i[3]+i[4]+i[5])
        Consumed = (i[0]+i[1]+i[2]+i[3]+i[4]+i[5])
        RE = (round((Renewable/Consumed)*100,1))

        clients.append({'RE':RE})  

    emscur.close()

    return clients

@app.get('/Dashboard/topClients')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    clients = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"}) 
    
    emscur = processed_db.cursor()

    emscur.execute("""SELECT polledtime,tenantname,Energy,physical_location FROM ( SELECT polledtime,Energy,tenantname,physical_location, ROW_NUMBER() 
                        OVER (PARTITION BY HOUR(polledtime) ORDER BY Energy DESC) AS row_num FROM EMS.Clientshourlysum 
                        WHERE DATE(polledtime) = CURDATE() ) AS ranked_data
                        WHERE row_num <= 4 order by polledtime desc limit 4;""")
    
    res = emscur.fetchall()
    
    for i in res:
        clients.append({i[1]:[i[2],i[3]]})
    
    emscur.close()

    return clients

@app.post('/Dashboard/topClients/Filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_emsdb)):
    clients = []

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as emscur:
                emscur.execute(f"""SELECT polledTime,tenantname, SUM(Energy),physical_location AS total_energy FROM EMS.Clientshourlysum
                WHERE date(polledTime) = '{value}' GROUP BY tenantname order by total_energy desc limit 4;""")

                res = emscur.fetchall()

                for i in res:
                    clients.append({i[1]:[i[2],i[3]]})
                
                emscur.close()
    
    except mysql.connector.Error as e:
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)

    return clients

@app.get('/Dashboard/facilityUsage')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    energy = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"}) 
    
    emscur = processed_db.cursor()

    emscur.execute("SELECT sum(Energy) FROM EMS.Clientshourlysum where date(polledTime) = curdate();")

    clientres = emscur.fetchall()

    if len(clientres) > 0:
        if clientres[0][0] != None:
            client = clientres[0][0]
        else:
            client = 0
    else:
        client = 0

    emscur.execute("SELECT sum(mlcpgf),sum(eblock1f),sum(eblockf9),sum(eblockgf),sum(dblock3f),sum(dblock7f),sum(cblocktf),sum(bblocktf),sum(ablocktf) FROM EMS.OthersConsumption where date(polledTime) = curdate();")

    othersres = emscur.fetchall()

    if len(othersres) > 0:
        if othersres[0][0] == None:
            mlcp = 0
        else:
            mlcp = othersres[0][0]

        if othersres[0][1] == None:
            eblk1 = 0
        else:
            eblk1 = othersres[0][1]

        if othersres[0][2] == None:
            eblk9 = 0
        else:
            eblk9 = othersres[0][2]

        if othersres[0][3] == None:
            eblkg = 0
        else:
            eblkg = othersres[0][3]

        if othersres[0][4] == None:
            dblk3 = 0
        else:
            dblk3 = othersres[0][4]

        if othersres[0][5] == None:
            dblk7 = 0
        else:
            dblk7 = othersres[0][5]

        if othersres[0][6] == None:
            cblkt = 0
        else:
            cblkt = othersres[0][6]

        if othersres[0][7] == None:
            bblkt = 0
        else:
            bblkt = othersres[0][7]

        if othersres[0][8] == None:
            cblkt = 0
        else:
            cblkt = othersres[0][8]
        others = (mlcp+eblk1+eblk9+eblkg+dblk3+dblk7+cblkt+bblkt)/1000
    else:
        others = 0
    
    emscur.execute("SELECT sum(Energy) FROM EMS.Comareahourlysum where date(polledTime) = curdate();")

    commonres = emscur.fetchall()

    if len(commonres) > 0:
        if commonres[0][0] != None:
            commonarea = commonres[0][0]
        else:
            commonarea = 0
    else:
        commonarea = 0
    
    emscur.execute("SELECT sum(Total_ElectricalEnergy) FROM EMS.ElectricalEnergy where date(polledTime) = curdate();")

    chillerres = emscur.fetchall()

    if len(chillerres) > 0:
        if chillerres[0][0] != None:
            chiller = chillerres[0][0]
        else:
            chiller = 0
    else:
        chiller = 0

    data = {'chiller':chiller,'commonarea':commonarea,'others':others,'client':client}

    sorted_data = dict(sorted(data.items(), key=lambda item: item[1], reverse=True))

    energy.append(sorted_data)

    emscur.close()

    return energy


@app.post('/Dashboard/facilityUsage/Filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_emsdb)):
    energy = []

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as emscur:
                emscur.execute(f"SELECT sum(Energy) FROM EMS.Clientshourlysum where date(polledTime) = '{value}';")

                clientres = emscur.fetchall()

                if len(clientres) > 0:
                    if clientres[0][0] != None:
                        client = clientres[0][0]
                    else:
                        client = 0
                else:
                    client = 0

                emscur.execute(f"SELECT sum(mlcpgf),sum(eblock1f),sum(eblockf9),sum(eblockgf),sum(dblock3f),sum(dblock7f),sum(cblocktf),sum(bblocktf),sum(ablocktf) FROM EMS.OthersConsumption where date(polledTime) = '{value}';")

                othersres = emscur.fetchall()

                if len(othersres) > 0:
                    if othersres[0][0] == None:
                        mlcp = 0
                    else:
                        mlcp = othersres[0][0]

                    if othersres[0][1] == None:
                        eblk1 = 0
                    else:
                        eblk1 = othersres[0][1]

                    if othersres[0][2] == None:
                        eblk9 = 0
                    else:
                        eblk9 = othersres[0][2]

                    if othersres[0][3] == None:
                        eblkg = 0
                    else:
                        eblkg = othersres[0][3]

                    if othersres[0][4] == None:
                        dblk3 = 0
                    else:
                        dblk3 = othersres[0][4]

                    if othersres[0][5] == None:
                        dblk7 = 0
                    else:
                        dblk7 = othersres[0][5]

                    if othersres[0][6] == None:
                        cblkt = 0
                    else:
                        cblkt = othersres[0][6]

                    if othersres[0][7] == None:
                        bblkt = 0
                    else:
                        bblkt = othersres[0][7]

                    if othersres[0][8] == None:
                        cblkt = 0
                    else:
                        cblkt = othersres[0][8]
                    others = (mlcp+eblk1+eblk9+eblkg+dblk3+dblk7+cblkt+bblkt)/1000
                else:
                    others = 0
                
                emscur.execute(f"SELECT sum(Energy) FROM EMS.Comareahourlysum where date(polledTime) = '{value}';")

                commonres = emscur.fetchall()

                if len(commonres) > 0:
                    if commonres[0][0] != None:
                        commonarea = commonres[0][0]
                    else:
                        commonarea = 0
                else:
                    commonarea = 0
                
                emscur.execute(f"SELECT sum(Total_ElectricalEnergy) FROM EMS.ElectricalEnergy where date(polledTime) = '{value}';")

                chillerres = emscur.fetchall()

                if len(chillerres) > 0:
                    if chillerres[0][0] != None:
                        chiller = chillerres[0][0]
                    else:
                        chiller = 0
                else:
                    chiller = 0

                data = {'chiller':chiller,'commonarea':commonarea,'others':others,'client':client}

                sorted_data = dict(sorted(data.items(), key=lambda item: item[1], reverse=True))

                energy.append(sorted_data)

                emscur.close()
    
    except mysql.connector.Error as e:
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)

    return energy


@app.get('/Dashboard/REprofile')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    RE_list = []
    curRE = 0
    prevRE = 0
    Roof = 0

    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    emscur = processed_db.cursor()
    
    emscur.execute("select polledDate,sum(gridEnergy),sum(wheeledinEnergy),sum(rooftopEnergy),sum(deisel),sum(wheeledinEnergy2) from EMS.buidingConsumptionDayWise where polledDate <= curdate() and polledDate >= date_sub(curdate(),interval 6 day);")

    curres = emscur.fetchall()

    Wheeled = 0

    for i in curres:
        if i[5]:
            try:
                curRE = ((i[2]+i[3]+i[5])/(i[1]+i[2]+i[3]+i[4]+i[5]))*100
            except Exception as ex:
                print(ex)
                curRE = 0
        else:
            try:
                curRE = ((i[2]+i[3])/(i[1]+i[2]+i[3]+i[4]))*100
            except Exception as ex:
                print(ex)
                curRE = 0

        Roof = (i[3] / (i[3]+i[2]))*100

        Wheeled = (i[2] / (i[2]+i[3]))*100

        print(curRE,Roof,Wheeled)


    emscur.execute("select polledDate,sum(gridEnergy),sum(wheeledinEnergy),sum(rooftopEnergy),sum(deisel),sum(wheeledinEnergy2)  from EMS.buidingConsumptionDayWise where polledDate <= date_sub(curdate(),interval 7 day) and polledDate >= date_sub(curdate(),interval 13 day);")

    prevres = emscur.fetchall()

    for i in prevres:
        if i[5]:
            try:
                prevRE = ((i[2]+i[3]+i[5])/(i[1]+i[2]+i[3]+i[4]+i[5]))*100
            except Exception as ex:
                print(ex)
                prevRE = 0
        else:
            try:
                prevRE = ((i[2]+i[3])/(i[1]+i[2]+i[3]+i[4]))*100
            except Exception as ex:
                print(ex)
                prevRE = 0

    emscur.execute("select polledDate,gridEnergy,wheeledinEnergy,rooftopEnergy,deisel,wheeledinEnergy2 from EMS.buidingConsumptionDayWise where polledDate <= curdate() and polledDate >= date_sub(curdate(),interval 29 day);")

    curmres = emscur.fetchall()

    curmRE = 0
    Roofm = 0
    Wheeledm = 0
    prevmRE = 0
    Wheeledm2 = 0

    for i in curmres:
        if i[5]:
            try:
                curmRE = ((i[2]+i[3]+i[5])/(i[1]+i[2]+i[3]+i[4]+i[5]))*100
            except Exception as ex:
                print(ex)
                curmRE = 0
        else:
            try:
                curmRE = ((i[2]+i[3])/(i[1]+i[2]+i[3]+i[4]))*100
            except Exception as ex:
                print(ex)
                curmRE = 0

        Roofm = (i[3] / (i[3]+i[2]))*100

        Wheeledm = (i[2] / (i[2]+i[3]))*100

    emscur.execute("select polledDate,gridEnergy,wheeledinEnergy,rooftopEnergy,deisel,wheeledinEnergy2 from EMS.buidingConsumptionDayWise where polledDate <= date_sub(curdate(),interval 30 day) and polledDate >= date_sub(curdate(),interval 59 day);")

    prevmres = emscur.fetchall()

    for i in prevmres:
        if i[5]:
            try:
                prevmRE = ((i[2]+i[3]+i[5])/(i[1]+i[2]+i[3]+i[4]+i[5]))*100
            except Exception as ex:
                print(ex)
                prevmRE = 0
        else:
            try:
                prevmRE = ((i[2]+i[3])/(i[1]+i[2]+i[3]+i[4]))*100
            except Exception as ex:
                print(ex)
                prevmRE = 0

    RE_list.append({"CurrentWeek":round(curRE,1),"RoofWeek":round(Roof),"WheeledWeek":round(Wheeled),"diffWeek":round(curRE - prevRE,1),
                    "CurrentMonth":round(curmRE,1),"RoofMont":round(Roofm),"WheeledMonth":round(Wheeledm),"diffMonth":round(curmRE - prevmRE,1)})

    emscur.close()

    return RE_list


@app.get('/Dashboard/Highlights')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    Highlights = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT round(sum(gridEnergy)),round(sum(wheeledinEnergy)),round(sum(rooftopEnergy)),round(sum(wheeledinEnergy2)) FROM EMS.buildingConsumption where date(polledTime) = curdate();")
    
    res = bms_cur.fetchall()

    if res[0][0] != None:
        grid = res[0][0]
    else:
        grid = 0
    if res[0][1] != None:
        wheeled = res[0][1]
    else:
        wheeled = 0
    if res[0][2] != None:
        rooftop = res[0][2]
    else:
        rooftop = 0
    if res[0][3] != None:
        wheeled2 = res[0][3]
    else:
        wheeled2 = 0

    bms_cur.execute("SELECT round(total_energy_difference) FROM meterdata.diselenergy where date(polled_time) = curdate() order by polled_time desc limit 1;")

    diselres = bms_cur.fetchall()

    if len(diselres) > 0:
        if diselres[0][0] != None:
            diesel = diselres[0][0]
        else:
            diesel = 0

    bms_cur.execute("SELECT avgpowerfactor,minpowerfactor FROM EMS.schneider7230processed where date(polledTime) = curdate() order by polledTime desc limit 1;")
    
    powerres = bms_cur.fetchall()

    avgfac = powerres[0][0]
    minfac = powerres[0][1]

    RE = round((wheeled+wheeled2+rooftop)/(grid+wheeled2+wheeled+diesel)*100,3)

    Highlights.append({'wheeled':wheeled,'wheeled2':wheeled2,'rooftop':rooftop,'grid':grid,
                       'diesel':diesel,'avgFactor':avgfac,'minFactor':minfac,'RE':RE})
    bms_cur.close()

    return Highlights

@app.post('/Dashboard/Highlights/Filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_emsdb)):
    Highlights = []

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bms_cur:

                bms_cur.execute(f"SELECT round(sum(gridEnergy)),round(sum(wheeledinEnergy)),round(sum(rooftopEnergy)),round(sum(wheeledinEnergy2)) FROM EMS.buildingConsumption where date(polledTime) = '{value}';")
                
                res = bms_cur.fetchall()

                if res[0][0] != None:
                    grid = res[0][0]
                else:
                    grid = 0
                if res[0][1] != None:
                    wheeled = res[0][1]
                else:
                    wheeled = 0
                if res[0][2] != None:
                    rooftop = res[0][2]
                else:
                    rooftop = 0
                if res[0][3] != None:
                    wheeled2 = res[0][3]
                else:
                    wheeled2 = 0

                bms_cur.execute(f"SELECT sum(Energy) FROM EMS.DGHourly where date(polledTime) = '{value}';")

                diselres = bms_cur.fetchall()

                if diselres[0][0] != None:
                    diesel = diselres[0][0]
                else:
                    diesel = 0

                bms_cur.execute(f"SELECT avg(avgpowerfactor),min(minpowerfactor) FROM EMS.schneider7230processed where date(polledTime) = '{value}';")
                
                powerres = bms_cur.fetchall()

                avgfac = powerres[0][0]
                minfac = powerres[0][1]

                RE = (wheeled+wheeled2+rooftop)/(grid+wheeled2+wheeled+diesel)

                Highlights.append({'wheeled':wheeled,'wheeled2':wheeled2,'rooftop':rooftop,'grid':grid,
                                'diesel':diesel,'avgFactor':avgfac,'minFactor':minfac,'RE':RE})
                bms_cur.close()
    
    except mysql.connector.Error as e:
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)

    return Highlights


@app.get('/Dashboard/WheeledInSolar2')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    WheeledInSolar_response = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT sum(Energy),sum(irradiation) FROM EMS.WheeledHourlyph2 where date(polledTime) = curdate();")
    
    res = bms_cur.fetchall()
    print(res)


    specificyield = res[0][0]/2008.36
    performancePercentage = (res[0][0]/((res[0][1])*2008.36)) * 100

    if performancePercentage > 100:
        performancePercentage = 81
    
    print(performancePercentage)

    WheeledInSolar_response.append({'meterenergy_diff_sum':round(res[0][0]),'wmsirradiation_sum':round(res[0][1],2),
                              'performance':performancePercentage,'specific_yield':round(specificyield,2)})
    bms_cur.close()
    processed_db.close()

    return WheeledInSolar_response


@app.get('/Dashboard/WheeledInSolar')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    WheeledInSolar_response = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT sum(Energy),sum(irradiation) FROM EMS.WheeledHourly where date(polledTime) = curdate();")
    
    res = bms_cur.fetchall()
    print(res)


    specificyield = res[0][0]/2008.36
    performancePercentage = (res[0][0]/((res[0][1])*2008.36)) * 100

    if performancePercentage > 100:
        performancePercentage = 81
    
    print(performancePercentage)

    WheeledInSolar_response.append({'meterenergy_diff_sum':round(res[0][0]),'wmsirradiation_sum':round(res[0][1],2),
                              'performance':performancePercentage,'specific_yield':round(specificyield,2)})
    bms_cur.close()
    processed_db.close()

    return WheeledInSolar_response

#Rooftop_Solar  card api

@app.get('/Dashboard/RoofTopSolar')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    RoofTop_response = []
    try:
        processed_db = get_meterdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT sum(energy),sum(irradiation) FROM EMS.roofTopHour where date(polledTime) = curdate();")
    
    res = bms_cur.fetchall()

    specificyield = res[0][0]/1075.8
    roof = res[0][0]
    irrad = (res[0][1])
    performancePercentage = (roof/(irrad*1075.8))*100

    print(roof,irrad)
    print(performancePercentage)

    if performancePercentage > 100:
        performancePercentage = 81

    RoofTop_response.append({'cumulative_energy':round(res[0][0]),'sensorradiation':round(res[0][1],2),
                              'performance':performancePercentage,'specific_yield':round(specificyield,2)})
    
    bms_cur.close()
    processed_db.close()

    return RoofTop_response

#c02 reduction api

@app.get('/Dashboard/co2')
def peak_demand_date(db: mysql.connector.connect = Depends(get_meterdb)):
    Co2_response = []
    try:
        processed_db = get_meterdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("select sum(energy) from EMS.roofTopHour where date(polledTime) = curdate();")
    
    res = bms_cur.fetchall()
    rooftop = res[0][0]

    bms_cur.execute("SELECT sum(Energy) FROM EMS.WheeledHourly where date(polledTime) = curdate();")

    res1 = bms_cur.fetchall()
    wheeled = res1[0][0]

    co2 = [{'co2reduced':round(((rooftop+wheeled)/1000)*0.71,2)}]

    bms_cur.close()
    processed_db.close()

    return co2


# Dashboard Grid  api
@app.get('/Dashboard/Grid')
def peak_demand_date(db: mysql.connector.connect = Depends(get_meterdb)):
    Grid_response = []
    try:
        processed_db = get_meterdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT sum(Energy) as GridEnergy FROM EMS.Gridhourly where date(polledTime)=curdate();")
    
    res = bms_cur.fetchall()

    print(res)

    for i in res:
        Grid_response.append({'GridEnergy':round(i[0])})
    
    bms_cur.close()
    processed_db.close()

    return Grid_response


# Dashboard PowerFactor api
@app.get('/Dashboard/PowerFactor')
def peak_demand_date(db: mysql.connector.connect = Depends(get_meterdb)):
    powerFactor = []
    try:
        processed_db = get_meterdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT avgpowerfactor,minpowerfactor FROM EMS.schneider7230processed where date(polledTime) = curdate() order by polledTime desc limit 1;")
    
    res = bms_cur.fetchall()
    print(res)

    for i in res:
        powerFactor.append({'average_powerfactor':i[0],'minimum_powerfactor':i[1]})
    
    bms_cur.close()
    processed_db.close()

    return powerFactor


@app.get('/Dashboard/ThermalStorage')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    thermalStorage = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("select polledTime,coolingEnergy from EMS.ThermalHourly where date(polledTime) = curdate();")
    
    res = bms_cur.fetchall()

    # print(res)

    for i in res:
        polledTime = str(i[0])[11:16]
        thermalStorage.append({'coolingEnergy':round(i[1],2),'polledTime':polledTime})
    
    bms_cur.close()
    processed_db.close()

    return thermalStorage


@app.get('/Dashboard/chillerstatus')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    chiller_list = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("""SELECT chiller1,chiller2,chiller3,chiller4,chiller5,chiller6,chiller7,chiller8 
                    FROM EMS.ChillerStatus 
                    where date(polledTime) = curdate() 
                    order by polledTime desc limit 1;""")

    res = bms_cur.fetchall()

    for i in res:
        chiller_list.append({'chiller1':i[0],'chiller2':i[1],'chiller3':i[2],'chiller4':i[3],
                             'chiller5':i[4],'chiller6':i[5],'chiller7':i[6],'chiller8':i[7]})
        
    bms_cur.close()
    processed_db.close()
    
    return chiller_list


@app.post('/Dashboard/ThermalStorage/filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_emsdb)):
    thermalStorage = []

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:
                bmscur.execute(f"select polledTime,coolingEnergy from EMS.ThermalHourly where date(polledTime) = '{value}';")

                res = bmscur.fetchall()

                for i in res:
                    polledTime = str(i[0])[11:16]
                    thermalStorage.append({'coolingEnergy':round(i[1],2),'polledTime':polledTime})  
                
                bmscur.close() 
    
    except mysql.connector.Error as e:
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)

    return thermalStorage


@app.get('/Dashboard/ltoBatteryData')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    lto_list = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT chargingEnergy,dischargingEnergy,energyAvailable,packsoc,polledTime FROM EMS.LTObatteryHourly where date(polledTime) = curdate();")

    res = bms_cur.fetchall()

    for i in res:
        polledTime = str(i[4])[11:16]
        if i[0] != None or i[1] != None:
            lto_list.append({'chargingEnergy':i[0],'dischargingEnergy':i[1],'idleEnergy':0,'energy_available':i[2],'Pacsoc':i[3],'polledTime':polledTime})
        else:
            lto_list.append({'chargingEnergy':0,'dischargingEnergy':0,'idleEnergy':0.01,'energy_available':i[2],'Pacsoc':i[3],'polledTime':polledTime})

    
    bms_cur.close()
    processed_db.close()
    
    return lto_list


@app.post('/Dashboard/ltoBattery/filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_emsdb)):
    lto_list = []

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:
                bmscur.execute(f"SELECT chargingEnergy,dischargingEnergy,energyAvailable,packsoc,polledTime FROM EMS.LTObatteryHourly where date(polledTime) = '{value}'")

                res = bmscur.fetchall()

                for i in res:
                    polledTime = str(i[4])[11:16]
                    if i[0] != None or i[1] != None:
                        lto_list.append({'chargingEnergy':i[0],'dischargingEnergy':i[1],'idleEnergy':0,'energy_available':i[2],'Pacsoc':i[3],'polledTime':polledTime})
                    else:
                        lto_list.append({'chargingEnergy':0,'dischargingEnergy':0,'idleEnergy':0.01,'energy_available':i[2],'Pacsoc':i[3],'polledTime':polledTime})
                
                bmscur.close()
    except mysql.connector.Error as e:
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)

    return lto_list


@app.get('/Dashboard/upsBattery')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    lto_list = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT chargingEnergy,discharhingEnergy,energyAvailable,packsoc,polledTime FROM EMS.UPSbatteryHourly where date(polledTime) = curdate();")

    res = bms_cur.fetchall()

    for i in res:
        polledTime = str(i[4])[11:16]
        if i[0] != None or i[1] != None:
            lto_list.append({'chargingEnergy':i[0],'dischargingEnergy':i[1],'idleEnergy':0,'energy_available':i[2],'Pacsoc':i[3],'polledTime':polledTime})
        else:
            lto_list.append({'chargingEnergy':0,'dischargingEnergy':0,'idleEnergy':0.01,'energy_available':i[2],'Pacsoc':i[3],'polledTime':polledTime})

    
    bms_cur.close()
    processed_db.close()
    
    return lto_list



@app.post('/Dashboard/upsBattery/filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_emsdb)):
    lto_list = []

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:
                bmscur.execute(f"SELECT chargingEnergy,discharhingEnergy,energyAvailable,packsoc,polledTime FROM EMS.UPSbatteryHourly where date(polledTime) = '{value}'")

                res = bmscur.fetchall()

                for i in res:
                    polledTime = str(i[4])[11:16]
                    if i[0] != None or i[1] != None:
                        lto_list.append({'chargingEnergy':i[0],'dischargingEnergy':i[1],'idleEnergy':0,'energy_available':i[2],'Pacsoc':i[3],'polledTime':polledTime})
                    else:
                        lto_list.append({'chargingEnergy':0,'dischargingEnergy':0,'idleEnergy':0.01,'energy_available':i[2],'Pacsoc':i[3],'polledTime':polledTime})
                
                bmscur.close()
    except mysql.connector.Error as e:
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)

    return lto_list


#Evcharger api
@app.get("/dashboard/EvCharger")
def ev_charger_dashboard(db: mysql.connector.connect = Depends(get_emsdb)):
    EVCharger_list = []
    try:
        # Get data from the database
        processed_db = get_emsdb()
        charger_cur = processed_db.cursor()

        charger_cur.execute("SELECT chargpointname,energyconsumption,totalsessions FROM evcharger WHERE DATE(servertime) = CURDATE();")
        result = charger_cur.fetchall()

        # Close the database connection
        charger_cur.close()
        processed_db.close()

        # Initialize variables
        NoOfChargers = 0
        CP1_1Status = ""
        LEV4_1Status = ""
        CP11_1Status = ""
        CP12_1Status = ""
        CP13_1Status = ""
        CP14_1Status = ""

        # Initialize dictionaries for each charger
        CP1_1 = {'CP1_1Energy': 0, 'CP1_1TotalSession': 0, 'CP1_1NoOf_chargers': 0}
        LEV4_1 = {'LEV4_1Energy': 0, 'LEV4_1TotalSession': 0, 'LEV4_NoOf_chargers': 0}
        CP11_1 = {'CP11_1Energy': 0, 'CP11_1TotalSession': 0, 'CP11_1NoOf_chargers': 0}
        CP12_1 = {'CP12_1Energy': 0, 'CP12_1TotalSession': 0, 'CP12_1NoOf_chargers': 0}
        CP13_1 = {'CP13_1Energy': 0, 'CP13_1TotalSession': 0, 'CP13_1NoOf_chargers': 0}
        CP14_1 = {'CP14_1Energy': 0, 'CP14_1TotalSession': 0, 'CP14_1NoOf_chargers': 0}

        # Process the result and update dictionaries
        for entry in result:
            charger_name = entry[0]
            energy_consumption = float(entry[1])

            if charger_name == 'CP1_1':
                CP1_1['CP1_1Energy'] += energy_consumption
                CP1_1['CP1_1TotalSession'] = entry[2]
                if energy_consumption > 0:
                    CP1_1['CP1_1NoOf_chargers'] = 'Active'
                    CP1_1Status = 'active'
                    NoOfChargers += 1

            if charger_name == 'LEV4_1':
                LEV4_1['LEV4_1Energy'] += energy_consumption
                LEV4_1['LEV4_1TotalSession'] = entry[2]
                if energy_consumption > 0:
                    LEV4_1['LEV4_1NoOf_chargers'] = 'Active'
                    LEV4_1Status = 'active'
                    NoOfChargers += 1

            if charger_name == 'CP11_1':
                CP11_1['CP11_1Energy'] += energy_consumption
                CP11_1['CP11_1TotalSession'] = entry[2]
                if energy_consumption > 0:
                    CP11_1['CP11_1NoOf_chargers'] = 'Active'
                    CP11_1Status = 'active'
                    NoOfChargers += 1
            
            if charger_name == 'CP12_1':
                CP12_1['CP12_1Energy'] += energy_consumption
                CP12_1['CP12_1TotalSession'] = entry[2]
                if energy_consumption > 0:
                    CP12_1['CP12_1NoOf_chargers'] = 'Active'
                    CP12_1Status = 'active'
                    NoOfChargers += 1
            
            if charger_name == 'CP13_1':
                CP13_1['CP13_1Energy'] += energy_consumption
                CP13_1['CP13_1TotalSession'] = entry[2]
                if energy_consumption > 0:
                    CP13_1['CP13_1NoOf_chargers'] = 'Active'
                    CP13_1Status = 'active'
                    NoOfChargers += 1

            if charger_name == 'CP14_1':
                CP14_1['CP14_1Energy'] += energy_consumption
                CP14_1['CP14_1TotalSession'] = entry[2]
                if energy_consumption > 0:
                    CP14_1['CP14_1NoOf_chargers'] = 'Active'
                    CP14_1Status = 'active'
                    NoOfChargers += 1

        # Calculate total energy and total sessions
        total_energy = CP1_1['CP1_1Energy'] + LEV4_1['LEV4_1Energy'] + CP11_1['CP11_1Energy'] + CP12_1['CP12_1Energy'] + CP13_1['CP13_1Energy'] + CP14_1['CP14_1Energy']
        total_sessions = CP1_1['CP1_1TotalSession'] + LEV4_1['LEV4_1TotalSession'] + CP11_1['CP11_1TotalSession'] + CP12_1['CP12_1TotalSession'] + CP13_1['CP13_1TotalSession'] + CP14_1['CP14_1TotalSession']

        # Create the final result dictionary
        EVCharger_list.append({
            "totalEnergy": round(total_energy, 1),
            "totalSessions": total_sessions,
            "NoOfChargersUsed": NoOfChargers,
            "CP1_1Status": CP1_1Status,
            "LEV4_1Status": LEV4_1Status,
            "CP11_1Status": CP11_1Status,
            "CP12_1Status": CP12_1Status,
            "CP13_1Status": CP13_1Status,
            "CP14_1Status": CP14_1Status,
            "CP1_1EnergyConsumed": CP1_1['CP1_1Energy'],
            "LEV4_1EnergyConsumed": LEV4_1['LEV4_1Energy'],
            "CP11_1EnergyConsumed": CP11_1['CP11_1Energy'],
            "CP12_1EnergyConsumed": CP12_1['CP12_1Energy'],
            "CP13_1EnergyConsumed": CP13_1['CP13_1Energy'],
            "CP14_1EnergyConsumed": CP14_1['CP14_1Energy'],
            "CP12_1Location":"Pond area",
            "CP13_1Location":"Pond area",
            "CP14_1Location":"Pond area",
            "LEV4_1Location":"MLCP 3rd floor",
            "CP12_1Capacity":"3.3kW",
            "CP13_1Capacity":"3.3kW",
            "CP14_1Capacity":"7kW",
            "LEV4_1Capacity":"7kW"

            # Repeat the same for other chargers...
        })

        return EVCharger_list

    except Exception as e:
        # Handle exceptions and return an HTTP 500 response
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    
 #Chillers Api   
@app.get('/Dashboard/thermal')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    thermalStorage = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("select polledTime,coolingEnergy from EMS.ThermalHourly where date(polledTime) = curdate();")
    
    res = bms_cur.fetchall()

    # print(res)

    for i in res:
        polledTime = str(i[0])[11:16]
        thermalStorage.append({'coolingEnergy':round(i[1],2),'polledTime':polledTime})
    
    bms_cur.close()
    processed_db.close()

    return thermalStorage

@app.post('/Dashboard/thermal/filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_emsdb)):
    thermalStorage = []
    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:

                bmscur.execute(f"select polledTime,coolingEnergy from EMS.ThermalHourly where date(polledTime) = '{value}';")
                
                res = bmscur.fetchall()
                
                for i in res:
                    polledTime = str(i[0])[11:16]
                    thermalStorage.append({'coolingEnergy':round(i[1],2),'polledTime':polledTime})
                
                bmscur.close()
    except mysql.connector.Error as e:
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)

    return thermalStorage


#Hot Water Storage api
@app.get('/Dashboard/HotWaterStorage')
def peak_demand_date(db: mysql.connector.connect = Depends(get_RAWemsdb)):
    HotWaterStorage_Response=[]
    try:
        processed_db = get_RAWemsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT recordtimestamp,tankBottomTemperature,tankTopTemperature,hotWaterDeliveryTemperature,hotWaterdeliveryRate,tankFuildVolume FROM EMS.HotWaterStorage where date(recordtimestamp)=curdate() order by recordtimestamp desc limit 1")
    
    res = bms_cur.fetchall()

    # print(res)

    for i in res:
        Stored_Water_Temperature=(i[1]+i[2])/2
        Delivery_Temperature=i[3]
        Hot_water_delivery_Flow_rate=i[4]
        Energy_Delivered=(Hot_water_delivery_Flow_rate*4.2*(Stored_Water_Temperature-Delivery_Temperature))
        Mass_of_stored_water=i[5]
        Refrigerant_temperature=55
        Energy_Stored=0
         
        
        HotWaterStorage_Response.append({
      "Stored_Water_Temperature":Stored_Water_Temperature,
      "Delivery_Temperature":Delivery_Temperature,
      "Hot_water_delivery_Flow_rate":Hot_water_delivery_Flow_rate,
      "Energy_Delivered":Energy_Delivered,
      "Mass_of_stored_water":Mass_of_stored_water,
      "Refrigerant_temperature":Refrigerant_temperature,
      "Energy_Stored":Energy_Stored,
    })
        
    bms_cur.close()

    return HotWaterStorage_Response


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=5002)
