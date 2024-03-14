import mysql.connector
import time

while True:
    emsdb = mysql.connector.connect(
            host="43.205.196.66",
            user="emsroot",
            password="22@teneT",
            database='EMS',
            port=3307
        )

    emscur = emsdb.cursor()

    emscur.execute("select polledTime,gridEnergy,rooftopEnergy,wheeledinEnergy,bmsgrid,diesel from EMS.buildingConsumption where date(polledTime) = '2024-03-02'")

    dayres = emscur.fetchall()
    
    grid_val = 0
    wheeled_val = 0
    bms_val = 0
    grid_dict = {}
    roof_dict = {}
    wheel_dict = {}
    bms_dict = {}
    dg_dict = {}

    def SegDay(polledTime,grid,rooftop,wheeledin,bmsgrid,diesel):
        polledDate = str(polledTime)[0:10]

        if bmsgrid == None:
            bmsgrid = 0
        if diesel == None:
            diesel = 0
        if grid == None:
            grid = 0
        if rooftop == None:
            rooftop = 0
        if wheeledin == None:
            wheeledin = 0

        if polledDate in grid_dict.keys():
            grid_dict[polledDate] += grid
        else:
            grid_dict[polledDate] = grid
        
        if polledDate in roof_dict.keys():
            roof_dict[polledDate] += rooftop
        else:
            roof_dict[polledDate] = rooftop
        
        if polledDate in wheel_dict.keys():
            wheel_dict[polledDate] += wheeledin
        else:
            wheel_dict[polledDate] = wheeledin
        
        if polledDate in bms_dict.keys():
            bms_dict[polledDate] += bmsgrid
        else:
            bms_dict[polledDate] = bmsgrid
        
        if polledDate in dg_dict.keys():
            dg_dict[polledDate] += diesel
        else:
            dg_dict[polledDate] = diesel

    for i in dayres:
        SegDay(i[0],i[1],i[2],i[3],i[4],i[5])

    
    for i in wheel_dict.keys():
        val = (i,wheel_dict[i])
        sql = "INSERT INTO EMS.buidingConsumptionDayWise(polledDate,wheeledinEnergy) VALUES(%s,%s)"
        try:
            emscur.execute(sql,val)
            print(val)
            emsdb.commit()
            print("Wheeled Daywise")
        except mysql.connector.IntegrityError:
            sql = "UPDATE EMS.buidingConsumptionDayWise SET wheeledinEnergy = %s WHERE polledDate = %s"
            val = (wheel_dict[i],i)
            emscur.execute(sql,val)
            print(val)
            emsdb.commit()
            print("Wheeled Daywise")

    for i in grid_dict.keys():
        grid_val = grid_dict[i] - wheel_dict[i]
        val = (i,grid_val)
        sql = "INSERT INTO EMS.buidingConsumptionDayWise(polledDate,gridEnergy) VALUES(%s,%s)"
        try:
            emscur.execute(sql,val)
            print(val)
            emsdb.commit()
            print("Grid Daywise")
        except mysql.connector.IntegrityError:
            sql = "UPDATE EMS.buidingConsumptionDayWise SET gridEnergy = %s WHERE polledDate = %s"
            val = (grid_val,i)
            emscur.execute(sql,val)
            print(val)
            emsdb.commit()
            print("Grid Daywise")
    
    for i in bms_dict.keys():
        bms_val = bms_dict[i] - wheel_dict[i]
        val = (i,bms_val)
        sql = "INSERT INTO EMS.buidingConsumptionDayWise(polledDate,bmsgrid) VALUES(%s,%s)"
        try:
            emscur.execute(sql,val)
            print(val)
            emsdb.commit()
            print("BMS Grid Daywise")
        except mysql.connector.IntegrityError:
            sql = "UPDATE EMS.buidingConsumptionDayWise SET bmsgrid = %s WHERE polledDate = %s"
            val = (bms_val,i)
            emscur.execute(sql,val)
            print(val)
            emsdb.commit()
            print("BMS Grid Daywise")
    
    for i in roof_dict.keys():
        val = (i,roof_dict[i])
        sql = "INSERT INTO EMS.buidingConsumptionDayWise(polledDate,rooftopEnergy) VALUES(%s,%s)"
        try:
            emscur.execute(sql,val)
            print(val)
            emsdb.commit()
            print("Rooftop Daywise")
        except mysql.connector.IntegrityError:
            sql = "UPDATE EMS.buidingConsumptionDayWise SET rooftopEnergy = %s WHERE polledDate = %s"
            val = (roof_dict[i],i)
            emscur.execute(sql,val)
            print(val)
            emsdb.commit()
            print("Rooftop Daywise")

    for i in dg_dict.keys():
        val = (i,dg_dict[i])
        sql = "INSERT INTO EMS.buidingConsumptionDayWise(polledDate,deisel) VALUES(%s,%s)"
        try:
            emscur.execute(sql,val)
            print(val)
            emsdb.commit()
            print("Diesel Daywise")
        except mysql.connector.IntegrityError:
            sql = "UPDATE EMS.buidingConsumptionDayWise SET deisel = %s WHERE polledDate = %s"
            val = (dg_dict[i],i)
            emscur.execute(sql,val)
            print(val)
            emsdb.commit()
            print("Diesel Daywise")
    
    emscur.close()

    time.sleep(500)