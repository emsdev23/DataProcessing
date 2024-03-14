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

    # emscur.execute("SELECT timestamp,total_energy_difference FROM meterdata.diselenergyhourly where date(timestamp) = date(curdate()-1);")

    # dieselres = emscur.fetchall()

    # def writeDiesel(polledTime,Energy):
    #     sql = ("INSERT INTO EMS.buildingConsumption(polledTime,Diesel) VALUES(%s,%s)")
    #     val = (polledTime,Energy)
    #     print(val)
    #     try:
    #         emscur.execute(sql,val)
    #         emsdb.commit()
    #         print("Diesel value added")
    #     except mysql.connector.IntegrityError:
    #         sql = """UPDATE EMS.buildingConsumption SET Diesel = %s WHERE polledTime = %s"""
    #         val = (Energy,polledTime)
    #         emscur.execute(sql,val)
    #         emsdb.commit()
    #         print("Diesel value added")

    # for i in dieselres:
    #     tim = str(i[0])[0:14]+"00:00"
    #     # print(i)
    #     writeDiesel(tim,i[1])


    #---------------------------------------------------------------------------------------------------------------------

    # emscur.execute("SELECT polledTime,peakdemand FROM EMS.peakdemandHourly where date(polledTime) = date(curdate()-1);")

    # peakres = emscur.fetchall()

    # def writePeak(polledTime,PeakDemand):
    #     sql = ("INSERT INTO buildingConsumption(polledTime,peakDemand) VALUES(%s,%s)")
    #     val = (polledTime,PeakDemand)
    #     print(val)
    #     try:
    #         emscur.execute(sql,val)
    #         emsdb.commit()
    #         print("Peak Demand value added")
    #     except mysql.connector.IntegrityError:
    #         sql = """UPDATE buildingConsumption SET peakDemand = %s WHERE polledTime = %s"""
    #         val = (PeakDemand,polledTime)
    #         emscur.execute(sql,val)
    #         emsdb.commit()
    #         print("Peak Demand value added")

    # for i in peakres:
    #     # print(i)
    #     writePeak(i[0],i[1])
    
    # #---------------------------------------------------------------------------------------------------------------------

    # emscur.execute("SELECT polledTime,Energy FROM EMS.BMSgridhourly where date(polledTime) = date(curdate()-1);")

    # grid = emscur.fetchall()

    # def writebmsGrid(polledTime,Energy):
    #     sql = ("INSERT INTO buildingConsumption(polledTime,bmsgrid) VALUES(%s,%s)")
    #     val = (polledTime,Energy)
    #     print(val)
    #     try:
    #         emscur.execute(sql,val)
    #         emsdb.commit()
    #         print("BMS Grid value added")
    #     except mysql.connector.IntegrityError:
    #         sql = """UPDATE buildingConsumption SET bmsgrid = %s WHERE polledTime = %s"""
    #         val = (Energy,polledTime)
    #         emscur.execute(sql,val)
    #         emsdb.commit()
    #         print("BMS Grid value added")

    # for i in grid:
        # writebmsGrid(i[0],i[1])
    
    # #---------------------------------------------------------------------------------------------------------------------

    # emscur.execute("SELECT polledTime,Energy FROM EMS.Gridhourly where date(polledTime) = date(curdate()-1) ;")

    # grid = emscur.fetchall()

    # def writeGrid(polledTime,Energy):
    #     sql = ("INSERT INTO buildingConsumption(polledTime,gridEnergy) VALUES(%s,%s)")
    #     val = (polledTime,Energy)
    #     print(val)
    #     try:
    #         emscur.execute(sql,val)
    #         emsdb.commit()
    #         print("Grid value added")
    #     except mysql.connector.IntegrityError:
    #         sql = """UPDATE buildingConsumption SET gridEnergy = %s WHERE polledTime = %s"""
    #         val = (Energy,polledTime)
    #         emscur.execute(sql,val)
    #         emsdb.commit()
    #         print("Grid value added")

    # for i in grid:
    #     writeGrid(i[0],i[1])

    
    # #---------------------------------------------------------------------------------------------------------------------


    # emscur.execute("SELECT polledTime,Energy FROM EMS.WheeledHourly where date(polledTime) = date(curdate()-1);")

    # wheeled = emscur.fetchall()

    # def writeWheeled(polledTime,Energy):
    #     sql = ("INSERT INTO buildingConsumption(polledTime,wheeledinEnergy) VALUES(%s,%s)")
    #     val = (polledTime,Energy)
    #     print(val)
    #     try:
    #         emscur.execute(sql,val)
    #         emsdb.commit()
    #         print("Wheeled value added")
    #     except mysql.connector.IntegrityError:
    #         sql = """UPDATE buildingConsumption SET wheeledinEnergy = %s WHERE polledTime = %s"""
    #         val = (Energy,polledTime)
    #         emscur.execute(sql,val)
    #         emsdb.commit()
    #         print("Wheeled value added")

    # for i in wheeled:
    #     writeWheeled(i[0],i[1])

    #  #---------------------------------------------------------------------------------------------------------------------


    # emscur.execute("SELECT polledTime,Energy FROM EMS.roofTopHour where date(polledTime) = date(curdate()-1);")

    # rooftop = emscur.fetchall()

    # def writeRooftop(polledTime,Energy):
    #     sql = ("INSERT INTO buildingConsumption(polledTime,rooftopEnergy) VALUES(%s,%s)")
    #     val = (polledTime,Energy)
    #     try:
    #         print(val)
    #         emscur.execute(sql,val)
    #         emsdb.commit()
    #         print("Rooftop value added")
    #     except mysql.connector.IntegrityError:
    #         sql = """UPDATE buildingConsumption SET rooftopEnergy = %s WHERE polledTime = %s"""
    #         val = (Energy,polledTime)
    #         print(val)
    #         emscur.execute(sql,val)
    #         emsdb.commit()
    #         print("Rooftop value added")

    # for i in rooftop:
    #     writeRooftop(i[0],i[1])
    

    # #---------------------------------------------------------------------------------------------------------------------

    # emscur.execute("SELECT polledTime,discharhingEnergy FROM EMS.UPSbatteryHourly where date(polledTime) = date(curdate()-1) ;")

    # discharge = emscur.fetchall()

    # def writeBattery(polledTime,Energy):
    #     sql = ("INSERT INTO buildingConsumption(polledTime,batteryEnergy) VALUES(%s,%s)")
    #     val = (polledTime,Energy)
    #     print(val)
    #     try:
    #         emscur.execute(sql,val)
    #         emsdb.commit()
    #         print("Battery discharge value added")
    #     except mysql.connector.IntegrityError:
    #         sql = """UPDATE buildingConsumption SET batteryEnergy = %s WHERE polledTime = %s"""
    #         val = (Energy,polledTime)
    #         emscur.execute(sql,val)
    #         emsdb.commit()
    #         print("Battery discharge value added")

    # for i in discharge:
    #     writeBattery(i[0],i[1])
    
    #  #---------------------------------------------------------------------------------------------------------------------

    emscur.execute("SELECT recordTime,coolingEnergy FROM EMS.ThermalStorageProcessed where date(recordTime) = date(curdate()-1);")

    thermal = emscur.fetchall()

    def writeThermal(polledTime,Energy):
        sql = ("INSERT INTO buildingConsumption(polledTime,thermalDischarge) VALUES(%s,%s)")
        val = (polledTime,Energy)
        print(val)
        try:
            emscur.execute(sql,val)
            emsdb.commit()
            print("Thermal discharge value added")
        except mysql.connector.IntegrityError:
            sql = """UPDATE buildingConsumption SET thermalDischarge = %s WHERE polledTime = %s"""
            val = (Energy,polledTime)
            emscur.execute(sql,val)
            emsdb.commit()
            print("Thermal discharge value added")

    for i in thermal:
        writeThermal(i[0],i[1])

    
     #---------------------------------------------------------------------------------------------------------------------
        
    
    # emscur.execute("SELECT polledTime,Energy FROM EMS.DGHourly where date(polledTime) >= date_sub(curdate(),interval 1 DAY);")

    # diesel = emscur.fetchall()

    # def writeDiesel(polledTime,Energy):
    #     sql = ("INSERT INTO buildingConsumption(polledTime,diesel) VALUES(%s,%s)")
    #     val = (polledTime,Energy)
    #     print(val)
    #     try:
    #         emscur.execute(sql,val)
    #         emsdb.commit()
    #         print("Diesel value added")
    #     except mysql.connector.IntegrityError:
    #         sql = """UPDATE buildingConsumption SET diesel = %s WHERE polledTime = %s"""
    #         val = (Energy,polledTime)
    #         emscur.execute(sql,val)
    #         emsdb.commit()
    #         print("Diesel value added")

    # for i in diesel:
    #     writeDiesel(i[0],i[1])


    time.sleep(600)