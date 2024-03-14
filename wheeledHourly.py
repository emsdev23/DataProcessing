import time
import mysql.connector
from decimal import Decimal
from datetime import datetime, timedelta


while True:
    rad = {}
    radhr = {}
    hourly_wheeled= {}

    emsdb = mysql.connector.connect(
                        host="43.205.196.66",
                        user="emsroot",
                        password="22@teneT",
                        database='EMS',
                        port=3307
                    )
    
    processeddb = mysql.connector.connect(
                host="121.242.232.151",
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

    proscur.execute("select createdTime,ctsedogenergy from bmsmgmtprodv13.ctsedogreadings where date(createdTime) >= date(curdate()-1);")

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

    emscur.execute("""select wmstimestamp,CAST(wmsirradiation AS DECIMAL(18, 2)) from EMS.EMSWMSData WHERE DATE(wmstimestamp) = date(curdate()-1);""")
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
    
    proscur.close()

    time.sleep(100)