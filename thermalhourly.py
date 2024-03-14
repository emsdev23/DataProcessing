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

    try:
        unprocesseddb = mysql.connector.connect(
		host="121.242.232.151",
		user="emsrouser",
		password="emsrouser@151",
		database='bmsmgmtprodv13',
		port=3306
	      )
    except:
         print("ERROR")
         
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
        unproscur.execute("""select polledTime,coolingEnergyConsumption,tsOutletADPvalveStatus,tsOutletBDPvalveStatus,HValve from thermalStorageMQTTReadings where Date(polledTime) = date(curdate()-1) order by polledTime asc ;""")
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
    
    time.sleep(100)