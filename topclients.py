import mysql.connector
import time

while True:

    try:
        bmsdb = mysql.connector.connect(
                    host="121.242.232.151",
                    user="bmsrouser6",
                    password="bmsrouser6@151U",
                    database='bmsmgmtprodv13',
                    port=3306
                )
        
        bmscur = bmsdb.cursor()
    except Exception as ex:
        print(ex)
        time.sleep(10)
        continue

    try:
        awsdb = mysql.connector.connect(
                            host="43.205.196.66",
                            user="emsroot",
                            password="22@teneT",
                            database='EMS',
                            port=3307
                        )
        
        awscur = awsdb.cursor()
    except Exception as ex:
        print(ex)
        time.sleep(10)
        continue

    bmscur.execute("SELECT acmetersubsystemid,acmeterenergy,acmeterpolledtimestamp FROM bmsmgmtprodv13.acmeterreadings where date(acmeterpolledtimestamp) >= DATE_SUB(NOW(),INTERVAL 365 DAY);")

    clients = bmscur.fetchall()

    # print(clients)

    # ginger1197 = {}
    ginger1137 = {}

    caterpillar115 = {}
    caterpillar116 = {}

    tcs674 = {}
    tcs675 = {}


    def SegClients(id,Energy,polledTime):
        polledTime = str(polledTime)

        # if id == 1017: #or id == 1016 or id == 993 or id == 992 or id == 114 or id == 112 or id == 47 or id == 34 or id == 33:
        #     polledTime = str(polledTime)[0:17]+"00"
        #     minte = int(polledTime[14:16])

        #     if minte >=0 and minte <15:
        #         polledTime = polledTime[0:14]+"00:00"
        #         # print(polledTime)
        #     if minte >=15 and minte <30:
        #         polledTime = polledTime[0:14]+"15:00"
        #         # print(polledTime)
        #     if minte >=30 and minte <45:
        #         polledTime = polledTime[0:14]+"30:00"
        #         # print(polledTime)
        #     if minte >=45 and minte <=59:
        #         polledTime = polledTime[0:14]+"45:00"
        #         # print(polledTime)
            
        #     if polledTime not in arci1017.keys():
        #         arci1017[polledTime] = Energy
        #     else:
        #         arci1017[polledTime] += Energy
        #     # print('ARCI',id,Energy,polledTime)
        
        # if id == 1016: #or id == 1016 or id == 993 or id == 992 or id == 114 or id == 112 or id == 47 or id == 34 or id == 33:
        #     polledTime = str(polledTime)[0:17]+"00"
        #     minte = int(polledTime[14:16])

        #     if minte >=0 and minte <15:
        #         polledTime = polledTime[0:14]+"00:00"
        #         # print(polledTime)
        #     if minte >=15 and minte <30:
        #         polledTime = polledTime[0:14]+"15:00"
        #         # print(polledTime)
        #     if minte >=30 and minte <45:
        #         polledTime = polledTime[0:14]+"30:00"
        #         # print(polledTime)
        #     if minte >=45 and minte <=59:
        #         polledTime = polledTime[0:14]+"45:00"
        #         # print(polledTime)
            
        #     if polledTime not in arci1016.keys():
        #         arci1016[polledTime] = Energy
        #     else:
        #         arci1016[polledTime] += Energy
        
        # if id == 993: #or id == 1016 or id == 993 or id == 992 or id == 114 or id == 112 or id == 47 or id == 34 or id == 33:
        #     polledTime = str(polledTime)[0:17]+"00"
        #     minte = int(polledTime[14:16])

        #     if minte >=0 and minte <15:
        #         polledTime = polledTime[0:14]+"00:00"
        #         # print(polledTime)
        #     if minte >=15 and minte <30:
        #         polledTime = polledTime[0:14]+"15:00"
        #         # print(polledTime)
        #     if minte >=30 and minte <45:
        #         polledTime = polledTime[0:14]+"30:00"
        #         # print(polledTime)
        #     if minte >=45 and minte <=59:
        #         polledTime = polledTime[0:14]+"45:00"
        #         # print(polledTime)
            
        #     if polledTime not in arci993.keys():
        #         arci993[polledTime] = Energy
        #     else:
        #         arci993[polledTime] += Energy
        
        # if id == 992: #or id == 1016 or id == 993 or id == 992 or id == 114 or id == 112 or id == 47 or id == 34 or id == 33:
        #     polledTime = str(polledTime)[0:17]+"00"
        #     minte = int(polledTime[14:16])

        #     if minte >=0 and minte <15:
        #         polledTime = polledTime[0:14]+"00:00"
        #         # print(polledTime)
        #     if minte >=15 and minte <30:
        #         polledTime = polledTime[0:14]+"15:00"
        #         # print(polledTime)
        #     if minte >=30 and minte <45:
        #         polledTime = polledTime[0:14]+"30:00"
        #         # print(polledTime)
        #     if minte >=45 and minte <=59:
        #         polledTime = polledTime[0:14]+"45:00"
        #         # print(polledTime)
            
        #     if polledTime not in arci992.keys():
        #         arci992[polledTime] = Energy
        #     else:
        #         arci992[polledTime] += Energy
        
        # if id == 114: #or id == 1016 or id == 993 or id == 992 or id == 114 or id == 112 or id == 47 or id == 34 or id == 33:
        #     polledTime = str(polledTime)[0:17]+"00"
        #     minte = int(polledTime[14:16])

        #     if minte >=0 and minte <15:
        #         polledTime = polledTime[0:14]+"00:00"
        #         # print(polledTime)
        #     if minte >=15 and minte <30:
        #         polledTime = polledTime[0:14]+"15:00"
        #         # print(polledTime)
        #     if minte >=30 and minte <45:
        #         polledTime = polledTime[0:14]+"30:00"
        #         # print(polledTime)
        #     if minte >=45 and minte <=59:
        #         polledTime = polledTime[0:14]+"45:00"
        #         # print(polledTime)
            
        #     if polledTime not in arci114.keys():
        #         arci114[polledTime] = Energy
        #     else:
        #         arci114[polledTime] += Energy
        
        # if id == 112: #or id == 1016 or id == 993 or id == 992 or id == 114 or id == 112 or id == 47 or id == 34 or id == 33:
        #     polledTime = str(polledTime)[0:17]+"00"
        #     minte = int(polledTime[14:16])

        #     if minte >=0 and minte <15:
        #         polledTime = polledTime[0:14]+"00:00"
        #         # print(polledTime)
        #     if minte >=15 and minte <30:
        #         polledTime = polledTime[0:14]+"15:00"
        #         # print(polledTime)
        #     if minte >=30 and minte <45:
        #         polledTime = polledTime[0:14]+"30:00"
        #         # print(polledTime)
        #     if minte >=45 and minte <=59:
        #         polledTime = polledTime[0:14]+"45:00"
        #         # print(polledTime)
            
        #     if polledTime not in arci112.keys():
        #         arci112[polledTime] = Energy
        #     else:
        #         arci112[polledTime] += Energy
        
        # if id == 47: #or id == 1016 or id == 993 or id == 992 or id == 114 or id == 112 or id == 47 or id == 34 or id == 33:
        #     polledTime = str(polledTime)[0:17]+"00"
        #     minte = int(polledTime[14:16])

        #     if minte >=0 and minte <15:
        #         polledTime = polledTime[0:14]+"00:00"
        #         # print(polledTime)
        #     if minte >=15 and minte <30:
        #         polledTime = polledTime[0:14]+"15:00"
        #         # print(polledTime)
        #     if minte >=30 and minte <45:
        #         polledTime = polledTime[0:14]+"30:00"
        #         # print(polledTime)
        #     if minte >=45 and minte <=59:
        #         polledTime = polledTime[0:14]+"45:00"
        #         # print(polledTime)
            
        #     if polledTime not in arci47.keys():
        #         arci47[polledTime] = Energy
        #     else:
        #         arci47[polledTime] += Energy
            
        # if id == 34: #or id == 1016 or id == 993 or id == 992 or id == 114 or id == 112 or id == 47 or id == 34 or id == 33:
        #     polledTime = str(polledTime)[0:17]+"00"
        #     minte = int(polledTime[14:16])

        #     if minte >=0 and minte <15:
        #         polledTime = polledTime[0:14]+"00:00"
        #         # print(polledTime)
        #     if minte >=15 and minte <30:
        #         polledTime = polledTime[0:14]+"15:00"
        #         # print(polledTime)
        #     if minte >=30 and minte <45:
        #         polledTime = polledTime[0:14]+"30:00"
        #         # print(polledTime)
        #     if minte >=45 and minte <=59:
        #         polledTime = polledTime[0:14]+"45:00"
        #         # print(polledTime)
            
        #     if polledTime not in arci34.keys():
        #         arci34[polledTime] = Energy
        #     else:
        #         arci34[polledTime] += Energy
    
        # if id == 33: #or id == 1016 or id == 993 or id == 992 or id == 114 or id == 112 or id == 47 or id == 34 or id == 33:
        #     polledTime = str(polledTime)[0:17]+"00"
        #     minte = int(polledTime[14:16])

        #     if minte >=0 and minte <15:
        #         polledTime = polledTime[0:14]+"00:00"
        #         # print(polledTime)
        #     if minte >=15 and minte <30:
        #         polledTime = polledTime[0:14]+"15:00"
        #         # print(polledTime)
        #     if minte >=30 and minte <45:
        #         polledTime = polledTime[0:14]+"30:00"
        #         # print(polledTime)
        #     if minte >=45 and minte <=59:
        #         polledTime = polledTime[0:14]+"45:00"
        #         # print(polledTime)
            
        #     if polledTime not in arci33.keys():
        #         arci33[polledTime] = Energy
        #     else:
        #         arci33[polledTime] += Energy

        # if id == 933: #id == 1175 or id == 1295 or id == 1292:
        #     # polledTime = str(polledTime)[0:17]+"00"
        #     minte = int(polledTime[14:16])

        #     if minte >=0 and minte <15:
        #         polledTime = polledTime[0:14]+"00:00"
        #         # print(polledTime)
        #     if minte >=15 and minte <30:
        #         polledTime = polledTime[0:14]+"15:00"
        #         # print(polledTime)
        #     if minte >=30 and minte <45:
        #         polledTime = polledTime[0:14]+"30:00"
        #         # print(polledTime)
        #     if minte >=45 and minte <=59:
        #         polledTime = polledTime[0:14]+"45:00"

        #     if polledTime  not in sgri933.keys():
        #         sgri933[polledTime] = Energy
        #     else:
        #         sgri933[polledTime] += Energy
        
        # if id == 932: #id == 1175 or id == 1295 or id == 1292:
        #     # polledTime = str(polledTime)[0:17]+"00"
        #     minte = int(polledTime[14:16])

        #     if minte >=0 and minte <15:
        #         polledTime = polledTime[0:14]+"00:00"
        #         # print(polledTime)
        #     if minte >=15 and minte <30:
        #         polledTime = polledTime[0:14]+"15:00"
        #         # print(polledTime)
        #     if minte >=30 and minte <45:
        #         polledTime = polledTime[0:14]+"30:00"
        #         # print(polledTime)
        #     if minte >=45 and minte <=59:
        #         polledTime = polledTime[0:14]+"45:00"

        #     if polledTime  not in sgri932.keys():
        #         sgri932[polledTime] = Energy
        #     else:
        #         sgri932[polledTime] += Energy
        
        # if id == 931: #id == 1175 or id == 1295 or id == 1292:
        #     # polledTime = str(polledTime)[0:17]+"00"
        #     minte = int(polledTime[14:16])

        #     if minte >=0 and minte <15:
        #         polledTime = polledTime[0:14]+"00:00"
        #         # print(polledTime)
        #     if minte >=15 and minte <30:
        #         polledTime = polledTime[0:14]+"15:00"
        #         # print(polledTime)
        #     if minte >=30 and minte <45:
        #         polledTime = polledTime[0:14]+"30:00"
        #         # print(polledTime)
        #     if minte >=45 and minte <=59:
        #         polledTime = polledTime[0:14]+"45:00"

        #     if polledTime  not in sgri931.keys():
        #         sgri931[polledTime] = Energy
        #     else:
        #         sgri931[polledTime] += Energy
        
        # if id == 930: #id == 1175 or id == 1295 or id == 1292:
        #     # polledTime = str(polledTime)[0:17]+"00"
        #     minte = int(polledTime[14:16])

        #     if minte >=0 and minte <15:
        #         polledTime = polledTime[0:14]+"00:00"
        #         # print(polledTime)
        #     if minte >=15 and minte <30:
        #         polledTime = polledTime[0:14]+"15:00"
        #         # print(polledTime)
        #     if minte >=30 and minte <45:
        #         polledTime = polledTime[0:14]+"30:00"
        #         # print(polledTime)
        #     if minte >=45 and minte <=59:
        #         polledTime = polledTime[0:14]+"45:00"

        #     if polledTime  not in sgri930.keys():
        #         sgri930[polledTime] = Energy
        #     else:
        #         sgri930[polledTime] += Energy
        
        # if id == 929: #id == 1175 or id == 1295 or id == 1292:
        #     # polledTime = str(polledTime)[0:17]+"00"
        #     minte = int(polledTime[14:16])

        #     if minte >=0 and minte <15:
        #         polledTime = polledTime[0:14]+"00:00"
        #         # print(polledTime)
        #     if minte >=15 and minte <30:
        #         polledTime = polledTime[0:14]+"15:00"
        #         # print(polledTime)
        #     if minte >=30 and minte <45:
        #         polledTime = polledTime[0:14]+"30:00"
        #         # print(polledTime)
        #     if minte >=45 and minte <=59:
        #         polledTime = polledTime[0:14]+"45:00"

        #     if polledTime  not in sgri929.keys():
        #         sgri929[polledTime] = Energy
        #     else:
        #         sgri929[polledTime] += Energy
        
        # if id == 928: #id == 1175 or id == 1295 or id == 1292:
        #     # polledTime = str(polledTime)[0:17]+"00"
        #     minte = int(polledTime[14:16])

        #     if minte >=0 and minte <15:
        #         polledTime = polledTime[0:14]+"00:00"
        #         # print(polledTime)
        #     if minte >=15 and minte <30:
        #         polledTime = polledTime[0:14]+"15:00"
        #         # print(polledTime)
        #     if minte >=30 and minte <45:
        #         polledTime = polledTime[0:14]+"30:00"
        #         # print(polledTime)
        #     if minte >=45 and minte <=59:
        #         polledTime = polledTime[0:14]+"45:00"

        #     if polledTime  not in sgri928.keys():
        #         sgri928[polledTime] = Energy
        #     else:
        #         sgri928[polledTime] += Energy
        
        # if id == 927: #id == 1175 or id == 1295 or id == 1292:
        #     # polledTime = str(polledTime)[0:17]+"00"
        #     minte = int(polledTime[14:16])

        #     if minte >=0 and minte <15:
        #         polledTime = polledTime[0:14]+"00:00"
        #         # print(polledTime)
        #     if minte >=15 and minte <30:
        #         polledTime = polledTime[0:14]+"15:00"
        #         # print(polledTime)
        #     if minte >=30 and minte <45:
        #         polledTime = polledTime[0:14]+"30:00"
        #         # print(polledTime)
        #     if minte >=45 and minte <=59:
        #         polledTime = polledTime[0:14]+"45:00"

        #     if polledTime  not in sgri927.keys():
        #         sgri927[polledTime] = Energy
        #     else:
        #         sgri927[polledTime] += Energy
        
        # if id == 520: #id == 1175 or id == 1295 or id == 1292:
        #     # polledTime = str(polledTime)[0:17]+"00"
        #     minte = int(polledTime[14:16])

        #     if minte >=0 and minte <15:
        #         polledTime = polledTime[0:14]+"00:00"
        #         # print(polledTime)
        #     if minte >=15 and minte <30:
        #         polledTime = polledTime[0:14]+"15:00"
        #         # print(polledTime)
        #     if minte >=30 and minte <45:
        #         polledTime = polledTime[0:14]+"30:00"
        #         # print(polledTime)
        #     if minte >=45 and minte <=59:
        #         polledTime = polledTime[0:14]+"45:00"

        #     if polledTime  not in sgri520.keys():
        #         sgri520[polledTime] = Energy
        #     else:
        #         sgri520[polledTime] += Energy
        
        # if id == 519: #id == 1175 or id == 1295 or id == 1292:
        #     # polledTime = str(polledTime)[0:17]+"00"
        #     minte = int(polledTime[14:16])

        #     if minte >=0 and minte <15:
        #         polledTime = polledTime[0:14]+"00:00"
        #         # print(polledTime)
        #     if minte >=15 and minte <30:
        #         polledTime = polledTime[0:14]+"15:00"
        #         # print(polledTime)
        #     if minte >=30 and minte <45:
        #         polledTime = polledTime[0:14]+"30:00"
        #         # print(polledTime)
        #     if minte >=45 and minte <=59:
        #         polledTime = polledTime[0:14]+"45:00"

        #     if polledTime  not in sgri519.keys():
        #         sgri519[polledTime] = Energy
        #     else:
        #         sgri519[polledTime] += Energy
        
        # if id == 518: #id == 1175 or id == 1295 or id == 1292:
        #     # polledTime = str(polledTime)[0:17]+"00"
        #     minte = int(polledTime[14:16])

        #     if minte >=0 and minte <15:
        #         polledTime = polledTime[0:14]+"00:00"
        #         # print(polledTime)
        #     if minte >=15 and minte <30:
        #         polledTime = polledTime[0:14]+"15:00"
        #         # print(polledTime)
        #     if minte >=30 and minte <45:
        #         polledTime = polledTime[0:14]+"30:00"
        #         # print(polledTime)
        #     if minte >=45 and minte <=59:
        #         polledTime = polledTime[0:14]+"45:00"

        #     if polledTime  not in sgri518.keys():
        #         sgri518[polledTime] = Energy
        #     else:
        #         sgri518[polledTime] += Energy
        
        # if id == 517: #id == 1175 or id == 1295 or id == 1292:
        #     # polledTime = str(polledTime)[0:17]+"00"
        #     minte = int(polledTime[14:16])

        #     if minte >=0 and minte <15:
        #         polledTime = polledTime[0:14]+"00:00"
        #         # print(polledTime)
        #     if minte >=15 and minte <30:
        #         polledTime = polledTime[0:14]+"15:00"
        #         # print(polledTime)
        #     if minte >=30 and minte <45:
        #         polledTime = polledTime[0:14]+"30:00"
        #         # print(polledTime)
        #     if minte >=45 and minte <=59:
        #         polledTime = polledTime[0:14]+"45:00"

        #     if polledTime  not in sgri517.keys():
        #         sgri517[polledTime] = Energy
        #     else:
        #         sgri517[polledTime] += Energy
        
        # if id == 516: #id == 1175 or id == 1295 or id == 1292:
        #     # polledTime = str(polledTime)[0:17]+"00"
        #     minte = int(polledTime[14:16])

        #     if minte >=0 and minte <15:
        #         polledTime = polledTime[0:14]+"00:00"
        #         # print(polledTime)
        #     if minte >=15 and minte <30:
        #         polledTime = polledTime[0:14]+"15:00"
        #         # print(polledTime)
        #     if minte >=30 and minte <45:
        #         polledTime = polledTime[0:14]+"30:00"
        #         # print(polledTime)
        #     if minte >=45 and minte <=59:
        #         polledTime = polledTime[0:14]+"45:00"

        #     if polledTime  not in sgri516.keys():
        #         sgri516[polledTime] = Energy
        #     else:
        #         sgri516[polledTime] += Energy
        
        # if id == 514: #id == 1175 or id == 1295 or id == 1292:
        #     # polledTime = str(polledTime)[0:17]+"00"
        #     minte = int(polledTime[14:16])

        #     if minte >=0 and minte <15:
        #         polledTime = polledTime[0:14]+"00:00"
        #         # print(polledTime)
        #     if minte >=15 and minte <30:
        #         polledTime = polledTime[0:14]+"15:00"
        #         # print(polledTime)
        #     if minte >=30 and minte <45:
        #         polledTime = polledTime[0:14]+"30:00"
        #         # print(polledTime)
        #     if minte >=45 and minte <=59:
        #         polledTime = polledTime[0:14]+"45:00"

        #     if polledTime  not in sgri515.keys():
        #         sgri515[polledTime] = Energy
        #     else:
        #         sgri515[polledTime] += Energy
        
        # if id == 519: #id == 1175 or id == 1295 or id == 1292:
        #     # polledTime = str(polledTime)[0:17]+"00"
        #     minte = int(polledTime[14:16])

        #     if minte >=0 and minte <15:
        #         polledTime = polledTime[0:14]+"00:00"
        #         # print(polledTime)
        #     if minte >=15 and minte <30:
        #         polledTime = polledTime[0:14]+"15:00"
        #         # print(polledTime)
        #     if minte >=30 and minte <45:
        #         polledTime = polledTime[0:14]+"30:00"
        #         # print(polledTime)
        #     if minte >=45 and minte <=59:
        #         polledTime = polledTime[0:14]+"45:00"

        #     if polledTime  not in sgri514.keys():
        #         sgri514[polledTime] = Energy
        #     else:
        #         sgri514[polledTime] += Energy
        
        # if id == 513: #id == 1175 or id == 1295 or id == 1292:
        #     # polledTime = str(polledTime)[0:17]+"00"
        #     minte = int(polledTime[14:16])

        #     if minte >=0 and minte <15:
        #         polledTime = polledTime[0:14]+"00:00"
        #         # print(polledTime)
        #     if minte >=15 and minte <30:
        #         polledTime = polledTime[0:14]+"15:00"
        #         # print(polledTime)
        #     if minte >=30 and minte <45:
        #         polledTime = polledTime[0:14]+"30:00"
        #         # print(polledTime)
        #     if minte >=45 and minte <=59:
        #         polledTime = polledTime[0:14]+"45:00"

        #     if polledTime  not in sgri513.keys():
        #         sgri513[polledTime] = Energy
        #     else:
        #         sgri513[polledTime] += Energy
        
        # if id == 418: #id == 1175 or id == 1295 or id == 1292:
        #     # polledTime = str(polledTime)[0:17]+"00"
        #     minte = int(polledTime[14:16])

        #     if minte >=0 and minte <15:
        #         polledTime = polledTime[0:14]+"00:00"
        #         # print(polledTime)
        #     if minte >=15 and minte <30:
        #         polledTime = polledTime[0:14]+"15:00"
        #         # print(polledTime)
        #     if minte >=30 and minte <45:
        #         polledTime = polledTime[0:14]+"30:00"
        #         # print(polledTime)
        #     if minte >=45 and minte <=59:
        #         polledTime = polledTime[0:14]+"45:00"

        #     if polledTime  not in sgri418.keys():
        #         sgri418[polledTime] = Energy
        #     else:
        #         sgri418[polledTime] += Energy

        # if id == 933 or id == 932 or id == 931 or id == 930 or id == 929 or id == 928 or id == 927 or id == 520 or id == 519 or id == 518 or id == 517 or id == 516 or id == 515 or id == 514 or id == 513 or id == 418:
        #     # polledTime = str(polledTime)[0:17]+"00"
        #     if polledTime not in sgri.keys():
        #         sgri[polledTime] = Energy
        #     else:
        #         sgri[polledTime] += Energy
        #     # print('SGRI',id,Energy,polledTime)
        
        if id == 1137:
            minte = int(polledTime[14:16])

            if minte >=0 and minte <15:
                polledTime = polledTime[0:14]+"00:00"
                # print(polledTime)
            if minte >=15 and minte <30:
                polledTime = polledTime[0:14]+"15:00"
                # print(polledTime)
            if minte >=30 and minte <45:
                polledTime = polledTime[0:14]+"30:00"
                # print(polledTime)
            if minte >=45 and minte <=59:
                polledTime = polledTime[0:14]+"45:00"
            # polledTime = str(polledTime)[0:17]+"00"
            if polledTime not in ginger1137.keys():
                ginger1137[polledTime] = Energy
            else:
                ginger1137[polledTime] += Energy
            # print('Ginger',id,Energy,polledTime)
   
        
        if id == 115:
            minte = int(polledTime[14:16])

            if minte >=0 and minte <15:
                polledTime = polledTime[0:14]+"00:00"
                # print(polledTime)
            if minte >=15 and minte <30:
                polledTime = polledTime[0:14]+"15:00"
                # print(polledTime)
            if minte >=30 and minte <45:
                polledTime = polledTime[0:14]+"30:00"
                # print(polledTime)
            if minte >=45 and minte <=59:
                polledTime = polledTime[0:14]+"45:00"

            # polledTime = str(polledTime)[0:17]+"00"
            if polledTime not in caterpillar115.keys():
                caterpillar115[polledTime] = Energy
            else:
                caterpillar115[polledTime] += Energy
            # print('Caterpillar',id,Energy,polledTime)
        
        if id == 116:
            minte = int(polledTime[14:16])

            if minte >=0 and minte <15:
                polledTime = polledTime[0:14]+"00:00"
                # print(polledTime)
            if minte >=15 and minte <30:
                polledTime = polledTime[0:14]+"15:00"
                # print(polledTime)
            if minte >=30 and minte <45:
                polledTime = polledTime[0:14]+"30:00"
                # print(polledTime)
            if minte >=45 and minte <=59:
                polledTime = polledTime[0:14]+"45:00"
                
            # polledTime = str(polledTime)[0:17]+"00"
            if polledTime not in caterpillar116.keys():
                caterpillar116[polledTime] = Energy
            else:
                caterpillar116[polledTime] += Energy
            # print('Caterpillar',id,Energy,polledTime)
        
        # if id == 122 or id == 123 or id == 125 or id == 685 or id == 1400:
        #     # polledTime = str(polledTime)[0:17]+"00"
        #     if polledTime not in ifmr.keys():
        #         ifmr[polledTime] = Energy
        #     else:
        #         ifmr[polledTime] += Energy
        #     # print('IFMR',id,Energy,polledTime)
        
        # if id == 78 or id == 79 or id == 80 or id == 81 or id == 1446:
        #     # polledTime = str(polledTime)[0:17]+"00"
        #     if polledTime not in nms.keys():
        #         nms[polledTime] = Energy
        #     else:
        #         nms[polledTime] += Energy
            # print('NMS',id,Energy,polledTime)
        
        if id == 674:
            # polledTime = str(polledTime)[0:17]+"00"
            minte = int(polledTime[14:16])

            if minte >=0 and minte <15:
                polledTime = polledTime[0:14]+"00:00"
                # print(polledTime)
            if minte >=15 and minte <30:
                polledTime = polledTime[0:14]+"15:00"
                # print(polledTime)
            if minte >=30 and minte <45:
                polledTime = polledTime[0:14]+"30:00"
                # print(polledTime)
            if minte >=45 and minte <=59:
                polledTime = polledTime[0:14]+"45:00"
            if polledTime not in tcs674.keys():
                tcs674[polledTime] = Energy
            else:
                tcs674[polledTime] += Energy
            # print('TCS',id,Energy,polledTime)
        
        if id == 675:
            # polledTime = str(polledTime)[0:17]+"00"
            minte = int(polledTime[14:16])

            if minte >=0 and minte <15:
                polledTime = polledTime[0:14]+"00:00"
                # print(polledTime)
            if minte >=15 and minte <30:
                polledTime = polledTime[0:14]+"15:00"
                # print(polledTime)
            if minte >=30 and minte <45:
                polledTime = polledTime[0:14]+"30:00"
                # print(polledTime)
            if minte >=45 and minte <=59:
                polledTime = polledTime[0:14]+"45:00"

            if polledTime not in tcs675.keys():
                tcs675[polledTime] = Energy
            else:
                tcs675[polledTime] += Energy
            # print('TCS',id,Energy,polledTime)


    for i in clients:
        SegClients(i[0],i[1]/1000,i[2])
        # print(i[0],i[1]/1000,i[2])  

        
    def gingerWrite(id,polledTime,Energy):
        if id == '1197':
            sql = "INSERT INTO EMS.gingerClient(id1197,polledTime) VALUES(%s,%s)"
            val = (Energy,polledTime)
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("ID 1197 inserted")
            except mysql.connector.errors.IntegrityError:
                try:
                    sql = "UPDATE EMS.gingerClient set id1197 = %s where polledTime = %s"
                    val = (Energy,polledTime)
                    awscur.execute(sql,val)
                    awsdb.commit()
                    print(val)
                    print("ID 1197 updated")
                except mysql.connector.Error as err:
                    print(f"Error: {err}")
        
        if id == '1137':
            sql = "INSERT INTO EMS.gingerClient(id1137,polledTime) VALUES(%s,%s)"
            val = (Energy,polledTime)
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("ID 1137 inserted")
            except mysql.connector.errors.IntegrityError:
                try:
                    sql = "UPDATE EMS.gingerClient set id1137 = %s where polledTime = %s"
                    val = (Energy,polledTime)
                    awscur.execute(sql,val)
                    awsdb.commit()
                    print(val)
                    print("ID 1137 updated")
                except mysql.connector.Error as err:
                    print(f"Error: {err}") 
    
    def tcsWrite(id,polledTime,Energy):
        if id == '674':
            sql = "INSERT INTO EMS.tcsClient(id675,polledTime) VALUES(%s,%s)"
            val = (Energy,polledTime)
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("ID 674 inserted")
            except mysql.connector.errors.IntegrityError:
                try:
                    sql = "UPDATE EMS.tcsClient set id675 = %s where polledTime = %s"
                    val = (Energy,polledTime)
                    awscur.execute(sql,val)
                    awsdb.commit()
                    print(val)
                    print("ID 674 updated")
                except mysql.connector.Error as err:
                    print(f"Error: {err}") 
        
        if id == '675':
            sql = "INSERT INTO EMS.tcsClient(id675,polledTime) VALUES(%s,%s)"
            val = (Energy,polledTime)
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("ID 675 inserted")
            except mysql.connector.errors.IntegrityError:
                try:
                    sql = "UPDATE EMS.tcsClient set id675 = %s where polledTime = %s"
                    val = (Energy,polledTime)
                    awscur.execute(sql,val)
                    awsdb.commit()
                    print(val)
                    print("ID 675 updated")
                except mysql.connector.Error as err:
                    print(f"Error: {err}") 
    
    def caterpillarWrite(id,polledTime,Energy):
        if id == '115':
            sql = "INSERT INTO EMS.caterpilarClient(id115,polledTime) VALUES(%s,%s)"
            val = (Energy,polledTime)
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("ID 115 inserted")
            except mysql.connector.errors.IntegrityError:
                try:
                    sql = "UPDATE EMS.caterpilarClient set id115 = %s where polledTime = %s"
                    val = (Energy,polledTime)
                    awscur.execute(sql,val)
                    awsdb.commit()
                    print(val)
                    print("ID 115 updated")
                except mysql.connector.Error as err:
                    print(f"Error: {err}")

        if id == '116':
            sql = "INSERT INTO EMS.caterpilarClient(id116,polledTime) VALUES(%s,%s)"
            val = (Energy,polledTime)
            try:
                awscur.execute(sql,val)
                awsdb.commit()
                print(val)
                print("ID 116 inserted")
            except mysql.connector.errors.IntegrityError:
                try:
                    sql = "UPDATE EMS.caterpilarClient set id116 = %s where polledTime = %s"
                    val = (Energy,polledTime)
                    awscur.execute(sql,val)
                    awsdb.commit()
                    print(val)
                    print("ID 116 updated")
                except mysql.connector.Error as err:
                    print(f"Error: {err}") 
   
    gingertime = sorted(ginger1137.keys())

    gingerhr = {gingertime[i]: ginger1137[gingertime[i]] - ginger1137[gingertime[i-1]] for i in range(1, len(gingertime))}

    for i in gingerhr.keys():
        gingerWrite('1137',i,gingerhr[i])

    
    tcstime = sorted(tcs674.keys())

    tcshr = {tcstime[i]: tcs674[tcstime[i]] - tcs674[tcstime[i-1]] for i in range(1, len(tcstime))}

    for i in tcshr.keys():
        tcsWrite('674',i,tcshr[i])

    tcstime = sorted(tcs675.keys())

    tcshr = {tcstime[i]: tcs675[tcstime[i]] - tcs675[tcstime[i-1]] for i in range(1, len(tcstime))}

    for i in tcshr.keys():
        tcsWrite('675',i,tcshr[i])
    
    caterpilartime = sorted(caterpillar115.keys())

    caterhr = {caterpilartime[i]: caterpillar115[caterpilartime[i]] - caterpillar115[caterpilartime[i-1]] for i in range(1, len(caterpilartime))}

    for i in caterhr.keys():
        caterpillarWrite('115',i,caterhr[i])

    caterpilartime = sorted(caterpillar116.keys())

    caterhr = {caterpilartime[i]: caterpillar116[caterpilartime[i]] - caterpillar116[caterpilartime[i-1]] for i in range(1, len(caterpilartime))}

    for i in caterhr.keys():
        caterpillarWrite('116',i,caterhr[i])

    time.sleep(200)