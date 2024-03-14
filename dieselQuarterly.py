import mysql.connector
import time

while True:

    awsdb = mysql.connector.connect(
                    host="43.205.196.66",
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                )

    awscur = awsdb.cursor()

    awscur.execute("select polledTime,DG1,DG2,DG3,DG4,DG5 from EMS.DieselMinWise where date(polledTime) >= date(curdate()-1);")

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


    time.sleep(100)