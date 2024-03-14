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

    dg_hr = {}

    emscur.execute("SELECT polledTime,dg1Energy+dg2Energy+dg3Energy+dg4Energy+dg5Energy as dgEnergy FROM EMS.dieselQuarterly where date(polledTime) >= date(curdate()-1);")

    res = emscur.fetchall()

    def HourSeg(polledTime,Energy):
        polledTime = str(polledTime)[0:14]+"00:00"
        if polledTime in dg_hr.keys():
            dg_hr[polledTime].append(Energy)
        else:
            dg_hr[polledTime] = [Energy]

    for i in res:
        if i[1] != None:
            HourSeg(i[0],i[1])
        else:
            HourSeg(i[0],0)

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

    time.sleep(60)
