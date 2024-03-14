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

    emscur.execute("select max(Energy) , polledDate, month(polledDate) as gdmon, year(polledDate) as gdyr FROM EMS.GridProcessed where Energy <= 90000 group by gdmon,gdyr;")

    res = emscur.fetchall()

    for i in res:
        val = (i[0],i[1])
        sql = "INSERT INTO EMS.gridMonthly(Energy,polledDate) VALUES(%s,%s)"
        print(val)
        try:
            emscur.execute(sql,val)
            emsdb.commit()
            print("Grid monthly inserted")
        except mysql.connector.errors.IntegrityError:
            sql = "UPDATE EMS.gridMonthly SET Energy = %s WHERE polledDate = %s"
            val = (i[0],i[1])
            emscur.execute(sql,val)
            emsdb.commit()
            print("Grid monthly updated")

    time.sleep(100)