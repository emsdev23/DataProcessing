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
    emscur.execute("SELECT Energy,polledTime from EMS.Gridhourly where date(polledTime) = date(curdate()-1)")
    data = emscur.fetchall()
        
    grid_dict = {}
    def SummarizeDay(Energy,polledTime):
        day = str(polledTime)[0:10]
        if day in grid_dict.keys():
            grid_dict[day] += Energy
        else:
            grid_dict[day] = Energy
            
    for i in data:
        SummarizeDay(i[0],i[1])
            
    for i in grid_dict.keys():
        sql = "INSERT INTO GridProcessed(polledDate,Energy) VALUES(%s,%s)"
        val = (i,grid_dict[i])
        print(val)
        try:
            emscur.execute(sql,val)
            print("Grid data added")
            emsdb.commit()
        except mysql.connector.IntegrityError:
            sql = """UPDATE GridProcessed SET Energy = %s WHERE polledDate= %s;"""
            values = (grid_dict[i],i)
            emscur.execute(sql,values)
            print("Grid data updated")
            emsdb.commit()
    
    time.sleep(300)