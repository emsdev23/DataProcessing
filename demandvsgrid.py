import mysql.connector

try:
    processeddb = mysql.connector.connect(
            host="121.242.232.151",
            user="emsrouser",
            password="emsrouser@151",
            database='bmsmgmt_olap_prod_v13',
            port=3306
            )
except Exception as ex:
    print("BMS database not connected")
    print(ex)

try:
    emsdb = mysql.connector.connect(
            host="121.242.232.211",
            user="emsroot",
            password="22@teneT",
            database='EMS',
            port=3306
            )
except Exception as ex:
    print("EMS database not connected")
    print(ex)

try:
    procur = processeddb.cursor()
except Exception as ex:
    print("BMS cursor error")
    print(ex)

try:
    emscur = emsdb.cursor()
except Exception as ex:
    print("EMS cursor error")
    print(ex)

procur.execute("SELECT polledTime,totalApparentPower2 FROM bmsmgmt_olap_prod_v13.hvacSchneider7230Polling where date(polledTime) = date(curdate()-1);")

peakres = procur.fetchall()

peak_dict = {}

def peakSeg(polledTime,demand):
    min = str(polledTime)[0:17]+"00"
    if min not in peak_dict.keys():
        peak_dict[min] = demand


for i in peakres:
    peakSeg(i[0],i[1])

procur.execute("select FLOOR(acmeterenergy),polledTime from bmsmgmt_olap_prod_v13.MVPPolling where mvpnum in ('MVP1','MVP2','MVP3','MVP4') and date(polledTime) = date(curdate()-1);")

gridres = procur.fetchall()

min_dict = {}

def minSeg(polledTime,Energy):
    mins = str(polledTime)[0:17]+"00"
    if Energy != None:
        if mins in min_dict.keys():
            min_dict[mins] += Energy
        else:
            min_dict[mins] = Energy


for i in gridres:
    minSeg(i[1],i[0])
    
keys_order = list(min_dict.keys())

previous_val = None

min_grid = {}

for key in keys_order:
    current_val = min_dict[key]

    if previous_val is not None:
        diff = (current_val - previous_val)/1000
        
        min_grid[key] = diff

    previous_val = current_val

# for i in min_dict.keys():
#     print(i,min_dict[i])


for i in peak_dict.keys():
    sql = "INSERT INTO EMS.peakDemandvs(polledTime,peakDemand) VALUES(%s,%s)"
    val = (i,peak_dict[i])
    try:
        emscur.execute(sql,val)
        emsdb.commit()
        print("peak demand minute inserted")
    except mysql.connector.errors.IntegrityError:
        sql = "UPDATE peakDemandvs SET peakDemand = %s WHERE polledTime = %s"
        val = (peak_dict[i],i)
        emscur.execute(sql,val)
        emsdb.commit()
        print("peak demand minute inserted")

for i in min_grid.keys():
    sql = "INSERT INTO EMS.peakDemandvs(polledTime,gridEnergy) VALUES(%s,%s)"
    val = (i,min_grid[i])
    try:
        emscur.execute(sql,val)
        emsdb.commit()
        print("Grid minute inserted")
    except mysql.connector.errors.IntegrityError:
        sql = "UPDATE peakDemandvs SET gridEnergy = %s WHERE polledTime = %s"
        val = (min_grid[i],i)
        emscur.execute(sql,val)
        emsdb.commit()
        print("Grid minute inserted")

    
