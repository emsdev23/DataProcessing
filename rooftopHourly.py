import mysql.connector
# from datetime import date
import time

while True:
  unprocesseddb = mysql.connector.connect(
        host="121.242.232.151",
        user="emsrouser",
        password="emsrouser@151",
        database='bmsmgmtprodv13',
        port=3306
      )

  emsdb = mysql.connector.connect(
    host="43.205.196.66",
    user="emsroot",
    password="22@teneT",
    database='EMS',
    port=3307
)
  
  unpdbcur = unprocesseddb.cursor()
  emscur = emsdb.cursor()

  unpdbcur.execute("""select FLOOR(acmeterenergy),acmeterpolledtimestamp from bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1035 and Date(acmeterpolledtimestamp) = date(curdate()-1); """)

  result = unpdbcur.fetchall()

  roofhr = {}
  ph1 = {}
  ph2 = {}

  def ph1Hour(Energy,Time):
    hour=str(Time)[0:13]+":00:00"
    if hour in ph1.keys():
      ph1[hour] += Energy  
    else:
      ph1[hour] = Energy
    
  def ph2Hour(Energy,Time):
    hour=str(Time)[0:13]+":00:00"
      # print(Time)
    if hour in ph2.keys():
      ph2[hour] += Energy  
    else:
      ph2[hour] = Energy

  def HourSummmarize(Energy,Time):
      hour=str(Time)[0:13]+":00:00"
      # print(Time)
      if hour in roofhr.keys():
        roofhr[hour] += Energy  
      else:
        roofhr[hour] = Energy

  for i in range(1,len(result)):
      ph2Hour(round(((result[i][0]-result[i-1][0])/1000),2),result[i][1])
      HourSummmarize(round(((result[i][0]-result[i-1][0])/1000),2),result[i][1])
  # print(roofhr)   
  unpdbcur.execute("""select FLOOR(acmeterenergy),acmeterpolledtimestamp from bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1147 and Date(acmeterpolledtimestamp) = date(curdate()-1); """)

  result2 = unpdbcur.fetchall()

  for i in range(1,len(result2)):
      ph1Hour(round(((result2[i][0]-result2[i-1][0])/1000),2),result2[i][1])
      HourSummmarize(round(((result2[i][0]-result2[i-1][0])/1000),2),result2[i][1])

  for i in roofhr.keys():
        # print(i,roofhr[i])
        sql = """INSERT INTO roofTopHour(polledTime,energy) VALUES(%s,%s)"""
        val = (i,roofhr[i])
        print(val)
        try:
          emscur.execute(sql,val)
          print("Rooftop hourly Processed data written")
          emsdb.commit()
        except mysql.connector.IntegrityError:
          sql = """UPDATE roofTopHour SET energy = %s WHERE polledTime = %s"""
          val = (roofhr[i],i)
          emscur.execute(sql,val)
          print("Rooftop hourly Processed data written")
          emsdb.commit()
  
  for i in ph1.keys():
        # print(i,roofhr[i])
        sql = """INSERT INTO roofTopHour(polledTime,ph1Actual) VALUES(%s,%s)"""
        val = (i,ph1[i])
        print(val)
        try:
          emscur.execute(sql,val)
          print("Rooftop hourly ph1 Processed data written")
          emsdb.commit()
        except mysql.connector.IntegrityError:
          sql = """UPDATE roofTopHour SET ph1Actual = %s WHERE polledTime = %s"""
          val = (ph1[i],i)
          emscur.execute(sql,val)
          print("Rooftop hourly ph1 Processed data written")
          emsdb.commit()
  
  for i in ph2.keys():
        # print(i,roofhr[i])
        sql = """INSERT INTO roofTopHour(polledTime,ph2Actual) VALUES(%s,%s)"""
        val = (i,ph2[i])
        print(val)
        try:
          emscur.execute(sql,val)
          print("Rooftop hourly ph2 Processed data written")
          emsdb.commit()
        except mysql.connector.IntegrityError:
          sql = """UPDATE roofTopHour SET ph2Actual = %s WHERE polledTime = %s"""
          val = (ph2[i],i)
          emscur.execute(sql,val)
          print("Rooftop hourly ph2 Processed data written")
          emsdb.commit()
  
  rad = {}
  radhr = {}

  def HourSummmarize(irrad,Time):
      hour=str(Time)[0:13]+":00:00"
      if hour in radhr.keys():
          radhr[hour].append(irrad)
      else:
          radhr[hour] = [irrad]


  unpdbcur.execute("""select sensorpolledtimestamp,sensorsolarradiation from sensorreadings WHERE DATE(sensorpolledtimestamp)  >= date(curdate()-1);""")    

  result = unpdbcur.fetchall()

  total_radiation = 0
  irradiation = 0
  expected1147 = 0
  expected1035 = 0


  for row in result:
      HourSummmarize(row[1],row[0])
  

  for i in radhr.keys():
      normalized_solar_radiation = round(sum(radhr[i])/4000,2)
      expected1147 = (normalized_solar_radiation * 0.75 * 4877 * 0.156) - ((normalized_solar_radiation * 0.75 * 4877 * 0.156) * 0.0719)
      expected1035 = (normalized_solar_radiation * 0.75 * 1777 * 0.156) - ((normalized_solar_radiation * 0.75 * 1777 * 0.156) * 0.0871)
      print(i,normalized_solar_radiation,expected1147,expected1035)

      sql = """INSERT INTO roofTopHour(irradiation,expph1Energy,expph2Energy,polledTime) VALUES(%s,%s,%s,%s);"""
      val = (normalized_solar_radiation,expected1147,expected1035,i)
      try:
          emscur.execute(sql,val)
          emsdb.commit()
          print("Rooftop Irradiation and expected inserted") 

      except mysql.connector.IntegrityError:
          sql = """UPDATE roofTopHour SET  irradiation = %s, expph1Energy = %s, expph2Energy = %s WHERE polledTime = %s;"""
          val = (normalized_solar_radiation,expected1147,expected1035,i)
          emscur.execute(sql,val)
          emsdb.commit()
          print("Rooftop Irradiation and expected updated")  

  time.sleep(120)
