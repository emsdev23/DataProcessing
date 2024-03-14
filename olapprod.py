import time
import datetime
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import pooling

source_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="source_pool",
            pool_size= 1,
            host="121.242.232.151",
            user="emsrouser",
            password="emsrouser@151",
            database="bmsmgmt_olap_prod_v13"
)
        
        # Connect to the d
dest_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="dest_pool",
            pool_size=1,
            host="43.205.196.66",
            user="emsroot",
            password="22@teneT",
            database="meterdata",
            port = 3307
           
)

while True:
    try:
        current_time = datetime.now()
        current_minute = current_time.minute
        current_second = current_time.second
        start_hour = current_time.hour
        end_hour = current_time.hour
        current_hour = current_time.hour
        


          


  


        if current_time.minute in [13,28,36,59]:
         
            print("BLOCK WISE DAY WISE")
            source_db = source_pool.get_connection()
            source_cur = source_db.cursor()
            query = "SELECT DATE_FORMAT(day_interval, '%Y-%m-%d') AS day_interval, SUM(CASE WHEN mvpnum IN ('A-Block1', 'A-Block2') THEN GREATEST(0, cummulativeenergy) ELSE 0 END) AS ABLOCK, SUM(CASE WHEN mvpnum = 'B-Block' THEN GREATEST(0, cummulativeenergy) ELSE 0 END) AS cumulativeenergyb, SUM(CASE WHEN mvpnum = 'D-Block' THEN GREATEST(0, cummulativeenergy) ELSE 0 END) AS cumulativeenergy, SUM(CASE WHEN mvpnum IN ('C-Block-North', 'C-Block-South') THEN GREATEST(0, cummulativeenergy) ELSE 0 END) AS cummulativeenergyd, SUM(CASE WHEN mvpnum IN ('MLCP1', 'MLCP2') THEN GREATEST(0, cummulativeenergy) ELSE 0 END) AS MLCP, SUM(CASE WHEN mvpnum IN ('Auditorium') THEN GREATEST(0, cummulativeenergy) ELSE 0 END) AS Auditorium, MAX(last_polledTime) AS last_polledTime FROM (SELECT mvpnum, SUM(energy_difference) AS cummulativeenergy, MAX(polledTime) AS last_polledTime, DATE(polledTime) AS day_interval FROM (SELECT mvpnum, acmeterenergy - LAG(acmeterenergy) OVER (PARTITION BY mvpnum ORDER BY polledTime) AS energy_difference, polledTime FROM MVPPolling WHERE DATE(polledTime) = CURDATE()  AND mvpnum IN ('A-Block1', 'A-Block2', 'B-Block', 'C-Block-North', 'C-Block-South', 'D-Block', 'MLCP1', 'MLCP2','Auditorium')) AS subquery WHERE energy_difference IS NOT NULL GROUP BY mvpnum, day_interval) AS pivot_data GROUP BY day_interval;"
            source_cur.execute(query)
            # Fetch all the rows from the source database
            rows = source_cur.fetchall()
            print(rows)
            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []
            
            # Loop through the rows and append the values to the insert_rows li
            for row in rows:
                timestamp,ablock,bblock,dblock,cblock,mlcp,Auditorium,times = row
                insert_rows.append((timestamp,ablock,bblock,dblock,cblock,mlcp,Auditorium))
                
                
              
            
            dest_db = dest_pool.get_connection()
            dest_cur = dest_db.cursor()

            for row in insert_rows:
                timestamp,ablock,bblock,dblock,cblock,mlcp,Auditorium = row
                # Check if a record with the same polledTime exists
                dest_cur.execute("""SELECT COUNT(*) FROM BlockwiseDaywise WHERE timestamp = %s""", (timestamp,))

                record_count = dest_cur.fetchone()[0]

                if record_count > 0:
                     # Update the existing record
                   update_query = """UPDATE BlockwiseDaywise SET ABLOCK = %s,BBlock=%s,CBLOCK=%s,DBLOCK=%s,MLCP=%s,auditorium=%s WHERE timestamp = %s"""
                   dest_cur.execute(update_query, (ablock,bblock,cblock,dblock,mlcp,Auditorium,timestamp))
                else:
                    # Insert a new record
                   insert_query = """INSERT INTO BlockwiseDaywise (ABLOCK,BBlock,CBLOCK,DBLOCK,MLCP,auditorium,timestamp) VALUES (%s, %s,%s,%s,%s,%s,%s)"""
                   dest_cur.execute(insert_query, (ablock,bblock,cblock,dblock,mlcp,Auditorium,timestamp))

                dest_db.commit()

            dest_cur.close()
            dest_db.close()
            source_cur.close()
            source_db.close() 
       

        if current_time.minute in [20,35,39,59]:
         
            print("BLOCKWISE HOURLY ENERGY HOURLY")
            source_db = source_pool.get_connection()
            source_cur = source_db.cursor()
            query = "SELECT DATE_FORMAT(polledTime, '%Y-%m-%d %H:00:00') AS hour_interval, SUM(CASE WHEN mvpnum IN ('A-Block1', 'A-Block2') THEN GREATEST(0, cummulativeenergy) END) AS ABLOCK, SUM(CASE WHEN mvpnum = 'B-Block' THEN GREATEST(0, cummulativeenergy) END) AS cumulativeenergy_B_Block, SUM(CASE WHEN mvpnum = 'D-Block' THEN GREATEST(0, cummulativeenergy) END) AS cumulativeenergy_D_Block, SUM(CASE WHEN mvpnum IN ('C-Block-North', 'C-Block-South') THEN GREATEST(0, cummulativeenergy) END) AS CBLOCK, SUM(CASE WHEN mvpnum IN ('MLCP1', 'MLCP2') THEN GREATEST(0, cummulativeenergy) END) AS MLCP, SUM(CASE WHEN mvpnum IN ('Auditorium') THEN GREATEST(0, cummulativeenergy) END) AS Auditorium, MAX(last_polledTime) AS last_polledTime FROM (SELECT mvpnum, SUM(energy_difference) AS cummulativeenergy, MAX(polledTime) AS last_polledTime, polledTime FROM (SELECT mvpnum, acmeterenergy - LAG(acmeterenergy) OVER (PARTITION BY mvpnum ORDER BY polledTime) AS energy_difference, polledTime FROM MVPPolling WHERE DATE(polledTime) = CURDATE() AND mvpnum IN ('A-Block1', 'A-Block2', 'B-Block', 'C-Block-North', 'C-Block-South', 'D-Block', 'MLCP1', 'MLCP2','Auditorium')) AS subquery WHERE energy_difference IS NOT NULL GROUP BY mvpnum, polledTime) AS pivot_data GROUP BY hour_interval;"
            source_cur.execute(query)
            # Fetch all the rows from the source database
            rows = source_cur.fetchall()
            print(rows)
            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []
            
            # Loop through the rows and append the values to the insert_rows li
            for row in rows:
                timestamp,ablock,bblock,dblock,cblock,mlcp,Auditorium,times = row
                insert_rows.append((timestamp,ablock,bblock,dblock,cblock,mlcp,Auditorium))
                
                
              
            
            dest_db = dest_pool.get_connection()
            dest_cur = dest_db.cursor()

            for row in insert_rows:
                timestamp,ablock,bblock,dblock,cblock,mlcp,Auditorium = row
                # Check if a record with the same polledTime exists
                dest_cur.execute("""SELECT COUNT(*) FROM blockwisehourly WHERE timestamp = %s""", (timestamp,))

                record_count = dest_cur.fetchone()[0]

                if record_count > 0:
                     # Update the existing record
                   update_query = """UPDATE blockwisehourly SET ABLOCK = %s,BBlock=%s,CBLOCK=%s,DBLOCK=%s,MLCP=%s,auditorium=%s WHERE timestamp = %s"""
                   dest_cur.execute(update_query, (ablock,bblock,cblock,dblock,mlcp,Auditorium,timestamp))
                else:
                    # Insert a new record
                   insert_query = """INSERT INTO blockwisehourly (ABLOCK,BBlock,CBLOCK,DBLOCK,MLCP,auditorium,timestamp) VALUES (%s, %s,%s,%s,%s,%s,%s)"""
                   dest_cur.execute(insert_query, (ablock,bblock,cblock,dblock,mlcp,Auditorium,timestamp))

                dest_db.commit()

            dest_cur.close()
            dest_db.close()
            source_cur.close()
        
            source_db.close() 
            
        
        if current_time.minute in [50,35,10,59]:
         
            print("BLOCK WISE FIFTEEN MINUTES")
            source_db = source_pool.get_connection()
            source_cur = source_db.cursor()
            query = "SELECT CONCAT(DATE_FORMAT(polledTime, '%Y-%m-%d %H:'), LPAD(15 * (MINUTE(polledTime) DIV 15), 2, '00'), ':00') AS interval_15_minutes, SUM(CASE WHEN mvpnum IN ('A-Block1', 'A-Block2') THEN GREATEST(0, cummulativeenergy) END) AS ABLOCK, SUM(CASE WHEN mvpnum = 'B-Block' THEN GREATEST(0, cummulativeenergy) END) AS cumulativeenergy_B_Block, SUM(CASE WHEN mvpnum = 'D-Block' THEN GREATEST(0, cummulativeenergy) END) AS cumulativeenergy_D_Block, SUM(CASE WHEN mvpnum IN ('C-Block-North', 'C-Block-South') THEN GREATEST(0, cummulativeenergy) END) AS CBLOCK, SUM(CASE WHEN mvpnum IN ('MLCP1', 'MLCP2') THEN GREATEST(0, cummulativeenergy) END) AS MLCP, SUM(CASE WHEN mvpnum IN ('Auditorium') THEN GREATEST(0, cummulativeenergy) END) AS Auditorium, MAX(last_polledTime) AS last_polledTime FROM (SELECT mvpnum, SUM(energy_difference) AS cummulativeenergy, MAX(polledTime) AS last_polledTime, polledTime FROM (SELECT mvpnum, acmeterenergy - LAG(acmeterenergy) OVER (PARTITION BY mvpnum ORDER BY polledTime) AS energy_difference, polledTime FROM MVPPolling WHERE DATE(polledTime) = CURDATE() AND mvpnum IN ('A-Block1', 'A-Block2', 'B-Block', 'C-Block-North', 'C-Block-South', 'D-Block', 'MLCP1', 'MLCP2','Auditorium')) AS subquery WHERE energy_difference > 0 GROUP BY mvpnum, polledTime) AS pivot_data GROUP BY interval_15_minutes;"
            source_cur.execute(query)
            # Fetch all the rows from the source database
            rows = source_cur.fetchall()
            print(rows)
            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []
            
            # Loop through the rows and append the values to the insert_rows li
            for row in rows:
                timestamp,ablock,bblock,dblock,cblock,mlcp,Auditorium,times = row
                insert_rows.append((timestamp,ablock,bblock,dblock,cblock,mlcp,Auditorium))
                
                
              
            
            dest_db = dest_pool.get_connection()
            dest_cur = dest_db.cursor()

            for row in insert_rows:
                timestamp,ablock,bblock,dblock,cblock,mlcp,Auditorium = row
                # Check if a record with the same polledTime exists
                dest_cur.execute("""SELECT COUNT(*) FROM BLOCK_WISE_ENERGY WHERE timestamp = %s""", (timestamp,))

                record_count = dest_cur.fetchone()[0]

                if record_count > 0:
                     # Update the existing record
                   update_query = """UPDATE BLOCK_WISE_ENERGY SET AbLOCK = %s,Bblock =%s,Cblock=%s,Dblock=%s,MLCP=%s,auditorium=%s WHERE timestamp = %s"""
                   dest_cur.execute(update_query, (ablock,bblock,cblock,dblock,mlcp,Auditorium,timestamp))
                else:
                    # Insert a new record
                   insert_query = """INSERT INTO BLOCK_WISE_ENERGY (AbLOCK,Bblock,Cblock,Dblock,MLCP,auditorium,timestamp) VALUES (%s,%s,%s,%s,%s,%s,%s)"""
                   dest_cur.execute(insert_query, (ablock,bblock,cblock,dblock,mlcp,Auditorium,timestamp))

                dest_db.commit()

            dest_cur.close()
            dest_db.close()
            source_cur.close()
            source_db.close() 
         
        if current_time.minute in [57,35,22,59]:
         
            print("PEAKDEMAND QUARTER")
            source_db = source_pool.get_connection()
            source_cur = source_db.cursor()
            query = "SELECT CONCAT(DATE_FORMAT(polledTime, '%Y-%m-%d %H:'), LPAD(15 * (MINUTE(polledTime) DIV 15), 2, '00'), ':00') AS interval_15_minutes, ROUND(MAX(totalApparentPower2), 2) AS max_totalApparentPower2, MAX(polledTime) AS min_polledTime FROM hvacSchneider7230Polling WHERE DATE(polledTime) = CURDATE() GROUP BY interval_15_minutes;"
            source_cur.execute(query)
            # Fetch all the rows from the source database
            rows = source_cur.fetchall()
            print(rows)
            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []
            
            # Loop through the rows and append the values to the insert_rows li
            for row in rows:
                timestamp,max_totalApparentPower2,TIMES = row
                insert_rows.append((timestamp,max_totalApparentPower2))
                
                
              
            
            dest_db = dest_pool.get_connection()
            dest_cur = dest_db.cursor()

            for row in insert_rows:
                timestamp,max_totalApparentPower2 = row
                # Check if a record with the same polledTime exists
                dest_cur.execute("""SELECT COUNT(*) FROM peakdemandquarter WHERE polledTime = %s""", (timestamp,))

                record_count = dest_cur.fetchone()[0]

                if record_count > 0:
                     # Update the existing record
                   update_query = """UPDATE peakdemandquarter SET peakdemand = %s WHERE polledTime = %s"""
                   dest_cur.execute(update_query, (max_totalApparentPower2,timestamp))
                else:
                    # Insert a new record
                   insert_query = """INSERT INTO peakdemandquarter (peakdemand,polledTime) VALUES (%s,%s)"""
                   dest_cur.execute(insert_query, (max_totalApparentPower2,timestamp))

                dest_db.commit()

            dest_cur.close()
            dest_db.close()
            source_cur.close()
            source_db.close()
            
        
        if current_time.minute in [5,15,26,46,56]:
         
            print("GRID ENERGY HOURLY")
            source_db = source_pool.get_connection()
            source_cur = source_db.cursor()
            query = "SELECT DATE_FORMAT(polledTime, '%Y-%m-%d %H:00:00') AS hourly_time, SUM(energy_difference) / 1000 AS cumulative_energy, MAX(polledTime) AS last_polledTime FROM (SELECT mvpnum, acmeterenergy - LAG(acmeterenergy) OVER (PARTITION BY mvpnum ORDER BY polledTime) AS energy_difference, polledTime FROM MVPPolling WHERE DATE(polledTime) = CURDATE() AND mvpnum IN ('MVP1', 'MVP2', 'MVP3', 'MVP4')) subquery WHERE energy_difference IS NOT NULL GROUP BY hourly_time ORDER BY hourly_time;"
            source_cur.execute(query)
            # Fetch all the rows from the source database
            rows = source_cur.fetchall()
            print(rows)
            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []
            
            # Loop through the rows and append the values to the insert_rows li
            for row in rows:
                timestamp,cumulative_energy,TIMES = row
                insert_rows.append((timestamp,cumulative_energy))
                
                
              
            
            dest_db = dest_pool.get_connection()
            dest_cur = dest_db.cursor()

            for row in insert_rows:
                timestamp,cumulative_energy = row
                # Check if a record with the same polledTime exists
                dest_cur.execute("""SELECT COUNT(*) FROM gridenergyhourly WHERE timestamp = %s""", (timestamp,))

                record_count = dest_cur.fetchone()[0]

                if record_count > 0:
                     # Update the existing record
                   update_query = """UPDATE gridenergyhourly SET cumulative_energy = %s WHERE timestamp = %s"""
                   dest_cur.execute(update_query, (cumulative_energy,timestamp))
                else:
                    # Insert a new record
                   insert_query = """INSERT INTO gridenergyhourly (cumulative_energy,timestamp) VALUES (%s,%s)"""
                   dest_cur.execute(insert_query, (cumulative_energy,timestamp))

                dest_db.commit()

            dest_cur.close()
            dest_db.close()
            source_cur.close()
            source_db.close() 
            
        if current_time.minute in [57,41,7,59]:
         
            print("GRID ENERGY QUARTERLY")
            source_db = source_pool.get_connection()
            source_cur = source_db.cursor()
            query = "SELECT CONCAT(DATE_FORMAT(polledTime, '%Y-%m-%d %H:'), LPAD(15 * (MINUTE(polledTime) DIV 15), 2, '00'), ':00') AS interval_15_minutes, SUM(energy_difference) / 1000 AS cumulative_energy, MAX(polledTime) AS last_polledTime FROM (SELECT mvpnum, acmeterenergy - LAG(acmeterenergy) OVER (PARTITION BY mvpnum ORDER BY polledTime) AS energy_difference, polledTime FROM MVPPolling WHERE DATE(polledTime) = CURDATE() AND mvpnum IN ('MVP1', 'MVP2', 'MVP3', 'MVP4')) subquery WHERE energy_difference IS NOT NULL GROUP BY interval_15_minutes ORDER BY interval_15_minutes;"
            source_cur.execute(query)
            # Fetch all the rows from the source database
            rows = source_cur.fetchall()
            print(rows)
            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []
            
            # Loop through the rows and append the values to the insert_rows li
            for row in rows:
                timestamp,cumulative_energy,TIMES = row
                insert_rows.append((timestamp,cumulative_energy))
                
                
              
            
            dest_db = dest_pool.get_connection()
            dest_cur = dest_db.cursor()

            for row in insert_rows:
                timestamp,cumulative_energy = row
                # Check if a record with the same polledTime exists
                dest_cur.execute("""SELECT COUNT(*) FROM gridenergyquarterly WHERE timestamp = %s""", (timestamp,))

                record_count = dest_cur.fetchone()[0]

                if record_count > 0:
                     # Update the existing record
                   update_query = """UPDATE gridenergyquarterly SET cumulative_energy = %s WHERE timestamp = %s"""
                   dest_cur.execute(update_query, (cumulative_energy,timestamp))
                else:
                    # Insert a new record
                   insert_query = """INSERT INTO gridenergyquarterly (cumulative_energy,timestamp) VALUES (%s,%s)"""
                   dest_cur.execute(insert_query, (cumulative_energy,timestamp))

                dest_db.commit()

            dest_cur.close()
            dest_db.close()
            source_cur.close()
            source_db.close() 
        if current_time.minute in [31,46,16,1]:
         
            print("DISEL ENERGY FIFTEEN MINUTES")
            source_db = source_pool.get_connection()
            source_cur = source_db.cursor()
            query = "SELECT CONCAT(DATE_FORMAT(polledTime, '%Y-%m-%d %H:'), LPAD(15 * (MINUTE(polledTime) DIV 15), 2, '00'), ':00') AS interval_15_minutes, ROUND(SUM(CASE WHEN DGNum = 1 THEN energy_difference ELSE 0 END) * 1000, 2) AS DGNum_1_energy_difference, ROUND(SUM(CASE WHEN DGNum = 2 THEN energy_difference ELSE 0 END) * 1000, 2) AS DGNum_2_energy_difference, ROUND(SUM(CASE WHEN DGNum = 3 THEN energy_difference ELSE 0 END) * 1000, 2) AS DGNum_3_energy_difference, ROUND(SUM(CASE WHEN DGNum = 4 THEN energy_difference ELSE 0 END) * 1000, 2) AS DGNum_4_energy_difference, ROUND(SUM(CASE WHEN DGNum = 5 THEN energy_difference ELSE 0 END) * 1000, 2) AS DGNum_5_energy_difference, ROUND(SUM(energy_difference) * 1000, 2) AS Total_energy_difference, MAX(polledTime) AS min_polledTime FROM (SELECT DGNum, dgrealenergy - LAG(dgrealenergy, 1) OVER (PARTITION BY DGNum ORDER BY polledTime) AS energy_difference, polledTime FROM DGPolling WHERE DATE(polledTime) = CURDATE() AND DGNum IN (1, 2, 3, 4, 5) ORDER BY polledTime DESC) AS subquery GROUP BY interval_15_minutes ORDER BY interval_15_minutes;"
            source_cur.execute(query)
            # Fetch all the rows from the source database
            rows = source_cur.fetchall()
            print(rows)
            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []
            
            # Loop through the rows and append the values to the insert_rows li
            for row in rows:
                timestamp,DGNum_1_energy_difference,DGNum_2_energy_difference,DGNum_3_energy_difference,DGNum_4_energy_difference,DGNum_5_energy_difference,Total_energy_difference,max_polledTime = row
                insert_rows.append((timestamp,DGNum_1_energy_difference,DGNum_2_energy_difference,DGNum_3_energy_difference,DGNum_4_energy_difference,DGNum_5_energy_difference,Total_energy_difference))
                
                
              
            
            dest_db = dest_pool.get_connection()
            dest_cur = dest_db.cursor()

            for row in insert_rows:
                timestamp,DGNum_1_energy_difference,DGNum_2_energy_difference,DGNum_3_energy_difference,DGNum_4_energy_difference,DGNum_5_energy_difference,Total_energy_difference = row
                # Check if a record with the same polledTime exists
                dest_cur.execute("""SELECT COUNT(*) FROM diselenergyquaterly WHERE timestamp = %s""", (timestamp,))

                record_count = dest_cur.fetchone()[0]

                if record_count > 0:
                     # Update the existing record
                   update_query = """UPDATE diselenergyquaterly SET DGNum_1_energy_difference = %s,DGNum_2_energy_difference =%s,DGNum_3_energy_difference=%s,DGNum_4_energy_difference=%s,DGNum_5_energy_difference =%s,total_energy_difference=%s WHERE timestamp = %s"""
                   dest_cur.execute(update_query, (DGNum_1_energy_difference,DGNum_2_energy_difference,DGNum_3_energy_difference,DGNum_4_energy_difference,DGNum_5_energy_difference,Total_energy_difference,timestamp))
                else:
                    # Insert a new record
                   insert_query = """INSERT INTO diselenergyquaterly (DGNum_5_energy_difference,DGNum_4_energy_difference,DGNum_3_energy_difference,DGNum_2_energy_difference,DGNum_1_energy_difference,Total_energy_difference,timestamp) VALUES (%s,%s,%s,%s,%s,%s,%s)"""
                   dest_cur.execute(insert_query, (DGNum_5_energy_difference,DGNum_4_energy_difference,DGNum_3_energy_difference,DGNum_2_energy_difference,DGNum_1_energy_difference,Total_energy_difference,timestamp)),

                dest_db.commit()

            dest_cur.close()
            dest_db.close()
            source_cur.close()
            source_db.close()
            
       
        
        if current_time.minute in [15,51,28,41,52,2]:
         
            print("MVP1,MVP2,MVP3,MVP4")
            source_db = source_pool.get_connection()
            source_cur = source_db.cursor()
            query = "SELECT DATE_FORMAT(polledTime, '%Y-%m-%d %H:%i:00') AS minute_interval, ROUND(MAX(CASE WHEN mvpnum = 'MVP1' THEN acmeterpower/1000 END), 2) AS Total_MVP1_acmeterpower, ROUND(MAX(CASE WHEN mvpnum = 'MVP2' THEN acmeterpower/1000 END), 2) AS Total_MVP2_acmeterpower, ROUND(MAX(CASE WHEN mvpnum = 'MVP3' THEN acmeterpower/1000 END), 2) AS Total_MVP3_acmeterpower, ROUND(MAX(CASE WHEN mvpnum = 'MVP4' THEN acmeterpower/1000 END), 2) AS Total_MVP4_acmeterpower, ROUND(SUM(CASE WHEN mvpnum IN ('MVP1', 'MVP2','MVP3','MVP4') THEN acmeterpower/1000 END), 2) AS Total_acmeterpower FROM MVPPolling WHERE mvpnum IN ('MVP1', 'MVP2', 'MVP3', 'MVP4') AND polledTime >= NOW() - INTERVAL 60 MINUTE GROUP BY minute_interval;"
            source_cur.execute(query)
            # Fetch all the rows from the source database
            rows = source_cur.fetchall()
            print(rows)
            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []
            
            # Loop through the rows and append the values to the insert_rows l
            for row in rows:
                minute_interval,Total_MVP1_acmeterpower,Total_MVP2_acmeterpower,Total_MVP3_acmeterpower,Total_MVP4_acmeterpower,Total_acmeterpower = row
                insert_rows.append(( minute_interval,Total_MVP1_acmeterpower,Total_MVP2_acmeterpower,Total_MVP3_acmeterpower,Total_MVP4_acmeterpower,Total_acmeterpower))
                
                
              
            
            dest_db = dest_pool.get_connection()
            dest_cur = dest_db.cursor()

            for row in insert_rows:
                 minute_interval,Total_MVP1_acmeterpower,Total_MVP2_acmeterpower,Total_MVP3_acmeterpower,Total_MVP4_acmeterpower,Total_acmeterpower  = row
                # Check if a record with the same polledTime exists
                 dest_cur.execute("""SELECT COUNT(*) FROM kv_vs_kwh WHERE timestamp = %s""", (minute_interval,))

                 record_count = dest_cur.fetchone()[0]

                 if record_count > 0:
                     # Update the existing record
                    update_query = """UPDATE kv_vs_kwh SET  mvp1 = %s,mvp2=%s,mvp3=%s,mvp4=%s,TOTALMVP=%s WHERE timestamp = %s"""
                    dest_cur.execute(update_query, (Total_MVP1_acmeterpower,Total_MVP2_acmeterpower,Total_MVP3_acmeterpower,Total_MVP4_acmeterpower,Total_acmeterpower,minute_interval))
                 else:
                    # Insert a new record
                    insert_query = """INSERT INTO kv_vs_kwh (timestamp,mvp1,mvp2,mvp3,mvp4,TOTALMVP) VALUES (%s,%s,%s,%s,%s,%s)"""
                    dest_cur.execute(insert_query, (minute_interval,Total_MVP1_acmeterpower,Total_MVP2_acmeterpower,Total_MVP3_acmeterpower,Total_MVP4_acmeterpower,Total_acmeterpower))

                 dest_db.commit()

            dest_cur.close()
            dest_db.close()
            source_cur.close()
            source_db.close() 
            
        if current_time.minute in [17,31,26,47]:
         
            print("PEAK DEMAND KWH")
            source_db = source_pool.get_connection()
            source_cur = source_db.cursor()
            query = "SELECT DATE_FORMAT(polledTime, '%Y-%m-%d %H:%i:00') AS minute_interval, ROUND(MAX(totalApparentPower2), 2) AS rounded_totalApparentPower2 FROM hvacSchneider7230Polling WHERE polledTime >= NOW() - INTERVAL 60 MINUTE GROUP BY minute_interval ORDER BY minute_interval;"
            source_cur.execute(query)
            # Fetch all the rows from the source database
            rows = source_cur.fetchall()
            print(rows)
            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []
            
            # Loop through the rows and append the values to the insert_rows li
            for row in rows:
                timestamp,max_totalApparentPower2 = row
                insert_rows.append((timestamp,max_totalApparentPower2))
                
                
              
            
            dest_db = dest_pool.get_connection()
            dest_cur = dest_db.cursor()

            for row in insert_rows:
                timestamp,max_totalApparentPower2 = row
                # Check if a record with the same polledTime exists
                dest_cur.execute("""SELECT COUNT(*) FROM kv_vs_kwh WHERE timestamp = %s""", (timestamp,))

                record_count = dest_cur.fetchone()[0]

                if record_count > 0:
                     # Update the existing record
                   update_query = """UPDATE kv_vs_kwh SET peakmax = %s WHERE timestamp = %s"""
                   dest_cur.execute(update_query, (max_totalApparentPower2,timestamp))
                else:
                    # Insert a new record
                   insert_query = """INSERT INTO kv_vs_kwh (peakmax,timestamp) VALUES (%s,%s)"""
                   dest_cur.execute(insert_query, (max_totalApparentPower2,timestamp))

                dest_db.commit()

            dest_cur.close()
            dest_db.close()
            source_cur.close()
            source_db.close() 
        if current_time.minute in [59,45,22,47]:
         
            print("DISEL  ENERGY HOURLY")
            source_db = source_pool.get_connection()
            source_cur = source_db.cursor()
            query = "SELECT DATE_FORMAT(polledTime, '%Y-%m-%d %H:00:00') AS hour_interval,ROUND( SUM(energy_difference) * 1000,2) AS total_energy_difference,MIN(polledTime) AS min_polledTime FROM (SELECT DGNum,dgrealenergy - LAG(dgrealenergy, 1) OVER (PARTITION BY DGNum ORDER BY polledTime) AS energy_difference,polledTime FROM DGPolling WHERE DGNum IN (1, 2, 3, 4, 5) AND date(polledTime) = CURDATE() ORDER BY polledTime) AS subquery GROUP BY hour_interval;"
            source_cur.execute(query)
            # Fetch all the rows from the source database
            rows = source_cur.fetchall()
            print(rows)
            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []
            
            # Loop through the rows and append the values to the insert_rows li
            for row in rows:
                timestamp,total_energy_difference,min_polledTime = row
                insert_rows.append((timestamp,total_energy_difference))
                
                
              
            
            dest_db = dest_pool.get_connection()
            dest_cur = dest_db.cursor()

            for row in insert_rows:
                timestamp,total_energy_difference = row
                # Check if a record with the same polledTime exists
                dest_cur.execute("""SELECT COUNT(*) FROM diselenergyhourly WHERE timestamp = %s""", (timestamp,))

                record_count = dest_cur.fetchone()[0]

                if record_count > 0:
                     # Update the existing record
                   update_query = """UPDATE diselenergyhourly SET total_energy_difference = %s WHERE timestamp = %s"""
                   dest_cur.execute(update_query, (total_energy_difference,timestamp,))
                else:
                    # Insert a new record
                   insert_query = """INSERT INTO diselenergyhourly (total_energy_difference,timestamp) VALUES (%s,%s)"""
                   dest_cur.execute(insert_query, (total_energy_difference,timestamp,))

                dest_db.commit()

            dest_cur.close()
            dest_db.close()
            source_cur.close()
            source_db.close()       
        if current_minute in [12,22,36,48,52]:
            source_db = source_pool.get_connection()
            source_cur = source_db.cursor()
            
            # Execute the query
            query = "SELECT (SELECT SUM(total_energy_difference)/1000 FROM (SELECT mvpnum, SUM(CASE WHEN energy_difference > 0 AND energy_difference < 1000000 THEN energy_difference ELSE 0 END) AS total_energy_difference FROM (SELECT mvpnum, acmeterenergy - LAG(acmeterenergy) OVER (PARTITION BY mvpnum ORDER BY polledTime) AS energy_difference FROM MVPPolling WHERE polledTime >= CURDATE() AND polledTime <= NOW() AND mvpnum IN ('MVP1', 'MVP2', 'MVP3', 'MVP4')) subquery WHERE energy_difference IS NOT NULL GROUP BY mvpnum) t) AS cumulative_energy, MAX(polledTime) AS last_polledTime FROM MVPPolling WHERE DATE(polledTime) = CURDATE() AND mvpnum IN ('MVP1', 'MVP2', 'MVP3', 'MVP4') ORDER BY polledTime DESC;"
            source_cur.execute(query)

            # Fetch all the rows from the source database
            rows = source_cur.fetchall()

            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []

            # Loop through the rows and append the values to the insert_rows list
            for row in rows:
                cumulative_energy,timestamp= row
                insert_rows.append((cumulative_energy,timestamp))
                print("-----------------------------------------------------------------------------------------------------------")
                print(row)
                print(insert_rows)
                print("GRID ENERGY TODAY")
                
            dest_db = dest_pool.get_connection()
            dest_cur = dest_db.cursor()

            # Insert the rows into the destination database
            insert_query = "INSERT INTO gridenergytoday (cumulative_energy,timestamp) VALUES (%s,%s);"
            dest_cur.executemany(insert_query, insert_rows)
            dest_db.commit()
            source_cur.close()
            source_db.close()
            dest_cur.close()
            dest_db.close()
      
        if current_time.day ==1 and current_time.minute == 28:
         
            print("GRID ENERGY monthly")
            source_db = source_pool.get_connection()
            source_cur = source_db.cursor()
            query = "SELECT DATE_FORMAT(polledTime, '%Y-%m-%d 00:00:00') AS month, ROUND(SUM(ROUND(total_energy_difference / 1000, 2)), 2) AS cumulative_energy, MAX(polledTime) AS last_polledTime FROM (SELECT mvpnum, ROUND(SUM(CASE WHEN energy_difference > 0 AND energy_difference < 1000000 THEN energy_difference ELSE 0 END), 2) AS total_energy_difference, MAX(polledTime) AS polledTime FROM (SELECT mvpnum, acmeterenergy - LAG(acmeterenergy) OVER (PARTITION BY mvpnum ORDER BY polledTime) AS energy_difference, polledTime FROM MVPPolling WHERE (YEAR(polledTime) = YEAR(CURDATE()) AND MONTH(polledTime) = MONTH(CURDATE()) - 1 OR YEAR(polledTime) = YEAR(CURDATE()) - 1 AND MONTH(polledTime) = 12) AND mvpnum IN ('MVP1', 'MVP2', 'MVP3', 'MVP4')) subquery WHERE energy_difference IS NOT NULL GROUP BY mvpnum, MONTH(polledTime)) t GROUP BY month ORDER BY month DESC;"
            source_cur.execute(query)
            # Fetch all the rows from the source database
            rows = source_cur.fetchall()
            print(rows)
            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []
            
            # Loop through the rows and append the values to the insert_rows li
            for row in rows:
                timestamp,total_energy_difference,min_polledTime = row
                insert_rows.append((timestamp,total_energy_difference))
                
                
              
            
            dest_db = dest_pool.get_connection()
            dest_cur = dest_db.cursor()

            for row in insert_rows:
                timestamp,max_totalApparentPower2 = row
                # Check if a record with the same polledTime exists
                dest_cur.execute("""SELECT COUNT(*) FROM gridenergymonthly WHERE timestamp = %s""", (timestamp,))

                record_count = dest_cur.fetchone()[0]

                if record_count > 0:
                     # Update the existing record
                   update_query = """UPDATE gridenergymonthly SET total_cummulative_sum = %s WHERE timestamp = %s"""
                   dest_cur.execute(update_query, (total_energy_difference,timestamp,))
                else:
                    # Insert a new record
                   insert_query = """INSERT INTO gridenergymonthly (total_cummulative_sum,timestamp) VALUES (%s,%s)"""
                   dest_cur.execute(insert_query, (total_energy_difference,timestamp,))

                dest_db.commit()

            dest_cur.close()
            dest_db.close()
            source_cur.close()
            source_db.close()
            
        if current_minute in [21,24,36,46,56,6]:
            source_db = source_pool.get_connection()
            source_cur = source_db.cursor()
            query = "SELECT MAX(polledTime) AS max_polledTime,ROUND(SUM(energy_difference) * 1000, 2) AS total_energy_difference FROM (SELECT DGNum,dgrealenergy - LAG(dgrealenergy, 1) OVER (PARTITION BY DGNum ORDER BY polledTime) AS energy_difference,polledTime FROM DGPolling WHERE DGNum IN (1, 2, 3, 4, 5) AND DATE(polledTime) = CURDATE() ORDER BY polledTime DESC) AS subquery;"
            source_cur.execute(query)

            #SELECT SUM(energy_diff) FROM (SELECT coolingenergy_consumption - LAG(coolingenergy_consumption) OVER (ORDER BY received_time DESC) AS energy_diff FROM EMSThermalstorage WHERE received_time >= NOW() - INTERVAL 15 MINUTE) AS energy_diff_query;

            # Fetch all the rows from the source database
            rows = source_cur.fetchall()

            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []

            # Loop through the rows and append the values to the insert_rows list
            for row in rows:
                max_polledTime,total_energy_difference = row
                insert_rows.append((max_polledTime,total_energy_difference))
                print("-----------------------------------------------------------------------------------------------------------")
                print(row)
                print("DISEL ENERGY TODAY")
            
            dest_db = dest_pool.get_connection()
            dest_cur = dest_db.cursor()

            # Insert the rows into the destination database
            insert_query = "INSERT INTO diselenergy(polled_time,total_energy_difference) VALUES (%s,%s);"
            dest_cur.executemany(insert_query, insert_rows)
            dest_db.commit()
            source_cur.close()
            source_db.close()
            dest_cur.close()
            dest_db.close()
            
        if current_hour == 1 and current_minute == 34:



            source_db = source_pool.get_connection()
            source_cur = source_db.cursor()
            query = "SELECT SUM(energy_difference)*1000 AS total_energy_difference, polledTime FROM (SELECT DGNum, dgrealenergy - LAG(dgrealenergy, 1) OVER (PARTITION BY DGNum ORDER BY polledTime) AS energy_difference, polledTime FROM DGPolling WHERE DGNum IN (1,2,3,4,5) AND DATE(polledTime) = DATE_SUB(CURDATE(), INTERVAL 1 DAY) ORDER BY polledTime DESC) AS subquery;"
            source_cur.execute(query)

            #SELECT SUM(energy_diff) FROM (SELECT coolingenergy_consumption - LAG(coolingenergy_consumption) OVER (ORDER BY received_time DESC) AS energy_diff FROM EMSThermalstorage WHERE received_time >= NOW() - INTERVAL 15 MINUTE) AS energy_diff_query;

            # Fetch all the rows from the source database
            rows = source_cur.fetchall()

            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []

            # Loop through the rows and append the values to the insert_rows list
            for row in rows:
                total_energy_difference,timestamp = row
                if timestamp.hour == 23 and timestamp.minute > 55:
                   timestamp = timestamp.replace(hour=00, minute=00, second=0)  # Set hour to 24, minute and second to 0
                elif timestamp.minute > 30:
                   timestamp = timestamp  # Add 1 hour to the timestam
                   timestamp = timestamp.replace(minute=0, second=0)
                print(timestamp)
                insert_rows.append((total_energy_difference,timestamp))
                print("-----------------------------------------------------------------------------------------------------------")
                print(row)
                print("DISEL ENERGY DAY WISE")
          
            
            dest_db = dest_pool.get_connection()
            dest_cur = dest_db.cursor()

            # Insert the rows into the destination databas
            insert_query = "INSERT INTO diselenergydaywise(total_energy_difference,timestamp) VALUES (%s,%s);"
            dest_cur.executemany(insert_query, insert_rows)
            dest_db.commit()
            source_cur.close()
            source_db.close()
            dest_cur.close()
            dest_db.close()
    
        if current_time.day ==1 and current_time.minute == 28:
         
            print("disel ENERGY monthly")
            source_db = source_pool.get_connection()
            source_cur = source_db.cursor()
            query = "SELECT ROUND(SUM(energy_difference) * 1000, 2) AS total_energy_difference, MIN(polledTime) AS min_polledTime FROM (SELECT DGNum, dgrealenergy - LAG(dgrealenergy, 1) OVER (PARTITION BY DGNum ORDER BY polledTime) AS energy_difference, polledTime FROM DGPolling WHERE DGNum IN (1, 2, 3, 4, 5) AND ((YEAR(polledTime) = YEAR(CURDATE()) AND MONTH(polledTime) = MONTH(CURDATE()) - 1) OR (YEAR(polledTime) = YEAR(CURDATE()) - 1 AND MONTH(polledTime) = 12)) ORDER BY polledTime DESC) AS subquery;"
            source_cur.execute(query)
            # Fetch all the rows from the source database
            rows = source_cur.fetchall()
            print(rows)
            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []
            
            # Loop through the rows and append the values to the insert_rows li
            for row in rows:
                total_energy_difference,min_polledTime = row
                insert_rows.append((total_energy_difference,min_polledTime))
                
                
              
            
            dest_db = dest_pool.get_connection()
            dest_cur = dest_db.cursor()

            for row in insert_rows:
                total_energy_difference,min_polledTime = row
                # Check if a record with the same polledTime exists
                dest_cur.execute("""SELECT COUNT(*) FROM diselenergymonthly WHERE timestamp = %s""", (min_polledTime,))

                record_count = dest_cur.fetchone()[0]

                if record_count > 0:
                     # Update the existing record
                   update_query = """UPDATE diselenergymonthly SET total_cummulative_sum = %s WHERE timestamp = %s"""
                   dest_cur.execute(update_query, (total_energy_difference,min_polledTime,))
                else:
                    # Insert a new record
                   insert_query = """INSERT INTO diselenergymonthly (total_cummulative_sum,timestamp) VALUES (%s,%s)"""
                   dest_cur.execute(insert_query, (total_energy_difference,min_polledTime,))

                dest_db.commit()

            dest_cur.close()
            dest_db.close()
            source_cur.close()
            source_db.close() 
            
        
        if current_hour == 1 and current_minute == 32:
            
            source_db = source_pool.get_connection()
            source_cur = source_db.cursor()

           

            
            query = "SELECT MAX(totalApparentPower2), MIN(polledTime) FROM hvacSchneider7230Polling WHERE DATE(polledTime) = CURDATE() - INTERVAL 1 DAY;"
            source_cur.execute(query)
            # Fetch all the rows from the source database
            rows = source_cur.fetchall()
            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []
            # Loop through the rows and append the values to the insert_rows lis
            for row in rows:
                totalApparentPower2,polledTime= row
                insert_rows.append((totalApparentPower2,polledTime))
                print("-----------------------------------------------------------------------------------------------------------")
                print(row)
            dest_db = dest_pool.get_connection()
            dest_cur = dest_db.cursor()
                # Insert the rows into the destination database
            insert_query = "INSERT INTO peakdemanddaily(peakdemand,polledTime) VALUES (%s, %s);"
            dest_cur.executemany(insert_query, insert_rows)
            dest_db.commit()
            source_cur.close()
            source_db.close()
            dest_cur.close()
            dest_db.close()
   
        if current_hour == 2 and current_minute ==55:
         
            print("BLOCKWISE ENERGY HOURLY")
            source_db = source_pool.get_connection()
            source_cur = source_db.cursor()
            query = "SELECT DATE_FORMAT(polledTime, '%Y-%m-%d %H:00:00') AS hour_interval, SUM(CASE WHEN mvpnum IN ('A-Block1', 'A-Block2') THEN cummulativeenergy END) AS ABLOCK, SUM(CASE WHEN mvpnum = 'B-Block' THEN cummulativeenergy END) AS cumulativeenergy_B_Block, SUM(CASE WHEN mvpnum = 'D-Block' THEN cummulativeenergy END) AS cumulativeenergy_D_Block, SUM(CASE WHEN mvpnum IN ('C-Block-North', 'C-Block-South') THEN cummulativeenergy END) AS CBLOCK, SUM(CASE WHEN mvpnum IN ('MLCP1', 'MLCP2') THEN cummulativeenergy END) AS MLCP,SUM(CASE WHEN mvpnum IN ('Auditorium') THEN cummulativeenergy END) AS Auditorium,MAX(last_polledTime) AS last_polledTime FROM (SELECT mvpnum, SUM(energy_difference) AS cummulativeenergy, MAX(polledTime) AS last_polledTime, polledTime FROM (SELECT mvpnum, acmeterenergy - LAG(acmeterenergy) OVER (PARTITION BY mvpnum ORDER BY polledTime) AS energy_difference, polledTime FROM MVPPolling WHERE DATE(polledTime) = CURDATE()-1 AND mvpnum IN ('A-Block1', 'A-Block2', 'B-Block', 'C-Block-North', 'C-Block-South', 'D-Block', 'MLCP1', 'MLCP2','Auditorium')) AS subquery WHERE energy_difference IS NOT NULL GROUP BY mvpnum, polledTime) AS pivot_data GROUP BY hour_interval;"
            source_cur.execute(query)
            # Fetch all the rows from the source database
            rows = source_cur.fetchall()
            print(rows)
            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []
            
            # Loop through the rows and append the values to the insert_rows li
            for row in rows:
                timestamp,ablock,bblock,dblock,cblock,mlcp,Auditorium,times = row
                insert_rows.append((timestamp,ablock,bblock,dblock,cblock,mlcp,Auditorium))
                
                
              
            
            dest_db = dest_pool.get_connection()
            dest_cur = dest_db.cursor()

            for row in insert_rows:
                timestamp,ablock,bblock,dblock,cblock,mlcp,Auditorium = row
                # Check if a record with the same polledTime exists
                dest_cur.execute("""SELECT COUNT(*) FROM blockwisehourly WHERE timestamp = %s""", (timestamp,))

                record_count = dest_cur.fetchone()[0]

                if record_count > 0:
                     # Update the existing record
                   update_query = """UPDATE blockwisehourly SET ABLOCK = %s,BBlock=%s,CBLOCK=%s,DBLOCK=%s,MLCP=%s,auditorium=%s WHERE timestamp = %s"""
                   dest_cur.execute(update_query, (ablock,bblock,cblock,dblock,mlcp,Auditorium,timestamp))
                else:
                    # Insert a new record
                   insert_query = """INSERT INTO blockwisehourly (ABLOCK,BBlock,CBLOCK,DBLOCK,MLCP,auditorium,timestamp) VALUES (%s, %s,%s,%s,%s,%s,%s)"""
                   dest_cur.execute(insert_query, (ablock,bblock,cblock,dblock,mlcp,Auditorium,timestamp))

                dest_db.commit()

            dest_cur.close()
            dest_db.close()
            source_cur.close()
        
            source_db.close() 

        if current_hour == 1 and current_minute ==42:
         
            print("BLOCK ENERGY fifteen minutes")
            source_db = source_pool.get_connection()
            source_cur = source_db.cursor()
            query = "SELECT CONCAT(DATE_FORMAT(polledTime, '%Y-%m-%d %H:'), LPAD(15 * (MINUTE(polledTime) DIV 15), 2, '00'), ':00') AS interval_15_minutes, SUM(CASE WHEN mvpnum IN ('A-Block1', 'A-Block2') THEN cummulativeenergy END) AS ABLOCK, SUM(CASE WHEN mvpnum = 'B-Block' THEN cummulativeenergy END) AS cumulativeenergy_B_Block, SUM(CASE WHEN mvpnum = 'D-Block' THEN cummulativeenergy END) AS cumulativeenergy_D_Block, SUM(CASE WHEN mvpnum IN ('C-Block-North', 'C-Block-South') THEN cummulativeenergy END) AS CBLOCK, SUM(CASE WHEN mvpnum IN ('MLCP1', 'MLCP2') THEN cummulativeenergy END) AS MLCP,SUM(CASE WHEN mvpnum IN ('Auditorium') THEN cummulativeenergy END) AS Auditorium, MAX(last_polledTime) AS last_polledTime FROM (SELECT mvpnum, SUM(energy_difference) AS cummulativeenergy, MAX(polledTime) AS last_polledTime, polledTime FROM (SELECT mvpnum, acmeterenergy - LAG(acmeterenergy) OVER (PARTITION BY mvpnum ORDER BY polledTime) AS energy_difference, polledTime FROM MVPPolling WHERE date(polledTime) = CURDATE()-1 AND mvpnum IN ('A-Block1', 'A-Block2', 'B-Block', 'C-Block-North', 'C-Block-South', 'D-Block', 'MLCP1', 'MLCP2','Auditorium')) AS subquery WHERE energy_difference IS NOT NULL GROUP BY mvpnum, HOUR(polledTime), MINUTE(polledTime) DIV 15) AS pivot_data GROUP BY interval_15_minutes;"
            source_cur.execute(query)
            # Fetch all the rows from the source database
            rows = source_cur.fetchall()
            print(rows)
            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []
            
            # Loop through the rows and append the values to the insert_rows li
            for row in rows:
                timestamp,ablock,bblock,dblock,cblock,mlcp,Auditorium,times = row
                insert_rows.append((timestamp,ablock,bblock,dblock,cblock,mlcp,Auditorium))
                
                
              
            
            dest_db = dest_pool.get_connection()
            dest_cur = dest_db.cursor()

            for row in insert_rows:
                timestamp,ablock,bblock,dblock,cblock,mlcp,Auditorium = row
                # Check if a record with the same polledTime exists
                dest_cur.execute("""SELECT COUNT(*) FROM BLOCK_WISE_ENERGY WHERE timestamp = %s""", (timestamp,))

                record_count = dest_cur.fetchone()[0]

                if record_count > 0:
                     # Update the existing record
                   update_query = """UPDATE BLOCK_WISE_ENERGY SET AbLOCK = %s,Bblock =%s,Cblock=%s,Dblock=%s,MLCP=%s,auditorium=%s WHERE timestamp = %s"""
                   dest_cur.execute(update_query, (ablock,bblock,cblock,dblock,mlcp,Auditorium,timestamp))
                else:
                    # Insert a new record
                   insert_query = """INSERT INTO BLOCK_WISE_ENERGY (AbLOCK,Bblock,Cblock,Dblock,MLCP,auditorium,timestamp) VALUES (%s,%s,%s,%s,%s,%s,%s)"""
                   dest_cur.execute(insert_query, (ablock,bblock,cblock,dblock,mlcp,Auditorium,timestamp))

                dest_db.commit()

            dest_cur.close()
            dest_db.close()
            source_cur.close()
            source_db.close()
      
        if current_hour == 1 and current_minute ==34:
         
            print("peakdemand quarter")
            source_db = source_pool.get_connection()
            source_cur = source_db.cursor()
            query = "SELECT CONCAT(DATE_FORMAT(polledTime, '%Y-%m-%d %H:'), LPAD(15 * (MINUTE(polledTime) DIV 15), 2, '00'), ':00') AS interval_15_minutes, ROUND(MAX(totalApparentPower2), 2) AS max_totalApparentPower2, MAX(polledTime) AS min_polledTime FROM hvacSchneider7230Polling WHERE DATE(polledTime) = CURDATE()-1 GROUP BY interval_15_minutes;"
            source_cur.execute(query)
            # Fetch all the rows from the source database
            rows = source_cur.fetchall()
            print(rows)
            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []
            
            # Loop through the rows and append the values to the insert_rows li
            for row in rows:
                timestamp,max_totalApparentPower2,TIMES = row
                insert_rows.append((timestamp,max_totalApparentPower2))
                
                
              
            
            dest_db = dest_pool.get_connection()
            dest_cur = dest_db.cursor()

            for row in insert_rows:
                timestamp,max_totalApparentPower2 = row
                # Check if a record with the same polledTime exists
                dest_cur.execute("""SELECT COUNT(*) FROM peakdemandquarter WHERE polledTime = %s""", (timestamp,))

                record_count = dest_cur.fetchone()[0]

                if record_count > 0:
                     # Update the existing record
                   update_query = """UPDATE peakdemandquarter SET peakdemand = %s WHERE polledTime = %s"""
                   dest_cur.execute(update_query, (max_totalApparentPower2,timestamp))
                else:
                    # Insert a new record
                   insert_query = """INSERT INTO peakdemandquarter (peakdemand,polledTime) VALUES (%s,%s)"""
                   dest_cur.execute(insert_query, (max_totalApparentPower2,timestamp))

                dest_db.commit()

            dest_cur.close()
            dest_db.close()
            source_cur.close()
            source_db.close()

        if current_hour == 1 and current_minute ==55:
         
            print("GRID ENERGY HOURLY")
            source_db = source_pool.get_connection()
            source_cur = source_db.cursor()
            query = "SELECT DATE_FORMAT(polledTime, '%Y-%m-%d %H:00:00') AS hourly_time, SUM(energy_difference) / 1000 AS cumulative_energy, MAX(polledTime) AS last_polledTime FROM (SELECT mvpnum, acmeterenergy - LAG(acmeterenergy) OVER (PARTITION BY mvpnum ORDER BY polledTime) AS energy_difference, polledTime FROM MVPPolling WHERE DATE(polledTime) = CURDATE()-1 AND mvpnum IN ('MVP1', 'MVP2', 'MVP3', 'MVP4')) subquery WHERE energy_difference IS NOT NULL GROUP BY hourly_time ORDER BY hourly_time;"
            source_cur.execute(query)
            # Fetch all the rows from the source database
            rows = source_cur.fetchall()
            print(rows)
            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []
            
            # Loop through the rows and append the values to the insert_rows li
            for row in rows:
                timestamp,cumulative_energy,TIMES = row
                insert_rows.append((timestamp,cumulative_energy))
                
                
              
            
            dest_db = dest_pool.get_connection()
            dest_cur = dest_db.cursor()

            for row in insert_rows:
                timestamp,cumulative_energy = row
                # Check if a record with the same polledTime exists
                dest_cur.execute("""SELECT COUNT(*) FROM gridenergyhourly WHERE timestamp = %s""", (timestamp,))

                record_count = dest_cur.fetchone()[0]

                if record_count > 0:
                     # Update the existing record
                   update_query = """UPDATE gridenergyhourly SET cumulative_energy = %s WHERE timestamp = %s"""
                   dest_cur.execute(update_query, (cumulative_energy,timestamp))
                else:
                    # Insert a new record
                   insert_query = """INSERT INTO gridenergyhourly (cumulative_energy,timestamp) VALUES (%s,%s)"""
                   dest_cur.execute(insert_query, (cumulative_energy,timestamp))

                dest_db.commit()

            dest_cur.close()
            dest_db.close()
            source_cur.close()
            source_db.close() 

        if current_hour == 3 and current_minute ==55:
         
            print("diselenergy ENERGY fifteen minutes")
            source_db = source_pool.get_connection()
            source_cur = source_db.cursor()
            query = "SELECT CONCAT(DATE_FORMAT(polledTime, '%Y-%m-%d %H:'), LPAD(15 * (MINUTE(polledTime) DIV 15), 2, '00'), ':00') AS interval_15_minutes, ROUND(SUM(CASE WHEN DGNum = 1 THEN energy_difference ELSE 0 END) * 1000, 2) AS DGNum_1_energy_difference, ROUND(SUM(CASE WHEN DGNum = 2 THEN energy_difference ELSE 0 END) * 1000, 2) AS DGNum_2_energy_difference, ROUND(SUM(CASE WHEN DGNum = 3 THEN energy_difference ELSE 0 END) * 1000, 2) AS DGNum_3_energy_difference, ROUND(SUM(CASE WHEN DGNum = 4 THEN energy_difference ELSE 0 END) * 1000, 2) AS DGNum_4_energy_difference, ROUND(SUM(CASE WHEN DGNum = 5 THEN energy_difference ELSE 0 END) * 1000, 2) AS DGNum_5_energy_difference, ROUND(SUM(energy_difference) * 1000, 2) AS Total_energy_difference, MAX(polledTime) AS min_polledTime FROM (SELECT DGNum, dgrealenergy - LAG(dgrealenergy, 1) OVER (PARTITION BY DGNum ORDER BY polledTime) AS energy_difference, polledTime FROM DGPolling WHERE DATE(polledTime) = CURDATE()-1 AND DGNum IN (1, 2, 3, 4, 5) ORDER BY polledTime DESC) AS subquery GROUP BY interval_15_minutes ORDER BY interval_15_minutes;"
            source_cur.execute(query)
            # Fetch all the rows from the source database
            rows = source_cur.fetchall()
            print(rows)
            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []
            
            # Loop through the rows and append the values to the insert_rows li
            for row in rows:
                timestamp,DGNum_1_energy_difference,DGNum_2_energy_difference,DGNum_3_energy_difference,DGNum_4_energy_difference,DGNum_5_energy_difference,Total_energy_difference,max_polledTime = row
                insert_rows.append((timestamp,DGNum_1_energy_difference,DGNum_2_energy_difference,DGNum_3_energy_difference,DGNum_4_energy_difference,DGNum_5_energy_difference,Total_energy_difference))
                
                
              
            
            dest_db = dest_pool.get_connection()
            dest_cur = dest_db.cursor()

            for row in insert_rows:
                timestamp,DGNum_1_energy_difference,DGNum_2_energy_difference,DGNum_3_energy_difference,DGNum_4_energy_difference,DGNum_5_energy_difference,Total_energy_difference = row
                # Check if a record with the same polledTime exists
                dest_cur.execute("""SELECT COUNT(*) FROM diselenergyquaterly WHERE timestamp = %s""", (timestamp,))

                record_count = dest_cur.fetchone()[0]

                if record_count > 0:
                     # Update the existing record
                   update_query = """UPDATE diselenergyquaterly SET DGNum_1_energy_difference = %s,DGNum_2_energy_difference =%s,DGNum_3_energy_difference=%s,DGNum_4_energy_difference=%s,DGNum_5_energy_difference =%s,total_energy_difference=%s WHERE timestamp = %s"""
                   dest_cur.execute(update_query, (DGNum_1_energy_difference,DGNum_2_energy_difference,DGNum_3_energy_difference,DGNum_4_energy_difference,DGNum_5_energy_difference,Total_energy_difference,timestamp))
                else:
                    # Insert a new record
                   insert_query = """INSERT INTO diselenergyquaterly (DGNum_5_energy_difference,DGNum_4_energy_difference,DGNum_3_energy_difference,DGNum_2_energy_difference,DGNum_1_energy_difference,Total_energy_difference,timestamp) VALUES (%s,%s,%s,%s,%s,%s,%s)"""
                   dest_cur.execute(insert_query, (DGNum_5_energy_difference,DGNum_4_energy_difference,DGNum_3_energy_difference,DGNum_2_energy_difference,DGNum_1_energy_difference,Total_energy_difference,timestamp)),

                dest_db.commit()

            dest_cur.close()
            dest_db.close()
            source_cur.close()
            source_db.close()

       
        if current_hour == 3 and current_minute ==23:
         
            print("grid energy fifteen minute")
            source_db = source_pool.get_connection()
            source_cur = source_db.cursor()
            query = "SELECT CONCAT(DATE_FORMAT(polledTime, '%Y-%m-%d %H:'), LPAD(15 * (MINUTE(polledTime) DIV 15), 2, '00'), ':00') AS interval_15_minutes, SUM(energy_difference) / 1000 AS cumulative_energy, MAX(polledTime) AS last_polledTime FROM (SELECT mvpnum, acmeterenergy - LAG(acmeterenergy) OVER (PARTITION BY mvpnum ORDER BY polledTime) AS energy_difference, polledTime FROM MVPPolling WHERE DATE(polledTime) = CURDATE()-1 AND mvpnum IN ('MVP1', 'MVP2', 'MVP3', 'MVP4')) subquery WHERE energy_difference IS NOT NULL GROUP BY interval_15_minutes ORDER BY interval_15_minutes;"
            source_cur.execute(query)
            # Fetch all the rows from the source database
            rows = source_cur.fetchall()
            print(rows)
            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []
            
            # Loop through the rows and append the values to the insert_rows li
            for row in rows:
                timestamp,cumulative_energy,TIMES = row
                insert_rows.append((timestamp,cumulative_energy))
                
                
              
            
            dest_db = dest_pool.get_connection()
            dest_cur = dest_db.cursor()

            for row in insert_rows:
                timestamp,cumulative_energy = row
                # Check if a record with the same polledTime exists
                dest_cur.execute("""SELECT COUNT(*) FROM gridenergyquarterly WHERE timestamp = %s""", (timestamp,))

                record_count = dest_cur.fetchone()[0]

                if record_count > 0:
                     # Update the existing record
                   update_query = """UPDATE gridenergyquarterly SET cumulative_energy = %s WHERE timestamp = %s"""
                   dest_cur.execute(update_query, (cumulative_energy,timestamp))
                else:
                    # Insert a new recor
                   insert_query = """INSERT INTO gridenergyquarterly (cumulative_energy,timestamp) VALUES (%s,%s)"""
                   dest_cur.execute(insert_query, (cumulative_energy,timestamp))

                dest_db.commit()

            dest_cur.close()
            dest_db.close()
            source_cur.close()
            source_db.close()
                 
        if current_hour == 4 and current_minute == 38:
            

            source_db = source_pool.get_connection()
            source_cur = source_db.cursor()
         
            
            
            # Execute the query
            query = "SELECT (SELECT SUM(total_energy_difference / 1000) FROM (SELECT mvpnum, SUM(energy_difference) AS total_energy_difference FROM (SELECT mvpnum, acmeterenergy - LAG(acmeterenergy) OVER (PARTITION BY mvpnum ORDER BY polledTime) AS energy_difference FROM MVPPolling WHERE polledTime >= DATE_SUB(CURDATE(), INTERVAL 1 DAY) AND polledTime < CURDATE() AND mvpnum IN ('MVP1', 'MVP2', 'MVP3', 'MVP4')) subquery WHERE energy_difference IS NOT NULL GROUP BY mvpnum) t) AS cumulative_energy, MAX(polledTime) AS last_polledTime FROM MVPPolling WHERE DATE(polledTime) = DATE_SUB(CURDATE(), INTERVAL 1 DAY) AND mvpnum IN ('MVP1', 'MVP2', 'MVP3', 'MVP4') ORDER BY polledTime DESC;"
            source_cur.execute(query)

            # Fetch all the rows from th source database
            rows = source_cur.fetchall()

            # Create a list to sore the rows to be inserted into the destination database
            insert_rows = []

            # Loop through the row and append the values to the insert_rows list
            for row in rows:
                cumulative_energy,last_polled_time= row
                # Set minute and second to 00
                insert_rows.append((cumulative_energy,last_polled_time))
                print("-----------------------------------------------------------------------------------------------------------")
                print(row)
                print("GRID ENERGY DAY WISE")
                
            dest_db = dest_pool.get_connection()
            dest_cur = dest_db.cursor()

            # Insert the rows into the destination database
            insert_query = "INSERT INTO gridenergydaywise(cumulative_energy,timestamp) VALUES (%s,%s);"
            dest_cur.executemany(insert_query, insert_rows)
            dest_db.commit()
            source_cur.close()
            source_db.close()
            dest_cur.close()
            dest_db.close()
         

               
   

      
   



  
  


        time.sleep(60) 

    except Exception as e:
        print("An error occurred: ", e)
        time.sleep(12)
