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

    emscur.execute("SELECT polledTime FROM EMS.fiveMinData where date(polledTime) = curdate() and wheeled >= 0 order by polledTime desc limit 1;")

    wheeled_time_res = emscur.fetchall()

    if len(wheeled_time_res) !=0:
        wheeled_time = wheeled_time_res[0][0]

        print("Last Wheeled Time",wheeled_time)
        

        emscur.execute(f"SELECT polledTime,wheeled FROM EMS.minWiseData where polledTime >= '{wheeled_time}'")
    
    else:
        emscur.execute(f"SELECT polledTime,wheeled FROM EMS.minWiseData where polledTime >= curdate()")

    wheeled_res = emscur.fetchall()

    emscur.execute("SELECT polledTime FROM EMS.fiveMinData where date(polledTime) = curdate() and grid >= 0 order by polledTime desc limit 1;")

    grid_time_res = emscur.fetchall()

    if len(grid_time_res) != 0:
    
        grid_time = grid_time_res[0][0]

        print("Last Wheeled Time",grid_time)

        emscur.execute(f"SELECT polledTime,grid FROM EMS.minWiseData where polledTime >= '{grid_time}'")
    
    else:
        emscur.execute(f"SELECT polledTime,grid FROM EMS.minWiseData where polledTime >= curdate()")

    grid_res = emscur.fetchall()

    dict_wheel = {}
    dict_grid = {}

    def segFiveMinGrid(polledTime,grid):
        polledTime = str(polledTime)
        mint = (int(polledTime[14:16]))

        if mint >=0 and mint <5:
            polledTime = polledTime[0:14]+"00:00"
            if polledTime in dict_grid.keys():
                dict_grid[polledTime].append(grid)
            else:
                dict_grid[polledTime] = [grid]

        if mint >=10 and mint <15:
            polledTime = polledTime[0:14]+"05:00"
            if polledTime in dict_grid.keys():
                dict_grid[polledTime].append(grid)
            else:
                dict_grid[polledTime] = [grid]
        
        if mint >=10 and mint <15:
            polledTime = polledTime[0:14]+"10:00"
            if polledTime in dict_grid.keys():
                dict_grid[polledTime].append(grid)
            else:
                dict_grid[polledTime] = [grid]
        
        if mint >=15 and mint <20:
            polledTime = polledTime[0:14]+"15:00"
            if polledTime in dict_grid.keys():
                dict_grid[polledTime].append(grid)
            else:
                dict_grid[polledTime] = [grid]
                
        if mint >=20 and mint <25:
            polledTime = polledTime[0:14]+"20:00"
            if polledTime in dict_grid.keys():
                dict_grid[polledTime].append(grid)
            else:
                dict_grid[polledTime] = [grid]
                
        if mint >=25 and mint <30:
            polledTime = polledTime[0:14]+"25:00"
            if polledTime in dict_grid.keys():
                dict_grid[polledTime].append(grid)
            else:
                dict_grid[polledTime] = [grid]
        
        if mint >=30 and mint <35:
            polledTime = polledTime[0:14]+"30:00"
            if polledTime in dict_grid.keys():
                dict_grid[polledTime].append(grid)
            else:
                dict_grid[polledTime] = [grid]
        
        if mint >=35 and mint <40:
            polledTime = polledTime[0:14]+"35:00"
            if polledTime in dict_grid.keys():
                dict_grid[polledTime].append(grid)
            else:
                dict_grid[polledTime] = [grid]
        
        if mint >=40 and mint <45:
            polledTime = polledTime[0:14]+"40:00"
            if polledTime in dict_grid.keys():
                dict_grid[polledTime].append(grid)
            else:
                dict_grid[polledTime] = [grid]
        
        if mint >=45 and mint <50:
            polledTime = polledTime[0:14]+"45:00"
            if polledTime in dict_grid.keys():
                dict_grid[polledTime].append(grid)
            else:
                dict_grid[polledTime] = [grid]
        
        if mint >=50 and mint <55:
            polledTime = polledTime[0:14]+"50:00"
            if polledTime in dict_grid.keys():
                dict_grid[polledTime].append(grid)
            else:
                dict_grid[polledTime] = [grid]
                
        if mint >=55 and mint <60:
            polledTime = polledTime[0:14]+"55:00"
            if polledTime in dict_grid.keys():
                dict_grid[polledTime].append(grid)
            else:
                dict_grid[polledTime] = [grid]

    def segFiveMinWheel(polledTime,wheel):
        polledTime = str(polledTime)
        mint = (int(polledTime[14:16]))

        if mint >=0 and mint <5:
            polledTime = polledTime[0:14]+"00:00"
            if polledTime in dict_wheel.keys():
                dict_wheel[polledTime].append(wheel)
            else:
                dict_wheel[polledTime] = [wheel]

        if mint >=5 and mint < 10:
            polledTime = polledTime[0:14]+"05:00"
            if polledTime in dict_wheel.keys():
                dict_wheel[polledTime].append(wheel)
            else:
                dict_wheel[polledTime] = [wheel]
        
        if mint >=10 and mint <15:
            polledTime = polledTime[0:14]+"10:00"
            if polledTime in dict_wheel.keys():
                dict_wheel[polledTime].append(wheel)
            else:
                dict_wheel[polledTime] = [wheel]
        
        
        if mint >=15 and mint <20:
            polledTime = polledTime[0:14]+"15:00"
            if polledTime in dict_wheel.keys():
                dict_wheel[polledTime].append(wheel)
            else:
                dict_wheel[polledTime] = [wheel]
                
        if mint >=20 and mint <25:
            polledTime = polledTime[0:14]+"20:00"
            if polledTime in dict_wheel.keys():
                dict_wheel[polledTime].append(wheel)
            else:
                dict_wheel[polledTime] = [wheel]
                
        if mint >=25 and mint <30:
            polledTime = polledTime[0:14]+"25:00"
            if polledTime in dict_wheel.keys():
                dict_wheel[polledTime].append(wheel)
            else:
                dict_wheel[polledTime] = [wheel]
        
        if mint >=30 and mint <35:
            polledTime = polledTime[0:14]+"30:00"
            if polledTime in dict_wheel.keys():
                dict_wheel[polledTime].append(wheel)
            else:
                dict_wheel[polledTime] = [wheel]
        
        if mint >=35 and mint <40:
            polledTime = polledTime[0:14]+"35:00"
            if polledTime in dict_wheel.keys():
                dict_wheel[polledTime].append(wheel)
            else:
                dict_wheel[polledTime] = [wheel]
        
        if mint >=40 and mint <45:
            polledTime = polledTime[0:14]+"40:00"
            if polledTime in dict_wheel.keys():
                dict_wheel[polledTime].append(wheel)
            else:
                dict_wheel[polledTime] = [wheel]
        
        if mint >=45 and mint <50:
            polledTime = polledTime[0:14]+"45:00"
            if polledTime in dict_wheel.keys():
                dict_wheel[polledTime].append(wheel)
            else:
                dict_wheel[polledTime] = [wheel]
        
        if mint >=50 and mint <55:
            polledTime = polledTime[0:14]+"50:00"
            if polledTime in dict_wheel.keys():
                dict_wheel[polledTime].append(wheel)
            else:
                dict_wheel[polledTime] = [wheel]
                
        if mint >=55 and mint <60:
            polledTime = polledTime[0:14]+"55:00"
            if polledTime in dict_wheel.keys():
                dict_wheel[polledTime].append(wheel)
            else:
                dict_wheel[polledTime] = [wheel]


    for i in wheeled_res:
        if i[1] != None:
            segFiveMinWheel(i[0],i[1])
    
    for i in grid_res:
        if i[1] != None:   
            segFiveMinGrid(i[0],i[1])


    for i in dict_wheel.keys():
        val = (i,round(sum(dict_wheel[i]),2))
        sql = "INSERT INTO EMS.fiveMinData(polledTime,wheeled) values(%s,%s)"
        print(val)
        try:
            emscur.execute(sql,val)
            emsdb.commit()
            print("Wheeled inserted")
        except mysql.connector.errors.IntegrityError:
            sql = "UPDATE EMS.fiveMinData SET wheeled = %s WHERE polledTime = %s"
            val = (round(sum(dict_wheel[i]),2),i)
            emscur.execute(sql,val)
            emsdb.commit()
            print("Wheeled Updated")

    for i in dict_grid.keys():
        val = (i,round(sum(dict_grid[i]),2))
        sql = "INSERT INTO EMS.fiveMinData(polledTime,grid) values(%s,%s)"
        print(val)
        try:
            emscur.execute(sql,val)
            emsdb.commit()
            print("Grid inserted")
        except mysql.connector.errors.IntegrityError:
            sql = "UPDATE EMS.fiveMinData SET grid = %s WHERE polledTime = %s"
            val = (round(sum(dict_grid[i]),2),i)
            emscur.execute(sql,val)
            emsdb.commit()
            print("Grid Updated")

    time.sleep(100)