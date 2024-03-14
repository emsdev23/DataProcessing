import pandas as pd
import mysql.connector
import time

while True:
    processeddb = mysql.connector.connect(
                        host="121.242.232.151",
                        user="emsrouser",
                        password="emsrouser@151",
                        database='bmsmgmt_olap_prod_v13',
                        port=3306
                        )

    awsdb = mysql.connector.connect(
                        host="43.205.196.66",
                        user="emsroot",
                        password="22@teneT",
                        database='EMS',
                        port=3307
                    )

    awscur = awsdb.cursor()

    awscur.execute("select polledTime from EMS.DieselMinWise where date(polledTime) = curdate() order by polledTime desc limit 1;")

    dateres = awscur.fetchall()

    if len(dateres) !=0:
    
        dated = str(dateres[0][0])

        query = f"""SELECT polledTime,dgrealenergy*1000 as Energy,DGNum FROM bmsmgmt_olap_prod_v13.DGPolling where polledTime >= '{dated}';"""
    
    else:
        query = f"""SELECT polledTime,dgrealenergy*1000 as Energy,DGNum FROM bmsmgmt_olap_prod_v13.DGPolling where date(polledTime) >=  curdate();"""
    
    df = pd.read_sql_query(query,processeddb)

    df['polledTime'] = pd.to_datetime(df['polledTime']).dt.strftime('%Y-%m-%d %H:%M:00')

    df['Diff'] = df.groupby('DGNum')['Energy'].diff()

    DG_df = df.pivot_table(index='polledTime', columns='DGNum', values='Diff', aggfunc='sum').reset_index()
    DG_df[1].fillna(0, inplace=True)
    DG_df[2].fillna(0, inplace=True)
    DG_df[3].fillna(0, inplace=True)
    DG_df[4].fillna(0, inplace=True)
    DG_df[5].fillna(0, inplace=True)

    for index, row in DG_df.iterrows():
        val = (row['polledTime'],row[1],row[2],row[3],row[4],row[5])
        sql = "INSERT INTO EMS.DieselMinWise(polledTime,DG1 ,DG2 ,DG3 ,DG4, DG5) values(%s,%s,%s,%s,%s,%s)"
        try:
            awscur.execute(sql,val)
            awsdb.commit()
            print(val)
            print("Diesel Min Inserted")
        except mysql.connector.errors.IntegrityError:
            continue

    time.sleep(120)
