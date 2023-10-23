# Importing Mysql
from pymysql import Error
import pandas as pd
import test

# Reading data from csv file
flight_df = pd.read_csv("flights_data.csv")
flight_df = flight_df.dropna()

def sqlconnect():
    try:
        # Connecting to database
        connection = test.sqlconnectjson()
        print("Connection made to database", test.sqlconnectjson().record)
            # Creating tables in Database
        flight_details_table = "CREATE TABLE IF NOT EXISTS flight_details(id int, flight_num int, dep_time int, " \
                                   "sched_dep_time int, arr_time int, sched_arr_time int, carrier varchar(10), " \
                                   "origin varchar(10), dest varchar(10), air_time int, distance int, PRIMARY KEY(id))"
        test.sqlconnectjson().cursor.execute(flight_details_table)
        print("Table created successfully")
            #####Inserting into table
        for i, row in flight_df.iterrows():
                            # here %S means string values
            sql = "INSERT INTO project.flight_details VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            print(tuple(row))
            test.sqlconnectjson().cursor.execute(sql, tuple(row))
            print("Record inserted")
            connection.commit()

    except Error as e:
        print("Error in connecting to database", e)

    finally:
        connection.close()

sqlconnect()