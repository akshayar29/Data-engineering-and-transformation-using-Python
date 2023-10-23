# Importing Mysql
import pymysql
from pymysql import Error
import pandas as pd
from sqlalchemy import create_engine
import json

# Reading data from csv file
flight_dep_df = pd.read_excel("flights_dep_arr_data.xlsx", sheet_name="dep")
flight_arr_df = pd.read_excel("flights_dep_arr_data.xlsx", sheet_name="arr")


class SqlTables:

    def joindf(self):
        flight_dep_arr_df = pd.merge(flight_dep_df, flight_arr_df, on='Id')
        self.flights_df = flight_dep_arr_df[pd.notnull(flight_dep_arr_df['dest'])]
        self.flights_df.to_csv("flightcommute_data.csv", index=False)
        missing_df = flight_dep_arr_df[pd.isnull(flight_dep_arr_df['dest'])]
        print(self.flights_df)
        print(missing_df)
        missing_df.to_csv("flights_missing_data.csv", index=False)

    def sqlconnect(self):

        try:
            # Connecting to database
            with open('usercredentials.json', 'r') as file:
                self.data = json.load(file)
            self.connection = pymysql.connect(host=self.data['userdetails']['host'],
                                         user=self.data['userdetails']['username'],
                                         password=self.data['userdetails']['password'],
                                         db=self.data['userdetails']['database'])
            self.engine = create_engine('mysql+pymysql://', creator=lambda: self.connection)
            self.cursor = self.connection.cursor()
            self.cursor.execute("select database();")
            record = self.cursor.fetchone()
            print("Connection made to database", record)
            return self.cursor

        except Error as e:
            print("Error in connecting to database", e)

    def createtables(self):

        table = 'flight_commute'
        # Check if table exists in database
        check_table = f"SHOW TABLES LIKE '{table}'"
        self.cursor.execute(check_table)
        table_exists = self.cursor.fetchone() is not None
        if table_exists:
            sqlquery = "SELECT * from project.flight_commute"
            old_df = pd.read_sql(sqlquery, con=self.engine)
            df = pd.merge(self.flights_df,old_df, on=['Id','flight_num','dep_time','sched_dep_time','carrier','air_time','arr_time','sched_arr_time','origin','dest','distance'], how='left',indicator=True)
            print(df)
            extra_rows = df[df["_merge"]=='left_only'].drop('_merge', axis=1)
            print(extra_rows)
            extra_rows.to_sql(name=table, con=self.engine, if_exists='append', index=False)


        else:
            # Creating tables in Database
            flight_commute = "CREATE TABLE IF NOT EXISTS flight_commute(Id int, flight_num int, dep_time int, " \
                             "sched_dep_time int, carrier varchar(10), air_time int,arr_time int, " \
                             "sched_arr_time int,origin varchar(10), dest varchar(10), distance int, PRIMARY KEY(id))"
            self.cursor.execute(flight_commute)
            print("Table 'flight_commute' created successfully")

            #####Inserting into flight_commute table
            flightcommute_df = self.flights_df
            table_name = 'flight_commute'
            flightcommute_df.to_sql(name=table_name, con=self.engine, if_exists='append', index=False)

    def jsonlistmanipulate(self):
        list1 = self.data["tablelist1"]
        list2 = self.data["tablelist2"]
        #Using list comprehension
        table_name = [table for table in list1 for table2 in list2 if table == table2]
        if table_name:
            print("The matched table name is:" + table_name[0])
        else:
            print("There are no matching tables")

        #Using nested for loop
        #for i in list1:
            #for j in list2:
                #if i==j:
                   # table_name = i
                    #print("The matched table name is:" + str(table_name))
                #else:
                    #print("There are no matching tables")
        sqlquery = f"SELECT * from {table_name[0]}"
        self.cursor.execute(sqlquery)
        records = self.cursor.fetchall()
        print(records)
    def sqldisconnect(self):
        self.connection.close()


obj1 = SqlTables()
#obj1.joindf()
obj1.sqlconnect()
#obj1.createtables()
obj1.jsonlistmanipulate()
obj1.sqldisconnect()
