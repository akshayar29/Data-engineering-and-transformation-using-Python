#Importing Mysql
import pymysql
from pymysql import Error
import yaml
import json

def sqlconnectyaml():
    global connection
    try:
        # Connecting to database
        #loading yaml file
        with open('usercredentials.yml','r') as file:
            credentials = yaml.safe_load(file)
        connection = pymysql.connect(host=credentials['user']['host'], user=credentials['user']['username'],
                                     password=credentials['user']['password'], db=credentials['user']['database'])
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("Connection made to database", record)
        sql = "SHOW TABLES from project"
        cursor.execute(sql)
        tables = cursor.fetchall()
        print(tables)
        return cursor
        return record

    except Error as e:
        print("Error in connecting to database", e)

    finally:
        connection.close()

def sqlconnectjson():

    try:
        # Connecting to database
        #loading yaml file
        with open('usercredentials.json','r') as file:
            credentials = json.load(file)
        connection = pymysql.connect(host=credentials['userdetails']['host'], user=credentials['userdetails']['username'],
                                     password=credentials['userdetails']['password'], db=credentials['userdetails']['database'])
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("Connection made to database", record)
        sql = "SHOW TABLES from project"
        cursor.execute(sql)
        tables = cursor.fetchall()
        print(tables)

    except Error as e:
        print("Error in connecting to database", e)


#sqlconnectyaml()
sqlconnectjson()