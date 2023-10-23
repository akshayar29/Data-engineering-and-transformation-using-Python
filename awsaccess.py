import pymysql
from pymysql import Error
import pandas as pd
from sqlalchemy import create_engine
import json
import boto3
from io import StringIO


class Aws:

    def jsonacess(self):
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket="pyproject1", Key="usercredentials.json")
        json_data = json.loads(response['Body'].read())
        self.data = json_data['userdetails']

    def sqlconnect(self):
        try:
            # Connecting to database
            self.connection = pymysql.connect(host=self.data['host'],
                                              user=self.data['username'],
                                              password=self.data['password'],
                                              db=self.data['database'])
            self.engine = create_engine('mysql+pymysql://', creator=lambda: self.connection)
            self.cursor = self.connection.cursor()
            self.cursor.execute("select database();")
            record = self.cursor.fetchone()
            print("Connection made to database", record)

        except Error as e:
            print("Error in connecting to database", e)

    def loaddata(self):
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket="pyproject1", Key="flightcommute_data.csv")
        csv_file = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_file))
        print(df)
        df.to_sql(name='flight_commute', con=self.engine, if_exists='replace', index=False)
        print("Records inserted successfully")

    def sqldisconnect(self):
            self.connection.close()

#Initializing class object
obj = Aws()
obj.jsonacess()
obj.sqlconnect()
obj.loaddata()
obj.sqldisconnect()
