from pymysql import Error
import pandas as pd
import datetime
import test

class IncrementalUpdate:

    def __init__(self):
        self.src_df = None
        self.trgt_df = None

    def sqlconnect(self):
        try:
            # Connecting to database
            self.connection = test.sqlconnectjson()
            print("Connection made to database", test.sqlconnectjson().record)
            return self.cursor

        except Error as e:
            print("Error in connecting to database", e)
    def loaddata(self):
        self.src_df = pd.read_csv("flightcommute_data.csv")
        self.trgt_df = pd.read_sql("SELECT * from project.flight_commute", con=self.engine)
        print(self.src_df)
        print(self.trgt_df)

    def comparedf(self):
        #Extracting the new updates using left join
        newupdate_df = pd.merge(self.src_df, self.trgt_df,
                      on=['Id', 'flight_num', 'dep_time', 'sched_dep_time', 'carrier', 'air_time', 'arr_time',
                          'sched_arr_time', 'origin', 'dest', 'distance'], how='left', indicator=True)
        print(newupdate_df)
        newupdate_df = newupdate_df[newupdate_df["_merge"] == 'left_only'].drop('_merge', axis=1)
        print(newupdate_df)
        #Generating new primary key for extracted new records
        #newupdate_df['new_id'] = range(len(newupdate_df))

        #Attaching current date and end date
        current_date = datetime.datetime.now().date()
        print(current_date)

        # Update the end date for the existing records in the target table
        self.trgt_df.loc[self.trgt_df['Id'].isin(newupdate_df['Id']),'end_date'] = current_date

        newupdate_df['start_date'] = current_date
        newupdate_df['end_date'] = pd.to_datetime('9999-12-31').date()
        print(newupdate_df)

    def sqldisconnect(self):
        self.cursor.close()
        self.connection.close()


scd1 = IncrementalUpdate()
scd1.sqlconnect()
scd1.loaddata()
scd1.comparedf()
scd1.sqldisconnect()