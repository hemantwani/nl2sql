import sqlite3
import pandas as pd
import re
import datetime
import os


class Import:
    def __init__(self):
        pass
    def import_data(self):
        now = re.sub(r'[^A-Za-z0-9]+','',str(datetime.datetime.now()))
        session_id = "122132445464"
        file_name = session_id+now
        directory = "./Session_DB/{}/".format(session_id)
        if not os.path.exists(directory):
            os.makedirs(directory)

        # data = pd.read_csv("data_set.csv")
        data = pd.read_csv("data1.csv")

        conn = sqlite3.connect(directory + "{}".format(file_name))
        cols = list(data)

        for every_col in cols:
            dtype = data[every_col].dtype
            if dtype == "object":
                data[every_col] = data[every_col].str.lower()

        data.to_sql('sample', conn, if_exists='replace', index=False)
        conn.commit()
        cursor = conn.cursor()

        ##########read the sql db
        self.df = pd.read_sql_query("SELECT * FROM sample", conn)  ##every time reading large data takes much time.

        return {"dataframe": self.df, "conn": conn, "cursor": cursor}