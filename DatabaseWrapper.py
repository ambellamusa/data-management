import mysql.connector
from Configurations import Data


class MySqlDatabase:
    def __init__(self):
        self.mydb = mysql.connector.connect(
            host="", #insert details
            user="",
            passwd="",
            database=""
        )

        self.mycursor = self.mydb.cursor(buffered=True)
        self.mycursor.execute("CREATE TABLE IF NOT EXISTS AI_Expectations"
                              "(file_name VARCHAR(255),"
                              " client_id VARCHAR(255),"
                              " upload_date DATETIME,"
                              " columns VARCHAR(512),"
                              " Min FLOAT,"
                              " Max FLOAT"
                              ")"
                              )

    def save_to_database(self, class_obj_arg):
        sql = ("INSERT INTO AI_Expectations(file_name,"
                              "client_id, upload_date, columns, Min, Max)"
                              "values (%s, %s,%s, %s, %s, %s)")
        value = (class_obj_arg.FileName, class_obj_arg.ClientID, class_obj_arg.UploadDate,
                 class_obj_arg.Columns, class_obj_arg.Min, class_obj_arg.Max)

        self.mycursor.execute(sql, value)
        self.mydb.commit()

    def find_record(self, file_name_arg):
        sql = "SELECT * from AI_Expectations " \
              "where file_name = '%s'" % file_name_arg
        self.mycursor.execute(sql)
        row = self.mycursor.fetchone()
        if row is not None:
            return True
        else:
            return False

    def close_connection(self):
        self.mydb.close()