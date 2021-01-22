"""
test case for creating a sqllite db connection and trying to fetch
the information from the tables and create and modify tables.
"""
import sqlite3

class CreateDBConnection():
    def __init__(self, dbFile):
        try:
            global conn
            conn = sqlite3.connect(dbFile)
            cursor = conn.cursor()
            conn.commit()
            # cursor.execute("select * from testCase t1 JOIN testCaseTwo t2 ")
            # # cursor.execute("select * from testCase")
            # # cursor.fetchall()
            # for row in cursor:
            #     print(row)
        except Exception as e:
            print("Exception is:- ", e)

        finally:
            if (self.checkConn()):
                conn.close()

    def checkConn(self):
        try:
            conn.cursor()
            return True
        except Exception as e:
            return False


if __name__=="__main__":
    CreateDBConnection("/home/fidel/testDB.db")

