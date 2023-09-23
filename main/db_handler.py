#from DbConnector import DbConnector
from tabulate import tabulate
from main.db_connectors import MySQLConnector
from mysql.connector import errorcode, Error
import pandas as pd
from sqlalchemy import create_engine
import os

class DatabaseHandler:
    def __init__(self):
        self.connection = MySQLConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_tables(self, tables):
        for table_name in tables:
            table_description = tables[table_name]
            try:
                print("Creating table {}: ".format(table_name), end='')
                self.cursor.execute(table_description)
                self.db_connection.commit()
            except Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("already exists.")
                else:
                    print(err.msg)
            else:
                print("OK")

    def drop_tables(self, tables):
        self.cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
        for table_name in tables:
            print("Dropping table %s..." % table_name)
            query = "DROP TABLE %s"
            self.cursor.execute(query % table_name)
        self.cursor.execute("SET FOREIGN_KEY_CHECKS=1;")

    def write_dataframe(self, dataframe: pd.DataFrame, table_name: str, chunk_size=5000):
        try:
            engine = create_engine(f"mysql+mysqlconnector://{os.environ.get('DB_USER')}:{os.environ.get('DB_PASSWORD')}@tdt4225-10.idi.ntnu.no:3306/default_db")

            # Calculate the number of chunks
            num_chunks = (len(dataframe) - 1) // chunk_size + 1

            for i in range(num_chunks):
                # Split DataFrame into a chunk
                chunk = dataframe[i * chunk_size: (i + 1) * chunk_size]

                # Insert the chunk into the database
                chunk.to_sql(table_name, con=engine, if_exists='append', index=False)
                print(f"{table_name}: Inserted chunk {i + 1}/{num_chunks}")

            print(f"{table_name}: Dataframe written to database in {num_chunks} chunks")

        except Exception as e:
            print("ERROR: Failed to write dataframe to database:", e)

    def insert_data(self, table_name):
        names = ['Bobby', 'Mc', 'McSmack', 'Board']
        for name in names:
            # Take note that the name is wrapped in '' --> '%s' because it is a string,
            # while an int would be %s etc
            query = "INSERT INTO %s (name) VALUES ('%s')"
            self.cursor.execute(query % (table_name, name))
        self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    program = None
    try:
        program = DatabaseHandler()
        program.show_tables()
        df = pd.DataFrame({'id':['sd','ab','cd'],'has_labels':[True,False,True]})
        print(df)
        program.write_dataframe(dataframe=df, table_name="user")
        program.fetch_data("user")
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
