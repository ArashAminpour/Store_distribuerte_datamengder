import sys
sys.path.append('/Users/nicolas/Workspace/School/V2023/Big Data/Exercise 2/Store_distribuerte_datamengder')

from main.db_handler import DatabaseHandler
from queries import TABLES

def main():
    try:
        program = DatabaseHandler()
        program.create_tables(tables=TABLES)
        program.connection.close_connection()
    except Exception as e:
        print("ERROR: Failed to use database:", e)


if __name__ == '__main__':
    main()
