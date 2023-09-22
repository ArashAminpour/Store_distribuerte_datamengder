from main.db_handler import DatabaseHandler
from utils.queries import TABLES

def bootstrap():
    try:
        program = DatabaseHandler()
        program.create_tables(tables=TABLES)
        program.connection.close_connection()
    except Exception as e:
        print("ERROR: Failed to use database:", e)


if __name__ == '__main__':
    bootstrap()
