from main.db_handler import DatabaseHandler
from utils.queries import TABLES

def cleanup():
    try:
        program = DatabaseHandler()
        program.drop_tables(tables=TABLES)
        program.connection.close_connection()
    except Exception as e:
        print("ERROR: Failed to use database:", e)


if __name__ == '__main__':
    cleanup()