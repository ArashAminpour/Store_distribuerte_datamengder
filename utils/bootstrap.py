from main.db_handler import DatabaseHandler
from utils.queries import TABLES
from processing.data_pipeline import pipeline

def bootstrap():
    user, activity, track_point = pipeline()
    try:
        program = DatabaseHandler()
        program.create_tables(tables=TABLES)
        program.show_tables()
        program.write_dataframe(dataframe=user, table_name="user")
        program.write_dataframe(dataframe=activity, table_name="activity")
        program.write_dataframe(dataframe=track_point, table_name="track_point")
        program.connection.close_connection()
    except Exception as e:
        print("ERROR: Failed to use database:", e)


if __name__ == '__main__':
    bootstrap()
