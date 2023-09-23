from main.db_handler import DatabaseHandler
from processing.data_pipeline import pipeline
from utils.queries import TABLES
from tests.tests import *
from config.constants import DATA_PATH

def bootstrap(data_path):
    check_unique_activities(data_path)
    user, activity, track_point = pipeline(data_path)
    run_tests(user, activity, track_point)
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
    bootstrap(DATA_PATH)
