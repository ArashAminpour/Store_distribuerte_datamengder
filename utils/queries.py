DB_NAME = 'employees'

TABLES = {}

TABLES['user'] = '''
    CREATE TABLE user (
        id VARCHAR(255) PRIMARY KEY,
        has_labels BOOLEAN
    );
'''

TABLES['activity'] = '''
    CREATE TABLE activity (
        id VARCHAR(255) PRIMARY KEY,
        user_id VARCHAR(255),
        transportation_mode VARCHAR(255),
        start_date_time DATETIME,
        end_date_time DATETIME,
        FOREIGN KEY (user_id) REFERENCES user(id) ON DELETE CASCADE
    );
'''

# TABLES['activity'] = '''
#     CREATE TABLE activity (
#         id INT PRIMARY KEY AUTO_INCREMENT,
#         user_id VARCHAR(255),
#         transportation_mode VARCHAR(255),
#         start_date_time DATETIME,
#         end_date_time DATETIME,
#         FOREIGN KEY (user_id) REFERENCES user(id) ON DELETE CASCADE
#     );
# '''

TABLES['track_point'] = '''
    CREATE TABLE track_point (
        id INT PRIMARY KEY AUTO_INCREMENT,
        activity_id INT,
        lat DOUBLE,
        lon DOUBLE,
        altitude INT,
        date_days DOUBLE,
        date_time DATETIME,
        FOREIGN KEY (activity_id) REFERENCES activity(id) ON DELETE CASCADE
    );
'''