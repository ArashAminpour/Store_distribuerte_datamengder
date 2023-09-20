from __future__ import print_function

import mysql.connector
from mysql.connector import errorcode
from queries import TABLES

def create_database(cursor, db_name='default_db'):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(db_name))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

if __name__ == '__main__':
    cnx = mysql.connector.connect(user='root', password='pass')
    cursor = cnx.cursor()
    
    db_name = 'default_db'
    print(TABLES)

    try:
        cursor.execute("USE {}".format(db_name))
    except mysql.connector.Error as err:
        print("Database {} does not exists.".format(db_name))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor)
            print("Database {} created successfully.".format(db_name))
            cnx.database = db_name
        else:
            print(err)
            exit(1)

    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print("Creating table {}: ".format(table_name), end='')
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

    cursor.close()
    cnx.close()