from __future__ import print_function

import mysql.connector
from mysql.connector import errorcode
from tables import TABLES

def drop_database(cursor, db_name='default_db'):
    try:
        cursor.execute(
            "DROP DATABASE {}".format(db_name))
        
        print("Successfully dropped database: {}".format(db_name))
    except mysql.connector.Error as err:
        print("Failed dropping database: {}".format(err))
        exit(1)

if __name__ == '__main__':
    cnx = mysql.connector.connect(user='root', password='pass')
    cursor = cnx.cursor()
    
    db_name = 'default_db'

    drop_database(cursor, db_name)