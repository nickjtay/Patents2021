import csv
import os
import sqlite3
import sys
from operator import itemgetter
from sqlite3 import Error

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return conn

def delete_table(conn, delete_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(delete_table_sql)
    except Error as e:
        print(e)

def main():
    field_size_limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(field_size_limit)
            break
        except OverflowError:
            field_size_limit = int(field_size_limit / 10)

    database = r"D:\Patents\DB\patents - Copy.db"
    sql_delete_brf_sum_text_table = """ DROP TABLE brf_sum_text; """
    conn = create_connection(database)

    if conn is not None:
        # create projects table
        delete_table(conn, sql_delete_brf_sum_text_table)
    else:
        print("Error! cannot create the database connection.")

if __name__ == '__main__':
    main()

