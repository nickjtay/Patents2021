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

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
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

    database = r"D:\Patents\DB\patents.db"
    sql_create_patent_inventor_table = """ CREATE TABLE IF NOT EXISTS patent_inventor (
                                    patent_id string,
                                    inventor_id string,
                                    location_id string
                                    ); """
    conn = create_connection(database)

    if conn is not None:
        # create projects table
        create_table(conn, sql_create_patent_inventor_table)
    else:
        print("Error! cannot create the database connection.")

    cols = ["patent_id", "inventor_id", "location_id"]
    chunksize = 10000
    rootdir =  r"D:\Patents\Data\Extracted\patent_inventor"
    for subdir, dirs, files in os.walk(rootdir):
        for file in files:
            filepath = subdir + os.sep + file
            if filepath.endswith(".tsv"):
                with conn, open(filepath, encoding="utf8") as f:
                    reader = csv.DictReader(f, dialect='excel-tab')
                    chunk = []
                    for i, row in enumerate(reader):
                        if i % chunksize == 0 and i > 0:
                            conn.executemany(
                                """
                                INSERT INTO patent_inventor
                                    VALUES(?, ?, ?)
                                """, chunk
                            )
                            chunk = []
                        items = itemgetter(*cols)(row)
                        chunk.append(items)
            continue
        else:
            continue

if __name__ == '__main__':
    main()

