import csv
from brf_sum_text_import import create_connection

database = r"D:\Patents\DB\patents.db"
conn = create_connection(database)

def select_uuids(conn):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    ## cur.execute("SELECT text FROM brf_sum_text LIMIT 1")
    cur.execute("SELECT DISTINCT uuid FROM brf_sum_text")
    rows = cur.fetchall()
    return rows

uuids = select_uuids(conn)

with open('uuids.csv', 'w', newline='') as csvfile:
    spamwriter = csv.writer(csvfile, delimiter=',',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
    for i in uuids:
        spamwriter.writerow(i)