import gensim
import csv
import sqlite3
from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import STOPWORDS
from nltk.stem import WordNetLemmatizer, SnowballStemmer
from nltk.stem.porter import *
from patent_import import create_connection, create_table
import numpy as np
np.random.seed(2018)
import nltk
nltk.download('wordnet')



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

    ##for row in rows:
    ##    print(row)
    return rows

def select_one_brf_sum_text(conn, uuid):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("""SELECT text FROM brf_sum_text WHERE uuid = ?""", uuid)
    rows = cur.fetchall()
    return rows

stemmer = SnowballStemmer("english")

def lemmatize_stemming(text):
    return stemmer.stem(WordNetLemmatizer().lemmatize(text, pos='v'))

def preprocess(text):
    result = []
    for token in gensim.utils.simple_preprocess(text):
        if token not in gensim.parsing.preprocessing.STOPWORDS and len(token) > 3:
            result.append(lemmatize_stemming(token))
    return result

def create_lemma_text_table(conn):
    sql_create_lemma_text_table = """ CREATE TABLE IF NOT EXISTS lemma_text (
                                    uuid PRIMARY KEY ASC,
                                    text BLOB
                                    ); """
    if conn is not None:
        create_table(conn, sql_create_lemma_text_table)
    else:
        print("Error! cannot create the database connection.")

def insert_lemma_text(conn, row):
    """
    Create a new project into the projects table
    :param conn:
    :param project:
    :return: project id
    """
    sql = ''' INSERT INTO lemma_text(uuid,text)
              VALUES(?,?) '''

    cur = conn.cursor()
    cur.execute(sql, row)
    conn.commit()
    return cur.lastrowid

def drop_lemma_text(conn):
    """
    Create a new project into the projects table
    :param conn:
    :param project:
    :return: project id
    """
    sql = ''' DROP TABLE lemma_text'''
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    return cur.lastrowid

#drop_lemma_text(conn)

create_lemma_text_table(conn)

filename = 'uuids.csv'

with open(filename, 'r') as csvfile:
    datareader = csv.reader(csvfile)
    for row in datareader:
        patent = select_one_brf_sum_text(conn, (row[0],))
        patent=patent[0][0].replace('\n',' ')
        patent = preprocess(patent)
        insert_lemma_text(conn, [row[0],str(patent)])