import psycopg2
import sys
import datetime
from config import DB

def flush_db(con):
    cur = con.cursor()
    cur.execute('DROP DATABASE IF EXISTS {};'.format(DB['dbname']))

def create_db(con):
    cur = con.cursor()
    cur.execute('CREATE DATABASE {};'.format(DB['dbname']))
    con.close()
    con = psycopg2.connect(host=DB['ip'], user=DB['username'], password=DB['password'], dbname=DB['dbname'])
    con.autocommit = True
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE urls (
            id SERIAL UNIQUE,
            url char(255)  NOT NULL,
            time_added timestamp  NOT NULL,
            checked boolean  DEFAULT FALSE,
            UNIQUE (url)
        );

        CREATE TABLE links (
            url char(255)  NOT NULL,
            link char(255)  NOT NULL,
            FOREIGN KEY (url) REFERENCES urls(url) ON UPDATE CASCADE ON DELETE CASCADE
        );

        CREATE TABLE url_info (
            url char(255)  NOT NULL,
            real_url char(255)  NOT NULL,
            page_size integer  NOT NULL,
            answer_code char(255)  NOT NULL,
            time_checked timestamp  NOT NULL,
            FOREIGN KEY (url) REFERENCES urls(url) ON UPDATE CASCADE ON DELETE CASCADE
        );
        """)
    cur.execute("INSERT INTO urls (url, time_added) VALUES ('https://google.com', '{}');".format(datetime.datetime.now()))
    cur.execute("INSERT INTO urls (url, time_added) VALUES ('https://yandex.ru', '{}');".format(datetime.datetime.now()))
    con.close()

if __name__ == "__main__":
    con = psycopg2.connect(host=DB['ip'], user=DB['username'], password=DB['password'], dbname='postgres')
    con.autocommit = True
    flush_db(con)
    create_db(con)