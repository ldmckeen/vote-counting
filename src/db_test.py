import sqlite3

from db import db_connect


def test_sqlite3_connection():
    with sqlite3.connect('warehouse.db') as con:
        cursor = con.cursor()
        assert list(cursor.execute('SELECT 1')) == [(1,)]


def test_db_connect():
    db_name = 'warehouse.db'
    conn = db_connect(db_name)
    cursor = conn.cursor()
    assert list(cursor.execute('SELECT 1')) == [(1,)]
