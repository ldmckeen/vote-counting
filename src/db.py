import sqlite3


def db_connect(db_name):
    try:
        conn = sqlite3.connect(db_name)
    except sqlite3.Error as er:
        print(f'SQLite error: {er}')
        return er

    return conn
