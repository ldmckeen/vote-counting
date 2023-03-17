import json
import logging
import os
import sqlite3
import sys

from db import db_connect


def insert_into_db(db_cursor, votes_data):
    """Ingest Voting Data into Data Warehouse."""

    # Loop through file data and insert into votes table row by row
    for line in votes_data:
        line_values = json.loads(line)
        # Ensure only values we want from file go into table
        line_tuple = tuple([
            int(line_values['Id']),
            int(line_values['PostId']),
            int(line_values['VoteTypeId']),
            line_values['CreationDate']
        ])
        sql_insert = "INSERT INTO votes VALUES(?, ?, ?, ?)"
        try:
            db_cursor.execute(sql_insert, line_tuple)
        except sqlite3.Error as er:
            print(f'SQLite error: {er}')
            continue


def ingest_data(file_name):
    """Pull Voting Data from source."""
    try:
        with open(file_name) as votes_in:
            db_name = 'warehouse.db'

            # Instantiate DB connection and assign cursor
            try:
                conn = db_connect(db_name)
            except:
                # logging(f"{conn}")
                logging.error()

            cur = conn.cursor()

            sql = """
            CREATE TABLE IF NOT EXISTS votes(
            Id INTEGER PRIMARY KEY,
            PostId INTEGER,
            VoteTypeId INTEGER,
            CreationDate TIMESTAMP)
            """
            try:
                cur.execute(sql)
            except:
                logging.ERROR()
                conn.close()

            insert_into_db(cur, votes_in)  # Insert data into DB

            res = cur.execute("SELECT * FROM votes")
            res_data = res.fetchall()

            conn.commit()  # Commit result to DB
            conn.close()  # Close DB Connection

            return res_data

    except FileNotFoundError as er:
        print(f"Please download the dataset using 'make fetch_data'{er}")


def main():
    """Run Script."""
    file_name = sys.argv[1]
    ingest_data(file_name)


if __name__ == '__main__':
    """
    Script Entrypoint.
    """

    try:
        main()
    except Exception as err:
        logging.info(
            f'{os.path.basename(__file__)} failed due to error: {err}'
        )
        raise err
