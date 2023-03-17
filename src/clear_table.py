import json
import logging
import os
import sqlite3
import sys

from db import db_connect


def clear_table(table):
    """Helper function to clear table."""
    try:
        db_name = 'warehouse.db'

        # Instantiate DB connection and assign cursor
        conn = db_connect(db_name)
        cur = conn.cursor()

        sql = "DROP TABLE IF EXISTS " + table
        cur.execute(sql)

        conn.commit()  # Commit result to DB
        conn.close()  # Close DB Connection

    except FileNotFoundError as er:
        print(f"Please download the dataset using 'make fetch_data'{er}")


def main():
    """Run Script."""
    table_name = sys.argv[1]
    clear_table(table_name)


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
