import logging
import os

from db import db_connect


def detect_outliers():
    """Detect and Print Outlier vote count per week."""
    try:
        db_name = 'warehouse.db'

        # Instantiate DB connection and assign cursor
        conn = db_connect(db_name)
        cur = conn.cursor()

        # Fetch vote table data Query
        sql = "SELECT Year, WeekNumber, voteCountPerWeek " \
              "FROM " \
              "(" \
            "SELECT Year, WeekNumber, voteCountPerWeek," \
            "AVG(voteCountPerWeek) OVER (PARTITION BY Year) AS avgPerWeek," \
            "(AVG(voteCountPerWeek) OVER (PARTITION BY Year)) * 0.2 AS benchMark," \
            "ABS(voteCountPerWeek - (AVG(voteCountPerWeek) OVER (PARTITION BY Year))) AS diffToBench," \
            "case when ABS(voteCountPerWeek - (AVG(voteCountPerWeek) over (PARTITION BY Year))) > " \
            "(avg(voteCountPerWeek) OVER (PARTITION BY Year))*0.2 "\
            "THEN 1 "\
            "ELSE 0 "\
            "END AS Flag "\
            "FROM " \
            "(" \
            "SELECT "\
            "strftime('%Y', CreationDate) Year "\
            ",strftime('%W', CreationDate) WeekNumber "\
            ",COUNT(*) AS voteCountPerWeek "\
            "FROM votes GROUP BY Year, WeekNumber) a "\
            "GROUP BY Year, WeekNumber, voteCountPerWeek" \
              ") a WHERE flag = 1; "\

        # Store and print results from outlier query
        res = cur.execute(sql)
        data = res.fetchall()
        for row in data:
            print(row)

        # conn.commit()  # Commit result to DB
        conn.close()  # Close DB Connection
        return data

    except ConnectionError as e:
        print(f"Issue Retrieving Data{e}")


def main():
    """Run Script."""
    detect_outliers()


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
