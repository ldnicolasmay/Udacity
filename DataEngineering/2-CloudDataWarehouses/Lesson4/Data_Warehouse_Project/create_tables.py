import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Iterates over the list of SQL queries that drop all the necessary Redshift tables. Dropping these tables is required to prevent (1) Redshift from throwing an error if we try to create an already existing table or (2) loading duplicate data into already existing tables.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Iterates over the list of SQL queries that create all the necessary Redshift tables.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Driver that (1) parses the config file, (2) connects to the Redshift cluster to create a cursor, (3) runs the `drop_tables` and `create_tables` functions, and (3) closes the cluster connecction.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()