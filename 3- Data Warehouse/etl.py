import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """This function is for copying data into two tables.
    Args:
        cur(cursor) : This is the cursor with contain the query
        conn(String): This is the connection string to the database sparkifydb
    """
    list_copy_table = ['staging_events','staging_songs']
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print("\tThe copy of data to table '' {} '' is finished".format(list_copy_table[copy_table_queries.index(query)]))
        except Exception as e:
            print(e)


def insert_tables(cur, conn):
    """This function is for insertind data into other tables.
    Args:
        cur(cursor) : This is the cursor with contain the query
        conn(String): This is the connection string to the database sparkifydb
    """
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)
    

    
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    
    insert_tables(cur, conn)
    print("\tInsert data to other tables is finished")
    
    conn.close()


if __name__ == "__main__":
    main()