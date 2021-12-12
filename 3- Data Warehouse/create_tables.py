import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries



def drop_tables(cur, conn):
    """This function is used for dropping tables it they're exists, in redshift.
    Args:
        cur(cursor): This is a cursor witch will have the query and will execute.
        conn(str)  : The connection string
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error. Impossible to execute the query for dropping table.")
            print(e)


def create_tables(cur, conn):
    """This function is used for creating the tables.
     Args:
        cur(cursor): This is a cursor witch will have the query and will execute.
        conn(str)  : The connection string
        
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error. Impossible to execute the query for creating table.")
            print(e)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    drop_tables(cur, conn)
    
    create_tables(cur, conn)
    print("\tTables created with success")
    
    conn.close()    
        


if __name__ == "__main__":
    main()