
import psycopg2
import configparser
import sql_queries as sq

def create_table(conn, cur):
    for table in sq.list_query_table_to_create:
        try:
            cur.execute(table)
            conn.commit()
        except psycopg2.Error as e:
            print(f"Error - Cannot execute the query {table}")
            print(e)


if __name__ == "__main__":
    config_flightxru = configparser.ConfigParser()
    config_flightxru.read("configuration/flightxru.cfg")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config_flightxru["CLUSTER"].values()))
    cur = conn.cursor()

    create_table(conn, cur)

    conn.close()


