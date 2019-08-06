import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, create_schema_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def create_schemas(cur, conn):
    for query in create_schema_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    host = config.get('CLUSTER', 'HOST')
    dbname = config.get('CLUSTER', 'DB_NAME')
    user = config.get('CLUSTER', 'DB_USER')
    password= config.get('CLUSTER', 'DB_PASSWORD')
    port = config.get('CLUSTER', 'DB_PORT')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(host,dbname,user,password,port))
    
    try:
        cur = conn.cursor()
    except Exception as e:
        print("connection failed:",e)
        
    #Create schemas
    try:
        create_schemas(cur, conn)
    except Exception as e:
        print("Schema creation failed:",e)
    
    #Drop tables
    try:
        drop_tables(cur, conn)
    except Exception as e:
        print("Failed to drop tables:",e)
    
    #Create tables
    try:
        create_tables(cur, conn)
    except Exception as e:
        print("Failed to create tables:",e)

    conn.close()


if __name__ == "__main__":
    main()