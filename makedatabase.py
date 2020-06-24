import psycopg2
import logging

def make_database():
    dbname = 'airflow_local'
    username = 'abhishek'
    tablename = 'weather_table'

    conn = psycopg2.connect(database=dbname, user=username)

    curr = conn.cursor()

    create_table = """ CREATE TABLE IF NOT EXISTS %s
                (
                    city    TEXT,
                    country TEXT,
                    weather TEXT
                )
                """ % tablename

    table_exists_new = """ SELECT EXISTS 
                (
                       SELECT 1
                       FROM   information_schema.tables 
                       WHERE  table_schema = 'public'
                       AND    table_name = 'weather_table'
                );
                """ 


    table_exists = "select exists(select * from information_schema.tables where table_schema='airflow_local' and table_name='weather_table');"

    curr.execute(table_exists_new)
    result = curr.fetchone()
    logging.info(("table exists result : {}".format(result[0])))
    print ('result :' + str(result[0]))
    conn.commit()
    conn.close()


if __name__ == "__main__":
    make_database()