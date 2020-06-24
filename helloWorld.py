from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import psycopg2

def print_hello():
    return 'Hello world!'

def run_sql_query(connection, schema_name, table_name):
    sql_query = """
        SELECT *
        FROM information_schema.columns
        WHERE table_schema = '{schema}' AND table_name  = '{table_name}'
        ORDER BY ordinal_position
        LIMIT 5;
    """.format(
        schema=schema_name,
        table_name=table_name
    )
    cursor = connection.cursor()
    cursor.execute(sql_query)
    logging.info("executed query {}".format(cursor.query))
    records = cursor.fetchall()
    cursor.close()
    connection.commit()
    result = []
    for row in records:
        result.append(str(row[0]).lower())
    return result

def get_rows_count(connection, schema, table):
    sql_query = "SELECT count(*) from {}.{};"
    cursor = connection.cursor()
    cursor.execute(sql_query.format(schema, table))
    logging.info("executed query {}".format(cursor.query))
    result = cursor.fetchone()
    cursor.close()
    connection.commit()
    return result[0]


def check_if_table_exists(connection, schema, table):
    sql_query = "select exists(select * from information_schema.tables where table_schema=%s and " \
                "table_name=%s);"
    cursor = connection.cursor()
    cursor.execute(sql_query, (schema, table))
    logging.info("executed query {}".format(cursor.query))
    result = cursor.fetchone()
    cursor.close()
    connection.commit()
    logging.info(("table exists result : {}".format(result[0])))
    return result[0]

dag = DAG('hello_world', description='Simple tutorial DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

connection = psycopg2.connect("dbname=airflow_local user=abhishek password=''")
schema = 'airflow_local'
table = 'company'

dbtest_operator = PythonOperator(task_id='dbtest_task', op_args=[connection, schema, table],
                       python_callable=check_if_table_exists,
                       dag=dag)

dummy_operator >> dbtest_operator