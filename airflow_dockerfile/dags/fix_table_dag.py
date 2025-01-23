from datetime import datetime, timedelta
import time
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


from psycopg2.extras import execute_values


def insert_data(schema, table_name):
    time.sleep(5)
    try:
        df = pd.read_csv(f'/opt/airflow/plugins/{table_name}.csv', delimiter=',', encoding='cp1251')
    except Exception as e:
        raise e
    print(df.iloc[0])

    existing_columns = df.columns
    if table_name == 'md_currency_d' and 'CURRENCY_CODE' in existing_columns:
        df = pd.read_csv(f'/opt/airflow/plugins /{table_name}.csv', delimiter=',', encoding='cp1252', dtype={'CURRENCY_CODE': str})

    postgres_hook = PostgresHook(postgres_conn_id='postgres_db')
    engine = postgres_hook.get_sqlalchemy_engine()

    date_columns = postgres_hook.get_records(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE data_type = 'date' AND table_schema = '{schema}' AND table_name = '{table_name}';
    """)




    primary_key_query = f"""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = '{schema}'
            AND tc.table_name = '{table_name}';
    """
    primary_keys = postgres_hook.get_records(primary_key_query)
    key_columns = [key[0] for key in primary_keys]

    if not key_columns:
        insert_query = f"""
            INSERT INTO "{schema}"."{table_name}" ({', '.join(f'"{col}"' for col in df.columns)})
            VALUES %s;
        """
    else:
        insert_query = f"""
            INSERT INTO "{schema}"."{table_name}" ({', '.join(f'"{col}"' for col in df.columns)})
            VALUES %s
            ON CONFLICT ({', '.join(f'"{col}"' for col in key_columns)}) DO UPDATE
            SET {', '.join(f'"{col}" = EXCLUDED."{col}"' for col in df.columns if col not in key_columns)};
        """

    conn = engine.raw_connection()

    try:
        with conn.cursor() as cur:
            for row in df.itertuples(index=False, name=None):
                execute_values(cur, insert_query, [row])
        conn.commit()
    except Exception as e:
        raise e
    finally:
        conn.close()

default_args = {
    'owner' : 'daniil',
    'start_date' : datetime(2025, 1, 1),
    'retries' : 2,
    'retry_delay': timedelta(seconds=5)
}

with DAG(
    'fix_table_dag',
    default_args = default_args,
    description = 'Loading data to rd',
    catchup = False,
    schedule = '0 0 * * *',
    template_searchpath = '/'
 ) as dag:

    product_load = PythonOperator(
        task_id = 'product_load',
        python_callable = insert_data,
        op_kwargs = {'schema': 'rd', 'table_name' : 'product'}
    )

    del_product_duplicate = SQLExecuteQueryOperator(
        task_id = 'del_product_duplicate',
        conn_id = 'postgres_db',
        sql = """
            delete from rd.product
            where ctid not in (
                select min(ctid)
                from rd.product
                group by product_rk, product_name, effective_from_date, effective_to_date);
        """
    )

    (
        product_load >> del_product_duplicate
    )
