from datetime import datetime, timedelta
from fix_holiday_info import insert_data

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


default_args = {
    'owner' : 'daniil',
    'start_date' : datetime(2025, 1, 1),
    'retries' : 2,
    'retry_delay': timedelta(seconds=5)
}

with DAG(
    'fix_balance_turnover',
    default_args = default_args,
    description = 'Reloading data to dm',
    catchup = False,
    schedule = '0 0 * * *',
    template_searchpath = '/'
 ) as dag:

    # в csv файле dict_currency.csv на одну запись больше, поэтому дополняем данные в бд
    clear_currency = SQLExecuteQueryOperator(
        task_id = 'clear_currency',
        conn_id = 'postgres_db',
        sql = """
            truncate table dm.dict_currency;
        """
    )

    currency_load = PythonOperator(
        task_id = 'currency_load',
        python_callable = insert_data,
        op_kwargs = {'schema': 'dm', 'table_name' : 'dict_currency'}
    )

    clear_turnover_balance = SQLExecuteQueryOperator(
        task_id = 'clear_turnover_balance',
        conn_id = 'postgres_db',
        sql = """
            truncate table dm.account_balance_turnover;
        """
    )

    fix_balance = SQLExecuteQueryOperator(
        task_id='fix_balance',
        conn_id='postgres_db',
        sql='/opt/airflow/scripts/fix_balance.sql',
        autocommit=True
    )

    (
        clear_currency >>
        currency_load >>
        clear_turnover_balance >>
        fix_balance
    )
