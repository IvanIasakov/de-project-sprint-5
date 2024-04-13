from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime
import pendulum
import os
from pathlib import Path

dag = DAG('fill_cdm_dag',
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'cdm', 'courier_ledger'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)

def get_SQL()->str:
    file_name = os.path.dirname(os.path.realpath(__file__))+"/sql/"
    file_name+=os.path.splitext(os.path.basename(__file__))[0]+".sql"
    SQL=""
    if os.path.exists(file_name):
        fl=Path(file_name)
        SQL=fl.read_text()
    return SQL


fill_cdm_courier_ledger = PostgresOperator(
    task_id='fill_courier_ledger',
    postgres_conn_id='PG_WAREHOUSE_CONNECTION',
    sql=get_SQL(),
    dag=dag,
)

fill_cdm_courier_ledger