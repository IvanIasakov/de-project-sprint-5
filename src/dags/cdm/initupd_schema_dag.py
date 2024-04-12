import logging
import os

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from cdm.schema_initupd import SchemaDdl
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'cdm', 'schema', 'ddl', 'update_dm'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_proj_cdm_initupd_schema_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Забираем путь до каталога с SQL-файлами из переменных Airflow.
    #ddl_path = Variable.get("EXAMPLE_STG_DDL_FILES_PATH")
    ddl_path = os.path.dirname(os.path.realpath(__file__))+"/ddl"

    # Объявляем таск, который создает структуру таблиц.
    @task(task_id="schema_initupd")
    def schema_initUpd():
        rest_loader = SchemaDdl(dwh_pg_connect, log)
        rest_loader.initUpd_schema(ddl_path)
        

    # Инициализируем объявленные таски.
    initUpd_schema = schema_initUpd()

    # Задаем последовательность выполнения тасков. У нас только инициализация схемы.
    initUpd_schema  # type: ignore


# Вызываем функцию, описывающую даг.
cdm_initupd_schema_dag = sprint5_proj_cdm_initupd_schema_dag()  # noqa
