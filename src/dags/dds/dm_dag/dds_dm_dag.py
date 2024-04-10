import logging

import pendulum
from airflow.decorators import dag, task
from dds.dm_dag.dm_users_loader import UserLoader
from dds.dm_dag.dm_restaurants_loader import RestLoader
from dds.dm_dag.dm_timestamps_loader import TsLoader
from dds.dm_dag.dm_products_loader import ProductLoader
from dds.dm_dag.dm_orders_loader import OrdersLoader
from dds.dm_dag.fct_product_sales import FactsLoader
from dds.dm_dag.bonus_update import BonusLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_dds_dm_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="users_load")
    def load_users():
        # создаем экземпляр класса, в котором реализована логика.
        user_loader = UserLoader(dwh_pg_connect, dwh_pg_connect, log)
        user_loader.load_users()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    users_dms = load_users()

    @task(task_id="rests_load")
    def load_rests():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = RestLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_rests()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    rests_dms = load_rests()

    @task(task_id="tss_load")
    def load_tss():
        # создаем экземпляр класса, в котором реализована логика.
        tss_loader = TsLoader(dwh_pg_connect, dwh_pg_connect, log)
        tss_loader.load_tss()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    tss_dms = load_tss()

    @task(task_id="products_load")
    def load_prods():
        # создаем экземпляр класса, в котором реализована логика.
        prod_loader = ProductLoader(dwh_pg_connect, dwh_pg_connect, log)
        prod_loader.load_products()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    prods_dms = load_prods()

    @task(task_id="orders_load")
    def load_orders():
        # создаем экземпляр класса, в котором реализована логика.
        order_loader = OrdersLoader(dwh_pg_connect, dwh_pg_connect, log)
        order_loader.load_orders()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    orders_dms = load_orders()

    @task(task_id="facts_load")
    def load_facts():
        # создаем экземпляр класса, в котором реализована логика.
        facts_loader = FactsLoader(dwh_pg_connect, dwh_pg_connect, log)
        facts_loader.load_forders()  # Вызываем функцию, которая перельет данные.
        bonus_loader = BonusLoader(dwh_pg_connect, dwh_pg_connect, log)
        bonus_loader.load_bonus()
    # Инициализируем объявленные таски.
    facts_dms = load_facts()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    users_dms >> rests_dms >> tss_dms >> prods_dms >> orders_dms >> facts_dms # type: ignore


dds_dm_dag = sprint5_dds_dm_dag()
