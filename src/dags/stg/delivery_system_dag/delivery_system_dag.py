import logging

import pendulum
from airflow.decorators import dag, task
from stg.delivery_system_dag.courier_loader import CourierLoader
from stg.delivery_system_dag.deliverys_loader import DeliveryLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_stg_delivery_system_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="couriers_load")
    def load_couriers():
        # создаем экземпляр класса, в котором реализована логика.
        courier_loader = CourierLoader(dwh_pg_connect, log)
        courier_loader.load_couriers()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    couriers_stg = load_couriers()

    @task(task_id="delyverys_load")
    def load_deliverys():
        # создаем экземпляр класса, в котором реализована логика.
        courier_loader = DeliveryLoader(dwh_pg_connect, log)
        courier_loader.load_deliverys(7)  # Вызываем функцию, которая перельет данные. За последние 7 дней

    # Инициализируем объявленные таски.
    deliverys_stg = load_deliverys()


    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    couriers_stg >> deliverys_stg # type: ignore


delivery_system_dag = sprint5_stg_delivery_system_dag()
